// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Redecryptor queue regressions with real Megolm sessions, a persisted SQLite
//! event cache and an ordinary live `Timeline`.

use std::{collections::BTreeSet, path::Path, sync::Arc, time::Duration};

use futures_core::Stream;
use futures_util::{FutureExt as _, StreamExt as _, pin_mut};
use matrix_sdk::{
    Client, Room,
    deserialized_responses::{TimelineEvent, TimelineEventKind},
    event_cache::DecryptionRetryRequest,
    test_utils::mocks::MatrixMockServer,
};
use matrix_sdk_base::{
    crypto::{
        DecryptionSettings, EncryptionSettings, OlmMachine, TrustRequirement,
        olm::InboundGroupSession, store::CryptoStore,
    },
    event_cache::store::{EventCacheStoreLockGuard, EventCacheStoreLockState},
    linked_chunk::{ChunkIdentifier, LinkedChunkId, Position, Update},
    sync::{JoinedRoomUpdate, RoomUpdates},
};
use matrix_sdk_common::cross_process_lock::CrossProcessLockConfig;
use matrix_sdk_test::{JoinedRoomBuilder, async_test};
use matrix_sdk_ui::timeline::{RoomExt, Timeline};
use ruma::{
    EventId, RoomId, device_id, event_id,
    events::{
        AnySyncStateEvent, AnySyncTimelineEvent, room::encrypted::OriginalSyncRoomEncryptedEvent,
    },
    room_id,
    serde::Raw,
    user_id,
};
use serde_json::{Value, json};
use tempfile::TempDir;

const UTD_ID: &str = "$utd:example.org";

fn utd_id() -> &'static EventId {
    event_id!("$utd:example.org")
}

fn test_room_id() -> &'static RoomId {
    room_id!("!redecryption:example.org")
}

fn event(event_type: &str, content: Value) -> Value {
    json!({
        "event_id": UTD_ID, "room_id": test_room_id(), "sender": "@alice:example.org",
        "origin_server_ts": 1000, "type": event_type, "content": content,
        "unsigned": {"age": 42},
    })
}

fn raw(event: &Value) -> Raw<AnySyncTimelineEvent> {
    Raw::new(event).unwrap().cast_unchecked()
}

fn create_event(room_id: &RoomId, event_id: &str) -> Raw<AnySyncStateEvent> {
    let mut create = event("m.room.create", json!({"room_version": "11"}));
    create["room_id"] = json!(room_id);
    create["state_key"] = json!("");
    create["event_id"] = json!(event_id);
    Raw::new(&create).unwrap().cast_unchecked()
}

fn text_message() -> Value {
    event("m.room.message", json!({"msgtype": "m.text", "body": "Hello"}))
}

fn session_id(envelope: &Value) -> String {
    envelope["content"]["session_id"].as_str().unwrap().to_owned()
}

/// Encrypt `plaintext` with a fresh outbound session of a sender that isn't
/// the test client, so the client can't decrypt it until its keys are imported.
async fn encrypted_fixture(room_id: &RoomId, plaintext: &Value) -> (Value, OlmMachine) {
    let sender = OlmMachine::new(user_id!("@alice:example.org"), device_id!("ALICE")).await;
    sender
        .share_room_key(room_id, std::iter::empty(), EncryptionSettings::default())
        .await
        .unwrap();
    let encrypted = sender
        .encrypt_room_event_raw(
            room_id,
            plaintext["type"].as_str().unwrap(),
            &Raw::new(&plaintext["content"]).unwrap().cast_unchecked(),
        )
        .await
        .unwrap();
    let mut envelope = event("m.room.encrypted", serde_json::to_value(encrypted.content).unwrap());
    envelope["room_id"] = json!(room_id);
    envelope["unsigned"] = plaintext["unsigned"].clone();
    (envelope, sender)
}

async fn decrypt(room: &Room, envelope: &Value) -> TimelineEvent {
    room.decrypt_event(raw(envelope).cast_ref_unchecked::<OriginalSyncRoomEncryptedEvent>(), None)
        .await
        .unwrap()
}

fn with_event_id(event: &TimelineEvent, event_id: &str) -> TimelineEvent {
    let mut copy = event.clone();
    let mut value: Value = serde_json::from_str(copy.raw().json().get()).unwrap();
    value["event_id"] = json!(event_id);
    copy.replace_raw(Raw::new(&value).unwrap().cast_unchecked());
    copy
}

/// Model keys received by the notification process: persist real imported
/// keys without notifying this process's room-key stream. This isolates the
/// explicit and cache-driven retry paths.
async fn import_keys_silently(client: &Client, sender: &OlmMachine) {
    let keys = sender.store().export_room_keys(|_| true).await.unwrap();
    let sessions = keys.iter().map(|key| InboundGroupSession::from_export(key).unwrap()).collect();
    let machine = client.olm_machine_for_testing().await;
    CryptoStore::save_inbound_group_sessions(&**machine.as_ref().unwrap().store(), sessions, None)
        .await
        .unwrap();
}

async fn store(client: &Client) -> EventCacheStoreLockGuard {
    let (EventCacheStoreLockState::Clean(store) | EventCacheStoreLockState::Dirty(store)) =
        client.event_cache_store().lock().await.unwrap();
    store
}

async fn barrier(client: &Client) {
    tokio::time::timeout(
        Duration::from_secs(5),
        client.event_cache().redecryptor_barrier_for_testing(),
    )
    .await
    .expect("the redecryptor must consume and finish its queued work");
}

async fn cache_event(client: &Client, room_id: &RoomId, event: TimelineEvent) {
    let mut update = JoinedRoomUpdate::default();
    update.timeline.events.push(event);
    let mut updates = RoomUpdates::default();
    updates.joined.insert(room_id.to_owned(), update);
    client.event_cache().handle_room_updates(updates).await.unwrap();
}

async fn sqlite_client(server: &MatrixMockServer, directory: &Path) -> Client {
    let client = server
        .client_builder()
        .on_builder(|builder| {
            builder
                .sqlite_store(directory, None)
                .cross_process_store_config(CrossProcessLockConfig::SingleProcess)
                .with_decryption_settings(DecryptionSettings {
                    sender_device_trust_requirement: TrustRequirement::Untrusted,
                })
        })
        .build()
        .await;
    client.event_cache().subscribe().unwrap();
    client
}

async fn setup_sqlite() -> (MatrixMockServer, Client, Room, TempDir) {
    let server = MatrixMockServer::new().await;
    let directory = tempfile::tempdir().unwrap();
    let client = sqlite_client(&server, directory.path()).await;
    server
        .mock_sync()
        .ok_and_run(&client, |builder| {
            builder.add_joined_room(
                JoinedRoomBuilder::new(test_room_id())
                    .add_state_event(create_event(test_room_id(), "$create:example.org")),
            );
        })
        .await;
    server.mock_room_state_encryption().plain().mount().await;
    let room = client.get_room(test_room_id()).unwrap();
    (server, client, room, directory)
}

async fn wait_for_item<S: Stream + Unpin>(
    timeline: &Timeline,
    updates: &mut S,
    event_id: &EventId,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while timeline.item_by_event_id(event_id).await.is_none() {
            updates.next().await.expect("live timeline stream");
        }
    })
    .await
    .expect("the timeline must show the event");
}

async fn wait_for_message<S: Stream + Unpin>(
    timeline: &Timeline,
    updates: &mut S,
    event_id: &EventId,
) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while timeline
            .item_by_event_id(event_id)
            .await
            .is_none_or(|item| item.content().as_message().is_none())
        {
            updates.next().await.expect("live timeline stream");
        }
    })
    .await
    .expect("the timeline must show the decrypted message");
}

async fn is_decrypted(room: &Room, event_id: &EventId) -> bool {
    let (cache, _handles) = room.event_cache().await.unwrap();
    matches!(
        cache.find_event_strict(event_id).await.unwrap().unwrap().kind,
        TimelineEventKind::Decrypted(_)
    )
}

#[async_test]
async fn test_queued_refresh_does_not_delay_a_later_utd_retry() {
    let (server, client, room, _directory) = setup_sqlite().await;
    let (utd_envelope, utd_sender) = encrypted_fixture(test_room_id(), &text_message()).await;
    let utd_session = session_id(&utd_envelope);
    cache_event(&client, test_room_id(), decrypt(&room, &utd_envelope).await).await;
    let timeline = room.timeline().await.unwrap();
    let (_, updates) = timeline.subscribe().await;
    pin_mut!(updates);
    barrier(&client).await;

    // A large refresh backlog: 24 sessions with 8 persisted events each.
    let mut sessions = BTreeSet::new();
    for session_index in 0..24 {
        let (envelope, sender) = encrypted_fixture(test_room_id(), &text_message()).await;
        import_keys_silently(&client, &sender).await;
        let decrypted = decrypt(&room, &envelope).await;
        assert!(matches!(decrypted.kind, TimelineEventKind::Decrypted(_)));
        sessions.insert(session_id(&envelope));
        let store = store(&client).await;
        for event_index in 0..8 {
            let id = format!("$refresh-{session_index}-{event_index}:example.org");
            store.save_event(test_room_id(), with_event_id(&decrypted, &id)).await.unwrap();
        }
    }

    let before = client.event_cache().redecryption_stats_for_testing();
    let (refresh_reached, refresh_resume) =
        client.event_cache().pause_next_refresh_for_testing().await;
    let request = DecryptionRetryRequest {
        room_id: test_room_id().to_owned(),
        utd_session_ids: Default::default(),
        refresh_info_session_ids: sessions.clone(),
    };
    client.event_cache().request_decryption(request.clone());
    tokio::time::timeout(Duration::from_secs(5), refresh_reached).await.unwrap().unwrap();
    // Duplicate requests coalesce instead of queuing more copies of the backlog.
    for _ in 0..24 {
        client.event_cache().request_decryption(request.clone());
    }

    let other_room_id = room_id!("!other:example.org");
    server
        .mock_sync()
        .ok_and_run(&client, |builder| {
            builder.add_joined_room(
                JoinedRoomBuilder::new(other_room_id)
                    .add_state_event(create_event(other_room_id, "$other-create:example.org")),
            );
        })
        .await;
    let other_room = client.get_room(other_room_id).unwrap();
    let (mut other_envelope, other_sender) =
        encrypted_fixture(other_room_id, &text_message()).await;
    // Event IDs are globally unique in the persisted cache, across rooms too.
    // Reusing the first room's fixture ID would overwrite its stored UTD.
    let other_id = event_id!("$other-utd:example.org");
    other_envelope["event_id"] = json!(other_id);
    let other_session = session_id(&other_envelope);
    cache_event(&client, other_room_id, decrypt(&other_room, &other_envelope).await).await;
    let other_timeline = other_room.timeline().await.unwrap();
    {
        let store = store(&client).await;
        let utds = store
            .get_room_events(test_room_id(), Some("m.room.encrypted"), Some(&utd_session))
            .await
            .unwrap();
        assert_eq!(utds.len(), 1);
        assert!(store.find_event(other_room_id, other_id).await.unwrap().is_some());
    }

    import_keys_silently(&client, &other_sender).await;
    other_timeline.retry_decryption([other_session]).await;
    import_keys_silently(&client, &utd_sender).await;
    let (utd_reached, utd_resume) =
        client.event_cache().pause_next_redecryption_for_testing().await;
    timeline.retry_decryption([utd_session]).await;
    refresh_resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(30), utd_reached).await.unwrap().unwrap();

    let at_utd = client.event_cache().redecryption_stats_for_testing();
    assert!(
        at_utd.refresh_queries - before.refresh_queries < sessions.len(),
        "the UTD waited for the refresh backlog: {at_utd:?}"
    );
    utd_resume.send(()).unwrap();
    barrier(&client).await;

    assert!(
        is_decrypted(&other_room, other_id).await,
        "a second room must not wait behind the first room's refresh"
    );
    wait_for_message(&timeline, &mut updates, utd_id()).await;

    let after = client.event_cache().redecryption_stats_for_testing();
    assert!(after.refresh_queries - before.refresh_queries <= sessions.len() + 1, "{after:?}");
    assert!(after.info_queries - before.info_queries <= sessions.len() + 1, "{after:?}");
}

#[async_test]
async fn test_refresh_of_a_utd_session_is_kept_and_new_refresh_causes_are_not_lost() {
    let (_server, client, room, _directory) = setup_sqlite().await;
    let (envelope, sender) = encrypted_fixture(test_room_id(), &text_message()).await;
    let session = session_id(&envelope);
    cache_event(&client, test_room_id(), decrypt(&room, &envelope).await).await;
    let timeline = room.timeline().await.unwrap();
    barrier(&client).await;
    import_keys_silently(&client, &sender).await;

    // A persisted decrypted event of the same session, with stale encryption info.
    let mut stale = with_event_id(&decrypt(&room, &envelope).await, "$stale-info:example.org");
    let TimelineEventKind::Decrypted(decrypted) = &mut stale.kind else { unreachable!() };
    let expected_info = decrypted.encryption_info.clone();
    Arc::make_mut(&mut decrypted.encryption_info).sender_device = Some("STALE".into());
    store(&client).await.save_event(test_room_id(), stale.clone()).await.unwrap();

    let (reached, resume) = client.event_cache().pause_next_refresh_for_testing().await;
    client.event_cache().request_decryption(DecryptionRetryRequest {
        room_id: test_room_id().to_owned(),
        utd_session_ids: [session.clone()].into(),
        refresh_info_session_ids: [session.clone()].into(),
    });
    tokio::time::timeout(Duration::from_secs(5), reached).await.unwrap().unwrap();
    assert!(
        is_decrypted(&room, utd_id()).await,
        "UTD must be applied before refresh of the same session"
    );

    // A new refresh cause for the same session arrives while refresh is paused.
    let TimelineEventKind::Decrypted(decrypted) = &mut stale.kind else { unreachable!() };
    Arc::make_mut(&mut decrypted.encryption_info).sender_device = Some("LATER".into());
    store(&client).await.save_event(test_room_id(), stale).await.unwrap();
    client.event_cache().request_decryption(DecryptionRetryRequest {
        room_id: test_room_id().to_owned(),
        utd_session_ids: Default::default(),
        refresh_info_session_ids: [session].into(),
    });
    resume.send(()).unwrap();
    barrier(&client).await;

    let (cache, _handles) = room.event_cache().await.unwrap();
    let refreshed =
        cache.find_event_strict(event_id!("$stale-info:example.org")).await.unwrap().unwrap();
    let TimelineEventKind::Decrypted(decrypted) = refreshed.kind else { unreachable!() };
    assert_eq!(decrypted.encryption_info, expected_info);
    assert!(timeline.item_by_event_id(utd_id()).await.is_some());
}

#[async_test]
async fn test_cached_history_refreshes_only_loaded_events_and_recovers_oldest_utd_first() {
    const CHUNKS: usize = 24;
    let (server, client, room, directory) = setup_sqlite().await;
    let (utd_envelope, utd_sender) = encrypted_fixture(test_room_id(), &text_message()).await;
    let utd = decrypt(&room, &utd_envelope).await;

    // Persist 24 chunks of decrypted history, each with its own session. The
    // oldest event of the oldest chunk is a UTD.
    let mut updates = vec![Update::Clear];
    for chunk_index in 0..CHUNKS {
        let (envelope, sender) = encrypted_fixture(test_room_id(), &text_message()).await;
        import_keys_silently(&client, &sender).await;
        let template = decrypt(&room, &envelope).await;
        let mut events: Vec<_> = (0..128)
            .map(|event_index| {
                let id = format!("$history-{chunk_index:02}-{event_index:03}:example.org");
                with_event_id(&template, &id)
            })
            .collect();
        if chunk_index == 0 {
            events[0] = utd.clone();
        }
        let identifier = ChunkIdentifier::new(chunk_index as u64);
        updates.push(Update::NewItemsChunk {
            previous: chunk_index.checked_sub(1).map(|index| ChunkIdentifier::new(index as u64)),
            new: identifier,
            next: None,
        });
        updates.push(Update::PushItems { at: Position::new(identifier, 0), items: events });
    }
    store(&client)
        .await
        .handle_linked_chunk_updates(LinkedChunkId::Room(test_room_id()), updates)
        .await
        .unwrap();

    // Reopen, so the history is only on disk and gets loaded by pagination.
    drop(room);
    drop(client);
    let client = sqlite_client(&server, directory.path()).await;
    let room = client.get_room(test_room_id()).unwrap();
    let (refresh_reached, refresh_resume) =
        client.event_cache().pause_next_refresh_for_testing().await;
    let timeline = room.timeline().await.unwrap();
    let (_, updates) = timeline.subscribe().await;
    pin_mut!(updates);
    tokio::time::timeout(Duration::from_secs(5), refresh_reached).await.unwrap().unwrap();
    while !timeline.paginate_backwards(128).await.unwrap() {}
    wait_for_item(&timeline, &mut updates, utd_id()).await;
    assert!(timeline.item_by_event_id(utd_id()).await.unwrap().content().is_unable_to_decrypt());

    import_keys_silently(&client, &utd_sender).await;
    let (utd_reached, utd_resume) =
        client.event_cache().pause_next_redecryption_for_testing().await;
    refresh_resume.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(30), utd_reached).await.unwrap().unwrap();
    let at_utd = client.event_cache().redecryption_stats_for_testing();
    assert!(at_utd.info_queries < CHUNKS, "the oldest UTD waited for history refresh: {at_utd:?}");
    utd_resume.send(()).unwrap();
    barrier(&client).await;
    wait_for_message(&timeline, &mut updates, utd_id()).await;

    let stats = client.event_cache().redecryption_stats_for_testing();
    assert_eq!(stats.refresh_queries, 0, "opening and pagination must only refresh loaded events");
    assert!(
        stats.info_queries <= 5 * CHUNKS,
        "refresh must grow with newly loaded events: {stats:?}"
    );
    assert!(!server.server().received_requests().await.unwrap().iter().any(|request| {
        request.url.path().contains("/context/") || request.url.path().contains("/messages")
    }));
}

#[async_test]
async fn test_explicit_retry_uses_only_supplied_sessions() {
    let (_server, client, room, _directory) = setup_sqlite().await;
    let (first, first_sender) = encrypted_fixture(test_room_id(), &text_message()).await;
    let (mut second, second_sender) = encrypted_fixture(test_room_id(), &text_message()).await;
    let second_id = event_id!("$second:example.org");
    second["event_id"] = json!(second_id);
    let first_session = session_id(&first);
    let second_session = session_id(&second);
    assert_ne!(first_session, second_session);
    cache_event(&client, test_room_id(), decrypt(&room, &first).await).await;
    cache_event(&client, test_room_id(), decrypt(&room, &second).await).await;
    let timeline = room.timeline().await.unwrap();
    barrier(&client).await;
    import_keys_silently(&client, &first_sender).await;
    import_keys_silently(&client, &second_sender).await;

    timeline.retry_decryption([first_session]).await;
    barrier(&client).await;
    assert!(is_decrypted(&room, utd_id()).await);
    assert!(!is_decrypted(&room, second_id).await);

    timeline.retry_decryption([second_session]).await;
    barrier(&client).await;
    assert!(is_decrypted(&room, second_id).await);
}

#[async_test]
async fn test_queued_decryption_preserves_redaction_and_newer_content() {
    for redacted in [true, false] {
        let (_server, client, room, _directory) = setup_sqlite().await;
        let (envelope, sender) = encrypted_fixture(test_room_id(), &text_message()).await;
        let session = session_id(&envelope);
        cache_event(&client, test_room_id(), decrypt(&room, &envelope).await).await;
        let timeline = room.timeline().await.unwrap();
        barrier(&client).await;
        import_keys_silently(&client, &sender).await;
        let reports = client.event_cache().subscribe_to_decryption_reports();
        pin_mut!(reports);

        // Pause after decryption, before the result is applied, and change the
        // cached event in the meantime.
        let (reached, resume) = client.event_cache().pause_next_redecryption_for_testing().await;
        timeline.retry_decryption([session]).await;
        tokio::time::timeout(Duration::from_secs(5), reached).await.unwrap().unwrap();
        let (cache, _handles) = room.event_cache().await.unwrap();
        if redacted {
            let mut redaction = event("m.room.redaction", json!({"redacts": UTD_ID}));
            redaction["event_id"] = json!("$redaction:example.org");
            redaction["origin_server_ts"] = json!(2000);
            cache_event(&client, test_room_id(), TimelineEvent::from_plaintext(raw(&redaction)))
                .await;
            let current = cache.find_event_strict(utd_id()).await.unwrap().unwrap();
            assert!(current.raw().deserialize().is_ok_and(|event| event.is_redacted()));
        } else {
            let mut newer = envelope.clone();
            newer["unsigned"]["newer_copy"] = json!(true);
            let decrypted = decrypt(&room, &newer).await;
            assert!(matches!(decrypted.kind, TimelineEventKind::Decrypted(_)));
            cache.replace_event_for_testing(utd_id(), decrypted).await;
        }
        resume.send(()).unwrap();
        barrier(&client).await;

        if redacted {
            assert!(
                reports.next().now_or_never().is_none(),
                "a skipped redaction must not be reported as resolved"
            );
        }
        let current = cache.find_event_strict(utd_id()).await.unwrap().unwrap();
        let persisted =
            store(&client).await.find_event(test_room_id(), utd_id()).await.unwrap().unwrap();
        for event in [current, persisted] {
            if redacted {
                assert!(event.raw().deserialize().is_ok_and(|event| event.is_redacted()));
            } else {
                assert!(matches!(event.kind, TimelineEventKind::Decrypted(_)));
                let value: Value = serde_json::from_str(event.raw().json().get()).unwrap();
                assert_eq!(value["unsigned"]["newer_copy"], json!(true));
            }
        }
    }
}
