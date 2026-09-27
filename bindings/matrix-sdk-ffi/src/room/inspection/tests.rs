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

use std::{sync::Arc, time::Duration};

use futures_util::StreamExt;
use matrix_sdk::{config::RequestConfig, test_utils::mocks::MatrixMockServer};
use matrix_sdk_base::{
    crypto::{DecryptionSettings, EncryptionSettings, OlmMachine, TrustRequirement},
    event_cache::store::{EventCacheStore, MemoryStore},
    store::StoreConfig,
};
use matrix_sdk_common::cross_process_lock::CrossProcessLockConfig;
use matrix_sdk_test::JoinedRoomBuilder;
use matrix_sdk_ui::timeline::RoomExt;
use ruma::{RoomVersionId, device_id, event_id, room_id, user_id};
use serde_json::{Value, json};
use tokio::sync::Notify;
use wiremock::{
    Mock, Request, ResponseTemplate,
    matchers::{method, path_regex},
};

use super::*;
use RoomTimelineEventDisposition::{Hidden, Indeterminate, UnableToDecrypt, Visible};

const ID: &str = "$inspection:example.org";
fn inspection_room_id() -> &'static RoomId {
    room_id!("!inspection:example.org")
}
const TARGET: &str = "$missing-target:example.org";

fn event(event_type: &str, content: Value) -> Value {
    json!({
        "event_id": ID, "room_id": inspection_room_id(), "sender": "@alice:example.org",
        "origin_server_ts": 1000, "type": event_type, "content": content,
        "unsigned": {"age": 42, "custom_metadata": {"keep": true}},
    })
}

fn raw(event: &Value) -> Raw<AnySyncTimelineEvent> {
    Raw::new(event).unwrap().cast_unchecked()
}

fn poll_content() -> Value {
    json!({
        "org.matrix.msc3381.poll.start": {
            "question": {"org.matrix.msc1767.text": "Lunch?"},
            "answers": [{"id": "a", "org.matrix.msc1767.text": "Soup"}],
            "kind": "org.matrix.msc3381.poll.disclosed", "max_selections": 1
        },
        "org.matrix.msc1767.text": "Lunch?\n1. Soup"
    })
}

fn text_message() -> Value {
    event("m.room.message", json!({"msgtype": "m.text", "body": "Hello"}))
}

fn message_edit() -> Value {
    let mut edit = text_message();
    edit["content"]["m.new_content"] = edit["content"].clone();
    edit["content"]["m.relates_to"] = json!({"rel_type": "m.replace", "event_id": TARGET});
    edit
}

fn reaction() -> Value {
    event(
        "m.reaction",
        json!({"m.relates_to": {
            "rel_type": "m.annotation", "event_id": TARGET, "key": "👍"
        }}),
    )
}

fn poll_response() -> Value {
    event(
        "org.matrix.msc3381.poll.response",
        json!({
            "org.matrix.msc3381.poll.response": {"answers": ["a"]},
            "m.relates_to": {"rel_type": "m.reference", "event_id": TARGET}
        }),
    )
}

fn poll_end() -> Value {
    event(
        "org.matrix.msc3381.poll.end",
        json!({
            "org.matrix.msc3381.poll.end": {}, "org.matrix.msc1767.text": "Ended",
            "m.relates_to": {"rel_type": "m.reference", "event_id": TARGET}
        }),
    )
}

fn encrypted_state() -> Value {
    let mut state = event(
        "m.room.encrypted",
        json!({
            "algorithm": "m.megolm.v1.aes-sha2",
            "ciphertext": "ciphertext", "session_id": "session_id"
        }),
    );
    state["state_key"] = json!("");
    state
}

fn cases() -> Vec<(Value, RoomTimelineEventDisposition)> {
    let mut reply = text_message()["content"].clone();
    reply["m.relates_to"] = json!({"m.in_reply_to": {"event_id": TARGET}});
    let mut thread = reply.clone();
    thread["m.relates_to"]["rel_type"] = json!("m.thread");
    thread["m.relates_to"]["event_id"] = json!(TARGET);
    let poll_edit = json!({
        "m.new_content": poll_content(),
        "m.relates_to": {"rel_type": "m.replace", "event_id": TARGET}
    });
    let reference = json!({"rel_type": "m.reference", "event_id": TARGET});
    let mut cases = vec![
        (text_message(), Visible),
        (event("m.room.message", reply), Visible),
        (event("m.room.message", thread), Visible),
        (event("org.matrix.msc3381.poll.start", poll_content()), Visible),
        (message_edit(), Hidden),
        (event("org.matrix.msc3381.poll.start", poll_edit), Hidden),
        (reaction(), Hidden),
        (poll_response(), Hidden),
        (poll_end(), Hidden),
        (
            event("m.call.candidates", json!({"call_id": "call", "version": 0, "candidates": []})),
            Hidden,
        ),
        (event("m.room.redaction", json!({"redacts": TARGET})), Hidden),
        (event("org.example.custom", json!({"m.relates_to": reference})), Indeterminate),
        (
            event("m.room.message", json!({"msgtype": "org.example.custom", "body": "Keep"})),
            Indeterminate,
        ),
        (event("m.room.message", json!({"msgtype": "m.text"})), Indeterminate),
        (event("m.reaction", json!({})), Indeterminate),
        (event("org.matrix.msc3381.poll.response", json!({})), Indeterminate),
        (event("m.room.encrypted", json!({"algorithm": "unknown"})), Indeterminate),
        (encrypted_state(), Indeterminate),
    ];
    for msgtype in ["m.image", "m.audio", "m.video", "m.file"] {
        cases.push((
            event(
                "m.room.message",
                json!({
                    "msgtype": msgtype, "body": "Attachment", "url": "mxc://example.org/file"
                }),
            ),
            Visible,
        ));
    }
    let mut state = event("org.example.state", json!({}));
    state["state_key"] = json!("");
    cases.push((state, Indeterminate));
    // A custom msgtype is indeterminate even when it has a recognized edit relation.
    let mut custom_edit = event(
        "m.room.message",
        json!({
            "msgtype": "org.example.custom", "body": "Keep", "m.new_content": {
                "msgtype": "org.example.custom", "body": "Keep"
            }, "m.relates_to": {"rel_type": "m.replace", "event_id": TARGET}
        }),
    );
    cases.push((custom_edit.clone(), Indeterminate));
    custom_edit["content"]["m.relates_to"]["event_id"] = json!("invalid");
    cases.push((custom_edit, Indeterminate));
    cases
}

#[test]
fn inspection_classifies_recognized_events_conservatively() {
    let rules = RoomVersionId::V11.rules().unwrap();
    for (event, expected) in cases() {
        assert_eq!(classify_event(&raw(&event), Some(&rules)), expected, "{event}");
    }
    assert_eq!(classify_event(&raw(&event("m.reaction", json!({}))), None), Indeterminate);
    assert_eq!(classify_event(&raw(&text_message()), None), Indeterminate);
}

#[test]
fn inspection_redactions_follow_room_version_rules() {
    for version in [RoomVersionId::V6, RoomVersionId::V11] {
        let rules = version.rules().unwrap();
        for event_type in [
            "m.room.message",
            "m.reaction",
            "org.matrix.msc3381.poll.response",
            "org.matrix.msc3381.poll.end",
            "org.matrix.msc3381.poll.start",
            "m.room.encrypted",
            "org.example.custom",
        ] {
            let mut redacted = event(event_type, json!({}));
            redacted["unsigned"]["redacted_because"] = event("m.room.redaction", json!({}));
            let expected = match event_type {
                "m.reaction" => Hidden,
                "org.example.custom" => Indeterminate,
                _ => Visible,
            };
            assert_eq!(
                classify_event(&raw(&redacted), Some(&rules)),
                expected,
                "{version}: {event_type}"
            );
        }
        let mut redaction = event("m.room.redaction", json!({}));
        redaction["redacts"] = json!(TARGET);
        // Ruma accepts the legacy field as a compatibility fallback even in v11.
        assert_eq!(classify_event(&raw(&redaction), Some(&rules)), Hidden);
        redaction.as_object_mut().unwrap().remove("redacts");
        redaction["content"]["redacts"] = json!(TARGET);
        assert_eq!(classify_event(&raw(&redaction), Some(&rules)), Hidden);

        // Redacting a redaction retains its target in v11, but removes it in v6.
        use ruma::events::{RedactContent, room::redaction::RoomRedactionEventContent};
        let content: RoomRedactionEventContent =
            serde_json::from_value(redaction["content"].clone()).unwrap();
        redaction["content"] = serde_json::to_value(content.redact(&rules.redaction)).unwrap();
        redaction["unsigned"]["redacted_because"] = event("m.room.redaction", json!({}));
        assert_eq!(
            classify_event(&raw(&redaction), Some(&rules)),
            if version == RoomVersionId::V11 { Hidden } else { Visible }
        );
    }
}

async fn setup(trust: TrustRequirement) -> (MatrixMockServer, Room, MemoryStore) {
    let server = MatrixMockServer::new().await;
    let store = MemoryStore::new();
    let config =
        StoreConfig::new(CrossProcessLockConfig::SingleProcess).event_cache_store(store.clone());
    let client = server
        .client_builder()
        .on_builder(|b| {
            b.store_config(config).with_decryption_settings(DecryptionSettings {
                sender_device_trust_requirement: trust,
            })
        })
        .build()
        .await;
    client.event_cache().subscribe().unwrap();
    let mut create =
        event("m.room.create", json!({"creator": "@alice:example.org", "room_version": "11"}));
    create["state_key"] = json!("");
    create["event_id"] = json!("$create:example.org");
    server
        .mock_sync()
        .ok_and_run(&client, |s| {
            s.add_joined_room(
                JoinedRoomBuilder::new(inspection_room_id())
                    .add_state_event(raw(&create).cast_unchecked()),
            );
        })
        .await;
    server.mock_room_state_encryption().plain().mount().await;
    Mock::given(method("GET"))
        .and(path_regex("/room_keys/version$"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"errcode": "M_NOT_FOUND", "error": "No backup"})),
        )
        .mount(server.server())
        .await;
    let room = Room::new(client.get_room(inspection_room_id()).unwrap(), None);
    (server, room, store)
}

async fn mock_event(server: &MatrixMockServer, value: Value) -> wiremock::MockGuard {
    Mock::given(method("GET"))
        .and(path_regex(r"/rooms/[^/]+/event/\$inspection:example.org$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(value))
        .expect(1)
        .mount_as_scoped(server.server())
        .await
}

#[tokio::test]
async fn inspection_fetches_complete_events_without_writing_cache_or_fetching_targets() {
    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    for (expected_event, disposition) in cases() {
        let guard = mock_event(&server, expected_event.clone()).await;
        let result = room.inspect_timeline_event(ID.into()).await.unwrap();
        assert_eq!(result.disposition, disposition, "{expected_event}");
        assert!(result.decryption_failure.is_none());
        assert_eq!(result.event.event_id.as_deref(), Some(ID));
        assert_eq!(result.event.room_id, inspection_room_id().as_str());
        assert_eq!(serde_json::from_str::<Value>(&result.event.raw_json).unwrap(), expected_event);
        assert_eq!(
            serde_json::from_str::<Value>(&result.event.content_json).unwrap(),
            expected_event["content"]
        );
        assert!(
            store
                .find_event(inspection_room_id(), event_id!("$inspection:example.org"))
                .await
                .unwrap()
                .is_none()
        );
        drop(guard);
    }
    let (cache, _) = room.inner.event_cache().await.unwrap();
    assert!(cache.find_event(event_id!("$missing-target:example.org")).await.unwrap().is_none());
    assert_eq!(cache.event_focused_cache_count_for_testing().await.unwrap(), 0);
    for request in server.server().received_requests().await.unwrap() {
        assert!(!request.url.path().contains("/context/"));
        assert!(!request.url.path().contains("/messages"));
        if request.url.path().contains("/event/") {
            assert!(request.url.path().ends_with(ID));
        }
        for forbidden in ["/send/", "/redact/", "/receipt/", "/typing/"] {
            assert!(!request.url.path().contains(forbidden), "{}", request.url);
        }
    }
}

#[tokio::test]
async fn inspection_reads_cached_event_and_leaves_live_timeline_unchanged() {
    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    let original = reaction();
    store
        .save_event(inspection_room_id(), TimelineEvent::from_plaintext(raw(&original)))
        .await
        .unwrap();
    let timeline = room.inner.timeline().await.unwrap();
    let (items_before, mut changes) = timeline.subscribe().await;
    let (cache, _) = room.inner.event_cache().await.unwrap();
    let focused_count = cache.event_focused_cache_count_for_testing().await.unwrap();
    let requests_before = server.server().received_requests().await.unwrap().len();
    let result = room.inspect_timeline_event(ID.into()).await.unwrap();
    assert_eq!(result.disposition, Hidden);
    assert_eq!(server.server().received_requests().await.unwrap().len(), requests_before);
    assert_eq!(cache.event_focused_cache_count_for_testing().await.unwrap(), focused_count);
    assert!(tokio::time::timeout(Duration::from_millis(30), changes.next()).await.is_err());
    let (items_after, _) = timeline.subscribe().await;
    assert_eq!(items_before.len(), items_after.len());
    assert_eq!(
        store
            .find_event(inspection_room_id(), event_id!("$inspection:example.org"))
            .await
            .unwrap()
            .unwrap()
            .raw()
            .json()
            .get(),
        raw(&original).json().get()
    );
}

#[tokio::test]
async fn inspection_rejects_wrong_id_room_and_unusable_envelopes() {
    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    let before = server.server().received_requests().await.unwrap().len();
    assert!(room.inspect_timeline_event("invalid".into()).await.is_err());
    assert_eq!(server.server().received_requests().await.unwrap().len(), before);
    for (field, value) in [
        ("event_id", json!(TARGET)),
        ("event_id", Value::Null),
        ("room_id", json!("!other:example.org")),
        ("type", Value::Null),
    ] {
        let mut wrong = text_message();
        wrong[field] = value;
        let guard = mock_event(&server, wrong).await;
        assert!(room.inspect_timeline_event(ID.into()).await.is_err(), "{field}");
        assert!(
            store
                .find_event(inspection_room_id(), event_id!("$inspection:example.org"))
                .await
                .unwrap()
                .is_none()
        );
        drop(guard);
    }
}

#[tokio::test]
async fn inspection_propagates_lookup_errors() {
    let (server, room, _) = setup(TrustRequirement::Untrusted).await;
    for (status, code) in [(404, "M_NOT_FOUND"), (403, "M_FORBIDDEN"), (500, "M_UNKNOWN")] {
        let guard = Mock::given(method("GET"))
            .and(path_regex("/event/"))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_json(json!({"errcode": code, "error": "Unavailable"})),
            )
            .expect(1)
            .mount_as_scoped(server.server())
            .await;
        let error = room.inspect_timeline_event(ID.into()).await.err().expect("lookup must fail");
        assert!(matches!(error, ClientError::MatrixApi { code: actual, .. } if actual == code));
        drop(guard);
    }
}

#[tokio::test]
async fn inspection_cancellation_stops_the_call_without_writing_cache() {
    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    let started = Arc::new(Notify::new());
    let notify = started.clone();
    Mock::given(method("GET"))
        .and(path_regex("/event/"))
        .respond_with(move |_: &Request| {
            notify.notify_one();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_secs(30))
                .set_body_json(text_message())
        })
        .expect(1)
        .mount(server.server())
        .await;
    let task = tokio::spawn(async move { room.inspect_timeline_event(ID.into()).await });
    tokio::time::timeout(Duration::from_secs(2), started.notified()).await.unwrap();
    task.abort();
    assert!(task.await.err().unwrap().is_cancelled());
    assert!(
        store
            .find_event(inspection_room_id(), event_id!("$inspection:example.org"))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn inspection_network_timeout_is_an_error() {
    let server = MatrixMockServer::new().await;
    let client = server
        .client_builder()
        .on_builder(|b| {
            b.request_config(
                RequestConfig::new().disable_retry().timeout(Duration::from_millis(50)),
            )
        })
        .build()
        .await;
    let room = Room::new(server.sync_joined_room(&client, inspection_room_id()).await, None);
    Mock::given(method("GET"))
        .and(path_regex("/event/"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(30)))
        .expect(1)
        .mount(server.server())
        .await;
    assert!(room.inspect_timeline_event(ID.into()).await.is_err());
}

async fn encrypted_fixture(plaintext: &Value) -> (Value, OlmMachine) {
    let sender = OlmMachine::new(user_id!("@alice:example.org"), device_id!("ALICE")).await;
    sender
        .share_room_key(inspection_room_id(), std::iter::empty(), EncryptionSettings::default())
        .await
        .unwrap();
    let encrypted = sender
        .encrypt_room_event_raw(
            inspection_room_id(),
            plaintext["type"].as_str().unwrap(),
            &Raw::new(&plaintext["content"]).unwrap().cast_unchecked(),
        )
        .await
        .unwrap();
    let mut envelope = event("m.room.encrypted", serde_json::to_value(encrypted.content).unwrap());
    envelope["unsigned"] = plaintext["unsigned"].clone();
    (envelope, sender)
}

async fn import_keys(room: &Room, sender: &OlmMachine) {
    let keys = sender.store().export_room_keys(|_| true).await.unwrap();
    room.inner
        .client()
        .olm_machine_for_testing()
        .await
        .as_ref()
        .unwrap()
        .store()
        .import_exported_room_keys(keys, |_, _| {})
        .await
        .unwrap();
}

#[tokio::test]
async fn inspection_redecrypts_stale_cached_utds_after_importing_keys_without_writing_them() {
    for (plaintext, disposition) in [
        (text_message(), Visible),
        (message_edit(), Hidden),
        (reaction(), Hidden),
        (poll_response(), Hidden),
        (poll_end(), Hidden),
    ] {
        let (server, room, store) = setup(TrustRequirement::Untrusted).await;
        let (envelope, sender) = encrypted_fixture(&plaintext).await;
        let utd = room
            .inner
            .decrypt_event(
                raw(&envelope).cast_ref_unchecked::<OriginalSyncRoomEncryptedEvent>(),
                None,
            )
            .await
            .unwrap();
        assert!(utd.kind.is_utd());
        store.save_event(inspection_room_id(), utd.clone()).await.unwrap();
        let first = room.inspect_timeline_event(ID.into()).await.unwrap();
        assert_eq!(first.disposition, UnableToDecrypt);
        assert!(matches!(first.decryption_failure, Some(EncryptedMessage::MegolmV1AesSha2 { .. })));
        import_keys(&room, &sender).await;
        // Model a stale on-disk UTD explicitly. A background cache redecryptor
        // may also react to key imports; inspection must not depend on it.
        store.save_event(inspection_room_id(), utd).await.unwrap();
        let result = room.inspect_timeline_event(ID.into()).await.unwrap();
        assert_eq!(result.disposition, disposition);
        assert!(result.decryption_failure.is_none());
        assert_eq!(result.event.event_id.as_deref(), Some(ID));
        assert_eq!(result.event.event_type, plaintext["type"]);
        let decrypted: Value = serde_json::from_str(&result.event.raw_json).unwrap();
        assert_eq!(decrypted["content"], plaintext["content"]);
        assert_eq!(decrypted["unsigned"], plaintext["unsigned"]);
        assert!(result.event.encryption_info.is_some());
        // No fetching of the original event, relation target, or timeline context.
        assert!(
            !server.server().received_requests().await.unwrap().iter().any(|r| r
                .url
                .path()
                .contains("/event/")
                || r.url.path().contains("/context/"))
        );
    }
}

#[tokio::test]
async fn inspection_preserves_trust_failures_and_unknown_utd_causes() {
    let (_server, room, store) = setup(TrustRequirement::CrossSigned).await;
    let (envelope, sender) = encrypted_fixture(&reaction()).await;
    import_keys(&room, &sender).await;
    store
        .save_event(inspection_room_id(), TimelineEvent::from_plaintext(raw(&envelope)))
        .await
        .unwrap();
    let result = room.inspect_timeline_event(ID.into()).await.unwrap();
    assert_eq!(result.disposition, UnableToDecrypt);
    assert!(matches!(
        result.decryption_failure,
        Some(EncryptedMessage::MegolmV1AesSha2 { cause: UtdCause::UnknownDevice, .. })
    ));

    let (_server, room, store) = setup(TrustRequirement::Untrusted).await;
    let mut corrupted = envelope;
    corrupted["content"]["ciphertext"] = json!("AA");
    store
        .save_event(inspection_room_id(), TimelineEvent::from_plaintext(raw(&corrupted)))
        .await
        .unwrap();
    let result = room.inspect_timeline_event(ID.into()).await.unwrap();
    assert_eq!(result.disposition, UnableToDecrypt);
    assert!(matches!(
        result.decryption_failure,
        Some(EncryptedMessage::MegolmV1AesSha2 { cause: UtdCause::Unknown, .. })
    ));
}

#[tokio::test]
async fn inspection_of_fetched_encrypted_events_never_populates_event_cache() {
    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    let (envelope, sender) = encrypted_fixture(&message_edit()).await;
    for expected in [UnableToDecrypt, Hidden] {
        let guard = mock_event(&server, envelope.clone()).await;
        assert_eq!(room.inspect_timeline_event(ID.into()).await.unwrap().disposition, expected);
        assert!(
            store
                .find_event(inspection_room_id(), event_id!("$inspection:example.org"))
                .await
                .unwrap()
                .is_none()
        );
        drop(guard);
        if expected == UnableToDecrypt {
            import_keys(&room, &sender).await;
        }
    }
}

#[tokio::test]
async fn inspection_preserves_a_cached_decrypted_event_without_local_keys() {
    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    let (envelope, sender) = encrypted_fixture(&message_edit()).await;
    let decrypted = sender
        .decrypt_room_event(
            raw(&envelope).cast_ref_unchecked(),
            inspection_room_id(),
            &DecryptionSettings { sender_device_trust_requirement: TrustRequirement::Untrusted },
        )
        .await
        .unwrap();
    let expected = TimelineEvent::from_decrypted(decrypted, None);
    store.save_event(inspection_room_id(), expected.clone()).await.unwrap();
    let before = server.server().received_requests().await.unwrap().len();
    let result = room.inspect_timeline_event(ID.into()).await.unwrap();
    assert_eq!(result.disposition, Hidden);
    assert!(result.decryption_failure.is_none());
    assert_eq!(result.event.raw_json, expected.raw().json().get());
    assert_eq!(server.server().received_requests().await.unwrap().len(), before);
    assert!(matches!(
        store
            .find_event(inspection_room_id(), event_id!("$inspection:example.org"))
            .await
            .unwrap()
            .unwrap()
            .kind,
        TimelineEventKind::Decrypted(_)
    ));
}

#[tokio::test]
async fn inspection_fetch_cannot_overwrite_a_concurrently_decrypted_cache_entry() {
    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    let (envelope, sender) = encrypted_fixture(&text_message()).await;
    let decrypted = sender
        .decrypt_room_event(
            raw(&envelope).cast_ref_unchecked(),
            inspection_room_id(),
            &DecryptionSettings { sender_device_trust_requirement: TrustRequirement::Untrusted },
        )
        .await
        .unwrap();
    let started = Arc::new(Notify::new());
    let notify = started.clone();
    Mock::given(method("GET"))
        .and(path_regex("/event/"))
        .respond_with(move |_: &Request| {
            notify.notify_one();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(100))
                .set_body_json(&envelope)
        })
        .expect(1)
        .mount(server.server())
        .await;
    let task = tokio::spawn(async move { room.inspect_timeline_event(ID.into()).await });
    tokio::time::timeout(Duration::from_secs(2), started.notified()).await.unwrap();
    let expected = TimelineEvent::from_decrypted(decrypted, None);
    store.save_event(inspection_room_id(), expected.clone()).await.unwrap();
    assert_eq!(task.await.unwrap().unwrap().disposition, UnableToDecrypt);
    let retained = store
        .find_event(inspection_room_id(), event_id!("$inspection:example.org"))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(retained.kind, TimelineEventKind::Decrypted(_)));
    assert_eq!(retained.raw().json().get(), expected.raw().json().get());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn inspection_propagates_cache_storage_failures_without_fetching() {
    let server = MatrixMockServer::new().await;
    let directory = tempfile::tempdir().unwrap();
    let client = server
        .client_builder()
        .on_builder(|b| {
            b.sqlite_store(directory.path(), None)
                .cross_process_store_config(CrossProcessLockConfig::SingleProcess)
        })
        .build()
        .await;
    client.event_cache().subscribe().unwrap();
    let room = Room::new(server.sync_joined_room(&client, inspection_room_id()).await, None);
    let (cache, _) = room.inner.event_cache().await.unwrap();
    assert!(cache.find_event_strict(event_id!("$inspection:example.org")).await.unwrap().is_none());
    client.event_cache_store().close().await.unwrap();
    assert!(matches!(
        cache.find_event_strict(event_id!("$inspection:example.org")).await,
        Err(EventCacheError::Storage(_))
    ));
    let before = server.server().received_requests().await.unwrap().len();
    assert!(room.inspect_timeline_event(ID.into()).await.is_err());
    assert_eq!(server.server().received_requests().await.unwrap().len(), before);

    // Existing SDK callers retain their cache-miss fallback on lookup failures.
    assert!(cache.find_event(event_id!("$inspection:example.org")).await.unwrap().is_none());
    let expected = text_message();
    let _guard = mock_event(&server, expected.clone()).await;
    let fetched =
        room.inner.load_or_fetch_event(event_id!("$inspection:example.org"), None).await.unwrap();
    assert_eq!(serde_json::from_str::<Value>(fetched.raw().json().get()).unwrap(), expected);
}

#[tokio::test]
async fn inspection_handles_cached_encrypted_state_conservatively() {
    use matrix_sdk::deserialized_responses::UnableToDecryptReason;

    let (server, room, store) = setup(TrustRequirement::Untrusted).await;
    let envelope = encrypted_state();
    let raw_state = raw(&envelope);
    let utd = TimelineEvent::from_utd(
        raw_state.clone(),
        UnableToDecryptInfo { session_id: None, reason: UnableToDecryptReason::Unknown },
    );
    for (cached, expected) in
        [(TimelineEvent::from_plaintext(raw_state.clone()), Indeterminate), (utd, UnableToDecrypt)]
    {
        store.save_event(inspection_room_id(), cached).await.unwrap();
        let before = server.server().received_requests().await.unwrap().len();
        let result = room.inspect_timeline_event(ID.into()).await.unwrap();
        assert_eq!(result.disposition, expected);
        assert_eq!(result.decryption_failure.is_some(), expected == UnableToDecrypt);
        assert_eq!(serde_json::from_str::<Value>(&result.event.raw_json).unwrap(), envelope);
        assert_eq!(server.server().received_requests().await.unwrap().len(), before);
    }

    let mut redacted = envelope;
    redacted["content"] = json!({});
    redacted["unsigned"]["redacted_because"] = event("m.room.redaction", json!({}));
    let rules = RoomVersionId::V11.rules().unwrap();
    assert_eq!(classify_event(&raw(&redacted), Some(&rules)), Indeterminate);
}
