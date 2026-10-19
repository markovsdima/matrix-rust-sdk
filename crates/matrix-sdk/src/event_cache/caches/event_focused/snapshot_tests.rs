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

use std::{future::pending, sync::Arc, time::Duration};

use futures_util::FutureExt as _;
use matrix_sdk_base::RoomState;
use matrix_sdk_test::{ALICE, async_test, event_factory::EventFactory};
use ruma::{event_id, room_id};

use super::*;
use crate::{
    Client,
    event_cache::{EventCacheDropHandles, RoomEventCache},
    test_utils::mocks::{MatrixMockServer, RoomContextResponseTemplate},
};

async fn setup()
-> (MatrixMockServer, Client, Arc<EventCacheDropHandles>, RoomEventCache, EventFocusedCache) {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let room_id = room_id!("!focused-boundary:example.org");
    client.event_cache().subscribe().unwrap();
    client.base_client().get_or_create_room(room_id, RoomState::Joined);
    let (room, handles) = client.get_room(room_id).unwrap().event_cache().await.unwrap();
    server
        .mock_room_event_context()
        .ok(RoomContextResponseTemplate::new(
            EventFactory::new()
                .room(room_id)
                .sender(*ALICE)
                .text_msg("target")
                .event_id(event_id!("$target"))
                .into_event(),
        ))
        .mock_once()
        .mount()
        .await;
    let focus = room
        .get_or_create_event_focused_cache(
            event_id!("$target").to_owned(),
            0,
            EventFocusThreadMode::Automatic,
        )
        .await
        .unwrap();
    // Start empty so the two boundary tests can isolate a single mutation.
    focus.inner.write().await.chunk = EventLinkedChunk::new();
    (server, client, handles, room, focus)
}

#[async_test]
async fn test_focused_snapshot_does_not_hold_room_lock_while_waiting_for_pagination() {
    let (_server, _client, _handles, room, focus) = setup().await;
    // Network pagination holds the focus lock while Room::messages() saves
    // results under the room lock. Reproduce that lock order without sleeps.
    let mut pagination = focus.inner.write().await;
    let mut snapshot = Box::pin(room.subscribe_to_event_with_room_updates(
        event_id!("$target").to_owned(),
        EventFocusThreadMode::Automatic,
    ));
    assert!(snapshot.as_mut().now_or_never().is_none());
    let event = EventFactory::new()
        .room(room.room_id())
        .sender(*ALICE)
        .text_msg("paginated")
        .event_id(event_id!("$target"))
        .into_event();
    tokio::time::timeout(Duration::from_secs(2), room.save_events([event.clone()]))
        .await
        .expect("snapshot must not block pagination's room save");
    pagination.chunk.push_live_events(None, &[event]);
    pagination.notify_subscribers(EventsOrigin::Pagination);
    drop(pagination);
    let snapshot =
        tokio::time::timeout(Duration::from_secs(2), snapshot).await.unwrap().unwrap().unwrap();
    assert_eq!(snapshot.events.len(), 1);
    assert_eq!(snapshot.events[0].event_id().as_deref(), Some(event_id!("$target")));
    assert!(snapshot.updates.is_empty());
    assert!(snapshot.room_updates.is_empty());
}

#[async_test]
async fn test_cancelled_focused_mutation_is_flushed_before_joint_snapshot() {
    let (_server, _client, _handles, room, focus) = setup().await;
    let (events, mut existing) = focus.subscribe().await;
    let mut old_replica: imbl::Vector<_> = events.into_iter().collect();
    let factory = EventFactory::new().room(room.room_id()).sender(*ALICE);
    let first = factory.text_msg("first").event_id(event_id!("$first")).into_event();
    let mut mutation = Box::pin(async {
        let mut inner = focus.inner.write().await;
        inner.chunk.push_live_events(None, &[first]);
        pending::<()>().await;
    });
    assert!(mutation.as_mut().now_or_never().is_none());
    drop(mutation);
    let mut snapshot = room
        .subscribe_to_event_with_room_updates(
            event_id!("$target").to_owned(),
            EventFocusThreadMode::Automatic,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.events.len(), 1);
    assert!(snapshot.updates.is_empty());
    for diff in existing.try_recv().unwrap().diffs {
        diff.apply(&mut old_replica);
    }
    assert_eq!(old_replica.len(), 1);
    let second = factory.text_msg("second").event_id(event_id!("$second")).into_event();
    {
        let mut inner = focus.inner.write().await;
        inner.chunk.push_live_events(None, &[second]);
        inner.notify_subscribers(EventsOrigin::Pagination);
    }
    let mut new_replica: imbl::Vector<_> = snapshot.events.into_iter().collect();
    for diff in existing.try_recv().unwrap().diffs {
        diff.apply(&mut old_replica);
    }
    for diff in snapshot.updates.try_recv().unwrap().diffs {
        diff.apply(&mut new_replica);
    }
    let ids = |events: imbl::Vector<Event>| {
        events.into_iter().map(|e| e.event_id().unwrap()).collect::<Vec<_>>()
    };
    let expected = vec![event_id!("$first").to_owned(), event_id!("$second").to_owned()];
    assert_eq!(ids(old_replica), expected);
    assert_eq!(ids(new_replica), expected);
    assert!(existing.is_empty());
    assert!(snapshot.updates.is_empty());
}

#[async_test]
async fn test_focused_snapshot_loads_aggregations_without_unloaded_thread_history() {
    let (_server, _client, _handles, room, focus) = setup().await;
    let factory = EventFactory::new().room(room.room_id()).sender(*ALICE);
    let target = event_id!("$target");
    let root = factory.text_msg("root").event_id(target).into_event();
    focus.inner.write().await.chunk.push_live_events(None, std::slice::from_ref(&root));
    let mut stored = vec![root];
    for i in 0..1000 {
        let reply_id = ruma::EventId::parse(format!("$reply{i}")).unwrap();
        stored.push(
            factory.text_msg("reply").event_id(&reply_id).in_thread(target, target).into_event(),
        );
    }
    stored.push(factory.reaction(target, "reaction").event_id(event_id!("$reaction")).into_event());
    stored.push(
        factory
            .text_msg("* edited")
            .event_id(event_id!("$edit"))
            .edit(
                target,
                ruma::events::room::message::RoomMessageEventContentWithoutRelation::text_plain(
                    "edited",
                ),
            )
            .into_event(),
    );
    room.save_events(stored).await;
    let snapshot = room
        .subscribe_to_event_with_room_updates(target.to_owned(), EventFocusThreadMode::Automatic)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.events.len(), 1);
    let ids: std::collections::BTreeSet<_> =
        snapshot.relations.iter().filter_map(Event::event_id).collect();
    assert_eq!(ids, [event_id!("$reaction").to_owned(), event_id!("$edit").to_owned()].into());
    assert!(snapshot.updates.is_empty());
    assert!(snapshot.room_updates.is_empty());
}

#[async_test]
async fn test_thread_snapshot_repairs_backing_root_and_notifies_existing_subscriber() {
    thread_root_repair(false).await;
}

#[async_test]
async fn test_thread_snapshot_repairs_root_redacted_by_sync() {
    thread_root_repair(true).await;
}

async fn thread_root_repair(from_sync: bool) {
    let (server, client, _handles, room, _focus) = setup().await;
    let root_id = event_id!("$thread-root");
    let root = EventFactory::new()
        .room(room.room_id())
        .sender(*ALICE)
        .text_msg("root before redaction")
        .event_id(root_id)
        .into_event();
    room.save_events([root.clone()]).await;
    {
        let mut state = room.state().write().await.unwrap();
        state
            .get_or_reload_thread(root_id.to_owned())
            .add_live_events(vec![root.clone()])
            .await
            .unwrap();
    }
    let (_, mut existing) = room.subscribe_to_thread(root_id.to_owned()).await.unwrap();
    // A thread root is not an m.thread reply: room redaction handling does
    // not remove it from the thread. Restore the authoritative stored form.
    let mut json: serde_json::Value = serde_json::from_str(root.raw().json().get()).unwrap();
    json["content"] = serde_json::json!({});
    json["unsigned"] = serde_json::json!({"redacted_because": {
        "type":"m.room.redaction", "event_id":"$redaction", "sender":ALICE.as_str(),
        "origin_server_ts":1, "content":{}, "redacts":root_id
    }});
    let mut redacted = root.clone();
    redacted
        .replace_raw(ruma::serde::Raw::from_json(serde_json::value::to_raw_value(&json).unwrap()));
    if from_sync {
        server
            .sync_room(
                &client,
                matrix_sdk_test::JoinedRoomBuilder::new(room.room_id()).add_timeline_event(
                    EventFactory::new()
                        .room(room.room_id())
                        .sender(*ALICE)
                        .redaction(root_id)
                        .event_id(event_id!("$redaction")),
                ),
            )
            .await;
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let state = room.state().read().await.unwrap();
                if state
                    .find_event(root_id)
                    .await
                    .unwrap()
                    .unwrap()
                    .1
                    .raw()
                    .deserialize()
                    .unwrap()
                    .is_redacted()
                {
                    break;
                }
                drop(state);
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    } else {
        room.save_events([redacted]).await;
    }
    let mut snapshot =
        room.subscribe_to_thread_with_room_updates(root_id.to_owned()).await.unwrap();
    assert!(snapshot.events[0].raw().deserialize().unwrap().is_redacted());
    let (backing, _) = room.subscribe_to_thread(root_id.to_owned()).await.unwrap();
    assert!(
        backing[0].raw().deserialize().unwrap().is_redacted(),
        "snapshot repair must reach the backing thread cache"
    );
    let mut replica: imbl::Vector<_> = vec![root].into();
    for diff in existing.try_recv().unwrap().diffs {
        diff.apply(&mut replica);
    }
    assert!(replica[0].raw().deserialize().unwrap().is_redacted());
    assert!(
        matches!(
            snapshot.updates.try_recv(),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty)
        ),
        "repairs are already in the new snapshot"
    );
    let again = room.subscribe_to_thread_with_room_updates(root_id.to_owned()).await.unwrap();
    assert!(again.events[0].raw().deserialize().unwrap().is_redacted());
    assert!(existing.is_empty(), "a second snapshot must not repeat the repair");
}
