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

use std::{future::pending, sync::Arc};

use assert_matches2::assert_let;
use futures_util::{FutureExt as _, pin_mut};
use matrix_sdk_base::{
    RoomState,
    event_cache::Event,
    linked_chunk::{ChunkIdentifier, LinkedChunkId, Position, Update},
    sync::{JoinedRoomUpdate, Timeline},
};
use matrix_sdk_test::{ALICE, async_test, event_factory::EventFactory};
use ruma::{OwnedEventId, event_id, room_id};
use tokio::sync::{Barrier, broadcast::error::TryRecvError};

use super::{
    super::{TimelineVectorDiffs, pagination::PaginatedCache as _},
    RoomEventCache, RoomEventCacheSubscriber, RoomEventCacheUpdate,
};
use crate::test_utils::client::MockClientBuilder;

fn drain_updates(events: &mut imbl::Vector<Event>, receiver: &mut RoomEventCacheSubscriber) {
    loop {
        match receiver.try_recv() {
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs {
                diffs, ..
            })) => {
                for diff in diffs {
                    diff.apply(events);
                }
            }
            Ok(_) => {}
            Err(TryRecvError::Empty) => break,
            Err(error) => panic!("unexpected receiver error: {error}"),
        }
    }
}

fn ids(events: impl IntoIterator<Item = Event>) -> Vec<OwnedEventId> {
    events.into_iter().map(|event| event.event_id().unwrap()).collect()
}

async fn sync_event(cache: &RoomEventCache, event: Event) {
    cache
        .handle_joined_room_update(JoinedRoomUpdate {
            timeline: Timeline { events: vec![event], ..Default::default() },
            ..Default::default()
        })
        .await
        .unwrap();
}

#[async_test]
async fn test_disk_mutation_is_published_before_releasing_its_lock() {
    let room_id = room_id!("!snapshot-disk:example.org");
    let client = MockClientBuilder::new(None).build().await;
    let factory = EventFactory::new().room(room_id).sender(*ALICE);
    let first = factory.text_msg("first").event_id(event_id!("$first")).into_event();
    let last = factory.text_msg("last").event_id(event_id!("$last")).into_event();

    // Start with one disk-only chunk and one loaded chunk.
    client
        .event_cache_store()
        .lock()
        .await
        .unwrap()
        .as_clean()
        .unwrap()
        .handle_linked_chunk_updates(
            LinkedChunkId::Room(room_id),
            vec![
                Update::NewItemsChunk { previous: None, new: ChunkIdentifier::new(0), next: None },
                Update::PushItems {
                    at: Position::new(ChunkIdentifier::new(0), 0),
                    items: vec![first.clone()],
                },
                Update::NewItemsChunk {
                    previous: Some(ChunkIdentifier::new(0)),
                    new: ChunkIdentifier::new(1),
                    next: None,
                },
                Update::PushItems {
                    at: Position::new(ChunkIdentifier::new(1), 0),
                    items: vec![last.clone()],
                },
            ],
        )
        .await
        .unwrap();

    client.event_cache().subscribe().unwrap();
    client.base_client().get_or_create_room(room_id, RoomState::Joined);
    let (cache, _handles) = client.get_room(room_id).unwrap().event_cache().await.unwrap();
    let (snapshot, mut existing_receiver) = cache.subscribe().await.unwrap();
    let mut existing_events = snapshot.into_iter().collect();

    // Stop at the old mutation/send split, before the pagination caller has any
    // opportunity to run a separate conclusion step.
    cache.inner.load_more_events_backwards().await.unwrap();
    let (snapshot, mut new_receiver) = cache.subscribe().await.unwrap();
    assert_eq!(ids(snapshot.clone()), ids([first, last]));
    assert!(new_receiver.is_empty());

    // Existing receivers must already have the matching insertion; the new one
    // must only receive subsequent mutations.
    drain_updates(&mut existing_events, &mut existing_receiver);
    assert_eq!(ids(existing_events.clone()), ids(snapshot.clone()));

    let next = factory.text_msg("next").event_id(event_id!("$next")).into_event();
    sync_event(&cache, next).await;
    let mut new_events = snapshot.into_iter().collect();
    drain_updates(&mut existing_events, &mut existing_receiver);
    drain_updates(&mut new_events, &mut new_receiver);
    assert_eq!(ids(existing_events), ids(cache.events().await.unwrap()));
    assert_eq!(ids(new_events), ids(cache.events().await.unwrap()));
}

#[async_test]
async fn test_cancelled_mutation_cannot_replay_pending_diffs_after_a_snapshot() {
    let room_id = room_id!("!snapshot-cancelled:example.org");
    let client = MockClientBuilder::new(None).build().await;
    client.event_cache().subscribe().unwrap();
    client.base_client().get_or_create_room(room_id, RoomState::Joined);
    let (cache, _handles) = client.get_room(room_id).unwrap().event_cache().await.unwrap();
    let factory = EventFactory::new().room(room_id).sender(*ALICE);
    let (snapshot, mut existing_receiver) = cache.subscribe().await.unwrap();
    let mut existing_events = snapshot.into_iter().collect();
    let first = factory.text_msg("first").event_id(event_id!("$first")).into_event();

    // Model cancellation at an async persistence/post-processing point after
    // mutating the linked chunk but before draining/publishing its diffs.
    let mut mutation = Box::pin(async {
        let mut state = cache.inner.state.write().await.unwrap();
        state.room_linked_chunk_mut().push_live_events(None, &[first.clone()]);
        pending::<()>().await;
    });
    assert!(mutation.as_mut().now_or_never().is_none());
    drop(mutation);

    let (snapshot, mut receiver) = cache.subscribe().await.unwrap();
    assert_eq!(ids(snapshot.clone()), ids([first]));
    assert!(receiver.is_empty());
    let mut events = snapshot.into_iter().collect();

    let second = factory.text_msg("second").event_id(event_id!("$second")).into_event();
    sync_event(&cache, second).await;
    drain_updates(&mut events, &mut receiver);
    drain_updates(&mut existing_events, &mut existing_receiver);
    assert_eq!(ids(events), ids(cache.events().await.unwrap()));
    assert_eq!(ids(existing_events), ids(cache.events().await.unwrap()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_concurrent_sync_network_clear_and_sent_echo_preserve_snapshot_boundary() {
    let room_id = room_id!("!snapshot-concurrent:example.org");
    let client = MockClientBuilder::new(None).build().await;
    client.event_cache().subscribe().unwrap();
    client.base_client().get_or_create_room(room_id, RoomState::Joined);
    let (cache, _handles) = client.get_room(room_id).unwrap().event_cache().await.unwrap();
    let factory = EventFactory::new().room(room_id).sender(*ALICE);
    // Keep the cache loaded when short-lived subscribers are replaced.
    let (_, _keep_alive) = cache.subscribe().await.unwrap();

    for wave in 0..100 {
        let barrier = Arc::new(Barrier::new(2));
        let writer_cache = cache.clone();
        let writer_barrier = barrier.clone();
        let sync = factory.text_msg(format!("sync {wave}")).into_event();
        let page = factory.text_msg(format!("page {wave}")).into_event();
        let echo = factory.text_msg(format!("echo {wave}")).into_event();
        let writer = tokio::spawn(async move {
            writer_barrier.wait().await;
            if wave % 8 == 0 {
                writer_cache.clear().await.unwrap();
            }
            sync_event(&writer_cache, sync).await;
            writer_cache
                .inner
                .conclude_backwards_pagination_from_network(vec![page], None, None)
                .await
                .unwrap()
                .unwrap();
            writer_cache.insert_sent_event_from_send_queue(echo).await.unwrap();
        });
        barrier.wait().await;
        let (snapshot, mut receiver) = cache.subscribe().await.unwrap();
        let mut events = snapshot.into_iter().collect();
        writer.await.unwrap();
        drain_updates(&mut events, &mut receiver);
        assert_eq!(ids(events), ids(cache.events().await.unwrap()), "wave {wave}");
    }
}

#[async_test]
async fn test_snapshot_waits_for_an_in_progress_mutation() {
    let room_id = room_id!("!snapshot-lock:example.org");
    let client = MockClientBuilder::new(None).build().await;
    client.event_cache().subscribe().unwrap();
    client.base_client().get_or_create_room(room_id, RoomState::Joined);
    let (cache, _handles) = client.get_room(room_id).unwrap().event_cache().await.unwrap();
    let factory = EventFactory::new().room(room_id).sender(*ALICE);
    let mut guard = cache.inner.state.write().await.unwrap();
    guard.room_linked_chunk_mut().push_live_events(
        None,
        &[factory.text_msg("in progress").event_id(event_id!("$in-progress")).into_event()],
    );
    let subscription = cache.subscribe();
    pin_mut!(subscription);
    assert!(subscription.as_mut().now_or_never().is_none());
    drop(guard);
    let (snapshot, mut receiver) = subscription.await.unwrap();
    assert_eq!(ids(snapshot), [event_id!("$in-progress").to_owned()]);
    assert_let!(Err(TryRecvError::Empty) = receiver.try_recv());
}
