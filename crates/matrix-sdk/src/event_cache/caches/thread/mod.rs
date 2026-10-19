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

//! Threads-related data structures.

pub mod pagination;
mod state;

use std::{fmt, sync::Arc};

use matrix_sdk_base::event_cache::{Event, store::EventCacheStoreLock};
use ruma::{EventId, OwnedEventId, OwnedRoomId};
pub(super) use state::LockedThreadEventCacheState;
use tokio::sync::broadcast::{Receiver, Sender};
use tracing::error;

use self::pagination::ThreadPagination;
use super::{
    super::Result, EventsOrigin, TimelineVectorDiffs, room::RoomEventCacheLinkedChunkUpdate,
};
use crate::room::WeakRoom;

/// All the information related to a single thread.
#[derive(Clone)]
pub(super) struct ThreadEventCache {
    inner: Arc<ThreadEventCacheInner>,
}

/// The (non-cloneable) details of the `RoomEventCache`.
struct ThreadEventCacheInner {
    /// The thread root ID.
    thread_id: OwnedEventId,

    /// The room where this thread belongs to.
    weak_room: WeakRoom,

    /// State for this thread's event cache.
    state: LockedThreadEventCacheState,
}

impl fmt::Debug for ThreadEventCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ThreadEventCache").finish_non_exhaustive()
    }
}

impl ThreadEventCache {
    /// Create a new empty thread event cache.
    pub fn new(
        room_id: OwnedRoomId,
        thread_id: OwnedEventId,
        weak_room: WeakRoom,
        store: EventCacheStoreLock,
        linked_chunk_update_sender: Sender<RoomEventCacheLinkedChunkUpdate>,
    ) -> Self {
        Self {
            inner: Arc::new(ThreadEventCacheInner {
                thread_id: thread_id.clone(),
                weak_room,
                state: LockedThreadEventCacheState::new(
                    room_id,
                    thread_id,
                    store,
                    linked_chunk_update_sender,
                ),
            }),
        }
    }

    /// Subscribe to live events from this thread.
    pub async fn subscribe(&self) -> Result<(Vec<Event>, Receiver<TimelineVectorDiffs>)> {
        let mut state = self.inner.state.write().await?;
        let diffs = state.thread_linked_chunk_mut().updates_as_vector_diffs();
        if !diffs.is_empty() {
            let _ =
                state.state.sender.send(TimelineVectorDiffs { diffs, origin: EventsOrigin::Cache });
        }

        let events =
            state.thread_linked_chunk().events().map(|(_position, item)| item.clone()).collect();

        let recv = state.state.sender.subscribe();

        Ok((events, recv))
    }

    /// The caller already holds the room lock, matching the room -> thread
    /// order used by sync. Hold the thread lock through hydration and repairs
    /// so pagination cannot invalidate positions or replay pre-snapshot diffs.
    pub(in crate::event_cache) async fn subscribe_with_room_updates(
        &self,
        room: &super::room::RoomEventCache,
        room_state: super::room::RoomEventCacheStateLockWriteGuard<'_>,
    ) -> Result<super::room::FocusedTimelineSnapshot> {
        let mut state = self.inner.state.write().await?;
        let diffs = state.thread_linked_chunk_mut().updates_as_vector_diffs();
        if !diffs.is_empty() {
            let _ = state.sender.send(TimelineVectorDiffs { diffs, origin: EventsOrigin::Cache });
        }
        let events = state.thread_linked_chunk().events().map(|(_, event)| event.clone()).collect();
        let receiver = state.sender.subscribe();
        let (mut snapshot, repairs) =
            room.focused_snapshot_locked(room_state, events, receiver, true).await?;
        if !repairs.is_empty() {
            // Like Event focus, repair the backing cache too. Read positions
            // once under the same lock, avoiding a scan for each replacement.
            let mut repairs = repairs.into_iter().peekable();
            let replacements: Vec<_> = state
                .thread_linked_chunk()
                .events()
                .enumerate()
                .filter_map(|(index, (position, _))| {
                    if repairs.peek().is_some_and(|(repair_index, _)| *repair_index == index) {
                        let (_, event) = repairs.next().unwrap();
                        Some((position, event))
                    } else {
                        None
                    }
                })
                .collect();
            for (position, event) in replacements {
                state
                    .thread_linked_chunk_mut()
                    .replace_event_at(position, event)
                    .expect("position was read under the same thread lock");
            }
            state.propagate_changes().await?;
            let diffs = state.thread_linked_chunk_mut().updates_as_vector_diffs();
            let _ = state.sender.send(TimelineVectorDiffs { diffs, origin: EventsOrigin::Cache });
            snapshot.updates = state.sender.subscribe();
        }
        Ok(snapshot)
    }

    /// Return a [`ThreadPagination`] useful for running back-pagination queries
    /// in this thread.
    pub fn pagination(&self) -> ThreadPagination {
        ThreadPagination::new(self.inner.clone())
    }

    /// Clear a thread, after a gappy sync for instance.
    pub async fn clear(&mut self) -> Result<()> {
        let mut state = self.inner.state.write().await?;

        let updates_as_vector_diffs = state.reset().await?;

        if !updates_as_vector_diffs.is_empty() {
            let _ = state.state.sender.send(TimelineVectorDiffs {
                diffs: updates_as_vector_diffs,
                origin: EventsOrigin::Cache,
            });
        }

        Ok(())
    }

    /// Push some live events to this thread, and propagate the updates to
    /// the listeners.
    pub async fn add_live_events(&mut self, events: Vec<Event>) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let mut state = self.inner.state.write().await?;
        let timeline_event_diffs = state.handle_sync(events).await?;

        if !timeline_event_diffs.is_empty() {
            let _ = state.state.sender.send(TimelineVectorDiffs {
                diffs: timeline_event_diffs,
                origin: EventsOrigin::Sync,
            });
        }

        Ok(())
    }

    /// Remove an event from an thread event linked chunk, if it exists.
    ///
    /// If the event has been found and removed, then an update will be
    /// propagated to observers.
    pub(super) async fn remove_if_present(&mut self, event_id: &EventId) -> Result<()> {
        let mut state = self.inner.state.write().await?;

        let Some(position) = state.thread_linked_chunk().events().find_map(|(position, event)| {
            (event.event_id().as_deref() == Some(event_id)).then_some(position)
        }) else {
            // Event not found in the linked chunk, nothing to do.
            return Ok(());
        };

        if let Err(err) = state.remove_events(vec![(event_id.to_owned(), position)]).await {
            error!(%err, "a thread linked chunk position was valid a few lines above, but invalid when deleting");
            return Err(err);
        }

        let timeline_event_diffs = state.thread_linked_chunk_mut().updates_as_vector_diffs();

        if !timeline_event_diffs.is_empty() {
            let _ = state.state.sender.send(TimelineVectorDiffs {
                diffs: timeline_event_diffs,
                origin: EventsOrigin::Sync,
            });
        }

        Ok(())
    }

    /// Returns the latest event ID in this thread, if any.
    pub async fn latest_event_id(&self) -> Result<Option<OwnedEventId>> {
        Ok(self
            .inner
            .state
            .read()
            .await?
            .thread_linked_chunk()
            .revents()
            .next()
            .and_then(|(_position, event)| event.event_id()))
    }
}
