// Copyright 2025 The Matrix.org Foundation C.I.C.
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

use std::{collections::HashSet, sync::atomic::Ordering};

use matrix_sdk_base::event_cache::Event;
use ruma::{OwnedEventId, events::relation::RelationType, time::Instant};
use tokio::sync::broadcast::Receiver;

use super::{
    RoomEventCache, RoomEventCacheGenericUpdate, RoomEventCacheStateLockWriteGuard,
    RoomEventCacheSubscriber, RoomEventCacheUpdate,
};
use crate::event_cache::{
    EventCacheError, EventFocusThreadMode, EventsOrigin, Result, TimelineVectorDiffs,
};

/// A focused event snapshot and the two streams that continue it.
///
/// `events` alone defines positional diffs in `updates`. `relations` must be
/// applied as aggregations, without adding entries to that positional order.
/// Receipts, members and the fully-read marker must additionally be refreshed
/// from the room state store when recovering from lag.
#[allow(missing_debug_implementations)]
pub struct FocusedTimelineSnapshot {
    /// The focused cache's positional event sequence.
    pub events: Vec<Event>,
    /// Related events outside the focused sequence, in cache order per target.
    pub relations: Vec<Event>,
    /// Positional updates strictly after the focused snapshot.
    pub updates: Receiver<TimelineVectorDiffs>,
    /// Room aggregations and control updates after the room snapshot boundary.
    pub room_updates: RoomEventCacheSubscriber,
}

pub(super) type FocusedSnapshotAndRepairs = (FocusedTimelineSnapshot, Vec<(usize, Event)>);

impl RoomEventCache {
    /// Subscribe to an existing event focus and the room's aggregation/control
    /// updates. Returns `None` if that focused cache has not been created.
    pub async fn subscribe_to_event_with_room_updates(
        &self,
        target: OwnedEventId,
        thread_mode: EventFocusThreadMode,
    ) -> Result<Option<FocusedTimelineSnapshot>> {
        // Drop the room lock before waiting for the focus lock: pagination
        // holds the focus lock when saving network results into this room.
        let cache = self.get_event_focused_cache(target, thread_mode).await?;
        match cache {
            Some(cache) => cache.subscribe_with_room_updates(self).await.map(Some),
            None => Ok(None),
        }
    }

    /// Subscribe to a thread and its room updates at a recovery boundary.
    pub async fn subscribe_to_thread_with_room_updates(
        &self,
        root: OwnedEventId,
    ) -> Result<FocusedTimelineSnapshot> {
        let mut state = self.inner.state.write().await?;
        let thread = state.get_or_reload_thread(root).clone();
        // Keep room -> thread lock order and repair the backing thread while
        // it is locked, not only the returned snapshot. In particular, thread
        // roots are not removed by the room's redacted-reply handling.
        thread.subscribe_with_room_updates(self, state).await
    }

    /// Subscribe to pinned events and their room updates at a recovery boundary.
    pub async fn subscribe_to_pinned_events_with_room_updates(
        &self,
    ) -> Result<FocusedTimelineSnapshot> {
        let room = self.inner.weak_room.get().ok_or(EventCacheError::ClientDropped)?;
        let state = self.inner.state.write().await?;
        let (events, receiver) = state.subscribe_to_pinned_events(room).await?;
        // Pinned cache already includes its relations. Re-reading each relation
        // transitively would add work and could duplicate the cache's ordering.
        self.focused_snapshot_locked(state, events, receiver, false)
            .await
            .map(|(snapshot, _)| snapshot)
    }

    pub(in crate::event_cache) async fn focused_snapshot(
        &self,
        events: Vec<Event>,
        receiver: Receiver<TimelineVectorDiffs>,
    ) -> Result<FocusedSnapshotAndRepairs> {
        self.focused_snapshot_locked(self.inner.state.write().await?, events, receiver, true).await
    }

    pub(in crate::event_cache) async fn focused_snapshot_locked(
        &self,
        mut state: RoomEventCacheStateLockWriteGuard<'_>,
        mut events: Vec<Event>,
        updates: Receiver<TimelineVectorDiffs>,
        load_relations: bool,
    ) -> Result<FocusedSnapshotAndRepairs> {
        let started = Instant::now();
        let mut relation_targets = 0usize;
        let mut relations = Vec::new();
        let mut repairs = Vec::new();
        if load_relations {
            let mut seen: HashSet<_> = events.iter().filter_map(Event::event_id).collect();
            for (index, event) in events.iter_mut().enumerate() {
                let Some(id) = event.event_id() else { continue };
                relation_targets += 1;
                // Replies in a thread are positional events, not aggregations.
                // In particular, focusing its root must not recursively load
                // the entire thread just to restore edits, reactions or polls.
                let filters = vec![
                    RelationType::Annotation,
                    RelationType::Replacement,
                    RelationType::Reference,
                ];
                if let Some((stored, related)) =
                    state.find_event_with_relations(&id, Some(filters)).await?
                {
                    // Event-focused caches don't receive all redactions. Never
                    // restore an unredacted or encrypted version over newer
                    // stored redaction/decryption. Other cached fields retain
                    // the focus snapshot's version and positional boundary.
                    let redacted = stored
                        .raw()
                        .get_field::<serde_json::Value>("unsigned")
                        .ok()
                        .flatten()
                        .is_some_and(|u| u.get("redacted_because").is_some());
                    if (redacted && event.raw().json().get() != stored.raw().json().get())
                        || (event.kind.is_utd() && !stored.kind.is_utd())
                    {
                        repairs.push((index, stored.clone()));
                        *event = stored;
                    }
                    relations.extend(
                        related
                            .into_iter()
                            .filter(|e| e.event_id().is_some_and(|id| seen.insert(id))),
                    );
                }
            }
        }

        // Flush cancelled room mutations before subscribing, exactly as in
        // subscribe(). Both snapshots are now covered, without cloning the
        // unrelated room history into this focused recovery.
        let diffs = state.room_linked_chunk_mut().updates_as_vector_diffs();
        if !diffs.is_empty() {
            self.inner.update_sender.send(
                RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs {
                    diffs,
                    origin: EventsOrigin::Cache,
                }),
                Some(RoomEventCacheGenericUpdate { room_id: self.inner.room_id.clone() }),
            );
        }
        let subscriber_count = state.subscriber_count().clone();
        subscriber_count.fetch_add(1, Ordering::SeqCst);
        let room_updates = RoomEventCacheSubscriber::new(
            self.inner.update_sender.new_room_receiver(),
            self.inner.room_id.clone(),
            self.inner.auto_shrink_sender.clone(),
            subscriber_count,
        );
        tracing::debug!(
            target: "matrix_sdk::event_cache::focused_snapshot",
            snapshot_events = events.len(), relation_targets,
            snapshot_relations = relations.len(), repairs = repairs.len(),
            locked_section_ms = started.elapsed().as_millis() as u64,
            "Focused snapshot acquired"
        );
        Ok((FocusedTimelineSnapshot { events, relations, updates, room_updates }, repairs))
    }
}
