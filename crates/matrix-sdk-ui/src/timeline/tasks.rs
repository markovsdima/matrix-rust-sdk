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

//! Long-lived tasks for the timeline.

use std::collections::BTreeSet;

use eyeball_im::VectorDiff;
use matrix_sdk::{
    deserialized_responses::TimelineEvent,
    event_cache::{
        EventsOrigin, RoomEventCache, RoomEventCacheSubscriber, RoomEventCacheUpdate,
        TimelineVectorDiffs,
    },
    send_queue::RoomSendQueueUpdate,
};
use ruma::time::Instant;
use tokio::sync::broadcast::{Receiver, error::RecvError};
use tracing::{error, info, trace, warn};

use crate::timeline::{TimelineController, TimelineFocus, event_item::RemoteEventOrigin};

/// Only inserted/reloaded events need a refresh after loading from disk.
/// Set diffs come from redecryption, redactions, receipts or metadata updates;
/// scheduling them again would feed the worker's own changes back into it.
fn newly_loaded_events(diffs: &[VectorDiff<TimelineEvent>]) -> Vec<TimelineEvent> {
    let mut events = Vec::new();
    for diff in diffs {
        match diff {
            VectorDiff::Append { values } | VectorDiff::Reset { values } => {
                events.extend(values.iter().cloned())
            }
            VectorDiff::PushFront { value }
            | VectorDiff::PushBack { value }
            | VectorDiff::Insert { value, .. } => events.push(value.clone()),
            _ => {}
        }
    }
    events
}

/// Long-lived task that forwards the [`RoomEventCacheUpdate`]s (remote echoes)
/// to the timeline.
pub(in crate::timeline) async fn room_event_cache_updates_task(
    room_event_cache: RoomEventCache,
    timeline_controller: TimelineController,
    mut room_event_cache_subscriber: RoomEventCacheSubscriber,
    timeline_focus: TimelineFocus,
) {
    trace!("Spawned the event subscriber task.");
    let timeline_id = timeline_controller.diagnostic_id();
    let mut rebuild = 0u64;
    let mut skipped_total = 0u64;
    debug_assert!(matches!(timeline_focus, TimelineFocus::Live { .. }));

    loop {
        trace!("Waiting for an event.");

        let update = match room_event_cache_subscriber.recv().await {
            Ok(up) => up,
            Err(RecvError::Closed) => break,
            Err(RecvError::Lagged(num_skipped)) => {
                rebuild = rebuild.saturating_add(1);
                skipped_total = skipped_total.saturating_add(num_skipped);
                warn!(
                    target: "matrix_sdk_ui::timeline::cache_lag",
                    timeline_id, rebuild, lag_skipped = num_skipped, skipped_total,
                    "Timeline cache lag"
                );
                let started = Instant::now();

                // subscribe() publishes pending vector changes and takes the
                // snapshot/new receiver under the room mutation lock. Retained
                // messages on the old receiver are already in that snapshot.
                let (initial_events, next_subscriber) = match room_event_cache.subscribe().await {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        error!(
                            target: "matrix_sdk_ui::timeline::cache_lag",
                            timeline_id, rebuild, ?err,
                            "Timeline cache rebuild failed to acquire snapshot"
                        );
                        break;
                    }
                };
                room_event_cache_subscriber = next_subscriber;
                let snapshot_events = initial_events.len();
                let newly_loaded =
                    timeline_controller.newly_loaded_snapshot_events(&initial_events).await;
                info!(
                    target: "matrix_sdk_ui::timeline::cache_lag",
                    timeline_id, rebuild, snapshot_events,
                    "Timeline cache rebuild started"
                );

                #[cfg(test)]
                {
                    let hook = timeline_controller.cache_rebuild_hook.lock().await.take();
                    if let Some((reached, resume)) = hook {
                        let _ = reached.send(());
                        let _ = resume.await;
                    }
                }

                // Rebuild also reloads persisted profiles, public/private
                // receipts and the fully-read marker. They are not all present
                // in the event snapshot; their channel updates may have lagged.
                let output = timeline_controller
                    .replace_remote_events_after_cache_lag(initial_events, RemoteEventOrigin::Cache)
                    .await;
                if !newly_loaded.is_empty() {
                    timeline_controller
                        .room()
                        .client()
                        .event_cache()
                        .request_decryption_for_loaded_events(
                            timeline_controller.room().room_id(),
                            newly_loaded,
                        );
                }
                info!(
                    target: "matrix_sdk_ui::timeline::cache_lag",
                    timeline_id, rebuild, snapshot_events,
                    output_operations = output.operations,
                    output_item_occurrences = output.item_occurrences,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Timeline cache rebuild finished"
                );
                continue;
            }
        };

        handle_room_update(&timeline_controller, &timeline_focus, update).await;
    }
}

async fn handle_room_update(
    timeline_controller: &TimelineController,
    timeline_focus: &TimelineFocus,
    update: RoomEventCacheUpdate,
) {
    match update {
        RoomEventCacheUpdate::MoveReadMarkerTo { event_id } => {
            trace!(target = %event_id, "Handling fully read marker.");
            timeline_controller.handle_fully_read_marker(event_id).await;
        }

        RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, origin }) => {
            trace!("Received new timeline events diffs");
            let origin = match origin {
                EventsOrigin::Sync => RemoteEventOrigin::Sync,
                EventsOrigin::Pagination => RemoteEventOrigin::Pagination,
                EventsOrigin::Cache => RemoteEventOrigin::Cache,
            };

            let loaded = if matches!(origin, RemoteEventOrigin::Cache) {
                newly_loaded_events(&diffs)
            } else {
                Vec::new()
            };

            if matches!(timeline_focus, TimelineFocus::Live { .. }) {
                timeline_controller.handle_remote_events_with_diffs(diffs, origin).await;
            } else if !matches!(timeline_focus, TimelineFocus::PinnedEvents) {
                // Only handle the remote aggregation for a non-live timeline, that's not the
                // pinned events one (since the latter handles remote aggregations on its own).
                timeline_controller.handle_remote_aggregations(diffs, origin).await;
            }

            if !loaded.is_empty() {
                timeline_controller
                    .room()
                    .client()
                    .event_cache()
                    .request_decryption_for_loaded_events(
                        timeline_controller.room().room_id(),
                        loaded,
                    );
            }
        }

        RoomEventCacheUpdate::AddEphemeralEvents { events } => {
            trace!("Received new ephemeral events from sync.");

            // TODO: ephemeral (read receipts) should be handled by the event cache (#4113).
            timeline_controller.handle_ephemeral_events(events).await;
        }

        RoomEventCacheUpdate::UpdateMembers { ambiguity_changes } => {
            if !ambiguity_changes.is_empty() {
                let member_ambiguity_changes = ambiguity_changes
                    .values()
                    .flat_map(|change| change.user_ids())
                    .collect::<BTreeSet<_>>();
                timeline_controller.force_update_sender_profiles(&member_ambiguity_changes).await;
            }
        }
    }
}

/// Long-lived task that forwards [`RoomSendQueueUpdate`]s (local echoes) to the
/// timeline.
pub(in crate::timeline) async fn room_send_queue_update_task(
    mut send_queue_stream: Receiver<RoomSendQueueUpdate>,
    timeline_controller: TimelineController,
) {
    trace!("spawned the local echo task!");

    loop {
        match send_queue_stream.recv().await {
            Ok(update) => timeline_controller.handle_room_send_queue_update(update).await,

            Err(RecvError::Lagged(num_missed)) => {
                warn!("missed {num_missed} local echoes, ignoring those missed");
            }

            Err(RecvError::Closed) => {
                trace!("channel closed, exiting the local echo handler");
                break;
            }
        }
    }
}

#[cfg(test)]
mod focused_test_scheduler;
#[cfg(test)]
pub(in crate::timeline) use focused_test_scheduler::{FocusedTestCommand, FocusedTestLane};

enum FocusedUpdate {
    Room(Result<RoomEventCacheUpdate, RecvError>),
    Focus(Result<TimelineVectorDiffs, RecvError>),
}

/// A single owner of both focused streams. A recovery replaces both receivers;
/// no independent task can apply an old aggregation or positional diff across
/// the snapshot boundary.
pub(in crate::timeline) async fn focused_timeline_updates_task(
    room_event_cache: RoomEventCache,
    timeline_controller: TimelineController,
    mut room_updates: RoomEventCacheSubscriber,
    mut focused_updates: Receiver<TimelineVectorDiffs>,
    focus: TimelineFocus,
) {
    let timeline_id = timeline_controller.diagnostic_id();
    let mut rebuild = 0u64;
    let mut skipped_total = 0u64;
    // The cache publishes focused positional changes before the corresponding
    // room aggregations. Prefer that order when both streams become ready,
    // then alternate priority so a busy focus cannot starve room controls.
    let mut prefer_focused = true;
    #[cfg(test)]
    let mut scheduler = focused_test_scheduler::FocusedTestScheduler::new(
        timeline_controller.focused_task_hook.lock().await.take(),
    );

    loop {
        #[cfg(test)]
        let scheduled = scheduler.next(&mut room_updates, &mut focused_updates).await;
        #[cfg(not(test))]
        let scheduled: Option<FocusedUpdate> = None;

        let update = match scheduled {
            Some(update) => update,
            None if prefer_focused => tokio::select! {
                biased;
                update = focused_updates.recv() => FocusedUpdate::Focus(update),
                update = room_updates.recv() => FocusedUpdate::Room(update),
            },
            None => tokio::select! {
                biased;
                update = room_updates.recv() => FocusedUpdate::Room(update),
                update = focused_updates.recv() => FocusedUpdate::Focus(update),
            },
        };
        // A focus-only pagination batch does not spend the next focus's
        // priority. Yield to room controls once they are actually waiting.
        prefer_focused = matches!(&update, FocusedUpdate::Room(_)) || room_updates.is_empty();
        let lag_source = match &update {
            FocusedUpdate::Room(_) => "room",
            FocusedUpdate::Focus(_) => "focus",
        };
        match update {
            FocusedUpdate::Room(Ok(update)) => {
                handle_room_update(&timeline_controller, &focus, update).await
            }
            FocusedUpdate::Focus(Ok(update)) => {
                let origin = match update.origin {
                    EventsOrigin::Sync => RemoteEventOrigin::Sync,
                    EventsOrigin::Pagination => RemoteEventOrigin::Pagination,
                    EventsOrigin::Cache => RemoteEventOrigin::Cache,
                };
                let loaded = if matches!(origin, RemoteEventOrigin::Cache) {
                    newly_loaded_events(&update.diffs)
                } else {
                    Vec::new()
                };
                timeline_controller.handle_remote_events_with_diffs(update.diffs, origin).await;
                if !loaded.is_empty() {
                    timeline_controller
                        .room()
                        .client()
                        .event_cache()
                        .request_decryption_for_loaded_events(
                            timeline_controller.room().room_id(),
                            loaded,
                        );
                }
            }
            FocusedUpdate::Room(Err(RecvError::Closed))
            | FocusedUpdate::Focus(Err(RecvError::Closed)) => break,
            FocusedUpdate::Room(Err(RecvError::Lagged(skipped)))
            | FocusedUpdate::Focus(Err(RecvError::Lagged(skipped))) => {
                #[cfg(test)]
                timeline_controller
                    .focused_rebuilds
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                #[cfg(test)]
                timeline_controller
                    .focused_skipped
                    .fetch_add(skipped, std::sync::atomic::Ordering::Relaxed);
                rebuild = rebuild.saturating_add(1);
                skipped_total = skipped_total.saturating_add(skipped);
                warn!(target: "matrix_sdk_ui::timeline::cache_lag", timeline_id, rebuild,
                    lag_source, lag_skipped = skipped, skipped_total, "Focused timeline cache lag");
                let started = Instant::now();
                let snapshot = match timeline_controller
                    .focused_snapshot(&focus, &room_event_cache)
                    .await
                {
                    Ok(snapshot) => snapshot,
                    Err(err) => {
                        error!(target: "matrix_sdk_ui::timeline::cache_lag", timeline_id, rebuild, ?err,
                            "Focused timeline rebuild failed to acquire snapshot");
                        break;
                    }
                };
                let snapshot_events = snapshot.events.len();
                let snapshot_relations = snapshot.relations.len();
                let loaded =
                    timeline_controller.newly_loaded_snapshot_events(&snapshot.events).await;
                focused_updates = snapshot.updates;
                room_updates = snapshot.room_updates;
                info!(target: "matrix_sdk_ui::timeline::cache_lag", timeline_id, rebuild,
                    snapshot_events, snapshot_relations, "Focused timeline cache rebuild started");
                #[cfg(test)]
                if let Some((reached, resume)) =
                    timeline_controller.cache_rebuild_hook.lock().await.take()
                {
                    let _ = reached.send(());
                    let _ = resume.await;
                }
                let output = timeline_controller
                    .rebuild_focused_snapshot(snapshot.events, snapshot.relations)
                    .await;
                if !loaded.is_empty() {
                    timeline_controller
                        .room()
                        .client()
                        .event_cache()
                        .request_decryption_for_loaded_events(
                            timeline_controller.room().room_id(),
                            loaded,
                        );
                }
                info!(target: "matrix_sdk_ui::timeline::cache_lag", timeline_id, rebuild,
                    snapshot_events, snapshot_relations, output_operations = output.operations,
                    output_item_occurrences = output.item_occurrences,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "Focused timeline cache rebuild finished");
                prefer_focused = true;
            }
        }
        #[cfg(test)]
        scheduler.complete(&room_updates, &focused_updates);
    }
}
