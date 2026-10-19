// Copyright 2023 Kévin Commaille
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

use std::{cmp::Ordering, collections::HashMap};

use futures_core::Stream;
use indexmap::IndexMap;
use ruma::{
    EventId, MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedUserId, UserId,
    events::receipt::{Receipt, ReceiptEventContent, ReceiptThread, ReceiptType},
};
use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tracing::{debug, error, instrument, trace, warn};

use super::{
    AllRemoteEvents, ObservableItemsTransaction, RelativePosition, RoomDataProvider,
    TimelineMetadata, TimelineState, rfind_event_by_id,
};
use crate::timeline::{TimelineItem, TimelineItemContent, controller::TimelineStateTransaction};

/// In-memory caches for read receipts.
#[derive(Clone, Debug, Default)]
pub(super) struct ReadReceipts {
    /// Map of public read receipts on events.
    ///
    /// Event ID => User ID => Read receipt of the user.
    by_event: HashMap<OwnedEventId, IndexMap<OwnedUserId, Receipt>>,

    /// In-memory cache of all latest read receipts by user.
    ///
    /// User ID => Receipt type => Read receipt of the user of the given
    /// type.
    latest_by_user: HashMap<OwnedUserId, HashMap<ReceiptType, (OwnedEventId, Receipt)>>,

    /// Receipts recovered after live-cache lag whose targets are outside the
    /// snapshot. Bound to one entry per cached user/type, cleared with metadata.
    /// An unknown target can be older or newer; without positions or timestamps
    /// we conservatively retain persisted state over implicit inference. Normal
    /// initial timelines do not use this protection. Loading the target, a
    /// newer timestamp, or a store-confirmed notification for the same
    /// user/type/thread releases protection. A Main fallback also yields to a
    /// store-confirmed Unthreaded receipt, the preferred key for that timeline.
    authoritative_pending: HashMap<OwnedUserId, HashMap<ReceiptType, PendingReceiptSource>>,

    /// A sender to notify of changes to the receipts of our own user.
    own_user_read_receipts_changed_sender: watch::Sender<()>,

    /// Numeric work counters, retained across metadata clears. These count
    /// visited entries, not retained events, and contain no event/user data.
    search_counts: ReadReceiptSearchCounts,
}

/// Why an unloaded stored receipt is protected during recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PendingReceiptSource {
    /// Keep the authoritative thread even if the item's cached receipt still
    /// has another thread: updates to the same event ID can leave it unchanged.
    Thread(ReceiptThread),
    /// Our unthreaded receipt was absent, so initial loading used Main instead.
    OwnMainFallback,
}

/// Cumulative receipt work. Membership guards for authoritative receipts are
/// separate from these positional/display searches and are not included.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(in crate::timeline) struct ReadReceiptSearchCounts {
    pub position_event_visits: u64,
    pub fallback_item_visits: u64,
    pub compute_event_visits: u64,
    pub deferred_item_adds: u64,
    pub missing_item_targets: u64,
}

impl ReadReceipts {
    pub(super) fn search_counts(&self) -> ReadReceiptSearchCounts {
        self.search_counts
    }

    /// Empty the caches.
    pub(super) fn clear(&mut self) {
        self.by_event.clear();
        self.latest_by_user.clear();
        self.authoritative_pending.clear();
    }

    /// Subscribe to changes in the read receipts of our own user.
    pub(super) fn subscribe_own_user_read_receipts_changed(
        &self,
    ) -> impl Stream<Item = ()> + use<> {
        let subscriber = self.own_user_read_receipts_changed_sender.subscribe();
        WatchStream::from_changes(subscriber)
    }

    /// Read the latest read receipt of the given type for the given user, from
    /// the in-memory cache.
    pub(crate) fn get_latest(
        &self,
        user_id: &UserId,
        receipt_type: &ReceiptType,
    ) -> Option<&(OwnedEventId, Receipt)> {
        self.latest_by_user.get(user_id).and_then(|map| map.get(receipt_type))
    }

    /// Insert or update in the local cache the latest read receipt for the
    /// given user.
    fn upsert_latest(
        &mut self,
        user_id: OwnedUserId,
        receipt_type: ReceiptType,
        read_receipt: (OwnedEventId, Receipt),
    ) {
        self.latest_by_user.entry(user_id).or_default().insert(receipt_type, read_receipt);
    }

    /// Update the timeline items with the given read receipt if it is more
    /// recent than the current one.
    ///
    /// In the process, if applicable, this method updates the inner maps to use
    /// the new receipt. If `is_own_user_id` is `false`, it also updates the
    /// receipts on the corresponding timeline items.
    ///
    /// Currently this method only works reliably if the timeline was started
    /// from the end of the timeline.
    #[instrument(skip_all, fields(user_id = %new_receipt.user_id, event_id = %new_receipt.event_id))]
    fn maybe_update_read_receipt(
        &mut self,
        new_receipt: FullReceipt<'_>,
        is_own_user_id: bool,
        from_store: bool,
        protect_unknown: Option<PendingReceiptSource>,
        timeline_items: &mut ObservableItemsTransaction<'_>,
    ) {
        let all_events = timeline_items.all_remote_events();

        if from_store
            && !all_events.iter().any(|event| event.event_id == new_receipt.event_id)
            && self.get_latest(new_receipt.user_id, &new_receipt.receipt_type).is_some_and(
                |(old_event_id, old_receipt)| {
                    all_events.iter().any(|event| event.event_id == *old_event_id)
                        && old_receipt
                            .ts
                            .zip(new_receipt.receipt.ts)
                            .is_some_and(|(old, new)| old > new)
                },
            )
        {
            // An unloaded stored receipt can also point into older history.
            // Preserve a newer known implicit receipt when timestamps provide
            // ordering evidence; don't turn it into an artificial pending one.
            return;
        }

        if self.is_authoritative_pending(new_receipt.user_id, &new_receipt.receipt_type, all_events)
            && !from_store
        {
            // Store state can be ahead of this snapshot. Neither a queued old
            // receipt nor an implicit receipt on an old sender event may move
            // it backwards while its target's position is still unknown.
            let newer_timestamp = self
                .get_latest(new_receipt.user_id, &new_receipt.receipt_type)
                .and_then(|(_, receipt)| receipt.ts.zip(new_receipt.receipt.ts))
                .is_some_and(|(old, new)| new > old);
            if !newer_timestamp {
                return;
            }
        }
        // Refreshing a Main fallback from the store does not make it an
        // independent Main receipt. Preserve its scope until it is replaced
        // or acknowledged; a later Unthreaded confirmation must still win.
        let protect_unknown = protect_unknown.map(|source| {
            if new_receipt.receipt.thread == ReceiptThread::Main
                && self
                    .authoritative_pending
                    .get(new_receipt.user_id)
                    .and_then(|types| types.get(&new_receipt.receipt_type))
                    == Some(&PendingReceiptSource::OwnMainFallback)
            {
                PendingReceiptSource::OwnMainFallback
            } else {
                source
            }
        });
        if let Some(types) = self.authoritative_pending.get_mut(new_receipt.user_id) {
            types.remove(&new_receipt.receipt_type);
            if types.is_empty() {
                self.authoritative_pending.remove(new_receipt.user_id);
            }
        }
        if let Some(source) = protect_unknown
            && !all_events.iter().any(|event| event.event_id == new_receipt.event_id)
        {
            self.authoritative_pending
                .entry(new_receipt.user_id.to_owned())
                .or_default()
                .insert(new_receipt.receipt_type.clone(), source);
        }

        // Get old receipt.
        let old_receipt = self.get_latest(new_receipt.user_id, &new_receipt.receipt_type);

        if old_receipt
            .as_ref()
            .is_some_and(|(old_receipt_event_id, _)| old_receipt_event_id == new_receipt.event_id)
        {
            // The receipt has not changed so there is nothing to do.
            if !is_own_user_id {
                trace!("receipt hasn't changed, nothing to do");
            }
            return;
        }

        let old_event_id = old_receipt.map(|(event_id, _)| event_id.clone());

        // Find receipts positions.
        let mut old_receipt_pos = None;
        let mut old_item_pos = None;
        let mut old_item_event_id = None;
        let mut new_receipt_pos = None;
        let mut new_item_pos = None;
        let mut new_item_event_id = None;

        for (pos, event) in all_events.iter().rev().enumerate() {
            self.search_counts.position_event_visits += 1;
            if old_receipt_pos.is_none() && old_event_id.as_ref() == Some(&event.event_id) {
                old_receipt_pos = Some(pos);
            }

            // The receipt should appear on the first visible event that can show read
            // receipts.
            if old_receipt_pos.is_some()
                && old_item_event_id.is_none()
                && event.visible
                && event.can_show_read_receipts
            {
                old_item_pos = event.timeline_item_index;
                old_item_event_id = Some(event.event_id.clone());
            }

            if new_receipt_pos.is_none() && new_receipt.event_id == event.event_id {
                new_receipt_pos = Some(pos);
            }

            // The receipt should appear on the first visible event that can show read
            // receipts.
            if new_receipt_pos.is_some()
                && new_item_event_id.is_none()
                && event.visible
                && event.can_show_read_receipts
            {
                new_item_pos = event.timeline_item_index;
                new_item_event_id = Some(event.event_id.clone());
            }

            if old_item_event_id.is_some() && new_item_event_id.is_some() {
                // We have everything we need, stop.
                break;
            }
        }

        // Check if the old receipt is more recent than the new receipt.
        if let Some(old_receipt_pos) = old_receipt_pos {
            if let Some(new_receipt_pos) = new_receipt_pos {
                if old_receipt_pos < new_receipt_pos {
                    // The old receipt is more recent than the new one.
                    if !is_own_user_id {
                        trace!(
                            "the previous read receipt is more recent than the new one, exiting"
                        );
                    }
                    return;
                }
            } else if !from_store {
                // The old receipt is more recent since we can't find the new receipt in the
                // timeline and we supposedly have all events since the end of the timeline.
                if !is_own_user_id {
                    trace!(
                        "we had a previous read receipt, but couldn't find the event \
                         targeted by the new read receipt in the timeline, exiting"
                    );
                }
                return;
            }
        }

        // The new receipt is deemed more recent from now on because:
        // - If old_receipt_pos is Some, we already checked all the cases where it
        //   wouldn't be more recent.
        // - If both old_receipt_pos and new_receipt_pos are None, they are both
        //   explicit read receipts so the server should only send us a more recent
        //   receipt.
        // - If old_receipt_pos is None and new_receipt_pos is Some, the new receipt is
        //   more recent because it has a place in the timeline.

        if !is_own_user_id {
            trace!(
                from_event = ?old_event_id,
                from_visible_event = ?old_item_event_id,
                to_event = ?new_receipt.event_id,
                to_visible_event = ?new_item_event_id,
                ?old_item_pos,
                ?new_item_pos,
                "moving read receipt",
            );

            // Remove the old receipt from the old event.
            if let Some(old_event_id) = old_event_id {
                self.remove_event_receipt_for_user(&old_event_id, new_receipt.user_id);
            }

            // Add the new receipt to the new event.
            self.add_event_receipt_for_user(
                new_receipt.event_id.to_owned(),
                new_receipt.user_id.to_owned(),
                new_receipt.receipt.clone(),
            );
        }

        // Update the receipt of the user.
        self.upsert_latest(
            new_receipt.user_id.to_owned(),
            new_receipt.receipt_type,
            (new_receipt.event_id.to_owned(), new_receipt.receipt.clone()),
        );

        if is_own_user_id {
            self.own_user_read_receipts_changed_sender.send_replace(());
            // This receipt cannot change items in the timeline.
            return;
        }

        if new_item_event_id == old_item_event_id {
            // The receipt did not change in the timeline.
            return;
        }

        // Metadata is registered before a newly loaded event is rendered. Its
        // receipt is already in `by_event`, and `compute_event_receipts` will
        // put it on the newly created item. Searching the current item vector
        // here cannot succeed and would scan a growing prefix on every append.
        // Only the two pre-insertion callers opt into this path; unexpected
        // missing items in explicit receipt updates remain diagnosed below.
        let new_item_pending = new_receipt.target_is_pending
            && new_item_pos.is_none()
            && new_item_event_id.as_deref() == Some(new_receipt.event_id);
        let timeline_update = ReadReceiptTimelineUpdate {
            old_item_pos,
            old_event_id: old_item_event_id,
            new_item_pos,
            new_event_id: new_item_event_id,
            new_item_pending,
        };

        timeline_update.apply(
            timeline_items,
            new_receipt.user_id.to_owned(),
            new_receipt.receipt.clone(),
            &mut self.search_counts,
        );
    }

    /// Returns the cached receipts by user for a given `event_id`.
    fn get_event_receipts(&self, event_id: &EventId) -> Option<&IndexMap<OwnedUserId, Receipt>> {
        self.by_event.get(event_id)
    }

    /// A notification matching the store acknowledges this user/type/thread,
    /// including when its target has changed since recovery. Release protection
    /// before applying it with ordinary live ordering; otherwise a pending old
    /// target would reject the new value when timestamps are absent or equal.
    /// Another thread must not release the current receipt's protection, except
    /// for Unthreaded superseding our Main fallback: main and unthreaded receipts
    /// share the same user/type cache entry.
    pub(super) fn acknowledge_recovery_receipt(
        &mut self,
        user: &UserId,
        receipt_type: &ReceiptType,
        thread: &ReceiptThread,
    ) {
        let Some(types) = self.authoritative_pending.get_mut(user) else { return };
        let acknowledged = types.get(receipt_type).is_some_and(|source| match source {
            PendingReceiptSource::Thread(protected_thread) => protected_thread == thread,
            PendingReceiptSource::OwnMainFallback => {
                matches!(thread, ReceiptThread::Main | ReceiptThread::Unthreaded)
            }
        });
        if acknowledged {
            types.remove(receipt_type);
            if types.is_empty() {
                self.authoritative_pending.remove(user);
            }
        }
    }

    fn is_authoritative_pending(
        &self,
        user_id: &UserId,
        receipt_type: &ReceiptType,
        all_events: &AllRemoteEvents,
    ) -> bool {
        self.authoritative_pending
            .get(user_id)
            .is_some_and(|types| types.contains_key(receipt_type))
            && self.get_latest(user_id, receipt_type).is_some_and(|(event_id, _)| {
                !all_events.iter().any(|event| event.event_id == *event_id)
            })
    }

    fn authoritative_pending_order(
        &self,
        user_id: &UserId,
        all_events: &AllRemoteEvents,
    ) -> Option<Ordering> {
        let public_pending = self.is_authoritative_pending(user_id, &ReceiptType::Read, all_events);
        let private_pending =
            self.is_authoritative_pending(user_id, &ReceiptType::ReadPrivate, all_events);
        if (public_pending || private_pending)
            && let Some((public, private)) = self
                .get_latest(user_id, &ReceiptType::Read)
                .and_then(|(_, receipt)| receipt.ts)
                .zip(
                    self.get_latest(user_id, &ReceiptType::ReadPrivate)
                        .and_then(|(_, receipt)| receipt.ts),
                )
            && public != private
        {
            return Some(public.cmp(&private));
        }
        match (public_pending, private_pending) {
            (true, false) => Some(Ordering::Greater),
            (false, true) => Some(Ordering::Less),
            _ => None,
        }
    }

    /// Get the visible receipt target using the receipt caches and event order
    /// from the same state (which may be an uncommitted rebuild transaction).
    fn latest_user_read_receipt_timeline_event_id(
        &self,
        user_id: &UserId,
        all_remote_events: &AllRemoteEvents,
    ) -> Option<OwnedEventId> {
        // We only need to use the local map, since receipts for known events are
        // already loaded from the store.
        let public_read_receipt = self.get_latest(user_id, &ReceiptType::Read);
        let private_read_receipt = self.get_latest(user_id, &ReceiptType::ReadPrivate);

        // Let's assume that a private read receipt should be more recent than a public
        // read receipt, otherwise there's no point in the private read receipt,
        // and use it as default.
        let (latest_receipt_id, _) = match self
            .authoritative_pending_order(user_id, all_remote_events)
            .unwrap_or_else(|| {
                TimelineMetadata::compare_optional_receipts(
                    public_read_receipt,
                    private_read_receipt,
                    all_remote_events,
                )
            }) {
            Ordering::Greater => public_read_receipt?,
            Ordering::Less => private_read_receipt?,
            _ => unreachable!(),
        };

        // Find the corresponding visible event.
        all_remote_events
            .iter()
            .rev()
            .skip_while(|ev| ev.event_id != *latest_receipt_id)
            .find(|ev| ev.visible && ev.can_show_read_receipts)
            .map(|ev| ev.event_id.clone())
    }

    /// Mark the given event as seen by the user with the given receipt.
    fn add_event_receipt_for_user(
        &mut self,
        event_id: OwnedEventId,
        user_id: OwnedUserId,
        receipt: Receipt,
    ) {
        self.by_event.entry(event_id).or_default().insert(user_id, receipt);
    }

    /// Unmark the given event as seen by the user.
    fn remove_event_receipt_for_user(&mut self, event_id: &EventId, user_id: &UserId) {
        if let Some(map) = self.by_event.get_mut(event_id) {
            map.swap_remove(user_id);
            // Remove the entire map if this was the last entry.
            if map.is_empty() {
                self.by_event.remove(event_id);
            }
        }
    }

    /// Get the read receipts by user for the given event.
    ///
    /// This includes all the receipts on the event as well as all the receipts
    /// on the following events that are filtered out (not visible).
    #[instrument(skip(self, timeline_items, at_end))]
    pub(super) fn compute_event_receipts(
        &mut self,
        event_id: &EventId,
        timeline_items: &mut ObservableItemsTransaction<'_>,
        at_end: bool,
    ) -> IndexMap<OwnedUserId, Receipt> {
        let mut all_receipts = self.get_event_receipts(event_id).cloned().unwrap_or_default();

        if at_end {
            // No need to search for extra receipts, there are no events after.
            trace!(
                "early return because @end, retrieved receipts: {}",
                all_receipts.iter().map(|(u, _)| u.as_str()).collect::<Vec<_>>().join(", ")
            );
            return all_receipts;
        }

        trace!(
            "loaded receipts: {}",
            all_receipts.iter().map(|(u, _)| u.as_str()).collect::<Vec<_>>().join(", ")
        );

        // We are going to add receipts for hidden events to this item.
        //
        // However: since we may be inserting an event at a random position, the
        // previous timeline item may already be holding some hidden read
        // receipts. As a result, we need to be careful here: if we're inserting
        // after an event that holds hidden read receipts, then we should steal
        // them from it.
        //
        // Find the event, go past it, and keep a reference to the previous rendered
        // timeline item, if any.
        let mut events_iter = timeline_items.all_remote_events().iter();
        let mut prev_event_and_item_index = None;

        for meta in events_iter.by_ref() {
            self.search_counts.compute_event_visits += 1;
            if meta.event_id == event_id {
                break;
            }
            if let Some(item_index) = meta.timeline_item_index {
                prev_event_and_item_index = Some((meta.event_id.clone(), item_index));
            }
        }

        // Include receipts from all the following events that are hidden or can't show
        // read receipts.
        let mut hidden = Vec::new();
        let mut hidden_event_visits = 0;
        for hidden_receipt_event_meta in events_iter
            .inspect(|_| hidden_event_visits += 1)
            .take_while(|meta| !meta.visible || !meta.can_show_read_receipts)
        {
            if let Some(event_receipts) =
                self.get_event_receipts(&hidden_receipt_event_meta.event_id)
            {
                trace!(%hidden_receipt_event_meta.event_id, "found receipts on hidden event");
                hidden.extend(event_receipts.clone());
            }
        }
        self.search_counts.compute_event_visits += hidden_event_visits;

        // Steal hidden receipts from the previous timeline item, if it carried them.
        if let Some((prev_event_id, prev_item_index)) = prev_event_and_item_index {
            let prev_item = &timeline_items[prev_item_index];
            // Technically, we could unwrap the `as_event()`, because this is a rendered
            // item for an event in all_remote_events, but this extra check is
            // cheap.
            if let Some(remote_prev_item) = prev_item.as_event() {
                let prev_receipts = remote_prev_item.read_receipts().clone();
                for (user_id, _) in &hidden {
                    if !prev_receipts.contains_key(user_id) {
                        continue;
                    }
                    let mut up = ReadReceiptTimelineUpdate {
                        old_item_pos: Some(prev_item_index),
                        old_event_id: Some(prev_event_id.clone()),
                        new_item_pos: None,
                        new_event_id: None,
                        new_item_pending: false,
                    };
                    up.remove_old_receipt(timeline_items, user_id, &mut self.search_counts);
                }
            }
        }

        all_receipts.extend(hidden);
        trace!(
            "computed receipts: {}",
            all_receipts.iter().map(|(u, _)| u.as_str()).collect::<Vec<_>>().join(", ")
        );
        all_receipts
    }
}

struct FullReceipt<'a> {
    event_id: &'a EventId,
    user_id: &'a UserId,
    receipt_type: ReceiptType,
    receipt: &'a Receipt,
    /// The target's metadata has just been inserted, but its visible item has
    /// not been created yet. Hidden targets can still move a receipt onto an
    /// existing preceding visible item.
    target_is_pending: bool,
}

/// A read receipt update in the timeline.
#[derive(Clone, Debug, Default)]
struct ReadReceiptTimelineUpdate {
    /// The position of the timeline item that had the old receipt of the user,
    /// if any.
    old_item_pos: Option<usize>,
    /// The old event that had the receipt of the user, if any.
    old_event_id: Option<OwnedEventId>,
    /// The position of the timeline item that has the new receipt of the user,
    /// if any.
    new_item_pos: Option<usize>,
    /// The new event that has the receipt of the user, if any.
    new_event_id: Option<OwnedEventId>,
    /// An expected, not-yet-rendered target; its item constructor loads receipts
    /// from the cache after this update finishes.
    new_item_pending: bool,
}

impl ReadReceiptTimelineUpdate {
    /// Remove the old receipt from the corresponding timeline item.
    #[instrument(skip_all)]
    fn remove_old_receipt(
        &mut self,
        items: &mut ObservableItemsTransaction<'_>,
        user_id: &UserId,
        counts: &mut ReadReceiptSearchCounts,
    ) {
        let Some(event_id) = &self.old_event_id else {
            // Nothing to do.
            return;
        };

        let item_pos = self.old_item_pos.or_else(|| {
            items
                .iter_remotes_region()
                .rev()
                .inspect(|_| counts.fallback_item_visits += 1)
                .filter_map(|(nth, item)| Some((nth, item.as_event()?)))
                .find_map(|(nth, event_item)| {
                    (event_item.event_id() == Some(event_id)).then_some(nth)
                })
        });

        let Some(item_pos) = item_pos else {
            counts.missing_item_targets += 1;
            debug!(%event_id, %user_id, "inconsistent state: old event item for read receipt was not found");
            return;
        };

        self.old_item_pos = Some(item_pos);

        let event_item = &items[item_pos];
        let event_item_id = event_item.unique_id().to_owned();

        let Some(mut event_item) = event_item.as_event().cloned() else {
            warn!("received a read receipt for a virtual item, this should not be possible");
            return;
        };

        if let Some(remote_event_item) = event_item.as_remote_mut() {
            if remote_event_item.read_receipts.swap_remove(user_id).is_none() {
                debug!(
                    %event_id, %user_id,
                    "inconsistent state: old event item for user's read \
                     receipt doesn't have a receipt for the user"
                );
            }
            trace!(%user_id, %event_id, "removed read receipt from event item");
            items.replace_read_receipts(item_pos, TimelineItem::new(event_item, event_item_id));
        } else {
            warn!("received a read receipt for a local item, this should not be possible");
        }
    }

    /// Add the new receipt to the corresponding timeline item.
    #[instrument(skip_all)]
    fn add_new_receipt(
        self,
        items: &mut ObservableItemsTransaction<'_>,
        user_id: OwnedUserId,
        receipt: Receipt,
        counts: &mut ReadReceiptSearchCounts,
    ) {
        let Some(event_id) = self.new_event_id else {
            // Nothing to do.
            return;
        };

        if self.new_item_pending {
            counts.deferred_item_adds += 1;
            return;
        }

        let old_item_pos = self.old_item_pos.unwrap_or(0);

        let item_pos = self.new_item_pos.or_else(|| {
            items
                .iter_remotes_region()
                .inspect(|_| counts.fallback_item_visits += 1)
                // Don't iterate over all items if the `old_item_pos` is known: the `item_pos`
                // for the new item is necessarily _after_ the old item.
                .skip_while(|(nth, _)| *nth < old_item_pos)
                .find_map(|(nth, item)| {
                    if let Some(event_item) = item.as_event() {
                        (event_item.event_id() == Some(&event_id)).then_some(nth)
                    } else {
                        None
                    }
                })
        });

        let Some(item_pos) = item_pos else {
            counts.missing_item_targets += 1;
            debug!(
                %event_id, %user_id,
                "inconsistent state: new event item for read receipt was not found",
            );
            return;
        };

        debug_assert!(
            item_pos >= self.old_item_pos.unwrap_or(0),
            "The new receipt must be added on a timeline item that is _after_ the timeline item \
             that was holding the old receipt"
        );

        let event_item = &items[item_pos];
        let event_item_id = event_item.unique_id().to_owned();

        let Some(mut event_item) = event_item.as_event().cloned() else {
            warn!("received a read receipt for a virtual item, this should not be possible");
            return;
        };

        if let Some(remote_event_item) = event_item.as_remote_mut() {
            trace!(%user_id, %event_id, "added read receipt to event item");
            remote_event_item.read_receipts.insert(user_id, receipt);
            items.replace_read_receipts(item_pos, TimelineItem::new(event_item, event_item_id));
        } else {
            warn!("received a read receipt for a local item, this should not be possible");
        }
    }

    /// Apply this update to the timeline.
    fn apply(
        mut self,
        items: &mut ObservableItemsTransaction<'_>,
        user_id: OwnedUserId,
        receipt: Receipt,
        counts: &mut ReadReceiptSearchCounts,
    ) {
        self.remove_old_receipt(items, &user_id, counts);
        self.add_new_receipt(items, user_id, receipt, counts);
    }
}

impl<P: RoomDataProvider> TimelineStateTransaction<'_, P> {
    pub(super) fn handle_explicit_read_receipts(
        &mut self,
        receipt_event_content: ReceiptEventContent,
        own_user_id: &UserId,
        from_store: bool,
        protect_unknown: bool,
    ) {
        trace!("handling explicit read receipts");
        let own_receipt_thread = self.focus.receipt_thread();

        for (event_id, receipt_types) in receipt_event_content.0 {
            for (receipt_type, receipts) in receipt_types {
                // Discard the read marker updates in this function.
                if !matches!(receipt_type, ReceiptType::Read | ReceiptType::ReadPrivate) {
                    continue;
                }

                for (user_id, receipt) in receipts {
                    if matches!(own_receipt_thread, ReceiptThread::Unthreaded | ReceiptThread::Main)
                    {
                        // If the own receipt thread is unthreaded or main, we maintain maximal
                        // compatibility with clients using either unthreaded or main-thread read
                        // receipts by allowing both here.
                        match receipt.thread {
                            ReceiptThread::Unthreaded | ReceiptThread::Main => {
                                // Processing happens below.
                            }

                            ReceiptThread::Thread(thread_root) => {
                                // Special processing for threads: try to find a timeline item for
                                // the root, and update its thread summary, if the receipt is for
                                // ourselves.
                                //
                                // TODO: This is temporary code, and should be removed when #4113 is
                                // done.
                                if user_id == self.meta.own_user_id
                                    && let Some((item_pos, item)) =
                                        rfind_event_by_id(&self.items, &thread_root)
                                {
                                    trace!(
                                        "thread root has been found; will update thread summary with the latest receipts"
                                    );

                                    let item_id = item.internal_id.to_owned();
                                    let mut new_item = item.clone();

                                    let TimelineItemContent::MsgLike(msglike) =
                                        &mut new_item.content
                                    else {
                                        // Only MsgLike items have a thread summary.
                                        continue;
                                    };

                                    let Some(thread_summary) = &mut msglike.thread_summary else {
                                        // No thread summary to update.
                                        continue;
                                    };

                                    // Assume read receipts only move forward, at the moment.
                                    if receipt_type == ReceiptType::Read {
                                        thread_summary.public_read_receipt_event_id =
                                            Some(event_id.clone());
                                    } else if receipt_type == ReceiptType::ReadPrivate {
                                        thread_summary.private_read_receipt_event_id =
                                            Some(event_id.clone());
                                    } else {
                                        // We don't know this receipt type; skip it.
                                        continue;
                                    }

                                    self.items.replace_read_receipts(
                                        item_pos,
                                        TimelineItem::new(new_item, item_id),
                                    );
                                }

                                // Couldn't find an item for this new read receipt; process the
                                // next receipt.
                                continue;
                            }

                            _ => {
                                // No processing: ignore.
                                continue;
                            }
                        }
                    } else if own_receipt_thread != receipt.thread {
                        // Otherwise, we only keep the receipts of the same thread kind.
                        continue;
                    }

                    let is_own_user_id = user_id == own_user_id;

                    let full_receipt = FullReceipt {
                        event_id: &event_id,
                        user_id: &user_id,
                        receipt_type: receipt_type.clone(),
                        receipt: &receipt,
                        target_is_pending: false,
                    };

                    let protection = protect_unknown
                        .then(|| PendingReceiptSource::Thread(receipt.thread.clone()));
                    self.meta.read_receipts.maybe_update_read_receipt(
                        full_receipt,
                        is_own_user_id,
                        from_store,
                        protection,
                        &mut self.items,
                    );
                }
            }
        }
    }

    /// Load the read receipts from the store for the given event ID.
    ///
    /// Populates the read receipts in-memory caches.
    pub(super) async fn load_read_receipts_for_event(
        &mut self,
        event_id: &EventId,
        room_data_provider: &P,
    ) {
        trace!(%event_id, "loading initial receipts for an event");

        let receipt_thread = self.focus.receipt_thread();

        let receipts = if matches!(receipt_thread, ReceiptThread::Unthreaded | ReceiptThread::Main)
        {
            // If the requested receipt thread is unthreaded or main, we maintain maximal
            // compatibility with clients using either unthreaded or main-thread read
            // receipts by allowing both here.

            // First, load the main receipts.
            let mut main_receipts =
                room_data_provider.load_event_receipts(event_id, ReceiptThread::Main).await;

            // Then, load the unthreaded receipts.
            let unthreaded_receipts =
                room_data_provider.load_event_receipts(event_id, ReceiptThread::Unthreaded).await;

            // We can safely extend both here: if a key is already set, then that means that
            // the user has the unthreaded and main receipt on the main event,
            // which is fine, and something we display as the one user receipt.
            main_receipts.extend(unthreaded_receipts);
            main_receipts
        } else {
            // In all other cases, return what's requested, and only that (threaded
            // receipts).
            room_data_provider.load_event_receipts(event_id, receipt_thread.clone()).await
        };

        let own_user_id = room_data_provider.own_user_id();

        // Since they are explicit read receipts, we need to check if they are
        // superseded by implicit read receipts.
        for (user_id, receipt) in receipts {
            let full_receipt = FullReceipt {
                event_id,
                user_id: &user_id,
                receipt_type: ReceiptType::Read,
                receipt: &receipt,
                target_is_pending: true,
            };

            self.meta.read_receipts.maybe_update_read_receipt(
                full_receipt,
                user_id == own_user_id,
                false,
                None,
                &mut self.items,
            );
        }
    }

    /// Add an implicit read receipt to the given event item, if it is more
    /// recent than the current read receipt for the sender of the event.
    ///
    /// According to the spec, read receipts should not point to events sent by
    /// our own user, but these events are used to reset the notification
    /// count, so we need to handle them locally too. For that we create an
    /// "implicit" read receipt, compared to the "explicit" ones sent by the
    /// client.
    pub(super) fn maybe_add_implicit_read_receipt(
        &mut self,
        event_id: &EventId,
        sender: Option<&UserId>,
        timestamp: Option<MilliSecondsSinceUnixEpoch>,
    ) {
        let (Some(user_id), Some(timestamp)) = (sender, timestamp) else {
            // We cannot add a read receipt if we do not know the user or the timestamp.
            return;
        };

        trace!(%user_id, %event_id, "adding implicit read receipt");

        let mut receipt = Receipt::new(timestamp);
        receipt.thread = self.focus.receipt_thread();

        let full_receipt = FullReceipt {
            event_id,
            user_id,
            receipt_type: ReceiptType::Read,
            receipt: &receipt,
            target_is_pending: true,
        };

        let is_own_event = sender.is_some_and(|sender| sender == self.meta.own_user_id);

        self.meta.read_receipts.maybe_update_read_receipt(
            full_receipt,
            is_own_event,
            false,
            None,
            &mut self.items,
        );
    }

    /// Update the read receipts on the event with the given event ID and the
    /// previous visible event because of a visibility change.
    #[instrument(skip(self))]
    pub(super) fn maybe_update_read_receipts_of_prev_event(&mut self, event_id: &EventId) {
        // Find the previous visible event, if there is one.
        let Some(prev_event_meta) = self
            .items
            .all_remote_events()
            .iter()
            .rev()
            // Find the event item.
            .skip_while(|meta| meta.event_id != event_id)
            // Go past the event item.
            .skip(1)
            // Find the first visible item that can show read receipts.
            .find(|meta| meta.visible && meta.can_show_read_receipts)
        else {
            trace!("Couldn't find any previous visible event, exiting");
            return;
        };

        let Some((prev_item_pos, prev_event_item)) =
            rfind_event_by_id(&self.items, &prev_event_meta.event_id)
        else {
            error!("inconsistent state: timeline item of visible event was not found");
            return;
        };

        let prev_event_item_id = prev_event_item.internal_id.to_owned();
        let mut prev_event_item = prev_event_item.clone();

        let Some(remote_prev_event_item) = prev_event_item.as_remote_mut() else {
            warn!("loading read receipts for a local item, this should not be possible");
            return;
        };

        let read_receipts = self.meta.read_receipts.compute_event_receipts(
            &remote_prev_event_item.event_id,
            &mut self.items,
            false,
        );

        // If the count did not change, the receipts did not change either.
        if read_receipts.len() == remote_prev_event_item.read_receipts.len() {
            trace!("same count of read receipts, not doing anything");
            return;
        }

        trace!("replacing read receipts with the new ones");
        remote_prev_event_item.read_receipts = read_receipts;
        self.items.replace_read_receipts(
            prev_item_pos,
            TimelineItem::new(prev_event_item, prev_event_item_id),
        );
    }
}

impl<P: RoomDataProvider> TimelineStateTransaction<'_, P> {
    /// Get the visible receipt target from this transaction's current state.
    pub(super) fn latest_user_read_receipt_timeline_event_id(
        &self,
        user_id: &UserId,
    ) -> Option<OwnedEventId> {
        self.meta
            .read_receipts
            .latest_user_read_receipt_timeline_event_id(user_id, self.items.all_remote_events())
    }

    /// Populates our own latest read receipt in the in-memory by-user read
    /// receipt cache.
    pub(super) async fn populate_initial_user_receipt(
        &mut self,
        room_data_provider: &P,
        receipt_type: ReceiptType,
        recovering_from_lag: bool,
    ) {
        let own_user_id = room_data_provider.own_user_id().to_owned();

        let receipt_thread = self.focus.receipt_thread();
        let wants_unthreaded_receipts = receipt_thread == ReceiptThread::Unthreaded;

        let mut read_receipt = room_data_provider
            .load_user_receipt(receipt_type.clone(), receipt_thread, &own_user_id)
            .await;

        let uses_main_fallback = wants_unthreaded_receipts && read_receipt.is_none();
        if uses_main_fallback {
            // Fallback to the one in the main thread.
            read_receipt = room_data_provider
                .load_user_receipt(receipt_type.clone(), ReceiptThread::Main, &own_user_id)
                .await;
        }

        if let Some((event_id, receipt)) = read_receipt {
            self.meta.read_receipts.maybe_update_read_receipt(
                FullReceipt {
                    event_id: &event_id,
                    user_id: &own_user_id,
                    receipt_type,
                    receipt: &receipt,
                    target_is_pending: false,
                },
                true,
                recovering_from_lag,
                recovering_from_lag.then(|| {
                    if uses_main_fallback {
                        PendingReceiptSource::OwnMainFallback
                    } else {
                        PendingReceiptSource::Thread(receipt.thread.clone())
                    }
                }),
                &mut self.items,
            );
        }
    }
}

impl<P: RoomDataProvider> TimelineState<P> {
    /// Get the latest read receipt for the given user.
    ///
    /// Useful to get the latest read receipt, whether it's private or public.
    pub(super) async fn latest_user_read_receipt(
        &self,
        user_id: &UserId,
        receipt_thread: ReceiptThread,
        room_data_provider: &P,
    ) -> Option<(OwnedEventId, Receipt)> {
        let all_remote_events = self.items.all_remote_events();

        let public_read_receipt = self
            .meta
            .user_receipt(
                user_id,
                ReceiptType::Read,
                receipt_thread.clone(),
                room_data_provider,
                all_remote_events,
            )
            .await;

        let private_read_receipt = self
            .meta
            .user_receipt(
                user_id,
                ReceiptType::ReadPrivate,
                receipt_thread,
                room_data_provider,
                all_remote_events,
            )
            .await;

        // Let's assume that a private read receipt should be more recent than a public
        // read receipt (otherwise there's no point in the private read receipt),
        // and use it as the default.
        match self
            .meta
            .read_receipts
            .authoritative_pending_order(user_id, all_remote_events)
            .unwrap_or_else(|| {
                TimelineMetadata::compare_optional_receipts(
                    public_read_receipt.as_ref(),
                    private_read_receipt.as_ref(),
                    all_remote_events,
                )
            }) {
            Ordering::Greater => public_read_receipt,
            Ordering::Less => private_read_receipt,
            _ => unreachable!(),
        }
    }

    /// Get the ID of the visible timeline event with the latest read receipt
    /// for the given user.
    pub(super) fn latest_user_read_receipt_timeline_event_id(
        &self,
        user_id: &UserId,
    ) -> Option<OwnedEventId> {
        self.meta
            .read_receipts
            .latest_user_read_receipt_timeline_event_id(user_id, self.items.all_remote_events())
    }
}

impl TimelineMetadata {
    /// Get the latest receipt of the given type for the given user in the
    /// timeline.
    ///
    /// This will attempt to read the latest user receipt for a user from the
    /// cache, or load it from the storage if missing from the cache.
    ///
    /// If the `ReceiptThread` is `Unthreaded`, it will try to find either the
    /// unthreaded or the main-thread read receipt, to be maximally
    /// compatible with clients using one or the other. Otherwise, it will
    /// select only the receipts for that specific thread.
    pub(super) async fn user_receipt<P: RoomDataProvider>(
        &self,
        user_id: &UserId,
        receipt_type: ReceiptType,
        receipt_thread: ReceiptThread,
        room_data_provider: &P,
        all_remote_events: &AllRemoteEvents,
    ) -> Option<(OwnedEventId, Receipt)> {
        if let Some(receipt) = self.read_receipts.get_latest(user_id, &receipt_type) {
            // Since it is in the timeline, it should be the most recent.
            return Some(receipt.clone());
        }

        if receipt_thread == ReceiptThread::Unthreaded {
            // Maintain compatibility with clients using either the unthreaded and main read
            // receipts, and try to find the most recent one.
            let unthreaded_read_receipt = room_data_provider
                .load_user_receipt(receipt_type.clone(), ReceiptThread::Unthreaded, user_id)
                .await;

            let main_thread_read_receipt = room_data_provider
                .load_user_receipt(receipt_type.clone(), ReceiptThread::Main, user_id)
                .await;

            // Let's use the unthreaded read receipt as default, since it's the one we
            // should be using.
            match Self::compare_optional_receipts(
                main_thread_read_receipt.as_ref(),
                unthreaded_read_receipt.as_ref(),
                all_remote_events,
            ) {
                Ordering::Greater => main_thread_read_receipt,
                Ordering::Less => unthreaded_read_receipt,
                _ => unreachable!(),
            }
        } else {
            // In all the other cases, use the thread's read receipt. A main-thread receipt
            // in particular will use this code path, and not be compatible with
            // an unthreaded read receipt.
            room_data_provider
                .load_user_receipt(receipt_type.clone(), receipt_thread, user_id)
                .await
        }
    }

    /// Compares two optional receipts to know which one is more recent.
    ///
    /// Returns `Ordering::Greater` if the left-hand side is more recent than
    /// the right-hand side, and `Ordering::Less` if it is older. If it's
    /// not possible to know which one is the more recent, defaults to
    /// `Ordering::Less`, making the right-hand side the default.
    fn compare_optional_receipts(
        lhs: Option<&(OwnedEventId, Receipt)>,
        rhs_or_default: Option<&(OwnedEventId, Receipt)>,
        all_remote_events: &AllRemoteEvents,
    ) -> Ordering {
        // If we only have one, use it.
        let Some((lhs_event_id, lhs_receipt)) = lhs else {
            return Ordering::Less;
        };
        let Some((rhs_event_id, rhs_receipt)) = rhs_or_default else {
            return Ordering::Greater;
        };

        // Compare by position in the timeline.
        if let Some(relative_pos) =
            Self::compare_events_positions(lhs_event_id, rhs_event_id, all_remote_events)
        {
            if relative_pos == RelativePosition::Before {
                return Ordering::Greater;
            }

            return Ordering::Less;
        }

        // Compare by timestamp.
        if let Some((lhs_ts, rhs_ts)) = lhs_receipt.ts.zip(rhs_receipt.ts) {
            if lhs_ts > rhs_ts {
                return Ordering::Greater;
            }

            return Ordering::Less;
        }

        Ordering::Less
    }
}

#[cfg(test)]
mod receipt_search_tests {
    use matrix_sdk_test::BOB;
    use ruma::{event_id, events::receipt::Receipt};

    use super::{FullReceipt, ReadReceipts, ReceiptType};
    use crate::timeline::controller::{ObservableItems, metadata::EventMeta};

    #[test]
    fn pending_target_receipt_is_loaded_by_item_creation_without_missing_item_search() {
        let event_id = event_id!("$pending-receipt-target");
        let mut items = ObservableItems::new();
        let mut items = items.transaction();
        items.push_back_remote_event(EventMeta::new(event_id.to_owned(), true, true, None));
        let mut receipts = ReadReceipts::default();
        let receipt = Receipt::default();

        receipts.maybe_update_read_receipt(
            FullReceipt {
                event_id,
                user_id: *BOB,
                receipt_type: ReceiptType::Read,
                receipt: &receipt,
                target_is_pending: true,
            },
            false,
            false,
            None,
            &mut items,
        );

        let rendered_receipts = receipts.compute_event_receipts(event_id, &mut items, true);
        let rendered = rendered_receipts.get(*BOB).expect("new item carries the pending receipt");
        assert_eq!(rendered.ts, receipt.ts);
        assert_eq!(rendered.thread, receipt.thread);
        assert_eq!(receipts.get_latest(*BOB, &ReceiptType::Read).unwrap().0, event_id);
        let counts = receipts.search_counts();
        assert_eq!(counts.position_event_visits, 1);
        assert_eq!(counts.deferred_item_adds, 1);
        assert_eq!(counts.fallback_item_visits, 0);
        assert_eq!(counts.compute_event_visits, 0);
        assert_eq!(counts.missing_item_targets, 0);

        receipts.clear();
        assert_eq!(receipts.search_counts(), counts, "numeric diagnostics survive rebuild clears");
    }

    #[test]
    fn unexpected_missing_explicit_receipt_item_remains_diagnosed() {
        let event_id = event_id!("$unexpectedly-missing-receipt-target");
        let mut items = ObservableItems::new();
        let mut items = items.transaction();
        // Model a broken mapping. Unlike a pending insertion, this does not
        // opt into deferral and must retain the fallback/inconsistency path.
        items.push_back_remote_event(EventMeta::new(event_id.to_owned(), true, true, None));
        let mut receipts = ReadReceipts::default();
        receipts.maybe_update_read_receipt(
            FullReceipt {
                event_id,
                user_id: *BOB,
                receipt_type: ReceiptType::Read,
                receipt: &Receipt::default(),
                target_is_pending: false,
            },
            false,
            false,
            None,
            &mut items,
        );
        assert_eq!(receipts.search_counts().deferred_item_adds, 0);
        assert_eq!(receipts.search_counts().missing_item_targets, 1);
    }
}
