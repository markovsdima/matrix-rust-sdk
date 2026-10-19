// Copyright 2023 The Matrix.org Foundation C.I.C.
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

use std::sync::Arc;

use eyeball_im::VectorDiff;
use matrix_sdk::{deserialized_responses::TimelineEvent, send_queue::SendHandle};
use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedTransactionId, OwnedUserId,
    events::{
        AnyMessageLikeEventContent, AnySyncEphemeralRoomEvent,
        receipt::{ReceiptEventContent, ReceiptType},
    },
    room_version_rules::RoomVersionRules,
    serde::Raw,
    time::Instant,
};
use tracing::{instrument, trace, warn};

use super::{
    super::{
        Profile,
        date_dividers::DateDividerAdjuster,
        event_handler::{Flow, TimelineAction, TimelineEventContext, TimelineEventHandler},
        event_item::RemoteEventOrigin,
        traits::RoomDataProvider,
    },
    DateDividerMode, TimelineMetadata, TimelineSettings, TimelineStateTransaction,
    metadata::ReceiptReconciliationCounts,
    observable_items::ObservableItems,
};
use crate::{timeline::controller::TimelineFocusKind, unable_to_decrypt_hook::UtdHookManager};

#[derive(Debug)]
pub(in crate::timeline) struct TimelineState<P: RoomDataProvider> {
    pub items: ObservableItems,
    pub meta: TimelineMetadata,

    /// The kind of focus of this timeline.
    pub(super) focus: Arc<TimelineFocusKind>,

    /// Phantom data for the room data provider.
    _phantom: std::marker::PhantomData<P>,
}

impl<P: RoomDataProvider> TimelineState<P> {
    pub(super) fn new(
        focus: Arc<TimelineFocusKind>,
        own_user_id: OwnedUserId,
        room_version_rules: RoomVersionRules,
        internal_id_prefix: Option<String>,
        unable_to_decrypt_hook: Option<Arc<UtdHookManager>>,
        is_room_encrypted: bool,
    ) -> Self {
        let mut meta = TimelineMetadata::new(
            own_user_id,
            room_version_rules,
            internal_id_prefix,
            unable_to_decrypt_hook,
            is_room_encrypted,
        );
        meta.aggregations =
            super::Aggregations::for_focus(!matches!(&*focus, TimelineFocusKind::Live { .. }));
        Self { items: ObservableItems::new(), meta, focus, _phantom: std::marker::PhantomData }
    }

    /// Handle updates on events as [`VectorDiff`]s.
    pub(super) async fn handle_remote_events_with_diffs(
        &mut self,
        diffs: Vec<VectorDiff<TimelineEvent>>,
        origin: RemoteEventOrigin,
        room_data: &P,
        settings: &TimelineSettings,
    ) {
        if diffs.is_empty() {
            return;
        }

        let mut transaction = self.transaction();
        transaction.handle_remote_events_with_diffs(diffs, origin, room_data, settings).await;
        transaction.commit();
    }

    /// Handle remote aggregations on events as [`VectorDiff`]s.
    pub(super) async fn handle_remote_aggregations(
        &mut self,
        diffs: Vec<VectorDiff<TimelineEvent>>,
        origin: RemoteEventOrigin,
        room_data: &P,
        settings: &TimelineSettings,
    ) {
        if diffs.is_empty() {
            return;
        }

        let mut transaction = self.transaction();
        transaction.handle_remote_aggregations(diffs, origin, room_data, settings).await;
        transaction.commit();
    }

    /// Marks the given event as fully read, using the read marker received from
    /// sync.
    pub(super) fn handle_fully_read_marker(&mut self, fully_read_event_id: OwnedEventId) {
        let mut txn = self.transaction();
        txn.set_fully_read_event(fully_read_event_id);
        txn.commit();
    }

    #[instrument(skip_all)]
    pub(super) async fn handle_ephemeral_events(
        &mut self,
        events: Vec<Raw<AnySyncEphemeralRoomEvent>>,
        room_data_provider: &P,
    ) -> Option<ReceiptReconciliationCounts> {
        if events.is_empty() {
            return None;
        }

        let mut txn = self.transaction();
        let mut reconciliation = ReceiptReconciliationCounts::default();

        trace!("Handling ephemeral room events");
        let own_user_id = room_data_provider.own_user_id();
        for raw_event in events {
            match raw_event.deserialize() {
                Ok(AnySyncEphemeralRoomEvent::Receipt(ev)) => {
                    let needs_reconciliation =
                        txn.meta.control_recovery.as_ref().is_some_and(|recovery| {
                            ev.content.0.values().any(|types| {
                                types.iter().any(|(receipt_type, receipts)| {
                                    receipts.iter().any(|(user_id, receipt)| {
                                        !recovery.acknowledged_receipts.contains(&(
                                            user_id.clone(),
                                            receipt_type.clone(),
                                            receipt.thread.clone(),
                                        ))
                                    })
                                })
                            })
                        });
                    // Once every key in this EDU is acknowledged, retain the
                    // normal whole-EDU path without per-receipt maps or I/O.
                    if !needs_reconciliation {
                        txn.handle_explicit_read_receipts(ev.content, own_user_id, false, false);
                        continue;
                    }
                    for (event_id, types) in ev.content.0 {
                        for (receipt_type, receipts) in types {
                            for (user_id, receipt) in receipts {
                                let key =
                                    (user_id.clone(), receipt_type.clone(), receipt.thread.clone());
                                let needs_reconciliation =
                                    txn.meta.control_recovery.as_ref().is_some_and(|recovery| {
                                        !recovery.acknowledged_receipts.contains(&key)
                                    });
                                let mut from_store = false;
                                let (current_event_id, current_receipt) = if needs_reconciliation {
                                    let started = Instant::now();
                                    let stored = room_data_provider
                                        .load_user_receipt(
                                            receipt_type.clone(),
                                            receipt.thread.clone(),
                                            &user_id,
                                        )
                                        .await;
                                    reconciliation.store_reads =
                                        reconciliation.store_reads.saturating_add(1);
                                    reconciliation.store_read_us =
                                        reconciliation.store_read_us.saturating_add(
                                            u64::try_from(started.elapsed().as_micros())
                                                .unwrap_or(u64::MAX),
                                        );
                                    if stored.as_ref().is_some_and(|(id, value)| {
                                        id == &event_id
                                            && value.ts == receipt.ts
                                            && value.thread == receipt.thread
                                    }) {
                                        reconciliation.matched =
                                            reconciliation.matched.saturating_add(1);
                                        // This key has caught up. In particular, a fresh receipt
                                        // outside the loaded range must use ordinary live rules,
                                        // not the authoritative-store exception used by rebuild.
                                        if let Some(recovery) = txn.meta.control_recovery.as_mut() {
                                            recovery.acknowledged_receipts.insert(key);
                                        }
                                        txn.meta.read_receipts.acknowledge_recovery_receipt(
                                            &user_id,
                                            &receipt_type,
                                            &receipt.thread,
                                        );
                                        (event_id.clone(), receipt)
                                    } else if let Some(stored) = stored {
                                        reconciliation.differed =
                                            reconciliation.differed.saturating_add(1);
                                        from_store = true;
                                        stored
                                    } else {
                                        reconciliation.missing =
                                            reconciliation.missing.saturating_add(1);
                                        // No authoritative observation: use live rules,
                                        // but don't claim to have reached a store boundary.
                                        (event_id.clone(), receipt)
                                    }
                                } else {
                                    (event_id.clone(), receipt)
                                };
                                let mut current = ReceiptEventContent(Default::default());
                                current
                                    .0
                                    .entry(current_event_id)
                                    .or_default()
                                    .entry(receipt_type.clone())
                                    .or_default()
                                    .insert(user_id, current_receipt);
                                txn.handle_explicit_read_receipts(
                                    current,
                                    own_user_id,
                                    from_store,
                                    from_store,
                                );
                            }
                        }
                    }
                }
                Ok(_) => {}
                Err(e) => {
                    let event_type = raw_event.get_field::<String>("type").ok().flatten();
                    warn!(event_type, "Failed to deserialize ephemeral event: {e}");
                }
            }
        }

        let reconciliation = (reconciliation.store_reads > 0).then_some(reconciliation);
        if let (Some(batch), Some(recovery)) = (reconciliation, txn.meta.control_recovery.as_mut())
        {
            recovery.receipt_reconciliation.add(batch);
        }
        txn.commit();
        reconciliation
    }

    /// Adds a local echo (for an event) to the timeline.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    pub(super) async fn handle_local_event(
        &mut self,
        own_user_id: OwnedUserId,
        own_profile: Option<Profile>,
        date_divider_mode: DateDividerMode,
        txn_id: OwnedTransactionId,
        send_handle: Option<SendHandle>,
        content: AnyMessageLikeEventContent,
    ) {
        let mut txn = self.transaction();

        let mut date_divider_adjuster = DateDividerAdjuster::new(date_divider_mode);

        let is_thread_focus = txn.focus.is_thread();
        let (in_reply_to, thread_root) =
            txn.meta.process_content_relations(&content, None, &txn.items, is_thread_focus);

        // TODO merge with other should_add, one way or another?
        let should_add_new_items = match &txn.focus {
            TimelineFocusKind::Live { hide_threaded_events } => {
                thread_root.is_none() || !hide_threaded_events
            }
            TimelineFocusKind::Thread { root_event_id, .. } => {
                thread_root.as_ref().is_some_and(|r| r == root_event_id)
            }
            TimelineFocusKind::Event { .. } | TimelineFocusKind::PinnedEvents => {
                // Don't add new items to these timelines; aggregations are added independently
                // of the `should_add_new_items` value.
                false
            }
        };

        let ctx = TimelineEventContext {
            sender: own_user_id,
            sender_profile: own_profile,
            forwarder: None,
            forwarder_profile: None,
            timestamp: MilliSecondsSinceUnixEpoch::now(),
            read_receipts: Default::default(),
            // An event sent by ourselves is never matched against push rules.
            is_highlighted: false,
            flow: Flow::Local { txn_id, send_handle },
            should_add_new_items,
        };

        let timeline_action = TimelineAction::from_content(content, in_reply_to, thread_root, None);
        TimelineEventHandler::new(&mut txn, ctx)
            .handle_event(&mut date_divider_adjuster, timeline_action, None)
            .await;
        txn.adjust_date_dividers(date_divider_adjuster);

        txn.commit();
    }

    #[cfg(test)]
    pub(super) fn handle_read_receipts(
        &mut self,
        receipt_event_content: ReceiptEventContent,
        own_user_id: &ruma::UserId,
    ) {
        let mut txn = self.transaction();
        txn.handle_explicit_read_receipts(receipt_event_content, own_user_id, false, false);
        txn.commit();
    }

    pub(super) fn clear(&mut self) {
        let mut txn = self.transaction();
        txn.clear();
        txn.commit();
    }

    /// Replaces the existing events in the timeline with the given remote ones.
    ///
    /// Note: when the `position` is [`TimelineEnd::Front`], prepended events
    /// should be ordered in *reverse* topological order, that is, `events[0]`
    /// is the most recent.
    pub(super) async fn replace_with_remote_events<Events>(
        &mut self,
        events: Events,
        origin: RemoteEventOrigin,
        room_data_provider: &P,
        settings: &TimelineSettings,
    ) where
        Events: IntoIterator,
        Events::Item: Into<TimelineEvent>,
    {
        let mut txn = self.transaction();
        txn.clear();
        txn.handle_remote_events_with_diffs(
            vec![VectorDiff::Append { values: events.into_iter().map(Into::into).collect() }],
            origin,
            room_data_provider,
            settings,
        )
        .await;
        // Reconcile stored own receipts with implicit receipts from this
        // snapshot. Initial loading uses ordinary live ordering: a known
        // implicit target may take precedence over an unloaded stored target.
        // Lag recovery restores unknown targets separately with protection.
        if settings.track_read_receipts.is_enabled() {
            txn.populate_initial_user_receipt(room_data_provider, ReceiptType::Read, false).await;
            txn.populate_initial_user_receipt(room_data_provider, ReceiptType::ReadPrivate, false)
                .await;
        }
        txn.commit();
    }

    pub(super) fn mark_all_events_as_encrypted(&mut self) {
        // When this transaction finishes, all items in the timeline will be emitted
        // again with the updated encryption value.
        let mut txn = self.transaction();
        txn.mark_all_events_as_encrypted();
        txn.commit();
    }

    pub(super) fn transaction(&mut self) -> TimelineStateTransaction<'_, P> {
        TimelineStateTransaction::new(&mut self.items, &mut self.meta, &self.focus)
    }
}

#[cfg(test)]
mod tests {
    use matrix_sdk::test_utils::mocks::MatrixMockServer;
    use matrix_sdk_test::{BOB, CAROL, JoinedRoomBuilder, async_test, event_factory::EventFactory};
    use ruma::{event_id, events::receipt::ReceiptThread, room_id};

    use super::*;
    use crate::timeline::{
        TimelineFocus, TimelineReadReceiptTracking, controller::TimelineController,
    };

    async fn recovery_counts(
        controller: &TimelineController,
    ) -> (u64, ReceiptReconciliationCounts) {
        let state = controller.state.read().await;
        let recovery = state.meta.control_recovery.as_ref().unwrap();
        (recovery.generation, recovery.receipt_reconciliation)
    }

    #[async_test]
    async fn test_recovery_receipt_diagnostics_follow_reads_and_recovery_epochs() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room_id = room_id!("!receipt-diagnostics:example.org");
        let room = server.sync_room(&client, JoinedRoomBuilder::new(room_id)).await;
        let factory = EventFactory::new().room(room_id).sender(*CAROL);
        let a = event_id!("$receipt-diagnostics-a");
        let b = event_id!("$receipt-diagnostics-b");
        let controller = TimelineController::new(
            room,
            TimelineFocus::Live { hide_threaded_events: false },
            None,
            None,
            false,
            TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            },
        );
        let snapshot: Vec<TimelineEvent> = [a, b]
            .into_iter()
            .map(|id| factory.text_msg("receipt target").event_id(id).into_event())
            .collect();
        let receipt = |id| {
            factory
                .read_receipts()
                .add_with_timestamp(id, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
                .into_event()
        };
        controller
            .replace_with_initial_remote_events(snapshot.clone(), RemoteEventOrigin::Cache)
            .await;
        controller.handle_ephemeral_events(vec![receipt(a).into_raw()]).await;
        assert!(controller.state.read().await.meta.control_recovery.is_none());

        controller
            .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
            .await;
        assert_eq!(recovery_counts(&controller).await, (1, Default::default()));

        // Missing store values are not an acknowledgement. Two EDUs in one
        // input batch must count two reads, not one key or one log record.
        controller
            .handle_ephemeral_events(vec![receipt(a).into_raw(), receipt(a).into_raw()])
            .await;
        let (_, missing) = recovery_counts(&controller).await;
        assert_eq!(
            (missing.store_reads, missing.matched, missing.differed, missing.missing),
            (2, 0, 0, 2)
        );
        assert_eq!(controller.latest_user_read_receipt(*BOB).await.unwrap().0, a);

        // Persisted B is ahead of queued A. It stays authoritative, but every
        // stale delivery still needs another read until B itself arrives.
        server.sync_room(&client, JoinedRoomBuilder::new(room_id).add_receipt(receipt(b))).await;
        for _ in 0..2 {
            controller.handle_ephemeral_events(vec![receipt(a).into_raw()]).await;
        }
        let (_, ahead) = recovery_counts(&controller).await;
        assert_eq!((ahead.store_reads, ahead.matched, ahead.differed, ahead.missing), (4, 0, 2, 2));
        assert!(ahead.store_read_us >= missing.store_read_us);
        assert_eq!(controller.latest_user_read_receipt(*BOB).await.unwrap().0, b);

        controller.handle_ephemeral_events(vec![receipt(b).into_raw()]).await;
        let acknowledged = recovery_counts(&controller).await;
        let (_, totals) = acknowledged;
        assert_eq!(
            (totals.store_reads, totals.matched, totals.differed, totals.missing),
            (5, 1, 2, 2)
        );
        assert!(totals.store_read_us >= ahead.store_read_us);
        controller.handle_ephemeral_events(vec![receipt(a).into_raw()]).await;
        assert_eq!(recovery_counts(&controller).await, acknowledged);
        assert_eq!(controller.latest_user_read_receipt(*BOB).await.unwrap().0, b);

        // Ordinary Clear preserves acknowledgements and diagnostics; a new
        // rebuild advances the epoch and starts fresh totals, even after Clear.
        controller.clear().await;
        controller.handle_ephemeral_events(vec![receipt(b).into_raw()]).await;
        assert_eq!(recovery_counts(&controller).await, acknowledged);
        for generation in [2, 3] {
            controller
                .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
                .await;
            assert_eq!(recovery_counts(&controller).await, (generation, Default::default()));
            controller.handle_ephemeral_events(vec![receipt(b).into_raw()]).await;
            let (actual_generation, fresh) = recovery_counts(&controller).await;
            assert_eq!(actual_generation, generation);
            assert_eq!(
                (fresh.store_reads, fresh.matched, fresh.differed, fresh.missing),
                (1, 1, 0, 0)
            );
            assert_eq!(controller.latest_user_read_receipt(*BOB).await.unwrap().0, b);
        }
    }
}
