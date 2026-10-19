// Copyright 2026 The Matrix.org Foundation C.I.C.
// SPDX-License-Identifier: Apache-2.0

//! Regression tests against the production room-cache subscriber task.
//! A lag recovery must replace the snapshot and its subscription together.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::{Duration, Instant},
};

use eyeball_im::VectorDiff;
use futures_util::{FutureExt, StreamExt};
use matrix_sdk::{
    Client,
    deserialized_responses::TimelineEvent,
    event_cache::{EventCacheDropHandles, RoomEventCache, RoomEventCacheSubscriber},
    test_utils::mocks::{MatrixMockServer, RoomMessagesResponseTemplate},
};
use matrix_sdk_test::{ALICE, BOB, JoinedRoomBuilder, event_factory::EventFactory};
use ruma::{
    EventId, OwnedEventId, TransactionId, event_id,
    events::{
        AnySyncMessageLikeEvent, AnySyncTimelineEvent,
        receipt::{ReceiptThread, ReceiptType},
        room::{
            member::MembershipState,
            message::{
                MessageType, RedactedRoomMessageEventContent, RoomMessageEventContent,
                SyncRoomMessageEvent,
            },
        },
    },
    mxc_uri, room_id,
};
use tokio::{
    sync::oneshot,
    task::JoinHandle,
    time::{sleep, timeout},
};

use super::{TimelineController, TimelineSettings};
use crate::timeline::{
    TimelineDetails, TimelineFocus, TimelineItem, TimelineReadReceiptTracking,
    event_item::RemoteEventOrigin, subscriber::TimelineSubscriber,
    tasks::room_event_cache_updates_task,
};

#[matrix_sdk_test::async_test]
async fn test_live_rebuild_preserves_pending_aggregations_and_reconciles_remote_echo() {
    live_pending_aggregations(true).await;
}

#[matrix_sdk_test::async_test]
async fn test_live_pending_aggregations_remote_echo_without_rebuild() {
    live_pending_aggregations(false).await;
}

async fn live_pending_aggregations(recovering: bool) {
    use crate::timeline::EventSendState;
    use ruma::events::room::message::RoomMessageEventContentWithoutRelation;
    for (sent, sync_first, with_txn) in
        [(false, true, true), (false, true, false), (false, false, true), (true, false, true)]
    {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room_id = room_id!("!live-pending:example.org");
        let room = server.sync_room(&client, JoinedRoomBuilder::new(room_id)).await;
        let factory = EventFactory::new().room(room_id).sender(client.user_id().unwrap());
        let edit_target = event_id!("$edit-target");
        let reaction_target = event_id!("$reaction-target");
        let redact_target = event_id!("$redact-target");
        let mut snapshot: Vec<TimelineEvent> = [edit_target, reaction_target, redact_target]
            .into_iter()
            .map(|id| factory.text_msg("original").event_id(id).into_event())
            .collect();
        let controller = TimelineController::new(
            room,
            TimelineFocus::Live { hide_threaded_events: false },
            None,
            None,
            false,
            TimelineSettings::default(),
        );
        controller
            .replace_with_initial_remote_events(snapshot.clone(), RemoteEventOrigin::Cache)
            .await;
        let edit_txn = TransactionId::new();
        let reaction_txn = TransactionId::new();
        let redact_txn = TransactionId::new();
        let content = factory
            .text_msg("* pending edit")
            .edit(edit_target, RoomMessageEventContentWithoutRelation::text_plain("pending edit"))
            .into_raw_timeline()
            .get_field::<RoomMessageEventContent>("content")
            .unwrap()
            .unwrap();
        controller.handle_local_event(edit_txn.clone(), content.into(), None).await;
        controller
            .handle_local_event(
                reaction_txn.clone(),
                ruma::events::reaction::ReactionEventContent::new(
                    ruma::events::relation::Annotation::new(
                        reaction_target.to_owned(),
                        "pending".into(),
                    ),
                )
                .into(),
                None,
            )
            .await;
        controller.handle_local_redaction(redact_txn.clone(), redact_target.to_owned()).await;
        if sent {
            controller
                .update_event_send_state(
                    &edit_txn,
                    EventSendState::Sent { event_id: event_id!("$confirmed-edit").to_owned() },
                )
                .await;
        }
        for _ in 0..2 {
            let (initial, output) = controller.subscribe().await;
            if recovering {
                controller
                    .replace_remote_events_after_cache_lag(
                        snapshot.clone(),
                        RemoteEventOrigin::Cache,
                    )
                    .await;
            }
            let items = controller.items().await;
            let event = |id| {
                items
                    .iter()
                    .filter_map(|i| i.as_event())
                    .find(|e| e.event_id() == Some(id))
                    .unwrap()
            };
            assert_eq!(event(edit_target).content().as_message().unwrap().body(), "pending edit");
            assert!(event(reaction_target).content().reactions().unwrap().contains_key("pending"));
            assert!(event(redact_target).content().is_redacted());
            assert_eq!(controller.state.read().await.items.all_remote_events().iter().count(), 3);
            assert_output_replays_to_current(&controller, initial, output).await;
        }
        // Cancellation after recovery must still find the original transaction
        // and undo the optimistic reaction/redaction.
        assert!(controller.discard_local_echo(&reaction_txn).await);
        assert!(controller.discard_local_echo(&redact_txn).await);
        let items = controller.items().await;
        assert!(
            !items
                .iter()
                .filter_map(|i| i.as_event())
                .find(|e| e.event_id() == Some(redact_target))
                .unwrap()
                .content()
                .is_redacted()
        );
        assert!(
            !items
                .iter()
                .filter_map(|i| i.as_event())
                .find(|e| e.event_id() == Some(reaction_target))
                .unwrap()
                .content()
                .reactions()
                .unwrap()
                .contains_key("pending")
        );
        if !sent && !sync_first {
            controller
                .update_event_send_state(
                    &edit_txn,
                    EventSendState::Sent { event_id: event_id!("$confirmed-edit").to_owned() },
                )
                .await;
        }
        let remote =
            factory.text_msg("* confirmed edit").event_id(event_id!("$confirmed-edit")).edit(
                edit_target,
                RoomMessageEventContentWithoutRelation::text_plain("confirmed edit"),
            );
        let remote = if with_txn { remote.unsigned_transaction_id(&edit_txn) } else { remote };
        let remote = remote.into_event();
        controller
            .handle_remote_events_with_diffs(
                vec![VectorDiff::PushBack { value: remote.clone() }],
                RemoteEventOrigin::Sync,
            )
            .await;
        snapshot.push(remote);
        if sync_first {
            controller
                .update_event_send_state(
                    &edit_txn,
                    EventSendState::Sent { event_id: event_id!("$confirmed-edit").to_owned() },
                )
                .await;
        }
        if recovering {
            controller
                .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
                .await;
        }
        let items = controller.items().await;
        let edited = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(edit_target))
            .unwrap();
        assert_eq!(edited.content().as_message().unwrap().body(), "confirmed edit");
        assert!(
            controller.state.read().await.meta.aggregations.pending_local_aggregations().is_empty()
        );
        // The same send/sync orderings must reconcile reactions and redactions,
        // not just edits. Their targets remain remote throughout recovery.
        let reaction_txn = TransactionId::new();
        let redact_txn = TransactionId::new();
        controller
            .handle_local_event(
                reaction_txn.clone(),
                ruma::events::reaction::ReactionEventContent::new(
                    ruma::events::relation::Annotation::new(
                        reaction_target.to_owned(),
                        "confirmed".into(),
                    ),
                )
                .into(),
                None,
            )
            .await;
        controller.handle_local_redaction(redact_txn.clone(), redact_target.to_owned()).await;
        let identities = [
            (&reaction_txn, event_id!("$confirmed-reaction")),
            (&redact_txn, event_id!("$confirmed-redaction")),
        ];
        if sent {
            for (txn, event_id) in identities {
                controller
                    .update_event_send_state(
                        txn,
                        EventSendState::Sent { event_id: event_id.to_owned() },
                    )
                    .await;
            }
        }
        if recovering {
            // Also retain pending effects whose targets temporarily leave the
            // cache's loaded window; they must apply when the targets return.
            controller
                .replace_remote_events_after_cache_lag(
                    Vec::<TimelineEvent>::new(),
                    RemoteEventOrigin::Cache,
                )
                .await;
            assert_eq!(
                controller.state.read().await.meta.aggregations.pending_local_aggregations().len(),
                2
            );
            controller
                .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
                .await;
        }
        if !sent && !sync_first {
            for (txn, event_id) in identities {
                controller
                    .update_event_send_state(
                        txn,
                        EventSendState::Sent { event_id: event_id.to_owned() },
                    )
                    .await;
            }
        }
        let reaction = factory
            .reaction(reaction_target, "confirmed")
            .event_id(event_id!("$confirmed-reaction"));
        let redaction =
            factory.redaction(redact_target).event_id(event_id!("$confirmed-redaction"));
        let reaction =
            if with_txn { reaction.unsigned_transaction_id(&reaction_txn) } else { reaction };
        let redaction =
            if with_txn { redaction.unsigned_transaction_id(&redact_txn) } else { redaction };
        let remote: Vec<TimelineEvent> = vec![reaction.into_event(), redaction.into_event()];
        controller
            .handle_remote_events_with_diffs(
                vec![VectorDiff::Append { values: remote.clone().into() }],
                RemoteEventOrigin::Sync,
            )
            .await;
        snapshot.extend(remote);
        if sync_first {
            for (txn, event_id) in identities {
                controller
                    .update_event_send_state(
                        txn,
                        EventSendState::Sent { event_id: event_id.to_owned() },
                    )
                    .await;
            }
        }
        if recovering {
            controller
                .replace_remote_events_after_cache_lag(snapshot, RemoteEventOrigin::Cache)
                .await;
        }
        let items = controller.items().await;
        let reaction = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(reaction_target))
            .unwrap();
        assert_eq!(reaction.content().reactions().unwrap()["confirmed"].len(), 1);
        let redacted = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(redact_target))
            .unwrap();
        assert!(redacted.content().is_redacted());
        assert!(redacted.unredacted_item.is_none());
        assert!(
            controller.state.read().await.meta.aggregations.pending_local_aggregations().is_empty()
        );
    }
}

#[matrix_sdk_test::async_test]
async fn test_latest_pending_edit_wins_after_previous_send_response() {
    use crate::timeline::EventSendState;
    use ruma::events::room::message::RoomMessageEventContentWithoutRelation;
    for focused in [false, true] {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room_id = room_id!("!pending-edit-order:example.org");
        let room = server.sync_room(&client, JoinedRoomBuilder::new(room_id)).await;
        let target = event_id!("$own-original");
        let factory = EventFactory::new().room(room_id).sender(client.user_id().unwrap());
        let snapshot: Vec<TimelineEvent> =
            vec![factory.text_msg("original").event_id(target).into_event()];
        let focus = if focused {
            TimelineFocus::Thread { root_event_id: target.to_owned() }
        } else {
            TimelineFocus::Live { hide_threaded_events: false }
        };
        let controller =
            TimelineController::new(room, focus, None, None, false, TimelineSettings::default());
        controller
            .replace_with_initial_remote_events(snapshot.clone(), RemoteEventOrigin::Cache)
            .await;
        for (index, body) in [(0, "first pending edit"), (1, "second pending edit")] {
            let txn = TransactionId::new();
            let content = factory
                .text_msg(format!("* {body}"))
                .edit(target, RoomMessageEventContentWithoutRelation::text_plain(body))
                .into_raw_timeline()
                .get_field::<RoomMessageEventContent>("content")
                .unwrap()
                .unwrap();
            controller.handle_local_event(txn.clone(), content.into(), None).await;
            let items = controller.items().await;
            let item = items
                .iter()
                .filter_map(|i| i.as_event())
                .find(|e| e.event_id() == Some(target))
                .unwrap();
            assert_eq!(
                item.content().as_message().unwrap().body(),
                body,
                "a previously sent edit must not hide the newer local edit"
            );
            controller
                .update_event_send_state(
                    &txn,
                    EventSendState::Sent {
                        event_id: EventId::parse(format!("$pending-edit-{index}")).unwrap(),
                    },
                )
                .await;
        }
        for _ in 0..2 {
            controller
                .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
                .await;
            let items = controller.items().await;
            let item = items
                .iter()
                .filter_map(|i| i.as_event())
                .find(|e| e.event_id() == Some(target))
                .unwrap();
            assert_eq!(item.content().as_message().unwrap().body(), "second pending edit");
        }
    }
}

#[derive(Default, Debug)]
struct Counts {
    operations: usize,
    item_occurrences: usize,
    virtual_occurrences: usize,
    adds: usize,
    repeat_adds: usize,
    unchanged_repeat_adds: usize,
    sets: usize,
    receipt_sets: usize,
    unchanged_sets: usize,
    clears: usize,
    resets: usize,
    removes: usize,
    seen: HashSet<String>,
    last_by_id: HashMap<String, String>,
    occurrences: HashMap<String, usize>,
    mirror: imbl::Vector<Arc<TimelineItem>>,
}

impl Counts {
    fn item(&mut self, item: &TimelineItem, is_set: bool, previous: Option<&TimelineItem>) {
        self.item_occurrences += 1;
        self.virtual_occurrences += usize::from(item.as_virtual().is_some());
        let Some(event) = item.as_event() else { return };
        let Some(id) = event.event_id() else { return };
        let id = id.to_string();
        // The fixture fixes sender, timestamp and plaintext content. This
        // fingerprint is not full equality for arbitrary SDK event objects.
        let fingerprint = format!(
            "{:?}/{:?}/{:?}/{:?}",
            event.event_id(),
            event.content(),
            event.sender_profile(),
            event.read_receipts()
        );
        if is_set {
            self.sets += 1;
            if let Some(previous) = previous.and_then(TimelineItem::as_event) {
                let receipt_changed = format!("{:?}", previous.read_receipts())
                    != format!("{:?}", event.read_receipts());
                self.receipt_sets += usize::from(receipt_changed);
                self.unchanged_sets += usize::from(
                    !receipt_changed
                        && previous.event_id() == event.event_id()
                        && format!("{:?}", previous.content()) == format!("{:?}", event.content())
                        && format!("{:?}", previous.sender_profile())
                            == format!("{:?}", event.sender_profile()),
                );
            }
        } else {
            self.adds += 1;
            self.repeat_adds += usize::from(self.seen.contains(&id));
            self.unchanged_repeat_adds +=
                usize::from(self.last_by_id.get(&id) == Some(&fingerprint));
        }
        *self.occurrences.entry(id.clone()).or_default() += 1;
        self.last_by_id.insert(id.clone(), fingerprint);
        self.seen.insert(id);
    }

    fn diff(&mut self, diff: VectorDiff<Arc<TimelineItem>>) {
        self.operations += 1;
        let apply = diff.clone();
        match diff {
            VectorDiff::Append { values } => {
                for value in values {
                    self.item(&value, false, None);
                }
            }
            VectorDiff::Reset { values } => {
                self.resets += 1;
                for value in values {
                    self.item(&value, false, None);
                }
            }
            VectorDiff::PushFront { value }
            | VectorDiff::PushBack { value }
            | VectorDiff::Insert { value, .. } => self.item(&value, false, None),
            VectorDiff::Set { index, value } => {
                let previous = self.mirror.get(index).cloned();
                self.item(&value, true, previous.as_deref());
            }
            VectorDiff::Clear => self.clears += 1,
            VectorDiff::Remove { .. } | VectorDiff::PopFront | VectorDiff::PopBack => {
                self.removes += 1;
            }
            VectorDiff::Truncate { .. } => {}
        }
        apply.apply(&mut self.mirror);
    }
}

#[matrix_sdk_test::async_test]
async fn test_cache_lag_recovery_does_not_replay_retained_prepends() {
    for history in [200usize, 1000] {
        for (receipts, attachments) in [(false, false), (true, false), (false, true)] {
            let server = MatrixMockServer::new().await;
            let client = server.client_builder().build().await;
            let room_id = room_id!("!lag-repro:example.org");
            client.event_cache().subscribe().unwrap();
            let room = server
                .sync_room(&client, JoinedRoomBuilder::new(room_id).set_timeline_prev_batch("seed"))
                .await;
            let (cache, _drop_handle) = room.event_cache().await.unwrap();
            // Keep the initial cache retained without running a UI listener.
            let (_, _keep_alive) = cache.subscribe().await.unwrap();
            let factory = EventFactory::new().room(room_id).sender(*ALICE);
            let make_event = |index: usize| {
                let message = if attachments && index.is_multiple_of(8) {
                    factory.image(
                        format!("image {index}"),
                        mxc_uri!("mxc://example.org/test").to_owned(),
                    )
                } else {
                    factory.text_msg(format!("message {index}"))
                };
                message
                    .event_id(&EventId::parse(format!("$lag-{index}")).unwrap())
                    .server_ts(1_700_000_000_000u64 + index as u64)
            };
            {
                let _mock = server
                    .mock_room_messages()
                    .ok(RoomMessagesResponseTemplate::default()
                        .end_token("page-0")
                        .events((40..history).rev().map(make_event).collect::<Vec<_>>()))
                    .mock_once()
                    .mount_as_scoped()
                    .await;
                cache.pagination().run_backwards_once(history as u16).await.unwrap();
            }

            let (initial, subscriber) = cache.subscribe().await.unwrap();
            assert_eq!(initial.len(), history - 40);
            let focus = TimelineFocus::Live { hide_threaded_events: false };
            let controller = TimelineController::new(
                room,
                focus.clone(),
                None,
                None,
                false,
                TimelineSettings {
                    track_read_receipts: if receipts {
                        TimelineReadReceiptTracking::AllEvents
                    } else {
                        TimelineReadReceiptTracking::Disabled
                    },
                    event_filter: Arc::new(move |event, rules| {
                        if attachments {
                            matches!(event, AnySyncTimelineEvent::MessageLike(
                                AnySyncMessageLikeEvent::RoomMessage(SyncRoomMessageEvent::Original(message)))
                                if matches!(message.content.msgtype, MessageType::Image(_)))
                        } else {
                            super::default_event_filter(event, rules)
                        }
                    }),
                    ..Default::default()
                },
            );
            controller.replace_with_initial_remote_events(initial, RemoteEventOrigin::Cache).await;
            controller.live_lazy_paginate_backwards(history as u16).await;
            let (initial_items, mut output) = controller.subscribe().await;
            // Drain independently: 32 immediately-ready cache updates may be
            // handled without yielding on the main current-thread runtime.
            // This prevents a second lag at the observable-vector boundary.
            let (stop_sender, mut stop_receiver) = oneshot::channel();
            let collector = std::thread::spawn(move || {
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap()
                    .block_on(async move {
                        let mut counts = Counts::default();
                        counts.diff(VectorDiff::Append { values: initial_items });
                        loop {
                            tokio::select! {
                                biased;
                                diff = output.next() => match diff {
                                    Some(diffs) => for diff in diffs { counts.diff(diff); },
                                    None => break,
                                },
                                _ = &mut stop_receiver => break,
                            }
                        }
                        counts
                    })
            });

            // Forty real /messages responses overflow the 32-message broadcast
            // while this subscriber is deliberately not polled. No timed race.
            for (page, index) in (0..40).rev().enumerate() {
                let _mock = server
                    .mock_room_messages()
                    .ok(RoomMessagesResponseTemplate::default()
                        .end_token(format!("page-{}", page + 1))
                        .events(vec![make_event(index)]))
                    .mock_once()
                    .mount_as_scoped()
                    .await;
                cache.pagination().run_backwards_once(1).await.unwrap();
            }
            assert_eq!(cache.events().await.unwrap().len(), history);
            assert_eq!(subscriber.len(), 40);
            assert_eq!(
                controller.newly_loaded_snapshot_events(&cache.events().await.unwrap()).await.len(),
                40,
                "recovery must retry decryption only for new metadata IDs, including filtered events"
            );
            let task = tokio::spawn(room_event_cache_updates_task(
                cache.clone(),
                controller.clone(),
                subscriber,
                focus,
            ));

            // Recovery takes the entire snapshot and discards pre-snapshot
            // updates. It must not replay the 32 retained prepends a second time.
            timeout(Duration::from_secs(10), async {
                loop {
                    let count =
                        controller.state.read().await.items.all_remote_events().iter().count();
                    if count == history {
                        break;
                    }
                    assert!(count <= history, "unexpected metadata count {count}");
                    sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("lag recovery did not finish");

            // A fresh channel update is a delivery fence: checking only the
            // rebuilt length could accidentally pass before stale diffs replay.
            let marker = EventId::parse(format!("$lag-{}", history - 10)).unwrap();
            server
                .sync_room(
                    &client,
                    JoinedRoomBuilder::new(room_id).add_account_data(factory.fully_read(&marker)),
                )
                .await;
            timeout(Duration::from_secs(10), async {
                loop {
                    if controller.state.read().await.meta.fully_read_event.as_ref() == Some(&marker)
                    {
                        break;
                    }
                    sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .expect("post-recovery marker was not delivered");
            assert!(
                controller
                    .newly_loaded_snapshot_events(&cache.events().await.unwrap())
                    .await
                    .is_empty(),
                "an unchanged snapshot must not schedule the entire history for redecryption"
            );
            task.abort();
            let _ = task.await;
            stop_sender.send(()).unwrap();
            let counts = collector.join().unwrap();
            let (current_public_items, _) = controller.subscribe().await;
            assert_eq!(
                counts.mirror.iter().map(|item| item.unique_id()).collect::<Vec<_>>(),
                current_public_items.iter().map(|item| item.unique_id()).collect::<Vec<_>>(),
                "ordered replay must match the public timeline snapshot"
            );
            let state = controller.state.read().await;
            let metadata = state.items.all_remote_events();
            let metadata_len = metadata.iter().count();
            let metadata_unique =
                metadata.iter().map(|event| &event.event_id).collect::<HashSet<_>>().len();
            let items = state.items.clone_items();
            let visible = items.iter().filter_map(|item| item.as_event()).collect::<Vec<_>>();
            let visible_unique =
                visible.iter().filter_map(|item| item.event_id()).collect::<HashSet<_>>().len();
            let mut occurrence_histogram = std::collections::BTreeMap::<usize, usize>::new();
            for count in counts.occurrences.values() {
                *occurrence_histogram.entry(*count).or_default() += 1;
            }
            println!(
                "CACHE_LAG_RECOVERY history={history} receipts={receipts} attachments={attachments} adds={} repeat_adds={} unchanged_repeat_adds={} sets={} receipt_sets={} unchanged_sets={} clear={} reset={} removes={} unique={} metadata={metadata_len} metadata_unique={metadata_unique} visible={} visible_unique={visible_unique} occurrence_histogram={occurrence_histogram:?}",
                counts.adds,
                counts.repeat_adds,
                counts.unchanged_repeat_adds,
                counts.sets,
                counts.receipt_sets,
                counts.unchanged_sets,
                counts.clears,
                counts.resets,
                counts.removes,
                counts.seen.len(),
                visible.len()
            );
            assert_eq!(counts.clears, 1);
            assert_eq!(counts.resets, 0);
            let expected_visible = if attachments { history / 8 } else { history };
            let initial_visible = if attachments { (history - 40) / 8 } else { history - 40 };
            assert_eq!(counts.seen.len(), expected_visible);
            assert_eq!(metadata_unique, history);
            assert_eq!(metadata_len, history);
            assert_eq!(visible.len(), expected_visible);
            assert_eq!(visible_unique, expected_visible);
            assert_eq!(counts.adds, initial_visible + expected_visible);
            assert_eq!(counts.repeat_adds, initial_visible);
            assert_eq!(counts.sets, 0, "rebuild does not publish transient receipt moves");
            assert_eq!(counts.receipt_sets, counts.sets);
            assert_eq!(counts.unchanged_sets, 0);
        }
    }
}

struct LagFixture {
    server: MatrixMockServer,
    client: Client,
    cache: RoomEventCache,
    _drop_handles: Arc<EventCacheDropHandles>,
    controller: TimelineController,
    subscriber: Option<RoomEventCacheSubscriber>,
    factory: EventFactory,
}

impl LagFixture {
    async fn new() -> Self {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room_id = room_id!("!lag-state:example.org");
        let factory = EventFactory::new().room(room_id).sender(*ALICE);
        client.event_cache().subscribe().unwrap();
        let room = server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(room_id)
                    .set_timeline_prev_batch("seed")
                    .add_state_event(
                        factory
                            .member(&ALICE)
                            .membership(MembershipState::Join)
                            .display_name("Alice initial"),
                    )
                    .add_state_event(
                        factory.member(&BOB).membership(MembershipState::Join).display_name("Bob"),
                    )
                    .add_state_event(
                        factory
                            .member(client.user_id().unwrap())
                            .membership(MembershipState::Join)
                            .display_name("Me initial"),
                    ),
            )
            .await;
        let (cache, drop_handles) = room.event_cache().await.unwrap();
        let (_, keep_alive) = cache.subscribe().await.unwrap();
        {
            let _mock = server
                .mock_room_messages()
                .ok(RoomMessagesResponseTemplate::default().end_token("initial").events(
                    (40..80)
                        .rev()
                        .map(|i| {
                            factory
                                .text_msg(format!("message {i}"))
                                .event_id(&lag_id(i))
                                .server_ts(1_700_000_000_000u64 + i as u64)
                        })
                        .collect::<Vec<_>>(),
                ))
                .mock_once()
                .mount_as_scoped()
                .await;
            cache.pagination().run_backwards_once(40).await.unwrap();
        }
        let (events, subscriber) = cache.subscribe().await.unwrap();
        drop(keep_alive);
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
        controller.replace_with_initial_remote_events(events, RemoteEventOrigin::Cache).await;
        controller.live_lazy_paginate_backwards(200).await;
        Self {
            server,
            client,
            cache,
            _drop_handles: drop_handles,
            controller,
            subscriber: Some(subscriber),
            factory,
        }
    }

    async fn paginate(&self, range: std::ops::Range<i64>) {
        for index in range.rev() {
            let _mock = self
                .server
                .mock_room_messages()
                .ok(RoomMessagesResponseTemplate::default()
                    .end_token(format!("page-{index}"))
                    .events(vec![
                        self.factory
                            .text_msg(format!("message {index}"))
                            .event_id(&lag_id(index))
                            .server_ts((1_700_000_000_000i64 + index) as u64),
                    ]))
                .mock_once()
                .mount_as_scoped()
                .await;
            self.cache.pagination().run_backwards_once(1).await.unwrap();
        }
    }

    async fn pause_next_rebuild(&self) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (reached_sender, reached) = oneshot::channel();
        let (resume, resume_receiver) = oneshot::channel();
        *self.controller.cache_rebuild_hook.lock().await = Some((reached_sender, resume_receiver));
        (reached, resume)
    }

    fn start(&mut self) -> JoinHandle<()> {
        tokio::spawn(room_event_cache_updates_task(
            self.cache.clone(),
            self.controller.clone(),
            self.subscriber.take().unwrap(),
            TimelineFocus::Live { hide_threaded_events: false },
        ))
    }

    async fn wait_count(&self, expected: usize) {
        timeout(Duration::from_secs(10), async {
            loop {
                let count =
                    self.controller.state.read().await.items.all_remote_events().iter().count();
                if count == expected {
                    break;
                }
                assert!(count <= expected, "unexpected duplicate events: {count} > {expected}");
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("timeline did not reach the expected event count");
    }

    async fn wait_marker(&self, marker: &EventId) {
        timeout(Duration::from_secs(10), async {
            loop {
                if self.controller.state.read().await.meta.fully_read_event.as_deref()
                    == Some(marker)
                {
                    break;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("post-recovery marker was not delivered");
    }

    async fn assert_order_matches_cache(&self) {
        let expected = self
            .cache
            .events()
            .await
            .unwrap()
            .iter()
            .map(|event| event.event_id().unwrap())
            .collect::<Vec<_>>();
        let actual = self
            .controller
            .state
            .read()
            .await
            .items
            .all_remote_events()
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>();
        assert_eq!(actual, expected, "every event must occur exactly once, in cache order");
        assert_eq!(actual.iter().collect::<HashSet<_>>().len(), actual.len());
    }

    async fn sync_controls(
        &self,
        marker: i64,
        own_private: i64,
        own_public: i64,
        bob_receipt: i64,
        alice_name: &str,
        own_name: &str,
    ) {
        let (_, mut observer) = self.cache.subscribe().await.unwrap();
        self.server
            .sync_room(
                &self.client,
                JoinedRoomBuilder::new(self.cache.room_id())
                    .add_receipt(
                        self.factory
                            .read_receipts()
                            .add(
                                &lag_id(bob_receipt),
                                &BOB,
                                ReceiptType::Read,
                                ReceiptThread::Unthreaded,
                            )
                            .add(
                                &lag_id(own_private),
                                self.client.user_id().unwrap(),
                                ReceiptType::ReadPrivate,
                                ReceiptThread::Unthreaded,
                            )
                            .add(
                                &lag_id(own_public),
                                self.client.user_id().unwrap(),
                                ReceiptType::Read,
                                ReceiptThread::Unthreaded,
                            )
                            .into_event(),
                    )
                    .add_account_data(self.factory.fully_read(&lag_id(marker)))
                    .add_state_event(
                        self.factory
                            .member(&ALICE)
                            .membership(MembershipState::Join)
                            .display_name(alice_name),
                    )
                    .add_state_event(
                        self.factory
                            .member(self.client.user_id().unwrap())
                            .membership(MembershipState::Join)
                            .display_name(own_name),
                    ),
            )
            .await;
        timeout(Duration::from_secs(10), async {
            while !matches!(observer.recv().await.unwrap(),
                matrix_sdk::event_cache::RoomEventCacheUpdate::MoveReadMarkerTo { event_id }
                    if event_id == lag_id(marker))
            {}
        })
        .await
        .expect("cache did not publish control updates");
    }
}

fn lag_id(index: i64) -> OwnedEventId {
    EventId::parse(format!("$lag-{index}")).unwrap()
}

async fn reached_snapshot(reached: oneshot::Receiver<()>) {
    timeout(Duration::from_secs(10), reached)
        .await
        .expect("rebuild did not take a snapshot")
        .expect("rebuild stopped before taking a snapshot");
}

async fn assert_output_replays_to_current(
    controller: &TimelineController,
    mut mirror: imbl::Vector<Arc<TimelineItem>>,
    mut output: TimelineSubscriber,
) {
    while let Some(Some(diffs)) = output.next().now_or_never() {
        for diff in diffs {
            diff.apply(&mut mirror);
        }
    }
    let (current, _) = controller.subscribe().await;
    assert_eq!(
        format!("{mirror:?}"),
        format!("{current:?}"),
        "ordered public diffs must reconstruct the current timeline, including updated fields"
    );
}

#[matrix_sdk_test::async_test]
async fn test_cache_lag_recovery_keeps_concurrent_sync_pagination_redaction_and_remote_echo() {
    let mut fixture = LagFixture::new().await;
    let (initial, output) = fixture.controller.subscribe().await;
    let txn = TransactionId::new();
    fixture
        .controller
        .handle_local_event(
            txn.clone(),
            RoomMessageEventContent::text_plain("pending echo").into(),
            None,
        )
        .await;
    let local_id = fixture
        .controller
        .items()
        .await
        .iter()
        .find(|item| item.as_event().is_some_and(|event| event.transaction_id() == Some(&txn)))
        .unwrap()
        .unique_id()
        .clone();
    fixture.paginate(0..40).await;
    let (reached, resume) = fixture.pause_next_rebuild().await;
    let task = fixture.start();
    reached_snapshot(reached).await;

    fixture.sync_controls(70, 69, 65, 60, "Alice queued", "Me queued").await;

    // These events are after the snapshot boundary but before it is installed.
    // Both positional pagination diffs and live redaction/echo updates must be
    // retained by the fresh receiver and applied exactly once afterwards.
    let remote_echo = event_id!("$remote-echo");
    fixture
        .server
        .sync_room(
            &fixture.client,
            JoinedRoomBuilder::new(fixture.cache.room_id())
                .add_timeline_event(
                    fixture
                        .factory
                        .text_msg("pending echo")
                        .sender(fixture.client.user_id().unwrap())
                        .event_id(remote_echo)
                        .unsigned_transaction_id(&txn),
                )
                .add_timeline_event(
                    fixture
                        .factory
                        .redaction(&lag_id(20))
                        .event_id(event_id!("$redaction-during-rebuild")),
                ),
        )
        .await;
    timeout(Duration::from_secs(10), async {
        while fixture.cache.events().await.unwrap().len() != 82 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    fixture.sync_controls(77, 74, 72, 71, "Alice queued latest", "Me queued latest").await;
    fixture.paginate(-1..0).await;
    resume.send(()).unwrap();
    fixture.wait_count(83).await;
    fixture.wait_marker(&lag_id(77)).await;
    fixture.assert_order_matches_cache().await;

    let items = fixture.controller.items().await;
    let echo = items
        .iter()
        .find(|item| item.as_event().is_some_and(|event| event.event_id() == Some(remote_echo)))
        .unwrap();
    assert_eq!(echo.unique_id(), &local_id, "local echo identity must survive recovery");
    assert!(!echo.as_event().unwrap().is_local_echo());
    assert_eq!(
        items
            .iter()
            .filter_map(|item| item.as_event())
            .filter(|event| event.event_id() == Some(remote_echo))
            .count(),
        1
    );
    assert!(
        items
            .iter()
            .filter_map(|item| item.as_event())
            .find(|event| event.event_id() == Some(lag_id(20).as_ref()))
            .unwrap()
            .content()
            .is_redacted()
    );
    assert_eq!(fixture.controller.latest_user_read_receipt(&BOB).await.unwrap().0, lag_id(71));
    for event in
        items.iter().filter_map(|item| item.as_event()).filter(|event| event.sender() == *ALICE)
    {
        assert!(matches!(event.sender_profile(), TimelineDetails::Ready(profile)
            if profile.display_name.as_deref() == Some("Alice queued latest")));
    }
    task.abort();
    let _ = task.await;
    assert_output_replays_to_current(&fixture.controller, initial, output).await;
}

#[matrix_sdk_test::async_test]
async fn test_cache_lag_recovery_repeated_lag_restores_receipts_marker_members_and_local_echo() {
    let mut fixture = LagFixture::new().await;
    let (initial, output) = fixture.controller.subscribe().await;
    let txn = TransactionId::new();
    fixture
        .controller
        .handle_local_event(
            txn.clone(),
            RoomMessageEventContent::text_plain("unsent local echo").into(),
            None,
        )
        .await;
    let local_id = fixture
        .controller
        .items()
        .await
        .iter()
        .find(|item| item.as_event().is_some_and(|event| event.transaction_id() == Some(&txn)))
        .unwrap()
        .unique_id()
        .clone();

    // All these control messages fall out of the bounded channel. They are not
    // part of cache.events(), so replaying only raw events cannot restore them.
    fixture
        .server
        .sync_room(
            &fixture.client,
            JoinedRoomBuilder::new(fixture.cache.room_id()).add_timeline_event(
                fixture
                    .factory
                    .redaction(&lag_id(45))
                    .event_id(event_id!("$redaction-before-rebuild")),
            ),
        )
        .await;
    fixture.sync_controls(70, 69, 65, 60, "Alice changed", "Me changed").await;
    fixture.paginate(0..40).await;
    let (first_reached, first_resume) = fixture.pause_next_rebuild().await;
    let task = fixture.start();
    reached_snapshot(first_reached).await;

    // Overflow the new receiver while the first rebuild is suspended. The next
    // recovery must advance the boundary again and restore the latest controls.
    fixture.sync_controls(75, 74, 72, 71, "Alice newest", "Me newest").await;
    fixture.paginate(-40..0).await;
    let (second_reached, second_resume) = fixture.pause_next_rebuild().await;
    first_resume.send(()).unwrap();
    reached_snapshot(second_reached).await;
    second_resume.send(()).unwrap();
    fixture.wait_count(121).await;
    fixture.wait_marker(&lag_id(75)).await;
    fixture.assert_order_matches_cache().await;

    assert_eq!(
        fixture
            .controller
            .latest_user_read_receipt(fixture.client.user_id().unwrap())
            .await
            .unwrap()
            .0,
        lag_id(74),
        "private receipt must survive metadata reset"
    );
    {
        let state = fixture.controller.state.read().await;
        let own_user_id = fixture.client.user_id().unwrap();
        assert_eq!(
            state.meta.read_receipts.get_latest(own_user_id, &ReceiptType::Read).unwrap().0,
            lag_id(72),
        );
        assert_eq!(
            state.meta.read_receipts.get_latest(own_user_id, &ReceiptType::ReadPrivate).unwrap().0,
            lag_id(74),
        );
    }
    assert_eq!(fixture.controller.latest_user_read_receipt(&BOB).await.unwrap().0, lag_id(71));
    let items = fixture.controller.items().await;
    assert!(
        items
            .iter()
            .filter_map(|item| item.as_event())
            .find(|event| event.event_id() == Some(lag_id(45).as_ref()))
            .unwrap()
            .content()
            .is_redacted(),
        "a redaction included in the snapshot must survive both rebuilds"
    );
    let local = items
        .iter()
        .find(|item| item.as_event().is_some_and(|event| event.transaction_id() == Some(&txn)))
        .unwrap();
    assert_eq!(local.unique_id(), &local_id);
    assert!(local.as_event().unwrap().is_local_echo());
    assert!(matches!(local.as_event().unwrap().sender_profile(), TimelineDetails::Ready(profile)
        if profile.display_name.as_deref() == Some("Me newest")));
    for event in
        items.iter().filter_map(|item| item.as_event()).filter(|event| event.sender() == *ALICE)
    {
        assert!(matches!(event.sender_profile(), TimelineDetails::Ready(profile)
            if profile.display_name.as_deref() == Some("Alice newest")));
    }
    let bob_receipt = items
        .iter()
        .filter_map(|item| item.as_event())
        .find(|event| event.event_id() == Some(lag_id(71).as_ref()))
        .unwrap();
    assert!(bob_receipt.read_receipts().contains_key(*BOB));
    let marker_index = items.iter().position(|item| item.is_read_marker()).unwrap();
    assert_eq!(items[marker_index - 1].as_event().unwrap().event_id(), Some(lag_id(75).as_ref()));

    // A final channel update proves that the second receiver is live as well.
    fixture
        .server
        .sync_room(
            &fixture.client,
            JoinedRoomBuilder::new(fixture.cache.room_id())
                .add_account_data(fixture.factory.fully_read(&lag_id(76))),
        )
        .await;
    fixture.wait_marker(&lag_id(76)).await;
    fixture.assert_order_matches_cache().await;
    task.abort();
    let _ = task.await;
    assert_output_replays_to_current(&fixture.controller, initial, output).await;
}

#[matrix_sdk_test::async_test]
async fn test_cache_lag_recovery_does_not_reapply_stale_control_payloads() {
    let mut fixture = LagFixture::new().await;
    let (mut mirror, mut output) = fixture.controller.subscribe().await;
    fixture.paginate(0..40).await;
    let (reached, resume) = fixture.pause_next_rebuild().await;
    let task = fixture.start();
    reached_snapshot(reached).await;

    // A is already in the snapshot. The latest persisted receipt/read marker B
    // points at an event absent from it: event order cannot reject A as stale
    // until B's timeline diff has been applied. Replaying A must not briefly
    // put a receipt or marker at A after restoring the newer persisted state.
    fixture.sync_controls(60, 60, 60, 60, "Alice queued", "Me queued").await;
    let latest = event_id!("$receipt-latest");
    let following = event_id!("$after-receipt-latest");
    fixture
        .server
        .sync_room(
            &fixture.client,
            JoinedRoomBuilder::new(fixture.cache.room_id())
                .add_timeline_event(
                    fixture
                        .factory
                        .text_msg("latest receipt target")
                        .event_id(latest)
                        .server_ts(1_700_000_000_080u64),
                )
                .add_timeline_event(
                    fixture
                        .factory
                        .text_msg("following message")
                        .event_id(following)
                        .server_ts(1_700_000_000_081u64),
                )
                .add_receipt(
                    fixture
                        .factory
                        .read_receipts()
                        .add(latest, &BOB, ReceiptType::Read, ReceiptThread::Unthreaded)
                        .add(
                            latest,
                            fixture.client.user_id().unwrap(),
                            ReceiptType::Read,
                            ReceiptThread::Unthreaded,
                        )
                        .add(
                            latest,
                            fixture.client.user_id().unwrap(),
                            ReceiptType::ReadPrivate,
                            ReceiptThread::Unthreaded,
                        )
                        .into_event(),
                )
                .add_account_data(fixture.factory.fully_read(latest)),
        )
        .await;
    timeout(Duration::from_secs(10), async {
        while fixture.cache.events().await.unwrap().len() != 82 {
            sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
    resume.send(()).unwrap();
    fixture.wait_count(82).await;
    fixture.wait_marker(latest).await;
    fixture.assert_order_matches_cache().await;
    task.abort();
    let _ = task.await;

    while let Some(Some(diffs)) = output.next().now_or_never() {
        for diff in diffs {
            diff.apply(&mut mirror);
            for event in mirror.iter().filter_map(|item| item.as_event()) {
                if event.read_receipts().contains_key(*BOB) {
                    assert_eq!(
                        event.event_id(),
                        Some(latest),
                        "stale queued receipt A must not reappear after recovery restored B"
                    );
                }
            }
            if let Some(marker) = mirror.iter().position(|item| item.is_read_marker()) {
                let preceding =
                    mirror.iter().take(marker).rev().find_map(|item| item.as_event()).unwrap();
                assert_eq!(
                    preceding.event_id(),
                    Some(latest),
                    "stale queued fully-read marker A must not reappear after recovery restored B"
                );
            }
        }
    }
    let items = fixture.controller.items().await;
    assert_eq!(format!("{mirror:?}"), format!("{items:?}"));
    assert_eq!(fixture.controller.latest_user_read_receipt(&BOB).await.unwrap().0, latest);
    assert_eq!(
        fixture
            .controller
            .latest_user_read_receipt(fixture.client.user_id().unwrap())
            .await
            .unwrap()
            .0,
        latest
    );
}

/// Directly measures the same production rebuild entry point with a stable
/// snapshot. Fixture construction, initial loading and subscriber replay are
/// outside the timed interval. Run identical fixture source against both builds:
/// `RUST_LOG=off cargo test --offline -p matrix-sdk-ui --lib
/// cache_rebuild_volume -- --ignored --nocapture --test-threads=1`.
#[matrix_sdk_test::async_test]
#[ignore = "diagnostic volume fixture, including a 17k-event rebuild"]
async fn test_cache_rebuild_volume() {
    for history in [1000usize, 17_000] {
        for (attachments, receipts) in [(false, true), (true, true), (true, false)] {
            measure_rebuild(history, attachments, receipts).await;
        }
    }
}

async fn measure_rebuild(history: usize, attachments: bool, receipts: bool) {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let room_id = room_id!("!rebuild-volume:example.org");
    let room = server.sync_room(&client, JoinedRoomBuilder::new(room_id)).await;
    let factory = EventFactory::new().room(room_id).sender(*ALICE);
    let snapshot = (0..history)
        .map(|index| {
            let message = if index.is_multiple_of(8) {
                factory
                    .image(format!("image {index}"), mxc_uri!("mxc://example.org/test").to_owned())
            } else {
                factory.text_msg(format!("message {index}"))
            };
            TimelineEvent::from_plaintext(
                message
                    .event_id(&lag_id(index as i64))
                    .server_ts(1_700_000_000_000u64 + index as u64)
                    .into_raw_sync(),
            )
        })
        .collect::<Vec<_>>();
    let controller = TimelineController::new(
        room,
        TimelineFocus::Live { hide_threaded_events: false },
        None,
        None,
        false,
        TimelineSettings {
            track_read_receipts: if receipts {
                TimelineReadReceiptTracking::AllEvents
            } else {
                TimelineReadReceiptTracking::Disabled
            },
            event_filter: Arc::new(move |event, rules| {
                if attachments {
                    matches!(event, AnySyncTimelineEvent::MessageLike(
                        AnySyncMessageLikeEvent::RoomMessage(SyncRoomMessageEvent::Original(message)))
                        if matches!(message.content.msgtype, MessageType::Image(_)))
                } else {
                    super::default_event_filter(event, rules)
                }
            }),
            ..Default::default()
        },
    );
    controller.replace_with_initial_remote_events(snapshot.clone(), RemoteEventOrigin::Cache).await;
    controller.live_lazy_paginate_backwards(u16::MAX).await;
    let (initial, mut output) = controller.subscribe().await;
    let mut counts = Counts { mirror: initial, ..Default::default() };
    let receipt_before = controller.state.read().await.meta.read_receipts.search_counts();
    let started = Instant::now();
    let producer =
        controller.replace_remote_events_after_cache_lag(snapshot, RemoteEventOrigin::Cache).await;
    let elapsed_us = started.elapsed().as_micros();
    let receipt_after = controller.state.read().await.meta.read_receipts.search_counts();
    while let Some(Some(diffs)) = output.next().now_or_never() {
        for diff in diffs {
            counts.diff(diff);
        }
    }
    let (current, _) = controller.subscribe().await;
    assert_eq!(format!("{:?}", counts.mirror), format!("{current:?}"));
    let expected_visible = if attachments { history.div_ceil(8) } else { history };
    let visible = current.iter().filter_map(|item| item.as_event()).collect::<Vec<_>>();
    assert_eq!(visible.len(), expected_visible);
    assert_eq!(
        visible.iter().filter_map(|item| item.event_id()).collect::<HashSet<_>>().len(),
        expected_visible
    );
    assert_eq!(controller.state.read().await.items.all_remote_events().iter().count(), history);
    println!(
        "CACHE_REBUILD_VOLUME {}",
        serde_json::json!({
            "history": history,
            "attachments": attachments,
            "receipt_tracking": receipts,
            "elapsed_us": elapsed_us,
            "receipt_position_event_visits": receipt_after.position_event_visits - receipt_before.position_event_visits,
            "receipt_fallback_item_visits": receipt_after.fallback_item_visits - receipt_before.fallback_item_visits,
            "receipt_compute_event_visits": receipt_after.compute_event_visits - receipt_before.compute_event_visits,
            "receipt_deferred_item_adds": receipt_after.deferred_item_adds - receipt_before.deferred_item_adds,
            "receipt_missing_item_targets": receipt_after.missing_item_targets - receipt_before.missing_item_targets,
            "producer_operations": producer.operations,
            "producer_item_occurrences": producer.item_occurrences,
            "subscriber_operations": counts.operations,
            "subscriber_item_occurrences": counts.item_occurrences,
            "subscriber_virtual_occurrences": counts.virtual_occurrences,
            "event_adds": counts.adds,
            "event_sets": counts.sets,
            "receipt_sets": counts.receipt_sets,
            "unchanged_sets": counts.unchanged_sets,
            "clears": counts.clears,
            "resets": counts.resets,
            "final_event_items": visible.len(),
            "final_virtual_items": current.len() - visible.len(),
        })
    );
}

#[matrix_sdk_test::async_test]
async fn test_cache_lag_recovery_preserves_edits_reactions_and_subscriber_skip() {
    let mut fixture = LagFixture::new().await;
    // Keep a nonzero lazy window. The coalesced producer's absolute positions
    // must still translate into a correct public positional replica.
    fixture.controller.state.read().await.meta.subscriber_skip_count.update(30, true);
    let (initial, output) = fixture.controller.subscribe().await;
    assert!(initial.len() < fixture.controller.items().await.len());
    fixture
        .server
        .sync_room(
            &fixture.client,
            JoinedRoomBuilder::new(fixture.cache.room_id())
                .add_timeline_event(
                    fixture
                        .factory
                        .text_msg(" * edited body")
                        .edit(&lag_id(75), MessageType::text_plain("edited body").into())
                        .event_id(event_id!("$edit-before-rebuild")),
                )
                .add_timeline_event(
                    fixture
                        .factory
                        .reaction(&lag_id(75), "👍")
                        .sender(*BOB)
                        .event_id(event_id!("$reaction-before-rebuild")),
                ),
        )
        .await;
    fixture.paginate(0..40).await;
    let task = fixture.start();
    fixture.wait_count(82).await;
    fixture
        .server
        .sync_room(
            &fixture.client,
            JoinedRoomBuilder::new(fixture.cache.room_id())
                .add_account_data(fixture.factory.fully_read(&lag_id(77))),
        )
        .await;
    fixture.wait_marker(&lag_id(77)).await;
    // Keep the cache listener alive during this assertion: dropping the last
    // subscriber is allowed to unload older cache chunks from memory.
    fixture.assert_order_matches_cache().await;
    task.abort();
    let _ = task.await;
    let items = fixture.controller.items().await;
    let edited = items
        .iter()
        .filter_map(|item| item.as_event())
        .find(|event| event.event_id() == Some(lag_id(75).as_ref()))
        .unwrap();
    assert_eq!(edited.content().as_message().unwrap().body(), "edited body");
    let reactions = edited.content().reactions().unwrap();
    assert_eq!(reactions.len(), 1);
    assert_eq!(reactions.get("👍").unwrap().len(), 1);
    assert!(fixture.controller.state.read().await.meta.subscriber_skip_count.get() > 0);
    assert_output_replays_to_current(&fixture.controller, initial, output).await;
}

#[matrix_sdk_test::async_test]
async fn test_cache_rebuild_does_not_replay_already_delivered_edits() {
    for attachments in [false, true] {
        for skip in [0, 2] {
            let server = MatrixMockServer::new().await;
            let client = server.client_builder().build().await;
            let room_id = room_id!("!already-edited:example.org");
            let room = server.sync_room(&client, JoinedRoomBuilder::new(room_id)).await;
            let factory = EventFactory::new().room(room_id).sender(*ALICE);
            let target = event_id!("$already-edited-image");
            let survivor = event_id!("$unchanged-image");
            let image = |id: &EventId| {
                factory
                    .image("image".into(), mxc_uri!("mxc://example.org/test").to_owned())
                    .event_id(id)
                    .server_ts(1_700_000_000_000u64)
                    .into_event()
            };
            let mut settings = TimelineSettings::default();
            if attachments {
                settings.event_filter = Arc::new(|event, _| {
                    matches!(event,
                    AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
                        SyncRoomMessageEvent::Original(message)))
                    if matches!(message.content.msgtype, MessageType::Image(_)))
                });
            }
            let controller = TimelineController::new(
                room,
                TimelineFocus::Live { hide_threaded_events: false },
                None,
                None,
                false,
                settings,
            );
            controller
                .replace_with_initial_remote_events(
                    vec![image(target), image(survivor)],
                    RemoteEventOrigin::Cache,
                )
                .await;
            let edit = factory
                .text_msg(" * now text")
                .edit(target, MessageType::text_plain("now text").into())
                .event_id(event_id!("$already-delivered-edit"))
                .into_event();
            // Deliver the live edit before subscribing to rebuild output. The
            // durable catalogue has already removed this attachment.
            controller
                .handle_remote_events_with_diffs(
                    vec![VectorDiff::PushBack { value: edit.clone() }],
                    RemoteEventOrigin::Sync,
                )
                .await;
            controller.live_lazy_paginate_backwards(u16::MAX).await;
            controller.state.read().await.meta.subscriber_skip_count.update(skip, true);
            let (mut replica, mut output) = controller.subscribe().await;
            let (mut full_replica, mut full_output) = controller.subscribe_filter_map(Some).await;
            let assert_no_old_image = |items: &imbl::Vector<Arc<TimelineItem>>| {
                for event in items.iter().filter_map(|item| item.as_event()) {
                    if event.event_id() == Some(target) {
                        assert!(
                            matches!(
                                event.content().as_message().unwrap().msgtype(),
                                MessageType::Text(_)
                            ),
                            "reinserted an obsolete image: attachments={attachments}, skip={skip}"
                        );
                    }
                }
            };
            assert_no_old_image(&full_replica);
            for _ in 0..2 {
                let counts = controller
                    .replace_remote_events_after_cache_lag(
                        vec![image(target), image(survivor), edit.clone()],
                        RemoteEventOrigin::Cache,
                    )
                    .await;
                // Validate every unbatched diff, including Append values: an
                // obsolete image followed by Set(text) is not a harmless no-op.
                while let Some(Some(diff)) = full_output.next().now_or_never() {
                    assert!(!matches!(diff, VectorDiff::Set { .. }), "redundant edit effect");
                    diff.apply(&mut full_replica);
                    assert_unique_replica(&full_replica);
                    assert_no_old_image(&full_replica);
                }
                while let Some(Some(diffs)) = output.next().now_or_never() {
                    for diff in diffs {
                        assert!(!matches!(diff, VectorDiff::Set { .. }));
                        diff.apply(&mut replica);
                        assert_unique_replica(&replica);
                        assert_no_old_image(&replica);
                    }
                }
                assert_eq!(counts.operations, 2);
                assert_eq!(counts.item_occurrences, full_replica.len() as u64);
                assert_eq!(format!("{full_replica:?}"), format!("{:?}", controller.items().await));
                let (current, _) = controller.subscribe().await;
                assert_eq!(format!("{replica:?}"), format!("{current:?}"));
            }
        }
    }
}

#[matrix_sdk_test::async_test]
async fn test_cache_rebuild_keeps_catalog_effects_for_hidden_and_skipped_attachments() {
    for change in ["raw_redaction", "redaction", "edit_to_text", "bundled_edit_reaction"] {
        for skip in [0, 2] {
            let server = MatrixMockServer::new().await;
            let client = server.client_builder().build().await;
            let room_id = room_id!("!rebuild-catalog:example.org");
            let room = server.sync_room(&client, JoinedRoomBuilder::new(room_id)).await;
            let factory = EventFactory::new().room(room_id).sender(*ALICE);
            let target = event_id!("$attachment-target");
            let survivor = event_id!("$attachment-survivor");
            let image = |id: &EventId| {
                factory
                    .image("image".into(), mxc_uri!("mxc://example.org/test").to_owned())
                    .event_id(id)
                    .server_ts(1_700_000_000_000u64)
                    .into_event()
            };
            let controller = TimelineController::new(
                room,
                TimelineFocus::Live { hide_threaded_events: false },
                None,
                None,
                false,
                TimelineSettings {
                    event_filter: Arc::new(|event, _| {
                        matches!(event,
                        AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
                            SyncRoomMessageEvent::Original(message)))
                        if matches!(message.content.msgtype, MessageType::Image(_)))
                    }),
                    ..Default::default()
                },
            );
            controller
                .replace_with_initial_remote_events(
                    vec![image(target), image(survivor)],
                    RemoteEventOrigin::Cache,
                )
                .await;
            controller.live_lazy_paginate_backwards(u16::MAX).await;
            controller.state.read().await.meta.subscriber_skip_count.update(skip, true);
            let (mut replica, mut output) = controller.subscribe().await;
            let (mut full_replica, mut full_output) = controller.subscribe_filter_map(Some).await;
            // Mimic Zyna's long-lived catalogue: absence from a Reset/Clear or
            // a lazy subscriber window does not delete an attachment record.
            let mut catalog = HashSet::from([target.to_owned(), survivor.to_owned()]);
            let snapshot = match change {
                "raw_redaction" => vec![
                    factory
                        .redacted(&BOB, RedactedRoomMessageEventContent::new())
                        .event_id(target)
                        .into_event(),
                    image(survivor),
                ],
                "redaction" => {
                    vec![image(target), image(survivor), factory.redaction(target).into_event()]
                }
                "edit_to_text" => vec![
                    image(target),
                    image(survivor),
                    factory
                        .text_msg(" * now text")
                        .edit(target, MessageType::text_plain("now text").into())
                        .into_event(),
                ],
                "bundled_edit_reaction" => vec![
                    factory
                        .image("image".into(), mxc_uri!("mxc://example.org/test").to_owned())
                        .event_id(target)
                        .with_bundled_edit(
                            factory
                                .text_msg(" * now text")
                                .edit(target, MessageType::text_plain("now text").into()),
                        )
                        .into_event(),
                    image(survivor),
                    factory.reaction(target, "👍").sender(*BOB).into_event(),
                ],
                _ => unreachable!(),
            };
            controller
                .replace_remote_events_after_cache_lag(snapshot, RemoteEventOrigin::Cache)
                .await;
            let mut target_terminal_sets = 0;
            while let Some(Some(diffs)) = output.next().now_or_never() {
                for diff in diffs {
                    if let VectorDiff::Set { index, value } = &diff {
                        // AttachmentRow.other drops eventId. Zyna invalidates
                        // the catalogue using the previous positional media
                        // row, not the incoming text/redacted event's ID.
                        let previous_media_id = replica
                            .get(*index)
                            .and_then(|item| item.as_event())
                            .filter(|event| {
                                event.content().as_message().is_some_and(|message| {
                                    matches!(message.msgtype(), MessageType::Image(_))
                                })
                            })
                            .and_then(|event| event.event_id())
                            .map(ToOwned::to_owned);
                        if let Some(event) = value.as_event()
                            && event.event_id() == Some(target)
                        {
                            assert!(
                                event.content().is_redacted()
                                    || event.content().as_message().is_some_and(
                                        |message| matches!(message.msgtype(), MessageType::Text(_))
                                    )
                            );
                            target_terminal_sets += 1;
                            assert_eq!(
                                previous_media_id.as_deref(),
                                Some(target),
                                "catalogue invalidation needs the original media row: {change}, skip={skip}"
                            );
                            if let Some(previous_media_id) = previous_media_id {
                                catalog.remove(&previous_media_id);
                            }
                        }
                    }
                    // Only image upserts are persisted on add. Deletion is an
                    // explicit semantic effect on Set, as in the Zyna path.
                    let added = match &diff {
                        VectorDiff::Append { values } | VectorDiff::Reset { values } => {
                            values.iter().collect::<Vec<_>>()
                        }
                        VectorDiff::PushFront { value }
                        | VectorDiff::PushBack { value }
                        | VectorDiff::Insert { value, .. } => vec![value],
                        _ => Vec::new(),
                    };
                    for item in added {
                        if let Some(event) = item.as_event()
                            && event.content().as_message().is_some_and(|message| {
                                matches!(message.msgtype(), MessageType::Image(_))
                            })
                        {
                            catalog.insert(event.event_id().unwrap().to_owned());
                        }
                    }
                    diff.apply(&mut replica);
                    assert_unique_replica(&replica);
                }
            }
            while let Some(Some(diff)) = full_output.next().now_or_never() {
                diff.apply(&mut full_replica);
                assert_unique_replica(&full_replica);
            }
            assert_eq!(format!("{full_replica:?}"), format!("{:?}", controller.items().await));
            let (current, _) = controller.subscribe().await;
            assert_eq!(format!("{replica:?}"), format!("{current:?}"), "{change}, skip={skip}");
            assert_eq!(target_terminal_sets, 1, "exactly one final effect: {change}, skip={skip}");
            assert!(!catalog.contains(target), "stale catalogue attachment: {change}, skip={skip}");
            assert!(catalog.contains(survivor));
        }
    }
}

fn assert_unique_replica(items: &imbl::Vector<Arc<TimelineItem>>) {
    let mut event_ids = HashSet::new();
    let mut unique_ids = HashSet::new();
    for item in items {
        assert!(unique_ids.insert(item.unique_id()), "duplicate unique_id after one diff");
        if let Some(id) = item.as_event().and_then(|event| event.event_id()) {
            assert!(event_ids.insert(id), "duplicate event_id {id} after one diff");
        }
    }
}

#[matrix_sdk_test::async_test]
async fn test_cache_rebuild_publishes_only_final_reactions_for_existing_and_new_events() {
    let fixture = LagFixture::new().await;
    let snapshot = (0..1000)
        .flat_map(|index| {
            let id = lag_id(index);
            [
                fixture.factory.text_msg("message").event_id(&id).into_event(),
                fixture.factory.reaction(&id, "👍").sender(*BOB).into_event(),
            ]
        })
        .collect::<Vec<_>>();
    let (mut replica, mut output) = fixture.controller.subscribe_filter_map(Some).await;
    let counts = fixture
        .controller
        .replace_remote_events_after_cache_lag(snapshot, RemoteEventOrigin::Cache)
        .await;
    let mut sets = 0;
    while let Some(Some(diff)) = output.next().now_or_never() {
        if matches!(diff, VectorDiff::Set { .. }) {
            sets += 1;
        }
        diff.apply(&mut replica);
        assert_unique_replica(&replica);
        // Every newly published message already contains the final reaction.
        for event in replica.iter().filter_map(|item| item.as_event()) {
            assert_eq!(event.content().reactions().unwrap().get("👍").unwrap().len(), 1);
        }
    }
    assert_eq!(sets, 0);
    assert_eq!(counts.operations, 2);
    assert_eq!(counts.item_occurrences, replica.len() as u64);
    assert_eq!(replica.iter().filter(|item| item.as_event().is_some()).count(), 1000);
    assert_eq!(format!("{replica:?}"), format!("{:?}", fixture.controller.items().await));
}

#[matrix_sdk_test::async_test]
async fn test_ordinary_cache_clear_with_local_echo_discards_all_remote_metadata() {
    let fixture = LagFixture::new().await;
    let controller = &fixture.controller;
    controller
        .handle_local_event(
            TransactionId::new(),
            RoomMessageEventContent::text_plain("local").into(),
            None,
        )
        .await;
    let local_id = controller
        .items()
        .await
        .iter()
        .find(|item| item.is_local_echo())
        .unwrap()
        .unique_id()
        .clone();
    controller
        .handle_remote_events_with_diffs(vec![VectorDiff::Clear], RemoteEventOrigin::Sync)
        .await;
    assert_eq!(controller.state.read().await.items.all_remote_events().iter().count(), 0);
    let items = controller.items().await;
    let local = items.iter().find(|item| item.is_local_echo()).unwrap();
    assert_eq!(local.unique_id(), &local_id);
    assert_eq!(items.iter().filter_map(|item| item.as_event()).count(), 1);
    controller
        .handle_remote_events_with_diffs(
            vec![VectorDiff::PushBack {
                value: fixture.factory.text_msg("after clear").event_id(&lag_id(40)).into_event(),
            }],
            RemoteEventOrigin::Sync,
        )
        .await;
    assert_eq!(controller.state.read().await.items.all_remote_events().iter().count(), 1);
    assert_unique_replica(&controller.items().await);
}
