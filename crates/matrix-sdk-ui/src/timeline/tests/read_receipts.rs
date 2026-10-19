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

use assert_matches2::assert_matches;
use eyeball_im::VectorDiff;
use matrix_sdk::assert_next_with_timeout;
use matrix_sdk_test::{
    ALICE, BOB, CAROL, JoinedRoomBuilder, async_test, event_factory::EventFactory,
};
use ruma::{
    event_id,
    events::{
        AnySyncMessageLikeEvent, AnySyncTimelineEvent,
        receipt::{Receipt, ReceiptThread, ReceiptType},
        room::message::{MessageType, RoomMessageEventContent, SyncRoomMessageEvent},
    },
    owned_event_id, room_id,
    room_version_rules::RoomVersionRules,
    uint,
};
use stream_assert::{assert_next_matches, assert_pending};

use super::{ReadReceiptMap, TestRoomDataProvider};
use crate::timeline::{
    MsgLikeContent, MsgLikeKind, RoomExt, TimelineFocus, TimelineReadReceiptTracking,
    controller::TimelineSettings,
    event_item::RemoteEventOrigin,
    tests::{TestTimelineBuilder, encryption::get_client},
};

fn filter_notice(ev: &AnySyncTimelineEvent, _rules: &RoomVersionRules) -> bool {
    match ev {
        AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
            SyncRoomMessageEvent::Original(msg),
        )) => !matches!(msg.content.msgtype, MessageType::Notice(_)),
        _ => true,
    }
}

#[async_test]
async fn test_rebuild_and_receipt_refresh_preserve_own_receipt_beyond_snapshot() {
    let future_event_id = event_id!("$receipt-target-after-snapshot");
    let old_event_id = event_id!("$old-own-event");

    for receipt_type in [ReceiptType::Read, ReceiptType::ReadPrivate] {
        let mut receipts = ReadReceiptMap::new();
        receipts
            .entry(receipt_type.clone())
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(ALICE.to_owned(), (future_event_id.to_owned(), Receipt::default()));
        let timeline = TestTimelineBuilder::new()
            .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
            .settings(TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            })
            .build();
        let old_event = timeline
            .factory
            .text_msg("old own message")
            .sender(*ALICE)
            .event_id(old_event_id)
            .into_event();

        timeline
            .controller
            .replace_remote_events_after_cache_lag([old_event], RemoteEventOrigin::Cache)
            .await;
        assert_eq!(
            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
            future_event_id,
            "rebuild must preserve {receipt_type} outside the snapshot",
        );

        // A queued older payload must resolve to the current stored receipt,
        // including when its target is not in all_remote_events yet.
        let queued = timeline
            .factory
            .read_receipts()
            .add_with_timestamp(
                old_event_id,
                *ALICE,
                receipt_type.clone(),
                ReceiptThread::Unthreaded,
                None,
            )
            .into_event()
            .into_raw();
        timeline.controller.handle_ephemeral_events(vec![queued]).await;
        assert_eq!(
            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
            future_event_id,
            "queued notification must not undo restored {receipt_type}",
        );
    }
}

#[async_test]
async fn test_authoritative_own_receipt_accepts_target_outside_loaded_events() {
    let future_event_id = event_id!("$future-authoritative-receipt");
    let old_event_id = event_id!("$old-known-receipt");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(ALICE.to_owned(), (future_event_id.to_owned(), Receipt::default()));
    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .handle_live_event(
            timeline.factory.text_msg("own message").sender(*ALICE).event_id(old_event_id),
        )
        .await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, old_event_id);

    timeline
        .controller
        .replace_remote_events_after_cache_lag(
            [timeline
                .factory
                .text_msg("snapshot")
                .sender(*CAROL)
                .event_id(old_event_id)
                .into_event()],
            RemoteEventOrigin::Cache,
        )
        .await;

    let queued = timeline
        .factory
        .read_receipts()
        .add_with_timestamp(
            old_event_id,
            *ALICE,
            ReceiptType::Read,
            ReceiptThread::Unthreaded,
            None,
        )
        .into_event()
        .into_raw();
    timeline.controller.handle_ephemeral_events(vec![queued]).await;
    assert_eq!(
        timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
        future_event_id
    );
}

#[async_test]
async fn test_rebuild_preserves_newer_known_implicit_own_receipt() {
    let old_event_id = event_id!("$old-stored-receipt");
    let own_event_id = event_id!("$newer-own-message");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(ALICE.to_owned(), (old_event_id.to_owned(), Receipt::default()));
    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .controller
        .replace_with_initial_remote_events(
            [
                timeline.factory.text_msg("old").sender(*BOB).event_id(old_event_id).into_event(),
                timeline.factory.text_msg("own").sender(*ALICE).event_id(own_event_id).into_event(),
            ],
            RemoteEventOrigin::Cache,
        )
        .await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, own_event_id);
}

#[async_test]
async fn test_authoritative_public_receipt_waits_for_its_target() {
    let old_event_id = event_id!("$old-public-receipt");
    let future_event_id = event_id!("$future-public-receipt");
    let after_event_id = event_id!("$after-public-receipt");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(BOB.to_owned(), (future_event_id.to_owned(), Receipt::default()));
    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .handle_live_event(
            timeline.factory.text_msg("old message").sender(*BOB).event_id(old_event_id),
        )
        .await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, old_event_id);

    timeline
        .controller
        .replace_remote_events_after_cache_lag(
            [timeline
                .factory
                .text_msg("snapshot")
                .sender(*CAROL)
                .event_id(old_event_id)
                .into_event()],
            RemoteEventOrigin::Cache,
        )
        .await;

    let queued = timeline
        .factory
        .read_receipts()
        .add_with_timestamp(old_event_id, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
        .into_event()
        .into_raw();
    timeline.controller.handle_ephemeral_events(vec![queued]).await;
    assert_eq!(
        timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0,
        future_event_id
    );

    // The sender event is newer than the old snapshot, but older than the
    // authoritative receipt target. Its implicit receipt must not win.
    timeline
        .handle_live_event(timeline.factory.text_msg("intermediate message").sender(*BOB))
        .await;
    assert_eq!(
        timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0,
        future_event_id
    );
    assert!(
        timeline
            .controller
            .items()
            .await
            .iter()
            .filter_map(|item| item.as_event())
            .all(|event| !event.read_receipts().contains_key(*BOB))
    );

    timeline
        .handle_live_event(
            timeline.factory.text_msg("target message").sender(*CAROL).event_id(future_event_id),
        )
        .await;
    assert_eq!(
        timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0,
        future_event_id
    );
    let items = timeline.controller.items().await;
    let target = items
        .iter()
        .filter_map(|item| item.as_event())
        .find(|event| event.event_id() == Some(future_event_id))
        .unwrap();
    assert!(target.read_receipts().contains_key(*BOB));

    // Once the protected target is present, ordinary implicit advancement works.
    timeline
        .handle_live_event(
            timeline.factory.text_msg("newer message").sender(*BOB).event_id(after_event_id),
        )
        .await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, after_event_id);
}

#[async_test]
async fn test_initial_old_unloaded_private_receipt_does_not_block_implicit_receipt() {
    let own_event_id = event_id!("$new-own-after-unloaded-private");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::ReadPrivate)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(
            ALICE.to_owned(),
            (event_id!("$old-unloaded-private").to_owned(), Receipt::default()),
        );
    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .controller
        .replace_with_initial_remote_events(
            [timeline
                .factory
                .text_msg("new own")
                .sender(*ALICE)
                .event_id(own_event_id)
                .into_event()],
            RemoteEventOrigin::Cache,
        )
        .await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, own_event_id);
}

#[async_test]
async fn test_recovery_old_unloaded_receipt_uses_available_timestamps() {
    for receipt_type in [ReceiptType::Read, ReceiptType::ReadPrivate] {
        let mut receipts = ReadReceiptMap::new();
        receipts
            .entry(receipt_type)
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(
                ALICE.to_owned(),
                (
                    event_id!("$old-unloaded").to_owned(),
                    Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(5))),
                ),
            );
        let timeline = TestTimelineBuilder::new()
            .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
            .settings(TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            })
            .build();
        timeline.factory.set_next_ts(10);
        let snapshot_id = event_id!("$snapshot-own-after-old-receipt");
        timeline
            .controller
            .replace_remote_events_after_cache_lag(
                [timeline
                    .factory
                    .text_msg("snapshot")
                    .sender(*ALICE)
                    .event_id(snapshot_id)
                    .into_event()],
                RemoteEventOrigin::Cache,
            )
            .await;
        assert_eq!(
            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
            snapshot_id
        );
        let newest_id = event_id!("$fresh-own-after-recovery");
        timeline
            .handle_live_event(
                timeline.factory.text_msg("fresh own").sender(*ALICE).event_id(newest_id),
            )
            .await;
        assert_eq!(
            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
            newest_id
        );
    }
}

#[async_test]
async fn test_matching_receipt_notification_releases_recovery_protection() {
    let stored_id = event_id!("$unloaded-authoritative");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(ALICE.to_owned(), (stored_id.to_owned(), Receipt::default()));
    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .controller
        .replace_remote_events_after_cache_lag(
            [timeline.factory.text_msg("snapshot").sender(*ALICE).into_event()],
            RemoteEventOrigin::Cache,
        )
        .await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, stored_id);
    let acknowledgement = timeline
        .factory
        .read_receipts()
        .add_with_timestamp(stored_id, *ALICE, ReceiptType::Read, ReceiptThread::Unthreaded, None)
        .into_event()
        .into_raw();
    timeline.controller.handle_ephemeral_events(vec![acknowledgement]).await;
    let newest_id = event_id!("$fresh-after-acknowledgement");
    timeline
        .handle_live_event(
            timeline.factory.text_msg("fresh own").sender(*ALICE).event_id(newest_id),
        )
        .await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, newest_id);
}

#[async_test]
async fn test_read_receipts_updates_on_live_events() {
    let timeline = TestTimelineBuilder::new()
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    let mut stream = timeline.subscribe().await;

    let f = &timeline.factory;
    timeline.handle_live_event(f.text_msg("A").sender(*ALICE)).await;
    timeline.handle_live_event(f.text_msg("B").sender(*BOB)).await;

    // No read receipt for our own user.
    let item_a = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_a = item_a.as_event().unwrap();
    assert!(event_a.read_receipts().is_empty());

    let _date_divider = assert_next_matches!(stream, VectorDiff::PushFront { value } => value);

    // Implicit read receipt of Bob.
    let item_b = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_b = item_b.as_event().unwrap();
    assert_eq!(event_b.read_receipts().len(), 1);
    assert!(event_b.read_receipts().get(*BOB).is_some());

    // Implicit read receipt of Bob is updated.
    timeline.handle_live_event(f.text_msg("C").sender(*BOB)).await;

    let item_a = assert_next_matches!(stream, VectorDiff::Set { index: 2, value } => value);
    let event_a = item_a.as_event().unwrap();
    assert!(event_a.read_receipts().is_empty());

    let item_c = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_c = item_c.as_event().unwrap();
    assert_eq!(event_c.read_receipts().len(), 1);
    assert!(event_c.read_receipts().get(*BOB).is_some());

    timeline.handle_live_event(f.text_msg("D").sender(*ALICE)).await;

    let item_d = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_d = item_d.as_event().unwrap();
    assert!(event_d.read_receipts().is_empty());

    // Explicit read receipt is updated.
    timeline
        .handle_read_receipts([(
            event_d.event_id().unwrap().to_owned(),
            ReceiptType::Read,
            BOB.to_owned(),
            ReceiptThread::Unthreaded,
        )])
        .await;

    let item_c = assert_next_matches!(stream, VectorDiff::Set { index: 3, value } => value);
    let event_c = item_c.as_event().unwrap();
    assert!(event_c.read_receipts().is_empty());

    let item_d = assert_next_matches!(stream, VectorDiff::Set { index: 4, value } => value);
    let event_d = item_d.as_event().unwrap();
    assert_eq!(event_d.read_receipts().len(), 1);
    assert!(event_d.read_receipts().get(*BOB).is_some());
}

#[async_test]
async fn test_read_receipts_updates_on_back_paginated_events() {
    let timeline = TestTimelineBuilder::new()
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    let room_id = room_id!("!room:localhost");

    let f = EventFactory::new().room(room_id);

    timeline
        .handle_back_paginated_event(
            f.text_msg("A").sender(*BOB).event_id(event_id!("$event_a")).into_raw_timeline(),
        )
        .await;
    timeline
        .handle_back_paginated_event(
            f.text_msg("B")
                .sender(*CAROL)
                .event_id(event_id!("$event_with_bob_receipt"))
                .into_raw_timeline(),
        )
        .await;

    let items = timeline.controller.items().await;
    assert_eq!(items.len(), 3);
    assert!(items[0].is_date_divider());

    // Implicit read receipt of Bob.
    let event_a = items[2].as_event().unwrap();
    assert_eq!(event_a.read_receipts().len(), 1);
    assert!(event_a.read_receipts().get(*BOB).is_some());

    // Implicit read receipt of Carol, explicit read receipt of Bob ignored.
    let event_b = items[1].as_event().unwrap();
    assert_eq!(event_b.read_receipts().len(), 1);
    assert!(event_b.read_receipts().get(*CAROL).is_some());
}

#[async_test]
async fn test_read_receipts_updates_on_filtered_events() {
    let timeline = TestTimelineBuilder::new()
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            event_filter: Arc::new(filter_notice),
            ..Default::default()
        })
        .build();
    let mut stream = timeline.subscribe().await;

    let f = &timeline.factory;
    timeline.handle_live_event(f.text_msg("A").sender(*ALICE)).await;
    timeline.handle_live_event(f.notice("B").sender(*BOB)).await;

    // No read receipt for our own user.
    let item_a = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_a = item_a.as_event().unwrap();
    assert!(event_a.read_receipts().is_empty());

    let _date_divider = assert_next_matches!(stream, VectorDiff::PushFront { value } => value);

    // Implicit read receipt of Bob.
    let item_a = assert_next_matches!(stream, VectorDiff::Set { index: 1, value } => value);
    let event_a = item_a.as_event().unwrap();
    assert_eq!(event_a.read_receipts().len(), 1);
    assert!(event_a.read_receipts().get(*BOB).is_some());

    // Implicit read receipt of Bob is updated.
    timeline.handle_live_event(f.text_msg("C").sender(*BOB)).await;

    let item_a = assert_next_matches!(stream, VectorDiff::Set { index: 1, value } => value);
    let event_a = item_a.as_event().unwrap();
    assert!(event_a.read_receipts().is_empty());

    let item_c = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_c = item_c.as_event().unwrap();
    assert_eq!(event_c.read_receipts().len(), 1);
    assert!(event_c.read_receipts().get(*BOB).is_some());

    // Populate more events.
    let event_d_id = owned_event_id!("$event_d");

    timeline.handle_live_event(f.notice("D").sender(*ALICE).event_id(&event_d_id)).await;

    timeline.handle_live_event(f.text_msg("E").sender(*ALICE)).await;

    let item_e = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_e = item_e.as_event().unwrap();
    assert!(event_e.read_receipts().is_empty());

    // Explicit read receipt is updated but its visible event doesn't change.
    timeline
        .handle_read_receipts([(
            event_d_id,
            ReceiptType::Read,
            BOB.to_owned(),
            ReceiptThread::Unthreaded,
        )])
        .await;

    // Explicit read receipt is updated and its visible event changes.
    timeline
        .handle_read_receipts([(
            event_e.event_id().unwrap().to_owned(),
            ReceiptType::Read,
            BOB.to_owned(),
            ReceiptThread::Unthreaded,
        )])
        .await;

    let item_c = assert_next_matches!(stream, VectorDiff::Set { index: 2, value } => value);
    let event_c = item_c.as_event().unwrap();
    assert!(event_c.read_receipts().is_empty());

    let item_e = assert_next_matches!(stream, VectorDiff::Set { index: 3, value } => value);
    let event_e = item_e.as_event().unwrap();
    assert_eq!(event_e.read_receipts().len(), 1);
    assert!(event_e.read_receipts().get(*BOB).is_some());

    assert_pending!(stream);
}

#[async_test]
async fn test_read_receipts_updates_on_filtered_events_with_stored() {
    let event_with_bob_receipt_id = event_id!("$event_with_bob_receipt");

    // Add initial unthreaded private receipt.
    let mut initial_user_receipts = ReadReceiptMap::new();
    initial_user_receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(
            BOB.to_owned(),
            (
                event_with_bob_receipt_id.to_owned(),
                Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(5))),
            ),
        );

    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(initial_user_receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            event_filter: Arc::new(filter_notice),
            ..Default::default()
        })
        .build();
    let f = &timeline.factory;
    let mut stream = timeline.subscribe().await;

    timeline.handle_live_event(f.text_msg("A").sender(*ALICE)).await;
    timeline
        .handle_live_event(f.notice("B").sender(*CAROL).event_id(event_with_bob_receipt_id))
        .await;

    // No read receipt for our own user.
    let item_a = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_a = item_a.as_event().unwrap();
    assert!(event_a.read_receipts().is_empty());

    let _date_divider = assert_next_matches!(stream, VectorDiff::PushFront { value } => value);

    // Stored read receipt of Bob.
    let item_a = assert_next_matches!(stream, VectorDiff::Set { index: 1, value } => value);
    let event_a = item_a.as_event().unwrap();
    assert_eq!(event_a.read_receipts().len(), 1);
    assert!(event_a.read_receipts().get(*BOB).is_some());

    // Implicit read receipt of Carol.
    let item_a = assert_next_matches!(stream, VectorDiff::Set { index: 1, value } => value);
    let event_a = item_a.as_event().unwrap();
    assert_eq!(event_a.read_receipts().len(), 2);
    assert!(event_a.read_receipts().get(*BOB).is_some());
    assert!(event_a.read_receipts().get(*CAROL).is_some());

    // Implicit read receipt of Bob is updated.
    timeline.handle_live_event(f.text_msg("C").sender(*BOB)).await;

    let item_a = assert_next_matches!(stream, VectorDiff::Set { index: 1, value } => value);
    let event_a = item_a.as_event().unwrap();
    assert_eq!(event_a.read_receipts().len(), 1);

    let item_c = assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let event_c = item_c.as_event().unwrap();
    assert_eq!(event_c.read_receipts().len(), 1);
    assert!(event_c.read_receipts().get(*BOB).is_some());

    assert_pending!(stream);
}

#[async_test]
async fn test_read_receipts_updates_on_back_paginated_filtered_events() {
    let event_with_bob_receipt_id = event_id!("$event_with_bob_receipt");

    // Add initial unthreaded private receipt.
    let mut initial_user_receipts = ReadReceiptMap::new();
    initial_user_receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(
            BOB.to_owned(),
            (
                event_with_bob_receipt_id.to_owned(),
                Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(5))),
            ),
        );

    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(initial_user_receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            event_filter: Arc::new(filter_notice),
            ..Default::default()
        })
        .build();
    let mut stream = timeline.subscribe().await;
    let room_id = room_id!("!room:localhost");

    let f = EventFactory::new().room(room_id);

    timeline
        .handle_back_paginated_event(
            f.text_msg("A").sender(*ALICE).event_id(event_id!("$event_a")).into_raw_timeline(),
        )
        .await;
    timeline
        .handle_back_paginated_event(
            f.notice("B").sender(*CAROL).event_id(event_with_bob_receipt_id).into_raw_timeline(),
        )
        .await;

    // No read receipt for our own user.
    let item_a = assert_next_matches!(stream, VectorDiff::PushFront { value } => value);
    let event_a = item_a.as_event().unwrap();
    assert!(event_a.read_receipts().is_empty());

    let date_divider = assert_next_matches!(stream, VectorDiff::PushFront { value } => value);
    assert!(date_divider.is_date_divider());

    // Add non-filtered event to show read receipts.
    timeline
        .handle_back_paginated_event(
            f.text_msg("C").sender(*CAROL).event_id(event_id!("$event_c")).into_raw_timeline(),
        )
        .await;

    // Implicit read receipt of Carol.
    let item_c = assert_next_matches!(stream, VectorDiff::PushFront { value } => value);
    let event_c = item_c.as_event().unwrap();
    assert_eq!(event_c.read_receipts().len(), 2);
    assert!(event_c.read_receipts().get(*BOB).is_some());
    assert!(event_c.read_receipts().get(*CAROL).is_some());

    // Reinsert a new date divider before the first back-paginated event.
    let date_divider = assert_next_matches!(stream, VectorDiff::PushFront { value } => value);
    assert!(date_divider.is_date_divider());

    // Remove the last date divider.
    assert_next_matches!(stream, VectorDiff::Remove { index: 2 });

    assert_pending!(stream);
}

#[async_test]
async fn test_read_receipts_updates_on_message_decryption() {
    use std::io::Cursor;

    use assert_matches2::assert_let;
    use matrix_sdk_base::crypto::decrypt_room_key_export;
    use ruma::events::room::encrypted::{
        EncryptedEventScheme, MegolmV1AesSha2ContentInit, RoomEncryptedEventContent,
    };

    use crate::timeline::{EncryptedMessage, TimelineItemContent};

    fn filter_text_msg(ev: &AnySyncTimelineEvent, _rules: &RoomVersionRules) -> bool {
        match ev {
            AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
                SyncRoomMessageEvent::Original(msg),
            )) => !matches!(msg.content.msgtype, MessageType::Text(_)),
            _ => true,
        }
    }

    const SESSION_ID: &str = "gM8i47Xhu0q52xLfgUXzanCMpLinoyVyH7R58cBuVBU";
    const SESSION_KEY: &[u8] = b"\
        -----BEGIN MEGOLM SESSION DATA-----\n\
        ASKcWoiAVUM97482UAi83Avce62hSLce7i5JhsqoF6xeAAAACqt2Cg3nyJPRWTTMXxXH7TXnkfdlmBXbQtq5\
        bpHo3LRijcq2Gc6TXilESCmJN14pIsfKRJrWjZ0squ/XsoTFytuVLWwkNaW3QF6obeg2IoVtJXLMPdw3b2vO\
        vgwGY3OMP0XafH13j1vcb6YLzvgLkZQLnYvd47hv3yK/9GmKS9tokuaQ7dCVYckYcIOS09EDTs70YdxUd5WG\
        rQynATCLFP1p/NAGv70r9MK7Cy/mNpjD0r4qC7UEDIoi1kOWzHgnLo19wtvwsb8Fg8ATxcs3Wmtj8hIUYpDx\
        ia4sM10zbytUuaPUAfCDf42IyxdmOnGe1CueXhgI71y+RW0s0argNqUt7jB70JT0o9CyX6UBGRaqLk2MPY9T\
        hUu5J8X3UgIa6rcbWigzohzWm9rdbEHFrSWqjpfQYMaAKQQgETrjSy4XTrp2RhC2oNqG/hylI4ab+F4X6fpH\
        DYP1NqNMP5g36xNu7LhDnrUB5qsPjYOmWORxGLfudpF3oLYCSlr3DgHqEIB6HjQblLZ3KQuPBse3zxyROTnS\
        AhdPH4a/z1wioFtKNVph3hecsiKEdqnz4Y2coSIdhz58mJ9JWNQoFAENE5CSsoEZAGvafYZVpW4C75YY2zq1\
        wIeiFi1dT43/jLAUGkslsi1VvnyfUu8qO404RxYO3XHoGLMFoFLOO+lZ+VGci2Vz10AhxJhEBHxRKxw4k2uB\
        HztoSJUr/2Y\n\
        -----END MEGOLM SESSION DATA-----";

    let room_id = room_id!("!DovneieKSTkdHKpIXy:morpheus.localhost");
    let (client, server, event_factory) = get_client(room_id, None).await;

    let room = client.get_room(room_id).unwrap();
    let timeline = room
        .timeline_builder()
        .event_filter(filter_text_msg)
        .track_read_marker_and_receipts(TimelineReadReceiptTracking::AllEvents)
        .build()
        .await
        .unwrap();
    let (_, mut stream) = timeline.subscribe().await;

    let event = event_factory.notice("I am not encrypted").sender(&CAROL);
    let encrypted_event = event_factory
        .event(RoomEncryptedEventContent::new(
            EncryptedEventScheme::MegolmV1AesSha2(
                MegolmV1AesSha2ContentInit {
                    ciphertext: "\
                            AwgAEtABPRMavuZMDJrPo6pGQP4qVmpcuapuXtzKXJyi3YpEsjSWdzuRKIgJzD4P\
                            cSqJM1A8kzxecTQNJsC5q22+KSFEPxPnI4ltpm7GFowSoPSW9+bFdnlfUzEP1jPq\
                            YevHAsMJp2fRKkzQQbPordrUk1gNqEpGl4BYFeRqKl9GPdKFwy45huvQCLNNueql\
                            CFZVoYMuhxrfyMiJJAVNTofkr2um2mKjDTlajHtr39pTG8k0eOjSXkLOSdZvNOMz\
                            hGhSaFNeERSA2G2YbeknOvU7MvjiO0AKuxaAe1CaVhAI14FCgzrJ8g0y5nly+n7x\
                            QzL2G2Dn8EoXM5Iqj8W99iokQoVsSrUEnaQ1WnSIfewvDDt4LCaD/w7PGETMCQ"
                        .to_owned(),
                    sender_key: "DeHIg4gwhClxzFYcmNntPNF9YtsdZbmMy8+3kzCMXHA".to_owned(),
                    device_id: "NLAZCWIOCO".into(),
                    session_id: SESSION_ID.into(),
                }
                .into(),
            ),
            None,
        ))
        .sender(&BOB);

    server
        .mock_sync()
        .ok_and_run(&client, |builder| {
            builder.add_joined_room(
                JoinedRoomBuilder::new(room_id)
                    .add_timeline_event(event)
                    .add_timeline_event(encrypted_event),
            );
        })
        .await;

    assert_eq!(timeline.controller.items().await.len(), 3);
    let updates = assert_next_with_timeout!(stream);

    // The first event only has Carol's receipt.
    assert_matches!(&updates[0], VectorDiff::PushBack { value });
    let clear_event = value.as_event().unwrap();
    assert!(clear_event.content().is_message());
    assert_eq!(clear_event.read_receipts().len(), 1);
    assert!(clear_event.read_receipts().get(*CAROL).is_some());

    assert_matches!(&updates[2], VectorDiff::PushFront { value });
    assert!(value.is_date_divider());

    // The second event is encrypted and only has Bob's receipt.
    assert_matches!(&updates[1], VectorDiff::PushBack { value });
    let encrypted_event = value.as_event().unwrap();

    assert_let!(
        TimelineItemContent::MsgLike(MsgLikeContent {
            kind: MsgLikeKind::UnableToDecrypt(EncryptedMessage::MegolmV1AesSha2 {
                session_id,
                ..
            }),
            ..
        }) = encrypted_event.content()
    );

    assert_eq!(session_id, SESSION_ID);
    assert_eq!(encrypted_event.read_receipts().len(), 1);
    assert!(encrypted_event.read_receipts().get(*BOB).is_some());

    // Decrypt encrypted message.
    let exported_keys = decrypt_room_key_export(Cursor::new(SESSION_KEY), "1234").unwrap();

    client
        .olm_machine_for_testing()
        .await
        .as_ref()
        .unwrap()
        .store()
        .import_exported_room_keys(exported_keys, |_, _| {})
        .await
        .unwrap();

    let updates = assert_next_with_timeout!(stream);
    // The first event now has both receipts.
    assert_matches!(&updates[0], VectorDiff::Set { index: 1, value });
    let clear_event = value.as_event().unwrap();
    assert!(clear_event.content().is_message());
    assert_eq!(clear_event.read_receipts().len(), 2);
    assert!(clear_event.read_receipts().get(*CAROL).is_some());
    assert!(clear_event.read_receipts().get(*BOB).is_some());

    // The second event is removed.
    assert_matches!(&updates[1], VectorDiff::Remove { index: 2 });

    assert_pending!(stream);
}

#[async_test]
async fn test_initial_public_unthreaded_receipt() {
    let event_id = owned_event_id!("$event_with_receipt");

    // Add initial unthreaded public receipt.
    let mut initial_user_receipts = ReadReceiptMap::new();
    initial_user_receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(
            ALICE.to_owned(),
            (event_id.clone(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(10)))),
        );

    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(initial_user_receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();

    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, event_id);
}

#[async_test]
async fn test_initial_public_main_thread_receipt() {
    let event_id = owned_event_id!("$event_with_receipt");

    // Add initial public receipt on the main thread.
    let mut initial_user_receipts = ReadReceiptMap::new();
    initial_user_receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Main)
        .or_default()
        .insert(
            ALICE.to_owned(),
            (event_id.clone(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(10)))),
        );

    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(initial_user_receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();

    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, event_id);
}

#[async_test]
async fn test_initial_private_unthreaded_receipt() {
    let event_id = owned_event_id!("$event_with_receipt");

    // Add initial unthreaded private receipt.
    let mut initial_user_receipts = ReadReceiptMap::new();
    initial_user_receipts
        .entry(ReceiptType::ReadPrivate)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(
            ALICE.to_owned(),
            (event_id.clone(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(10)))),
        );

    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(initial_user_receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();

    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, event_id);
}

#[async_test]
async fn test_initial_private_main_thread_receipt() {
    let event_id = owned_event_id!("$event_with_receipt");

    // Add initial private receipt on the main thread.
    let mut initial_user_receipts = ReadReceiptMap::new();
    initial_user_receipts
        .entry(ReceiptType::ReadPrivate)
        .or_default()
        .entry(ReceiptThread::Main)
        .or_default()
        .insert(
            ALICE.to_owned(),
            (event_id.clone(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(10)))),
        );

    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(initial_user_receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();

    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, event_id);
}

#[async_test]
async fn test_clear_read_receipts() {
    let room_id = room_id!("!room:localhost");
    let event_a_id = event_id!("$event_a");
    let event_b_id = event_id!("$event_b");

    let timeline = TestTimelineBuilder::new()
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    let f = &timeline.factory;

    let event_a_content = RoomMessageEventContent::text_plain("A");
    timeline
        .handle_live_event(f.event(event_a_content.clone()).sender(*BOB).event_id(event_a_id))
        .await;

    let items = timeline.controller.items().await;
    assert_eq!(items.len(), 2);

    // Implicit read receipt of Bob.
    let event_a = items[1].as_event().unwrap();
    assert_eq!(event_a.read_receipts().len(), 1);
    assert!(event_a.read_receipts().get(*BOB).is_some());

    // We received a limited timeline.
    timeline.controller.clear().await;

    // New message via sync.
    timeline.handle_live_event(f.text_msg("B").sender(*BOB).event_id(event_b_id)).await;

    // Old message via back-pagination.
    timeline
        .handle_back_paginated_event(
            f.event(event_a_content)
                .sender(*BOB)
                .room(room_id)
                .event_id(event_a_id)
                .into_raw_timeline(),
        )
        .await;

    let items = timeline.controller.items().await;
    assert_eq!(items.len(), 3);

    // Old implicit read receipt of Bob is gone.
    let event_a = items[1].as_event().unwrap();
    assert_eq!(event_a.read_receipts().len(), 0);

    // New implicit read receipt of Bob.
    let event_b = items[2].as_event().unwrap();
    assert_eq!(event_b.read_receipts().len(), 1);
    assert!(event_b.read_receipts().get(*BOB).is_some());
}

#[async_test]
async fn test_implicit_read_receipt_before_explicit_read_receipt() {
    // Test a timeline in this order:
    // 1. $alice_event: sent by alice, has no explicit read receipts.
    // 2. $bob_event: sent by bob, has no explicit read receipts.
    // 3. $carol_event: sent by carol, has the explicit read receipts of all users.
    let room_id = room_id!("!room:localhost");
    let alice_event_id = owned_event_id!("$alice_event");
    let bob_event_id = owned_event_id!("$bob_event");
    let carol_event_id = owned_event_id!("$carol_event");

    // Add initial unthreaded private receipt.
    let mut initial_user_receipts = ReadReceiptMap::new();
    let unthreaded_read_receipts = initial_user_receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default();
    unthreaded_read_receipts.insert(
        ALICE.to_owned(),
        (carol_event_id.clone(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(10)))),
    );
    unthreaded_read_receipts.insert(
        BOB.to_owned(),
        (carol_event_id.clone(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(5)))),
    );
    unthreaded_read_receipts.insert(
        CAROL.to_owned(),
        (carol_event_id.clone(), Receipt::new(ruma::MilliSecondsSinceUnixEpoch(uint!(1)))),
    );

    let timeline = TestTimelineBuilder::new()
        .provider(TestRoomDataProvider::default().with_initial_user_receipts(initial_user_receipts))
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();

    // Check that the receipts are at the correct place.
    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, carol_event_id);
    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*BOB).await.unwrap();
    assert_eq!(receipt_event_id, carol_event_id);
    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*CAROL).await.unwrap();
    assert_eq!(receipt_event_id, carol_event_id);

    // Add the events.
    timeline
        .handle_back_paginated_event(
            timeline
                .factory
                .text_msg("I am Carol!")
                .sender(*CAROL)
                .room(room_id)
                .event_id(&carol_event_id)
                .into_raw_timeline(),
        )
        .await;
    timeline
        .handle_back_paginated_event(
            timeline
                .factory
                .text_msg("I am Bob!")
                .sender(*BOB)
                .room(room_id)
                .event_id(&bob_event_id)
                .into_raw_timeline(),
        )
        .await;
    timeline
        .handle_back_paginated_event(
            timeline
                .factory
                .text_msg("I am Alice!")
                .sender(*ALICE)
                .room(room_id)
                .event_id(&alice_event_id)
                .into_raw_timeline(),
        )
        .await;

    // The receipts shouldn't have moved.
    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, carol_event_id);
    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*BOB).await.unwrap();
    assert_eq!(receipt_event_id, carol_event_id);
    let (receipt_event_id, _) = timeline.controller.latest_user_read_receipt(*CAROL).await.unwrap();
    assert_eq!(receipt_event_id, carol_event_id);
}

#[async_test]
async fn test_threaded_latest_user_read_receipt() {
    let thread_root = owned_event_id!("$thread_root");
    let receipt_thread = ReceiptThread::Thread(thread_root.clone());

    let timeline = TestTimelineBuilder::new()
        .focus(TimelineFocus::Thread { root_event_id: thread_root })
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();

    // Sanity check: no read receipts before any events.
    assert!(timeline.controller.latest_user_read_receipt(*ALICE).await.is_none());
    assert!(timeline.controller.latest_user_read_receipt(*BOB).await.is_none());

    // Add some sync events.
    let f = &timeline.factory;

    timeline
        .handle_live_event(f.text_msg("hi I'm Bob.").sender(*ALICE).event_id(event_id!("$1")))
        .await;

    timeline
        .handle_live_event(f.text_msg("hi Alice, I'm Bob.").sender(*BOB).event_id(event_id!("$2")))
        .await;

    // Implicit receipts are taken into account.
    let (receipt_event_id, receipt) =
        timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, event_id!("$1"));
    assert_eq!(receipt.thread, receipt_thread);

    let (receipt_event_id, receipt) =
        timeline.controller.latest_user_read_receipt(*BOB).await.unwrap();
    assert_eq!(receipt_event_id, event_id!("$2"));
    assert_eq!(receipt.thread, receipt_thread);

    timeline
        .handle_live_event(f.text_msg("nice to meet you!").sender(*ALICE).event_id(event_id!("$3")))
        .await;

    // Alice's latest read receipt is updated.
    let (receipt_event_id, receipt) =
        timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, event_id!("$3"));
    assert_eq!(receipt.thread, receipt_thread);

    // But Bob's isn't.
    let (receipt_event_id, receipt) =
        timeline.controller.latest_user_read_receipt(*BOB).await.unwrap();
    assert_eq!(receipt_event_id, event_id!("$2"));
    assert_eq!(receipt.thread, receipt_thread);

    // Bob sees Alice's message.
    timeline
        .handle_read_receipts([(
            owned_event_id!("$3"),
            ReceiptType::Read,
            BOB.to_owned(),
            receipt_thread.clone(),
        )])
        .await;

    // Alice's latest read receipt is at the same position.
    let (receipt_event_id, receipt) =
        timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap();
    assert_eq!(receipt_event_id, event_id!("$3"));
    assert_eq!(receipt.thread, receipt_thread);

    // But Bob's has moved!
    let (receipt_event_id, receipt) =
        timeline.controller.latest_user_read_receipt(*BOB).await.unwrap();
    assert_eq!(receipt_event_id, event_id!("$3"));
    assert_eq!(receipt.thread, receipt_thread);
}

#[async_test]
async fn test_unthreaded_client_updates_threaded_read_receipts() {
    let timeline = TestTimelineBuilder::new()
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .focus(TimelineFocus::Live { hide_threaded_events: true })
        .build();
    let mut stream = timeline.subscribe().await;

    let event_b = event_id!("$event_b");

    // Alice sends 2 messages
    let f = &timeline.factory;
    timeline.handle_live_event(f.text_msg("A").sender(*ALICE)).await;
    timeline.handle_live_event(f.text_msg("B").sender(*ALICE).event_id(event_b)).await;

    assert_next_matches!(stream, VectorDiff::PushBack { .. });
    assert_next_matches!(stream, VectorDiff::PushFront { .. });
    assert_next_matches!(stream, VectorDiff::PushBack { .. });
    assert_pending!(stream);

    // Bob reads the last one from an unthreaded client
    timeline
        .handle_read_receipts([(
            event_b.to_owned(),
            ReceiptType::Read,
            BOB.to_owned(),
            ReceiptThread::Unthreaded,
        )])
        .await;

    // Alice's timeline gets updated
    let item_b = assert_next_matches!(stream, VectorDiff::Set { index: 2, value } => value);
    let event_b = item_b.as_event().unwrap();
    assert_eq!(event_b.read_receipts().len(), 1);
    assert!(event_b.read_receipts().get(*BOB).is_some());
    assert_pending!(stream);

    // Then Alice sends a message in a thread
    let event_c = event_id!("$event_c");
    let thread_event_id = event_id!("$thread");

    timeline
        .handle_live_event(
            f.text_msg("C")
                .sender(*ALICE)
                .event_id(event_c)
                .in_thread(thread_event_id, event_id!("$latest")),
        )
        .await;

    // Alice is using a threaded client so the main timeline shouldn't change
    assert_pending!(stream);

    // Bob reads the threaded message
    timeline
        .handle_read_receipts([(
            event_c.to_owned(),
            ReceiptType::Read,
            BOB.to_owned(),
            ReceiptThread::Thread(thread_event_id.to_owned()),
        )])
        .await;

    // The main timeline read receipts are still correct
    let event_b = timeline.controller.items().await[2].as_event().unwrap().to_owned();
    assert_eq!(event_b.read_receipts().len(), 1);
    assert!(event_b.read_receipts().get(*BOB).is_some());

    assert_pending!(stream);
}

#[async_test]
async fn test_recovery_receipt_ack_restores_live_semantics_and_stops_store_reads() {
    use std::sync::atomic::Ordering;
    let a = event_id!("$recovery-receipt-a");
    let b = event_id!("$recovery-receipt-b");
    let gap = event_id!("$receipt-in-unloaded-gap");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(BOB.to_owned(), (a.to_owned(), Receipt::default()));
    let provider = TestRoomDataProvider::default().with_initial_user_receipts(receipts);
    let timeline = TestTimelineBuilder::new()
        .provider(provider.clone())
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    let snapshot = vec![
        timeline.factory.text_msg("a").sender(*CAROL).event_id(a).into_event(),
        timeline.factory.text_msg("b").sender(*CAROL).event_id(b).into_event(),
    ];
    timeline
        .controller
        .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
        .await;
    let payload = |id| {
        timeline
            .factory
            .read_receipts()
            .add_with_timestamp(id, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
            .into_event()
            .into_raw()
    };
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(a)]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
    provider
        .receipt_overrides
        .write()
        .await
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(BOB.to_owned(), (gap.to_owned(), Receipt::default()));
    timeline.controller.handle_ephemeral_events(vec![payload(gap)]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
    assert_eq!(
        timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0,
        a,
        "after acknowledging recovery, an unloaded target must retain ordinary live semantics"
    );
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(b)]).await;
    assert_eq!(
        provider.receipt_loads.load(Ordering::Relaxed),
        before,
        "acknowledged receipt key must not keep consulting the store"
    );
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, b);
    // A new rebuild reopens this key's reconciliation independently.
    timeline
        .controller
        .replace_remote_events_after_cache_lag(snapshot, RemoteEventOrigin::Cache)
        .await;
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(a)]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, gap);
}

#[async_test]
async fn test_recovery_multi_key_receipt_edu_preserves_mixed_and_live_rules() {
    use std::sync::atomic::Ordering;

    let a = event_id!("$receipt-edu-a");
    let b = event_id!("$receipt-edu-b");
    let c = event_id!("$receipt-edu-c");
    let gap = event_id!("$receipt-edu-gap");
    let mut receipts = ReadReceiptMap::new();
    for user in [*BOB, *CAROL] {
        receipts
            .entry(ReceiptType::Read)
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(user.to_owned(), (a.to_owned(), Receipt::default()));
    }
    let provider = TestRoomDataProvider::default().with_initial_user_receipts(receipts);
    let timeline = TestTimelineBuilder::new()
        .provider(provider.clone())
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .controller
        .replace_remote_events_after_cache_lag(
            [a, b, c].map(|id| {
                timeline.factory.text_msg("message").sender(*ALICE).event_id(id).into_event()
            }),
            RemoteEventOrigin::Cache,
        )
        .await;
    timeline
        .controller
        .handle_ephemeral_events(vec![
            timeline
                .factory
                .read_receipts()
                .add_with_timestamp(a, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
                .into_event()
                .into_raw(),
        ])
        .await;
    for user in [*BOB, *CAROL] {
        provider
            .receipt_overrides
            .write()
            .await
            .entry(ReceiptType::Read)
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(user.to_owned(), (c.to_owned(), Receipt::default()));
    }
    let payload = |bob, carol| {
        timeline
            .factory
            .read_receipts()
            .add_with_timestamp(bob, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
            .add_with_timestamp(carol, *CAROL, ReceiptType::Read, ReceiptThread::Unthreaded, None)
            .into_event()
            .into_raw()
    };
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(b, a)]).await;
    assert_eq!(
        provider.receipt_loads.load(Ordering::Relaxed),
        before + 1,
        "only the unacknowledged key in a mixed EDU needs the store"
    );
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, b);
    assert_eq!(timeline.controller.latest_user_read_receipt(*CAROL).await.unwrap().0, c);
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(b, c)]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
    // Both keys are now confirmed. The entire next EDU uses live ordering and
    // gap rules, even when the base store disagrees with its payload.
    for user in [*BOB, *CAROL] {
        provider
            .receipt_overrides
            .write()
            .await
            .entry(ReceiptType::Read)
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(user.to_owned(), (a.to_owned(), Receipt::default()));
    }
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(c, gap)]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before);
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, c);
    assert_eq!(timeline.controller.latest_user_read_receipt(*CAROL).await.unwrap().0, c);
}

#[async_test]
async fn test_recovery_marker_store_read_does_not_lock_or_overwrite_concurrent_state() {
    use std::{sync::atomic::Ordering, time::Duration};

    use tokio::{sync::oneshot, time::timeout};

    for concurrent_change in ["rebuild", "clear", "marker", "same_marker"] {
        let a = event_id!("$marker-race-a");
        let b = event_id!("$marker-race-b");
        let provider = TestRoomDataProvider::default().with_fully_read_marker(a.to_owned());
        let timeline = TestTimelineBuilder::new()
            .provider(provider.clone())
            .settings(TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            })
            .build();
        let snapshot = vec![
            timeline.factory.text_msg("a").sender(*CAROL).event_id(a).into_event(),
            timeline.factory.text_msg("b").sender(*CAROL).event_id(b).into_event(),
            timeline.factory.text_msg("after").sender(*CAROL).into_event(),
        ];
        timeline
            .controller
            .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
            .await;
        // Also cover a store A -> B -> A cycle: the second notification of A
        // must invalidate an in-flight read of B even though the item stays put.
        let expected = if concurrent_change == "same_marker" {
            *provider.marker_override.write().await = Some(b.to_owned());
            a
        } else {
            b
        };
        let (started_tx, started_rx) = oneshot::channel();
        let (resume_tx, resume_rx) = oneshot::channel();
        *provider.marker_load_gate.lock().await = Some((started_tx, resume_rx));
        let before = provider.marker_loads.load(Ordering::Relaxed);
        let pending = timeline.controller.handle_fully_read_marker(a.to_owned());
        tokio::pin!(pending);
        tokio::select! {
            result = started_rx => result.unwrap(),
            _ = &mut pending => panic!("marker update finished before its store read"),
        }
        // A suspended store read must neither hold a write lock nor prevent a
        // new recovery/control update. Its result will be stale when resumed.
        timeout(Duration::from_secs(1), timeline.controller.items()).await.unwrap();
        *provider.marker_override.write().await = Some(expected.to_owned());
        timeout(Duration::from_secs(1), async {
            match concurrent_change {
                "rebuild" => {
                    timeline
                        .controller
                        .replace_remote_events_after_cache_lag(
                            snapshot.clone(),
                            RemoteEventOrigin::Cache,
                        )
                        .await;
                }
                "clear" => {
                    timeline
                        .controller
                        .handle_remote_events_with_diffs(
                            vec![
                                VectorDiff::Clear,
                                VectorDiff::Append { values: snapshot.into_iter().collect() },
                            ],
                            RemoteEventOrigin::Sync,
                        )
                        .await;
                }
                "marker" | "same_marker" => {
                    timeline.controller.handle_fully_read_marker(expected.to_owned()).await;
                }
                _ => unreachable!(),
            }
        })
        .await
        .unwrap();
        let after_change = provider.marker_loads.load(Ordering::Relaxed);
        assert!(after_change > before);
        resume_tx.send(()).unwrap();
        timeout(Duration::from_secs(1), pending).await.unwrap();
        assert_eq!(
            provider.marker_loads.load(Ordering::Relaxed),
            after_change + 1,
            "store must be re-read after {concurrent_change}"
        );
        let items = timeline.controller.items().await;
        let marker = items.iter().position(|item| item.is_read_marker()).unwrap();
        assert_eq!(
            items[marker - 1].as_event().unwrap().event_id(),
            Some(expected),
            "stale store read undid {concurrent_change}"
        );
    }
}

#[async_test]
async fn test_recovery_marker_same_value_is_not_a_revision_fence() {
    use std::sync::atomic::Ordering;
    let a = event_id!("$marker-a");
    let b = event_id!("$marker-b");
    let provider = TestRoomDataProvider::default().with_fully_read_marker(a.to_owned());
    let timeline = TestTimelineBuilder::new()
        .provider(provider.clone())
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    let snapshot = vec![
        timeline.factory.text_msg("a").sender(*CAROL).event_id(a).into_event(),
        timeline.factory.text_msg("b").sender(*CAROL).event_id(b).into_event(),
        timeline.factory.text_msg("after").sender(*CAROL).into_event(),
    ];
    // Ordinary live marker delivery does not consult the store.
    timeline
        .controller
        .replace_with_initial_remote_events(snapshot.clone(), RemoteEventOrigin::Cache)
        .await;
    let before = provider.marker_loads.load(Ordering::Relaxed);
    timeline.controller.handle_fully_read_marker(b.to_owned()).await;
    assert_eq!(provider.marker_loads.load(Ordering::Relaxed), before);
    for _ in 0..2 {
        timeline
            .controller
            .replace_remote_events_after_cache_lag(snapshot.clone(), RemoteEventOrigin::Cache)
            .await;
        // An ordinary cache Clear/Append is not a new receiver boundary.
        timeline
            .controller
            .handle_remote_events_with_diffs(
                vec![
                    VectorDiff::Clear,
                    VectorDiff::Append { values: snapshot.clone().into_iter().collect() },
                ],
                RemoteEventOrigin::Sync,
            )
            .await;
        // The store already contains the final A of queued A -> B -> A.
        // Equality with the first A cannot acknowledge a causal boundary.
        for id in [a, b, a] {
            timeline.controller.handle_fully_read_marker(id.to_owned()).await;
            let items = timeline.controller.items().await;
            let marker = items.iter().position(|item| item.is_read_marker()).unwrap();
            assert_eq!(items[marker - 1].as_event().unwrap().event_id(), Some(a));
        }
    }
}

#[async_test]
async fn test_recovery_ack_is_scoped_to_user_type_and_thread() {
    use std::sync::atomic::Ordering;
    let a = event_id!("$ack-key-a");
    let future = event_id!("$ack-key-future");
    for (second_user, second_type, second_thread) in [
        (CAROL.to_owned(), ReceiptType::Read, ReceiptThread::Unthreaded),
        (BOB.to_owned(), ReceiptType::ReadPrivate, ReceiptThread::Unthreaded),
        (BOB.to_owned(), ReceiptType::Read, ReceiptThread::Main),
    ] {
        let mut receipts = ReadReceiptMap::new();
        receipts
            .entry(ReceiptType::Read)
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(BOB.to_owned(), (a.to_owned(), Receipt::default()));
        let mut stored = Receipt::default();
        stored.thread = second_thread.clone();
        receipts
            .entry(second_type.clone())
            .or_default()
            .entry(second_thread.clone())
            .or_default()
            .insert(second_user.clone(), (future.to_owned(), stored));
        let provider = TestRoomDataProvider::default().with_initial_user_receipts(receipts);
        let timeline = TestTimelineBuilder::new().provider(provider.clone()).build();
        timeline
            .controller
            .replace_remote_events_after_cache_lag(
                [timeline.factory.text_msg("snapshot").sender(*ALICE).event_id(a).into_event()],
                RemoteEventOrigin::Cache,
            )
            .await;
        let before = provider.receipt_loads.load(Ordering::Relaxed);
        let first = timeline
            .factory
            .read_receipts()
            .add_with_timestamp(a, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
            .into_event()
            .into_raw();
        timeline.controller.handle_ephemeral_events(vec![first]).await;
        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
        let second = timeline
            .factory
            .read_receipts()
            .add_with_timestamp(a, &second_user, second_type.clone(), second_thread.clone(), None)
            .into_event()
            .into_raw();
        timeline.controller.handle_ephemeral_events(vec![second]).await;
        assert_eq!(
            provider.receipt_loads.load(Ordering::Relaxed),
            before + 2,
            "another user/type/thread must retain its own recovery boundary"
        );
        let matching = timeline
            .factory
            .read_receipts()
            .add_with_timestamp(future, &second_user, second_type, second_thread, None)
            .into_event()
            .into_raw();
        timeline.controller.handle_ephemeral_events(vec![matching.clone()]).await;
        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 3);
        timeline.controller.handle_ephemeral_events(vec![matching]).await;
        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 3);
    }
}

#[async_test]
async fn test_first_matching_receipt_after_rebuild_uses_live_gap_rules() {
    use std::sync::atomic::Ordering;
    let a = event_id!("$matching-receipt-a");
    let gap = event_id!("$matching-receipt-gap");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(BOB.to_owned(), (gap.to_owned(), Receipt::default()));
    let provider = TestRoomDataProvider::default().with_initial_user_receipts(receipts);
    let timeline = TestTimelineBuilder::new()
        .provider(provider.clone())
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .controller
        .replace_remote_events_after_cache_lag(
            [timeline
                .factory
                .text_msg("visible implicit receipt")
                .sender(*BOB)
                .event_id(a)
                .into_event()],
            RemoteEventOrigin::Cache,
        )
        .await;
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    let payload = timeline
        .factory
        .read_receipts()
        .add_with_timestamp(gap, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
        .into_event()
        .into_raw();
    timeline.controller.handle_ephemeral_events(vec![payload.clone()]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
    timeline.controller.handle_ephemeral_events(vec![payload]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, a);
}

#[async_test]
async fn test_recovery_matching_new_receipt_releases_previous_pending_target() {
    use std::sync::atomic::Ordering;

    use ruma::MilliSecondsSinceUnixEpoch;

    let old_id = event_id!("$ack-previous-unloaded-target");
    let new_id = event_id!("$ack-current-target");
    for receipt_type in [ReceiptType::Read, ReceiptType::ReadPrivate] {
        for thread in [ReceiptThread::Unthreaded, ReceiptThread::Main] {
            for target_loaded in [false, true] {
                for (old_ts, new_ts) in [
                    (None, None),
                    (
                        Some(MilliSecondsSinceUnixEpoch(uint!(10))),
                        Some(MilliSecondsSinceUnixEpoch(uint!(10))),
                    ),
                    (
                        Some(MilliSecondsSinceUnixEpoch(uint!(10))),
                        Some(MilliSecondsSinceUnixEpoch(uint!(20))),
                    ),
                ] {
                    let mut old_receipt = Receipt::default();
                    old_receipt.ts = old_ts;
                    old_receipt.thread = thread.clone();
                    let mut new_receipt = Receipt::default();
                    new_receipt.ts = new_ts;
                    new_receipt.thread = thread.clone();
                    let provider = TestRoomDataProvider::default();
                    let timeline = TestTimelineBuilder::new()
                        .provider(provider.clone())
                        .settings(TimelineSettings {
                            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                            ..Default::default()
                        })
                        .build();
                    let snapshot_id =
                        if target_loaded { new_id } else { event_id!("$ack-other-visible-event") };
                    let snapshot = vec![
                        timeline
                            .factory
                            .text_msg("snapshot")
                            .sender(*BOB)
                            .event_id(snapshot_id)
                            .into_event(),
                    ];
                    let payload = timeline
                        .factory
                        .read_receipts()
                        .add_with_timestamp(
                            new_id,
                            *ALICE,
                            receipt_type.clone(),
                            thread.clone(),
                            new_ts,
                        )
                        .into_event()
                        .into_raw();

                    // A subsequent lag must reopen reconciliation for this key.
                    for cycle in 0..2 {
                        provider
                            .receipt_overrides
                            .write()
                            .await
                            .entry(receipt_type.clone())
                            .or_default()
                            .entry(thread.clone())
                            .or_default()
                            .insert(ALICE.to_owned(), (old_id.to_owned(), old_receipt.clone()));
                        timeline
                            .controller
                            .replace_remote_events_after_cache_lag(
                                snapshot.clone(),
                                RemoteEventOrigin::Cache,
                            )
                            .await;
                        assert_eq!(
                            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                            old_id
                        );
                        provider
                            .receipt_overrides
                            .write()
                            .await
                            .entry(receipt_type.clone())
                            .or_default()
                            .entry(thread.clone())
                            .or_default()
                            .insert(ALICE.to_owned(), (new_id.to_owned(), new_receipt.clone()));

                        let before = provider.receipt_loads.load(Ordering::Relaxed);
                        timeline.controller.handle_ephemeral_events(vec![payload.clone()]).await;
                        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
                        assert_eq!(
                            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                            new_id,
                            "confirmed replacement: {receipt_type}, {thread:?}, \
                             loaded={target_loaded}, ts={old_ts:?}->{new_ts:?}, cycle={cycle}"
                        );

                        // Repeated delivery stays on the normal batch path.
                        // Exclude the latest-receipt getter's fallback reads for
                        // the other receipt type from this I/O assertion.
                        let before = provider.receipt_loads.load(Ordering::Relaxed);
                        timeline
                            .controller
                            .handle_ephemeral_events(vec![payload.clone(); 1000])
                            .await;
                        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before);
                        assert_eq!(
                            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                            new_id
                        );
                    }
                }
            }
        }
    }
}

#[async_test]
async fn test_recovery_ack_for_another_key_keeps_pending_receipt_protected() {
    let pending_id = event_id!("$protected-unthreaded-receipt");
    let other_id = event_id!("$other-acknowledged-receipt");
    for (user, receipt_type, thread) in [
        (BOB.to_owned(), ReceiptType::Read, ReceiptThread::Unthreaded),
        (ALICE.to_owned(), ReceiptType::ReadPrivate, ReceiptThread::Unthreaded),
        (ALICE.to_owned(), ReceiptType::Read, ReceiptThread::Main),
    ] {
        let mut receipts = ReadReceiptMap::new();
        receipts
            .entry(ReceiptType::Read)
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(ALICE.to_owned(), (pending_id.to_owned(), Receipt::default()));
        let mut other_receipt = Receipt::default();
        other_receipt.thread = thread.clone();
        receipts
            .entry(receipt_type.clone())
            .or_default()
            .entry(thread.clone())
            .or_default()
            .insert(user.clone(), (other_id.to_owned(), other_receipt));
        let timeline = TestTimelineBuilder::new()
            .provider(TestRoomDataProvider::default().with_initial_user_receipts(receipts))
            .settings(TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            })
            .build();
        timeline
            .controller
            .replace_remote_events_after_cache_lag(
                [timeline
                    .factory
                    .text_msg("snapshot")
                    .sender(*CAROL)
                    .event_id(other_id)
                    .into_event()],
                RemoteEventOrigin::Cache,
            )
            .await;
        timeline
            .controller
            .handle_ephemeral_events(vec![
                timeline
                    .factory
                    .read_receipts()
                    .add_with_timestamp(other_id, &user, receipt_type, thread, None)
                    .into_event()
                    .into_raw(),
            ])
            .await;
        timeline.handle_live_event(timeline.factory.text_msg("before target").sender(*ALICE)).await;
        assert_eq!(
            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
            pending_id,
            "acknowledging a different key must not release the pending public receipt"
        );

        // Its own acknowledgement still releases it.
        timeline
            .controller
            .handle_ephemeral_events(vec![
                timeline
                    .factory
                    .read_receipts()
                    .add_with_timestamp(
                        pending_id,
                        *ALICE,
                        ReceiptType::Read,
                        ReceiptThread::Unthreaded,
                        None,
                    )
                    .into_event()
                    .into_raw(),
            ])
            .await;
        let after_id = event_id!("$own-message-after-ack");
        timeline
            .handle_live_event(
                timeline
                    .factory
                    .text_msg("after acknowledgement")
                    .sender(*ALICE)
                    .event_id(after_id),
            )
            .await;
        assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, after_id);
    }
}

#[async_test]
async fn test_recovery_matching_public_receipt_updates_items_then_uses_live_diffs() {
    use std::sync::atomic::Ordering;

    let pending_id = event_id!("$public-pending-target");
    let target = event_id!("$public-confirmed-target");
    let next = event_id!("$public-live-target");
    let mut receipts = ReadReceiptMap::new();
    receipts
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(BOB.to_owned(), (pending_id.to_owned(), Receipt::default()));
    let provider = TestRoomDataProvider::default().with_initial_user_receipts(receipts);
    let timeline = TestTimelineBuilder::new()
        .provider(provider.clone())
        .settings(TimelineSettings {
            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
            ..Default::default()
        })
        .build();
    timeline
        .controller
        .replace_remote_events_after_cache_lag(
            [target, next].map(|id| {
                timeline.factory.text_msg("snapshot").sender(*CAROL).event_id(id).into_event()
            }),
            RemoteEventOrigin::Cache,
        )
        .await;
    let payload = |id| {
        timeline
            .factory
            .read_receipts()
            .add_with_timestamp(id, *BOB, ReceiptType::Read, ReceiptThread::Unthreaded, None)
            .into_event()
            .into_raw()
    };
    // The store is ahead of this queued payload, restoring an unknown target.
    timeline.controller.handle_ephemeral_events(vec![payload(target)]).await;
    assert_eq!(timeline.controller.latest_user_read_receipt(*BOB).await.unwrap().0, pending_id);
    provider
        .receipt_overrides
        .write()
        .await
        .entry(ReceiptType::Read)
        .or_default()
        .entry(ReceiptThread::Unthreaded)
        .or_default()
        .insert(BOB.to_owned(), (target.to_owned(), Receipt::default()));
    let (_, mut stream) = timeline.controller.subscribe_raw().await;
    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(target)]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
    assert_next_matches!(stream, VectorDiff::Set { value, .. } => {
        let event = value.as_event().unwrap();
        assert_eq!(event.event_id(), Some(target));
        assert!(event.read_receipts().contains_key(*BOB));
    });
    assert_pending!(stream);

    let before = provider.receipt_loads.load(Ordering::Relaxed);
    timeline.controller.handle_ephemeral_events(vec![payload(target); 1000]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before);
    assert_pending!(stream);

    // After acknowledgement the store is no longer consulted, and the next
    // live receipt still removes the old avatar and adds it at the new target.
    timeline.controller.handle_ephemeral_events(vec![payload(next)]).await;
    assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before);
    assert_next_matches!(stream, VectorDiff::Set { value, .. } => {
        let event = value.as_event().unwrap();
        assert_eq!(event.event_id(), Some(target));
        assert!(!event.read_receipts().contains_key(*BOB));
    });
    assert_next_matches!(stream, VectorDiff::Set { value, .. } => {
        let event = value.as_event().unwrap();
        assert_eq!(event.event_id(), Some(next));
        assert!(event.read_receipts().contains_key(*BOB));
    });
    assert_pending!(stream);
}

#[async_test]
async fn test_recovery_unthreaded_ack_releases_own_main_fallback() {
    use std::sync::atomic::Ordering;

    use ruma::MilliSecondsSinceUnixEpoch;

    let main_id = event_id!("$pending-main-fallback");
    let new_id = event_id!("$confirmed-unthreaded-target");
    for receipt_type in [ReceiptType::Read, ReceiptType::ReadPrivate] {
        for target_loaded in [false, true] {
            for (old_ts, new_ts) in [
                (None, None),
                (
                    Some(MilliSecondsSinceUnixEpoch(uint!(10))),
                    Some(MilliSecondsSinceUnixEpoch(uint!(10))),
                ),
                (
                    Some(MilliSecondsSinceUnixEpoch(uint!(10))),
                    Some(MilliSecondsSinceUnixEpoch(uint!(20))),
                ),
            ] {
                for refresh_main in [false, true] {
                    let mut main = Receipt::default();
                    main.thread = ReceiptThread::Main;
                    main.ts = old_ts;
                    let mut unthreaded = Receipt::default();
                    unthreaded.ts = new_ts;
                    let provider = TestRoomDataProvider::default();
                    let timeline = TestTimelineBuilder::new()
                        .provider(provider.clone())
                        .settings(TimelineSettings {
                            track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                            ..Default::default()
                        })
                        .build();
                    let snapshot_id = if target_loaded {
                        new_id
                    } else {
                        event_id!("$fallback-other-visible-event")
                    };
                    let snapshot = vec![
                        timeline
                            .factory
                            .text_msg("snapshot")
                            .sender(*BOB)
                            .event_id(snapshot_id)
                            .into_event(),
                    ];
                    let payload = timeline
                        .factory
                        .read_receipts()
                        .add_with_timestamp(
                            new_id,
                            *ALICE,
                            receipt_type.clone(),
                            ReceiptThread::Unthreaded,
                            new_ts,
                        )
                        .into_event()
                        .into_raw();

                    for cycle in 0..2 {
                        // No unthreaded receipt: recovery must use Main as a fallback.
                        let mut stored = provider.receipt_overrides.write().await;
                        stored.clear();
                        stored
                            .entry(receipt_type.clone())
                            .or_default()
                            .entry(ReceiptThread::Main)
                            .or_default()
                            .insert(ALICE.to_owned(), (main_id.to_owned(), main.clone()));
                        drop(stored);
                        timeline
                            .controller
                            .replace_remote_events_after_cache_lag(
                                snapshot.clone(),
                                RemoteEventOrigin::Cache,
                            )
                            .await;
                        assert_eq!(
                            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                            main_id
                        );

                        // Without a newer timestamp, an unconfirmed payload must retain the fallback.
                        timeline.controller.handle_ephemeral_events(vec![payload.clone()]).await;
                        assert_eq!(
                            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                            if new_ts > old_ts { new_id } else { main_id }
                        );

                        if refresh_main {
                            // A stale queued Main payload reads the current Main value
                            // from the store. Its provenance must remain a fallback.
                            timeline
                                .controller
                                .handle_ephemeral_events(vec![
                                    timeline
                                        .factory
                                        .read_receipts()
                                        .add_with_timestamp(
                                            event_id!("$stale-main-payload"),
                                            *ALICE,
                                            receipt_type.clone(),
                                            ReceiptThread::Main,
                                            None,
                                        )
                                        .into_event()
                                        .into_raw(),
                                ])
                                .await;
                        }
                        provider
                            .receipt_overrides
                            .write()
                            .await
                            .entry(receipt_type.clone())
                            .or_default()
                            .entry(ReceiptThread::Unthreaded)
                            .or_default()
                            .insert(ALICE.to_owned(), (new_id.to_owned(), unthreaded.clone()));
                        let before = provider.receipt_loads.load(Ordering::Relaxed);
                        timeline.controller.handle_ephemeral_events(vec![payload.clone()]).await;
                        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
                        assert_eq!(
                            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                            new_id,
                            "Main fallback -> Unthreaded: {receipt_type}, loaded={target_loaded}, \
                             ts={old_ts:?}->{new_ts:?}, refresh_main={refresh_main}, cycle={cycle}"
                        );

                        let before = provider.receipt_loads.load(Ordering::Relaxed);
                        timeline
                            .controller
                            .handle_ephemeral_events(vec![payload.clone(); 1000])
                            .await;
                        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before);
                        assert_eq!(
                            timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                            new_id
                        );
                    }
                }
            }
        }
    }
}

#[async_test]
async fn test_recovery_unthreaded_ack_does_not_release_main_timeline_receipt() {
    let main_id = event_id!("$protected-main-receipt");
    let new_id = event_id!("$confirmed-other-thread");
    for receipt_type in [ReceiptType::Read, ReceiptType::ReadPrivate] {
        let mut main = Receipt::default();
        main.thread = ReceiptThread::Main;
        let provider = TestRoomDataProvider::default();
        provider
            .receipt_overrides
            .write()
            .await
            .entry(receipt_type.clone())
            .or_default()
            .entry(ReceiptThread::Main)
            .or_default()
            .insert(ALICE.to_owned(), (main_id.to_owned(), main.clone()));
        let timeline = TestTimelineBuilder::new()
            .provider(provider.clone())
            .focus(TimelineFocus::Live { hide_threaded_events: true })
            .settings(TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            })
            .build();
        timeline
            .controller
            .replace_remote_events_after_cache_lag(
                [timeline.factory.text_msg("snapshot").sender(*BOB).event_id(new_id).into_event()],
                RemoteEventOrigin::Cache,
            )
            .await;
        provider
            .receipt_overrides
            .write()
            .await
            .entry(receipt_type.clone())
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(ALICE.to_owned(), (new_id.to_owned(), Receipt::default()));
        timeline
            .controller
            .handle_ephemeral_events(vec![
                timeline
                    .factory
                    .read_receipts()
                    .add_with_timestamp(
                        new_id,
                        *ALICE,
                        receipt_type.clone(),
                        ReceiptThread::Unthreaded,
                        None,
                    )
                    .into_event()
                    .into_raw(),
            ])
            .await;
        assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, main_id);

        // Main was the requested key, not a fallback. Only its own confirmed
        // replacement releases protection when no timestamp orders the targets.
        provider
            .receipt_overrides
            .write()
            .await
            .entry(receipt_type.clone())
            .or_default()
            .entry(ReceiptThread::Main)
            .or_default()
            .insert(ALICE.to_owned(), (new_id.to_owned(), main));
        timeline
            .controller
            .handle_ephemeral_events(vec![
                timeline
                    .factory
                    .read_receipts()
                    .add_with_timestamp(new_id, *ALICE, receipt_type, ReceiptThread::Main, None)
                    .into_event()
                    .into_raw(),
            ])
            .await;
        assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, new_id);
    }
}

#[async_test]
async fn test_recovery_ack_uses_restored_thread_when_target_does_not_change() {
    use std::sync::atomic::Ordering;

    let pending_id = event_id!("$same-target-new-receipt-thread");
    let next_id = event_id!("$next-live-unthreaded-target");
    for receipt_type in [ReceiptType::Read, ReceiptType::ReadPrivate] {
        let mut main = Receipt::default();
        main.thread = ReceiptThread::Main;
        let provider = TestRoomDataProvider::default();
        provider
            .receipt_overrides
            .write()
            .await
            .entry(receipt_type.clone())
            .or_default()
            .entry(ReceiptThread::Main)
            .or_default()
            .insert(ALICE.to_owned(), (pending_id.to_owned(), main));
        let timeline = TestTimelineBuilder::new()
            .provider(provider.clone())
            .settings(TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            })
            .build();
        timeline
            .controller
            .replace_remote_events_after_cache_lag(
                [timeline.factory.text_msg("snapshot").sender(*BOB).event_id(next_id).into_event()],
                RemoteEventOrigin::Cache,
            )
            .await;
        provider
            .receipt_overrides
            .write()
            .await
            .entry(receipt_type.clone())
            .or_default()
            .entry(ReceiptThread::Unthreaded)
            .or_default()
            .insert(ALICE.to_owned(), (pending_id.to_owned(), Receipt::default()));

        // A stale notification restores an Unthreaded receipt at the same
        // event ID. Same-event updates need not replace the cached item receipt.
        for event_id in [event_id!("$stale-unthreaded-payload"), pending_id] {
            let before = provider.receipt_loads.load(Ordering::Relaxed);
            timeline
                .controller
                .handle_ephemeral_events(vec![
                    timeline
                        .factory
                        .read_receipts()
                        .add_with_timestamp(
                            event_id,
                            *ALICE,
                            receipt_type.clone(),
                            ReceiptThread::Unthreaded,
                            None,
                        )
                        .into_event()
                        .into_raw(),
                ])
                .await;
            assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before + 1);
            assert_eq!(
                timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0,
                pending_id
            );
        }

        // Confirmation must release the protection, not just stop store reads.
        let before = provider.receipt_loads.load(Ordering::Relaxed);
        timeline
            .controller
            .handle_ephemeral_events(vec![
                timeline
                    .factory
                    .read_receipts()
                    .add_with_timestamp(
                        next_id,
                        *ALICE,
                        receipt_type,
                        ReceiptThread::Unthreaded,
                        None,
                    )
                    .into_event()
                    .into_raw(),
            ])
            .await;
        assert_eq!(provider.receipt_loads.load(Ordering::Relaxed), before);
        assert_eq!(timeline.controller.latest_user_read_receipt(*ALICE).await.unwrap().0, next_id);
    }
}
