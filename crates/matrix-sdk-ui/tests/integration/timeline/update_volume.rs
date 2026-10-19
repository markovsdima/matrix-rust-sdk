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

//! Diagnostic reproduction against the release SDK; no homeserver or keys needed.
//! Run with `cargo test -p matrix-sdk-ui --test integration update_volume -- --ignored --nocapture`.
//! JSON lines beginning UPDATE_VOLUME are the measurements, not estimates.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use eyeball_im::VectorDiff;
use futures_util::StreamExt;
use matrix_sdk::test_utils::mocks::{MatrixMockServer, RoomMessagesResponseTemplate};
use matrix_sdk_test::{
    ALICE, BOB, JoinedRoomBuilder,
    event_factory::{EventBuilder, EventFactory},
};
use matrix_sdk_ui::timeline::{RoomExt, Timeline, TimelineItem, TimelineReadReceiptTracking};
use ruma::{
    OwnedEventId,
    events::{
        AnySyncMessageLikeEvent, AnySyncTimelineEvent,
        receipt::{ReceiptThread, ReceiptType},
        room::{
            member::MembershipState,
            message::{MessageType, RoomMessageEventContent, SyncRoomMessageEvent},
        },
    },
    room_id,
};
use serde::Serialize;
use serde_json::json;
use tokio::{
    task::JoinHandle,
    time::{sleep, timeout},
};

type Diff = VectorDiff<Arc<TimelineItem>>;
type Fingerprint = BTreeMap<&'static str, String>;

fn event_id(i: usize) -> OwnedEventId {
    format!("$volume-{i}").try_into().unwrap()
}

fn history_event(f: &EventFactory, i: usize) -> EventBuilder<RoomMessageEventContent> {
    let event = if i.is_multiple_of(8) {
        f.image(format!("image-{i}.jpg"), "mxc://example.org/image".into())
    } else {
        f.text_msg(format!("message-{i}"))
    };
    event.event_id(&event_id(i)).server_ts(1_700_000_000_000u64 + i as u64)
}

// Compare the exported fields below, excluding Arc identity and internal timeline ID.
// These fingerprints cover this plaintext fixture, not private SDK bookkeeping.
// Separate fields let us distinguish receipt motion, profiles and actual content.
fn fingerprint(item: &TimelineItem) -> Fingerprint {
    let Some(event) = item.as_event() else {
        return BTreeMap::from([("virtual", format!("{item:?}"))]);
    };
    let receipts: BTreeMap<_, _> = event
        .read_receipts()
        .iter()
        .map(|(user, receipt)| (user.to_string(), format!("{receipt:?}")))
        .collect();
    // Message::Debug intentionally omits the payload for privacy. Include it
    // explicitly here; all bodies/media URIs in this fixture are synthetic.
    let message = event.content().as_message().map(|message| {
        json!({
            "msgtype": message.msgtype(), "mentions": message.mentions(),
            "edited": message.is_edited(),
        })
    });
    BTreeMap::from([
        ("content", format!("{:?}|{message:?}", event.content())),
        ("sender_profile", format!("{:?}", event.sender_profile())),
        ("receipts", format!("{receipts:?}")),
        (
            "encryption",
            format!(
                "{:?}",
                (event.encryption_info(), event.get_shield(false), event.get_shield(true))
            ),
        ),
        (
            "original_json",
            event.original_json().map(|raw| raw.json().get().to_owned()).unwrap_or_default(),
        ),
        (
            "edit_json",
            event.latest_edit_json().map(|raw| raw.json().get().to_owned()).unwrap_or_default(),
        ),
        ("origin", format!("{:?}", event.origin())),
        (
            "other",
            format!(
                "{:?}",
                (
                    event.identifier(),
                    event.transaction_id(),
                    event.sender(),
                    event.timestamp(),
                    event.is_own(),
                    event.is_highlighted(),
                    event.send_state(),
                    event.local_created_at(),
                    event.forwarder(),
                    event.forwarder_profile()
                )
            ),
        ),
    ])
}

#[derive(Default, Serialize)]
struct Metrics {
    batches: usize,
    operations: usize,
    largest_batch: usize,
    kinds: BTreeMap<&'static str, usize>,
    event_occurrences: usize,
    virtual_occurrences: usize,
    unique_ids: usize,
    max_occurrences_one_id: usize,
    repeated_ids: usize,
    repeated_identical_data: usize,
    sets_identical_data: usize,
    set_field_changes: BTreeMap<&'static str, usize>,
    occurrences_per_id_histogram: BTreeMap<usize, usize>,
}

struct Observer {
    pending: Arc<Mutex<Vec<Vec<Diff>>>>,
    task: JoinHandle<()>,
    mirror: imbl::Vector<Arc<TimelineItem>>,
    previous: BTreeMap<String, Fingerprint>,
}

impl Observer {
    async fn new(timeline: &Timeline) -> Self {
        let (mirror, mut stream) = timeline.subscribe().await;
        let pending = Arc::new(Mutex::new(Vec::new()));
        let target = pending.clone();
        // Match a nonblocking FFI callback: enqueue whole batches immediately.
        // The diagnostic processing itself must not make the SDK subscriber lag.
        let task = tokio::spawn(async move {
            while let Some(batch) = stream.next().await {
                target.lock().unwrap().push(batch);
            }
        });
        let previous = mirror
            .iter()
            .filter_map(|item| {
                item.as_event()
                    .and_then(|event| event.event_id())
                    .map(|id| (id.to_string(), fingerprint(item)))
            })
            .collect();
        Self { pending, task, mirror, previous }
    }

    fn measure(&mut self) -> Metrics {
        let batches = std::mem::take(&mut *self.pending.lock().unwrap());
        let mut metrics = Metrics::default();
        let mut occurrences = BTreeMap::<String, usize>::new();
        for batch in batches {
            metrics.batches += 1;
            metrics.operations += batch.len();
            metrics.largest_batch = metrics.largest_batch.max(batch.len());
            for diff in batch {
                let kind = match &diff {
                    VectorDiff::Append { .. } => "append",
                    VectorDiff::Clear => "clear",
                    VectorDiff::PushFront { .. } => "push_front",
                    VectorDiff::PushBack { .. } => "push_back",
                    VectorDiff::PopFront => "pop_front",
                    VectorDiff::PopBack => "pop_back",
                    VectorDiff::Insert { .. } => "insert",
                    VectorDiff::Set { .. } => "set",
                    VectorDiff::Remove { .. } => "remove",
                    VectorDiff::Truncate { .. } => "truncate",
                    VectorDiff::Reset { .. } => "reset",
                };
                *metrics.kinds.entry(kind).or_default() += 1;
                if let VectorDiff::Set { index, value } = &diff {
                    let old = fingerprint(&self.mirror[*index]);
                    let new = fingerprint(value);
                    if old == new {
                        metrics.sets_identical_data += 1;
                    }
                    for (field, value) in &new {
                        if old.get(field) != Some(value) {
                            *metrics.set_field_changes.entry(field).or_default() += 1;
                        }
                    }
                }
                let values = match &diff {
                    VectorDiff::Append { values } | VectorDiff::Reset { values } => {
                        values.iter().collect::<Vec<_>>()
                    }
                    VectorDiff::PushFront { value }
                    | VectorDiff::PushBack { value }
                    | VectorDiff::Insert { value, .. }
                    | VectorDiff::Set { value, .. } => vec![value],
                    _ => Vec::new(),
                };
                for value in values {
                    if let Some(id) = value.as_event().and_then(|event| event.event_id()) {
                        metrics.event_occurrences += 1;
                        *occurrences.entry(id.to_string()).or_default() += 1;
                        let data = fingerprint(value);
                        if let Some(old) = self.previous.insert(id.to_string(), data.clone()) {
                            metrics.repeated_ids += 1;
                            metrics.repeated_identical_data += usize::from(old == data);
                        }
                    } else {
                        metrics.virtual_occurrences += 1;
                    }
                }
                diff.apply(&mut self.mirror);
            }
        }
        metrics.unique_ids = occurrences.len();
        metrics.max_occurrences_one_id = occurrences.values().copied().max().unwrap_or(0);
        for count in occurrences.into_values() {
            *metrics.occurrences_per_id_histogram.entry(count).or_default() += 1;
        }
        metrics
    }

    async fn checkpoint(
        &mut self,
        timeline: &Timeline,
        size: usize,
        mode: &str,
        phase: &str,
        who: &str,
    ) -> Metrics {
        // This is only a test settling window; production cannot use a fixed delay
        // as a delivery barrier for paginate_backwards.
        sleep(Duration::from_millis(80)).await;
        let metrics = self.measure();
        // Public subscriptions may skip an initial prefix (default: last 20
        // items); compare with a fresh subscriber, not the full internal items.
        let (actual, _) = timeline.subscribe().await;
        assert_eq!(self.mirror.len(), actual.len(), "mirror length after {phase}");
        for (from_stream, actual) in self.mirror.iter().zip(actual.iter()) {
            assert_eq!(fingerprint(from_stream), fingerprint(actual), "mirror data after {phase}");
        }
        println!(
            "UPDATE_VOLUME {}",
            json!({ "history": size, "mode": mode,
            "timeline": who, "phase": phase, "metrics": metrics })
        );
        metrics
    }
}

impl Drop for Observer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// Deliberately opt-in: emits diagnostic volume measurements.
// The separate cache-lag unit reproduction controls the Rust consumer directly.
#[tokio::test]
#[ignore = "diagnostic timeline update-volume reproduction"]
async fn update_volume() {
    for size in [200, 1000] {
        for mode in ["normal", "normal_and_attachments", "concurrent_pagination"] {
            run(size, mode).await;
        }
    }
}

async fn run(size: usize, mode: &str) {
    let second_timeline = mode != "normal";
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    client.event_cache().subscribe().unwrap();
    server.mock_room_state_encryption().plain().mount().await;
    let room_id = room_id!("!update-volume:example.org");
    let room = server
        .sync_room(
            &client,
            JoinedRoomBuilder::new(room_id)
                .set_timeline_limited()
                .set_timeline_prev_batch("initial-token"),
        )
        .await;
    let f = EventFactory::new().room(room_id).sender(*ALICE);
    let timeline = room.timeline().await.unwrap();
    let mut normal = Observer::new(&timeline).await;
    let attachments = if second_timeline {
        Some(room.timeline_builder().track_read_marker_and_receipts(TimelineReadReceiptTracking::Disabled)
            .event_filter(|event, _| {
            matches!(event, AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomMessage(
                SyncRoomMessageEvent::Original(message))) if matches!(message.content.msgtype, MessageType::Image(_)))
        }).build().await.unwrap())
    } else {
        None
    };
    let mut attachment_observer = match &attachments {
        Some(timeline) => Some(Observer::new(timeline).await),
        None => None,
    };

    // Same history and requests: the attachments timeline owns pagination when present.
    // Include one date divider without triggering the initial 20-item skip.
    let initial = 19;
    let events =
        (size - initial..size).map(|i| history_event(&f, i).into_raw_sync()).collect::<Vec<_>>();
    server.sync_room(&client, JoinedRoomBuilder::new(room_id).add_timeline_bulk(events)).await;
    let initial_metrics = normal.checkpoint(&timeline, size, mode, "initial_sync", "normal").await;
    assert_eq!(initial_metrics.unique_ids, initial);
    assert_eq!(initial_metrics.set_field_changes.get("receipts"), Some(&(initial - 1)));
    if let (Some(timeline), Some(observer)) = (&attachments, &mut attachment_observer) {
        observer.checkpoint(timeline, size, mode, "initial_sync", "attachments").await;
    }
    let paginator = attachments.as_ref().unwrap_or(&timeline);
    let mut end = size - initial;
    while end > 0 {
        let start = end.saturating_sub(100);
        let events = (start..end)
            .rev()
            .map(|i| history_event(&f, i).into_raw_timeline())
            .collect::<Vec<ruma::serde::Raw<ruma::events::AnyTimelineEvent>>>();
        let response = RoomMessagesResponseTemplate::default()
            .events(events)
            .end_token(format!("page-{start}"))
            .with_delay(Duration::from_millis(10));
        let _mock = server.mock_room_messages().ok(response).mock_once().mount_as_scoped().await;
        if mode == "concurrent_pagination" {
            // Both calls overlap and share the same cache pagination future. The
            // scoped mock requires exactly one HTTP request for this page.
            let (chat, gallery) = tokio::join!(
                timeline.paginate_backwards((end - start) as u16),
                paginator.paginate_backwards((end - start) as u16)
            );
            chat.unwrap();
            gallery.unwrap();
        } else {
            paginator.paginate_backwards((end - start) as u16).await.unwrap();
        }
        // Wait for the *ordinary* timeline to process each shared-cache page.
        timeout(Duration::from_secs(10), async {
            loop {
                if timeline.items().await.iter().filter(|item| item.as_event().is_some()).count()
                    == size - start
                {
                    break;
                }
                sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        end = start;
    }
    let paginated = normal.checkpoint(&timeline, size, mode, "back_pagination", "normal").await;
    assert_eq!(paginated.unique_ids, size - initial);
    assert_eq!(paginated.repeated_ids, 0);
    assert_eq!(paginated.kinds.get("set"), None);
    if let (Some(timeline), Some(observer)) = (&attachments, &mut attachment_observer) {
        observer.checkpoint(timeline, size, mode, "back_pagination", "attachments").await;
    }

    server
        .mock_get_members()
        .ok(vec![
            f.member(&ALICE).display_name("Alice").membership(MembershipState::Join).into_raw(),
        ])
        .mount()
        .await;
    timeline.fetch_members().await;
    let profiles = normal.checkpoint(&timeline, size, mode, "fetch_members", "normal").await;
    assert_eq!(profiles.unique_ids, size);
    assert!(profiles.event_occurrences >= size);
    assert!(
        timeline.items().await.iter().filter_map(|item| item.as_event()).all(|event| format!(
            "{:?}",
            event.sender_profile()
        )
        .contains("Alice"))
    );
    if let (Some(timeline), Some(observer)) = (&attachments, &mut attachment_observer) {
        observer.checkpoint(timeline, size, mode, "fetch_members", "attachments").await;
    }
    timeline.fetch_members().await;
    assert_eq!(
        normal.checkpoint(&timeline, size, mode, "fetch_members_again", "normal").await.operations,
        0
    );
    if let (Some(timeline), Some(observer)) = (&attachments, &mut attachment_observer) {
        observer.checkpoint(timeline, size, mode, "fetch_members_again", "attachments").await;
    }

    for phase in ["receipt", "same_receipt_again"] {
        server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(room_id).add_receipt(
                    f.read_receipts()
                        .add(
                            &event_id(size - 1),
                            *BOB,
                            ReceiptType::Read,
                            ReceiptThread::Unthreaded,
                        )
                        .into_event(),
                ),
            )
            .await;
        let metrics = normal.checkpoint(&timeline, size, mode, phase, "normal").await;
        if phase == "receipt" {
            assert_eq!(metrics.kinds.get("set"), Some(&1));
            assert_eq!(metrics.set_field_changes.get("receipts"), Some(&1));
        } else {
            assert_eq!(metrics.operations, 0);
        }
        if let (Some(timeline), Some(observer)) = (&attachments, &mut attachment_observer) {
            observer.checkpoint(timeline, size, mode, phase, "attachments").await;
        }
    }

    let (cache, _drop_handles) = room.event_cache().await.unwrap();
    let identical = cache.find_event(&event_id(0)).await.unwrap().unwrap();
    for (phase, event) in [
        ("cache_set_identical", identical.clone()),
        ("cache_set_identical_again", identical),
        (
            "cache_set_changed",
            f.image("changed.jpg".to_owned(), "mxc://example.org/changed".into())
                .event_id(&event_id(0))
                .server_ts(1_700_000_000_000u64)
                .into_event(),
        ),
    ] {
        cache.replace_event_for_testing(&event_id(0), event).await;
        let metrics = normal.checkpoint(&timeline, size, mode, phase, "normal").await;
        assert_eq!(metrics.kinds.get("set"), Some(&1));
        if phase == "cache_set_changed" {
            assert_eq!(metrics.set_field_changes.get("content"), Some(&1));
            assert_eq!(metrics.set_field_changes.get("original_json"), Some(&1));
        } else {
            assert_eq!(metrics.sets_identical_data, 1);
        }
        if let (Some(timeline), Some(observer)) = (&attachments, &mut attachment_observer) {
            observer.checkpoint(timeline, size, mode, phase, "attachments").await;
        }
    }

    for (phase, relation) in [
        ("reaction", f.reaction(&event_id(8), "👍").event_id(&event_id(size + 1)).into_raw_sync()),
        (
            "edit",
            f.text_msg(" * edited")
                .edit(&event_id(9), RoomMessageEventContent::text_plain("edited").into())
                .event_id(&event_id(size + 2))
                .into_raw_sync(),
        ),
        ("redaction", f.redaction(&event_id(8)).event_id(&event_id(size + 3)).into_raw_sync()),
    ] {
        server
            .sync_room(&client, JoinedRoomBuilder::new(room_id).add_timeline_event(relation))
            .await;
        let metrics = normal.checkpoint(&timeline, size, mode, phase, "normal").await;
        assert_eq!(metrics.set_field_changes.get("content"), Some(&1));
        assert_eq!(metrics.kinds.get("set"), Some(&if phase == "redaction" { 2 } else { 1 }));
        if phase == "edit" {
            assert_eq!(metrics.set_field_changes.get("edit_json"), Some(&1));
        }
        if let (Some(timeline), Some(observer)) = (&attachments, &mut attachment_observer) {
            observer.checkpoint(timeline, size, mode, phase, "attachments").await;
        }
    }
}
