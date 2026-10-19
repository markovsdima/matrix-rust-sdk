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

use super::{TimelineController, TimelineSettings};
use crate::timeline::{
    TimelineDetails, TimelineEventFocusThreadMode, TimelineFocus, TimelineItem,
    TimelineReadReceiptTracking,
    event_item::RemoteEventOrigin,
    tasks::{FocusedTestCommand, FocusedTestLane, focused_timeline_updates_task},
};
use eyeball_im::VectorDiff;
use futures_util::StreamExt;
use matrix_sdk::{
    Client, ThreadingSupport,
    deserialized_responses::TimelineEvent,
    event_cache::{
        EventCacheDropHandles, EventFocusThreadMode, RoomEventCache, RoomEventCacheUpdate,
    },
    test_utils::mocks::{
        MatrixMockServer, RoomContextResponseTemplate, RoomMessagesResponseTemplate,
        RoomRelationsResponseTemplate,
    },
};
use matrix_sdk_test::{ALICE, BOB, JoinedRoomBuilder, async_test, event_factory::EventFactory};
use ruma::{
    EventId, OwnedEventId,
    events::{
        receipt::{ReceiptThread, ReceiptType},
        room::{member::MembershipState, message::RoomMessageEventContentWithoutRelation},
    },
    room_id,
};
use serde_json::{Value, json};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

struct Lane {
    commands: mpsc::UnboundedSender<FocusedTestCommand>,
    lane: FocusedTestLane,
    task: Arc<JoinHandle<()>>,
    replica: Option<mpsc::UnboundedSender<oneshot::Sender<ReplicaState>>>,
}
impl Lane {
    fn start(&self) -> oneshot::Receiver<usize> {
        let (completed, wait) = oneshot::channel();
        self.commands.send(FocusedTestCommand { lane: self.lane, completed }).unwrap();
        wait
    }
    async fn drain(&mut self) {
        for _ in 0..1000 {
            let pending = timeout(Duration::from_secs(15), self.start()).await.unwrap().unwrap();
            if let Some(replica) = &self.replica {
                let (tx, rx) = oneshot::channel();
                replica.send(tx).unwrap();
                rx.await.unwrap();
            }
            if pending == 0 {
                return;
            }
        }
        panic!("queue did not drain");
    }
}
impl Drop for Lane {
    fn drop(&mut self) {
        self.task.abort();
    }
}
#[derive(Clone, Copy, Debug)]
enum Focus {
    Event,
    Thread,
    Pinned,
}
impl Focus {
    fn timeline(self) -> TimelineFocus {
        match self {
            Self::Event => TimelineFocus::Event {
                target: id("f1"),
                num_context_events: 2,
                thread_mode: TimelineEventFocusThreadMode::Automatic {
                    hide_threaded_events: false,
                },
            },
            Self::Thread => TimelineFocus::Thread { root_event_id: id("f0") },
            Self::Pinned => TimelineFocus::PinnedEvents,
        }
    }
    fn receipt_thread(self) -> ReceiptThread {
        if matches!(self, Self::Thread) {
            ReceiptThread::Thread(id("f0"))
        } else {
            ReceiptThread::Unthreaded
        }
    }
}
fn id(s: &str) -> OwnedEventId {
    EventId::parse(format!("$audit-{s}")).unwrap()
}
struct Fixture {
    server: MatrixMockServer,
    client: Client,
    cache: RoomEventCache,
    _keep: Arc<EventCacheDropHandles>,
    controller: TimelineController,
    factory: EventFactory,
    focus: Focus,
    room: Lane,
    focused: Lane,
    sequence: usize,
}
impl Fixture {
    async fn new(focus: Focus) -> Self {
        let server = MatrixMockServer::new().await;
        let client = server
            .client_builder()
            .on_builder(|b| {
                b.with_threading_support(ThreadingSupport::Enabled { with_subscriptions: false })
            })
            .build()
            .await;
        client.event_cache().subscribe().unwrap();
        let room_id = room_id!("!nonlive-audit:example.org");
        let factory = EventFactory::new().room(room_id).sender(*ALICE);
        let events: Vec<TimelineEvent> = (0..4)
            .map(|i| {
                let msg = factory
                    .text_msg(format!("anchor {i}"))
                    .event_id(&id(&format!("f{i}")))
                    .server_ts(1_700_000_000_000u64 + i);
                if matches!(focus, Focus::Thread) && i > 0 {
                    msg.in_thread(&id("f0"), &id("f0")).into_event()
                } else {
                    msg.into_event()
                }
            })
            .collect();
        server
            .mock_room_relations()
            .ok(RoomRelationsResponseTemplate::default()
                .events(Vec::<ruma::serde::Raw<ruma::events::AnyTimelineEvent>>::new()))
            .mount()
            .await;
        server
            .mock_room_event_context()
            .ok(RoomContextResponseTemplate::new(events[1].clone())
                .events_before(vec![events[0].clone()])
                .events_after(vec![events[2].clone(), events[3].clone()])
                .start("context-before")
                .end("context-after"))
            .mount()
            .await;
        let mut room_builder = JoinedRoomBuilder::new(room_id)
            .set_timeline_prev_batch("room-before")
            .add_state_event(
                factory
                    .member(*ALICE)
                    .membership(MembershipState::Join)
                    .display_name("Alice initial"),
            )
            .add_state_event(
                factory.member(*BOB).membership(MembershipState::Join).display_name("Bob"),
            )
            .add_state_event(
                factory
                    .member(client.user_id().unwrap())
                    .membership(MembershipState::Join)
                    .display_name("Me"),
            )
            .add_timeline_bulk(events.iter().map(|e| e.raw().clone()).collect::<Vec<_>>());
        if matches!(focus, Focus::Pinned) {
            room_builder = room_builder.add_state_event(
                factory.room_pinned_events((0..4).map(|i| id(&format!("f{i}"))).collect()),
            );
        }
        let room = server.sync_room(&client, room_builder).await;
        let (cache, keep) = room.event_cache().await.unwrap();
        timeout(Duration::from_secs(10), async {
            loop {
                if cache.events().await.unwrap().len() >= 4 {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let controller = TimelineController::new(
            room,
            focus.timeline(),
            None,
            None,
            false,
            TimelineSettings {
                track_read_receipts: TimelineReadReceiptTracking::AllEvents,
                ..Default::default()
            },
        );
        let (initial, receiver) = match focus {
            Focus::Event => {
                cache
                    .get_or_create_event_focused_cache(id("f1"), 2, EventFocusThreadMode::Automatic)
                    .await
                    .unwrap()
                    .subscribe()
                    .await
            }
            Focus::Thread => cache.subscribe_to_thread(id("f0")).await.unwrap(),
            Focus::Pinned => timeout(Duration::from_secs(10), async {
                loop {
                    let pair = cache.subscribe_to_pinned_events().await.unwrap();
                    if pair.0.len() >= 4 {
                        break pair;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap(),
        };
        controller.replace_with_initial_remote_events(initial, RemoteEventOrigin::Cache).await;
        let (_, room_receiver) = cache.subscribe().await.unwrap();
        let (commands, receiver_commands) = mpsc::unbounded_channel();
        *controller.focused_task_hook.lock().await = Some(receiver_commands);
        let task = Arc::new(tokio::spawn(focused_timeline_updates_task(
            cache.clone(),
            controller.clone(),
            room_receiver,
            receiver,
            focus.timeline(),
        )));
        Self {
            server,
            client,
            cache,
            _keep: keep,
            controller,
            factory,
            focus,
            room: Lane {
                commands: commands.clone(),
                lane: FocusedTestLane::Room,
                task: task.clone(),
                replica: None,
            },
            focused: Lane { commands, lane: FocusedTestLane::Focus, task, replica: None },
            sequence: 0,
        }
    }
    async fn sync(&mut self, mut builder: JoinedRoomBuilder) {
        self.sequence += 1;
        let sentinel = id(&format!("sync-{}", self.sequence));
        builder = builder.add_timeline_event(
            self.factory
                .text_msg("room-only fence")
                .sender(*BOB)
                .event_id(&sentinel)
                .server_ts(1_700_100_000_000u64 + self.sequence as u64),
        );
        let (_, mut observer) = self.cache.subscribe().await.unwrap();
        self.server.sync_room(&self.client, builder).await;
        timeout(Duration::from_secs(10), async {
            loop {
                if let RoomEventCacheUpdate::UpdateTimelineEvents(update) =
                    observer.recv().await.unwrap()
                {
                    let found = update.diffs.iter().any(|d| match d {
                        VectorDiff::Append { values } | VectorDiff::Reset { values } => {
                            values.iter().any(|e| e.event_id().as_ref() == Some(&sentinel))
                        }
                        VectorDiff::PushBack { value }
                        | VectorDiff::PushFront { value }
                        | VectorDiff::Insert { value, .. }
                        | VectorDiff::Set { value, .. } => {
                            value.event_id().as_ref() == Some(&sentinel)
                        }
                        _ => false,
                    });
                    if found {
                        break;
                    }
                }
            }
        })
        .await
        .expect("sync publication fence");
    }
    async fn overflow_step(
        &mut self,
        room_lag: bool,
        cycle: usize,
        index: usize,
        drain_other: bool,
    ) {
        if room_lag {
            let event = self
                .factory
                .text_msg("unrelated older room event")
                .event_id(&id(&format!("room-{cycle}-{index}")))
                .server_ts(1_600_000_000_000u64 - (cycle * 100 + index) as u64)
                .into_raw_timeline();
            let mock = self
                .server
                .mock_room_messages()
                .ok(RoomMessagesResponseTemplate::default()
                    .end_token(format!("room-page-{cycle}-{index}"))
                    .events(vec![event]))
                .mock_once()
                .mount_as_scoped()
                .await;
            self.cache.pagination().run_backwards_once(1).await.unwrap();
            drop(mock);
            if drain_other {
                self.focused.drain().await;
            }
        } else {
            match self.focus {
                Focus::Event => {
                    let event = self
                        .factory
                        .text_msg("focused older event")
                        .event_id(&id(&format!("context-{cycle}-{index}")))
                        .server_ts(1_600_000_000_000u64 - (cycle * 100 + index) as u64)
                        .into_raw_timeline();
                    let mock = self
                        .server
                        .mock_room_messages()
                        .ok(RoomMessagesResponseTemplate::default()
                            .end_token(format!("context-page-{cycle}-{index}"))
                            .events(vec![event]))
                        .mock_once()
                        .mount_as_scoped()
                        .await;
                    self.cache
                        .get_event_focused_cache(id("f1"), EventFocusThreadMode::Automatic)
                        .await
                        .unwrap()
                        .unwrap()
                        .paginate_backwards(1)
                        .await
                        .unwrap();
                    drop(mock);
                }
                Focus::Thread => {
                    let event = self
                        .factory
                        .text_msg("thread reply")
                        .event_id(&id(&format!("thread-{cycle}-{index}")))
                        .in_thread(&id("f0"), &id("f1"));
                    self.sync(
                        JoinedRoomBuilder::new(self.cache.room_id()).add_timeline_event(event),
                    )
                    .await;
                }
                Focus::Pinned => {
                    let event = self
                        .factory
                        .reaction(&id("f2"), format!("r-{cycle}-{index}"))
                        .event_id(&id(&format!("reaction-{cycle}-{index}")));
                    self.sync(
                        JoinedRoomBuilder::new(self.cache.room_id()).add_timeline_event(event),
                    )
                    .await;
                }
            }
            if drain_other {
                self.room.drain().await;
            }
        }
    }
    async fn controls(&mut self, phase: &str, cycle: usize, target: &str) {
        let name = format!("Alice {phase} {cycle}");
        let body = format!("edit {phase} {cycle}");
        let builder = JoinedRoomBuilder::new(self.cache.room_id())
            .add_state_event(
                self.factory.member(*ALICE).membership(MembershipState::Join).display_name(&name),
            )
            .add_account_data(self.factory.fully_read(&id(target)))
            .add_receipt(
                self.factory
                    .read_receipts()
                    .add_with_timestamp(
                        &id(target),
                        *BOB,
                        ReceiptType::Read,
                        self.focus.receipt_thread(),
                        None,
                    )
                    .add_with_timestamp(
                        &id(target),
                        self.client.user_id().unwrap(),
                        ReceiptType::ReadPrivate,
                        self.focus.receipt_thread(),
                        None,
                    )
                    .into_event(),
            )
            .add_timeline_event(
                self.factory
                    .text_msg("* edit")
                    .event_id(&id(&format!("edit-{phase}-{cycle}")))
                    .edit(&id("f1"), RoomMessageEventContentWithoutRelation::text_plain(&body)),
            )
            .add_timeline_event(
                self.factory
                    .reaction(&id("f2"), format!("effect-{phase}-{cycle}"))
                    .sender(*ALICE)
                    .event_id(&id(&format!("effect-{phase}-{cycle}"))),
            )
            .add_timeline_event(
                self.factory.redaction(&id("f3")).event_id(&id(&format!("redact-{phase}-{cycle}"))),
            );
        self.sync(builder).await;
    }
    async fn expected_events(&self) -> Vec<TimelineEvent> {
        match self.focus {
            Focus::Event => {
                self.cache
                    .get_event_focused_cache(id("f1"), EventFocusThreadMode::Automatic)
                    .await
                    .unwrap()
                    .unwrap()
                    .subscribe()
                    .await
                    .0
            }
            Focus::Thread => self.cache.subscribe_to_thread(id("f0")).await.unwrap().0,
            Focus::Pinned => self.cache.subscribe_to_pinned_events().await.unwrap().0,
        }
    }
    async fn result(&self, cycle: usize, room_lag: bool, replica: &ReplicaState) -> Value {
        let expected = self.expected_events().await;
        let expected_ids: Vec<_> = expected
            .iter()
            .filter_map(|e| {
                let raw = e.raw();
                let kind = raw.get_field::<String>("type").unwrap();
                let content = raw.get_field::<Value>("content").unwrap().unwrap_or_default();
                if kind.as_deref() == Some("m.room.message")
                    && content.pointer("/m.relates_to/rel_type").and_then(Value::as_str)
                        != Some("m.replace")
                {
                    e.event_id().map(|id| id.to_string())
                } else {
                    None
                }
            })
            .collect();
        let state = self.controller.state.read().await;
        let items: Vec<_> = state.items.iter().filter_map(|i| i.as_event()).collect();
        let actual_ids: Vec<_> =
            items.iter().filter_map(|e| e.event_id().map(ToString::to_string)).collect();
        let metadata: Vec<_> =
            state.items.all_remote_events().iter().map(|e| e.event_id.to_string()).collect();
        let anchors:Vec<_>=items.iter().filter(|e|e.event_id().is_some_and(|i|[id("f1"),id("f2"),id("f3")].iter().any(|x|x.as_str()==i.as_str()))).map(|e|json!({
            "id":e.event_id().unwrap().to_string(),"body":e.content().as_message().map(|m|m.body()),"redacted":e.content().is_redacted(),
            "reactions":e.content().reactions().map(|r|r.keys().cloned().collect::<Vec<_>>()),
            "receipts":e.read_receipts().keys().map(ToString::to_string).collect::<Vec<_>>(),
            "profile":match e.sender_profile(){TimelineDetails::Ready(p)=>p.display_name.clone(),_=>None}
        })).collect();
        let own = state
            .meta
            .read_receipts
            .get_latest(self.client.user_id().unwrap(), &ReceiptType::ReadPrivate)
            .map(|(id, _)| id.to_string());
        let public = state
            .meta
            .read_receipts
            .get_latest(*BOB, &ReceiptType::Read)
            .map(|(id, _)| id.to_string());
        let marker = state.meta.fully_read_event.as_ref().map(ToString::to_string);
        let target = id("f2").to_string();
        let contents_ok = anchors.iter().any(|e| {
            e["id"] == id("f1").as_str() && e["body"] == format!("edit concurrent {cycle}")
        }) && anchors.iter().any(|e| {
            e["id"] == id("f2").as_str()
                && e["reactions"]
                    .as_array()
                    .is_some_and(|r| r.contains(&json!(format!("effect-concurrent-{cycle}"))))
        }) && anchors
            .iter()
            .filter(|e| e["id"] == id("f3").as_str())
            .all(|e| e["redacted"] == true);
        let profiles_ok = anchors.len() >= 2
            && anchors
                .iter()
                .filter(|e| e["id"] != id("f3").as_str())
                .all(|e| e["profile"] == format!("Alice concurrent {cycle}"));
        json!({"focus":format!("{:?}",self.focus),"lag_channel":if room_lag{"room"}else{"focused"},"cycle":cycle,"lag_skipped_total":self.controller.focused_skipped.load(std::sync::atomic::Ordering::Relaxed),"rebuilds":self.controller.focused_rebuilds.load(std::sync::atomic::Ordering::Relaxed),
            "expected_ids":expected_ids,"actual_ids":actual_ids,"unique_ids":actual_ids.iter().collect::<BTreeSet<_>>().len(),
            "metadata":metadata,"metadata_unique":metadata.iter().collect::<BTreeSet<_>>().len(),"metadata_unique_correct":metadata.len()==metadata.iter().collect::<BTreeSet<_>>().len(),"anchors":anchors,"private_receipt":own,"marker":marker,
            "public_receipt":public,"public_receipt_correct":public.as_deref()==Some(target.as_str()),"receipt_avatar_correct":anchors.iter().any(|e|e["id"]==target&&e["receipts"].as_array().is_some_and(|r|r.contains(&json!(BOB.as_str())))),"content_correct":contents_ok,"profiles_correct":profiles_ok,"private_receipt_correct":own.as_deref()==Some(target.as_str()),"marker_correct":marker.as_deref()==Some(target.as_str()),
            "events_correct":actual_ids==expected_ids,"unique_correct":actual_ids.len()==actual_ids.iter().collect::<BTreeSet<_>>().len(),
            "output_operations":replica.operations,"output_resets":replica.resets,"replica_correct":format!("{:?}",replica.items)==format!("{:?}",state.items.iter().cloned().collect::<imbl::Vector<_>>()),"transient_duplicate_ids":replica.duplicates})
    }
}

#[derive(Clone)]
struct ReplicaState {
    items: imbl::Vector<Arc<TimelineItem>>,
    operations: usize,
    resets: usize,
    duplicates: bool,
}
struct Replica {
    tx: mpsc::UnboundedSender<oneshot::Sender<ReplicaState>>,
    task: JoinHandle<()>,
}
impl Replica {
    async fn new(controller: &TimelineController) -> Self {
        let (items, mut stream) = controller.subscribe_raw().await;
        let (tx, mut requests) = mpsc::unbounded_channel::<oneshot::Sender<ReplicaState>>();
        let task = tokio::spawn(async move {
            let mut state = ReplicaState { items, operations: 0, resets: 0, duplicates: false };
            loop {
                tokio::select! { biased;
                    Some(diff)=stream.next() => {
                        state.operations+=1;
                        state.resets+=usize::from(matches!(&diff,VectorDiff::Reset{..}));
                        diff.apply(&mut state.items);
                        let ids:Vec<_>=state.items.iter().filter_map(|i|i.as_event()?.event_id()).collect();
                        state.duplicates |= ids.len()!=ids.iter().collect::<BTreeSet<_>>().len();
                    }
                    Some(reply)=requests.recv() => { let _=reply.send(state.clone()); }
                    else => break,
                }
            }
        });
        Self { tx, task }
    }
    async fn snapshot(&self) -> ReplicaState {
        let (tx, rx) = oneshot::channel();
        self.tx.send(tx).unwrap();
        rx.await.unwrap()
    }
}
impl Drop for Replica {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn run_case(focus: Focus, room_lag: bool, no_lag: bool, lag_during_rebuild: bool) {
    let mut outcomes = Vec::new();
    let mut fixture = Fixture::new(focus).await;
    let replica = Replica::new(&fixture.controller).await;
    fixture.room.replica = Some(replica.tx.clone());
    fixture.focused.replica = Some(replica.tx.clone());
    for cycle in 0..2 {
        fixture.controls("queued", cycle, "f1").await;
        if room_lag {
            fixture.focused.drain().await;
        } else {
            fixture.room.drain().await;
        }
        for i in 0..40 {
            fixture.overflow_step(room_lag, cycle, i, true).await;
            if no_lag {
                fixture.room.drain().await;
                fixture.focused.drain().await;
            }
        }
        let mut rebuilding = None;
        let mut release = None;
        if !no_lag {
            let (reached, wait) = oneshot::channel();
            let (resume, resumed) = oneshot::channel();
            *fixture.controller.cache_rebuild_hook.lock().await = Some((reached, resumed));
            rebuilding =
                Some(if room_lag { fixture.room.start() } else { fixture.focused.start() });
            timeout(Duration::from_secs(15), wait).await.expect("lag must reach snapshot").unwrap();
            release = Some(resume);
        }
        fixture.controls("concurrent", cycle, "f2").await;
        if lag_during_rebuild {
            for i in 0..40 {
                fixture.overflow_step(room_lag, 10 + cycle, i, false).await;
            }
        }
        if let Some(release) = release {
            release.send(()).unwrap();
        }
        if let Some(rebuilding) = rebuilding {
            timeout(Duration::from_secs(15), rebuilding).await.unwrap().unwrap();
        }
        fixture.room.drain().await;
        fixture.focused.drain().await;
        fixture.room.drain().await;
        let replica_state = replica.snapshot().await;
        let mut result = fixture.result(cycle, room_lag, &replica_state).await;
        result["no_lag_control"] = json!(no_lag);
        println!("FOCUSED_RECOVERY {}", result);
        outcomes.push(result);
    }
    let rebuilds = fixture.controller.focused_rebuilds.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(
        rebuilds,
        if no_lag {
            0
        } else if lag_during_rebuild {
            4
        } else {
            2
        }
    );
    {
        let failed: Vec<_> = outcomes
            .iter()
            .filter(|r| {
                [
                    "events_correct",
                    "unique_correct",
                    "metadata_unique_correct",
                    "public_receipt_correct",
                    "receipt_avatar_correct",
                    "content_correct",
                    "profiles_correct",
                    "private_receipt_correct",
                    "marker_correct",
                    "replica_correct",
                ]
                .iter()
                .any(|key| r[*key] != true)
            })
            .collect();
        assert!(
            failed.is_empty(),
            "focused recovery violated invariants in {} cases",
            failed.len()
        );
    }
}
#[async_test]
async fn test_nonlive_event_room() {
    run_case(Focus::Event, true, false, false).await;
}
#[async_test]
async fn test_nonlive_event_focused() {
    run_case(Focus::Event, false, false, false).await;
}
#[async_test]
async fn test_nonlive_thread_room() {
    run_case(Focus::Thread, true, false, false).await;
}
#[async_test]
async fn test_nonlive_thread_focused() {
    run_case(Focus::Thread, false, false, false).await;
}
#[async_test]
async fn test_nonlive_pinned_room() {
    run_case(Focus::Pinned, true, false, false).await;
}
#[async_test]
async fn test_nonlive_pinned_focused() {
    run_case(Focus::Pinned, false, false, false).await;
}

#[async_test]
async fn test_no_lag_controls() {
    for focus in [Focus::Event, Focus::Thread, Focus::Pinned] {
        for room_lag in [true, false] {
            run_case(focus, room_lag, true, false).await;
        }
    }
}
#[async_test]
async fn test_lag_again_during_rebuild() {
    for focus in [Focus::Event, Focus::Thread, Focus::Pinned] {
        for room_lag in [true, false] {
            run_case(focus, room_lag, false, true).await;
        }
    }
}

impl Fixture {
    async fn pause_recovery(
        &mut self,
        room_lag: bool,
    ) -> (oneshot::Receiver<usize>, oneshot::Sender<()>) {
        let cycle =
            self.controller.focused_rebuilds.load(std::sync::atomic::Ordering::Relaxed) as usize;
        for i in 0..40 {
            self.overflow_step(room_lag, cycle, i, true).await;
        }
        let (reached, wait) = oneshot::channel();
        let (resume, resumed) = oneshot::channel();
        *self.controller.cache_rebuild_hook.lock().await = Some((reached, resumed));
        let rebuilding = if room_lag { self.room.start() } else { self.focused.start() };
        timeout(Duration::from_secs(15), wait).await.unwrap().unwrap();
        (rebuilding, resume)
    }
    async fn finish_recovery(
        &mut self,
        rebuilding: oneshot::Receiver<usize>,
        resume: oneshot::Sender<()>,
    ) {
        resume.send(()).unwrap();
        timeout(Duration::from_secs(15), rebuilding).await.unwrap().unwrap();
        self.room.drain().await;
        self.focused.drain().await;
        self.room.drain().await;
    }
}

#[async_test]
async fn test_first_redaction_during_rebuild() {
    for focus in [Focus::Event, Focus::Thread, Focus::Pinned] {
        for room_lag in [true, false] {
            let mut fixture = Fixture::new(focus).await;
            let replica = Replica::new(&fixture.controller).await;
            let (rebuilding, resume) = fixture.pause_recovery(room_lag).await;
            fixture.controls("concurrent", 0, "f2").await;
            fixture.finish_recovery(rebuilding, resume).await;
            let result = fixture.result(0, room_lag, &replica.snapshot().await).await;
            assert_eq!(result["content_correct"], true, "{result}");
            assert_eq!(result["events_correct"], true, "{result}");
            assert_eq!(result["replica_correct"], true, "{result}");
        }
    }
}

#[async_test]
async fn test_thread_local_echo_becomes_remote_during_rebuild() {
    use ruma::{TransactionId, events::room::message::RoomMessageEventContent};
    for room_lag in [true, false] {
        let mut fixture = Fixture::new(Focus::Thread).await;
        let replica = Replica::new(&fixture.controller).await;
        let txn = TransactionId::new();
        let content = fixture
            .factory
            .text_msg("pending thread echo")
            .in_thread(&id("f0"), &id("f1"))
            .into_raw_timeline()
            .get_field::<RoomMessageEventContent>("content")
            .unwrap()
            .unwrap();
        fixture.controller.handle_local_event(txn.clone(), content.into(), None).await;
        let local_id = fixture
            .controller
            .items()
            .await
            .iter()
            .find(|i| i.as_event().is_some_and(|e| e.transaction_id() == Some(&txn)))
            .unwrap()
            .unique_id()
            .clone();
        let (rebuilding, resume) = fixture.pause_recovery(room_lag).await;
        fixture
            .sync(
                JoinedRoomBuilder::new(fixture.cache.room_id()).add_timeline_event(
                    fixture
                        .factory
                        .text_msg("pending thread echo")
                        .sender(fixture.client.user_id().unwrap())
                        .event_id(&id("echo"))
                        .unsigned_transaction_id(&txn)
                        .in_thread(&id("f0"), &id("f1")),
                ),
            )
            .await;
        fixture.finish_recovery(rebuilding, resume).await;
        let items = fixture.controller.items().await;
        let echoes: Vec<_> = items
            .iter()
            .filter(|i| i.as_event().is_some_and(|e| e.event_id() == Some(id("echo").as_ref())))
            .collect();
        assert_eq!(echoes.len(), 1);
        assert_eq!(echoes[0].unique_id(), &local_id);
        assert!(!echoes[0].as_event().unwrap().is_local_echo());
        assert_eq!(format!("{:?}", replica.snapshot().await.items), format!("{:?}", items));
        let state = fixture.controller.state.read().await;
        assert_eq!(
            state.items.all_remote_events().iter().filter(|e| e.event_id == id("echo")).count(),
            1
        );
    }
}

#[async_test]
async fn test_event_late_decryption_and_redaction_during_rebuild() {
    use matrix_sdk::deserialized_responses::{
        AlgorithmInfo, DecryptedRoomEvent, EncryptionInfo, VerificationState,
    };
    for room_lag in [true, false] {
        for redaction_phase in [0, 1, 2] {
            let redact = redaction_phase != 0;
            let mut fixture = Fixture::new(Focus::Event).await;
            let cache = fixture
                .cache
                .get_event_focused_cache(id("f1"), EventFocusThreadMode::Automatic)
                .await
                .unwrap()
                .unwrap();
            let encrypted = fixture
                .factory
                .encrypted("ciphertext", "sender-key", "DEVICE", "session")
                .event_id(&id("utd"))
                .into_raw_timeline();
            let mock = fixture
                .server
                .mock_room_messages()
                .ok(RoomMessagesResponseTemplate::default()
                    .end_token("before-utd")
                    .events(vec![encrypted]))
                .mock_once()
                .mount_as_scoped()
                .await;
            cache.paginate_backwards(1).await.unwrap();
            drop(mock);
            fixture.focused.drain().await;
            fixture.room.drain().await;
            assert!(fixture.controller.items().await.iter().any(|i| i.as_event().is_some_and(
                |e| {
                    e.event_id() == Some(id("utd").as_ref()) && e.content().is_unable_to_decrypt()
                }
            )));
            let replica = Replica::new(&fixture.controller).await;
            if redaction_phase == 2 {
                fixture
                    .sync(JoinedRoomBuilder::new(fixture.cache.room_id()).add_timeline_event(
                        fixture.factory.redaction(&id("utd")).event_id(&id("redact-utd")),
                    ))
                    .await;
                fixture.room.drain().await;
                fixture.focused.drain().await;
            }
            let (rebuilding, resume) = fixture.pause_recovery(room_lag).await;
            let decrypted = DecryptedRoomEvent {
                event: fixture
                    .factory
                    .text_msg("late decrypted")
                    .event_id(&id("utd"))
                    .into_raw_timeline(),
                encryption_info: Arc::new(EncryptionInfo {
                    sender: (*ALICE).into(),
                    sender_device: None,
                    forwarder: None,
                    algorithm_info: AlgorithmInfo::MegolmV1AesSha2 {
                        curve25519_key: "sender-key".into(),
                        sender_claimed_keys: Default::default(),
                        session_id: Some("session".into()),
                    },
                    verification_state: VerificationState::Verified,
                }),
                unsigned_encryption_info: None,
            };
            let replaced = cache.replace_utds(&[(id("utd"), decrypted, None)]).await;
            if redaction_phase != 2 {
                assert!(replaced.contains(&id("utd")));
            }
            if redaction_phase == 1 {
                fixture
                    .sync(JoinedRoomBuilder::new(fixture.cache.room_id()).add_timeline_event(
                        fixture.factory.redaction(&id("utd")).event_id(&id("redact-utd")),
                    ))
                    .await;
            }
            fixture.finish_recovery(rebuilding, resume).await;
            let items = fixture.controller.items().await;
            let events: Vec<_> = items
                .iter()
                .filter_map(|i| i.as_event())
                .filter(|e| e.event_id() == Some(id("utd").as_ref()))
                .collect();
            assert_eq!(events.len(), 1);
            if redact {
                assert!(events[0].content().is_redacted());
            } else {
                assert_eq!(events[0].content().as_message().unwrap().body(), "late decrypted");
            }
            assert_eq!(format!("{:?}", replica.snapshot().await.items), format!("{:?}", items));
        }
    }
}

#[async_test]
async fn test_edit_in_focused_snapshot_then_new_room_edit() {
    for room_lag in [true, false] {
        let mut fixture = Fixture::new(Focus::Event).await;
        let cache = fixture
            .cache
            .get_event_focused_cache(id("f1"), EventFocusThreadMode::Automatic)
            .await
            .unwrap()
            .unwrap();
        let edit = fixture
            .factory
            .text_msg("* old edit")
            .event_id(&id("indexed-edit"))
            .server_ts(1u64)
            .edit(&id("f1"), RoomMessageEventContentWithoutRelation::text_plain("old edit"));
        let mock = fixture
            .server
            .mock_room_messages()
            .ok(RoomMessagesResponseTemplate::default()
                .end_token("before-edit")
                .events(vec![edit.into_raw_timeline()]))
            .mock_once()
            .mount_as_scoped()
            .await;
        cache.paginate_backwards(1).await.unwrap();
        drop(mock);
        fixture.focused.drain().await;
        fixture.room.drain().await;
        let (rebuilding, resume) = fixture.pause_recovery(room_lag).await;
        fixture.controls("concurrent", 0, "f2").await;
        fixture.finish_recovery(rebuilding, resume).await;
        let items = fixture.controller.items().await;
        let message = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(id("f1").as_ref()))
            .unwrap();
        assert_eq!(message.content().as_message().unwrap().body(), "edit concurrent 0");
    }
}

#[async_test]
async fn test_pending_local_aggregations_survive_rebuild_and_remote_echo() {
    use crate::timeline::EventSendState;
    use ruma::{TransactionId, events::room::message::RoomMessageEventContent};
    for (sent, sync_first, with_txn) in
        [(false, true, true), (false, true, false), (false, false, true), (true, false, true)]
    {
        let mut fixture = Fixture::new(Focus::Thread).await;
        fixture
            .sync(
                JoinedRoomBuilder::new(fixture.cache.room_id()).add_timeline_event(
                    fixture
                        .factory
                        .text_msg("own original")
                        .sender(fixture.client.user_id().unwrap())
                        .event_id(&id("own"))
                        .in_thread(&id("f0"), &id("f1")),
                ),
            )
            .await;
        fixture.focused.drain().await;
        fixture.room.drain().await;
        let txn = TransactionId::new();
        let content = fixture
            .factory
            .text_msg("* pending edit")
            .edit(&id("own"), RoomMessageEventContentWithoutRelation::text_plain("pending edit"))
            .into_raw_timeline()
            .get_field::<RoomMessageEventContent>("content")
            .unwrap()
            .unwrap();
        fixture.controller.handle_local_event(txn.clone(), content.into(), None).await;
        let reaction_txn = TransactionId::new();
        fixture
            .controller
            .handle_local_event(
                reaction_txn,
                ruma::events::reaction::ReactionEventContent::new(
                    ruma::events::relation::Annotation::new(id("f2"), "pending reaction".into()),
                )
                .into(),
                None,
            )
            .await;
        if sent {
            fixture
                .controller
                .update_event_send_state(&txn, EventSendState::Sent { event_id: id("own-edit") })
                .await;
        }
        let (rebuilding, resume) = fixture.pause_recovery(true).await;
        fixture.finish_recovery(rebuilding, resume).await;
        let items = fixture.controller.items().await;
        let own = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(id("own").as_ref()))
            .unwrap();
        assert_eq!(own.content().as_message().unwrap().body(), "pending edit");
        let target = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(id("f2").as_ref()))
            .unwrap();
        assert!(target.content().reactions().unwrap().contains_key("pending reaction"));
        drop(items);
        if !sent && !sync_first {
            fixture
                .controller
                .update_event_send_state(&txn, EventSendState::Sent { event_id: id("own-edit") })
                .await;
        }
        let remote = fixture
            .factory
            .text_msg("* confirmed edit")
            .sender(fixture.client.user_id().unwrap())
            .event_id(&id("own-edit"))
            .edit(&id("own"), RoomMessageEventContentWithoutRelation::text_plain("confirmed edit"));
        let remote = if with_txn { remote.unsigned_transaction_id(&txn) } else { remote };
        fixture
            .sync(JoinedRoomBuilder::new(fixture.cache.room_id()).add_timeline_event(remote))
            .await;
        fixture.room.drain().await;
        fixture.focused.drain().await;
        let items = fixture.controller.items().await;
        let own = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(id("own").as_ref()))
            .unwrap();
        let expected = if sync_first && !with_txn { "pending edit" } else { "confirmed edit" };
        assert_eq!(own.content().as_message().unwrap().body(), expected);
        drop(items);
        if sync_first {
            fixture
                .controller
                .update_event_send_state(&txn, EventSendState::Sent { event_id: id("own-edit") })
                .await;
        }
        // A second rebuild must not resurrect the acknowledged local version.
        let (rebuilding, resume) = fixture.pause_recovery(true).await;
        fixture.finish_recovery(rebuilding, resume).await;
        let items = fixture.controller.items().await;
        let own = items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(id("own").as_ref()))
            .unwrap();
        assert_eq!(own.content().as_message().unwrap().body(), "confirmed edit");
    }
}

#[async_test]
async fn test_focused_edit_timestamp_tie_invalid_edit_and_redacted_revision() {
    let mut fixture = Fixture::new(Focus::Event).await;
    for (suffix, sender, timestamp, body) in [
        ("edit-z", *ALICE, 20u64, "latest z"),
        ("edit-a", *ALICE, 20, "latest a"),
        ("edit-invalid", *BOB, 30, "invalid"),
    ] {
        fixture
            .sync(
                JoinedRoomBuilder::new(fixture.cache.room_id()).add_timeline_event(
                    fixture
                        .factory
                        .text_msg("* edit")
                        .sender(sender)
                        .event_id(&id(suffix))
                        .server_ts(timestamp)
                        .edit(&id("f1"), RoomMessageEventContentWithoutRelation::text_plain(body)),
                ),
            )
            .await;
        fixture.room.drain().await;
        fixture.focused.drain().await;
    }
    let items = fixture.controller.items().await;
    assert_eq!(
        items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(id("f1").as_ref()))
            .unwrap()
            .content()
            .as_message()
            .unwrap()
            .body(),
        "latest z"
    );
    let (rebuilding, resume) = fixture.pause_recovery(true).await;
    fixture
        .sync(
            JoinedRoomBuilder::new(fixture.cache.room_id())
                .add_timeline_event(
                    fixture
                        .factory
                        .text_msg("* older")
                        .event_id(&id("older-edit"))
                        .server_ts(10u64)
                        .edit(
                            &id("f1"),
                            RoomMessageEventContentWithoutRelation::text_plain("older"),
                        ),
                )
                .add_timeline_event(
                    fixture.factory.redaction(&id("edit-z")).event_id(&id("redact-edit-z")),
                ),
        )
        .await;
    fixture.finish_recovery(rebuilding, resume).await;
    let items = fixture.controller.items().await;
    assert_eq!(
        items
            .iter()
            .filter_map(|i| i.as_event())
            .find(|e| e.event_id() == Some(id("f1").as_ref()))
            .unwrap()
            .content()
            .as_message()
            .unwrap()
            .body(),
        "latest a"
    );
}
