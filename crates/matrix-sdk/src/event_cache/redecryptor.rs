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

//! The Redecryptor (affectionately known as R2D2) is a layer and long-running
//! background task which handles redecryption of events in case we couldn't
//! decrypt them immediately.
//!
//! There are various reasons why a room key might not be available immediately
//! when the event becomes available:
//!     - The to-device message containing the room key just arrives late, i.e.
//!       after the room event.
//!     - The event is a historic event and we need to first download the room
//!       key from the backup.
//!     - The event is a historic event in a previously unjoined room, we need
//!       to receive historic room keys as defined in [MSC3061].
//!
//! R2D2 listens to the [`OlmMachine`] for received room keys and new
//! m.room_key.withheld events.
//!
//! If a new room key has been received, it attempts to find any UTDs in the
//! [`EventCache`]. If R2D2 decrypts any UTDs from the event cache, it will
//! replace the events in the cache and send out new [`RoomEventCacheUpdate`]s
//! to any of its listeners.
//!
//! If a new withheld info has been received, it attempts to find any relevant
//! events and updates the [`EncryptionInfo`] of an event.
//!
//! There's an additional gotcha: the [`OlmMachine`] might get recreated by
//! calls to [`BaseClient::regenerate_olm()`]. When this happens, we will
//! receive a `None` on the room keys stream and we need to re-listen to it.
//!
//! Another gotcha is that room keys might be received on another process if the
//! [`Client`] is operating on a Apple iOS device. A separate process is used
//! in this case to receive push notifications. In this case, the room key will
//! be received and R2D2 won't get notified about it. To work around this,
//! decryption requests can be explicitly sent to R2D2.
//!
//! The final gotcha is that a room key might be received just in between the
//! time the event was initially tried to be decrypted and the time it took to
//! persist it in the event cache. To handle this race condition, R2D2 listens
//! to the event cache and attempts to decrypt any UTDs the event cache
//! persists.
//!
//! In the graph below, the Timeline block is meant to be the `Timeline` from
//! the `matrix-sdk-ui` crate, but it could be any other listener that
//! subscribes to [`RedecryptorReport`] stream.
//!
//! ```markdown
//!
//!      .----------------------.
//!     |                        |
//!     |      Beeb, boop!       |
//!     |                        .
//!      ----------------------._ \
//!                               -;  _____
//!                                 .`/L|__`.
//!                                / =[_]O|` \
//!                                |"+_____":|
//!                              __:='|____`-:__
//!                             ||[] ||====|| []||
//!                             ||[] ||====|| []||
//!                             |:== ||====|| ==:|
//!                             ||[] ||====|| []||
//!                             ||[] ||====|| []||
//!                            _||_  ||====||  _||_
//!                           (====) |:====:| (====)
//!                            }--{  | |  | |  }--{
//!                           (____) |_|  |_| (____)
//!
//!                              ┌─────────────┐
//!                              │             │
//!                  ┌───────────┤   Timeline  │◄────────────┐
//!                  │           │             │             │
//!                  │           └──────▲──────┘             │
//!                  │                  │                    │
//!                  │                  │                    │
//!                  │                  │                    │
//!              Decryption             │                Redecryptor
//!                request              │                  report
//!                  │        RoomEventCacheUpdates          │
//!                  │                  │                    │
//!                  │                  │                    │
//!                  │      ┌───────────┴───────────┐        │
//!                  │      │                       │        │
//!                  └──────►         R2D2          │────────┘
//!                         │                       │
//!                         └──▲─────────────────▲──┘
//!                            │                 │
//!                            │                 │
//!                            │                 │
//!                         Received        Received room
//!                          events          keys stream
//!                            │                 │
//!                            │                 │
//!                            │                 │
//!                    ┌───────┴──────┐  ┌───────┴──────┐
//!                    │              │  │              │
//!                    │  Event Cache │  │  OlmMachine  │
//!                    │              │  │              │
//!                    └──────────────┘  └──────────────┘
//! ```
//!
//! [MSC3061]: https://github.com/matrix-org/matrix-spec/pull/1655#issuecomment-2213152255

use std::{
    collections::{BTreeMap, BTreeSet},
    pin::Pin,
    sync::{Arc, Weak},
    time::Duration,
};

use as_variant::as_variant;
use futures_core::Stream;
use futures_util::{StreamExt, future::join_all, pin_mut};
#[cfg(doc)]
use matrix_sdk_base::{BaseClient, crypto::OlmMachine};
use matrix_sdk_base::{
    crypto::{
        store::types::{RoomKeyInfo, RoomKeyWithheldInfo},
        types::events::room::encrypted::EncryptedEvent,
    },
    deserialized_responses::{DecryptedRoomEvent, TimelineEvent, TimelineEventKind},
    event_cache::store::EventCacheStoreLockState,
    locks::Mutex,
    sleep::sleep,
    task_monitor::BackgroundTaskHandle,
    timer,
};
use matrix_sdk_common::deserialized_responses::EncryptionInfo;
use ruma::{
    OwnedEventId, OwnedRoomId, RoomId,
    events::{AnySyncTimelineEvent, room::encrypted::OriginalSyncRoomEncryptedEvent},
    push::Action,
    serde::Raw,
    time::Instant,
};
use tokio::sync::{
    broadcast::{self, Sender},
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio_stream::wrappers::{
    BroadcastStream, UnboundedReceiverStream, errors::BroadcastStreamRecvError,
};
use tracing::{info, instrument, trace, warn};

#[cfg(doc)]
use super::RoomEventCache;
use super::{
    EventCache, EventCacheError, EventCacheInner, EventsOrigin, RoomEventCacheGenericUpdate,
    RoomEventCacheUpdate, TimelineVectorDiffs,
    caches::room::{PostProcessingOrigin, RoomEventCacheLinkedChunkUpdate},
};
use crate::{Client, Result, Room, encryption::backups::BackupState, room::PushContext};

type SessionId<'a> = &'a str;
type OwnedSessionId = String;

type EventIdAndUtd = (OwnedEventId, Raw<AnySyncTimelineEvent>);
type EventIdAndEvent = (OwnedEventId, DecryptedRoomEvent);

#[cfg(any(test, feature = "testing"))]
#[derive(Default)]
pub(super) struct RedecryptionTestState {
    refresh_queries: std::sync::atomic::AtomicUsize,
    info_queries: std::sync::atomic::AtomicUsize,
    pause_refresh: tokio::sync::Mutex<
        Option<(tokio::sync::oneshot::Sender<()>, tokio::sync::oneshot::Receiver<()>)>,
    >,
}

/// Operation counts, rather than elapsed time, for redecryption regressions.
#[cfg(any(test, feature = "testing"))]
#[doc(hidden)]
#[derive(Debug)]
pub struct RedecryptionTestStats {
    pub refresh_queries: usize,
    pub info_queries: usize,
}
pub(in crate::event_cache) type ResolvedUtd =
    (OwnedEventId, DecryptedRoomEvent, Option<Vec<Action>>);

/// Work can outlive its source snapshot. Keep redactions and newer decrypted
/// content intact, including bundled relations; only refresh its metadata.
pub(in crate::event_cache) fn apply_resolved_utd(
    (_, decrypted, actions): &ResolvedUtd,
    target: &mut TimelineEvent,
) -> bool {
    if target.raw().deserialize().is_ok_and(|event| event.is_redacted()) {
        return false;
    }
    match &mut target.kind {
        TimelineEventKind::UnableToDecrypt { .. } => {
            target.kind = TimelineEventKind::Decrypted(decrypted.clone());
            if let Some(actions) = actions {
                target.set_push_actions(actions.clone());
            }
        }
        TimelineEventKind::Decrypted(current) => {
            current.encryption_info = decrypted.encryption_info.clone();
        }
        TimelineEventKind::PlainText { .. } => return false,
    }
    true
}

/// The information sent across the channel to the long-running task requesting
/// that the supplied set of sessions be retried.
#[derive(Debug, Clone)]
pub struct DecryptionRetryRequest {
    /// The room ID of the room the events belong to.
    pub room_id: OwnedRoomId,
    /// Events that are not decrypted.
    pub utd_session_ids: BTreeSet<OwnedSessionId>,
    /// Events that are decrypted but might need to have their
    /// [`EncryptionInfo`] refreshed.
    pub refresh_info_session_ids: BTreeSet<OwnedSessionId>,
}

pub(super) enum DecryptionRetryCommand {
    Wake,
    #[cfg(any(test, feature = "testing"))]
    Barrier(tokio::sync::oneshot::Sender<()>),
}

/// A report coming from the redecryptor.
#[derive(Debug, Clone)]
pub enum RedecryptorReport {
    /// Events which we were able to decrypt.
    ResolvedUtds {
        /// The room ID of the room the events belong to.
        room_id: OwnedRoomId,
        /// The list of event IDs of the decrypted events.
        events: BTreeSet<OwnedEventId>,
    },
    /// The redecryptor might have missed some room keys so it might not have
    /// re-decrypted events that are now decryptable.
    Lagging,
    /// A room key backup has become available.
    ///
    /// This means that components might want to tell R2D2 about events they
    /// care about to attempt a decryption.
    BackupAvailable,
}

pub(super) struct RedecryptorChannels {
    utd_reporter: Sender<RedecryptorReport>,
    pub(super) decryption_request_sender: UnboundedSender<DecryptionRetryCommand>,
    pub(super) decryption_request_receiver:
        Mutex<Option<UnboundedReceiver<DecryptionRetryCommand>>>,
    pending: Mutex<PendingRequests>,
}

impl RedecryptorChannels {
    pub(super) fn new() -> Self {
        let (utd_reporter, _) = broadcast::channel(100);
        let (decryption_request_sender, decryption_request_receiver) = unbounded_channel();

        Self {
            utd_reporter,
            decryption_request_sender,
            decryption_request_receiver: Mutex::new(Some(decryption_request_receiver)),
            pending: Default::default(),
        }
    }

    fn enqueue(&self, update: impl FnOnce(&mut PendingRequests)) {
        let wake = {
            let mut pending = self.pending.lock();
            let was_empty = pending.is_empty();
            update(&mut pending);
            was_empty && !pending.is_empty()
        };
        if wake {
            let _ = self.decryption_request_sender.send(DecryptionRetryCommand::Wake).inspect_err(
                |_| warn!("Requesting a decryption while the redecryption task has been shut down"),
            );
        }
    }
}

/// Merge requests before waking the worker, so pagination cannot accumulate a
/// FIFO of increasingly large copies of the same room's session sets.
#[derive(Default)]
struct PendingRequests {
    rooms: BTreeMap<OwnedRoomId, DecryptionRetryRequest>,
    loaded: BTreeMap<OwnedRoomId, BTreeMap<OwnedEventId, TimelineEvent>>,
}

impl PendingRequests {
    fn is_empty(&self) -> bool {
        self.rooms.is_empty() && self.loaded.is_empty()
    }

    fn insert(&mut self, request: DecryptionRetryRequest) {
        if request.utd_session_ids.is_empty() && request.refresh_info_session_ids.is_empty() {
            return;
        }
        let current =
            self.rooms.entry(request.room_id.clone()).or_insert_with(|| DecryptionRetryRequest {
                room_id: request.room_id.clone(),
                utd_session_ids: Default::default(),
                refresh_info_session_ids: Default::default(),
            });
        current.utd_session_ids.extend(request.utd_session_ids);
        // A session can contain both UTD and decrypted events. Keep its refresh
        // pending after the UTD instead of dropping the trust update.
        current.refresh_info_session_ids.extend(request.refresh_info_session_ids);
    }
}

const REDECRYPTION_BATCH_SIZE: usize = 32;
const MAX_INPUTS_BETWEEN_BATCHES: usize = 32;
const MAX_UTD_BATCHES_BEFORE_REFRESH: usize = 8;
const MAX_BATCH_ATTEMPTS: usize = 3;
const BATCH_RETRY_DELAY: Duration = Duration::from_millis(100);

enum RedecryptionBatch {
    LoadUtds { room: OwnedRoomId, session: OwnedSessionId },
    Decrypt { room: OwnedRoomId, events: Vec<EventIdAndUtd> },
    LoadRefresh { room: OwnedRoomId, session: OwnedSessionId },
    Refresh { room: OwnedRoomId, events: Vec<EventIdAndEvent> },
}

impl RedecryptionBatch {
    fn is_utd(&self) -> bool {
        matches!(self, Self::LoadUtds { .. } | Self::Decrypt { .. })
    }
}

struct FailedBatch {
    batch: RedecryptionBatch,
    attempts: usize,
    retry_at: Instant,
}

/// Work is deduplicated by room/session or room/event, and each event batch
/// yields back to input handling. Rooms rotate even while one has a long tail.
#[derive(Default)]
struct RedecryptionWork {
    utd_sessions: BTreeMap<OwnedRoomId, BTreeSet<OwnedSessionId>>,
    utd_events: BTreeMap<OwnedRoomId, BTreeMap<OwnedEventId, Raw<AnySyncTimelineEvent>>>,
    refresh_sessions: BTreeMap<OwnedRoomId, BTreeSet<OwnedSessionId>>,
    refresh_events: BTreeMap<OwnedRoomId, BTreeMap<OwnedEventId, DecryptedRoomEvent>>,
    last_utd_room: Option<OwnedRoomId>,
    last_refresh_room: Option<OwnedRoomId>,
    load_utds_next: bool,
    load_refresh_next: bool,
    consecutive_utd_batches: usize,
    failed_batches: Vec<FailedBatch>,
    #[cfg(any(test, feature = "testing"))]
    barriers: Vec<tokio::sync::oneshot::Sender<()>>,
}

fn next_room<T>(
    rooms: &BTreeMap<OwnedRoomId, T>,
    previous: &Option<OwnedRoomId>,
) -> Option<OwnedRoomId> {
    previous
        .as_ref()
        .and_then(|previous| {
            rooms
                .range((std::ops::Bound::Excluded(previous.clone()), std::ops::Bound::Unbounded))
                .next()
                .map(|(room, _)| room.clone())
        })
        .or_else(|| rooms.first_key_value().map(|(room, _)| room.clone()))
}

fn take_event_batch<T>(
    rooms: &mut BTreeMap<OwnedRoomId, BTreeMap<OwnedEventId, T>>,
    previous: &mut Option<OwnedRoomId>,
) -> Option<(OwnedRoomId, Vec<(OwnedEventId, T)>)> {
    let room = next_room(rooms, previous)?;
    let events = rooms.get_mut(&room).expect("selected room");
    let batch = (0..REDECRYPTION_BATCH_SIZE).filter_map(|_| events.pop_first()).collect();
    if events.is_empty() {
        rooms.remove(&room);
    }
    *previous = Some(room.clone());
    Some((room, batch))
}

impl RedecryptionWork {
    fn is_empty(&self) -> bool {
        !self.has_ready_work() && self.failed_batches.is_empty()
    }

    fn has_ready_work(&self) -> bool {
        !self.utd_sessions.is_empty()
            || !self.utd_events.is_empty()
            || !self.refresh_sessions.is_empty()
            || !self.refresh_events.is_empty()
    }

    fn has_runnable_work(&self) -> bool {
        self.has_ready_work()
            || self.failed_batches.iter().any(|batch| batch.retry_at <= Instant::now())
    }

    async fn wait_for_work(&self) {
        if self.has_ready_work() {
            tokio::task::yield_now().await;
        } else if let Some(retry_at) = self.failed_batches.iter().map(|batch| batch.retry_at).min()
        {
            sleep(retry_at.saturating_duration_since(Instant::now())).await;
        }
    }

    fn has_refresh_work(&self) -> bool {
        !self.refresh_sessions.is_empty()
            || !self.refresh_events.is_empty()
            || self
                .failed_batches
                .iter()
                .any(|batch| !batch.batch.is_utd() && batch.retry_at <= Instant::now())
    }

    fn take_failed_batch(&mut self, utd: bool) -> Option<(RedecryptionBatch, usize)> {
        let now = Instant::now();
        let index = self
            .failed_batches
            .iter()
            .position(|batch| batch.batch.is_utd() == utd && batch.retry_at <= now)?;
        let failed = self.failed_batches.remove(index);
        Some((failed.batch, failed.attempts))
    }

    fn take_batch(&mut self) -> Option<(RedecryptionBatch, usize)> {
        // UTDs normally go first. Under a continuous UTD stream, let one
        // refresh portion progress too, so trust updates cannot starve forever.
        let utds_first = !self.has_refresh_work()
            || self.consecutive_utd_batches < MAX_UTD_BATCHES_BEFORE_REFRESH;
        if utds_first {
            if let Some(batch) = self.take_failed_batch(true) {
                self.consecutive_utd_batches += 1;
                return Some(batch);
            }
            if !self.utd_sessions.is_empty() && (self.utd_events.is_empty() || self.load_utds_next)
            {
                self.consecutive_utd_batches += 1;
                let room = next_room(&self.utd_sessions, &self.last_utd_room)?;
                let pending = self.utd_sessions.get_mut(&room)?;
                let session = pending.pop_first()?;
                if pending.is_empty() {
                    self.utd_sessions.remove(&room);
                }
                self.last_utd_room = Some(room.clone());
                self.load_utds_next = false;
                return Some((RedecryptionBatch::LoadUtds { room, session }, 0));
            }
            if let Some((room, events)) =
                take_event_batch(&mut self.utd_events, &mut self.last_utd_room)
            {
                self.consecutive_utd_batches += 1;
                self.load_utds_next = true;
                return Some((RedecryptionBatch::Decrypt { room, events }, 0));
            }
        }
        if let Some(batch) = self.take_failed_batch(false) {
            self.consecutive_utd_batches = 0;
            return Some(batch);
        }
        if !self.refresh_sessions.is_empty()
            && (self.refresh_events.is_empty() || self.load_refresh_next)
        {
            self.consecutive_utd_batches = 0;
            let room = next_room(&self.refresh_sessions, &self.last_refresh_room)?;
            let sessions = self.refresh_sessions.get_mut(&room)?;
            let session = sessions.pop_first()?;
            if sessions.is_empty() {
                self.refresh_sessions.remove(&room);
            }
            self.last_refresh_room = Some(room.clone());
            self.load_refresh_next = false;
            return Some((RedecryptionBatch::LoadRefresh { room, session }, 0));
        }
        if let Some((room, events)) =
            take_event_batch(&mut self.refresh_events, &mut self.last_refresh_room)
        {
            self.consecutive_utd_batches = 0;
            self.load_refresh_next = true;
            return Some((RedecryptionBatch::Refresh { room, events }, 0));
        }
        None
    }

    fn merge(&mut self, pending: PendingRequests) {
        for (room, request) in pending.rooms {
            if !request.utd_session_ids.is_empty() {
                self.utd_sessions.entry(room.clone()).or_default().extend(request.utd_session_ids);
            }
            if !request.refresh_info_session_ids.is_empty() {
                self.refresh_sessions
                    .entry(room)
                    .or_default()
                    .extend(request.refresh_info_session_ids);
            }
        }
        for (room, events) in pending.loaded {
            self.add_loaded_events(&room, events.into_values());
        }
    }

    async fn execute_batch(
        &mut self,
        cache: &EventCache,
        batch: &RedecryptionBatch,
    ) -> Result<(), EventCacheError> {
        match batch {
            RedecryptionBatch::LoadUtds { room, session } => {
                let events = cache.get_utds(room, session).await?;
                trace!(
                    requested_sessions = 1,
                    selected_events = events.len(),
                    "Selected UTD retry events"
                );
                // Newer directly loaded snapshots win over a delayed store query.
                let pending = self.utd_events.entry(room.clone()).or_default();
                for (id, event) in events {
                    pending.entry(id).or_insert(event);
                }
                if pending.is_empty() {
                    self.utd_events.remove(room);
                }
                Ok(())
            }
            RedecryptionBatch::Decrypt { room, events } => {
                cache.retry_decryption_for_events(room, events.clone()).await
            }
            RedecryptionBatch::LoadRefresh { room, session } => {
                let events = cache.get_decrypted_events(room, session).await?;
                let pending = self.refresh_events.entry(room.clone()).or_default();
                for (id, event) in events {
                    pending.entry(id).or_insert(event);
                }
                if pending.is_empty() {
                    self.refresh_events.remove(room);
                }
                Ok(())
            }
            RedecryptionBatch::Refresh { room, events } => {
                if let Some(room) =
                    cache.inner.client().ok().and_then(|client| client.get_room(room))
                {
                    cache.update_encryption_info_for_events(&room, events.clone()).await?;
                }
                Ok(())
            }
        }
    }

    async fn run_batch(&mut self, cache: &EventCache) -> Result<(), EventCacheError> {
        let Some((batch, previous_attempts)) = self.take_batch() else {
            return Ok(());
        };
        let result = self.execute_batch(cache, &batch).await;
        if let Err(error) = &result {
            let attempts = previous_attempts + 1;
            if attempts < MAX_BATCH_ATTEMPTS {
                warn!(attempts, ?error, "Redecryption batch failed; scheduling a bounded retry");
                self.failed_batches.push(FailedBatch {
                    batch,
                    attempts,
                    retry_at: Instant::now() + BATCH_RETRY_DELAY * attempts as u32,
                });
            } else {
                warn!(attempts, ?error, "Redecryption batch failed; retry limit reached");
            }
        }
        result
    }

    fn add_loaded_events(
        &mut self,
        room: &RoomId,
        events: impl IntoIterator<Item = TimelineEvent>,
    ) {
        for event in events {
            if matches!(event.kind, TimelineEventKind::Decrypted(_)) {
                if let Some((id, decrypted)) = filter_timeline_event_to_decrypted(event) {
                    self.refresh_events.entry(room.to_owned()).or_default().insert(id, decrypted);
                }
            } else if let Some((id, raw)) = filter_timeline_event_to_utd(event) {
                self.utd_events.entry(room.to_owned()).or_default().insert(id, raw);
            }
        }
    }

    fn add_chunk_update(&mut self, update: RoomEventCacheLinkedChunkUpdate) {
        let room = update.linked_chunk_id.room_id();
        for event in update.updates.into_iter().flat_map(|update| update.into_items()) {
            if let Some((id, raw)) = filter_timeline_event_to_utd(event) {
                self.utd_events.entry(room.to_owned()).or_default().insert(id, raw);
            }
        }
    }
}

/// A function which can be used to filter and map [`TimelineEvent`]s into a
/// tuple of event ID and raw [`AnySyncTimelineEvent`].
///
/// The tuple can be used to attempt to redecrypt events.
fn filter_timeline_event_to_utd(
    event: TimelineEvent,
) -> Option<(OwnedEventId, Raw<AnySyncTimelineEvent>)> {
    let event_id = event.event_id();

    // Only pick out events that are UTDs, get just the Raw event as this is what
    // the OlmMachine needs.
    let event = as_variant!(event.kind, TimelineEventKind::UnableToDecrypt { event, .. } => event);
    // Zip the event ID and event together so we don't have to pick out the event ID
    // again. We need the event ID to replace the event in the cache.
    event_id.zip(event)
}

/// A function which can be used to filter an map [`TimelineEvent`]s into a
/// tuple of event ID and [`DecryptedRoomEvent`].
///
/// The tuple can be used to attempt to update the encryption info of the
/// decrypted event.
fn filter_timeline_event_to_decrypted(
    event: TimelineEvent,
) -> Option<(OwnedEventId, DecryptedRoomEvent)> {
    let event_id = event.event_id();

    let event = as_variant!(event.kind, TimelineEventKind::Decrypted(event) => event);
    // Zip the event ID and event together so we don't have to pick out the event ID
    // again. We need the event ID to replace the event in the cache.
    event_id.zip(event)
}

impl EventCache {
    /// Retrieve a set of events that we weren't able to decrypt.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The ID of the room where the events were sent to.
    /// * `session_id` - The unique ID of the room key that was used to encrypt
    ///   the event.
    async fn get_utds(
        &self,
        room_id: &RoomId,
        session_id: SessionId<'_>,
    ) -> Result<Vec<EventIdAndUtd>, EventCacheError> {
        let events = match self.inner.store.lock().await? {
            // If the lock is clean, no problem.
            // If the lock is dirty, it doesn't really matter as we are hitting the store
            // directly, there is no in-memory state to manage, so all good. Do not mark the lock as
            // non-dirty.
            EventCacheStoreLockState::Clean(guard) | EventCacheStoreLockState::Dirty(guard) => {
                guard.get_room_events(room_id, Some("m.room.encrypted"), Some(session_id)).await?
            }
        };

        Ok(events.into_iter().filter_map(filter_timeline_event_to_utd).collect())
    }

    /// Retrieve a set of events that we weren't able to decrypt from the memory
    /// of the event cache.
    async fn get_utds_from_memory(&self) -> BTreeMap<OwnedRoomId, Vec<EventIdAndUtd>> {
        let mut utds = BTreeMap::new();

        for (room_id, caches) in self.inner.by_room.read().await.iter() {
            let room_utds: Vec<_> = caches
                .all_events()
                .await
                .into_iter()
                .flatten()
                .filter_map(filter_timeline_event_to_utd)
                .collect();

            utds.insert(room_id.to_owned(), room_utds);
        }

        utds
    }

    async fn get_decrypted_events(
        &self,
        room_id: &RoomId,
        session_id: SessionId<'_>,
    ) -> Result<Vec<EventIdAndEvent>, EventCacheError> {
        #[cfg(any(test, feature = "testing"))]
        self.inner
            .redecryption_test_state
            .refresh_queries
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let events = match self.inner.store.lock().await? {
            // If the lock is clean, no problem.
            // If the lock is dirty, it doesn't really matter as we are hitting the store
            // directly, there is no in-memory state to manage, so all good. Do not mark the lock as
            // non-dirty.
            EventCacheStoreLockState::Clean(guard) | EventCacheStoreLockState::Dirty(guard) => {
                guard.get_room_events(room_id, None, Some(session_id)).await?
            }
        };

        Ok(events.into_iter().filter_map(filter_timeline_event_to_decrypted).collect())
    }

    async fn get_decrypted_events_from_memory(
        &self,
    ) -> BTreeMap<OwnedRoomId, Vec<EventIdAndEvent>> {
        let mut decrypted_events = BTreeMap::new();

        for (room_id, caches) in self.inner.by_room.read().await.iter() {
            let room_utds: Vec<_> = caches
                .all_events()
                .await
                .into_iter()
                .flatten()
                .filter_map(filter_timeline_event_to_decrypted)
                .collect();

            decrypted_events.insert(room_id.to_owned(), room_utds);
        }

        decrypted_events
    }

    /// Handle a chunk of events that we were previously unable to decrypt but
    /// have now successfully decrypted.
    ///
    /// This function will replace the existing UTD events in memory and the
    /// store and send out a [`RoomEventCacheUpdate`] for the newly
    /// decrypted events.
    ///
    /// # Arguments
    ///
    /// * `room_id` - The ID of the room where the events were sent to.
    /// * `events` - A chunk of events that were successfully decrypted.
    #[instrument(skip_all, fields(room_id))]
    async fn on_resolved_utds(
        &self,
        room_id: &RoomId,
        events: Vec<ResolvedUtd>,
    ) -> Result<(), EventCacheError> {
        if events.is_empty() {
            trace!("No events were redecrypted or updated, nothing to replace");
            return Ok(());
        }

        timer!("Resolving UTDs");

        // Get the cache for this particular room.
        let (room_cache, _drop_handles) = self.for_room(room_id).await?;

        let mut event_ids = BTreeSet::new();

        // Phase 1: under the room state write lock, collect cache handles and
        // perform all room-linked-chunk mutations. We deliberately do NOT call
        // replace_utds() on event-focused/pinned caches here to avoid an ABBA deadlock:
        // pagination holds an event-focused cache lock and then tries to acquire the
        // room state lock (via `save_events`), while this method would hold the
        // room state lock and try to acquire event-focused cache locks.
        let (pinned_cache, ef_caches) = {
            let mut state = room_cache.state().write().await?;

            let pinned_cache = state.pinned_event_cache().cloned();
            let ef_caches: Vec<_> = state.event_focused_caches().cloned().collect();

            // Consider the room linked chunk.
            let mut new_events = Vec::with_capacity(events.len());
            for resolved in &events {
                if let Some((location, mut target_event)) = state.find_event(&resolved.0).await? {
                    if !apply_resolved_utd(resolved, &mut target_event) {
                        continue;
                    }

                    // TODO: `replace_event_at()` propagates changes to the store for every
                    // event, we should probably have a bulk version of this?
                    state.replace_event_at(location, target_event.clone()).await?;
                    new_events.push(target_event);
                    event_ids.insert(resolved.0.clone());
                }
            }

            // Read receipt events aren't encrypted, so we can't have decrypted a new
            // one here. As a result, we don't have any new receipt events to
            // post-process, so we can just pass `None` here.
            //
            // Note: read receipts may be updated anyhow in the post-processing step,
            // as the redecryption may have decrypted some events that don't count as
            // unreads.
            let receipt_event = None;

            state
                .post_process_new_events(
                    new_events,
                    PostProcessingOrigin::Redecryption,
                    receipt_event,
                )
                .await?;

            room_cache.update_sender().send(
                RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs {
                    diffs: state.room_linked_chunk_mut().updates_as_vector_diffs(),
                    origin: EventsOrigin::Cache,
                }),
                Some(RoomEventCacheGenericUpdate { room_id: room_id.to_owned() }),
            );

            (pinned_cache, ef_caches)
        };
        // Room state write lock is dropped here.

        // Phase 2: replace UTDs in pinned and event-focused caches WITHOUT
        // holding the room state lock. These caches have their own internal
        // locks and don't need the room state lock.
        if let Some(pinned_cache) = pinned_cache {
            event_ids.extend(pinned_cache.replace_utds(&events).await?);
        }

        // TODO: This ain't great for performance; there shouldn't be that many
        // event-focused caches alive at the same time, but they could
        // accumulate over time. Consider keeping track of which linked chunk
        // contain which event id, to avoid doing the linear searches here.
        for replaced in join_all(ef_caches.iter().map(|cache| cache.replace_utds(&events))).await {
            event_ids.extend(replaced);
        }

        if !event_ids.is_empty() {
            let report =
                RedecryptorReport::ResolvedUtds { room_id: room_id.to_owned(), events: event_ids };
            let _ = self.inner.redecryption_channels.utd_reporter.send(report);
        }

        Ok(())
    }

    /// Attempt to decrypt a single event.
    async fn decrypt_event(
        &self,
        room_id: &RoomId,
        room: Option<&Room>,
        push_context: Option<&PushContext>,
        event: &Raw<EncryptedEvent>,
    ) -> Option<(DecryptedRoomEvent, Option<Vec<Action>>)> {
        if let Some(room) = room {
            match room
                .decrypt_event(
                    event.cast_ref_unchecked::<OriginalSyncRoomEncryptedEvent>(),
                    push_context,
                )
                .await
            {
                Ok(maybe_decrypted) => {
                    let actions = maybe_decrypted.push_actions().map(|a| a.to_vec());

                    if let TimelineEventKind::Decrypted(decrypted) = maybe_decrypted.kind {
                        Some((decrypted, actions))
                    } else {
                        warn!(
                            "Failed to redecrypt an event despite receiving a room key or request to redecrypt"
                        );
                        None
                    }
                }
                Err(e) => {
                    warn!(
                        "Failed to redecrypt an event despite receiving a room key or request to redecrypt {e:?}"
                    );
                    None
                }
            }
        } else {
            let client = self.inner.client().ok()?;
            let machine = client.olm_machine().await;
            let machine = machine.as_ref()?;

            match machine.decrypt_room_event(event, room_id, client.decryption_settings()).await {
                Ok(decrypted) => Some((decrypted, None)),
                Err(e) => {
                    warn!(
                        "Failed to redecrypt an event despite receiving a room key or a request to redecrypt {e:?}"
                    );
                    None
                }
            }
        }
    }

    /// Attempt to redecrypt a chunk of UTDs.
    #[instrument(skip_all, fields(room_id, session_id))]
    async fn retry_decryption_for_events(
        &self,
        room_id: &RoomId,
        events: Vec<EventIdAndUtd>,
    ) -> Result<(), EventCacheError> {
        trace!("Retrying to decrypt");

        if events.is_empty() {
            trace!("No relevant events found.");
            return Ok(());
        }

        let room = self.inner.client().ok().and_then(|client| client.get_room(room_id));
        let push_context =
            if let Some(room) = &room { room.push_context().await.ok().flatten() } else { None };

        let selected_events = events.len();
        // Attempt decryption without retaining payloads or IDs for diagnostics.
        let mut decrypted_events = Vec::with_capacity(events.len());

        for (event_id, event) in events {
            // If we managed to decrypt the event, and we should have to since we received
            // the room key for this specific event, then replace the event.
            if let Some((decrypted, actions)) = self
                .decrypt_event(
                    room_id,
                    room.as_ref(),
                    push_context.as_ref(),
                    event.cast_ref_unchecked(),
                )
                .await
            {
                decrypted_events.push((event_id, decrypted, actions));
            }
        }

        trace!(
            selected_events,
            decrypted_events = decrypted_events.len(),
            "Finished decryption attempts"
        );

        #[cfg(any(test, feature = "testing"))]
        if !decrypted_events.is_empty() {
            let pause = self.inner.redecryption_pause.lock().await.take();
            if let Some((reached, resume)) = pause {
                let _ = reached.send(());
                let _ = resume.await;
            }
        }

        // Replace the events and notify listeners that UTDs have been replaced with
        // decrypted events.
        self.on_resolved_utds(room_id, decrypted_events).await?;

        Ok(())
    }

    /// Attempt to update the encryption info for the given list of events.
    async fn update_encryption_info_for_events(
        &self,
        room: &Room,
        events: Vec<EventIdAndEvent>,
    ) -> Result<(), EventCacheError> {
        // Let's attempt to update their encryption info.
        let mut updated_events = Vec::with_capacity(events.len());
        let mut info_by_sender: BTreeMap<(String, ruma::OwnedUserId), Option<Arc<EncryptionInfo>>> =
            BTreeMap::new();

        for (event_id, mut event) in events {
            if let Some(session_id) = event.encryption_info.session_id() {
                let key = (session_id.to_owned(), event.encryption_info.sender.clone());
                let new_encryption_info = if let Some(info) = info_by_sender.get(&key) {
                    info.clone()
                } else {
                    #[cfg(any(test, feature = "testing"))]
                    {
                        let pause =
                            self.inner.redecryption_test_state.pause_refresh.lock().await.take();
                        if let Some((reached, resume)) = pause {
                            let _ = reached.send(());
                            let _ = resume.await;
                        }
                        self.inner
                            .redecryption_test_state
                            .info_queries
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                    let info =
                        room.get_encryption_info(session_id, &event.encryption_info.sender).await;
                    info_by_sender.insert(key, info.clone());
                    info
                };

                // Only create a replacement if the encryption info actually changed.
                if let Some(new_encryption_info) = new_encryption_info
                    && event.encryption_info != new_encryption_info
                {
                    event.encryption_info = new_encryption_info;
                    updated_events.push((event_id, event, None));
                }
            }
        }

        trace!(replacement_count = updated_events.len(), "Finished encryption-info refresh");

        self.on_resolved_utds(room.room_id(), updated_events).await
    }

    /// Explicitly request the redecryption of a set of events.
    ///
    /// The redecryption logic in the event cache might sometimes miss that a
    /// room key has become available and that a certain set of events has
    /// become decryptable.
    ///
    /// This might happen because some room keys might arrive in a separate
    /// process handling push notifications or if a room key arrives but the
    /// process shuts down before we could have decrypted the events.
    ///
    /// For this reason it is useful to tell the event cache explicitly that
    /// some events should be retried to be redecrypted.
    ///
    /// This method allows you to do so. The events that get decrypted, if any,
    /// will be advertised over the usual event cache subscription mechanism
    /// which can be accessed using the [`RoomEventCache::subscribe()`]
    /// method.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use matrix_sdk::{Client, event_cache::DecryptionRetryRequest};
    /// # use url::Url;
    /// # use ruma::owned_room_id;
    /// # use std::collections::BTreeSet;
    /// # async {
    /// # let homeserver = Url::parse("http://localhost:8080")?;
    /// # let client = Client::new(homeserver).await?;
    /// let event_cache = client.event_cache();
    /// let room_id = owned_room_id!("!my_room:localhost");
    ///
    /// let request = DecryptionRetryRequest {
    ///     room_id,
    ///     utd_session_ids: BTreeSet::from(["session_id".into()]),
    ///     refresh_info_session_ids: BTreeSet::new(),
    /// };
    ///
    /// event_cache.request_decryption(request);
    /// # anyhow::Ok(()) };
    /// ```
    pub fn request_decryption(&self, request: DecryptionRetryRequest) {
        self.inner.redecryption_channels.enqueue(|pending| pending.insert(request));
    }

    /// Retry events newly loaded from cache without querying all persisted
    /// events of their sessions. Callers must only supply inserted/reloaded
    /// events, not the worker's own encryption-info replacement diffs.
    #[doc(hidden)]
    pub fn request_decryption_for_loaded_events(
        &self,
        room_id: &RoomId,
        events: impl IntoIterator<Item = TimelineEvent>,
    ) {
        let events: BTreeMap<_, _> = events
            .into_iter()
            .filter_map(|event| {
                if matches!(
                    event.kind,
                    TimelineEventKind::Decrypted(_) | TimelineEventKind::UnableToDecrypt { .. }
                ) {
                    event.event_id().map(|id| (id, event))
                } else {
                    None
                }
            })
            .collect();
        if !events.is_empty() {
            self.inner.redecryption_channels.enqueue(|pending| {
                pending.loaded.entry(room_id.to_owned()).or_default().extend(events);
            });
        }
    }

    /// Wait for earlier explicit retries and queued linked-chunk updates.
    ///
    /// Tests must stop producing updates before using this barrier. It does not
    /// wait for unrelated sync or key-recovery tasks.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub async fn redecryptor_barrier_for_testing(&self) {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        assert!(
            self.inner
                .redecryption_channels
                .decryption_request_sender
                .send(DecryptionRetryCommand::Barrier(sender))
                .is_ok()
        );
        receiver.await.expect("redecryptor stopped before reaching the barrier");
    }

    /// Pause one successful retry after decryption, before it applies changes.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub async fn pause_next_redecryption_for_testing(
        &self,
    ) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (reached_sender, reached_receiver) = tokio::sync::oneshot::channel();
        let (resume_sender, resume_receiver) = tokio::sync::oneshot::channel();
        *self.inner.redecryption_pause.lock().await = Some((reached_sender, resume_receiver));
        (reached_receiver, resume_sender)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub async fn pause_next_refresh_for_testing(
        &self,
    ) -> (tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (reached_sender, reached_receiver) = tokio::sync::oneshot::channel();
        let (resume_sender, resume_receiver) = tokio::sync::oneshot::channel();
        *self.inner.redecryption_test_state.pause_refresh.lock().await =
            Some((reached_sender, resume_receiver));
        (reached_receiver, resume_sender)
    }

    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub fn redecryption_stats_for_testing(&self) -> RedecryptionTestStats {
        use std::sync::atomic::Ordering::Relaxed;
        RedecryptionTestStats {
            refresh_queries: self.inner.redecryption_test_state.refresh_queries.load(Relaxed),
            info_queries: self.inner.redecryption_test_state.info_queries.load(Relaxed),
        }
    }

    /// Model account retirement after a retry has started.
    #[doc(hidden)]
    #[cfg(any(test, feature = "testing"))]
    pub async fn abort_redecryptor_for_testing(&self) {
        let worker = &self.inner.drop_handles.get().expect("subscribed")._redecryptor._task;
        worker.abort();
        while !worker.is_finished() {
            tokio::task::yield_now().await;
        }
    }

    /// Subscribe to reports that the redecryptor generates.
    ///
    /// The redecryption logic in the event cache might sometimes miss that a
    /// room key has become available and that a certain set of events has
    /// become decryptable.
    ///
    /// This might happen because some room keys might arrive in a separate
    /// process handling push notifications or if room keys arrive faster than
    /// we can handle them.
    ///
    /// This stream can be used to get notified about such situations as well as
    /// a general channel where the event cache reports which events got
    /// successfully redecrypted.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use matrix_sdk::{Client, event_cache::RedecryptorReport};
    /// # use url::Url;
    /// # use tokio_stream::StreamExt;
    /// # async {
    /// # let homeserver = Url::parse("http://localhost:8080")?;
    /// # let client = Client::new(homeserver).await?;
    /// let event_cache = client.event_cache();
    ///
    /// let mut stream = event_cache.subscribe_to_decryption_reports();
    ///
    /// while let Some(Ok(report)) = stream.next().await {
    ///     match report {
    ///         RedecryptorReport::Lagging => {
    ///             // The event cache might have missed to redecrypt some events. We should tell
    ///             // it which events we care about, i.e. which events we're displaying to the
    ///             // user, and let it redecrypt things with an explicit request.
    ///         }
    ///         RedecryptorReport::BackupAvailable => {
    ///             // A backup has become available. We can, just like in the Lagging case, tell
    ///             // the event cache to attempt to redecrypt some events.
    ///             //
    ///             // This is only necessary with the BackupDownloadStrategy::OnDecryptionFailure
    ///             // as the decryption attempt in this case will trigger the download of the
    ///             // room key from the backup.
    ///         }
    ///         RedecryptorReport::ResolvedUtds { .. } => {
    ///             // This may be interesting for statistical reasons or in case we'd like to
    ///             // fetch and inspect these events in some manner.
    ///         }
    ///     }
    /// }
    /// # anyhow::Ok(()) };
    /// ```
    pub fn subscribe_to_decryption_reports(
        &self,
    ) -> impl Stream<Item = Result<RedecryptorReport, BroadcastStreamRecvError>> {
        BroadcastStream::new(self.inner.redecryption_channels.utd_reporter.subscribe())
    }
}

#[inline(always)]
fn upgrade_event_cache(cache: &Weak<EventCacheInner>) -> Option<EventCache> {
    cache.upgrade().map(|inner| EventCache { inner })
}

async fn schedule_memory_events(
    cache: &Weak<EventCacheInner>,
    work: &mut RedecryptionWork,
    report: RedecryptorReport,
) -> Result<(), ()> {
    let Some(cache) = upgrade_event_cache(cache) else {
        return Err(());
    };

    for (room, events) in cache.get_utds_from_memory().await {
        if !events.is_empty() {
            work.utd_events.entry(room).or_default().extend(events);
        }
    }
    for (room, events) in cache.get_decrypted_events_from_memory().await {
        if !events.is_empty() {
            work.refresh_events.entry(room).or_default().extend(events);
        }
    }
    let _ = cache.inner.redecryption_channels.utd_reporter.send(report);

    Ok(())
}

/// Struct holding on to the redecryption task.
///
/// This struct implements the bulk of the redecryption task. It listens to the
/// various streams that should trigger redecryption attempts.
///
/// For more info see the [module level docs](self).
pub(crate) struct Redecryptor {
    _task: BackgroundTaskHandle,
}

impl Redecryptor {
    /// Create a new [`Redecryptor`].
    ///
    /// This creates a task that listens to various streams and attempts to
    /// redecrypt UTDs that can be found inside the [`EventCache`].
    pub(super) fn new(
        client: &Client,
        cache: Weak<EventCacheInner>,
        receiver: UnboundedReceiver<DecryptionRetryCommand>,
        linked_chunk_update_sender: &Sender<RoomEventCacheLinkedChunkUpdate>,
    ) -> Self {
        let linked_chunk_stream = BroadcastStream::new(linked_chunk_update_sender.subscribe());
        let backup_state_stream = client.encryption().backups().state_stream();

        let task = client
            .task_monitor()
            .spawn_infinite_task("event_cache::redecryptor", async {
                let request_redecryption_stream = UnboundedReceiverStream::new(receiver);

                Self::listen_for_room_keys_task(
                    cache,
                    request_redecryption_stream,
                    linked_chunk_stream,
                    backup_state_stream,
                )
                .await;
            })
            .abort_on_drop();

        Self { _task: task }
    }

    /// (Re)-subscribe to the room key stream from the [`OlmMachine`].
    ///
    /// This needs to happen any time this stream returns a `None` meaning that
    /// the sending part of the stream has been dropped.
    async fn subscribe_to_room_key_stream(
        cache: &Weak<EventCacheInner>,
    ) -> Option<(
        impl Stream<Item = Result<Vec<RoomKeyInfo>, BroadcastStreamRecvError>>,
        impl Stream<Item = Vec<RoomKeyWithheldInfo>>,
    )> {
        let event_cache = cache.upgrade()?;
        let client = event_cache.client().ok()?;
        let machine = client.olm_machine().await;

        machine.as_ref().map(|m| {
            (m.store().room_keys_received_stream(), m.store().room_keys_withheld_received_stream())
        })
    }

    async fn redecryption_loop(
        cache: &Weak<EventCacheInner>,
        decryption_request_stream: &mut Pin<&mut impl Stream<Item = DecryptionRetryCommand>>,
        events_stream: &mut Pin<
            &mut impl Stream<Item = Result<RoomEventCacheLinkedChunkUpdate, BroadcastStreamRecvError>>,
        >,
        backup_state_stream: &mut Pin<
            &mut impl Stream<Item = Result<BackupState, BroadcastStreamRecvError>>,
        >,
        work: &mut RedecryptionWork,
    ) -> bool {
        let Some((room_key_stream, withheld_stream)) =
            Self::subscribe_to_room_key_stream(cache).await
        else {
            return false;
        };
        pin_mut!(room_key_stream);
        pin_mut!(withheld_stream);
        let mut input_budget = MAX_INPUTS_BETWEEN_BATCHES;

        loop {
            #[cfg(any(test, feature = "testing"))]
            if !work.barriers.is_empty() {
                use futures_util::FutureExt as _;
                let Some(cache) = upgrade_event_cache(cache) else {
                    return false;
                };
                work.merge(std::mem::take(&mut *cache.inner.redecryption_channels.pending.lock()));
                let mut drained = 0;
                while drained < MAX_INPUTS_BETWEEN_BATCHES {
                    let Some(Some(update)) = events_stream.next().now_or_never() else {
                        break;
                    };
                    work.add_chunk_update(update.expect("test must not lag linked-chunk updates"));
                    drained += 1;
                }
                if drained < MAX_INPUTS_BETWEEN_BATCHES && work.is_empty() {
                    for barrier in std::mem::take(&mut work.barriers) {
                        let _ = barrier.send(());
                    }
                }
            }
            if !work.has_runnable_work() {
                input_budget = MAX_INPUTS_BETWEEN_BATCHES;
            }

            // Inputs only enqueue work. At most one bounded work batch runs
            // before checking new requests/keys again; an intake budget also
            // guarantees refresh progress under a continuous input stream.
            tokio::select! {
                biased;
                Some(command) = decryption_request_stream.next(), if input_budget > 0 => {
                    input_budget -= 1;
                    let Some(cache) = upgrade_event_cache(cache) else { return false; };
                    match command {
                        DecryptionRetryCommand::Wake => {
                            let pending = std::mem::take(&mut *cache.inner.redecryption_channels.pending.lock());
                            trace!(request_rooms = pending.rooms.len(), loaded_rooms = pending.loaded.len(), "Consumed coalesced redecryption requests");
                            work.merge(pending);
                        }
                        #[cfg(any(test, feature = "testing"))]
                        DecryptionRetryCommand::Barrier(sender) => work.barriers.push(sender),
                    }
                }
                room_keys = room_key_stream.next(), if input_budget > 0 => {
                    input_budget -= 1;
                    match room_keys {
                        Some(Ok(keys)) => {
                            for key in keys {
                                work.utd_sessions.entry(key.room_id.clone()).or_default().insert(key.session_id.clone());
                                work.refresh_sessions.entry(key.room_id).or_default().insert(key.session_id);
                            }
                        }
                        Some(Err(_)) => {
                            warn!("The room key stream lagged, reporting the lag to our listeners");
                            if schedule_memory_events(cache, work, RedecryptorReport::Lagging).await.is_err() { return false; }
                        }
                        None => return true,
                    }
                }
                withheld_info = withheld_stream.next(), if input_budget > 0 => {
                    input_budget -= 1;
                    match withheld_info {
                        Some(infos) => {
                            for RoomKeyWithheldInfo { room_id, session_id, .. } in infos {
                                work.refresh_sessions.entry(room_id).or_default().insert(session_id);
                            }
                        }
                        None => return true,
                    }
                }
                Some(updates) = events_stream.next(), if input_budget > 0 => {
                    input_budget -= 1;
                    match updates {
                        Ok(update) => work.add_chunk_update(update),
                        Err(_) => {
                            if schedule_memory_events(cache, work, RedecryptorReport::Lagging).await.is_err() { return false; }
                        }
                    }
                }
                Some(update) = backup_state_stream.next(), if input_budget > 0 => {
                    input_budget -= 1;
                    let report = match update {
                        Ok(BackupState::Enabled) => Some(RedecryptorReport::BackupAvailable),
                        Err(_) => Some(RedecryptorReport::Lagging),
                        _ => None,
                    };
                    if let Some(report) = report {
                        if schedule_memory_events(cache, work, report).await.is_err() { return false; }
                    }
                }
                _ = work.wait_for_work(), if !work.is_empty() => {
                    input_budget = MAX_INPUTS_BETWEEN_BATCHES;
                    let Some(cache) = upgrade_event_cache(cache) else { return false; };
                    let started = Instant::now();
                    let _ = work.run_batch(&cache).await;
                    trace!(elapsed_ms = started.elapsed().as_millis(), "Finished redecryption work batch");
                }
                else => return false,
            }
        }
    }

    async fn listen_for_room_keys_task(
        cache: Weak<EventCacheInner>,
        decryption_request_stream: UnboundedReceiverStream<DecryptionRetryCommand>,
        events_stream: BroadcastStream<RoomEventCacheLinkedChunkUpdate>,
        backup_state_stream: impl Stream<Item = Result<BackupState, BroadcastStreamRecvError>>,
    ) {
        // We pin the decryption request stream here since that one doesn't need to be
        // recreated and we don't want to miss messages coming from the stream
        // while recreating it unnecessarily.
        pin_mut!(decryption_request_stream);
        pin_mut!(events_stream);
        pin_mut!(backup_state_stream);

        let mut work = RedecryptionWork::default();
        while Self::redecryption_loop(
            &cache,
            &mut decryption_request_stream,
            &mut events_stream,
            &mut backup_state_stream,
            &mut work,
        )
        .await
        {
            info!("Regenerating the re-decryption streams");

            // Report that the stream got recreated so listeners know about it, at the same
            // time retry to decrypt anything we have cached in memory.
            if schedule_memory_events(&cache, &mut work, RedecryptorReport::Lagging).await.is_err()
            {
                break;
            }
        }

        info!("Shutting down the event cache redecryptor");
    }
}

#[cfg(not(target_family = "wasm"))]
#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use assert_matches2::assert_matches;
    use async_trait::async_trait;
    use eyeball_im::VectorDiff;
    use matrix_sdk_base::{
        cross_process_lock::CrossProcessLockGeneration,
        crypto::types::events::{ToDeviceEvent, room::encrypted::ToDeviceEncryptedEventContent},
        deserialized_responses::{TimelineEventKind, VerificationState},
        event_cache::{
            Event, Gap,
            store::{EventCacheStore, EventCacheStoreError, MemoryStore},
        },
        linked_chunk::{
            ChunkIdentifier, ChunkIdentifierGenerator, ChunkMetadata, LinkedChunkId, Position,
            RawChunk, Update,
        },
        locks::Mutex,
        sleep::sleep,
        store::StoreConfig,
        timeout::timeout,
    };
    use matrix_sdk_common::cross_process_lock::CrossProcessLockConfig;
    use matrix_sdk_test::{JoinedRoomBuilder, async_test, event_factory::EventFactory};
    use ruma::{
        EventId, OwnedEventId, RoomId, RoomVersionId, device_id, event_id,
        events::{AnySyncTimelineEvent, relation::RelationType},
        room_id,
        serde::Raw,
        user_id,
    };
    use serde_json::json;
    use tokio::sync::oneshot::{self, Sender};
    use tracing::{Instrument, info};

    use crate::{
        Client, assert_let_timeout,
        encryption::EncryptionSettings,
        event_cache::{
            DecryptionRetryRequest, RoomEventCacheGenericUpdate, RoomEventCacheUpdate,
            TimelineVectorDiffs,
        },
        test_utils::mocks::MatrixMockServer,
    };

    #[async_test]
    async fn test_refresh_progresses_across_rooms_under_continuous_utd_requests() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let cache = client.event_cache();
        let mut work = super::RedecryptionWork::default();
        for room in [room_id!("!refresh-a:example.org"), room_id!("!refresh-b:example.org")] {
            work.refresh_sessions.insert(room.to_owned(), ["refresh-session".into()].into());
        }
        let mut iterations = 0;
        while cache.redecryption_stats_for_testing().refresh_queries < 2 && iterations < 32 {
            // Keep UTD work continuously ready. Sessions need not have events
            // to exercise the scheduler's fairness at the store boundary.
            work.utd_sessions
                .entry(room_id!("!utd:example.org").to_owned())
                .or_default()
                .insert(format!("utd-{iterations}"));
            work.run_batch(&cache).await.unwrap();
            iterations += 1;
        }
        assert_eq!(cache.redecryption_stats_for_testing().refresh_queries, 2);
        assert!(
            !work.utd_sessions.is_empty(),
            "refresh must progress while UTD work is still pending"
        );
    }

    #[async_test]
    async fn test_failed_session_query_stops_at_retry_limit() {
        let store = DelayingStore::new();
        store.delaying.store(false, Ordering::SeqCst);
        store.failing_queries.store(usize::MAX, Ordering::SeqCst);
        let server = MatrixMockServer::new().await;
        let client = server
            .client_builder()
            .on_builder(|builder| {
                builder.store_config(
                    StoreConfig::new(CrossProcessLockConfig::SingleProcess)
                        .event_cache_store(store.clone()),
                )
            })
            .build()
            .await;
        let cache = client.event_cache();
        let mut work = super::RedecryptionWork::default();
        work.refresh_sessions
            .insert(room_id!("!failed:example.org").to_owned(), ["session".into()].into());
        for attempt in 1..=super::MAX_BATCH_ATTEMPTS {
            assert!(work.run_batch(&cache).await.is_err());
            assert_eq!(store.query_count.load(Ordering::SeqCst), attempt);
            if attempt < super::MAX_BATCH_ATTEMPTS {
                assert!(!work.is_empty());
                assert_eq!(work.failed_batches.len(), 1);
                assert_eq!(work.failed_batches[0].attempts, attempt);
                // Advance just the retry deadline, without timing assertions.
                work.failed_batches[0].retry_at = ruma::time::Instant::now();
            }
        }
        assert!(work.is_empty(), "a persistent store error must not create a busy loop");
    }

    #[async_test]
    async fn test_failed_batches_recover_without_another_external_request() {
        #[derive(Clone, Copy, Debug)]
        enum Stage {
            LoadUtds,
            LoadRefresh,
            PersistDecryption,
            PersistRefresh,
        }

        let room_id = room_id!("!retry:localhost");
        let factory = EventFactory::new().room(room_id);
        let (alice, bob, server, store) = set_up_clients(room_id, false, true).await;
        let store = store.unwrap();
        store.delaying.store(false, Ordering::SeqCst);
        let cache = bob.event_cache();
        // Exercise the worker's batches directly so the injected failure and
        // retry deadline are deterministic; use real Megolm and store methods.
        cache.abort_redecryptor_for_testing().await;
        let (event, key) = prepare_room(&server, &factory, &alice, &bob, room_id).await;
        server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_joined_room(
                    JoinedRoomBuilder::new(room_id).add_timeline_event(event.clone()),
                );
            })
            .await;
        let (room_cache, _handles) = cache.for_room(room_id).await.unwrap();
        let event_id = event_id!("$some_id");
        let utd = room_cache.find_event_strict(event_id).await.unwrap().unwrap();
        assert_matches!(&utd.kind, TimelineEventKind::UnableToDecrypt { .. });
        server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_to_device_event(key.deserialize_as().unwrap());
            })
            .await;
        let room = bob.get_room(room_id).unwrap();
        let decrypted = room.decrypt_event(event.cast_ref_unchecked::<ruma::events::room::encrypted::OriginalSyncRoomEncryptedEvent>(), None).await.unwrap();
        assert_matches!(&decrypted.kind, TimelineEventKind::Decrypted(actual));
        let expected_info = actual.encryption_info.clone();
        assert_matches!(&expected_info.verification_state, VerificationState::Unverified(_));
        let session = expected_info.session_id().unwrap().to_owned();

        for stage in
            [Stage::LoadUtds, Stage::LoadRefresh, Stage::PersistDecryption, Stage::PersistRefresh]
        {
            let mut work = super::RedecryptionWork::default();
            let refresh = matches!(stage, Stage::LoadRefresh | Stage::PersistRefresh);
            let mut initial = if refresh { decrypted.clone() } else { utd.clone() };
            if let TimelineEventKind::Decrypted(event) = &mut initial.kind {
                Arc::make_mut(&mut event.encryption_info).verification_state =
                    VerificationState::Verified;
            }
            room_cache.replace_event_for_testing(event_id, initial.clone()).await;
            match stage {
                Stage::LoadUtds => {
                    work.utd_sessions.insert(room_id.to_owned(), [session.clone()].into());
                }
                Stage::PersistDecryption => {
                    work.utd_events
                        .insert(room_id.to_owned(), [(event_id.to_owned(), event.clone())].into());
                }
                Stage::LoadRefresh => {
                    work.refresh_sessions.insert(room_id.to_owned(), [session.clone()].into());
                }
                Stage::PersistRefresh => {
                    let TimelineEventKind::Decrypted(event) = initial.kind else {
                        panic!("decrypted fixture");
                    };
                    work.refresh_events
                        .insert(room_id.to_owned(), [(event_id.to_owned(), event)].into());
                }
            }
            if matches!(stage, Stage::LoadUtds | Stage::LoadRefresh) {
                store.failing_queries.store(1, Ordering::SeqCst);
            } else {
                store.failing_writes.store(1, Ordering::SeqCst);
            }
            assert!(work.run_batch(&cache).await.is_err(), "{stage:?}");
            assert!(!work.is_empty(), "failed work must remain pending: {stage:?}");
            assert_eq!(work.failed_batches.len(), 1, "{stage:?}");
            work.failed_batches[0].retry_at =
                ruma::time::Instant::now() + Duration::from_secs(3600);
            assert!(work.take_batch().is_none(), "respect retry backoff: {stage:?}");
            let another_room = room_id!("!other-retry:localhost");
            work.utd_sessions.insert(another_room.to_owned(), ["another-session".into()].into());
            work.run_batch(&cache).await.unwrap();
            assert!(
                !work.utd_sessions.contains_key(another_room),
                "new UTDs must not wait for backoff"
            );
            assert_eq!(work.failed_batches.len(), 1);
            work.failed_batches[0].retry_at = ruma::time::Instant::now();
            while !work.is_empty() {
                work.run_batch(&cache).await.unwrap();
            }
            let current = room_cache.find_event_strict(event_id).await.unwrap().unwrap();
            assert_matches!(current.kind, TimelineEventKind::Decrypted(actual));
            assert_eq!(actual.encryption_info, expected_info, "{stage:?}");
            let persisted =
                store.memory_store.find_event(room_id, event_id).await.unwrap().unwrap();
            assert_eq!(persisted.encryption_info(), Some(&expected_info), "{stage:?}");
        }
    }

    #[async_test]
    async fn test_failed_pinned_write_publishes_its_pending_replacement_on_retry() {
        use crate::test_utils::mocks::RoomRelationsResponseTemplate;

        let room_id = room_id!("!pinned-retry:localhost");
        let factory = EventFactory::new().room(room_id);
        let (alice, bob, server, store) = set_up_clients(room_id, false, true).await;
        let store = store.unwrap();
        store.delaying.store(false, Ordering::SeqCst);
        let cache = bob.event_cache();
        cache.abort_redecryptor_for_testing().await;
        let (event, key) = prepare_room(&server, &factory, &alice, &bob, room_id).await;
        let id = event_id!("$some_id");
        let pin = Raw::new(&json!({
            "event_id": "$pin", "sender": bob.user_id().unwrap(), "origin_server_ts": 1000,
            "type": "m.room.pinned_events", "state_key": "", "content": {"pinned": [id]},
        }))
        .unwrap()
        .cast_unchecked();
        server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_joined_room(
                    JoinedRoomBuilder::new(room_id).add_state_event(pin).add_timeline_event(event),
                );
            })
            .await;
        server.mock_room_relations().ok(RoomRelationsResponseTemplate::default()).mount().await;
        let (room_cache, _handles) = cache.for_room(room_id).await.unwrap();
        let (mut pinned_events, mut receiver) =
            room_cache.subscribe_to_pinned_events().await.unwrap();
        timeout(
            async {
                while pinned_events.is_empty() {
                    receiver.recv().await.unwrap();
                    pinned_events = room_cache.subscribe_to_pinned_events().await.unwrap().0;
                }
            },
            Duration::from_secs(5),
        )
        .await
        .unwrap();
        assert_matches!(&pinned_events[0].kind, TimelineEventKind::UnableToDecrypt { .. });
        server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_to_device_event(key.deserialize_as().unwrap());
            })
            .await;

        let mut work = super::RedecryptionWork::default();
        work.add_loaded_events(room_id, pinned_events);
        store.failing_pinned_writes.store(1, Ordering::SeqCst);
        assert!(work.run_batch(&cache).await.is_err());
        assert_eq!(work.failed_batches.len(), 1);
        work.failed_batches[0].retry_at = ruma::time::Instant::now();
        while !work.is_empty() {
            work.run_batch(&cache).await.unwrap();
        }
        let update = timeout(receiver.recv(), Duration::from_secs(5)).await.unwrap().unwrap();
        assert_matches!(&update.diffs[0], VectorDiff::Set { value, .. });
        assert_matches!(&value.kind, TimelineEventKind::Decrypted(_));
        let (events, _) = room_cache.subscribe_to_pinned_events().await.unwrap();
        assert_matches!(&events[0].kind, TimelineEventKind::Decrypted(_));
        let chunks =
            store.memory_store.load_all_chunks(LinkedChunkId::PinnedEvents(room_id)).await.unwrap();
        let persisted: Vec<_> = chunks
            .into_iter()
            .flat_map(|chunk| match chunk.content {
                matrix_sdk_base::linked_chunk::ChunkContent::Items(items) => items,
                matrix_sdk_base::linked_chunk::ChunkContent::Gap(_) => Vec::new(),
            })
            .collect();
        assert_matches!(&persisted[0].kind, TimelineEventKind::Decrypted(_));
    }

    #[async_test]
    async fn test_worker_retries_store_failures_and_remains_alive_after_the_limit() {
        let store = DelayingStore::new();
        store.delaying.store(false, Ordering::SeqCst);
        let server = MatrixMockServer::new().await;
        let client = server
            .client_builder()
            .on_builder(|builder| {
                builder.store_config(
                    StoreConfig::new(CrossProcessLockConfig::SingleProcess)
                        .event_cache_store(store.clone()),
                )
            })
            .build()
            .await;
        let cache = client.event_cache();
        cache.subscribe().unwrap();
        let mut expected_queries = 0;
        for (failures, attempts) in [(1, 2), (usize::MAX, super::MAX_BATCH_ATTEMPTS), (0, 1)] {
            store.failing_queries.store(failures, Ordering::SeqCst);
            cache.request_decryption(DecryptionRetryRequest {
                room_id: room_id!("!worker-retry:example.org").to_owned(),
                utd_session_ids: Default::default(),
                refresh_info_session_ids: ["session".into()].into(),
            });
            timeout(cache.redecryptor_barrier_for_testing(), Duration::from_secs(5)).await.unwrap();
            expected_queries += attempts;
            assert_eq!(store.query_count.load(Ordering::SeqCst), expected_queries);
        }
    }

    /// A wrapper for the memory store for the event cache.
    ///
    /// Delays the persisting of events, or linked chunk updates, to allow the
    /// testing of race conditions between the event cache and R2D2.
    #[derive(Debug, Clone)]
    struct DelayingStore {
        memory_store: MemoryStore,
        delaying: Arc<AtomicBool>,
        foo: Arc<Mutex<Option<Sender<()>>>>,
        failing_queries: Arc<AtomicUsize>,
        failing_writes: Arc<AtomicUsize>,
        failing_pinned_writes: Arc<AtomicUsize>,
        query_count: Arc<AtomicUsize>,
    }

    impl DelayingStore {
        fn new() -> Self {
            Self {
                memory_store: MemoryStore::new(),
                delaying: AtomicBool::new(true).into(),
                foo: Arc::new(Mutex::new(None)),
                failing_queries: Default::default(),
                failing_writes: Default::default(),
                failing_pinned_writes: Default::default(),
                query_count: Default::default(),
            }
        }

        async fn stop_delaying(&self) {
            let (sender, receiver) = oneshot::channel();

            {
                *self.foo.lock() = Some(sender);
            }

            self.delaying.store(false, Ordering::SeqCst);

            receiver.await.expect("We should be able to receive a response")
        }

        fn fail_if_requested(counter: &AtomicUsize) -> Result<(), EventCacheStoreError> {
            if counter
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| left.checked_sub(1))
                .is_ok()
            {
                Err(EventCacheStoreError::backend(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "injected transient store failure",
                )))
            } else {
                Ok(())
            }
        }
    }

    #[cfg_attr(target_family = "wasm", async_trait(?Send))]
    #[cfg_attr(not(target_family = "wasm"), async_trait)]
    impl EventCacheStore for DelayingStore {
        type Error = EventCacheStoreError;

        async fn close(&self) -> Result<(), EventCacheStoreError> {
            self.memory_store.close().await
        }

        async fn reopen(&self) -> Result<(), EventCacheStoreError> {
            self.memory_store.reopen().await
        }

        async fn try_take_leased_lock(
            &self,
            lease_duration_ms: u32,
            key: &str,
            holder: &str,
        ) -> Result<Option<CrossProcessLockGeneration>, Self::Error> {
            self.memory_store.try_take_leased_lock(lease_duration_ms, key, holder).await
        }

        async fn handle_linked_chunk_updates(
            &self,
            linked_chunk_id: LinkedChunkId<'_>,
            updates: Vec<Update<Event, Gap>>,
        ) -> Result<(), Self::Error> {
            Self::fail_if_requested(&self.failing_writes)?;
            if matches!(linked_chunk_id, LinkedChunkId::PinnedEvents(_)) {
                Self::fail_if_requested(&self.failing_pinned_writes)?;
            }
            // This is the key behaviour of this store - we wait to set this value until
            // someone calls `stop_delaying`.
            //
            // We use `sleep` here for simplicity. The cool way would be to use a custom
            // waker or something like that.
            while self.delaying.load(Ordering::SeqCst) {
                sleep(Duration::from_millis(10)).await;
            }

            let sender = self.foo.lock().take();
            let ret = self.memory_store.handle_linked_chunk_updates(linked_chunk_id, updates).await;

            if let Some(sender) = sender {
                sender.send(()).expect("We should be able to notify the other side that we're done with the storage operation");
            }

            ret
        }

        async fn load_all_chunks(
            &self,
            linked_chunk_id: LinkedChunkId<'_>,
        ) -> Result<Vec<RawChunk<Event, Gap>>, Self::Error> {
            self.memory_store.load_all_chunks(linked_chunk_id).await
        }

        async fn load_all_chunks_metadata(
            &self,
            linked_chunk_id: LinkedChunkId<'_>,
        ) -> Result<Vec<ChunkMetadata>, Self::Error> {
            self.memory_store.load_all_chunks_metadata(linked_chunk_id).await
        }

        async fn load_last_chunk(
            &self,
            linked_chunk_id: LinkedChunkId<'_>,
        ) -> Result<(Option<RawChunk<Event, Gap>>, ChunkIdentifierGenerator), Self::Error> {
            self.memory_store.load_last_chunk(linked_chunk_id).await
        }

        async fn load_previous_chunk(
            &self,
            linked_chunk_id: LinkedChunkId<'_>,
            before_chunk_identifier: ChunkIdentifier,
        ) -> Result<Option<RawChunk<Event, Gap>>, Self::Error> {
            self.memory_store.load_previous_chunk(linked_chunk_id, before_chunk_identifier).await
        }

        async fn clear_all_linked_chunks(&self) -> Result<(), Self::Error> {
            self.memory_store.clear_all_linked_chunks().await
        }

        async fn filter_duplicated_events(
            &self,
            linked_chunk_id: LinkedChunkId<'_>,
            events: Vec<OwnedEventId>,
        ) -> Result<Vec<(OwnedEventId, Position)>, Self::Error> {
            self.memory_store.filter_duplicated_events(linked_chunk_id, events).await
        }

        async fn find_event(
            &self,
            room_id: &RoomId,
            event_id: &EventId,
        ) -> Result<Option<Event>, Self::Error> {
            self.memory_store.find_event(room_id, event_id).await
        }

        async fn find_event_relations(
            &self,
            room_id: &RoomId,
            event_id: &EventId,
            filters: Option<&[RelationType]>,
        ) -> Result<Vec<(Event, Option<Position>)>, Self::Error> {
            self.memory_store.find_event_relations(room_id, event_id, filters).await
        }

        async fn get_room_events(
            &self,
            room_id: &RoomId,
            event_type: Option<&str>,
            session_id: Option<&str>,
        ) -> Result<Vec<Event>, Self::Error> {
            self.query_count.fetch_add(1, Ordering::SeqCst);
            Self::fail_if_requested(&self.failing_queries)?;
            self.memory_store.get_room_events(room_id, event_type, session_id).await
        }

        async fn save_event(&self, room_id: &RoomId, event: Event) -> Result<(), Self::Error> {
            self.memory_store.save_event(room_id, event).await
        }

        async fn optimize(&self) -> Result<(), Self::Error> {
            self.memory_store.optimize().await
        }

        async fn get_size(&self) -> Result<Option<usize>, Self::Error> {
            self.memory_store.get_size().await
        }
    }

    async fn set_up_clients(
        room_id: &RoomId,
        alice_enables_cross_signing: bool,
        use_delayed_store: bool,
    ) -> (Client, Client, MatrixMockServer, Option<DelayingStore>) {
        let alice_span = tracing::info_span!("alice");
        let bob_span = tracing::info_span!("bob");

        let alice_user_id = user_id!("@alice:localhost");
        let alice_device_id = device_id!("ALICEDEVICE");
        let bob_user_id = user_id!("@bob:localhost");
        let bob_device_id = device_id!("BOBDEVICE");

        let matrix_mock_server = MatrixMockServer::new().await;
        matrix_mock_server.mock_crypto_endpoints_preset().await;

        let encryption_settings = EncryptionSettings {
            auto_enable_cross_signing: alice_enables_cross_signing,
            ..Default::default()
        };

        // Create some clients for Alice and Bob.

        let alice = matrix_mock_server
            .client_builder_for_crypto_end_to_end(alice_user_id, alice_device_id)
            .on_builder(|builder| {
                builder
                    .with_enable_share_history_on_invite(true)
                    .with_encryption_settings(encryption_settings)
            })
            .build()
            .instrument(alice_span.clone())
            .await;

        let encryption_settings =
            EncryptionSettings { auto_enable_cross_signing: true, ..Default::default() };

        let (store_config, store) = if use_delayed_store {
            let store = DelayingStore::new();

            (
                StoreConfig::new(CrossProcessLockConfig::multi_process(
                    "delayed_store_event_cache_test",
                ))
                .event_cache_store(store.clone()),
                Some(store),
            )
        } else {
            (
                StoreConfig::new(CrossProcessLockConfig::multi_process(
                    "normal_store_event_cache_test",
                )),
                None,
            )
        };

        let bob = matrix_mock_server
            .client_builder_for_crypto_end_to_end(bob_user_id, bob_device_id)
            .on_builder(|builder| {
                builder
                    .with_enable_share_history_on_invite(true)
                    .with_encryption_settings(encryption_settings)
                    .store_config(store_config)
            })
            .build()
            .instrument(bob_span.clone())
            .await;

        bob.event_cache().subscribe().expect("Bob should be able to enable the event cache");

        // Ensure that Alice and Bob are aware of their devices and identities.
        matrix_mock_server.exchange_e2ee_identities(&alice, &bob).await;

        let event_factory = EventFactory::new().room(room_id).sender(alice_user_id);

        // Let us now create a room for them.
        let room_builder = JoinedRoomBuilder::new(room_id)
            .add_state_event(event_factory.create(alice_user_id, RoomVersionId::V1))
            .add_state_event(event_factory.room_encryption());

        matrix_mock_server
            .mock_sync()
            .ok_and_run(&alice, |builder| {
                builder.add_joined_room(room_builder.clone());
            })
            .instrument(alice_span)
            .await;

        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_joined_room(room_builder);
            })
            .instrument(bob_span)
            .await;

        (alice, bob, matrix_mock_server, store)
    }

    async fn prepare_room(
        matrix_mock_server: &MatrixMockServer,
        event_factory: &EventFactory,
        alice: &Client,
        bob: &Client,
        room_id: &RoomId,
    ) -> (Raw<AnySyncTimelineEvent>, Raw<ToDeviceEvent<ToDeviceEncryptedEventContent>>) {
        let alice_user_id = alice.user_id().unwrap();
        let bob_user_id = bob.user_id().unwrap();

        let alice_member_event = event_factory.member(alice_user_id).into_raw();
        let bob_member_event = event_factory.member(bob_user_id).into_raw();

        let room = alice
            .get_room(room_id)
            .expect("Alice should have access to the room now that we synced");

        // Alice will send a single event to the room, but this will trigger a to-device
        // message containing the room key to be sent as well. We capture both the event
        // and the to-device message.

        let event_type = "m.room.message";
        let content = json!({"body": "It's a secret to everybody", "msgtype": "m.text"});

        let event_id = event_id!("$some_id");
        let (event_receiver, mock) =
            matrix_mock_server.mock_room_send().ok_with_capture(event_id, alice_user_id);
        let (_guard, room_key) = matrix_mock_server.mock_capture_put_to_device(alice_user_id).await;

        {
            let _guard = mock.mock_once().mount_as_scoped().await;

            matrix_mock_server
                .mock_get_members()
                .ok(vec![alice_member_event.clone(), bob_member_event.clone()])
                .mock_once()
                .mount()
                .await;

            room.send_raw(event_type, content)
                .await
                .expect("We should be able to send an initial message");
        };

        // Let us retrieve the captured event and to-device message.
        let event = event_receiver.await.expect("Alice should have sent the event by now");
        let room_key = room_key.await;

        (event, room_key)
    }

    #[async_test]
    async fn test_redecryptor() {
        let room_id = room_id!("!test:localhost");

        let event_factory = EventFactory::new().room(room_id);
        let (alice, bob, matrix_mock_server, _) = set_up_clients(room_id, true, false).await;

        let (event, room_key) =
            prepare_room(&matrix_mock_server, &event_factory, &alice, &bob, room_id).await;

        // Let's now see what Bob's event cache does.

        let event_cache = bob.event_cache();
        let (room_cache, _) = event_cache
            .for_room(room_id)
            .await
            .expect("We should be able to get to the event cache for a specific room");

        let (_, mut subscriber) = room_cache.subscribe().await.unwrap();
        let mut generic_stream = event_cache.subscribe_to_room_generic_updates();

        // We regenerate the Olm machine to check if the room key stream is recreated to
        // correctly.
        bob.inner
            .base_client
            .regenerate_olm(None)
            .await
            .expect("We should be able to regenerate the Olm machine");

        // Let us forward the event to Bob.
        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_joined_room(JoinedRoomBuilder::new(room_id).add_timeline_event(event));
            })
            .await;

        // Alright, Bob has received an update from the cache.

        assert_let_timeout!(
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );

        // There should be a single new event, and it should be a UTD as we did not
        // receive the room key yet.
        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Append { values });
        assert_matches!(&values[0].kind, TimelineEventKind::UnableToDecrypt { .. });

        assert_let_timeout!(
            Ok(RoomEventCacheGenericUpdate { room_id: expected_room_id }) = generic_stream.recv()
        );
        assert_eq!(expected_room_id, room_id);
        assert!(generic_stream.is_empty());

        // Now we send the room key to Bob.
        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_to_device_event(
                    room_key
                        .deserialize_as()
                        .expect("We should be able to deserialize the room key"),
                );
            })
            .await;

        // Bob should receive a new update from the cache.
        assert_let_timeout!(
            Duration::from_secs(1),
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );

        // It should replace the UTD with a decrypted event.
        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Set { index, value });
        assert_eq!(*index, 0);
        assert_matches!(&value.kind, TimelineEventKind::Decrypted { .. });

        assert_let_timeout!(
            Ok(RoomEventCacheGenericUpdate { room_id: expected_room_id }) = generic_stream.recv()
        );
        assert_eq!(expected_room_id, room_id);
        assert!(generic_stream.is_empty());
    }

    #[async_test]
    async fn test_redecryptor_updating_encryption_info() {
        let bob_span = tracing::info_span!("bob");

        let room_id = room_id!("!test:localhost");

        let event_factory = EventFactory::new().room(room_id);
        let (alice, bob, matrix_mock_server, _) = set_up_clients(room_id, false, false).await;

        let (event, room_key) =
            prepare_room(&matrix_mock_server, &event_factory, &alice, &bob, room_id).await;

        // Let's now see what Bob's event cache does.

        let event_cache = bob.event_cache();
        let (room_cache, _) = event_cache
            .for_room(room_id)
            .instrument(bob_span.clone())
            .await
            .expect("We should be able to get to the event cache for a specific room");

        let (_, mut subscriber) = room_cache.subscribe().await.unwrap();
        let mut generic_stream = event_cache.subscribe_to_room_generic_updates();

        // Let us forward the event to Bob.
        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_joined_room(JoinedRoomBuilder::new(room_id).add_timeline_event(event));
            })
            .instrument(bob_span.clone())
            .await;

        // Alright, Bob has received an update from the cache.

        assert_let_timeout!(
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );

        // There should be a single new event, and it should be a UTD as we did not
        // receive the room key yet.
        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Append { values });
        assert_matches!(&values[0].kind, TimelineEventKind::UnableToDecrypt { .. });

        assert_let_timeout!(
            Ok(RoomEventCacheGenericUpdate { room_id: expected_room_id }) = generic_stream.recv()
        );
        assert_eq!(expected_room_id, room_id);
        assert!(generic_stream.is_empty());

        // Now we send the room key to Bob.
        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_to_device_event(
                    room_key
                        .deserialize_as()
                        .expect("We should be able to deserialize the room key"),
                );
            })
            .instrument(bob_span.clone())
            .await;

        // Bob should receive a new update from the cache.
        assert_let_timeout!(
            Duration::from_secs(1),
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );

        // It should replace the UTD with a decrypted event.
        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Set { index: 0, value });
        assert_matches!(&value.kind, TimelineEventKind::Decrypted { .. });

        let encryption_info = value.encryption_info().unwrap();
        assert_matches!(&encryption_info.verification_state, VerificationState::Unverified(_));

        assert_let_timeout!(
            Ok(RoomEventCacheGenericUpdate { room_id: expected_room_id }) = generic_stream.recv()
        );
        assert_eq!(expected_room_id, room_id);
        assert!(generic_stream.is_empty());

        let session_id = encryption_info.session_id().unwrap().to_owned();
        let alice_user_id = alice.user_id().unwrap();

        // Alice now creates the identity.
        alice
            .encryption()
            .bootstrap_cross_signing(None)
            .await
            .expect("Alice should be able to create the cross-signing keys");

        bob.update_tracked_users_for_testing([alice_user_id]).instrument(bob_span.clone()).await;
        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_change_device(alice_user_id);
            })
            .instrument(bob_span.clone())
            .await;

        bob.event_cache().request_decryption(DecryptionRetryRequest {
            room_id: room_id.into(),
            utd_session_ids: BTreeSet::new(),
            refresh_info_session_ids: BTreeSet::from([session_id]),
        });

        // Bob should again receive a new update from the cache, this time updating the
        // encryption info.
        assert_let_timeout!(
            Duration::from_secs(1),
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );

        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Set { index: 0, value });
        assert_matches!(&value.kind, TimelineEventKind::Decrypted { .. });
        let encryption_info = value.encryption_info().unwrap();

        assert_matches!(
            &encryption_info.verification_state,
            VerificationState::Unverified(_),
            "The event should now know about the identity but still be unverified"
        );

        assert_let_timeout!(
            Ok(RoomEventCacheGenericUpdate { room_id: expected_room_id }) = generic_stream.recv()
        );
        assert_eq!(expected_room_id, room_id);
        assert!(generic_stream.is_empty());
    }

    #[async_test]
    async fn test_event_is_redecrypted_even_if_key_arrives_while_event_processing() {
        let room_id = room_id!("!test:localhost");

        let event_factory = EventFactory::new().room(room_id);
        let (alice, bob, matrix_mock_server, delayed_store) =
            set_up_clients(room_id, true, true).await;

        let delayed_store = delayed_store.unwrap();

        let (event, room_key) =
            prepare_room(&matrix_mock_server, &event_factory, &alice, &bob, room_id).await;

        let event_cache = bob.event_cache();

        // Let's now see what Bob's event cache does.
        let (room_cache, _) = event_cache
            .for_room(room_id)
            .await
            .expect("We should be able to get to the event cache for a specific room");

        let (_, mut subscriber) = room_cache.subscribe().await.unwrap();
        let mut generic_stream = event_cache.subscribe_to_room_generic_updates();

        // Let us forward the event to Bob.
        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_joined_room(JoinedRoomBuilder::new(room_id).add_timeline_event(event));
            })
            .await;

        // Now we send the room key to Bob.
        matrix_mock_server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_to_device_event(
                    room_key
                        .deserialize_as()
                        .expect("We should be able to deserialize the room key"),
                );
            })
            .await;

        info!("Stopping the delay");
        delayed_store.stop_delaying().await;

        // Now that the first decryption attempt has failed since the sync with the
        // event did not contain the room key, and the decryptor has received
        // the room key but the event was not persisted in the cache as of yet,
        // let's the event cache process the event.

        // Alright, Bob has received an update from the cache.
        assert_let_timeout!(
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );

        // There should be a single new event, and it should be a UTD as we did not
        // receive the room key yet.
        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Append { values });
        assert_matches!(&values[0].kind, TimelineEventKind::UnableToDecrypt { .. });

        assert_let_timeout!(
            Ok(RoomEventCacheGenericUpdate { room_id: expected_room_id }) = generic_stream.recv()
        );
        assert_eq!(expected_room_id, room_id);

        // Bob should receive a new update from the cache.
        assert_let_timeout!(
            Duration::from_secs(1),
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );

        // It should replace the UTD with a decrypted event.
        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Set { index, value });
        assert_eq!(*index, 0);
        assert_matches!(&value.kind, TimelineEventKind::Decrypted { .. });

        assert_let_timeout!(
            Ok(RoomEventCacheGenericUpdate { room_id: expected_room_id }) = generic_stream.recv()
        );
        assert_eq!(expected_room_id, room_id);
        assert!(generic_stream.is_empty());
    }

    /// Regression test: the redecryptor must NOT hold the room state write
    /// lock while calling replace_utds() on event-focused caches, otherwise
    /// an ABBA deadlock occurs with concurrent event-focused cache pagination.
    #[async_test]
    async fn test_redecryptor_no_deadlock_with_event_focused_cache_pagination() {
        use crate::{
            event_cache::EventFocusThreadMode,
            test_utils::mocks::{RoomContextResponseTemplate, RoomMessagesResponseTemplate},
        };

        let room_id = room_id!("!test:localhost");
        let f = EventFactory::new().room(room_id);
        let (alice, bob, server, _) = set_up_clients(room_id, true, false).await;

        let (encrypted_event, room_key) = prepare_room(&server, &f, &alice, &bob, room_id).await;

        let event_cache = bob.event_cache();
        let (room_cache, _drop_handles) = event_cache
            .for_room(room_id)
            .await
            .expect("Bob should have an event cache for the room");

        let (_initial_events, mut subscriber) = room_cache.subscribe().await.unwrap();

        // Sync the encrypted event to Bob. Without the room key, it's a UTD.
        server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_joined_room(
                    JoinedRoomBuilder::new(room_id).add_timeline_event(encrypted_event),
                );
            })
            .await;

        // Consume the UTD update from the subscriber.
        assert_let_timeout!(
            Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs { diffs, .. })) =
                subscriber.recv()
        );
        assert_eq!(diffs.len(), 1);
        assert_matches!(&diffs[0], VectorDiff::Append { values });
        assert_matches!(&values[0].kind, TimelineEventKind::UnableToDecrypt { .. });

        // Create an event-focused cache with a backward pagination gap.
        let focused_event_id = event_id!("$focused");
        let bob_user_id = bob.user_id().unwrap();

        server
            .mock_room_event_context()
            .expect_any_access_token()
            .ok(RoomContextResponseTemplate::new(
                f.text_msg("focused msg")
                    .sender(bob_user_id)
                    .event_id(focused_event_id)
                    .into_event(),
            )
            .start("back-token"))
            .mock_once()
            .mount()
            .await;

        let event_focused_cache = room_cache
            .get_or_create_event_focused_cache(
                focused_event_id.to_owned(),
                20,
                EventFocusThreadMode::Automatic,
            )
            .await
            .unwrap();

        // Mock /messages with a long delay, simulating a slow network.
        server
            .mock_room_messages()
            .expect_any_access_token()
            .ok(RoomMessagesResponseTemplate::default().with_delay(Duration::from_secs(5)))
            .mock_once()
            .mount()
            .await;

        // Start backward pagination on the event-focused cache. This holds the cache
        // write lock across the slow /messages network request.
        let event_focused_cache_clone = event_focused_cache.clone();
        let pagination_task = tokio::spawn(async move {
            let _ = event_focused_cache_clone.paginate_backwards(20).await;
        });

        // Let the pagination task acquire the event-focused cache write lock.
        sleep(Duration::from_millis(200)).await;

        // Send the room key to Bob.
        //
        // The redecryptor background task will pick it up and decrypt the UTD.
        // Previously, on_resolved_utds would hold the room state write lock
        // while calling replace_utds on event-focused caches, creating an ABBA
        // deadlock with pagination (which holds event-focused lock and needs
        // room state lock via save_events).
        server
            .mock_sync()
            .ok_and_run(&bob, |builder| {
                builder.add_to_device_event(
                    room_key
                        .deserialize_as()
                        .expect("We should be able to deserialize the room key"),
                );
            })
            .await;

        // Wait for the redecryptor to process the room key.
        sleep(Duration::from_secs(1)).await;

        // Subscribing requires a room state read lock (which would be awaited forever,
        // before the fix).
        let (_events, _subscriber) = timeout(room_cache.subscribe(), Duration::from_millis(100))
            .await
            .expect("subscribing shouldn't timeout")
            .expect("subscribing should succeed");

        pagination_task.abort();
    }
}
