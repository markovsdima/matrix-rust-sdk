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
// See the License for that specific language governing permissions and
// limitations under the License.

use std::{
    collections::{HashMap, HashSet},
    fs,
    future::pending,
    io::Cursor,
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use futures_util::{StreamExt, pin_mut};
use matrix_sdk::{
    ComposerDraft as SdkComposerDraft, ComposerDraftType as SdkComposerDraftType,
    DraftAttachment as SdkDraftAttachment, DraftAttachmentContent, DraftThumbnail, EncryptionState,
    PredecessorRoom as SdkPredecessorRoom, RoomHero as SdkRoomHero, RoomMemberships, RoomState,
    SuccessorRoom as SdkSuccessorRoom,
    deserialized_responses::{
        AlgorithmInfo, EncryptionInfo, VerificationState as EventVerificationState,
    },
    encryption::LocalTrust,
    room::{
        IncludeRelations, RelationsOptions, Room as SdkRoom, RoomMemberRole,
        edit::EditedContent,
        power_levels::RoomPowerLevelChanges,
        reply::{EnforceThread, Reply, ReplyError},
    },
    send_queue::RoomSendQueueUpdate as SdkRoomSendQueueUpdate,
};
use matrix_sdk_common::{SendOutsideWasm, SyncOutsideWasm};
use matrix_sdk_ui::{
    timeline::{RoomExt, TimelineBuilder, default_event_filter},
    unable_to_decrypt_hook::UtdHookManager,
};
use mime::Mime;
use ruma::{
    EventId, Int, OwnedDeviceId, OwnedRoomOrAliasId, OwnedServerName, OwnedTransactionId,
    OwnedUserId, RoomAliasId, ServerName, UInt, UserId,
    api::Direction,
    assign,
    events::{
        AnyMessageLikeEventContent, AnySyncTimelineEvent, TimelineEventType,
        receipt::ReceiptThread,
        relation::RelationType,
        room::{
            MediaSource as RumaMediaSource,
            avatar::ImageInfo as RumaAvatarImageInfo,
            history_visibility::HistoryVisibility as RumaHistoryVisibility,
            join_rules::JoinRule as RumaJoinRule,
            message::{
                AddMentions, AudioMessageEventContent as RumaAudioMessageEventContent,
                FileMessageEventContent as RumaFileMessageEventContent,
                ImageMessageEventContent as RumaImageMessageEventContent,
                MessageType as RumaMessageType, RoomMessageEventContent,
                RoomMessageEventContentWithoutRelation, UnstableAmplitude,
                VideoMessageEventContent as RumaVideoMessageEventContent,
            },
        },
    },
    serde::Raw,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, warn};

use self::{power_levels::RoomPowerLevels, room_info::RoomInfo};
use crate::{
    TaskHandle,
    chunk_iterator::ChunkIterator,
    client::{JoinRule, ProgressWatcher, RoomVisibility},
    error::{
        ClientError, LiveLocationError, MediaInfoError, NotYetImplemented, QueueWedgeError,
        RoomError, UploadedFileError, UploadedImageError, UploadedVideoError, UploadedVoiceError,
    },
    event::TimelineEvent,
    identity_status_change::IdentityStatusChange,
    live_locations_observer::LiveLocationsObserver,
    room_member::{RoomMember, RoomMemberWithSenderInfo},
    room_preview::RoomPreview,
    ruma::{
        AudioInfo, AudioMessageContent, FileInfo, FileMessageContent, FormattedBody, ImageInfo,
        ImageMessageContent, MediaSource, MessageFormat, MessageType, ThumbnailInfo,
        UnstableAudioDetailsContent, UnstableVoiceContent, VideoInfo, VideoMessageContent,
    },
    runtime::get_runtime_handle,
    timeline::{
        AbstractProgress, LatestEventValue, ReceiptType, SendHandle, Timeline, UploadSource,
        configuration::{TimelineConfiguration, TimelineFilter},
        threads::{ThreadListService, ThreadSubscription},
    },
    utils::{AsyncRuntimeDropped, u64_to_uint},
};

mod power_levels;
pub mod room_info;

#[derive(Debug, Clone, uniffi::Enum)]
pub enum Membership {
    Invited,
    Joined,
    Left,
    Knocked,
    Banned,
}

impl From<RoomState> for Membership {
    fn from(value: RoomState) -> Self {
        match value {
            RoomState::Invited => Membership::Invited,
            RoomState::Joined => Membership::Joined,
            RoomState::Left => Membership::Left,
            RoomState::Knocked => Membership::Knocked,
            RoomState::Banned => Membership::Banned,
        }
    }
}

#[derive(uniffi::Object)]
pub struct Room {
    pub(super) inner: SdkRoom,
    utd_hook_manager: Option<Arc<UtdHookManager>>,
}

impl Room {
    pub(crate) fn new(inner: SdkRoom, utd_hook_manager: Option<Arc<UtdHookManager>>) -> Self {
        Room { inner, utd_hook_manager }
    }
}

#[derive(Clone, uniffi::Record)]
pub struct RawRoomEventEncryptionInfo {
    pub sender: String,
    pub sender_device: Option<String>,
    pub sender_curve25519_key_base64: Option<String>,
    pub sender_verified: bool,
}

impl From<&EncryptionInfo> for RawRoomEventEncryptionInfo {
    fn from(value: &EncryptionInfo) -> Self {
        let sender_curve25519_key_base64 = match &value.algorithm_info {
            AlgorithmInfo::OlmV1Curve25519AesSha2 { curve25519_public_key_base64 } => {
                Some(curve25519_public_key_base64.clone())
            }
            AlgorithmInfo::MegolmV1AesSha2 { curve25519_key, .. } => Some(curve25519_key.clone()),
        };

        Self {
            sender: value.sender.to_string(),
            sender_device: value.sender_device.as_ref().map(ToString::to_string),
            sender_curve25519_key_base64,
            sender_verified: matches!(value.verification_state, EventVerificationState::Verified),
        }
    }
}

#[derive(Clone, uniffi::Record)]
pub struct RawRoomEvent {
    pub room_id: String,
    pub event_type: String,
    pub event_id: Option<String>,
    pub sender: Option<String>,
    pub origin_server_ts_ms: Option<u64>,
    pub content_json: String,
    pub raw_json: String,
    pub encryption_info: Option<RawRoomEventEncryptionInfo>,
}

#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait RawRoomEventListener: SyncOutsideWasm + SendOutsideWasm {
    fn on_event(&self, event: RawRoomEvent);
}

#[derive(Deserialize)]
struct RawRoomEventDetails {
    #[serde(rename = "type")]
    event_type: String,
    event_id: Option<String>,
    sender: Option<String>,
    origin_server_ts: Option<u64>,
    content: Value,
}

fn raw_room_event_from_raw_json(
    room_id: String,
    raw_json: &str,
    encryption_info: Option<RawRoomEventEncryptionInfo>,
) -> serde_json::Result<RawRoomEvent> {
    let details = serde_json::from_str::<RawRoomEventDetails>(raw_json)?;
    let content_json = serde_json::to_string(&details.content)?;

    Ok(RawRoomEvent {
        room_id,
        event_type: details.event_type,
        event_id: details.event_id,
        sender: details.sender,
        origin_server_ts_ms: details.origin_server_ts,
        content_json,
        raw_json: raw_json.to_owned(),
        encryption_info,
    })
}

#[derive(Clone, Copy, uniffi::Enum)]
pub enum RawRoomRelationsDirection {
    Backward,
    Forward,
}

impl From<RawRoomRelationsDirection> for Direction {
    fn from(value: RawRoomRelationsDirection) -> Self {
        match value {
            RawRoomRelationsDirection::Backward => Direction::Backward,
            RawRoomRelationsDirection::Forward => Direction::Forward,
        }
    }
}

#[derive(Clone, uniffi::Record)]
pub struct RawRoomRelationsOptions {
    pub relation_type: Option<String>,
    pub event_type: Option<String>,
    pub from: Option<String>,
    pub limit: Option<u64>,
    pub direction: RawRoomRelationsDirection,
    pub recurse: bool,
}

#[derive(Clone, uniffi::Record)]
pub struct RawRoomRelations {
    pub chunk: Vec<RawRoomEvent>,
    pub prev_batch_token: Option<String>,
    pub next_batch_token: Option<String>,
    pub recursion_depth: Option<u64>,
}

#[matrix_sdk_ffi_macros::export]
impl Room {
    /// Returns the room's name from the state event if available, otherwise
    /// compute a room name based on the room's nature (DM or not) and number of
    /// members.
    pub fn display_name(&self) -> Option<String> {
        Some(self.inner.cached_display_name()?.to_string())
    }

    /// The raw name as present in the room state event.
    pub fn raw_name(&self) -> Option<String> {
        self.inner.name()
    }

    pub fn topic(&self) -> Option<String> {
        self.inner.topic()
    }

    pub fn avatar_url(&self) -> Option<String> {
        self.inner.avatar_url().map(|m| m.to_string())
    }

    pub async fn is_direct(&self) -> bool {
        self.inner.is_direct().await.unwrap_or(false)
    }

    /// Whether the room can be publicly joined or not, based on its join rule.
    ///
    /// Can return `None` if the join rule state event is missing.
    pub fn is_public(&self) -> Option<bool> {
        self.inner.is_public()
    }

    pub fn is_space(&self) -> bool {
        self.inner.is_space()
    }

    /// If this room is tombstoned, return the “reference” to the successor room
    /// —i.e. the room replacing this one.
    ///
    /// A room is tombstoned if it has received a [`m.room.tombstone`] state
    /// event.
    ///
    /// [`m.room.tombstone`]: https://spec.matrix.org/v1.14/client-server-api/#mroomtombstone
    pub fn successor_room(&self) -> Option<SuccessorRoom> {
        self.inner.successor_room().map(Into::into)
    }

    /// If this room is the successor of a tombstoned room, return the
    /// “reference” to the predecessor room.
    ///
    /// A room is tombstoned if it has received a [`m.room.tombstone`] state
    /// event.
    ///
    /// To determine if a room is the successor of a tombstoned room, the
    /// [`m.room.create`] must have been received, **with** a `predecessor`
    /// field.
    ///
    /// [`m.room.tombstone`]: https://spec.matrix.org/v1.14/client-server-api/#mroomtombstone
    /// [`m.room.create`]: https://spec.matrix.org/v1.14/client-server-api/#mroomcreate
    pub fn predecessor_room(&self) -> Option<PredecessorRoom> {
        self.inner.predecessor_room().map(Into::into)
    }

    pub fn canonical_alias(&self) -> Option<String> {
        self.inner.canonical_alias().map(|a| a.to_string())
    }

    pub fn alternative_aliases(&self) -> Vec<String> {
        self.inner.alt_aliases().iter().map(|a| a.to_string()).collect()
    }

    /// Get the user who created the invite, if any.
    pub async fn inviter(&self) -> Result<Option<RoomMember>, ClientError> {
        let invite_details = self.inner.invite_details().await?;

        match invite_details.inviter {
            Some(inviter) => Ok(Some(inviter.try_into()?)),
            None => Ok(None),
        }
    }

    /// The room's current membership state.
    pub fn membership(&self) -> Membership {
        self.inner.state().into()
    }

    /// Returns the room heroes for this room.
    pub fn heroes(&self) -> Vec<RoomHero> {
        self.inner.heroes().into_iter().map(Into::into).collect()
    }

    /// Is there a non expired membership with application "m.call" and scope
    /// "m.room" in this room.
    pub fn has_active_room_call(&self) -> bool {
        self.inner.has_active_room_call()
    }

    /// Returns a Vec of userId's that participate in the room call.
    ///
    /// MatrixRTC memberships with application "m.call" and scope "m.room" are
    /// considered. A user can occur twice if they join with two devices.
    /// convert to a set depending if the different users are required or the
    /// amount of sessions.
    ///
    /// The vector is ordered by oldest membership user to newest.
    pub fn active_room_call_participants(&self) -> Vec<String> {
        self.inner.active_room_call_participants().iter().map(|u| u.to_string()).collect()
    }

    /// Forces the currently active room key, which is used to encrypt messages,
    /// to be rotated.
    ///
    /// A new room key will be crated and shared with all the room members the
    /// next time a message will be sent. You don't have to call this method,
    /// room keys will be rotated automatically when necessary. This method is
    /// still useful for debugging purposes.
    pub async fn discard_room_key(&self) -> Result<(), ClientError> {
        self.inner.discard_room_key().await?;
        Ok(())
    }

    /// Create a timeline with a default configuration, i.e. a live timeline
    /// with read receipts and read marker tracking.
    pub async fn timeline(&self) -> Result<Arc<Timeline>, ClientError> {
        Ok(Timeline::new(self.inner.timeline().await?))
    }

    /// Build a new timeline instance with the given configuration.
    pub async fn timeline_with_configuration(
        &self,
        configuration: TimelineConfiguration,
    ) -> Result<Arc<Timeline>, ClientError> {
        let mut builder = matrix_sdk_ui::timeline::TimelineBuilder::new(&self.inner);

        builder = builder
            .with_focus(configuration.focus.try_into()?)
            .with_date_divider_mode(configuration.date_divider_mode.into())
            .track_read_marker_and_receipts(configuration.track_read_receipts);

        match configuration.filter {
            TimelineFilter::All => {
                // #nofilter.
            }

            TimelineFilter::OnlyMessage { types } => {
                builder = builder.event_filter(move |event, room_version_id| {
                    default_event_filter(event, room_version_id)
                        && match event {
                            AnySyncTimelineEvent::MessageLike(msg) => {
                                match msg.original_content() {
                                    Some(AnyMessageLikeEventContent::RoomMessage(content)) => {
                                        types.contains(&content.msgtype.into())
                                    }
                                    _ => false,
                                }
                            }
                            _ => false,
                        }
                });
            }

            TimelineFilter::EventFilter { filter: event_filter } => {
                builder = builder.event_filter(move |event, room_version_id| {
                    // Always perform the default filter first
                    default_event_filter(event, room_version_id) && event_filter.filter(event)
                });
            }
        }

        if let Some(internal_id_prefix) = configuration.internal_id_prefix {
            builder = builder.with_internal_id_prefix(internal_id_prefix);
        }

        if configuration.report_utds {
            if let Some(utd_hook_manager) = self.utd_hook_manager.clone() {
                builder = builder.with_unable_to_decrypt_hook(utd_hook_manager);
            } else {
                return Err(ClientError::Generic { msg: "Failed creating timeline because the configuration is set to report UTDs but no hook manager is set".to_owned(), details: None });
            }
        }

        let timeline = builder.build().await?;

        Ok(Timeline::new(timeline))
    }

    /// Listen for live raw timeline events in this room, filtered by event type.
    ///
    /// This bypasses the UI timeline item model, so relation events such as
    /// `m.reaction` and `m.room.redaction` can be observed even when they are
    /// aggregated into other timeline items.
    ///
    /// The returned task handle keeps the event handler registered and removes
    /// it when cancelled or dropped.
    pub fn subscribe_to_raw_timeline_events(
        &self,
        event_types: Vec<String>,
        listener: Box<dyn RawRoomEventListener>,
    ) -> Arc<TaskHandle> {
        let event_types = Arc::new(event_types.into_iter().collect::<HashSet<_>>());
        let listener: Arc<dyn RawRoomEventListener> = Arc::from(listener);
        let room_id: Arc<str> = self.inner.room_id().as_str().into();

        let event_handler = self.inner.add_event_handler({
            move |raw: Raw<AnySyncTimelineEvent>, encryption_info: Option<EncryptionInfo>| {
                let event_types = event_types.clone();
                let listener = listener.clone();
                let room_id = room_id.clone();

                async move {
                    let event_type = match raw.get_field::<String>("type") {
                        Ok(Some(event_type)) => event_type,
                        Ok(None) => {
                            warn!("Raw room event is missing an event type");
                            return;
                        }
                        Err(error) => {
                            warn!("Failed to parse raw room event type: {error}");
                            return;
                        }
                    };

                    if !event_types.contains(&event_type) {
                        return;
                    }

                    let encryption_info =
                        encryption_info.as_ref().map(RawRoomEventEncryptionInfo::from);
                    let raw_json = raw.json().get();
                    let event = match raw_room_event_from_raw_json(
                        room_id.to_string(),
                        raw_json,
                        encryption_info,
                    ) {
                        Ok(event) => event,
                        Err(error) => {
                            warn!("Failed to parse raw room event: {error}");
                            return;
                        }
                    };

                    listener.on_event(event);
                }
            }
        });

        let drop_guard = self.inner.client().event_handler_drop_guard(event_handler);

        Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            let _drop_guard = drop_guard;
            pending::<()>().await;
        })))
    }

    /// Retrieve raw room events related to a given event, using the Matrix
    /// relations API.
    ///
    /// This is useful to backfill relation events such as `m.reaction` events
    /// for a MatrixRTC membership event without going through the UI timeline
    /// aggregation model.
    pub async fn get_event_relations(
        &self,
        event_id: String,
        options: RawRoomRelationsOptions,
    ) -> Result<RawRoomRelations, ClientError> {
        let event_id = EventId::parse(event_id)?;
        let include_relations = match (options.relation_type, options.event_type) {
            (None, None) => IncludeRelations::AllRelations,
            (Some(relation_type), None) => {
                IncludeRelations::RelationsOfType(RelationType::from(relation_type.as_str()))
            }
            (Some(relation_type), Some(event_type)) => {
                IncludeRelations::RelationsOfTypeAndEventType(
                    RelationType::from(relation_type.as_str()),
                    TimelineEventType::from(event_type.as_str()),
                )
            }
            (None, Some(event_type)) => {
                return Err(ClientError::Generic {
                    msg: "event_type requires relation_type when querying room relations"
                        .to_owned(),
                    details: Some(format!("event_type={event_type}")),
                });
            }
        };
        let limit = options
            .limit
            .map(|limit| {
                UInt::new(limit).ok_or_else(|| ClientError::Generic {
                    msg: "relations limit is too large".to_owned(),
                    details: Some(format!("limit={limit}")),
                })
            })
            .transpose()?;

        let relations = self
            .inner
            .relations(
                event_id,
                RelationsOptions {
                    from: options.from,
                    dir: options.direction.into(),
                    limit,
                    include_relations,
                    recurse: options.recurse,
                },
            )
            .await?;

        let room_id = self.inner.room_id().to_string();
        let mut chunk = Vec::with_capacity(relations.chunk.len());
        for event in relations.chunk {
            let raw_json = event.raw().json().get();
            let encryption_info =
                event.encryption_info().map(|info| RawRoomEventEncryptionInfo::from(info.as_ref()));
            chunk.push(
                raw_room_event_from_raw_json(room_id.clone(), raw_json, encryption_info)
                    .map_err(ClientError::from_err)?,
            );
        }

        Ok(RawRoomRelations {
            chunk,
            prev_batch_token: relations.prev_batch_token,
            next_batch_token: relations.next_batch_token,
            recursion_depth: relations.recursion_depth.map(Into::into),
        })
    }

    pub fn id(&self) -> String {
        self.inner.room_id().to_string()
    }

    pub fn encryption_state(&self) -> EncryptionState {
        self.inner.encryption_state()
    }

    /// Checks whether the room is encrypted or not.
    ///
    /// **Note**: this info may not be reliable if you don't set up
    /// `m.room.encryption` as required state.
    async fn is_encrypted(&self) -> bool {
        self.inner
            .latest_encryption_state()
            .await
            .map(|state| state.is_encrypted())
            .unwrap_or(false)
    }

    async fn latest_event(&self) -> LatestEventValue {
        self.inner.latest_event().await.into()
    }

    pub async fn latest_encryption_state(&self) -> Result<EncryptionState, ClientError> {
        Ok(self.inner.latest_encryption_state().await?)
    }

    pub async fn members(&self) -> Result<Arc<RoomMembersIterator>, ClientError> {
        Ok(Arc::new(RoomMembersIterator::new(self.inner.members(RoomMemberships::empty()).await?)))
    }

    pub async fn members_no_sync(&self) -> Result<Arc<RoomMembersIterator>, ClientError> {
        Ok(Arc::new(RoomMembersIterator::new(
            self.inner.members_no_sync(RoomMemberships::empty()).await?,
        )))
    }

    pub async fn member(&self, user_id: String) -> Result<RoomMember, ClientError> {
        let user_id = UserId::parse(&*user_id)?;
        let member = self.inner.get_member(&user_id).await?.context("User not found")?;
        Ok(member.try_into().context("Unknown state membership")?)
    }

    pub async fn member_avatar_url(&self, user_id: String) -> Result<Option<String>, ClientError> {
        let user_id = UserId::parse(&*user_id)?;
        let member = self.inner.get_member(&user_id).await?.context("User not found")?;
        let avatar_url_string = member.avatar_url().map(|m| m.to_string());
        Ok(avatar_url_string)
    }

    pub async fn member_display_name(
        &self,
        user_id: String,
    ) -> Result<Option<String>, ClientError> {
        let user_id = UserId::parse(&*user_id)?;
        let member = self.inner.get_member(&user_id).await?.context("User not found")?;
        let avatar_url_string = member.display_name().map(|m| m.to_owned());
        Ok(avatar_url_string)
    }

    pub async fn set_own_member_display_name(
        &self,
        display_name: Option<String>,
    ) -> Result<(), ClientError> {
        self.inner.set_own_member_display_name(display_name).await?;
        Ok(())
    }

    /// Get the membership details for the current user.
    ///
    /// Returns:
    ///     - If the user was present in the room, a
    ///       [`matrix_sdk::room::RoomMemberWithSenderInfo`] containing both the
    ///       user info and the member info of the sender of the `m.room.member`
    ///       event.
    ///     - If the current user is not present, an error.
    pub async fn member_with_sender_info(
        &self,
        user_id: String,
    ) -> Result<RoomMemberWithSenderInfo, ClientError> {
        let user_id = UserId::parse(&*user_id)?;
        self.inner.member_with_sender_info(&user_id).await?.try_into()
    }

    pub async fn room_info(&self) -> Result<RoomInfo, ClientError> {
        RoomInfo::new(&self.inner).await
    }

    pub fn subscribe_to_room_info_updates(
        self: Arc<Self>,
        listener: Box<dyn RoomInfoListener>,
    ) -> Arc<TaskHandle> {
        let mut subscriber = self.inner.subscribe_info();
        Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            while subscriber.next().await.is_some() {
                match self.room_info().await {
                    Ok(room_info) => listener.call(room_info),
                    Err(e) => {
                        error!("Failed to compute new RoomInfo: {e}");
                    }
                }
            }
        })))
    }

    pub async fn set_is_favourite(
        &self,
        is_favourite: bool,
        tag_order: Option<f64>,
    ) -> Result<(), ClientError> {
        self.inner.set_is_favourite(is_favourite, tag_order).await?;
        Ok(())
    }

    pub async fn set_is_low_priority(
        &self,
        is_low_priority: bool,
        tag_order: Option<f64>,
    ) -> Result<(), ClientError> {
        self.inner.set_is_low_priority(is_low_priority, tag_order).await?;
        Ok(())
    }

    /// Send a raw event to the room.
    ///
    /// # Arguments
    ///
    /// * `event_type` - The type of the event to send.
    ///
    /// * `content` - The content of the event to send encoded as JSON string.
    pub async fn send_raw(&self, event_type: String, content: String) -> Result<(), ClientError> {
        let content_json: serde_json::Value =
            serde_json::from_str(&content).map_err(|e| ClientError::Generic {
                msg: format!("Failed to parse JSON: {e}"),
                details: Some(format!("{e:?}")),
            })?;

        self.inner.send_raw(&event_type, content_json).await?;

        Ok(())
    }

    /// Send a raw event to the room with a caller-provided transaction ID.
    ///
    /// # Arguments
    ///
    /// * `event_type` - The type of the event to send.
    ///
    /// * `content` - The content of the event to send encoded as JSON string.
    ///
    /// * `transaction_id` - The transaction ID to use for the event.
    pub async fn send_raw_with_transaction_id(
        &self,
        event_type: String,
        content: String,
        transaction_id: String,
    ) -> Result<(), ClientError> {
        let content_json: serde_json::Value =
            serde_json::from_str(&content).map_err(|e| ClientError::Generic {
                msg: format!("Failed to parse JSON: {e}"),
                details: Some(format!("{e:?}")),
            })?;

        let txn_id: OwnedTransactionId = transaction_id.into();

        self.inner.send_raw(&event_type, content_json).with_transaction_id(&txn_id).await?;

        Ok(())
    }

    /// Send a raw event to the room with a caller-provided transaction ID,
    /// returning the event ID from the server response.
    ///
    /// # Arguments
    ///
    /// * `event_type` - The type of the event to send.
    ///
    /// * `content` - The content of the event to send encoded as JSON string.
    ///
    /// * `transaction_id` - The transaction ID to use for the event.
    pub async fn send_raw_with_transaction_id_returning_event_id(
        &self,
        event_type: String,
        content: String,
        transaction_id: String,
    ) -> Result<String, ClientError> {
        let content_json: serde_json::Value =
            serde_json::from_str(&content).map_err(|e| ClientError::Generic {
                msg: format!("Failed to parse JSON: {e}"),
                details: Some(format!("{e:?}")),
            })?;

        let txn_id: OwnedTransactionId = transaction_id.into();

        let result =
            self.inner.send_raw(&event_type, content_json).with_transaction_id(&txn_id).await?;

        Ok(result.response.event_id.to_string())
    }

    /// Send a typed `m.room.message` event with a caller-provided transaction ID,
    /// returning the event ID from the server response.
    ///
    /// This bypasses the SDK send queue/local echo path while still using Ruma
    /// typed message content built by the caller.
    pub async fn send_message_type_with_transaction_id_returning_event_id(
        &self,
        msg_type: MessageType,
        transaction_id: String,
        reply_event_id: Option<String>,
    ) -> Result<String, ClientError> {
        let ruma_msg_type: RumaMessageType = msg_type.try_into()?;
        let content_without_relation = RoomMessageEventContentWithoutRelation::new(ruma_msg_type);

        let content = if let Some(reply_event_id) = reply_event_id {
            let event_id = EventId::parse(reply_event_id)?;
            let reply = Reply {
                event_id,
                enforce_thread: EnforceThread::MaybeThreaded,
                add_mentions: AddMentions::Yes,
            };

            self.inner
                .make_reply_event(content_without_relation, reply)
                .await
                .map_err(ClientError::from_err)?
        } else {
            RoomMessageEventContent::from(content_without_relation)
        };

        let txn_id: OwnedTransactionId = transaction_id.into();
        let result = self.inner.send(content).with_transaction_id(txn_id).await?;

        Ok(result.response.event_id.to_string())
    }

    /// Upload an image and optional thumbnail for a later typed `m.image` event.
    ///
    /// The returned JSON string is opaque to the caller and should be persisted
    /// as-is until it is passed to
    /// [`send_uploaded_image_with_transaction_id_returning_event_id`].
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_image_for_event(
        &self,
        original_file_path: String,
        thumbnail_file_path: Option<String>,
        original_mimetype: String,
        original_size: u64,
        original_width: u64,
        original_height: u64,
        thumbnail_mimetype: Option<String>,
        thumbnail_size: Option<u64>,
        thumbnail_width: Option<u64>,
        thumbnail_height: Option<u64>,
        blurhash: Option<String>,
    ) -> Result<String, UploadedImageError> {
        let original_mimetype = parse_image_mimetype(&original_mimetype, "original_mimetype")?;
        let original_size = validate_positive_uint(original_size, "original_size")?;
        let original_width = validate_positive_uint(original_width, "original_width")?;
        let original_height = validate_positive_uint(original_height, "original_height")?;
        let original =
            read_local_media_file(&original_file_path, original_size, "original_file_path")?;

        let thumbnail = read_thumbnail_input(
            thumbnail_file_path,
            thumbnail_mimetype,
            thumbnail_size,
            thumbnail_width,
            thumbnail_height,
        )?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();

        let media_source =
            upload_media_source(&self.inner, is_encrypted, &original_mimetype, original.data)
                .await?;

        let (thumbnail_source, thumbnail_info) = if let Some(thumbnail) = thumbnail {
            let thumbnail_source =
                upload_media_source(&self.inner, is_encrypted, &thumbnail.mimetype, thumbnail.data)
                    .await?;

            (
                Some(thumbnail_source),
                Some(UploadedThumbnailInfo {
                    mimetype: thumbnail.mimetype.essence_str().to_owned(),
                    size: thumbnail.size,
                    width: thumbnail.width,
                    height: thumbnail.height,
                }),
            )
        } else {
            (None, None)
        };

        let uploaded = UploadedImage {
            schema_version: UPLOADED_IMAGE_SCHEMA_VERSION,
            kind: UPLOADED_IMAGE_KIND.to_owned(),
            is_encrypted,
            filename: original.filename,
            media_source,
            thumbnail_source,
            image_info: UploadedImageInfo {
                mimetype: original_mimetype.essence_str().to_owned(),
                size: original_size,
                width: original_width,
                height: original_height,
                blurhash,
                is_animated: None,
            },
            thumbnail_info,
            original_mimetype: original_mimetype.essence_str().to_owned(),
        };

        serde_json::to_string(&uploaded).map_err(UploadedImageError::validation_err)
    }

    /// Send a previously uploaded image as a typed `m.image` event with a
    /// caller-provided transaction ID, returning the homeserver event ID.
    pub async fn send_uploaded_image_with_transaction_id_returning_event_id(
        &self,
        uploaded_image_json: String,
        transaction_id: String,
        caption: Option<String>,
        formatted_caption: Option<String>,
        reply_event_id: Option<String>,
    ) -> Result<String, UploadedImageError> {
        let uploaded: UploadedImage = serde_json::from_str(&uploaded_image_json)
            .map_err(UploadedImageError::validation_err)?;
        uploaded.validate()?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();
        if uploaded.is_encrypted != is_encrypted {
            return Err(UploadedImageError::validation(
                format!(
                    "Uploaded image encryption mismatch: uploaded is_encrypted={}, current room is_encrypted={}",
                    uploaded.is_encrypted, is_encrypted
                ),
                None,
            ));
        }

        let content_without_relation = uploaded.into_message_content(caption, formatted_caption)?;

        let content = if let Some(reply_event_id) = reply_event_id {
            let event_id =
                EventId::parse(reply_event_id).map_err(UploadedImageError::validation_err)?;
            let reply = Reply {
                event_id,
                enforce_thread: EnforceThread::MaybeThreaded,
                add_mentions: AddMentions::Yes,
            };

            self.inner
                .make_reply_event(content_without_relation, reply)
                .await
                .map_err(map_reply_error)?
        } else {
            RoomMessageEventContent::from(content_without_relation)
        };

        let txn_id: OwnedTransactionId = transaction_id.into();
        let result = self.inner.send(content).with_transaction_id(txn_id).await?;

        Ok(result.response.event_id.to_string())
    }

    /// Upload a voice message for a later typed `m.audio` voice event.
    ///
    /// The returned JSON string is opaque to the caller and should be persisted
    /// as-is until it is passed to
    /// [`send_uploaded_voice_with_transaction_id_returning_event_id`].
    pub async fn upload_voice_for_event(
        &self,
        file_path: String,
        mimetype: String,
        size: u64,
        duration: Duration,
        waveform: Vec<f32>,
    ) -> Result<String, UploadedVoiceError> {
        let mimetype = parse_audio_mimetype(&mimetype, "mimetype")?;
        let size = validate_positive_uint(size, "size")?;
        validate_duration(duration, "duration")?;
        validate_waveform(&waveform, "waveform")?;
        let file = read_local_media_file(&file_path, size, "file_path")?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();

        let media_source =
            upload_media_source(&self.inner, is_encrypted, &mimetype, file.data).await?;

        let uploaded = UploadedVoice {
            schema_version: UPLOADED_VOICE_SCHEMA_VERSION,
            kind: UPLOADED_VOICE_KIND.to_owned(),
            is_encrypted,
            filename: file.filename,
            media_source,
            voice_info: UploadedVoiceInfo {
                mimetype: mimetype.essence_str().to_owned(),
                size,
                duration,
                waveform,
            },
            original_mimetype: mimetype.essence_str().to_owned(),
        };

        serde_json::to_string(&uploaded).map_err(UploadedVoiceError::validation_err)
    }

    /// Send a previously uploaded voice message as a typed `m.audio` voice
    /// event with a caller-provided transaction ID, returning the homeserver
    /// event ID.
    pub async fn send_uploaded_voice_with_transaction_id_returning_event_id(
        &self,
        uploaded_voice_json: String,
        transaction_id: String,
        reply_event_id: Option<String>,
    ) -> Result<String, UploadedVoiceError> {
        let uploaded: UploadedVoice = serde_json::from_str(&uploaded_voice_json)
            .map_err(UploadedVoiceError::validation_err)?;
        uploaded.validate()?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();
        if uploaded.is_encrypted != is_encrypted {
            return Err(UploadedVoiceError::validation(
                format!(
                    "Uploaded voice encryption mismatch: uploaded is_encrypted={}, current room is_encrypted={}",
                    uploaded.is_encrypted, is_encrypted
                ),
                None,
            ));
        }

        let content_without_relation = uploaded.into_message_content()?;

        let content = if let Some(reply_event_id) = reply_event_id {
            let event_id =
                EventId::parse(reply_event_id).map_err(UploadedVoiceError::validation_err)?;
            let reply = Reply {
                event_id,
                enforce_thread: EnforceThread::MaybeThreaded,
                add_mentions: AddMentions::Yes,
            };

            self.inner
                .make_reply_event(content_without_relation, reply)
                .await
                .map_err(map_reply_error_for_voice)?
        } else {
            RoomMessageEventContent::from(content_without_relation)
        };

        let txn_id: OwnedTransactionId = transaction_id.into();
        let result = self.inner.send(content).with_transaction_id(txn_id).await?;

        Ok(result.response.event_id.to_string())
    }

    /// Upload a video and optional thumbnail for a later typed `m.video` event.
    ///
    /// The returned JSON string is opaque to the caller and should be persisted
    /// as-is until it is passed to
    /// [`send_uploaded_video_with_transaction_id_returning_event_id`].
    ///
    /// If provided, `progress_watcher` reports progress for the original video
    /// upload. Thumbnail upload progress is intentionally omitted.
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_video_for_event(
        &self,
        original_file_path: String,
        thumbnail_file_path: Option<String>,
        original_mimetype: String,
        original_size: u64,
        original_duration: Duration,
        original_width: u64,
        original_height: u64,
        thumbnail_mimetype: Option<String>,
        thumbnail_size: Option<u64>,
        thumbnail_width: Option<u64>,
        thumbnail_height: Option<u64>,
        blurhash: Option<String>,
        progress_watcher: Option<Box<dyn ProgressWatcher>>,
    ) -> Result<String, UploadedVideoError> {
        let original_mimetype = parse_video_mimetype(&original_mimetype, "original_mimetype")?;
        let original_size = validate_positive_uint(original_size, "original_size")?;
        validate_video_duration(original_duration, "original_duration")?;
        let original_width = validate_positive_uint(original_width, "original_width")?;
        let original_height = validate_positive_uint(original_height, "original_height")?;
        let original =
            validate_local_media_file(&original_file_path, original_size, "original_file_path")?;

        let thumbnail = read_thumbnail_input(
            thumbnail_file_path,
            thumbnail_mimetype,
            thumbnail_size,
            thumbnail_width,
            thumbnail_height,
        )?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();

        let (thumbnail_source, thumbnail_info) = if let Some(thumbnail) = thumbnail {
            let thumbnail_source =
                upload_media_source(&self.inner, is_encrypted, &thumbnail.mimetype, thumbnail.data)
                    .await?;

            (
                Some(thumbnail_source),
                Some(UploadedThumbnailInfo {
                    mimetype: thumbnail.mimetype.essence_str().to_owned(),
                    size: thumbnail.size,
                    width: thumbnail.width,
                    height: thumbnail.height,
                }),
            )
        } else {
            (None, None)
        };

        let media_source = upload_media_source_from_path(
            &self.inner,
            is_encrypted,
            &original_mimetype,
            &original.path,
            progress_watcher,
        )
        .await?;

        let uploaded = UploadedVideo {
            schema_version: UPLOADED_VIDEO_SCHEMA_VERSION,
            kind: UPLOADED_VIDEO_KIND.to_owned(),
            is_encrypted,
            filename: original.filename,
            media_source,
            thumbnail_source,
            video_info: UploadedVideoInfo {
                mimetype: original_mimetype.essence_str().to_owned(),
                size: original_size,
                duration: original_duration,
                width: original_width,
                height: original_height,
                blurhash,
            },
            thumbnail_info,
            original_mimetype: original_mimetype.essence_str().to_owned(),
        };

        serde_json::to_string(&uploaded).map_err(UploadedVideoError::validation_err)
    }

    /// Send a previously uploaded video as a typed `m.video` event with a
    /// caller-provided transaction ID, returning the homeserver event ID.
    pub async fn send_uploaded_video_with_transaction_id_returning_event_id(
        &self,
        uploaded_video_json: String,
        transaction_id: String,
        caption: Option<String>,
        formatted_caption: Option<String>,
        reply_event_id: Option<String>,
    ) -> Result<String, UploadedVideoError> {
        let uploaded: UploadedVideo = serde_json::from_str(&uploaded_video_json)
            .map_err(UploadedVideoError::validation_err)?;
        uploaded.validate()?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();
        if uploaded.is_encrypted != is_encrypted {
            return Err(UploadedVideoError::validation(
                format!(
                    "Uploaded video encryption mismatch: uploaded is_encrypted={}, current room is_encrypted={}",
                    uploaded.is_encrypted, is_encrypted
                ),
                None,
            ));
        }

        let content_without_relation = uploaded.into_message_content(caption, formatted_caption)?;

        let content = if let Some(reply_event_id) = reply_event_id {
            let event_id =
                EventId::parse(reply_event_id).map_err(UploadedVideoError::validation_err)?;
            let reply = Reply {
                event_id,
                enforce_thread: EnforceThread::MaybeThreaded,
                add_mentions: AddMentions::Yes,
            };

            self.inner
                .make_reply_event(content_without_relation, reply)
                .await
                .map_err(map_reply_error_for_video)?
        } else {
            RoomMessageEventContent::from(content_without_relation)
        };

        let txn_id: OwnedTransactionId = transaction_id.into();
        let result = self.inner.send(content).with_transaction_id(txn_id).await?;

        Ok(result.response.event_id.to_string())
    }

    /// Upload a file and optional thumbnail for a later typed `m.file` event.
    ///
    /// The returned JSON string is opaque to the caller and should be persisted
    /// as-is until it is passed to
    /// [`send_uploaded_file_with_transaction_id_returning_event_id`].
    ///
    /// If provided, `progress_watcher` reports progress for the original file
    /// upload. Thumbnail upload progress is intentionally omitted.
    #[allow(clippy::too_many_arguments)]
    pub async fn upload_file_for_event(
        &self,
        file_path: String,
        thumbnail_file_path: Option<String>,
        mimetype: String,
        size: u64,
        thumbnail_mimetype: Option<String>,
        thumbnail_size: Option<u64>,
        thumbnail_width: Option<u64>,
        thumbnail_height: Option<u64>,
        progress_watcher: Option<Box<dyn ProgressWatcher>>,
    ) -> Result<String, UploadedFileError> {
        let mimetype = parse_file_mimetype(&mimetype, "mimetype")?;
        let size = validate_positive_uint(size, "size")?;
        let file = validate_local_media_file(&file_path, size, "file_path")?;

        let thumbnail = read_thumbnail_input(
            thumbnail_file_path,
            thumbnail_mimetype,
            thumbnail_size,
            thumbnail_width,
            thumbnail_height,
        )?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();

        let (thumbnail_source, thumbnail_info) = if let Some(thumbnail) = thumbnail {
            let thumbnail_source =
                upload_media_source(&self.inner, is_encrypted, &thumbnail.mimetype, thumbnail.data)
                    .await?;

            (
                Some(thumbnail_source),
                Some(UploadedThumbnailInfo {
                    mimetype: thumbnail.mimetype.essence_str().to_owned(),
                    size: thumbnail.size,
                    width: thumbnail.width,
                    height: thumbnail.height,
                }),
            )
        } else {
            (None, None)
        };

        let media_source = upload_media_source_from_path(
            &self.inner,
            is_encrypted,
            &mimetype,
            &file.path,
            progress_watcher,
        )
        .await?;

        let uploaded = UploadedFile {
            schema_version: UPLOADED_FILE_SCHEMA_VERSION,
            kind: UPLOADED_FILE_KIND.to_owned(),
            is_encrypted,
            filename: file.filename,
            media_source,
            thumbnail_source,
            file_info: UploadedFileInfo { mimetype: mimetype.essence_str().to_owned(), size },
            thumbnail_info,
            original_mimetype: mimetype.essence_str().to_owned(),
        };

        serde_json::to_string(&uploaded).map_err(UploadedFileError::validation_err)
    }

    /// Send a previously uploaded file as a typed `m.file` event with a
    /// caller-provided transaction ID, returning the homeserver event ID.
    pub async fn send_uploaded_file_with_transaction_id_returning_event_id(
        &self,
        uploaded_file_json: String,
        transaction_id: String,
        caption: Option<String>,
        formatted_caption: Option<String>,
        reply_event_id: Option<String>,
    ) -> Result<String, UploadedFileError> {
        let uploaded: UploadedFile =
            serde_json::from_str(&uploaded_file_json).map_err(UploadedFileError::validation_err)?;
        uploaded.validate()?;

        let is_encrypted = self.inner.latest_encryption_state().await?.is_encrypted();
        if uploaded.is_encrypted != is_encrypted {
            return Err(UploadedFileError::validation(
                format!(
                    "Uploaded file encryption mismatch: uploaded is_encrypted={}, current room is_encrypted={}",
                    uploaded.is_encrypted, is_encrypted
                ),
                None,
            ));
        }

        let content_without_relation = uploaded.into_message_content(caption, formatted_caption)?;

        let content = if let Some(reply_event_id) = reply_event_id {
            let event_id =
                EventId::parse(reply_event_id).map_err(UploadedFileError::validation_err)?;
            let reply = Reply {
                event_id,
                enforce_thread: EnforceThread::MaybeThreaded,
                add_mentions: AddMentions::Yes,
            };

            self.inner
                .make_reply_event(content_without_relation, reply)
                .await
                .map_err(map_reply_error_for_file)?
        } else {
            RoomMessageEventContent::from(content_without_relation)
        };

        let txn_id: OwnedTransactionId = transaction_id.into();
        let result = self.inner.send(content).with_transaction_id(txn_id).await?;

        Ok(result.response.event_id.to_string())
    }

    /// Send a raw state event to the room.
    ///
    /// # Arguments
    ///
    /// * `event_type` - The type of the state event to send (e.g.
    ///   `"m.room.name"` or a custom type).
    ///
    /// * `state_key` - A unique key which defines the overwriting semantics for
    ///   this piece of room state. This is often an empty string.
    ///
    /// * `content` - The content of the state event encoded as a JSON string.
    ///
    /// Returns the event ID of the newly created state event.
    pub async fn send_state_event_raw(
        &self,
        event_type: String,
        state_key: String,
        content: String,
    ) -> Result<String, ClientError> {
        let content_json: serde_json::Value =
            serde_json::from_str(&content).map_err(|e| ClientError::Generic {
                msg: format!("Failed to parse JSON: {e}"),
                details: Some(format!("{e:?}")),
            })?;

        let response =
            self.inner.send_state_event_raw(&event_type, &state_key, content_json).await?;

        Ok(response.event_id.to_string())
    }

    /// Redacts an event from the room.
    ///
    /// # Arguments
    ///
    /// * `event_id` - The ID of the event to redact
    ///
    /// * `reason` - The reason for the event being redacted (optional).
    pub async fn redact(
        &self,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), ClientError> {
        let event_id = EventId::parse(event_id)?;
        self.inner.redact(&event_id, reason.as_deref(), None).await?;
        Ok(())
    }

    /// Redacts an event from the room with a caller-provided transaction ID,
    /// returning the homeserver event ID of the redaction event.
    ///
    /// # Arguments
    ///
    /// * `event_id` - The ID of the event to redact.
    ///
    /// * `reason` - The reason for the event being redacted (optional).
    ///
    /// * `transaction_id` - The transaction ID to use for the redaction event.
    pub async fn redact_with_transaction_id_returning_event_id(
        &self,
        event_id: String,
        reason: Option<String>,
        transaction_id: String,
    ) -> Result<String, ClientError> {
        let event_id = EventId::parse(event_id)?;
        let txn_id: OwnedTransactionId = transaction_id.into();

        let response = self.inner.redact(&event_id, reason.as_deref(), Some(txn_id)).await?;

        Ok(response.event_id.to_string())
    }

    pub fn active_members_count(&self) -> u64 {
        self.inner.active_members_count()
    }

    pub fn invited_members_count(&self) -> u64 {
        self.inner.invited_members_count()
    }

    pub fn joined_members_count(&self) -> u64 {
        self.inner.joined_members_count()
    }

    /// Reports an event from the room.
    ///
    /// # Arguments
    ///
    /// * `event_id` - The ID of the event to report
    ///
    /// * `reason` - The reason for the event being reported (optional).
    ///
    /// * `score` - The score to rate this content as where -100 is most
    ///   offensive and 0 is inoffensive (optional).
    pub async fn report_content(
        &self,
        event_id: String,
        reason: Option<String>,
    ) -> Result<(), ClientError> {
        self.inner.report_content(EventId::parse(event_id)?, reason).await?;

        Ok(())
    }

    /// Reports a room as inappropriate to the server.
    /// The caller is not required to be joined to the room to report it.
    ///
    /// # Arguments
    ///
    /// * `reason` - The reason the room is being reported.
    ///
    /// # Errors
    ///
    /// Returns an error if the room is not found or on rate limit
    pub async fn report_room(&self, reason: String) -> Result<(), ClientError> {
        self.inner.report_room(reason).await?;

        Ok(())
    }

    /// Ignores a user.
    ///
    /// # Arguments
    ///
    /// * `user_id` - The ID of the user to ignore.
    pub async fn ignore_user(&self, user_id: String) -> Result<(), ClientError> {
        let user_id = UserId::parse(user_id)?;
        self.inner.client().account().ignore_user(&user_id).await?;
        Ok(())
    }

    /// Leave this room.
    ///
    /// Only invited and joined rooms can be left.
    pub async fn leave(&self) -> Result<(), ClientError> {
        self.inner.leave().await?;
        Ok(())
    }

    /// Join this room.
    ///
    /// Only invited and left rooms can be joined via this method.
    pub async fn join(&self) -> Result<(), ClientError> {
        self.inner.join().await?;
        Ok(())
    }

    /// Sets a new name to the room.
    pub async fn set_name(&self, name: String) -> Result<(), ClientError> {
        self.inner.set_name(name).await?;
        Ok(())
    }

    /// Sets a new topic in the room.
    pub async fn set_topic(&self, topic: String) -> Result<(), ClientError> {
        self.inner.set_room_topic(&topic).await?;
        Ok(())
    }

    /// Upload and set the room's avatar.
    ///
    /// This will upload the data produced by the reader to the homeserver's
    /// content repository, and set the room's avatar to the MXC URI for the
    /// uploaded file.
    ///
    /// # Arguments
    ///
    /// * `mime_type` - The mime description of the avatar, for example
    ///   image/jpeg
    /// * `data` - The raw data that will be uploaded to the homeserver's
    ///   content repository
    /// * `media_info` - The media info used as avatar image info.
    pub async fn upload_avatar(
        &self,
        mime_type: String,
        data: Vec<u8>,
        media_info: Option<ImageInfo>,
    ) -> Result<(), ClientError> {
        let mime: Mime = mime_type.parse()?;
        self.inner
            .upload_avatar(
                &mime,
                data,
                media_info
                    .map(TryInto::try_into)
                    .transpose()
                    .map_err(|_| RoomError::InvalidMediaInfo)?,
            )
            .await?;
        Ok(())
    }

    /// Removes the current room avatar
    pub async fn remove_avatar(&self) -> Result<(), ClientError> {
        self.inner.remove_avatar().await?;
        Ok(())
    }

    pub async fn invite_user_by_id(&self, user_id: String) -> Result<(), ClientError> {
        let user =
            <&UserId>::try_from(user_id.as_str()).context("Could not create user from string")?;
        self.inner.invite_user_by_id(user).await?;
        Ok(())
    }

    pub async fn ban_user(
        &self,
        user_id: String,
        reason: Option<String>,
    ) -> Result<(), ClientError> {
        let user_id = UserId::parse(&user_id)?;
        Ok(self.inner.ban_user(&user_id, reason.as_deref()).await?)
    }

    pub async fn unban_user(
        &self,
        user_id: String,
        reason: Option<String>,
    ) -> Result<(), ClientError> {
        let user_id = UserId::parse(&user_id)?;
        Ok(self.inner.unban_user(&user_id, reason.as_deref()).await?)
    }

    pub async fn kick_user(
        &self,
        user_id: String,
        reason: Option<String>,
    ) -> Result<(), ClientError> {
        let user_id = UserId::parse(&user_id)?;
        Ok(self.inner.kick_user(&user_id, reason.as_deref()).await?)
    }

    pub fn own_user_id(&self) -> String {
        self.inner.own_user_id().to_string()
    }

    pub async fn typing_notice(&self, is_typing: bool) -> Result<(), ClientError> {
        Ok(self.inner.typing_notice(is_typing).await?)
    }

    pub fn subscribe_to_typing_notifications(
        self: Arc<Self>,
        listener: Box<dyn TypingNotificationsListener>,
    ) -> Arc<TaskHandle> {
        Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            let (_event_handler_drop_guard, mut subscriber) =
                self.inner.subscribe_to_typing_notifications();
            while let Ok(typing_user_ids) = subscriber.recv().await {
                let typing_user_ids =
                    typing_user_ids.into_iter().map(|user_id| user_id.to_string()).collect();
                listener.call(typing_user_ids);
            }
        })))
    }

    pub async fn subscribe_to_identity_status_changes(
        &self,
        listener: Box<dyn IdentityStatusChangeListener>,
    ) -> Result<Arc<TaskHandle>, ClientError> {
        let room = self.inner.clone();

        let status_changes = room.subscribe_to_identity_status_changes().await?;

        Ok(Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            let mut status_changes = pin!(status_changes);
            while let Some(identity_status_changes) = status_changes.next().await {
                listener.call(
                    identity_status_changes
                        .into_iter()
                        .map(|change| {
                            let user_id = change.user_id.to_string();
                            IdentityStatusChange { user_id, changed_to: change.changed_to }
                        })
                        .collect(),
                );
            }
        }))))
    }

    /// Set (or unset) a flag on the room to indicate that the user has
    /// explicitly marked it as unread.
    pub async fn set_unread_flag(&self, new_value: bool) -> Result<(), ClientError> {
        Ok(self.inner.set_unread_flag(new_value).await?)
    }

    /// Mark a room as read, by attaching a read receipt on the latest event.
    ///
    /// Note: this does NOT unset the unread flag; it's the caller's
    /// responsibility to do so, if need be.
    pub async fn mark_as_read(&self, receipt_type: ReceiptType) -> Result<(), ClientError> {
        let timeline = TimelineBuilder::new(&self.inner).build().await?;

        timeline.mark_as_read(receipt_type.into()).await?;
        Ok(())
    }

    /// Mark a room as fully read, by attaching a read receipt to the provided
    /// `event_id`.
    ///
    /// **Warning:** using this method is **NOT** recommended, as providing the
    /// latest event id can cause incorrect read receipts. This method won't
    /// check if sending the read receipt is necessary or valid. It should
    /// *only* be used when some constraint prevents you from instantiating a
    /// [`Timeline`]. For any other case use [`Timeline::mark_as_read`]
    /// instead.
    pub async fn mark_as_fully_read_unchecked(&self, event_id: String) -> Result<(), ClientError> {
        let event_id = EventId::parse(event_id)?;

        self.inner
            .send_single_receipt(ReceiptType::FullyRead.into(), ReceiptThread::Unthreaded, event_id)
            .await?;

        Ok(())
    }

    pub async fn get_power_levels(&self) -> Result<Arc<RoomPowerLevels>, ClientError> {
        let power_levels = self.inner.power_levels().await.map_err(matrix_sdk::Error::from)?;
        Ok(Arc::new(RoomPowerLevels::new(power_levels, self.inner.own_user_id().to_owned())))
    }

    pub async fn apply_power_level_changes(
        &self,
        changes: RoomPowerLevelChanges,
    ) -> Result<(), ClientError> {
        self.inner.apply_power_level_changes(changes).await?;
        Ok(())
    }

    pub async fn update_power_levels_for_users(
        &self,
        updates: Vec<UserPowerLevelUpdate>,
    ) -> Result<(), ClientError> {
        let updates = updates
            .iter()
            .map(|update| {
                let user_id: &UserId = update.user_id.as_str().try_into()?;
                let power_level = Int::new(update.power_level).context("Invalid power level")?;
                Ok((user_id, power_level))
            })
            .collect::<Result<Vec<_>>>()?;

        self.inner.update_power_levels(updates).await.map_err(ClientError::from_err)?;
        Ok(())
    }

    pub async fn suggested_role_for_user(
        &self,
        user_id: String,
    ) -> Result<RoomMemberRole, ClientError> {
        let user_id = UserId::parse(&user_id)?;
        Ok(self.inner.get_suggested_user_role(&user_id).await?)
    }

    pub async fn reset_power_levels(&self) -> Result<Arc<RoomPowerLevels>, ClientError> {
        Ok(Arc::new(RoomPowerLevels::new(
            self.inner.reset_power_levels().await?,
            self.inner.own_user_id().to_owned(),
        )))
    }

    pub async fn matrix_to_permalink(&self) -> Result<String, ClientError> {
        Ok(self.inner.matrix_to_permalink().await?.to_string())
    }

    pub async fn matrix_to_event_permalink(&self, event_id: String) -> Result<String, ClientError> {
        let event_id = EventId::parse(event_id)?;
        Ok(self.inner.matrix_to_event_permalink(event_id).await?.to_string())
    }

    /// Returns whether the send queue for that particular room is enabled or
    /// not.
    pub fn is_send_queue_enabled(&self) -> bool {
        self.inner.send_queue().is_enabled()
    }

    /// Enable or disable the send queue for that particular room.
    pub fn enable_send_queue(&self, enable: bool) {
        self.inner.send_queue().set_enabled(enable);
    }

    /// Subscribe to all send queue updates in this room.
    ///
    /// The given listener will be immediately called with
    /// `RoomSendQueueUpdate::NewLocalEvent` for each local echo existing in
    /// the queue.
    pub async fn subscribe_to_send_queue_updates(
        &self,
        listener: Box<dyn SendQueueListener>,
    ) -> Result<Arc<TaskHandle>, ClientError> {
        let q = self.inner.send_queue();
        let (local_echoes, mut subscriber) = q.subscribe().await?;

        for local_echo in local_echoes {
            listener.on_update(RoomSendQueueUpdate::NewLocalEvent {
                transaction_id: local_echo.transaction_id.into(),
            });
        }

        Ok(Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            loop {
                match subscriber.recv().await {
                    Ok(update) => match update.try_into() {
                        Ok(update) => listener.on_update(update),
                        Err(err) => error!("error when converting send queue update: {err}"),
                    },
                    Err(err) => error!("error when listening for send queue updates: {err}"),
                }
            }
        }))))
    }

    /// Store the given `ComposerDraft` in the state store using the current
    /// room id, as identifier.
    pub async fn save_composer_draft(
        &self,
        draft: ComposerDraft,
        thread_root: Option<String>,
    ) -> Result<(), ClientError> {
        let thread_root = thread_root.map(EventId::parse).transpose()?;
        Ok(self.inner.save_composer_draft(draft.try_into()?, thread_root.as_deref()).await?)
    }

    /// Retrieve the `ComposerDraft` stored in the state store for this room.
    pub async fn load_composer_draft(
        &self,
        thread_root: Option<String>,
    ) -> Result<Option<ComposerDraft>, ClientError> {
        let thread_root = thread_root.map(EventId::parse).transpose()?;
        Ok(self.inner.load_composer_draft(thread_root.as_deref()).await?.map(Into::into))
    }

    /// Remove the `ComposerDraft` stored in the state store for this room.
    pub async fn clear_composer_draft(
        &self,
        thread_root: Option<String>,
    ) -> Result<(), ClientError> {
        let thread_root = thread_root.map(EventId::parse).transpose()?;
        Ok(self.inner.clear_composer_draft(thread_root.as_deref()).await?)
    }

    /// Edit an event given its event id.
    ///
    /// Useful outside the context of a timeline, or when a timeline doesn't
    /// have the full content of an event.
    pub async fn edit(
        &self,
        event_id: String,
        new_content: Arc<RoomMessageEventContentWithoutRelation>,
    ) -> Result<(), ClientError> {
        let event_id = EventId::parse(event_id)?;

        let replacement_event = self
            .inner
            .make_edit_event(&event_id, EditedContent::RoomMessage((*new_content).clone()))
            .await?;

        self.inner.send_queue().send(replacement_event).await?;
        Ok(())
    }

    /// Remove verification requirements for the given users and
    /// resend messages that failed to send because their identities were no
    /// longer verified (in response to
    /// `SessionRecipientCollectionError::VerifiedUserChangedIdentity`)
    ///
    /// # Arguments
    ///
    /// * `user_ids` - The list of users identifiers received in the error
    /// * `transaction_id` - The send queue transaction identifier of the local
    ///   echo the send error applies to
    pub async fn withdraw_verification_and_resend(
        &self,
        user_ids: Vec<String>,
        send_handle: Arc<SendHandle>,
    ) -> Result<(), ClientError> {
        let user_ids: Vec<OwnedUserId> =
            user_ids.iter().map(UserId::parse).collect::<Result<_, _>>()?;

        let encryption = self.inner.client().encryption();

        for user_id in user_ids {
            if let Some(user_identity) = encryption.get_user_identity(&user_id).await? {
                user_identity.withdraw_verification().await?;
            }
        }

        send_handle.try_resend().await?;

        Ok(())
    }

    /// Set the local trust for the given devices to `LocalTrust::Ignored`
    /// and resend messages that failed to send because said devices are
    /// unverified (in response to
    /// `SessionRecipientCollectionError::VerifiedUserHasUnsignedDevice`).
    /// # Arguments
    ///
    /// * `devices` - The map of users identifiers to device identifiers
    ///   received in the error
    /// * `transaction_id` - The send queue transaction identifier of the local
    ///   echo the send error applies to
    pub async fn ignore_device_trust_and_resend(
        &self,
        devices: HashMap<String, Vec<String>>,
        send_handle: Arc<SendHandle>,
    ) -> Result<(), ClientError> {
        let encryption = self.inner.client().encryption();

        for (user_id, device_ids) in devices.iter() {
            let user_id = UserId::parse(user_id)?;

            for device_id in device_ids {
                let device_id: OwnedDeviceId = device_id.as_str().into();

                if let Some(device) = encryption.get_device(&user_id, &device_id).await? {
                    device.set_local_trust(LocalTrust::Ignored).await?;
                }
            }
        }

        send_handle.try_resend().await?;

        Ok(())
    }

    /// Clear the event cache storage for the current room.
    ///
    /// This will remove all the information related to the event cache, in
    /// memory and in the persisted storage, if enabled.
    pub async fn clear_event_cache_storage(&self) -> Result<(), ClientError> {
        let (room_event_cache, _drop_handles) = self.inner.event_cache().await?;
        room_event_cache.clear().await?;
        Ok(())
    }

    /// Subscribes to requests to join this room (knock member events), using a
    /// `listener` to be notified of the changes.
    ///
    /// The current requests to join the room will be emitted immediately
    /// when subscribing, along with a [`TaskHandle`] to cancel the
    /// subscription.
    pub async fn subscribe_to_knock_requests(
        self: Arc<Self>,
        listener: Box<dyn KnockRequestsListener>,
    ) -> Result<Arc<TaskHandle>, ClientError> {
        let (stream, seen_ids_cleanup_handle) = self.inner.subscribe_to_knock_requests().await?;

        let handle = Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            pin_mut!(stream);
            while let Some(requests) = stream.next().await {
                listener.call(requests.into_iter().map(Into::into).collect());
            }
            // Cancel the seen ids cleanup task
            seen_ids_cleanup_handle.abort();
        })));

        Ok(handle)
    }

    /// Return a debug representation for the internal room events data
    /// structure, one line per entry in the resulting vector.
    pub async fn room_events_debug_string(&self) -> Result<Vec<String>, ClientError> {
        let (cache, _drop_guards) = self.inner.event_cache().await?;
        Ok(cache.debug_string().await)
    }

    /// Update the canonical alias of the room.
    ///
    /// Note that publishing the alias in the room directory is done separately.
    pub async fn update_canonical_alias(
        &self,
        alias: Option<String>,
        alt_aliases: Vec<String>,
    ) -> Result<(), ClientError> {
        let new_alias = alias.map(TryInto::try_into).transpose()?;
        let new_alt_aliases =
            alt_aliases.into_iter().map(RoomAliasId::parse).collect::<Result<_, _>>()?;
        self.inner
            .privacy_settings()
            .update_canonical_alias(new_alias, new_alt_aliases)
            .await
            .map_err(Into::into)
    }

    /// Publish a new room alias for this room in the room directory.
    ///
    /// Returns:
    /// - `true` if the room alias didn't exist and it's now published.
    /// - `false` if the room alias was already present so it couldn't be
    ///   published.
    pub async fn publish_room_alias_in_room_directory(
        &self,
        alias: String,
    ) -> Result<bool, ClientError> {
        let new_alias = RoomAliasId::parse(alias)?;
        self.inner
            .privacy_settings()
            .publish_room_alias_in_room_directory(&new_alias)
            .await
            .map_err(Into::into)
    }

    /// Remove an existing room alias for this room in the room directory.
    ///
    /// Returns:
    /// - `true` if the room alias was present and it's now removed from the
    ///   room directory.
    /// - `false` if the room alias didn't exist so it couldn't be removed.
    pub async fn remove_room_alias_from_room_directory(
        &self,
        alias: String,
    ) -> Result<bool, ClientError> {
        let alias = RoomAliasId::parse(alias)?;
        self.inner
            .privacy_settings()
            .remove_room_alias_from_room_directory(&alias)
            .await
            .map_err(Into::into)
    }

    /// Enable End-to-end encryption in this room.
    pub async fn enable_encryption(&self) -> Result<(), ClientError> {
        self.inner.enable_encryption().await.map_err(Into::into)
    }

    /// Update room history visibility for this room.
    pub async fn update_history_visibility(
        &self,
        visibility: RoomHistoryVisibility,
    ) -> Result<(), ClientError> {
        let visibility: RumaHistoryVisibility = visibility.try_into()?;
        self.inner
            .privacy_settings()
            .update_room_history_visibility(visibility)
            .await
            .map_err(Into::into)
    }

    /// Update the join rule for this room.
    pub async fn update_join_rules(&self, new_rule: JoinRule) -> Result<(), ClientError> {
        let new_rule: RumaJoinRule = new_rule.try_into()?;
        self.inner.privacy_settings().update_join_rule(new_rule).await.map_err(Into::into)
    }

    /// Update the room's visibility in the room directory.
    pub async fn update_room_visibility(
        &self,
        visibility: RoomVisibility,
    ) -> Result<(), ClientError> {
        self.inner
            .privacy_settings()
            .update_room_visibility(visibility.into())
            .await
            .map_err(Into::into)
    }

    /// Returns the visibility for this room in the room directory.
    ///
    /// [Public](`RoomVisibility::Public`) rooms are listed in the room
    /// directory and can be found using it.
    pub async fn get_room_visibility(&self) -> Result<RoomVisibility, ClientError> {
        let visibility = self.inner.privacy_settings().get_room_visibility().await?;
        Ok(visibility.into())
    }

    /// Start the current users live location share in the room.
    pub async fn start_live_location_share(
        &self,
        duration_millis: u64,
    ) -> Result<String, ClientError> {
        let response = self.inner.start_live_location_share(duration_millis, None).await?;
        Ok(response.event_id.into())
    }

    /// Stop the current users live location share in the room.
    pub async fn stop_live_location_share(&self) -> Result<(), LiveLocationError> {
        self.inner.stop_live_location_share().await?;
        Ok(())
    }

    /// Send the current users live location beacon in the room.
    pub async fn send_live_location(&self, geo_uri: String) -> Result<(), LiveLocationError> {
        self.inner.send_location_beacon(geo_uri).await?;
        Ok(())
    }

    /// Declines a call (and stop ringing).
    ///
    /// # Arguments
    ///
    /// * `rtc_notification_event_id` - the event id of the m.rtc.notification
    ///   event.
    pub async fn decline_call(&self, rtc_notification_event_id: String) -> Result<(), ClientError> {
        let parsed_id = EventId::parse(rtc_notification_event_id.as_str())?;

        let content = self.inner.make_decline_call_event(&parsed_id).await?;

        self.inner.send_queue().send(content.into()).await?;

        Ok(())
    }

    /// Subscribes to call decline for a currently ringing call, using a
    /// `listener` to be notified when someone declines.
    ///
    /// Will error if `rtc_notification_event_id` is not a valid event id.
    /// Use the [`TaskHandle`] to cancel the subscription.
    pub fn subscribe_to_call_decline_events(
        self: Arc<Self>,
        rtc_notification_event_id: String,
        listener: Box<dyn CallDeclineListener>,
    ) -> Result<Arc<TaskHandle>, ClientError> {
        let parsed_id = EventId::parse(rtc_notification_event_id.as_str())?;

        Ok(Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            let (_event_handler_drop_guard, mut subscriber) =
                self.inner.subscribe_to_call_decline_events(&parsed_id);

            while let Ok(user_id) = subscriber.recv().await {
                listener.call(user_id.to_string());
            }
        }))))
    }

    /// Returns the active live location shares for this room.
    ///
    /// The returned [`LiveLocationsObserver`] object tracks which users are
    /// currently sharing their live location. It keeps the underlying event
    /// handlers registered — and therefore the share list up-to-date — for as
    /// long as it is alive. Call [`LiveLocationsObserver::subscribe`] on it to
    /// receive an initial snapshot and a stream of incremental updates.
    pub async fn live_locations_observer(&self) -> Arc<LiveLocationsObserver> {
        let inner = self.inner.live_locations_observer().await;
        Arc::new(LiveLocationsObserver::new(inner))
    }

    /// Forget this room.
    ///
    /// This communicates to the homeserver that it should forget the room.
    ///
    /// Only left or banned-from rooms can be forgotten.
    pub async fn forget(&self) -> Result<(), ClientError> {
        self.inner.forget().await?;
        Ok(())
    }

    /// Builds a `RoomPreview` from a room list item. This is intended for
    /// invited, knocked or banned rooms.
    async fn preview_room(&self, via: Vec<String>) -> Result<Arc<RoomPreview>, ClientError> {
        // Validate parameters first.
        let server_names: Vec<OwnedServerName> = via
            .into_iter()
            .map(|server| ServerName::parse(server).map_err(ClientError::from))
            .collect::<Result<_, ClientError>>()?;

        // Do the thing.
        let client = self.inner.client();
        let (room_or_alias_id, mut server_names) = if let Some(alias) = self.inner.canonical_alias()
        {
            let room_or_alias_id: OwnedRoomOrAliasId = alias.into();
            (room_or_alias_id, Vec::new())
        } else {
            let room_or_alias_id: OwnedRoomOrAliasId = self.inner.room_id().to_owned().into();
            (room_or_alias_id, server_names)
        };

        // If no server names are provided and the room's membership is invited,
        // add the server name from the sender's user id as a fallback value
        if server_names.is_empty()
            && let Ok(invite_details) = self.inner.invite_details().await
            && let Some(inviter) = invite_details.inviter
        {
            server_names.push(inviter.user_id().server_name().to_owned());
        }

        let room_preview = client.get_room_preview(&room_or_alias_id, server_names).await?;

        Ok(Arc::new(RoomPreview::new(AsyncRuntimeDropped::new(client), room_preview)))
    }

    /// Set a MSC4306 subscription to a thread in this room, based on the thread
    /// root event id.
    ///
    /// If `subscribed` is `true`, it will subscribe to the thread, with a
    /// precision that the subscription was manually requested by the user
    /// (i.e. not automatic).
    ///
    /// If the thread was already subscribed to (resp. unsubscribed from), while
    /// trying to subscribe to it (resp. unsubscribe from it), it will do
    /// nothing, i.e. subscribing (resp. unsubscribing) to a thread is an
    /// idempotent operation.
    pub async fn set_thread_subscription(
        &self,
        thread_root_event_id: String,
        subscribed: bool,
    ) -> Result<(), ClientError> {
        let thread_root = EventId::parse(thread_root_event_id)?;
        if subscribed {
            // This is a manual subscription.
            let automatic = None;
            self.inner.subscribe_thread(thread_root, automatic).await?;
        } else {
            self.inner.unsubscribe_thread(thread_root).await?;
        }
        Ok(())
    }

    /// Return the current MSC4306 thread subscription for the given thread root
    /// in this room.
    ///
    /// Returns `None` if the thread doesn't exist, or isn't subscribed to, or
    /// the server can't handle MSC4306; otherwise, returns the thread
    /// subscription status.
    pub async fn fetch_thread_subscription(
        &self,
        thread_root_event_id: String,
    ) -> Result<Option<ThreadSubscription>, ClientError> {
        let thread_root = EventId::parse(thread_root_event_id)?;
        Ok(self
            .inner
            .fetch_thread_subscription(thread_root)
            .await?
            .map(|sub| ThreadSubscription { automatic: sub.automatic }))
    }

    /// Creates a new [`ThreadListService`] for this room.
    ///
    /// The returned service provides a reactive, paginated list of thread roots
    /// for the room. Use [`ThreadListService::paginate`] to load pages and
    /// [`ThreadListService::subscribe_to_items_updates`] /
    /// [`ThreadListService::subscribe_to_pagination_state_updates`] to observe
    /// changes.
    pub fn thread_list_service(&self) -> Arc<ThreadListService> {
        // `no reactor running` panics
        let _guard = get_runtime_handle().enter();

        Arc::new(ThreadListService::new(&self.inner))
    }

    /// Either loads the event associated with the `event_id` from the event
    /// cache or fetches it from the homeserver.
    pub async fn load_or_fetch_event(
        &self,
        event_id: String,
    ) -> Result<TimelineEvent, ClientError> {
        let event_id = EventId::parse(event_id)?;
        let timeline_event = self.inner.load_or_fetch_event(&event_id, None).await?;
        Ok(timeline_event
            .kind
            .into_raw()
            .deserialize()?
            .into_full_event(self.inner.room_id().to_owned())
            .into())
    }
}

/// A listener for receiving call decline events in a room.
#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait CallDeclineListener: SyncOutsideWasm + SendOutsideWasm {
    fn call(&self, decliner_user_id: String);
}

impl From<matrix_sdk::room::knock_requests::KnockRequest> for KnockRequest {
    fn from(request: matrix_sdk::room::knock_requests::KnockRequest) -> Self {
        Self {
            event_id: request.event_id.to_string(),
            user_id: request.member_info.user_id.to_string(),
            room_id: request.room_id().to_string(),
            display_name: request.member_info.display_name.clone(),
            avatar_url: request.member_info.avatar_url.as_ref().map(|url| url.to_string()),
            reason: request.member_info.reason.clone(),
            timestamp: request.timestamp.map(|ts| ts.into()),
            is_seen: request.is_seen,
            actions: Arc::new(KnockRequestActions { inner: request }),
        }
    }
}

/// A listener for receiving new requests to a join a room.
#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait KnockRequestsListener: SendOutsideWasm + SyncOutsideWasm {
    fn call(&self, join_requests: Vec<KnockRequest>);
}

/// An FFI representation of a request to join a room.
#[derive(Debug, Clone, uniffi::Record)]
pub struct KnockRequest {
    /// The event id of the event that contains the `knock` membership change.
    pub event_id: String,
    /// The user id of the user who's requesting to join the room.
    pub user_id: String,
    /// The room id of the room whose access was requested.
    pub room_id: String,
    /// The optional display name of the user who's requesting to join the room.
    pub display_name: Option<String>,
    /// The optional avatar url of the user who's requesting to join the room.
    pub avatar_url: Option<String>,
    /// An optional reason why the user wants join the room.
    pub reason: Option<String>,
    /// The timestamp when this request was created.
    pub timestamp: Option<u64>,
    /// Whether the knock request has been marked as `seen` so it can be
    /// filtered by the client.
    pub is_seen: bool,
    /// A set of actions to perform for this knock request.
    pub actions: Arc<KnockRequestActions>,
}

/// A set of actions to perform for a knock request.
#[derive(Debug, Clone, uniffi::Object)]
pub struct KnockRequestActions {
    inner: matrix_sdk::room::knock_requests::KnockRequest,
}

#[matrix_sdk_ffi_macros::export]
impl KnockRequestActions {
    /// Accepts the knock request by inviting the user to the room.
    pub async fn accept(&self) -> Result<(), ClientError> {
        self.inner.accept().await.map_err(Into::into)
    }

    /// Declines the knock request by kicking the user from the room with an
    /// optional reason.
    pub async fn decline(&self, reason: Option<String>) -> Result<(), ClientError> {
        self.inner.decline(reason.as_deref()).await.map_err(Into::into)
    }

    /// Declines the knock request by banning the user from the room with an
    /// optional reason.
    pub async fn decline_and_ban(&self, reason: Option<String>) -> Result<(), ClientError> {
        self.inner.decline_and_ban(reason.as_deref()).await.map_err(Into::into)
    }

    /// Marks the knock request as 'seen'.
    ///
    /// **IMPORTANT**: this won't update the current reference to this request,
    /// a new one with the updated value should be emitted instead.
    pub async fn mark_as_seen(&self) -> Result<(), ClientError> {
        self.inner.mark_as_seen().await.map_err(Into::into)
    }
}

const UPLOADED_IMAGE_SCHEMA_VERSION: u16 = 1;
const UPLOADED_IMAGE_KIND: &str = "image";

#[derive(Debug, Serialize, Deserialize)]
struct UploadedImage {
    schema_version: u16,
    kind: String,
    is_encrypted: bool,
    filename: String,
    media_source: RumaMediaSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_source: Option<RumaMediaSource>,
    image_info: UploadedImageInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_info: Option<UploadedThumbnailInfo>,
    original_mimetype: String,
}

impl UploadedImage {
    fn validate(&self) -> Result<(), UploadedImageError> {
        if self.schema_version != UPLOADED_IMAGE_SCHEMA_VERSION {
            return Err(UploadedImageError::validation(
                format!("Unsupported UploadedImage schema_version: {}", self.schema_version),
                None,
            ));
        }

        if self.kind != UPLOADED_IMAGE_KIND {
            return Err(UploadedImageError::validation(
                format!("Unsupported UploadedImage kind: {}", self.kind),
                None,
            ));
        }

        if self.filename.is_empty() {
            return Err(UploadedImageError::validation("UploadedImage filename is empty", None));
        }

        validate_positive_uint(self.image_info.size, "image_info.size")?;
        validate_positive_uint(self.image_info.width, "image_info.width")?;
        validate_positive_uint(self.image_info.height, "image_info.height")?;
        parse_image_mimetype(&self.original_mimetype, "original_mimetype")?;
        parse_image_mimetype(&self.image_info.mimetype, "image_info.mimetype")?;

        if self.image_info.mimetype != self.original_mimetype {
            return Err(UploadedImageError::validation(
                "UploadedImage image_info.mimetype does not match original_mimetype",
                None,
            ));
        }

        validate_media_source_matches(
            &self.media_source,
            self.is_encrypted,
            "media_source",
            "UploadedImage",
        )?;

        match (&self.thumbnail_source, &self.thumbnail_info) {
            (Some(source), Some(info)) => {
                validate_media_source_matches(
                    source,
                    self.is_encrypted,
                    "thumbnail_source",
                    "UploadedImage",
                )?;
                validate_positive_uint(info.size, "thumbnail_info.size")?;
                validate_positive_uint(info.width, "thumbnail_info.width")?;
                validate_positive_uint(info.height, "thumbnail_info.height")?;
                parse_image_mimetype(&info.mimetype, "thumbnail_info.mimetype")?;
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(UploadedImageError::validation(
                    "UploadedImage thumbnail_source is present without thumbnail_info",
                    None,
                ));
            }
            (None, Some(_)) => {
                return Err(UploadedImageError::validation(
                    "UploadedImage thumbnail_info is present without thumbnail_source",
                    None,
                ));
            }
        }

        Ok(())
    }

    fn into_message_content(
        self,
        caption: Option<String>,
        formatted_caption: Option<String>,
    ) -> Result<RoomMessageEventContentWithoutRelation, UploadedImageError> {
        let source = ffi_media_source(self.media_source, "media_source")?;
        let thumbnail_source = self
            .thumbnail_source
            .map(|source| ffi_media_source(source, "thumbnail_source"))
            .transpose()?;

        let formatted_caption =
            formatted_caption.map(|body| FormattedBody { format: MessageFormat::Html, body });
        let caption = if formatted_caption.is_some() && caption.is_none() {
            Some(String::new())
        } else {
            caption
        };

        let image_content: RumaImageMessageEventContent = ImageMessageContent {
            filename: self.filename,
            caption,
            formatted_caption,
            source,
            info: Some(ImageInfo {
                height: Some(self.image_info.height),
                width: Some(self.image_info.width),
                mimetype: Some(self.original_mimetype),
                size: Some(self.image_info.size),
                thumbnail_info: self.thumbnail_info.map(|info| ThumbnailInfo {
                    height: Some(info.height),
                    width: Some(info.width),
                    mimetype: Some(info.mimetype),
                    size: Some(info.size),
                }),
                thumbnail_source,
                blurhash: self.image_info.blurhash,
                is_animated: self.image_info.is_animated,
            }),
        }
        .into();

        Ok(RoomMessageEventContentWithoutRelation::new(RumaMessageType::Image(image_content)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct UploadedImageInfo {
    mimetype: String,
    size: u64,
    width: u64,
    height: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    blurhash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_animated: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
struct UploadedThumbnailInfo {
    mimetype: String,
    size: u64,
    width: u64,
    height: u64,
}

const UPLOADED_VOICE_SCHEMA_VERSION: u16 = 1;
const UPLOADED_VOICE_KIND: &str = "voice";

#[derive(Debug, Serialize, Deserialize)]
struct UploadedVoice {
    schema_version: u16,
    kind: String,
    is_encrypted: bool,
    filename: String,
    media_source: RumaMediaSource,
    voice_info: UploadedVoiceInfo,
    original_mimetype: String,
}

impl UploadedVoice {
    fn validate(&self) -> Result<(), UploadedVoiceError> {
        if self.schema_version != UPLOADED_VOICE_SCHEMA_VERSION {
            return Err(UploadedVoiceError::validation(
                format!("Unsupported UploadedVoice schema_version: {}", self.schema_version),
                None,
            ));
        }

        if self.kind != UPLOADED_VOICE_KIND {
            return Err(UploadedVoiceError::validation(
                format!("Unsupported UploadedVoice kind: {}", self.kind),
                None,
            ));
        }

        if self.filename.is_empty() {
            return Err(UploadedVoiceError::validation("UploadedVoice filename is empty", None));
        }

        validate_positive_uint(self.voice_info.size, "voice_info.size")?;
        validate_duration(self.voice_info.duration, "voice_info.duration")?;
        validate_waveform(&self.voice_info.waveform, "voice_info.waveform")?;
        parse_audio_mimetype(&self.original_mimetype, "original_mimetype")?;
        parse_audio_mimetype(&self.voice_info.mimetype, "voice_info.mimetype")?;

        if self.voice_info.mimetype != self.original_mimetype {
            return Err(UploadedVoiceError::validation(
                "UploadedVoice voice_info.mimetype does not match original_mimetype",
                None,
            ));
        }

        validate_media_source_matches(
            &self.media_source,
            self.is_encrypted,
            "media_source",
            "UploadedVoice",
        )?;

        Ok(())
    }

    fn into_message_content(
        self,
    ) -> Result<RoomMessageEventContentWithoutRelation, UploadedVoiceError> {
        let source = ffi_media_source(self.media_source, "media_source")?;
        let duration = self.voice_info.duration;
        let audio_content: RumaAudioMessageEventContent = AudioMessageContent {
            filename: self.filename,
            caption: None,
            formatted_caption: None,
            source,
            info: Some(AudioInfo {
                duration: Some(duration),
                size: Some(self.voice_info.size),
                mimetype: Some(self.original_mimetype),
            }),
            audio: Some(UnstableAudioDetailsContent {
                duration,
                waveform: scale_voice_waveform(&self.voice_info.waveform),
            }),
            voice: Some(UnstableVoiceContent {}),
        }
        .into();

        Ok(RoomMessageEventContentWithoutRelation::new(RumaMessageType::Audio(audio_content)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct UploadedVoiceInfo {
    mimetype: String,
    size: u64,
    duration: Duration,
    waveform: Vec<f32>,
}

const UPLOADED_VIDEO_SCHEMA_VERSION: u16 = 1;
const UPLOADED_VIDEO_KIND: &str = "video";

#[derive(Debug, Serialize, Deserialize)]
struct UploadedVideo {
    schema_version: u16,
    kind: String,
    is_encrypted: bool,
    filename: String,
    media_source: RumaMediaSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_source: Option<RumaMediaSource>,
    video_info: UploadedVideoInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_info: Option<UploadedThumbnailInfo>,
    original_mimetype: String,
}

impl UploadedVideo {
    fn validate(&self) -> Result<(), UploadedVideoError> {
        if self.schema_version != UPLOADED_VIDEO_SCHEMA_VERSION {
            return Err(UploadedVideoError::validation(
                format!("Unsupported UploadedVideo schema_version: {}", self.schema_version),
                None,
            ));
        }

        if self.kind != UPLOADED_VIDEO_KIND {
            return Err(UploadedVideoError::validation(
                format!("Unsupported UploadedVideo kind: {}", self.kind),
                None,
            ));
        }

        if self.filename.is_empty() {
            return Err(UploadedVideoError::validation("UploadedVideo filename is empty", None));
        }

        validate_positive_uint(self.video_info.size, "video_info.size")?;
        validate_video_duration(self.video_info.duration, "video_info.duration")?;
        validate_positive_uint(self.video_info.width, "video_info.width")?;
        validate_positive_uint(self.video_info.height, "video_info.height")?;
        parse_video_mimetype(&self.original_mimetype, "original_mimetype")?;
        parse_video_mimetype(&self.video_info.mimetype, "video_info.mimetype")?;

        if self.video_info.mimetype != self.original_mimetype {
            return Err(UploadedVideoError::validation(
                "UploadedVideo video_info.mimetype does not match original_mimetype",
                None,
            ));
        }

        validate_media_source_matches(
            &self.media_source,
            self.is_encrypted,
            "media_source",
            "UploadedVideo",
        )?;

        match (&self.thumbnail_source, &self.thumbnail_info) {
            (Some(source), Some(info)) => {
                validate_media_source_matches(
                    source,
                    self.is_encrypted,
                    "thumbnail_source",
                    "UploadedVideo",
                )?;
                validate_positive_uint(info.size, "thumbnail_info.size")?;
                validate_positive_uint(info.width, "thumbnail_info.width")?;
                validate_positive_uint(info.height, "thumbnail_info.height")?;
                parse_image_mimetype(&info.mimetype, "thumbnail_info.mimetype")?;
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(UploadedVideoError::validation(
                    "UploadedVideo thumbnail_source is present without thumbnail_info",
                    None,
                ));
            }
            (None, Some(_)) => {
                return Err(UploadedVideoError::validation(
                    "UploadedVideo thumbnail_info is present without thumbnail_source",
                    None,
                ));
            }
        }

        Ok(())
    }

    fn into_message_content(
        self,
        caption: Option<String>,
        formatted_caption: Option<String>,
    ) -> Result<RoomMessageEventContentWithoutRelation, UploadedVideoError> {
        let source = ffi_media_source(self.media_source, "media_source")?;
        let thumbnail_source = self
            .thumbnail_source
            .map(|source| ffi_media_source(source, "thumbnail_source"))
            .transpose()?;

        let formatted_caption =
            formatted_caption.map(|body| FormattedBody { format: MessageFormat::Html, body });
        let caption = if formatted_caption.is_some() && caption.is_none() {
            Some(String::new())
        } else {
            caption
        };

        let video_content: RumaVideoMessageEventContent = VideoMessageContent {
            filename: self.filename,
            caption,
            formatted_caption,
            source,
            info: Some(VideoInfo {
                duration: Some(self.video_info.duration),
                height: Some(self.video_info.height),
                width: Some(self.video_info.width),
                mimetype: Some(self.original_mimetype),
                size: Some(self.video_info.size),
                thumbnail_info: self.thumbnail_info.map(|info| ThumbnailInfo {
                    height: Some(info.height),
                    width: Some(info.width),
                    mimetype: Some(info.mimetype),
                    size: Some(info.size),
                }),
                thumbnail_source,
                blurhash: self.video_info.blurhash,
            }),
        }
        .into();

        Ok(RoomMessageEventContentWithoutRelation::new(RumaMessageType::Video(video_content)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct UploadedVideoInfo {
    mimetype: String,
    size: u64,
    duration: Duration,
    width: u64,
    height: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    blurhash: Option<String>,
}

const UPLOADED_FILE_SCHEMA_VERSION: u16 = 1;
const UPLOADED_FILE_KIND: &str = "file";

#[derive(Debug, Serialize, Deserialize)]
struct UploadedFile {
    schema_version: u16,
    kind: String,
    is_encrypted: bool,
    filename: String,
    media_source: RumaMediaSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_source: Option<RumaMediaSource>,
    file_info: UploadedFileInfo,
    #[serde(skip_serializing_if = "Option::is_none")]
    thumbnail_info: Option<UploadedThumbnailInfo>,
    original_mimetype: String,
}

impl UploadedFile {
    fn validate(&self) -> Result<(), UploadedFileError> {
        if self.schema_version != UPLOADED_FILE_SCHEMA_VERSION {
            return Err(UploadedFileError::validation(
                format!("Unsupported UploadedFile schema_version: {}", self.schema_version),
                None,
            ));
        }

        if self.kind != UPLOADED_FILE_KIND {
            return Err(UploadedFileError::validation(
                format!("Unsupported UploadedFile kind: {}", self.kind),
                None,
            ));
        }

        if self.filename.is_empty() {
            return Err(UploadedFileError::validation("UploadedFile filename is empty", None));
        }

        validate_positive_uint(self.file_info.size, "file_info.size")?;
        parse_file_mimetype(&self.original_mimetype, "original_mimetype")?;
        parse_file_mimetype(&self.file_info.mimetype, "file_info.mimetype")?;

        if self.file_info.mimetype != self.original_mimetype {
            return Err(UploadedFileError::validation(
                "UploadedFile file_info.mimetype does not match original_mimetype",
                None,
            ));
        }

        validate_media_source_matches(
            &self.media_source,
            self.is_encrypted,
            "media_source",
            "UploadedFile",
        )?;

        match (&self.thumbnail_source, &self.thumbnail_info) {
            (Some(source), Some(info)) => {
                validate_media_source_matches(
                    source,
                    self.is_encrypted,
                    "thumbnail_source",
                    "UploadedFile",
                )?;
                validate_positive_uint(info.size, "thumbnail_info.size")?;
                validate_positive_uint(info.width, "thumbnail_info.width")?;
                validate_positive_uint(info.height, "thumbnail_info.height")?;
                parse_image_mimetype(&info.mimetype, "thumbnail_info.mimetype")?;
            }
            (None, None) => {}
            (Some(_), None) => {
                return Err(UploadedFileError::validation(
                    "UploadedFile thumbnail_source is present without thumbnail_info",
                    None,
                ));
            }
            (None, Some(_)) => {
                return Err(UploadedFileError::validation(
                    "UploadedFile thumbnail_info is present without thumbnail_source",
                    None,
                ));
            }
        }

        Ok(())
    }

    fn into_message_content(
        self,
        caption: Option<String>,
        formatted_caption: Option<String>,
    ) -> Result<RoomMessageEventContentWithoutRelation, UploadedFileError> {
        let source = ffi_media_source(self.media_source, "media_source")?;
        let thumbnail_source = self
            .thumbnail_source
            .map(|source| ffi_media_source(source, "thumbnail_source"))
            .transpose()?;

        let formatted_caption =
            formatted_caption.map(|body| FormattedBody { format: MessageFormat::Html, body });
        let caption = if formatted_caption.is_some() && caption.is_none() {
            Some(String::new())
        } else {
            caption
        };

        let file_content: RumaFileMessageEventContent = FileMessageContent {
            filename: self.filename,
            caption,
            formatted_caption,
            source,
            info: Some(FileInfo {
                mimetype: Some(self.original_mimetype),
                size: Some(self.file_info.size),
                thumbnail_info: self.thumbnail_info.map(|info| ThumbnailInfo {
                    height: Some(info.height),
                    width: Some(info.width),
                    mimetype: Some(info.mimetype),
                    size: Some(info.size),
                }),
                thumbnail_source,
            }),
        }
        .into();

        Ok(RoomMessageEventContentWithoutRelation::new(RumaMessageType::File(file_content)))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct UploadedFileInfo {
    mimetype: String,
    size: u64,
}

struct LocalMediaPath {
    path: PathBuf,
    filename: String,
}

struct LocalMediaFile {
    data: Vec<u8>,
    filename: String,
}

struct LocalThumbnail {
    data: Vec<u8>,
    mimetype: Mime,
    size: u64,
    width: u64,
    height: u64,
}

fn read_thumbnail_input(
    thumbnail_file_path: Option<String>,
    thumbnail_mimetype: Option<String>,
    thumbnail_size: Option<u64>,
    thumbnail_width: Option<u64>,
    thumbnail_height: Option<u64>,
) -> Result<Option<LocalThumbnail>, UploadedImageError> {
    let has_thumbnail_metadata = thumbnail_mimetype.is_some()
        || thumbnail_size.is_some()
        || thumbnail_width.is_some()
        || thumbnail_height.is_some();

    let Some(thumbnail_file_path) = thumbnail_file_path else {
        if has_thumbnail_metadata {
            return Err(UploadedImageError::validation(
                "thumbnail metadata was provided without thumbnail_file_path",
                None,
            ));
        }
        return Ok(None);
    };

    let thumbnail_mimetype = thumbnail_mimetype.ok_or_else(|| {
        UploadedImageError::validation(
            "thumbnail_mimetype is required with thumbnail_file_path",
            None,
        )
    })?;
    let thumbnail_size = thumbnail_size.ok_or_else(|| {
        UploadedImageError::validation("thumbnail_size is required with thumbnail_file_path", None)
    })?;
    let thumbnail_width = thumbnail_width.ok_or_else(|| {
        UploadedImageError::validation("thumbnail_width is required with thumbnail_file_path", None)
    })?;
    let thumbnail_height = thumbnail_height.ok_or_else(|| {
        UploadedImageError::validation(
            "thumbnail_height is required with thumbnail_file_path",
            None,
        )
    })?;

    let mimetype = parse_image_mimetype(&thumbnail_mimetype, "thumbnail_mimetype")?;
    let size = validate_positive_uint(thumbnail_size, "thumbnail_size")?;
    let width = validate_positive_uint(thumbnail_width, "thumbnail_width")?;
    let height = validate_positive_uint(thumbnail_height, "thumbnail_height")?;
    let file = read_local_media_file(&thumbnail_file_path, size, "thumbnail_file_path")?;

    Ok(Some(LocalThumbnail { data: file.data, mimetype, size, width, height }))
}

fn read_local_media_file(
    file_path: &str,
    expected_size: u64,
    field_name: &str,
) -> Result<LocalMediaFile, UploadedImageError> {
    let local_file = validate_local_media_file(file_path, expected_size, field_name)?;
    let data = fs::read(&local_file.path).map_err(|error| {
        UploadedImageError::validation(
            format!("Could not read {field_name}: {file_path}"),
            Some(format!("{error:?}")),
        )
    })?;

    if data.len() as u64 != expected_size {
        return Err(UploadedImageError::validation(
            format!("{field_name} size mismatch: expected {expected_size}, got {}", data.len()),
            None,
        ));
    }

    Ok(LocalMediaFile { data, filename: local_file.filename })
}

fn validate_local_media_file(
    file_path: &str,
    expected_size: u64,
    field_name: &str,
) -> Result<LocalMediaPath, UploadedImageError> {
    let path = PathBuf::from(file_path);
    let filename = path
        .file_name()
        .and_then(|file_name| file_name.to_str())
        .ok_or_else(|| {
            UploadedImageError::validation(
                format!("{field_name} must contain a valid UTF-8 file name"),
                None,
            )
        })?
        .to_owned();

    let metadata = fs::metadata(&path).map_err(|error| {
        UploadedImageError::validation(
            format!("Could not stat {field_name}: {file_path}"),
            Some(format!("{error:?}")),
        )
    })?;

    if !metadata.is_file() {
        return Err(UploadedImageError::validation(
            format!("{field_name} must point to a file: {file_path}"),
            None,
        ));
    }

    if metadata.len() != expected_size {
        return Err(UploadedImageError::validation(
            format!("{field_name} size mismatch: expected {expected_size}, got {}", metadata.len()),
            None,
        ));
    }

    Ok(LocalMediaPath { path, filename })
}

fn validate_positive_uint(value: u64, field_name: &str) -> Result<u64, UploadedImageError> {
    if value == 0 {
        return Err(UploadedImageError::validation(
            format!("{field_name} must be greater than 0"),
            None,
        ));
    }

    UInt::new(value).ok_or_else(|| {
        UploadedImageError::validation(format!("{field_name} is too large for Matrix UInt"), None)
    })?;

    Ok(value)
}

fn parse_image_mimetype(mimetype: &str, field_name: &str) -> Result<Mime, UploadedImageError> {
    let mimetype = mimetype.parse::<Mime>().map_err(|error| {
        UploadedImageError::validation(
            format!("Invalid {field_name}: {mimetype}"),
            Some(format!("{error:?}")),
        )
    })?;

    if mimetype.type_() != mime::IMAGE {
        return Err(UploadedImageError::validation(
            format!("{field_name} must be image/*, got {}", mimetype.essence_str()),
            None,
        ));
    }

    Ok(mimetype)
}

fn parse_audio_mimetype(mimetype: &str, field_name: &str) -> Result<Mime, UploadedVoiceError> {
    let mimetype = mimetype.parse::<Mime>().map_err(|error| {
        UploadedVoiceError::validation(
            format!("Invalid {field_name}: {mimetype}"),
            Some(format!("{error:?}")),
        )
    })?;

    if mimetype.type_() != mime::AUDIO {
        return Err(UploadedVoiceError::validation(
            format!("{field_name} must be audio/*, got {}", mimetype.essence_str()),
            None,
        ));
    }

    Ok(mimetype)
}

fn parse_video_mimetype(mimetype: &str, field_name: &str) -> Result<Mime, UploadedVideoError> {
    let mimetype = mimetype.parse::<Mime>().map_err(|error| {
        UploadedVideoError::validation(
            format!("Invalid {field_name}: {mimetype}"),
            Some(format!("{error:?}")),
        )
    })?;

    if mimetype.type_() != mime::VIDEO {
        return Err(UploadedVideoError::validation(
            format!("{field_name} must be video/*, got {}", mimetype.essence_str()),
            None,
        ));
    }

    Ok(mimetype)
}

fn parse_file_mimetype(mimetype: &str, field_name: &str) -> Result<Mime, UploadedFileError> {
    mimetype.parse::<Mime>().map_err(|error| {
        UploadedFileError::validation(
            format!("Invalid {field_name}: {mimetype}"),
            Some(format!("{error:?}")),
        )
    })
}

fn validate_duration(duration: Duration, field_name: &str) -> Result<(), UploadedVoiceError> {
    if duration.is_zero() {
        return Err(UploadedVoiceError::validation(
            format!("{field_name} must be greater than 0"),
            None,
        ));
    }

    Ok(())
}

fn validate_video_duration(duration: Duration, field_name: &str) -> Result<(), UploadedVideoError> {
    if duration.is_zero() {
        return Err(UploadedVideoError::validation(
            format!("{field_name} must be greater than 0"),
            None,
        ));
    }

    Ok(())
}

fn validate_waveform(waveform: &[f32], field_name: &str) -> Result<(), UploadedVoiceError> {
    if let Some(invalid_index) = waveform.iter().position(|value| !value.is_finite()) {
        return Err(UploadedVoiceError::validation(
            format!("{field_name} contains a non-finite value at index {invalid_index}"),
            None,
        ));
    }

    Ok(())
}

fn scale_voice_waveform(waveform: &[f32]) -> Vec<u16> {
    waveform
        .iter()
        .map(|value| ((*value).clamp(0.0, 1.0) * UnstableAmplitude::MAX as f32) as u16)
        .collect()
}

async fn upload_media_source(
    room: &SdkRoom,
    is_encrypted: bool,
    content_type: &Mime,
    data: Vec<u8>,
) -> Result<RumaMediaSource, UploadedImageError> {
    let client = room.client();

    if is_encrypted {
        let mut reader = Cursor::new(data);
        let encrypted_file = client.upload_encrypted_file(&mut reader).await?;
        Ok(RumaMediaSource::Encrypted(Box::new(encrypted_file)))
    } else {
        let response = client.media().upload(content_type, data, None).await?;
        Ok(RumaMediaSource::Plain(response.content_uri))
    }
}

async fn upload_media_source_from_path(
    room: &SdkRoom,
    is_encrypted: bool,
    content_type: &Mime,
    path: &Path,
    progress_watcher: Option<Box<dyn ProgressWatcher>>,
) -> Result<RumaMediaSource, UploadedImageError> {
    let client = room.client();

    if is_encrypted {
        let mut file = fs::File::open(path).map_err(|error| {
            UploadedImageError::validation(
                format!("Could not read media file: {}", path.display()),
                Some(format!("{error:?}")),
            )
        })?;
        let request = client.upload_encrypted_file(&mut file);

        if let Some(progress_watcher) = progress_watcher {
            let mut subscriber = request.subscribe_to_send_progress();
            get_runtime_handle().spawn(async move {
                while let Some(progress) = subscriber.next().await {
                    progress_watcher.transmission_progress(progress.into());
                }
            });
        }

        let encrypted_file = request.await?;
        Ok(RumaMediaSource::Encrypted(Box::new(encrypted_file)))
    } else {
        let data = fs::read(path).map_err(|error| {
            UploadedImageError::validation(
                format!("Could not read media file: {}", path.display()),
                Some(format!("{error:?}")),
            )
        })?;
        let request = client.media().upload(content_type, data, None);

        if let Some(progress_watcher) = progress_watcher {
            let mut subscriber = request.subscribe_to_send_progress();
            get_runtime_handle().spawn(async move {
                while let Some(progress) = subscriber.next().await {
                    progress_watcher.transmission_progress(progress.into());
                }
            });
        }

        let response = request.await?;
        Ok(RumaMediaSource::Plain(response.content_uri))
    }
}

fn validate_media_source_matches(
    source: &RumaMediaSource,
    is_encrypted: bool,
    field_name: &str,
    uploaded_type: &str,
) -> Result<(), UploadedImageError> {
    let source_is_encrypted = matches!(source, RumaMediaSource::Encrypted(_));
    if source_is_encrypted != is_encrypted {
        return Err(UploadedImageError::validation(
            format!(
                "{field_name} encryption mismatch: source is_encrypted={source_is_encrypted}, {uploaded_type} is_encrypted={is_encrypted}"
            ),
            None,
        ));
    }

    MediaSource::try_from(source.clone()).map_err(|error| {
        UploadedImageError::validation(format!("Invalid {field_name}"), Some(format!("{error:?}")))
    })?;

    Ok(())
}

fn ffi_media_source(
    source: RumaMediaSource,
    field_name: &str,
) -> Result<Arc<MediaSource>, UploadedImageError> {
    MediaSource::try_from(source).map(Arc::new).map_err(|error| {
        UploadedImageError::validation(format!("Invalid {field_name}"), Some(format!("{error:?}")))
    })
}

fn map_reply_error(error: ReplyError) -> UploadedImageError {
    let details = Some(format!("{error:?}"));
    match error {
        ReplyError::Fetch(_) => {
            UploadedImageError::retryable("Failed to fetch replied-to event", details)
        }
        ReplyError::Deserialization => {
            UploadedImageError::validation("Failed to deserialize replied-to event", details)
        }
        ReplyError::StateEvent => {
            UploadedImageError::validation("Cannot reply to a state event", details)
        }
    }
}

fn map_reply_error_for_voice(error: ReplyError) -> UploadedVoiceError {
    let details = Some(format!("{error:?}"));
    match error {
        ReplyError::Fetch(_) => {
            UploadedVoiceError::retryable("Failed to fetch replied-to event", details)
        }
        ReplyError::Deserialization => {
            UploadedVoiceError::validation("Failed to deserialize replied-to event", details)
        }
        ReplyError::StateEvent => {
            UploadedVoiceError::validation("Cannot reply to a state event", details)
        }
    }
}

fn map_reply_error_for_video(error: ReplyError) -> UploadedVideoError {
    let details = Some(format!("{error:?}"));
    match error {
        ReplyError::Fetch(_) => {
            UploadedVideoError::retryable("Failed to fetch replied-to event", details)
        }
        ReplyError::Deserialization => {
            UploadedVideoError::validation("Failed to deserialize replied-to event", details)
        }
        ReplyError::StateEvent => {
            UploadedVideoError::validation("Cannot reply to a state event", details)
        }
    }
}

fn map_reply_error_for_file(error: ReplyError) -> UploadedFileError {
    let details = Some(format!("{error:?}"));
    match error {
        ReplyError::Fetch(_) => {
            UploadedFileError::retryable("Failed to fetch replied-to event", details)
        }
        ReplyError::Deserialization => {
            UploadedFileError::validation("Failed to deserialize replied-to event", details)
        }
        ReplyError::StateEvent => {
            UploadedFileError::validation("Cannot reply to a state event", details)
        }
    }
}

/// Generates a `matrix.to` permalink to the given room alias.
#[matrix_sdk_ffi_macros::export]
pub fn matrix_to_room_alias_permalink(
    room_alias: String,
) -> std::result::Result<String, ClientError> {
    let room_alias = RoomAliasId::parse(room_alias)?;
    Ok(room_alias.matrix_to_uri().to_string())
}

#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait RoomInfoListener: SyncOutsideWasm + SendOutsideWasm {
    fn call(&self, room_info: RoomInfo);
}

#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait TypingNotificationsListener: SyncOutsideWasm + SendOutsideWasm {
    fn call(&self, typing_user_ids: Vec<String>);
}

#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait IdentityStatusChangeListener: SyncOutsideWasm + SendOutsideWasm {
    fn call(&self, identity_status_change: Vec<IdentityStatusChange>);
}

#[derive(uniffi::Object)]
pub struct RoomMembersIterator {
    chunk_iterator: ChunkIterator<matrix_sdk::room::RoomMember>,
}

impl RoomMembersIterator {
    fn new(members: Vec<matrix_sdk::room::RoomMember>) -> Self {
        Self { chunk_iterator: ChunkIterator::new(members) }
    }
}

#[matrix_sdk_ffi_macros::export]
impl RoomMembersIterator {
    fn len(&self) -> u32 {
        self.chunk_iterator.len()
    }

    fn next_chunk(&self, chunk_size: u32) -> Option<Vec<RoomMember>> {
        self.chunk_iterator
            .next(chunk_size)
            .map(|members| members.into_iter().filter_map(|m| m.try_into().ok()).collect())
    }
}

/// Information about a member considered to be a room hero.
#[derive(uniffi::Record)]
pub struct RoomHero {
    /// The user ID of the hero.
    user_id: String,
    /// The display name of the hero.
    display_name: Option<String>,
    /// The avatar URL of the hero.
    avatar_url: Option<String>,
}

impl From<SdkRoomHero> for RoomHero {
    fn from(value: SdkRoomHero) -> Self {
        Self {
            user_id: value.user_id.to_string(),
            display_name: value.display_name.clone(),
            avatar_url: value.avatar_url.as_ref().map(ToString::to_string),
        }
    }
}

/// An update for a particular user's power level within the room.
#[derive(uniffi::Record)]
pub struct UserPowerLevelUpdate {
    /// The user ID of the user to update.
    user_id: String,
    /// The power level to assign to the user.
    power_level: i64,
}

impl TryFrom<ImageInfo> for RumaAvatarImageInfo {
    type Error = MediaInfoError;

    fn try_from(value: ImageInfo) -> Result<Self, MediaInfoError> {
        let thumbnail_url = if let Some(media_source) = value.thumbnail_source {
            match &media_source.as_ref().media_source {
                RumaMediaSource::Plain(mxc_uri) => Some(mxc_uri.clone()),
                RumaMediaSource::Encrypted(_) => return Err(MediaInfoError::InvalidField),
            }
        } else {
            None
        };

        Ok(assign!(RumaAvatarImageInfo::new(), {
            height: value.height.map(u64_to_uint),
            width: value.width.map(u64_to_uint),
            mimetype: value.mimetype,
            size: value.size.map(u64_to_uint),
            thumbnail_info: value.thumbnail_info.map(Into::into).map(Box::new),
            thumbnail_url: thumbnail_url,
            blurhash: value.blurhash,
        }))
    }
}

/// Current draft of the composer for the room.
#[derive(uniffi::Record)]
pub struct ComposerDraft {
    /// The draft content in plain text.
    pub plain_text: String,
    /// If the message is formatted in HTML, the HTML representation of the
    /// message.
    pub html_text: Option<String>,
    /// The type of draft.
    pub draft_type: ComposerDraftType,
    /// Attachments associated with this draft.
    pub attachments: Vec<DraftAttachment>,
}

impl From<SdkComposerDraft> for ComposerDraft {
    fn from(value: SdkComposerDraft) -> Self {
        let SdkComposerDraft { plain_text, html_text, draft_type, attachments } = value;
        Self {
            plain_text,
            html_text,
            draft_type: draft_type.into(),
            attachments: attachments.into_iter().map(|a| a.into()).collect(),
        }
    }
}

impl TryFrom<ComposerDraft> for SdkComposerDraft {
    type Error = ClientError;

    fn try_from(value: ComposerDraft) -> std::result::Result<Self, Self::Error> {
        let ComposerDraft { plain_text, html_text, draft_type, attachments } = value;
        Ok(Self {
            plain_text,
            html_text,
            draft_type: draft_type.try_into()?,
            attachments: attachments
                .into_iter()
                .map(|a| a.try_into())
                .collect::<std::result::Result<Vec<_>, _>>()?,
        })
    }
}

/// An attachment stored with a composer draft.
#[derive(uniffi::Enum)]
pub enum DraftAttachment {
    Audio { audio_info: AudioInfo, source: UploadSource },
    File { file_info: FileInfo, source: UploadSource },
    Image { image_info: ImageInfo, source: UploadSource, thumbnail_source: Option<UploadSource> },
    Video { video_info: VideoInfo, source: UploadSource, thumbnail_source: Option<UploadSource> },
}

impl From<SdkDraftAttachment> for DraftAttachment {
    fn from(value: SdkDraftAttachment) -> Self {
        match value.content {
            DraftAttachmentContent::Image {
                data,
                mimetype,
                size,
                width,
                height,
                blurhash,
                thumbnail,
            } => {
                let thumbnail_source = thumbnail.as_ref().map(|t| UploadSource::Data {
                    bytes: t.data.clone(),
                    filename: t.filename.clone(),
                });
                let thumbnail_info = thumbnail.map(|t| ThumbnailInfo {
                    width: t.width,
                    height: t.height,
                    mimetype: t.mimetype,
                    size: t.size,
                });
                DraftAttachment::Image {
                    image_info: ImageInfo {
                        height,
                        width,
                        mimetype,
                        size,
                        thumbnail_info,
                        thumbnail_source: None,
                        blurhash,
                        is_animated: None,
                    },
                    source: UploadSource::Data { bytes: data, filename: value.filename },
                    thumbnail_source,
                }
            }
            DraftAttachmentContent::Video {
                data,
                mimetype,
                size,
                width,
                height,
                duration,
                blurhash,
                thumbnail,
            } => {
                let thumbnail_source = thumbnail.as_ref().map(|t| UploadSource::Data {
                    bytes: t.data.clone(),
                    filename: t.filename.clone(),
                });
                let thumbnail_info = thumbnail.map(|t| ThumbnailInfo {
                    width: t.width,
                    height: t.height,
                    mimetype: t.mimetype,
                    size: t.size,
                });
                DraftAttachment::Video {
                    video_info: VideoInfo {
                        duration,
                        height,
                        width,
                        mimetype,
                        size,
                        thumbnail_info,
                        thumbnail_source: None,
                        blurhash,
                    },
                    source: UploadSource::Data { bytes: data, filename: value.filename },
                    thumbnail_source,
                }
            }
            DraftAttachmentContent::Audio { data, mimetype, size, duration } => {
                DraftAttachment::Audio {
                    audio_info: AudioInfo { duration, size, mimetype },
                    source: UploadSource::Data { bytes: data, filename: value.filename },
                }
            }
            DraftAttachmentContent::File { data, mimetype, size } => DraftAttachment::File {
                file_info: FileInfo {
                    mimetype,
                    size,
                    thumbnail_info: None,
                    thumbnail_source: None,
                },
                source: UploadSource::Data { bytes: data, filename: value.filename },
            },
        }
    }
}

/// Resolve the bytes and filename from an `UploadSource`, reading the file
/// contents if needed.
fn read_upload_source(source: UploadSource) -> Result<(Vec<u8>, String), ClientError> {
    match source {
        UploadSource::Data { bytes, filename } => Ok((bytes, filename)),
        UploadSource::File { filename } => {
            let path: PathBuf = filename.into();
            let filename = path
                .file_name()
                .ok_or(ClientError::Generic {
                    msg: "Invalid attachment path".to_owned(),
                    details: None,
                })?
                .to_str()
                .ok_or(ClientError::Generic {
                    msg: "Invalid attachment path".to_owned(),
                    details: None,
                })?
                .to_owned();

            let bytes = fs::read(&path).map_err(|_| ClientError::Generic {
                msg: "Could not load file".to_owned(),
                details: None,
            })?;

            Ok((bytes, filename))
        }
    }
}

impl TryFrom<DraftAttachment> for SdkDraftAttachment {
    type Error = ClientError;

    fn try_from(value: DraftAttachment) -> Result<Self, Self::Error> {
        fn draft_thumbnail(
            thumbnail_info: Option<ThumbnailInfo>,
            thumbnail_source: Option<UploadSource>,
        ) -> Result<Option<DraftThumbnail>, ClientError> {
            if let Some(info) = thumbnail_info
                && let Some(source) = thumbnail_source
            {
                let (data, filename) = read_upload_source(source)?;
                Ok(Some(DraftThumbnail {
                    filename,
                    data,
                    mimetype: info.mimetype,
                    width: info.width,
                    height: info.height,
                    size: info.size,
                }))
            } else {
                Ok(None)
            }
        }

        match value {
            DraftAttachment::Image { image_info, source, thumbnail_source, .. } => {
                let (data, filename) = read_upload_source(source)?;
                Ok(Self {
                    filename,
                    content: DraftAttachmentContent::Image {
                        data,
                        mimetype: image_info.mimetype,
                        size: image_info.size,
                        width: image_info.width,
                        height: image_info.height,
                        blurhash: image_info.blurhash,
                        thumbnail: draft_thumbnail(image_info.thumbnail_info, thumbnail_source)?,
                    },
                })
            }
            DraftAttachment::Video { video_info, source, thumbnail_source, .. } => {
                let (data, filename) = read_upload_source(source)?;
                Ok(Self {
                    filename,
                    content: DraftAttachmentContent::Video {
                        data,
                        mimetype: video_info.mimetype,
                        size: video_info.size,
                        width: video_info.width,
                        height: video_info.height,
                        duration: video_info.duration,
                        blurhash: video_info.blurhash,
                        thumbnail: draft_thumbnail(video_info.thumbnail_info, thumbnail_source)?,
                    },
                })
            }
            DraftAttachment::Audio { audio_info, source, .. } => {
                let (data, filename) = read_upload_source(source)?;
                Ok(Self {
                    filename,
                    content: DraftAttachmentContent::Audio {
                        data,
                        mimetype: audio_info.mimetype,
                        size: audio_info.size,
                        duration: audio_info.duration,
                    },
                })
            }
            DraftAttachment::File { file_info, source, .. } => {
                let (data, filename) = read_upload_source(source)?;
                Ok(Self {
                    filename,
                    content: DraftAttachmentContent::File {
                        data,
                        mimetype: file_info.mimetype,
                        size: file_info.size,
                    },
                })
            }
        }
    }
}

/// The type of draft of the composer.
#[derive(uniffi::Enum)]
pub enum ComposerDraftType {
    /// The draft is a new message.
    NewMessage,
    /// The draft is a reply to an event.
    Reply {
        /// The ID of the event being replied to.
        event_id: String,
    },
    /// The draft is an edit of an event.
    Edit {
        /// The ID of the event being edited.
        event_id: String,
    },
}

impl From<SdkComposerDraftType> for ComposerDraftType {
    fn from(value: SdkComposerDraftType) -> Self {
        match value {
            SdkComposerDraftType::NewMessage => Self::NewMessage,
            SdkComposerDraftType::Reply { event_id } => Self::Reply { event_id: event_id.into() },
            SdkComposerDraftType::Edit { event_id } => Self::Edit { event_id: event_id.into() },
        }
    }
}

impl TryFrom<ComposerDraftType> for SdkComposerDraftType {
    type Error = ruma::IdParseError;

    fn try_from(value: ComposerDraftType) -> std::result::Result<Self, Self::Error> {
        let draft_type = match value {
            ComposerDraftType::NewMessage => Self::NewMessage,
            ComposerDraftType::Reply { event_id } => Self::Reply { event_id: event_id.try_into()? },
            ComposerDraftType::Edit { event_id } => Self::Edit { event_id: event_id.try_into()? },
        };

        Ok(draft_type)
    }
}

#[derive(Debug, Clone, uniffi::Enum)]
pub enum RoomHistoryVisibility {
    /// Previous events are accessible to newly joined members from the point
    /// they were invited onwards.
    ///
    /// Events stop being accessible when the member's state changes to
    /// something other than *invite* or *join*.
    Invited,

    /// Previous events are accessible to newly joined members from the point
    /// they joined the room onwards.
    /// Events stop being accessible when the member's state changes to
    /// something other than *join*.
    Joined,

    /// Previous events are always accessible to newly joined members.
    ///
    /// All events in the room are accessible, even those sent when the member
    /// was not a part of the room.
    Shared,

    /// All events while this is the `HistoryVisibility` value may be shared by
    /// any participating homeserver with anyone, regardless of whether they
    /// have ever joined the room.
    WorldReadable,

    /// A custom visibility value.
    Custom { value: String },
}

impl TryFrom<RumaHistoryVisibility> for RoomHistoryVisibility {
    type Error = NotYetImplemented;
    fn try_from(value: RumaHistoryVisibility) -> Result<Self, Self::Error> {
        match value {
            RumaHistoryVisibility::Invited => Ok(RoomHistoryVisibility::Invited),
            RumaHistoryVisibility::Shared => Ok(RoomHistoryVisibility::Shared),
            RumaHistoryVisibility::WorldReadable => Ok(RoomHistoryVisibility::WorldReadable),
            RumaHistoryVisibility::Joined => Ok(RoomHistoryVisibility::Joined),
            RumaHistoryVisibility::_Custom(_) => {
                Ok(RoomHistoryVisibility::Custom { value: value.to_string() })
            }
            _ => Err(NotYetImplemented),
        }
    }
}

impl TryFrom<RoomHistoryVisibility> for RumaHistoryVisibility {
    type Error = NotYetImplemented;
    fn try_from(value: RoomHistoryVisibility) -> Result<Self, Self::Error> {
        match value {
            RoomHistoryVisibility::Invited => Ok(RumaHistoryVisibility::Invited),
            RoomHistoryVisibility::Shared => Ok(RumaHistoryVisibility::Shared),
            RoomHistoryVisibility::Joined => Ok(RumaHistoryVisibility::Joined),
            RoomHistoryVisibility::WorldReadable => Ok(RumaHistoryVisibility::WorldReadable),
            RoomHistoryVisibility::Custom { .. } => Err(NotYetImplemented),
        }
    }
}

/// When a room A is tombstoned, it is replaced by a room B. The room A is the
/// predecessor of B, and B is the successor of A. This type holds information
/// about the successor room. See [`Room::successor_room`].
///
/// A room is tombstoned if it has received a [`m.room.tombstone`] state event.
///
/// [`m.room.tombstone`]: https://spec.matrix.org/v1.14/client-server-api/#mroomtombstone
#[derive(uniffi::Record)]
pub struct SuccessorRoom {
    /// The ID of the replacement room.
    pub room_id: String,

    /// The message explaining why the room has been tombstoned.
    pub reason: Option<String>,
}

impl From<SdkSuccessorRoom> for SuccessorRoom {
    fn from(value: SdkSuccessorRoom) -> Self {
        Self { room_id: value.room_id.to_string(), reason: value.reason }
    }
}

/// When a room A is tombstoned, it is replaced by a room B. The room A is the
/// predecessor of B, and B is the successor of A. This type holds information
/// about the predecessor room. See [`Room::predecessor_room`].
///
/// To know the predecessor of a room, the [`m.room.create`] state event must
/// have been received.
///
/// [`m.room.create`]: https://spec.matrix.org/v1.14/client-server-api/#mroomcreate
#[derive(uniffi::Record)]
pub struct PredecessorRoom {
    /// The ID of the replacement room.
    pub room_id: String,
}

impl From<SdkPredecessorRoom> for PredecessorRoom {
    fn from(value: SdkPredecessorRoom) -> Self {
        Self { room_id: value.room_id.to_string() }
    }
}

/// A listener to send queue updates in a specific room.
#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait SendQueueListener: SyncOutsideWasm + SendOutsideWasm {
    /// Called every time the send queue dispatches an update for the given
    /// room.
    fn on_update(&self, update: RoomSendQueueUpdate);
}

/// An update to a room send queue.
#[derive(uniffi::Enum)]
pub enum RoomSendQueueUpdate {
    /// A new local event is being sent.
    NewLocalEvent {
        /// Transaction id used to identify this event.
        transaction_id: String,
    },

    /// A local event that hadn't been sent to the server yet has been cancelled
    /// before sending.
    CancelledLocalEvent {
        /// Transaction id used to identify this event.
        transaction_id: String,
    },

    /// A local event's content has been replaced with something else.
    ReplacedLocalEvent {
        /// Transaction id used to identify this event.
        transaction_id: String,
    },

    /// An error happened when an event was being sent.
    ///
    /// The event has not been removed from the queue. All the send queues
    /// will be disabled after this happens, and must be manually re-enabled.
    SendError {
        /// Transaction id used to identify this event.
        transaction_id: String,
        /// Error received while sending the event.
        error: QueueWedgeError,
        /// Whether the error is considered recoverable or not.
        ///
        /// An error that's recoverable will disable the room's send queue,
        /// while an unrecoverable error will be parked, until the user
        /// decides to cancel sending it.
        is_recoverable: bool,
    },

    /// The event has been unwedged and sending is now being retried.
    RetryEvent {
        /// Transaction id used to identify this event.
        transaction_id: String,
    },

    /// The event has been sent to the server, and the query returned
    /// successfully.
    SentEvent {
        /// Transaction id used to identify this event.
        transaction_id: String,
        /// Received event id from the send response.
        event_id: String,
    },

    /// A media upload (consisting of a file and possibly a thumbnail) has made
    /// progress.
    MediaUpload {
        /// The media event this uploaded media relates to.
        related_to: String,

        /// The final media source for the file if it has finished uploading.
        file: Option<Arc<MediaSource>>,

        /// The index of the media within the transaction. A file and its
        /// thumbnail share the same index. Will always be 0 for non-gallery
        /// media uploads.
        index: u64,

        /// The combined upload progress across the file and, if existing, its
        /// thumbnail. For gallery uploads, the progress is reported per indexed
        /// gallery item.
        progress: AbstractProgress,
    },
}

impl TryFrom<SdkRoomSendQueueUpdate> for RoomSendQueueUpdate {
    type Error = ClientError;

    fn try_from(value: SdkRoomSendQueueUpdate) -> std::result::Result<Self, Self::Error> {
        Ok(match value {
            SdkRoomSendQueueUpdate::CancelledLocalEvent { transaction_id } => {
                Self::CancelledLocalEvent { transaction_id: transaction_id.into() }
            }
            SdkRoomSendQueueUpdate::MediaUpload { related_to, file, index, progress } => {
                Self::MediaUpload {
                    related_to: related_to.into(),
                    file: file.map(|source| source.try_into().map(Arc::new)).transpose()?,
                    index,
                    progress: progress.into(),
                }
            }
            SdkRoomSendQueueUpdate::NewLocalEvent(local_echo) => {
                Self::NewLocalEvent { transaction_id: local_echo.transaction_id.into() }
            }
            SdkRoomSendQueueUpdate::ReplacedLocalEvent { transaction_id, .. } => {
                Self::ReplacedLocalEvent { transaction_id: transaction_id.into() }
            }
            SdkRoomSendQueueUpdate::RetryEvent { transaction_id } => {
                Self::RetryEvent { transaction_id: transaction_id.into() }
            }
            SdkRoomSendQueueUpdate::SendError { transaction_id, error, is_recoverable } => {
                let as_queue_wedge_error: matrix_sdk::QueueWedgeError = (&*error).into();
                Self::SendError {
                    transaction_id: transaction_id.into(),
                    error: as_queue_wedge_error.into(),
                    is_recoverable,
                }
            }
            SdkRoomSendQueueUpdate::SentEvent { transaction_id, event_id } => {
                Self::SentEvent { transaction_id: transaction_id.into(), event_id: event_id.into() }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Mutex};

    use matrix_sdk::{
        deserialized_responses::{
            AlgorithmInfo, EncryptionInfo, VerificationLevel,
            VerificationState as EventVerificationState,
        },
        room::IncludeRelations,
        test_utils::mocks::{MatrixMockServer, RoomRelationsResponseTemplate},
    };
    use matrix_sdk_test::{JoinedRoomBuilder, event_factory::EventFactory};
    use ruma::{
        DeviceKeyAlgorithm, event_id,
        events::{AnySyncTimelineEvent, TimelineEventType, relation::RelationType},
        owned_device_id, owned_event_id, room_id,
        serde::Raw,
        user_id,
    };
    use serde_json::{Value, json};
    use tokio::{
        sync::mpsc,
        time::{Duration, timeout},
    };

    use super::{
        RawRoomEvent, RawRoomEventEncryptionInfo, RawRoomEventListener, RawRoomRelationsDirection,
        RawRoomRelationsOptions, Room,
    };

    struct TestRawRoomEventListener {
        sender: Mutex<mpsc::UnboundedSender<RawRoomEvent>>,
    }

    impl RawRoomEventListener for TestRawRoomEventListener {
        fn on_event(&self, event: RawRoomEvent) {
            let _ = self.sender.lock().expect("listener mutex poisoned").send(event);
        }
    }

    #[test]
    fn test_raw_room_event_encryption_info_maps_algorithm_details() {
        let megolm_info = EncryptionInfo {
            sender: user_id!("@alice:example.org").to_owned(),
            sender_device: Some(owned_device_id!("ALICEDEVICE")),
            forwarder: None,
            algorithm_info: AlgorithmInfo::MegolmV1AesSha2 {
                curve25519_key: "megolm_curve25519_key".to_owned(),
                sender_claimed_keys: BTreeMap::from([(
                    DeviceKeyAlgorithm::Ed25519,
                    "claimed_ed25519_key".to_owned(),
                )]),
                session_id: Some("session_id".to_owned()),
            },
            verification_state: EventVerificationState::Verified,
        };

        let mapped = RawRoomEventEncryptionInfo::from(&megolm_info);
        assert_eq!(mapped.sender, "@alice:example.org");
        assert_eq!(mapped.sender_device.as_deref(), Some("ALICEDEVICE"));
        assert_eq!(mapped.sender_curve25519_key_base64.as_deref(), Some("megolm_curve25519_key"));
        assert!(mapped.sender_verified);

        let olm_info = EncryptionInfo {
            sender: user_id!("@bob:example.org").to_owned(),
            sender_device: None,
            forwarder: None,
            algorithm_info: AlgorithmInfo::OlmV1Curve25519AesSha2 {
                curve25519_public_key_base64: "olm_curve25519_key".to_owned(),
            },
            verification_state: EventVerificationState::Unverified(
                VerificationLevel::UnsignedDevice,
            ),
        };

        let mapped = RawRoomEventEncryptionInfo::from(&olm_info);
        assert_eq!(mapped.sender, "@bob:example.org");
        assert!(mapped.sender_device.is_none());
        assert_eq!(mapped.sender_curve25519_key_base64.as_deref(), Some("olm_curve25519_key"));
        assert!(!mapped.sender_verified);
    }

    #[tokio::test]
    async fn test_raw_timeline_event_listener_receives_filtered_relation_events() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room_id = room_id!("!matrixrtc:example.org");
        let sender = user_id!("@alice:example.org");
        let membership_event_id = "$membership:example.org";
        let reaction_event_id = "$reaction:example.org";

        let sdk_room = server.sync_joined_room(&client, room_id).await;
        let room = Room::new(sdk_room, None);

        let (sender_tx, mut receiver) = mpsc::unbounded_channel();
        let _listener_handle = room.subscribe_to_raw_timeline_events(
            vec!["m.reaction".to_owned(), "m.room.redaction".to_owned()],
            Box::new(TestRawRoomEventListener { sender: Mutex::new(sender_tx) }),
        );

        let ignored_event = Raw::new(&json!({
            "content": {
                "body": "ignored",
                "msgtype": "m.text",
            },
            "event_id": "$message:example.org",
            "origin_server_ts": 999,
            "room_id": room_id,
            "sender": sender,
            "type": "m.room.message",
        }))
        .expect("raw ignored event should serialize")
        .cast_unchecked::<AnySyncTimelineEvent>();

        let reaction_event = Raw::new(&json!({
            "content": {
                "m.relates_to": {
                    "event_id": membership_event_id,
                    "key": "\u{1f590}\u{fe0f}",
                    "rel_type": "m.annotation",
                },
            },
            "event_id": reaction_event_id,
            "origin_server_ts": 1234,
            "room_id": room_id,
            "sender": sender,
            "type": "m.reaction",
        }))
        .expect("raw reaction event should serialize")
        .cast_unchecked::<AnySyncTimelineEvent>();

        let redaction_event = Raw::new(&json!({
            "content": {},
            "event_id": "$redaction:example.org",
            "origin_server_ts": 1235,
            "redacts": reaction_event_id,
            "room_id": room_id,
            "sender": sender,
            "type": "m.room.redaction",
        }))
        .expect("raw redaction event should serialize")
        .cast_unchecked::<AnySyncTimelineEvent>();

        server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(room_id).add_timeline_bulk(vec![
                    ignored_event,
                    reaction_event,
                    redaction_event,
                ]),
            )
            .await;

        let reaction = timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("listener should receive the reaction event")
            .expect("listener channel should stay open");
        assert_eq!(reaction.room_id, room_id.to_string());
        assert_eq!(reaction.event_type, "m.reaction");
        assert_eq!(reaction.event_id.as_deref(), Some(reaction_event_id));
        assert_eq!(reaction.sender.as_deref(), Some(sender.as_str()));
        assert_eq!(reaction.origin_server_ts_ms, Some(1234));
        assert!(reaction.encryption_info.is_none());

        let content: Value =
            serde_json::from_str(&reaction.content_json).expect("content should be JSON");
        assert_eq!(content["m.relates_to"]["event_id"], membership_event_id);
        assert_eq!(content["m.relates_to"]["rel_type"], "m.annotation");
        assert_eq!(content["m.relates_to"]["key"], "\u{1f590}\u{fe0f}");

        let raw: Value = serde_json::from_str(&reaction.raw_json).expect("raw should be JSON");
        assert_eq!(raw["event_id"], reaction_event_id);
        assert_eq!(raw["type"], "m.reaction");

        let redaction = timeout(Duration::from_secs(5), receiver.recv())
            .await
            .expect("listener should receive the redaction event")
            .expect("listener channel should stay open");
        assert_eq!(redaction.event_type, "m.room.redaction");
        assert_eq!(redaction.event_id.as_deref(), Some("$redaction:example.org"));

        let raw: Value = serde_json::from_str(&redaction.raw_json).expect("raw should be JSON");
        assert_eq!(raw["redacts"], reaction_event_id);
    }

    #[tokio::test]
    async fn test_get_event_relations_returns_filtered_raw_relation_events() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;

        let room_id = room_id!("!matrixrtc:example.org");
        let sender = user_id!("@alice:example.org");
        let target_event_id = owned_event_id!("$membership:example.org");
        let reaction_event_id = event_id!("$reaction:example.org");
        let f = EventFactory::new().room(room_id).sender(sender);

        let reaction_event = f
            .reaction(&target_event_id, "\u{1f590}\u{fe0f}")
            .event_id(reaction_event_id)
            .into_raw();

        server
            .mock_room_relations()
            .match_target_event(target_event_id.clone())
            .match_subrequest(IncludeRelations::RelationsOfTypeAndEventType(
                RelationType::Annotation,
                TimelineEventType::Reaction,
            ))
            .match_from("next_batch")
            .match_limit(20)
            .ok(RoomRelationsResponseTemplate::default()
                .events(vec![reaction_event])
                .prev_batch("prev_batch")
                .next_batch("more")
                .recursion_depth(1))
            .mock_once()
            .mount()
            .await;

        let sdk_room = server.sync_joined_room(&client, room_id).await;
        let room = Room::new(sdk_room, None);

        let result = room
            .get_event_relations(
                target_event_id.to_string(),
                RawRoomRelationsOptions {
                    relation_type: Some("m.annotation".to_owned()),
                    event_type: Some("m.reaction".to_owned()),
                    from: Some("next_batch".to_owned()),
                    limit: Some(20),
                    direction: RawRoomRelationsDirection::Backward,
                    recurse: true,
                },
            )
            .await
            .expect("room relations should load");

        assert_eq!(result.prev_batch_token.as_deref(), Some("prev_batch"));
        assert_eq!(result.next_batch_token.as_deref(), Some("more"));
        assert_eq!(result.recursion_depth, Some(1));
        assert_eq!(result.chunk.len(), 1);

        let reaction = &result.chunk[0];
        assert_eq!(reaction.room_id, room_id.to_string());
        assert_eq!(reaction.event_type, "m.reaction");
        assert_eq!(reaction.event_id.as_deref(), Some(reaction_event_id.as_str()));
        assert_eq!(reaction.sender.as_deref(), Some(sender.as_str()));
        assert!(reaction.encryption_info.is_none());

        let content: Value =
            serde_json::from_str(&reaction.content_json).expect("content should be JSON");
        assert_eq!(content["m.relates_to"]["event_id"], target_event_id.as_str());
        assert_eq!(content["m.relates_to"]["rel_type"], "m.annotation");
        assert_eq!(content["m.relates_to"]["key"], "\u{1f590}\u{fe0f}");

        let raw: Value = serde_json::from_str(&reaction.raw_json).expect("raw should be JSON");
        assert_eq!(raw["event_id"], reaction_event_id.as_str());
        assert_eq!(raw["type"], "m.reaction");
    }
}
