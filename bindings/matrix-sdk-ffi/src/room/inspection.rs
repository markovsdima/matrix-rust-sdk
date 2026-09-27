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

//! Inspect individual events without creating a UI timeline or writing events
//! to the event cache.

use anyhow::ensure;
use matrix_sdk::{
    deserialized_responses::{TimelineEvent, TimelineEventKind, UnableToDecryptInfo},
    event_cache::EventCacheError,
};
use matrix_sdk_base::crypto::types::events::UtdCause;
use matrix_sdk_ui::timeline::default_event_filter;
use ruma::{
    EventId, OwnedEventId, OwnedRoomId, RoomId,
    api::client::room::get_room_event,
    events::{
        AnyMessageLikeEventContent, AnySyncMessageLikeEvent, AnySyncStateEvent,
        AnySyncTimelineEvent, SyncMessageLikeEvent,
        room::{
            encrypted::{EncryptedEventScheme, OriginalSyncRoomEncryptedEvent},
            message::MessageType,
        },
    },
    room_version_rules::RoomVersionRules,
    serde::Raw,
};

use super::{RawRoomEvent, RawRoomEventEncryptionInfo, Room, raw_room_event_from_raw_json};
use crate::{error::ClientError, timeline::EncryptedMessage};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum RoomTimelineEventDisposition {
    /// Eligible as a standalone item under the SDK's default policy, including
    /// thread replies. This is not an aggregated timeline item and does not
    /// imply inclusion under an application's custom timeline filter.
    Visible,
    /// A recognized event excluded as a standalone item by the SDK's policy.
    Hidden,
    /// The SDK established a decryption failure, even if its cause is unknown.
    UnableToDecrypt,
    /// Insufficient information, malformed content, or an unsupported type.
    /// This must never be interpreted as permission to remove retained history.
    Indeterminate,
}

#[derive(Clone, uniffi::Record)]
pub struct RoomTimelineEventInspection {
    /// The requested event, with complete raw JSON and original identity.
    /// Decrypted JSON is sensitive account data and must not be logged.
    pub event: RawRoomEvent,
    pub disposition: RoomTimelineEventDisposition,
    /// Present exactly when disposition is `UnableToDecrypt`. Uses the same
    /// cause classification as the SDK timeline, including trust failures.
    pub decryption_failure: Option<EncryptedMessage>,
}

#[matrix_sdk_ffi_macros::export]
impl Room {
    /// Inspect an event using the cache first, then the homeserver if needed.
    /// Undecrypted message envelopes and cached message UTDs are retried with
    /// current SDK keys and trust settings. Already decrypted cache entries are
    /// returned without decrypting again or rechecking stricter trust settings.
    /// Neither fetched nor decrypted results are written to the event cache.
    /// Lookup and identity errors are returned to the caller.
    ///
    /// No timeline is created, focused, or paginated. `Visible` means eligible
    /// under the SDK's default policy; edits, reactions, and poll results are
    /// not aggregated. Unknown or malformed events are never classified hidden.
    /// Custom application filters may exclude a `Visible` event indefinitely.
    /// Encrypted state events are not decrypted by this API yet: unresolved
    /// envelopes are `Indeterminate`, while established SDK UTDs remain UTDs.
    ///
    /// Normal UniFFI cancellation stops this call and its owned work. Shared
    /// SDK key recovery already triggered by decryption may continue. No chat
    /// messages, redactions, read receipts, or typing events are sent.
    pub async fn inspect_timeline_event(
        &self,
        event_id: String,
    ) -> Result<RoomTimelineEventInspection, ClientError> {
        let event_id = EventId::parse(event_id)?;
        let cached = match self.inner.event_cache().await {
            Ok((cache, _handles)) => cache.find_event_strict(&event_id).await?,
            Err(EventCacheError::NotSubscribedYet) => None,
            Err(error) => return Err(error.into()),
        };

        let mut event = match cached {
            Some(event) => event,
            None => {
                // Room::event/load_or_fetch_event persist fetched responses.
                // Keep inspection read-only, including on cache misses.
                let request = get_room_event::v3::Request::new(
                    self.inner.room_id().to_owned(),
                    event_id.clone(),
                );
                let response =
                    self.inner.client().send(request).await.map_err(matrix_sdk::Error::from)?;
                TimelineEvent::from_plaintext(response.event.cast())
            }
        };
        validate_identity(event.raw(), &event_id, self.inner.room_id())?;

        // Do not treat a redacted encrypted event as an encrypted envelope.
        // Its empty content must follow the SDK's room-version redaction rules.
        let encrypted_envelope = matches!(
            event.raw().deserialize(),
            Ok(AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomEncrypted(
                SyncMessageLikeEvent::Original(_)
            )))
        );
        if !is_encrypted_state_event(event.raw())
            && (event.kind.is_utd()
                || (matches!(event.kind, TimelineEventKind::PlainText { .. })
                    && encrypted_envelope))
        {
            event = self
                .inner
                .decrypt_event(
                    event.raw().cast_ref_unchecked::<OriginalSyncRoomEncryptedEvent>(),
                    None,
                )
                .await?;
            validate_identity(event.raw(), &event_id, self.inner.room_id())?;
        }

        let (disposition, decryption_failure) = match &event.kind {
            TimelineEventKind::UnableToDecrypt { event, utd_info } => (
                RoomTimelineEventDisposition::UnableToDecrypt,
                Some(self.inspection_decryption_failure(event, utd_info).await),
            ),
            _ => {
                let rules = self.inner.clone_info().room_version().and_then(|v| v.rules());
                (classify_event(event.raw(), rules.as_ref()), None)
            }
        };
        let encryption_info =
            event.encryption_info().map(|info| RawRoomEventEncryptionInfo::from(info.as_ref()));
        let event = raw_room_event_from_raw_json(
            self.inner.room_id().to_string(),
            event.raw().json().get(),
            encryption_info,
        )
        .map_err(ClientError::from_err)?;

        Ok(RoomTimelineEventInspection { event, disposition, decryption_failure })
    }
}

impl Room {
    async fn inspection_decryption_failure(
        &self,
        event: &Raw<AnySyncTimelineEvent>,
        utd_info: &UnableToDecryptInfo,
    ) -> EncryptedMessage {
        if let Ok(AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::RoomEncrypted(
            SyncMessageLikeEvent::Original(encrypted),
        ))) = event.deserialize()
        {
            match encrypted.content.scheme {
                EncryptedEventScheme::MegolmV1AesSha2(content) => {
                    let cause = UtdCause::determine(
                        event,
                        self.inner.crypto_context_info().await,
                        utd_info,
                    );
                    return EncryptedMessage::MegolmV1AesSha2 {
                        session_id: content.session_id,
                        cause,
                    };
                }
                EncryptedEventScheme::OlmV1Curve25519AesSha2(content) => {
                    return EncryptedMessage::OlmV1Curve25519AesSha2 {
                        sender_key: content.sender_key,
                    };
                }
                _ => {}
            }
        }
        EncryptedMessage::Unknown
    }
}

fn validate_identity(
    raw: &Raw<AnySyncTimelineEvent>,
    requested_id: &EventId,
    room_id: &RoomId,
) -> anyhow::Result<()> {
    ensure!(
        raw.get_field::<OwnedEventId>("event_id")?.as_deref() == Some(requested_id),
        "Inspection event ID does not match the requested event"
    );
    if let Some(actual_room) = raw.get_field::<OwnedRoomId>("room_id")? {
        ensure!(actual_room == room_id, "Inspection event belongs to a different room");
    }
    Ok(())
}

fn is_encrypted_state_event(raw: &Raw<AnySyncTimelineEvent>) -> bool {
    // Inspect the envelope rather than a feature-gated Ruma enum variant. This
    // must also work when another crate enables encrypted state through Cargo
    // feature unification, without enabling any corresponding FFI feature.
    raw.get_field::<String>("type").ok().flatten().as_deref() == Some("m.room.encrypted")
        && raw.get_field::<String>("state_key").ok().flatten().is_some()
}

fn classify_event(
    raw: &Raw<AnySyncTimelineEvent>,
    rules: Option<&RoomVersionRules>,
) -> RoomTimelineEventDisposition {
    use RoomTimelineEventDisposition::{Hidden, Indeterminate, Visible};

    // Do not let the default filter classify an unsupported encrypted state
    // envelope as visible merely because Ruma recognizes its state event type.
    if is_encrypted_state_event(raw) {
        return Indeterminate;
    }

    let (Ok(event), Some(rules)) = (raw.deserialize(), rules) else {
        return Indeterminate;
    };
    // The default filter intentionally excludes unsupported message types, and
    // includes custom state/redacted events. Neither is conclusive inspection
    // evidence: only recognized, parsed types can be visible or hidden.
    match &event {
        AnySyncTimelineEvent::MessageLike(AnySyncMessageLikeEvent::_Custom(_))
        | AnySyncTimelineEvent::State(AnySyncStateEvent::_Custom(_)) => return Indeterminate,
        AnySyncTimelineEvent::MessageLike(message) => match message.original_content() {
            Some(AnyMessageLikeEventContent::RoomMessage(content))
                if matches!(content.msgtype, MessageType::_Custom(_)) =>
            {
                return Indeterminate;
            }
            Some(AnyMessageLikeEventContent::RoomEncrypted(_)) => return Indeterminate,
            _ => {}
        },
        _ => {}
    }

    if default_event_filter(&event, rules) { Visible } else { Hidden }
}

#[cfg(test)]
mod tests;
