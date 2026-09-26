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

//! Direct poll sends, independent of the timeline and SDK send queue.

use std::{collections::HashSet, fmt::Write as _};

use anyhow::{Context, ensure};
use matrix_sdk::room::edit::EditedContent;
use ruma::{
    EventId,
    events::poll::{
        unstable_end::UnstablePollEndEventContent,
        unstable_response::UnstablePollResponseEventContent,
        unstable_start::{
            NewUnstablePollStartEventContent, UnstablePollAnswer, UnstablePollAnswers,
            UnstablePollStartContentBlock,
        },
    },
};

use super::Room;
use crate::{error::ClientError, ruma::PollKind, timeline::PollAnswer};

/// Content for a direct poll start or edit, with caller-owned answer IDs.
///
/// Persist this entire value and the transaction ID before sending. A retry
/// must use the same values. When editing, retain IDs of existing answers and
/// assign new IDs to newly added answers. The existing `PollData` API is
/// unaffected.
#[derive(Clone, uniffi::Record)]
pub struct DirectPollData {
    pub question: String,
    /// Between 1 and 20 answers, with nonempty IDs unique within the poll.
    pub answers: Vec<PollAnswer>,
    /// Must be at least 1.
    pub max_selections: u8,
    pub poll_kind: PollKind,
}

impl DirectPollData {
    fn into_content(self) -> anyhow::Result<(String, UnstablePollStartContentBlock)> {
        ensure!(self.max_selections >= 1, "maxSelections must be at least 1");
        validate_answer_ids(self.answers.iter().map(|answer| answer.id.as_str()))?;

        let mut fallback_text = self.question.clone();
        for (index, answer) in self.answers.iter().enumerate() {
            write!(fallback_text, "\n{}. {}", index + 1, answer.text)
                .expect("writing to a String cannot fail");
        }

        let answers = UnstablePollAnswers::try_from(
            self.answers
                .into_iter()
                .map(|answer| UnstablePollAnswer::new(answer.id, answer.text))
                .collect::<Vec<_>>(),
        )
        .context("Failed to create poll answers")?;
        let mut content = UnstablePollStartContentBlock::new(self.question, answers);
        content.max_selections = self.max_selections.into();
        content.kind = self.poll_kind.into();
        Ok((fallback_text, content))
    }
}

fn validate_answer_ids<'a>(ids: impl IntoIterator<Item = &'a str>) -> anyhow::Result<()> {
    let mut seen = HashSet::new();
    for id in ids {
        ensure!(!id.is_empty(), "Poll answer IDs must not be empty");
        ensure!(seen.insert(id), "Poll answer IDs must be unique");
    }
    Ok(())
}

fn validate_transaction_id(transaction_id: &str) -> anyhow::Result<()> {
    ensure!(!transaction_id.is_empty(), "transactionId must not be empty");
    Ok(())
}

#[matrix_sdk_ffi_macros::export]
impl Room {
    /// Send an MSC3381 poll directly and return its server-assigned event ID.
    ///
    /// Uses normal SDK encryption and waits for the homeserver response without
    /// creating a timeline or enqueueing a local echo. Errors are returned to
    /// the caller. Retries must reuse both the transaction ID and poll data.
    /// An empty transaction ID is rejected before any network requests.
    pub async fn send_poll_start_with_transaction_id_returning_event_id(
        &self,
        poll_data: DirectPollData,
        transaction_id: String,
    ) -> Result<String, ClientError> {
        validate_transaction_id(&transaction_id)?;
        let (fallback_text, poll_start) = poll_data.into_content()?;
        let content = NewUnstablePollStartEventContent::plain_text(fallback_text, poll_start);
        let result = self.inner.send(content).with_transaction_id(transaction_id.into()).await?;
        Ok(result.response.event_id.to_string())
    }

    /// Send an MSC3381 vote directly and return the response event's ID.
    ///
    /// `poll_start_event_id` must identify the original poll start. Answer IDs
    /// must be nonempty and unique; an empty list is allowed to clear a vote.
    /// The caller checks the poll's current state and available answers. No
    /// responses or end events are fetched. Retries must reuse the payload and
    /// transaction ID. Normal SDK encryption applies and send errors propagate.
    /// An empty transaction ID is rejected before any network requests.
    pub async fn send_poll_response_with_transaction_id_returning_event_id(
        &self,
        poll_start_event_id: String,
        answers: Vec<String>,
        transaction_id: String,
    ) -> Result<String, ClientError> {
        validate_transaction_id(&transaction_id)?;
        let event_id = EventId::parse(poll_start_event_id)?;
        validate_answer_ids(answers.iter().map(String::as_str))?;
        let content = UnstablePollResponseEventContent::new(answers, event_id);
        let result = self.inner.send(content).with_transaction_id(transaction_id.into()).await?;
        Ok(result.response.event_id.to_string())
    }

    /// Edit an MSC3381 poll directly and return the edit event's ID.
    ///
    /// Always pass the original poll start ID, including for successive edits,
    /// never a previous edit's ID. Retain the IDs of remaining answers. The SDK
    /// loads the target if necessary and checks its author and event type. The
    /// caller checks whether editing is allowed by the poll's current state;
    /// no responses or end events are fetched. Retries must reuse the payload
    /// and transaction ID. Normal SDK encryption applies and errors propagate.
    /// An empty transaction ID is rejected before any network requests.
    pub async fn edit_poll_with_transaction_id_returning_event_id(
        &self,
        poll_start_event_id: String,
        poll_data: DirectPollData,
        transaction_id: String,
    ) -> Result<String, ClientError> {
        validate_transaction_id(&transaction_id)?;
        let event_id = EventId::parse(poll_start_event_id)?;
        let (fallback_text, new_content) = poll_data.into_content()?;
        let content = self
            .inner
            .make_edit_event(&event_id, EditedContent::PollStart { fallback_text, new_content })
            .await?;
        let result = self.inner.send(content).with_transaction_id(transaction_id.into()).await?;
        Ok(result.response.event_id.to_string())
    }

    /// End an MSC3381 poll directly and return the end event's ID.
    ///
    /// `poll_start_event_id` must identify the original poll start. `text` is
    /// the fallback text for clients without poll support. The caller checks
    /// the poll's state and permissions; no responses or end events are fetched.
    /// Retries must reuse the payload and transaction ID. Normal SDK encryption
    /// applies and send errors propagate. Success means server acceptance;
    /// the SDK's aggregation determines the resulting poll state.
    /// An empty transaction ID is rejected before any network requests.
    pub async fn end_poll_with_transaction_id_returning_event_id(
        &self,
        poll_start_event_id: String,
        text: String,
        transaction_id: String,
    ) -> Result<String, ClientError> {
        validate_transaction_id(&transaction_id)?;
        let event_id = EventId::parse(poll_start_event_id)?;
        let content = UnstablePollEndEventContent::new(text, event_id);
        let result = self.inner.send(content).with_transaction_id(transaction_id.into()).await?;
        Ok(result.response.event_id.to_string())
    }
}

#[cfg(test)]
mod tests;
