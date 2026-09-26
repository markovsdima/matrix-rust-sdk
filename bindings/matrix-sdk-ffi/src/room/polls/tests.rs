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

use matrix_sdk::test_utils::mocks::MatrixMockServer;
use ruma::{events::room::encrypted::OriginalSyncRoomEncryptedEvent, room_id, serde::Raw};
use serde_json::{Value, json};
use wiremock::{
    Mock, Request, ResponseTemplate,
    matchers::{method, path_regex},
};

use super::{DirectPollData, PollAnswer, PollKind, Room};
use crate::error::ClientError;

const POLL_ID: &str = "$original:example.org";
const START: &str = "org.matrix.msc3381.poll.start";
const RESPONSE: &str = "org.matrix.msc3381.poll.response";
const END: &str = "org.matrix.msc3381.poll.end";
const TEXT: &str = "org.matrix.msc1767.text";

fn poll_data() -> DirectPollData {
    DirectPollData {
        question: "Lunch?".into(),
        answers: vec![
            PollAnswer { id: "soup-id".into(), text: "Soup".into() },
            PollAnswer { id: "salad-id".into(), text: "Salad".into() },
        ],
        max_selections: 2,
        poll_kind: PollKind::Disclosed,
    }
}

fn start_content() -> Value {
    json!({
        START: {
            "question": { TEXT: "Lunch?" },
            "answers": [
                { "id": "soup-id", TEXT: "Soup" },
                { "id": "salad-id", TEXT: "Salad" },
            ],
            "max_selections": 2,
            "kind": "org.matrix.msc3381.poll.disclosed",
        },
        TEXT: "Lunch?\n1. Soup\n2. Salad",
    })
}

fn edited_data() -> DirectPollData {
    let mut data = poll_data();
    data.question = "Dinner?".into();
    // Remove soup, retain the salad ID (even when changing its label), and add
    // a new caller-assigned ID. No matching by text or position is allowed.
    data.answers = vec![
        PollAnswer { id: "salad-id".into(), text: "Green salad".into() },
        PollAnswer { id: "pasta-id".into(), text: "Pasta".into() },
    ];
    data.max_selections = 1;
    data.poll_kind = PollKind::Undisclosed;
    data
}

fn edit_content() -> Value {
    json!({
        "m.new_content": {
            START: {
                "question": { TEXT: "Dinner?" },
                "answers": [
                    { "id": "salad-id", TEXT: "Green salad" },
                    { "id": "pasta-id", TEXT: "Pasta" },
                ],
                "max_selections": 1,
                "kind": "org.matrix.msc3381.poll.undisclosed",
            },
            TEXT: "Dinner?\n1. Green salad\n2. Pasta",
        },
        "m.relates_to": { "rel_type": "m.replace", "event_id": POLL_ID },
    })
}

async fn setup(encrypted: bool) -> (MatrixMockServer, Room) {
    let server = MatrixMockServer::new().await;
    let client = server.client_builder().build().await;
    let room =
        Room::new(server.sync_joined_room(&client, room_id!("!polls:example.org")).await, None);
    if encrypted {
        server.mock_room_state_encryption().encrypted().mount().await;
        server.mock_get_members().ok(Vec::new()).mount().await;
    } else {
        server.mock_room_state_encryption().plain().mount().await;
    }
    (server, room)
}

fn original_event(room: &Room) -> Value {
    json!({
        "event_id": POLL_ID,
        "room_id": room.inner.room_id(),
        "sender": room.inner.own_user_id(),
        "origin_server_ts": 1,
        "type": START,
        "content": start_content(),
    })
}

async fn mock_original(server: &MatrixMockServer, event: Value) {
    Mock::given(method("GET"))
        .and(path_regex(r"/event/\$original:example.org$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(event))
        .mount(server.server())
        .await;
}

#[derive(Clone, Copy, Debug)]
enum Operation {
    Start,
    Response,
    Edit,
    End,
}

impl Operation {
    fn transaction_id(self) -> &'static str {
        match self {
            Self::Start => "start-txn",
            Self::Response => "response-txn",
            Self::Edit => "edit-txn",
            Self::End => "end-txn",
        }
    }

    fn event_type(self) -> &'static str {
        match self {
            Self::Start | Self::Edit => START,
            Self::Response => RESPONSE,
            Self::End => END,
        }
    }

    fn content(self) -> Value {
        match self {
            Self::Start => start_content(),
            Self::Response => json!({
                RESPONSE: { "answers": ["salad-id", "soup-id"] },
                "m.relates_to": { "rel_type": "m.reference", "event_id": POLL_ID },
            }),
            Self::Edit => edit_content(),
            Self::End => json!({
                END: {}, TEXT: "Voting ended",
                "m.relates_to": { "rel_type": "m.reference", "event_id": POLL_ID },
            }),
        }
    }

    async fn send(self, room: &Room) -> Result<String, ClientError> {
        self.send_with_transaction_id(room, self.transaction_id().into()).await
    }

    async fn send_with_transaction_id(
        self,
        room: &Room,
        txn: String,
    ) -> Result<String, ClientError> {
        match self {
            Self::Start => {
                room.send_poll_start_with_transaction_id_returning_event_id(poll_data(), txn).await
            }
            Self::Response => {
                room.send_poll_response_with_transaction_id_returning_event_id(
                    POLL_ID.into(),
                    vec!["salad-id".into(), "soup-id".into()],
                    txn,
                )
                .await
            }
            Self::Edit => {
                room.edit_poll_with_transaction_id_returning_event_id(
                    POLL_ID.into(),
                    edited_data(),
                    txn,
                )
                .await
            }
            Self::End => {
                room.end_poll_with_transaction_id_returning_event_id(
                    POLL_ID.into(),
                    "Voting ended".into(),
                    txn,
                )
                .await
            }
        }
    }
}

const OPERATIONS: [Operation; 4] =
    [Operation::Start, Operation::Response, Operation::Edit, Operation::End];

async fn assert_request(
    room: &Room,
    request: &Request,
    operation: Operation,
    encrypted: bool,
    event_id: &str,
) {
    let event_type = if encrypted { "m.room.encrypted" } else { operation.event_type() };
    assert!(
        request.url.path().ends_with(&format!("/send/{event_type}/{}", operation.transaction_id())),
        "{}",
        request.url
    );
    let content: Value = request.body_json().unwrap();
    if encrypted {
        assert_eq!(content["algorithm"], "m.megolm.v1.aes-sha2");
        assert!(content.get(START).is_none());
        let event: Raw<OriginalSyncRoomEncryptedEvent> = Raw::new(&json!({
            "event_id": event_id,
            "sender": room.inner.own_user_id(),
            "origin_server_ts": 100,
            "type": "m.room.encrypted",
            "content": content,
        }))
        .unwrap()
        .cast_unchecked();
        let decrypted = room.inner.decrypt_event(&event, None).await.unwrap();
        let plaintext: Value = serde_json::from_str(decrypted.raw().json().get()).unwrap();
        assert_eq!(plaintext["type"], operation.event_type());
        assert_eq!(plaintext["content"], operation.content());
    } else {
        assert_eq!(content, operation.content());
    }
}

async fn direct_poll_flow(encrypted: bool) {
    let (server, room) = setup(encrypted).await;
    // In an encrypted room, exercise fetching and decrypting the original poll
    // for make_edit_event as well as encrypting the outgoing events.
    for operation in OPERATIONS {
        let event_id = if matches!(operation, Operation::Start) {
            POLL_ID.to_owned()
        } else {
            format!("${}:example.org", operation.transaction_id())
        };
        let send_mock = Mock::given(method("PUT"))
            .and(path_regex(format!("/send/[^/]+/{}$", operation.transaction_id())))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "event_id": event_id })))
            .expect(2)
            .mount_as_scoped(server.server())
            .await;

        // No Timeline is constructed. Each operation is retried with exactly
        // the same caller data and transaction ID after server acceptance.
        assert_eq!(operation.send(&room).await.unwrap(), event_id);
        assert_eq!(operation.send(&room).await.unwrap(), event_id);

        let requests = server.server().received_requests().await.unwrap();
        let sends: Vec<_> = requests
            .iter()
            .filter(|r| {
                r.url.path().contains("/send/")
                    && r.url.path().ends_with(operation.transaction_id())
            })
            .collect();
        assert_eq!(sends.len(), 2);
        for request in &sends {
            assert_request(&room, request, operation, encrypted, &event_id).await;
        }
        if matches!(operation, Operation::Start) {
            let mut event = original_event(&room);
            if encrypted {
                event["type"] = json!("m.room.encrypted");
                event["content"] = sends[0].body_json().unwrap();
                // Reuse the event ID and timestamp when the SDK
                // decrypts this message again (Megolm replay protection).
                event["origin_server_ts"] = json!(100);
            }
            mock_original(&server, event).await;
        }
        drop(send_mock);
    }
}

#[tokio::test]
async fn direct_polls_send_and_retry_without_timeline() {
    direct_poll_flow(false).await;
}

#[tokio::test]
async fn direct_polls_encrypt_and_decrypt_all_four_events() {
    direct_poll_flow(true).await;
}

#[test]
fn direct_poll_data_validates_answer_ids_count_and_selection_limit() {
    for count in [0, 1, 20, 21] {
        let mut data = poll_data();
        data.answers = (0..count)
            .map(|i| PollAnswer { id: format!("id-{i}"), text: format!("Answer {i}") })
            .collect();
        assert_eq!(data.into_content().is_ok(), (1..=20).contains(&count));
    }
    let mut data = poll_data();
    data.answers[1].id = data.answers[0].id.clone();
    assert!(data.into_content().unwrap_err().to_string().contains("unique"));
    let mut data = poll_data();
    data.answers[0].id.clear();
    assert!(data.into_content().unwrap_err().to_string().contains("empty"));
    let mut data = poll_data();
    data.max_selections = 0;
    assert!(data.into_content().unwrap_err().to_string().contains("at least 1"));
    // The SDK imposes no UI policy requiring two options or clamping the
    // selection limit to the answer count.
    let mut data = poll_data();
    data.max_selections = u8::MAX;
    assert!(data.into_content().is_ok());
}

#[tokio::test]
async fn direct_polls_propagate_send_errors_and_allow_retry() {
    let (server, room) = setup(false).await;
    mock_original(&server, original_event(&room)).await;
    for operation in OPERATIONS {
        let failure =
            Mock::given(method("PUT"))
                .and(path_regex("/send/"))
                .respond_with(ResponseTemplate::new(403).set_body_json(
                    json!({ "errcode": "M_FORBIDDEN", "error": "Cannot send poll" }),
                ))
                .expect(1)
                .mount_as_scoped(server.server())
                .await;
        let error = operation.send(&room).await.unwrap_err();
        assert!(
            matches!(error, ClientError::MatrixApi { ref code, ref msg, .. } if code == "M_FORBIDDEN" && msg == "Cannot send poll"),
            "{error:?}"
        );
        drop(failure);

        let success = Mock::given(method("PUT"))
            .and(path_regex("/send/"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "event_id": "$retried:example.org" })),
            )
            .expect(1)
            .mount_as_scoped(server.server())
            .await;
        assert_eq!(operation.send(&room).await.unwrap(), "$retried:example.org");
        drop(success);
        let requests = server.server().received_requests().await.unwrap();
        let sends: Vec<_> = requests
            .iter()
            .filter(|r| {
                r.url.path().contains("/send/")
                    && r.url.path().ends_with(operation.transaction_id())
            })
            .collect();
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[0].url, sends[1].url);
        assert_eq!(sends[0].body_json::<Value>().unwrap(), sends[1].body_json::<Value>().unwrap());
    }
}

#[tokio::test]
async fn direct_polls_reject_empty_transaction_ids_before_network_requests() {
    for encrypted in [false, true] {
        let (server, room) = setup(encrypted).await;
        let initial_request_count = server.server().received_requests().await.unwrap().len();
        for operation in OPERATIONS {
            let error = operation.send_with_transaction_id(&room, String::new()).await.unwrap_err();
            let ClientError::Generic { msg, .. } = error else {
                panic!("{operation:?}, encrypted={encrypted}: {error:?}");
            };
            assert_eq!(
                msg, "transactionId must not be empty",
                "{operation:?}, encrypted={encrypted}"
            );
            assert_eq!(
                server.server().received_requests().await.unwrap().len(),
                initial_request_count,
                "{operation:?}, encrypted={encrypted} must fail before making any requests"
            );
        }
    }
}

#[tokio::test]
async fn direct_polls_reject_invalid_input_before_sending() {
    let (server, room) = setup(false).await;
    let mut invalid = poll_data();
    invalid.max_selections = 0;
    assert!(
        room.send_poll_start_with_transaction_id_returning_event_id(invalid.clone(), "txn".into())
            .await
            .is_err()
    );
    assert!(
        room.edit_poll_with_transaction_id_returning_event_id(
            POLL_ID.into(),
            invalid,
            "txn".into()
        )
        .await
        .is_err()
    );
    assert!(
        room.edit_poll_with_transaction_id_returning_event_id(
            "invalid".into(),
            poll_data(),
            "txn".into()
        )
        .await
        .is_err()
    );
    assert!(
        room.send_poll_response_with_transaction_id_returning_event_id(
            "invalid".into(),
            vec![],
            "txn".into()
        )
        .await
        .is_err()
    );
    assert!(
        room.end_poll_with_transaction_id_returning_event_id(
            "invalid".into(),
            "End".into(),
            "txn".into()
        )
        .await
        .is_err()
    );
    for answers in [vec![String::new()], vec!["same".into(), "same".into()]] {
        assert!(
            room.send_poll_response_with_transaction_id_returning_event_id(
                POLL_ID.into(),
                answers,
                "txn".into()
            )
            .await
            .is_err()
        );
    }
    assert!(
        !server.server().received_requests().await.unwrap().iter().any(|r| r
            .url
            .path()
            .contains("/send/")
            || r.url.path().contains("/event/"))
    );
}

#[tokio::test]
async fn direct_poll_response_allows_clearing_a_vote() {
    let (server, room) = setup(false).await;
    server
        .mock_room_send()
        .body_matches_partial_json(json!({ RESPONSE: { "answers": [] } }))
        .ok(ruma::event_id!("$cleared:example.org"))
        .mock_once()
        .mount()
        .await;
    assert_eq!(
        room.send_poll_response_with_transaction_id_returning_event_id(
            POLL_ID.into(),
            vec![],
            "clear-txn".into()
        )
        .await
        .unwrap(),
        "$cleared:example.org"
    );
}

#[tokio::test]
async fn direct_poll_edit_preserves_sdk_author_type_and_fetch_errors() {
    for case in ["author", "type", "fetch"] {
        let (server, room) = setup(false).await;
        let mut event = original_event(&room);
        match case {
            "author" => event["sender"] = json!("@someone-else:example.org"),
            "type" => {
                event["type"] = json!("m.room.message");
                event["content"] = json!({ "msgtype": "m.text", "body": "Not a poll" });
            }
            _ => {}
        }
        if case == "fetch" {
            server
                .mock_room_event()
                .ok_with_template(
                    ResponseTemplate::new(404).set_body_json(
                        json!({ "errcode": "M_NOT_FOUND", "error": "Missing poll" }),
                    ),
                )
                .mock_once()
                .mount()
                .await;
        } else {
            mock_original(&server, event).await;
        }
        let error = Operation::Edit.send(&room).await.unwrap_err().to_string();
        let expected = match case {
            "author" => "not the author",
            "type" => "isn't the same",
            _ => "Couldn't fetch",
        };
        assert!(error.contains(expected), "{case}: {error}");
        assert!(
            !server
                .server()
                .received_requests()
                .await
                .unwrap()
                .iter()
                .any(|r| r.url.path().contains("/send/"))
        );
    }
}

#[tokio::test]
async fn successive_poll_edits_always_reference_original_start() {
    let (server, room) = setup(false).await;
    mock_original(&server, original_event(&room)).await;
    for (txn, question) in [("first-edit", "First edit?"), ("second-edit", "Second edit?")] {
        let mut data = edited_data();
        data.question = question.into();
        let guard = server
            .mock_room_send()
            .body_matches_partial_json(json!({
                "m.relates_to": { "rel_type": "m.replace", "event_id": POLL_ID },
                "m.new_content": { START: { "question": { TEXT: question } } },
            }))
            .ok(ruma::OwnedEventId::try_from(format!("${txn}:example.org")).unwrap())
            .mock_once()
            .mount_as_scoped()
            .await;
        assert_eq!(
            room.edit_poll_with_transaction_id_returning_event_id(POLL_ID.into(), data, txn.into())
                .await
                .unwrap(),
            format!("${txn}:example.org")
        );
        drop(guard);
    }
}
