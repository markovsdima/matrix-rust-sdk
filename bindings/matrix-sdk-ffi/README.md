# FFI bindings for the rust matrix SDK

This uses [`uniffi`](https://mozilla.github.io/uniffi-rs/Overview.html) to build the matrix bindings for native support and wasm-bindgen for web-browser assembly support. Please refer to the specific section to figure out how to build and use the bindings for your platform.

## Features

Given the number of platforms targeted, we have broken out a number of features

### Functionality

- `sentry`: Enable error monitoring using Sentry, not supports on Wasm platforms.
- `sqlite`: Use SQLite for the session storage.
- `bundled-sqlite`: Use an embedded version of SQLite instead of the system provided one.
- `indexeddb`: Use IndexedDB for the session storage.

### Unstable specs

- `unstable-msc4274`: Adds support for gallery message types, which contain multiple media elements.

## Platforms

Each supported target should use features to build the relevant system. Here are some suggested feature flags for the major platforms:

- Android: `"bundled-sqlite,unstable-msc4274,sentry"`
- iOS: `"sqlite,unstable-msc4274,sentry"` when the app provides SQLite or SQLCipher.
- JavaScript/Wasm: `"indexeddb,unstable-msc4274"`

### Swift/iOS sync

TBD

### Direct polls

`Room` exposes four async throwing methods that wait for the homeserver and
return the ID of the event sent:

```swift
let data = DirectPollData(
    question: "Lunch?",
    answers: [
        PollAnswer(id: "soup-id", text: "Soup"),
        PollAnswer(id: "salad-id", text: "Salad")
    ],
    maxSelections: 1,
    pollKind: .disclosed
)
let pollId = try await room.sendPollStartWithTransactionIdReturningEventId(
    pollData: data, transactionId: startTransactionId
)
let responseId = try await room.sendPollResponseWithTransactionIdReturningEventId(
    pollStartEventId: pollId, answers: ["salad-id"], transactionId: responseTransactionId
)
let editId = try await room.editPollWithTransactionIdReturningEventId(
    pollStartEventId: pollId, pollData: updatedData, transactionId: editTransactionId
)
let endId = try await room.endPollWithTransactionIdReturningEventId(
    pollStartEventId: pollId, text: "Voting ended", transactionId: endTransactionId
)
```

These use the existing `org.matrix.msc3381.poll.*` format, normal SDK encryption,
and no Timeline or SDK send queue. `DirectPollData` accepts 1–20 answers with
nonempty, unique IDs and `maxSelections >= 1`. The existing `PollData` and
Timeline methods are unchanged. An empty response answer list clears a vote;
nonempty selections must contain nonempty, unique IDs. All four methods reject
an empty transaction ID before any network requests. Empty questions and answer
texts are allowed by this API; the app applies its own UI validation.

Persist the complete payload and transaction ID before the first send and reuse
both on retries. Every distinct operation needs its own transaction ID. Edits
retain IDs of remaining options and use new IDs for new options. Always pass the
original poll start ID to responses, edits, and endings, including successive
edits. The edit method preserves the SDK's author and event-type checks and may
fetch/decrypt the original event.

Edits can change how existing votes are counted. If a vote contains any answer
ID removed from the poll, aggregation discards that entire vote, including its
remaining valid selections. Reducing `maxSelections` keeps only the first
allowed number of selections in each otherwise valid vote. The vote events
remain in the room history; the app decides which edits to allow after voting
has begun.

The app owns the outgoing queue, optimistic state, and checks against the known
poll state and permissions. These methods do not fetch votes or poll endings to
check whether an action is currently allowed. Success means the server accepted
the event; use SDK aggregation to reconcile races with poll completion. Send,
encryption, validation, and edit preparation errors reach the caller.

These methods inherit the client's request configuration, including timeouts
and retries, set through `ClientBuilder.requestConfig`. They do not apply a
separate retry policy: one call may make multiple attempts before returning,
as well as requests needed to prepare encryption or fetch an edit's target.
Error mapping matches the existing direct-send APIs: recognized Matrix API
errors from sending retain their structured `ClientError.MatrixApi` form,
while network, validation, and edit preparation errors can be
`ClientError.Generic`. That generic variant alone does not distinguish
retryable failures from permanent ones.
