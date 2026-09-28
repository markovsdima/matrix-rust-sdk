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

### Inspecting retained timeline events

`Room.inspectTimelineEvent(eventId:)` inspects one event without creating a
Timeline, changing its focus, or paginating. It reads the room's event cache
first and fetches the event directly on a cache miss. Neither fetched nor
decrypted inspection results are written to the event cache. Undecrypted
message envelopes and cached message UTDs are retried using current SDK keys
and the client's decryption trust settings, with ordinary SDK key recovery.
Already decrypted cache entries are returned as stored, without decrypting
again or rechecking subsequently tightened trust settings. This preserves
previously decrypted data when local keys are no longer available.

Opening a Timeline and pagination retry only newly loaded cache events.
Pending work is coalesced per room, and UTD work normally runs ahead of
bounded encryption-info refresh batches. Explicit `retryDecryption` calls
use the supplied session IDs without scheduling unrelated refreshes.
Temporary cache errors retain work for up to three attempts with backoff;
a later key notification, reload, or explicit retry can schedule more work.
These changes keep the existing indexed UTD selection contract. They do not
add reconciliation of divergent memory/store copies or legacy cache repair.

```swift
let inspection = try await room.inspectTimelineEvent(eventId: eventId)
switch inspection.disposition {
case .visible:
    // Eligible under the default SDK policy, including thread replies.
    // Obtain an aggregated projection through the ordinary timeline flow.
    break
case .hidden:
    // A recognized event that is not a standalone timeline item.
    // Recheck the account, room, event, and current row before local cleanup.
    break
case .unableToDecrypt:
    // decryptionFailure is present, including when its cause is Unknown.
    break
case .indeterminate:
    // Preserve the candidate: there is insufficient classification evidence.
    break
}
```

The result uses `RawRoomEvent` to preserve the requested event's identity and
complete available JSON, including relations, replacement content, and
`unsigned`. This is sensitive account data, not diagnostic output. It is not
the replacement target, latest edit, or thread root. `visible` does not promise
presence in a live timeline or aggregate edits, reactions, or poll results.

The classification does not apply the app's custom timeline filter. For
example, a poll start or a supported state event can be `visible` under the
default SDK policy while remaining excluded by a message-only timeline.
Waiting for that filtered timeline will not necessarily produce a projection.
The app must choose a compatible projection source or specify a separate
hydration/filtering contract; absence from that timeline is not evidence for
deletion and does not change this inspection result to `hidden`.

Classification uses `default_event_filter` and the room's known version rules
after guarding against unsupported event/message types and malformed content.
Unknown types, unparseable events, and unavailable room-version rules are
`indeterminate`, never evidence for deletion. Redacted events follow the SDK
policy rather than inferring an original relation from missing content.
An established SDK decryption failure takes precedence over classification;
for supported encrypted message events, `EncryptedMessage` and `UtdCause`
retain the same level of detail as the existing timeline API, including trust
failures and unknown causes. For encrypted state UTDs, `decryptionFailure` is
`EncryptedMessage::Unknown`, without a typed session ID or cause; the original
event JSON is still preserved.
Encrypted state events are not decrypted by this API in this version, even if
another workspace crate enables `experimental-encrypted-state-events`.
Unresolved encrypted state envelopes are `indeterminate`; an established SDK
UTD remains `unableToDecrypt`. Inspection does not retry encrypted state UTDs
when keys become available. If normal SDK processing replaces the cached entry
with a decrypted state event, a subsequent inspection follows the normal
classification rules for that decrypted event.

Invalid or mismatching event/room identity, lookup failures, and errors that
prevent constructing the result throw `ClientError`. A 404, forbidden access,
or a timeout is never `hidden`. Requests inherit the client's timeout/retry
configuration. Cancellation uses normal UniFFI async cancellation and stops
the inspection's own work; already-triggered shared SDK key recovery may
continue. Inspection sends no messages, redactions, receipts, or typing events.

For Swift, generate bindings with `cargo xtask swift build-framework`. UniFFI
0.31 does not forward Swift task cancellation by itself; this build command
adds an inspection-specific bridge to its existing Rust future cancellation
API. The bridge does not change other SDK methods. Check the generated Swift
code against the matching XCFramework on macOS with
`python3 bindings/apple/Tests/TimelineEventInspection/check.py`; this exercises
actual calls, error propagation, and cancellation, including completion races.
Deterministic cancellation checks also verify that result and error buffers
allocated by the linked Rust library are freed exactly once, including when
cancellation arrives after a result is ready.

The app owns its bounded repair queue, retries, and protection against stale
async results. Only a confirmed `hidden` result can justify removing a matching
placeholder; preserve UTD, indeterminate, and failed candidates. If cleanup is
persisted, prevent late UTD writes from recreating that placeholder, while
allowing a later authoritative visible projection to supersede suppression.
