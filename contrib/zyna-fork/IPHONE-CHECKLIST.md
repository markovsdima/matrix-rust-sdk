# Diagnostic SDK integration and iPhone validation

This is the full checklist for the candidate after the focused/echo follow-up and
for future updates. Results from October 18–19, 2026, and untested paths are listed
in [ACCEPTANCE.md](ACCEPTANCE.md). A build from the final commit needs the short
identity/smoke check below; the entire investigation need not be repeated when
code is unchanged. Package integration and application changes are handled
separately in the Zyna validation workflow.

## Final artifact built from a commit

1. Check the manifest/checksum and the application's runtime `sdkGitSha()`. Ensure
   the application and Notification Service use the same package and matching
   bindings.
2. Open the large history and attachments, wait for processing, leave, and return.
   Check batch errors and final content. If a rebuild occurs, save its aggregates;
   no lag during a short smoke run is not a new recovery check.
3. With a second client, check sending, sequential edits, reaction addition and
   removal, and message deletion. A deleted message must not return after history
   updates or reopening the chat. Use Zyna with the reaction-queue and deletion
   presentation fixes.
4. If the existing DEBUG scenario is available on iPhone, check delayed deletion
   acknowledgment with the waiting placeholder. Do not add a new testing
   architecture for this check. A successful new deletion does not conclusively
   explain the original rare cell reappearance.
5. Record passed/failed/not exercised, the commit, checksum, and Zyna identity.
   Any new state rollback or duplication requires investigation before publication.
   With unchanged SDK code, this is a packaging/integration smoke check, not another
   controlled performance A/B.

## Establish identity first

From `package-result.json`, obtain the absolute package path, expected
`sdkGitSha()`, source fingerprint, iOS target/profile/features, and
`libmatrix_sdk_ffi.a` SHA256. For an uncommitted candidate, `sdkGitSha()` may contain
a diagnostic ID such as `e426f04f-zyna-dirty-<contenthash>`: it is **not a Git
commit**; the accompanying manifest establishes the correspondence. Publication
requires a build from the selected commit.

Verify the file checksum, connect the local package to `Zyna` and
`ZynaNotificationService`, and build Zyna Debug for a physical iPhone with the
Rust release SDK. Preserve the previous dependency for rollback. Check in the
running application:

```text
expression -l Swift -- import MatrixRustSDK
expression -l Swift -- MatrixRustSDK.sdkGitSha()
```

Include package identity, device model/iOS, application configuration, history
size, scenario, and action times in the report. Do not log message contents or
event IDs in the new diagnostics. Check that Rust tracing actually enables INFO
for `matrix_sdk_ui::timeline::cache_lag` and DEBUG for
`matrix_sdk::event_cache::focused_snapshot`. In current Zyna, `logLevel.debug`
already enables DEBUG for `matrix_sdk::event_cache`: this target is not one of
the SDK's immutable exclusions. An extra override is not currently required.
If the configuration changes, explicitly add
`matrix_sdk::event_cache::focused_snapshot` to `extraTargets` at `.debug` before
calling `initPlatform`, then restart the application. Do not enable global TRACE
just to collect aggregates.

## Scenarios

| Check | Actions | What to verify |
| --- | --- | --- |
| Large history | Open the chat and attachments, quickly load to the end, wait for processing, repeat | Per timeline: lag/skipped, rebuild start/end, snapshot size, operations/items, receipt work, and store reconciliation. Do not require exactly 2 operations when edits/redactions need effects |
| Exit after processing | Wait for empty queues, return to the room list | Queues reach zero, no write errors, observed objects are released; record memory separately without attributing it to individual patches |
| Ordinary Live without lag | Rapidly toggle one reaction; send two sequential edits; edit during an active send | After settling, exactly the intended state, without duplicate reactions/rows or an old edit returning. Compare with a second client |
| Pending message | Offline or on a poor connection, send a message, edit or delete it before completion, reconnect, and retry | Canceled messages do not return; dependent edits/redactions are not lost; no stuck or duplicate echo. Separately check sending without retry |
| Long Thread/Event | Receive new messages and edits/redactions from a second client while opening | Opening time, `locked_section_ms`, observable sync delay for this room; final edits/redactions and focus are preserved |
| Pinned and focus recovery | Change the pin set and edit/redact targets during opening/recovery | The requested focus is not replaced with live room history; repeated recovery does not restore stale state |
| Controls | Change read receipts/read marker and a member profile from another client under load | Values do not roll back after settling; receipt-store reconciliation stops for acknowledged keys |

The ordinary UI cannot force send response/sync ordering or guarantee repeated
lag. Deterministic orderings remain covered by SDK tests. On the phone, record
the lag actually observed and do not mark unexecuted scenarios as passed. If
Thread/Event/Pinned or marker APIs are not exposed by Zyna, record "unavailable
in the UI; covered by SDK tests" rather than adding application architecture
just for this run. Do not artificially block the callback.

## Interpreting results

- `locked_section_ms` excludes waiting for the room lock and the full subsequent
  repair. A small value does not rule out a long opening delay; measure both.
- `relation_targets` counts targets, not total SQL queries: relations are recursive.
- Receipt reconciliation may read the store repeatedly for one key while the
  payload is behind or the store is empty. Post-rebuild reconciliation counters
  matter as much as work inside the rebuild.
- The producer counts mutations before FFI batching/skip/filter. Swift callback
  counts, retained batches, and unique events measure different things; do not
  add them together.
- Record rapid switching with backlog separately. This is a known next task;
  fewer output operations do not guarantee a bound on the Swift queue.

The INFO record `Timeline recovery receipt reconciliation` on
`matrix_sdk_ui::timeline::cache_lag` contains `timeline_id`, `recovery_generation`,
`receipt_store_reads`, `receipt_store_matched`, `receipt_store_differed`,
`receipt_store_missing`, `receipt_store_read_us`, and matching `_total` fields.
There is one record per committed input batch, only if it read the store; initial
hydration is excluded. `missing` includes provider errors mapped to `None`.
Totals belong to the current generation: the next lag rebuild resets them, while
an ordinary `Clear` preserves them. `read_us` is the sum of I/O wait times, not
the full lock hold time. Records contain no event/user IDs, text, or receipt maps.

Return aggregate logs, measurement JSON, a passed/failed/not exercised scenario
table, and observations with the exact diagnostic ID. Highlight any data loss,
duplication, state rollback, hang, or noticeable sync delay as a blocking question
for SDK review. A device run without findings complements the regressions; it
does not replace them.
