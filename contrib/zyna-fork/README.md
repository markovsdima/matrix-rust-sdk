# Maintaining Zyna's timeline changes

This directory maps the fork's cache lag / rebuild changes and Apple SDK release
workflow. An upstream submission is not planned. The goal is to preserve the
behavior Zyna needs when updating the SDK base, with clear dependencies and tests.

- [Updating the base and keeping changes cohesive](MAINTENANCE.md).
- [Preparing commits, artifacts, and releases](RELEASE.md).
- [iOS handoff and iPhone checks](IPHONE-CHECKLIST.md).
- [Diagnostic Apple build instructions](BUILD.md).
- [Candidate acceptance and remaining release requirements](ACCEPTANCE.md).
- [Portable validation evidence and reproduction](evidence/README.md).

## Invariants to preserve

| Change | Required behavior | Main code locations |
| --- | --- | --- |
| Snapshot/receiver boundary | Snapshot data is neither lost nor applied twice; mutation, diff draining, and publication are coordinated | `matrix-sdk/src/event_cache/caches/room`, `event_focused`, `thread` |
| State recovery | Restore receipts, the read marker, profiles, pending local effects, and focus state together with events; an event array alone is insufficient | `matrix-sdk-ui/src/timeline/tasks.rs`, `controller/{mod,state,metadata,read_receipts}.rs` |
| Local/remote echo | Reconcile edits, reactions, and redactions in both send/sync orders; keep the latest pending edit visible | `controller/aggregations.rs`, `event_handler.rs` |
| Coalescing one rebuild | Emit the completed state of an ordinary rebuild; preserve edit/redaction effects required by the attachment catalogue | `controller/observable_items.rs` |
| Pending receipt targets | Do not scan items for a target that has not been inserted; give the new item its receipt when it is created | `controller/read_receipts.rs` |
| Diagnostics | Numeric aggregates without message contents or retained diagnostic snapshots | `timeline/tasks.rs`, `controller`, `event_cache/caches/room/focused.rs` |

Paths in the table are relative to `crates/`. These changes are related: simply
resubscribing cannot replace the cache boundary, and coalescing cannot emit only a
visible snapshot while dropping effects for attachments that disappeared. Ordinary
live updates do not use rebuild coalescing. Echo reconciliation and pending receipt
search changes also apply without lag.

## Evidence and its revision

Results apply to the sources and binaries identified below. The current candidate,
`e426f04f-zyna-dirty-a90e2aadf789`, passed SDK checks and Zyna runs on October 18–19,
2026; their scope is recorded in [ACCEPTANCE.md](ACCEPTANCE.md). This is a diagnostic
ID, not a published commit. A build from the final clean commit and identity/smoke
checks of that artifact remain release requirements.

| Source | What was actually checked |
| --- | --- |
| Original fork `e426f04f6e2290698deb83638d0b967569c5886f` → diagnostic base `12ae23d2a1bbbada53a8f1da400cecf28e8a17ca` | Strict negative control at N=200/1000, two lags: the old code replayed 32 retained prepends per cycle; the fixed code preserved exact events/IDs/metadata |
| Controlled comparisons on `12ae23d`, host release | Four variants with identical correctness fixes; 1000/17000 events, three modes, one warm-up plus five interleaved measured runs; a separate live control |
| Follow-up on 2026-10-17 | 386 UI unit, 144 timeline integration, 72 event-cache unit, and 70 targeted SDK integration tests passed; host/wasm static checks passed. This validates the earlier source snapshot, not subsequent edits automatically |
| Reported Zyna iPhone run, SDK `12ae23d` | Five rebuilds, snapshots 8574–19643, operations 2/2/2/2/5; fallback/compute visits 0. The later focused/echo changes were not in this binary |
| Current candidate, source snapshot `a90e2aadf789…`, 2026-10-18 | 387 UI unit, 144 timeline integration, 72 event-cache unit, 110 SDK integration tests; host/wasm static checks. Six volume fixtures preserved counters and final state; this was not a new timing benchmark |
| Zyna on the current candidate, 2026-10-18/19 | Ten rebuilds, snapshots 8524–20679, eight with 2 operations and two with 5; fallback/compute visits 0. Offline/automatic resume and ordinary receipts passed. Reaction and presentation defects were fixed separately in Zyna, without Rust changes |

From the controlled series on `12ae23d` (ordinary timeline, 17000 events):

| Individual change | Measured result | Scope of the conclusion |
| --- | --- | --- |
| Rebuild coalescing | 34001 → 2 operations; 34000 → 17001 item occurrences; 16999 → 0 receipt Sets | Producer median 293.193 → 304.048 ms: no producer speedup was demonstrated. Swift consumer speedup was not measured |
| Pending receipt search with coalescing enabled | 144491500 → 0 fallback visits; median 1042.398 → 304.048 ms | Host release, this specific fixture; position visits remain |
| Attachments, receipts off | 2127 → 2 operations; 2126 → 2126 item occurrences | Item occurrences are not halved; there is no receipt search to speed up in this mode |

These fixtures have one sender, an image every eighth event, one day, and one
virtual item. Two operations is the result for fixtures without extra semantic
effects, not a guarantee for every rebuild. Item occurrences are not unique
messages or megabytes; host timing does not predict iPhone timing. Removing one
quadratic section does not prove that the entire rebuild is linear.

The [evidence archive](evidence/README.md) preserves raw controlled measurements,
variant patches, SDK regression results, and selected device aggregates. It uses
relative paths and records which source records were normalized or summarized.
Private application logs, local build products, and temporary checkout paths are
not required to read the evidence. Earlier `contrib/timeline-*` working materials
remain historical and are excluded from the prepared commit set.

## Remaining limitations

- Event/Thread snapshots read relations under the room write lock, including on
  initial opening. `locked_section_ms` covers only the snapshot read/construction
  section, excluding lock acquisition and subsequent backing-cache repairs. It is
  logged at DEBUG on `matrix_sdk::event_cache::focused_snapshot`.
- After lag, receipt-store I/O remains under the timeline write lock. An
  unacknowledged key is reread on each delivery until it matches the store. A store
  value ahead of the payload, or a missing value, can cause repeated reads. This
  is not "one read per key."
- Preserving pending aggregates scans their map during rebuild; the cost for
  histories with many reactions has not been measured separately.
- Rapid switching can cause a Swift backlog and processing after leaving the
  screen. Pagination coordination, the history reader, and the inbox are separate
  tasks.
- A catalogue cannot infer the redaction of an event unknown to the SDK merely
  from its absence in the visible snapshot. Semantic effects cover known
  transitions, but do not replace a separate contract for durable catalogue
  synchronization.
