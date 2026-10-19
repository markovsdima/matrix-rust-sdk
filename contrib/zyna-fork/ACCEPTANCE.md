# Cache lag / rebuild acceptance — October 19, 2026

The diagnostic candidate is accepted for release preparation with the limitations
below. This is not a claim that all possible defects have been ruled out.
Publication requires the owner's diff review and commits, a build from a clean
commit, and validation of the final artifact in Zyna. The latest iOS runs do not
require further Rust changes.

## Identity

- Base: `e426f04f6e2290698deb83638d0b967569c5886f`.
- Runtime SDK: `e426f04f-zyna-dirty-a90e2aadf789` — a diagnostic ID, not a Git commit.
- Source snapshot fingerprint:
  `a90e2aadf78982fe3924ad79ea3ef0cca0a15050692daaba145f8b1648ca03f4`.
- Rust release, default features disabled; Apple features:
  `sqlite,sentry,unstable-msc4274,experimental-element-recent-emojis,experimental-push-secrets,experimental-search`.
- Local XCFramework: arm64 iPhone and x86_64 simulator, iOS deployment target 17.0.
  This is not the full platform set for the public Swift package.
- iPhone `libmatrix_sdk_ffi.a` SHA256:
  `5ab2c95300a724ce0bb46c910f6205569afd70b012bc8052edc79c23ff0cd2da`.
- Diagnostic XCFramework ZIP SHA256 / SPM checksum:
  `636c996847855dc28777fc9e739d420aecb03a3befa11c5666fab71f09d9f329`.

On October 19, all 1000 files outside `contrib` in the source manifest matched the
working tree byte for byte. Only documentation changed after the device runs.
The new commit and its binary checksum will differ; the diagnostic fingerprint
must not be presented as the final artifact's identity.

## SDK checks

These tests ran on the candidate sources on October 18. On October 19, their
results were checked against the saved reports; the tests were not rerun:

| Suite | Result |
| --- | --- |
| UI unit | 387 passed; one ignored volume fixture was checked separately |
| Timeline integration | 144 passed; one ignored diagnostic test was not run, and 37 unrelated tests were filtered out |
| Event-cache unit | 72 passed |
| SDK integration | 57 event-cache + 9 pinned + 4 thread + 40 send-queue = 110 passed |
| Static checks | Host/wasm UI Clippy with `-D warnings` and SDK wasm check passed; no execution in a browser |
| Volume regression | Six cases at 1000/17000 events, ordinary timeline and attachments with receipts off/on: previous counters and final state preserved; one run each, not a timing benchmark |
| Apple packaging | SQLite symbol checks and Swift typecheck passed for both slices |

Commands and required invariants are in [MAINTENANCE.md](MAINTENANCE.md). The full
list of passing tests is `sdk/passed-regressions.txt` inside the
[portable evidence archive](evidence/README.md).
The first send-queue run was blocked by the sandbox's loopback restriction; the
run with localhost access passed all 40 tests. Validation covers the complete
change set; intermediate commit prefixes were not built separately.

Separate attribution of the optimizations remains based on the earlier controlled
comparisons on `12ae23d`, described in [README.md](README.md). Do not extrapolate
their host timings to iPhone or convert item occurrences into megabytes of memory.

## Zyna checks

The evidence comes from application validation reports and the saved reaction
verification JSON. SDK acceptance reviewed those results without rerunning the
XCTest cases or independently auditing all Swift fixes. The same diagnostic SDK
ID was confirmed on both devices.
Configuration: Swift Debug / Rust release; iPhone 16 Pro Max / iOS 26.3 and Intel
iPhone 17 Pro simulator / iOS 26.3.1.

| Scenario | Verified result | Scope of the evidence |
| --- | --- | --- |
| Large iPhone rebuild | 10 rebuilds, snapshots 8524–20679; eight with 2 operations, two with 5; fallback/compute visits 0 | Not an old/new A/B; 2 operations is not a universal guarantee when semantic effects are required |
| Two ordinary attachment cycles | Queues drained and observed objects were released | Does not prove that every global cache is empty or every accepted write was committed |
| Rapid exit | Large Swift backlog reproduced; queues eventually drained and observed objects were released | Processing after exit remains; a separate next task |
| Offline / automatic resume | After 30 seconds offline, sending completed without Retry; both clients saw exactly one remote record | The app stayed alive; this UI run did not test SDK SendHandle, manual Retry, or arbitrary send/sync ordering |
| Ordinary receipts | Read appeared while the profile was open and persisted after returning | This run did not cover receipt-store reconciliation, fully-read markers, or member updates |
| Live edits and catalogue | Sequential text edits; caption edit/removal with the profile open; photo deletion reduced the catalogue from 2143→2142 | Not a forced concurrent rebuild or an in-flight SDK SendHandle check |
| Repeated reactions after the Zyna fix | 13 prepared=true; 7 add accepted; 7/6 taps → 1/0 reactions on both clients; IDs preserved | The sequence resumed after restarts/XCTest failures; intervals were 4.59–14.69 s, not a subsecond burst; stable for 28/31 s |

In the new reaction sessions, failedBatches=0, lag/rebuild=0, and reconciliation
reads=0. These are not new rebuild checks or measurements of an unexecuted path.

## Zyna fixes and the remaining observation

Zyna's reaction queue lost an accepted reaction ID when a stale ChatMessage caused
another add. The fix stores desiredPresence separately from the current operation.
The original stale-UI situation recurred in a live run with the correct result;
the report records 38 local tests passing in four suites. This is a separate Zyna
change, not an SDK benefit. It does not restore previously lost IDs or add
remote-only ID recovery.

A separate controlled test reproduced old text returning after a delayed deletion
acknowledgment: after a waiting placeholder, a saved animation preview reentered
the model. The old code failed the negative control; the fixed code passed,
including checks using a real Texture controller (38 tests / four suites). Three
live deletions were correct. Photo post-send scrolling was also fixed: simulator
offset 332.6667 pt → 0 pt, with the complete repeated scenario passing. These
presentation checks ran on the simulator, not a physical iPhone.

It is **not proven** that the preview mechanism caused the original rare live
cell reappearance: the earlier logs are insufficient. The confirmed Zyna defect
is fixed; the cause of the original episode remains unconfirmed. No evidence
establishes an SDK defect. This is an accepted release limitation, not a closed
attribution. If reappearance recurs on fixed Zyna, correlate SDK diffs, SQLite,
the model, and Texture's committed state before publishing a new artifact.

## Remaining publication requirements

- Owner reviews the staged diff and creates commits. The three groups are
  described in [RELEASE.md](RELEASE.md); historical `contrib/timeline-*` materials
  are excluded from the commit set.
- Choose actual Rust/Swift tags and final changelog fragment names.
- Build from a clean commit without a diagnostic SHA override; verify the required
  platforms, SQLite symbols, bindings, ZIP checksum, and manifest.
- Check the final artifact's identity/smoke following
  [IPHONE-CHECKLIST.md](IPHONE-CHECKLIST.md).

Receipt-store I/O under the timeline write lock and focused relation reads under
the room write lock remain unmeasured on iPhone. Current Zyna screens do not use
focused Thread/Event/Pinned timelines or SDK SendHandle; their coverage here is
SDK tests. There are no new memory/energy A/B results or proof that the entire
rebuild is linear. These limitations do not require expanding the application or
SDK architecture for this release.

## Acceptance sources

The [portable evidence archive](evidence/README.md) contains:

- `sdk/`: candidate regression results, passed test names, volume checks, source
  fingerprint, and diagnostic package metadata.
- `controlled/`: the earlier four-variant measurements, negative control, methods,
  and patches needed to reconstruct the benchmark sources.
- `device/iphone-aggregates.json`: selected original numeric recovery counters.
- `device/reaction-verification.json`: the recorded action counts, accepted
  operations, final states, and the resumed-run caveat.
- `device/report-provenance.json`: hashes identifying the private application
  reports reviewed for acceptance. Those full reports are not included.

The archive manifest identifies original input hashes and transformations. Saved
records remain historical, including expected FAIL results and checks pending at
the time. This document connects later results without rewriting that history.
Complete user logs are not public release assets.
