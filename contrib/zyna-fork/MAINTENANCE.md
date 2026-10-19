# Updating the fork's base

## Keep commits cohesive

One coherent fix with tests is preferable to several broken intermediate commits.
For this change set, use one main commit and two small preparatory commits:

1. **Wasm compatibility in the redecryptor.** An independent yield replacement with
   its own compilation check; do not present it as part of the cache lag fix.
2. **An existing test fixture correction.** `test_edits_touches_threads` was shown
   to fail on the original `e426f04f`; keep this test correction separate from the
   new product fixes.
3. **The main timeline changes.** Coordinated snapshots/subscriptions,
   Live/Event/Thread/Pinned state, receipts/markers/profiles, local echo and thread
   repairs, pending receipt search, coalescing with catalogue effects, diagnostics,
   regressions, and documentation. These parts overlap in implementation and
   together protect recovery.

Describe the reasons for the changes and the measured contribution of the two
optimizations separately, even within one main commit. There is no need to split
individual lines of a shared file into separate commits mechanically. Future
adaptation can address a specific part of this change set and its tests. Keep the
history of temporary experiments out of the product.

These groups describe the intended split, not proof that commits exist. Check the
actual diffs and dependencies before staging; publish only after the fork owner's
decision. Do not add an opt-in API for a hypothetical upstream PR. A new setting
adds another supported mode and needs a product requirement to justify it.

## When updating the SDK

1. Preserve published tags, release assets, and the manifest. Do not rewrite a SHA
   already referenced by the Swift package. Create a separate integration branch
   for the new base and keep Zyna's working package available for rollback.
2. Identify the exact current and target upstream revisions. Choose a merge or
   replay the fork's commits onto the new base according to repository history.
   Rebasing alone does not reduce semantic conflicts; preserve the purpose of each
   change.
3. For each row in the README's map, decide whether upstream now provides the same
   invariant, the fork's code is still needed, or adaptation is required. Do not
   retain two parallel mechanisms or remove a patch just because upstream added
   something with a similar name.
4. Check mutation/drain/send sites and async cancellation; room/focus/thread lock
   order; and the separation of positional events from relations. Snapshot events
   do not include all control-channel state. Check send/sync reconciliation and
   receipts without `ts`.
5. Run the regressions below, then the volume fixture. A clean merge and successful
   compilation do not prove that diff order or catalogue effects are preserved.
6. Build a local Apple candidate from a separate copy, verify symbols and identity,
   and run the iPhone checklist. Only then update the Swift package release.

## Required checks

Run these commands in the copy being validated. `--offline` is appropriate when
dependencies are already available locally. Tests using mock HTTP require
loopback access. Do not run builds concurrently with volume or timing comparisons.
Record the toolchain, features, profile, and exit codes.

```sh
CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk-ui --lib
CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk-ui --test integration timeline::
CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk --lib --features testing event_cache::
CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk --test integration --features testing event_cache::
CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk --test integration --features testing room::pinned_events::
CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk --test integration --features testing room::thread::
CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk --test integration --features testing send_queue::
RUST_LOG=off CARGO_INCREMENTAL=0 cargo test --offline --release -p matrix-sdk-ui --lib cache_rebuild_volume -- --ignored --nocapture --test-threads=1
cargo clippy --offline -p matrix-sdk-ui --all-targets -- -D warnings
cargo clippy --offline --target wasm32-unknown-unknown -p matrix-sdk-ui --features js -- -D warnings
cargo check --offline --target wasm32-unknown-unknown -p matrix-sdk --no-default-features --features js
```

SDK send-queue tests are required for echo changes: timeline integration tests do
not replace checks of dependent operations during HTTP sends. List the tests that
actually passed in `validation.json`; a substring filter is not necessarily a
complete suite.

Required scenarios include repeated lag and concurrent pagination/sync; exact
event counts and remote metadata without duplicate replay; uniqueness after every
positional diff; skip/filter/raw consumers; redactions, edits/reactions, late keys;
receipts/read markers and member updates; local → remote echo in both send/sync
orders; focused cache repair and existing subscribers. Some of these checks live
in `controller/cache_lag_repro.rs`, `focused_recovery_tests`, and the event-cache
snapshot tests.

The wasm check verifies compilation, not execution in a browser. Fixture
performance measures a controlled synthetic scenario, not the complete
SQLite/FFI/Swift/iPhone pipeline.
