# Preparing a fork release

This stage prepares files, staging, a local candidate, and instructions. Commits,
tags, PRs, asset publication, and switching Zyna require a separate decision by the
owner. Even a temporary build copy does not need a commit: a diagnostic ID and
source manifest distinguish uncommitted candidates.

Diagnostic candidate acceptance and its explicit limitations are recorded in
[ACCEPTANCE.md](ACCEPTANCE.md). SDK production code has not changed since the
device runs; updating documentation does not turn the existing binary into a
build of a future commit.

## iPhone candidate

Build from a separate immutable copy following [BUILD.md](BUILD.md).
Use Rust `release`, `aarch64-apple-ios`, and the existing deployment target 17.0.
Apple features:
`sqlite,sentry,unstable-msc4274,experimental-element-recent-emojis,experimental-push-secrets,experimental-search`,
with FFI default features disabled. Generate the Swift bindings and XCFramework in
the same run. Do not mix old bindings with a new library.

For each library, verify that `_sqlite3_*` definitions are absent and external
references to them are present; the application provides SQLite/SQLCipher. Save
the check result, architectures, Xcode/Rust versions, and exact command. Validate
the Swift manifest and typecheck for the intended SDK. A device-only XCFramework
is suitable for this iPhone run, but is not automatically a complete public
artifact for all consumers.

Save `package-result.json`, the source manifest, library SHA256, build log, and a
`validation.json` containing the checks actually performed alongside the package.
Do not present a diagnostic `sdkGitSha()` as a commit or automatically replace
Zyna's connected package. Fill in actual paths and hashes after building, not in
a document template.

## Staging and naming

Before `git add`, save the current `git status`, `git diff`, and
`git diff --cached`. Do not reset someone else's index. Combine the groups in
[MAINTENANCE.md](MAINTENANCE.md) if separating them would break correctness. Review
the final staged diff and ensure it contains no binaries, temporary results, or
unrelated prototypes. Large raw JSON/log/patch archives can be release assets
rather than source files.

Suggested titles, to be checked against the actual scope:

- Preparatory compatibility change: `fix(sdk): yield redecryption work portably on wasm`.
- Existing fixture correction: `test(event-cache): correct thread edit expectations`.
- Main cohesive change: `fix(timeline): recover consistently from cache lag and coalesce rebuild output`.
- PR within the fork: `Restore timelines consistently after cache lag and reduce rebuild updates`.

The PR describes the final behavior, main limitations, exact regression results,
and iPhone candidate results. Keep the history of unsuccessful intermediate
variants in the validation archive. After creating the PR, replace temporary
changelog fragment number `0` with the actual fork PR number. If no PR is used,
follow the fork's established convention rather than inventing a number.

In local history, Rust tags follow `zyna-ffi-26.05.13-<change>-beta.1`; the original
connected Swift version was `26.5.13-zyna.5-beta.15`. These are separate version
namespaces. Before choosing the next tag, check current tags in both repositories
and their manifests. No next version is assigned here. A release title can use
`Zyna SDK: cache-lag recovery and rebuild updates` plus the chosen version.

## What to publish after accepting the candidate

Release order: owner reviews the staged diff → owner creates commits → build from
the selected clean commit → check that artifact's identity/smoke in Zyna → publish
release assets and the Swift package. A branch/commit may be pushed earlier for
building and the PR; the public package must reference a verified, immutable ZIP.

1. The selected Rust commit/tag and an XCFramework ZIP built **from it**. The final
   manifest must contain the actual commit and runtime `sdkGitSha()`. If production
   code, features, or targets changed after device testing, determine which checks
   need repeating; do not claim a different binary was already tested on iPhone.
2. A Swift package with bindings from the same build and the supported slices.
   Preserve the agreed platforms for the public package; do not present an
   iPhone-only artifact as a replacement for a package with simulator/macOS support.
3. `package-result.json`, a SHA256 list, and a compact `validation.json`/report.
   Preserve the [portable evidence archive](evidence/README.md) and its checksum
   alongside any updated measurements supporting performance claims. Exclude
   private application data. Do not automatically
   publish complete local Zyna logs: a reviewed acceptance summary with its scope
   is sufficient for the release. Include full build logs if needed.
4. If `Package.swift` uses `.binaryTarget(url:checksum:)`, compute its checksum
   from the **exact ZIP being uploaded**, using `swift package compute-checksum`.
   Do not substitute the inner `.a` SHA256 for the package checksum. The URL,
   checksum, and asset must refer to the same immutable version.

Before handing this to the owner, show the staged scope, suggested titles,
artifact paths, hashes, checks actually passed, and remaining device checks.
Do not create a commit/tag/PR/release or change the Swift components dependency
before the owner's decision.
