# Diagnostic cache lag build for Zyna

The fix works inside the SDK without changing the Swift API. An iPhone run needs a
new XCFramework built from these sources and Zyna in **Debug** configuration: Rust
file tracing in `AppDelegate` and the log export screen are under `#if DEBUG`.
Build the Rust SDK with `--release` to retain optimizations.

Zyna's original version `26.5.13-zyna.5-beta.15` corresponds to Rust commit
`e426f04f6e2290698deb83638d0b967569c5886f`. Check the binary actually loaded through
`sdkGitSha()`: the remote Swift package version alone does not establish that a
local build has replaced it.

## Use a separate source copy

`xtask swift build-framework` deletes and recreates `bindings/apple/generated`.
Build in a separate temporary copy to preserve the existing XCFramework in the
working repository. The export below includes current tracked changes and new
files that are not ignored; run it after edits and regressions are complete. It
**does not create a commit**. For an uncommitted diagnostic candidate, it saves a
patch, the full base commit SHA, and a SHA-256 fingerprint of the file contents.
The supported `VERGEN_GIT_SHA` variable embeds an explicitly marked diagnostic ID
in the binary. This `sdkGitSha()` value is a **build identifier, not an existing
Git commit**.

Run the following from anywhere inside this checkout. The source root is resolved
by Git; the build directory is created under the system temporary directory. Keep
the resulting package and manifest in durable release storage if they are needed
after local validation.

```sh
export CACHE_LAG_SOURCE="$(git rev-parse --show-toplevel)"
export CACHE_LAG_BUILD_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/zyna-cache-lag.XXXXXX")"

python3 - <<'PY'
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

source = Path(os.environ["CACHE_LAG_SOURCE"])
build = Path(os.environ["CACHE_LAG_BUILD_ROOT"])
checkout = build / "source"

def git(path, *args, **kwargs):
    return subprocess.run(["git", "-C", str(path), *args], check=True, **kwargs)

base = git(source, "rev-parse", "HEAD", capture_output=True, text=True).stdout.strip()
patch = git(source, "diff", "--binary", "HEAD", capture_output=True).stdout
(build / "source.patch").write_bytes(patch)
subprocess.run(["git", "clone", "--no-hardlinks", "--no-checkout", str(source), str(checkout)], check=True)
git(checkout, "checkout", "--detach", base)
if patch:
    git(checkout, "apply", "--binary", str(build / "source.patch"))
untracked = git(source, "ls-files", "-z", "--others", "--exclude-standard", capture_output=True).stdout
for raw in filter(None, untracked.split(b"\0")):
    relative = Path(os.fsdecode(raw))
    target = checkout / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source / relative, target, follow_symlinks=False)
paths = git(checkout, "ls-files", "-z", "--cached", "--others", "--exclude-standard",
            capture_output=True).stdout
files = {}
for raw in sorted(set(filter(None, paths.split(b"\0")))):
    relative = os.fsdecode(raw)
    path = checkout / relative
    if path.is_file():
        files[relative] = hashlib.sha256(path.read_bytes()).hexdigest()
source_sha256 = hashlib.sha256("".join(
    path + "\0" + files[path] + "\n" for path in sorted(files)
).encode()).hexdigest()
build_id = base[:8] + "-zyna-dirty-" + source_sha256[:12]
manifest = {"source": str(checkout), "base_commit": base,
            "source_sha256": source_sha256, "expected_sdkGitSha": build_id,
            "files": files}
(build / "source-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
(build / "sdk-git-sha.txt").write_text(build_id + "\n")
(build / "base-sha.txt").write_text(base + "\n")
print("Build directory:", build)
print("Base commit:", base)
print("Source content SHA-256:", source_sha256)
print("Expected diagnostic sdkGitSha():", build_id)
PY
```

This directory contains the sources and manifest needed to verify a rebuild.
`HEAD` remains the base commit and **does not describe the uncommitted candidate**.
Before building, inspect `git -C "$CACHE_LAG_BUILD_ROOT/source" diff --stat HEAD`
and the manifest's file list. The fix does not require the history-reader
prototype. Nothing is pushed to GitHub or published.

Do not change the sources after freezing the manifest. Compare every file's SHA-256
against the manifest before and after building; generated outputs are excluded.
If reusing a target cache, clean the changed crates for every target:
`cargo clean -p matrix-sdk -p matrix-sdk-ui -p matrix-sdk-ffi --release --target <target>`.
This recompiles the workspace code, including the build script that embeds the ID.
Keep the cache and generated outputs in the separate copy, outside Zyna.

For a **release** build after the owner's commit and a clean-tree check, leave
`VERGEN_GIT_SHA` unset so that `sdkGitSha()` contains the actual short release
commit SHA. Do not publish a diagnostic `-zyna-dirty-` build as a build of that
commit; create a separate release artifact after committing.

## Build for iPhone and create a local Swift package

Requirements: Xcode, stable Rust with `aarch64-apple-ios`, and the `llvm-tools`
component for SQLite symbol checks. Xcode 26.3, Rust 1.95.0, and the installed iOS
target were verified on the investigation machine. Install components if needed:

```sh
rustup target add --toolchain stable aarch64-apple-ios
rustup component add --toolchain stable llvm-tools
```

```sh
export CACHE_LAG_PACKAGE="$CACHE_LAG_BUILD_ROOT/matrix-rust-components-swift"
cd "$CACHE_LAG_BUILD_ROOT/source"
export CARGO_TARGET_DIR="$CACHE_LAG_BUILD_ROOT/target"
mkdir -p target
export VERGEN_GIT_SHA="$(cat "$CACHE_LAG_BUILD_ROOT/sdk-git-sha.txt")"
CARGO_INCREMENTAL=0 cargo xtask swift build-framework \
  --release --target aarch64-apple-ios --sequentially \
  --ios-deployment-target 17.0 \
  --components-path "$CACHE_LAG_PACKAGE"

cat > "$CACHE_LAG_PACKAGE/Package.swift" <<'SWIFT'
// swift-tools-version:5.7
import PackageDescription
let package = Package(
    name: "MatrixRustSDK",
    platforms: [.iOS("17.0")],
    products: [.library(name: "MatrixRustSDK", targets: ["MatrixRustSDK"])],
    targets: [
        .binaryTarget(name: "MatrixSDKFFI", path: "MatrixSDKFFI.xcframework"),
        .target(name: "MatrixRustSDK", dependencies: [.target(name: "MatrixSDKFFI")])
    ]
)
SWIFT

swift package --package-path "$CACHE_LAG_PACKAGE" dump-package
shasum -a 256 "$CACHE_LAG_PACKAGE/MatrixSDKFFI.xcframework/ios-arm64/libmatrix_sdk_ffi.a" \
  > "$CACHE_LAG_BUILD_ROOT/ios-library-sha256.txt"
```

`xtask` uses `--no-default-features` and enables
`sqlite,sentry,unstable-msc4274,experimental-element-recent-emojis,experimental-push-secrets,experimental-search`.
It verifies that the archive does not define `_sqlite3_*`, leaving those symbols
to the application's SQLCipher/SQLite. Do not replace this command with an
arbitrary FFI build using default features. UniFFI Swift bindings and the
XCFramework are generated in the same run.

This variant contains only the iPhone slice. For a simulator, add
`--target aarch64-apple-ios-sim` or `--target x86_64-apple-ios` to match the Mac's
architecture. Use `--profile reldbg` instead of `--release` for LLDB and full Rust
debug symbols; prefer an SDK release build for performance measurements.

In a separate local Zyna project configuration, replace the remote
`matrix-rust-components-swift` dependency with **Add Local Package**, pointing to
`$CACHE_LAG_PACKAGE`. Two targets use the `MatrixRustSDK` product: `Zyna` and
`ZynaNotificationService`; retain the dependency for both. Run Zyna Debug on a
physical device. While paused in Swift code, check in LLDB:

```text
expression -l Swift -- import MatrixRustSDK
expression -l Swift -- MatrixRustSDK.sdkGitSha()
```

The result must match `sdk-git-sha.txt`. `build.rs` uses
`GitclBuilder::sha(true)`: vergen checks the `VERGEN_GIT_SHA` override first and
otherwise obtains the short SHA from Git. For a diagnostic candidate, save
`base-sha.txt`, `source-manifest.json`, the diagnostic ID and archive checksum,
Xcode configuration, and exported logs alongside the run results. Check the
actual `cargo:rustc-env=VERGEN_GIT_SHA=…` in the build script output and the
presence of this identifier in each built archive. Finding the string in an
archive does not replace checking `sdkGitSha()` in the running application.
To roll back, restore the Zyna dependency saved before the run and rebuild the
application. It may be a previously validated local package rather than the
original remote version `26.5.13-zyna.5-beta.15`.

## Collecting and interpreting logs

Use the normal application UI to reproduce the issue: **do not enable**
`ZYNA_DIAGNOSTICS`. Existing `MatrixRustSDKTracing` already saves INFO/WARN
timeline logs to rotating `zyna-app*.log` files with a 64 MB limit.
`ZYNA_RUST_EVENT_CACHE_TRACE` is not required for the new aggregate counters.
It enables a separate detailed TRACE profile and increases log volume
substantially.

After the run, restart with `ZYNA_DIAGNOSTICS=1` or the `-zynaDiagnostics 1`
argument. In the diagnostics screen, select **SDK logs (rust tracing) → Share
logs**. `Share event cache trace` exports a different profile and is not the
button for ordinary aggregate logs. `Tracing status` shows the directory, file
count, and whether initialization succeeded.

Records are correlated by numeric timeline ID and rebuild number within the
process. They contain lag/skipped, rebuild start/end, snapshot event count,
output operations, and item occurrences. Output counters are collected at the
UI timeline producer before subscriber skip/filter and FFI batching. They are
not callback batch counts, unique events, or bytes. `Set`, `Insert`, `PushFront`,
and `PushBack` each contribute one item occurrence; structural `Clear`/`Remove`
contribute an operation without a new item. Virtual items are included. With
no UI subscribers, the counters still reflect committed mutations even though
the observable vector does not send a batch. `lag_skipped` and `skipped_total`
count lost broadcast messages, not the events inside them. `snapshot_events`
counts all loaded cache events, including those excluded by the timeline filter.
`elapsed_ms` measures time from lag detection and includes snapshot acquisition.
Uncommitted transactions and changes discarded by a later `Clear` in the same
transaction are not counted.

Diagnostics retain only numeric counters and do not create an additional
subscriber or diagnostic snapshots. The new records contain no message contents.
Existing general SDK logs may contain other data; extracting cache lag/rebuild
lines is sufficient for aggregate analysis.

The new post-rebuild receipt reconciliation counters and focused DEBUG target
configuration are described in the [iPhone checklist](IPHONE-CHECKLIST.md).
