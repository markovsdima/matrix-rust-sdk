# Portable validation evidence

[cache-lag-2026-10.tar.gz](cache-lag-2026-10.tar.gz) preserves the evidence behind
the timeline changes without relying on temporary directories or a developer's
home directory. Verify it using [SHA256SUMS](SHA256SUMS). The archive is about
172 KiB and contains 44 files; it contains no libraries, Git bundles, screenshots,
or full application logs.

## Contents and provenance

| Directory | Contents | Revision and scope |
| --- | --- | --- |
| `controlled/` | All warm-up/measured values, median/min/max summaries, negative-control results, state equivalence, methods, toolchain/features, and variant patches | Historical comparison on `12ae23d`; four variants, 1000/17000 events, three rebuild modes, and a 1000-event live control |
| `sdk/` | Passed test names and results, static checks, six volume cases, source manifest, receipt diagnostic checks, and package metadata | Diagnostic candidate `e426f04f-zyna-dirty-a90e2aadf789`, tested on October 18, 2026 |
| `device/` | Selected original iPhone recovery aggregates, reaction verification JSON, and hashes of private application reports | Device observations on the same diagnostic candidate; not a controlled performance A/B |
| `manifest.json` | Per-file hashes, original input hashes, and transformation descriptions | Documents what was copied, normalized, or selected |

Local paths and the host name in historical records were replaced with relative
paths or labeled placeholders. Numeric measurements, outcomes, source hashes, and
variant hashes were retained. Paths to omitted logs or build products inside
those records describe historical provenance, not files supplied by this archive.
Private application reports are represented by hashes and the acceptance summary;
the complete reports are intentionally not included.

`sdk/diagnostic-package.json` retains its original build-time status, including a
then-pending iPhone check. Later device results are in `device/` and
[ACCEPTANCE.md](../ACCEPTANCE.md). Historical expected failures and discarded-run
caveats remain distinguishable from successful checks. Packaging this evidence
does not constitute a new test run.

## Inspect and verify

Run from anywhere inside this repository:

```sh
export SDK_SOURCE="$(git rev-parse --show-toplevel)"
export EVIDENCE_DIR="$SDK_SOURCE/contrib/zyna-fork/evidence"
(
  cd "$EVIDENCE_DIR"
  shasum -a 256 -c SHA256SUMS
)
export AUDIT_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/zyna-evidence.XXXXXX")"
tar -xzf "$EVIDENCE_DIR/cache-lag-2026-10.tar.gz" -C "$AUDIT_ROOT"
```

`controlled/raw-measurements.json` includes warm-ups explicitly marked as such.
`controlled/summary.json` contains the five measured values and median/min/max for
each metric. `controlled/correctness.json` includes the old negative control's
required exit code 101 and the corrected implementation's exit code 0. Read
`controlled/methods.txt` for the measured section and interpretation limits.

## Reconstruct the historical controlled comparison

The original experimental commits need not exist in a fresh clone. Start from
`e426f04f6e2290698deb83638d0b967569c5886f`, then apply
`controlled/patches/production-base.patch` to reconstruct the source changes in
`12ae23d` without its historical `contrib` reports. For each variant, apply its
`<variant>-from-final.patch` next. These include the common test harness and the
selected ablation. Do not also apply `common-harness.patch` or a second ablation.
For the old negative control, apply only `old-harness.patch` to the original base.

These patches are historical evidence, not changes to apply to the current SDK.
The worktrees below remain uncommitted; no temporary commits are necessary. The
recorded Git hashes identify the original measurement trees, while patch hashes
and file contents identify the reconstructed sources.

Use the recorded toolchain (Rust 1.95.0, x86_64 macOS) for comparable conditions.
Dependencies must be available for `--offline`, and mock HTTP tests need loopback
access. Build all variants sequentially before measuring. Separate target
directories avoid reusing local-path crate artifacts across source variants.

```sh
set -e
export CONTROL_ROOT="$AUDIT_ROOT/controlled"
mkdir -p "$CONTROL_ROOT/worktrees" "$CONTROL_ROOT/binaries" \
  "$CONTROL_ROOT/logs" "$CONTROL_ROOT/results" "$CONTROL_ROOT/states"

for variant in current no_coalescing no_pending neither old; do
  source_copy="$CONTROL_ROOT/worktrees/$variant"
  git clone --no-hardlinks --no-checkout "$SDK_SOURCE" "$source_copy"
  git -C "$source_copy" checkout --detach \
    e426f04f6e2290698deb83638d0b967569c5886f
  if [ "$variant" = old ]; then
    git -C "$source_copy" apply "$CONTROL_ROOT/patches/old-harness.patch"
  else
    git -C "$source_copy" apply "$CONTROL_ROOT/patches/production-base.patch"
    git -C "$source_copy" apply \
      "$CONTROL_ROOT/patches/$variant-from-final.patch"
  fi
  (
    cd "$source_copy"
    CARGO_INCREMENTAL=0 CARGO_TARGET_DIR="$AUDIT_ROOT/targets/$variant" \
      cargo test --offline --release -p matrix-sdk-ui --lib \
      --no-run --message-format=json
  ) > "$CONTROL_ROOT/logs/build-$variant.jsonl" || exit 1
  python3 - "$CONTROL_ROOT" "$variant" <<'PY'
import json
from pathlib import Path
import shutil
import sys

root, variant = Path(sys.argv[1]), sys.argv[2]
messages = [json.loads(line) for line in
            (root / "logs" / f"build-{variant}.jsonl").read_text().splitlines()
            if line.startswith("{")]
executables = [m["executable"] for m in messages
               if m.get("reason") == "compiler-artifact"
               and m["target"]["name"] == "matrix_sdk_ui"
               and m.get("executable")]
assert len(executables) == 1, executables
shutil.copy2(executables[0], root / "binaries" / variant)
PY
done

ln -s worktrees/current "$CONTROL_ROOT/final"
RUST_LOG=off "$CONTROL_ROOT/binaries/current" audit_lag:: \
  --nocapture --test-threads=1
# Expected failure (exit 101): the old implementation must expose the defect.
if RUST_LOG=off "$CONTROL_ROOT/binaries/old" audit_lag:: \
  --nocapture --test-threads=1; then
  echo 'Unexpected success of the old negative control' >&2
  exit 1
else
  test "$?" -eq 101
fi

python3 "$CONTROL_ROOT/scripts/run-measurements.py"
```

The runner performs one warm-up and five rotating measured rounds per variant,
with no concurrent builds. It writes new results under `controlled/results/` and
leaves the archived original values at `controlled/*.json` untouched. It also
compares complete exported state and the live diff stream across variants.
Do not run unrelated test processes during measurement.

The patch reconstruction and archived result consistency were checked while
preparing this portable package. The build/measurement commands were not rerun
for this documentation change. To validate the current implementation rather than
reproduce the historical comparison, use [MAINTENANCE.md](../MAINTENANCE.md).
