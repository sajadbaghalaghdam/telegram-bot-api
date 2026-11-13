#!/usr/bin/env bash
# Static baseline: binary size, bloat, build time, deps, machete, clippy, tests, LOC.
# usage: static.sh <results-dir>      env: SKIP_COLD_BUILD=1 to skip the clean-build timing
set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="${BENCH_BUILD_DIR:-$ROOT/bench/.build}"
OUT="${1:?results dir}/static"; mkdir -p "$OUT"
cd "$ROOT"

echo "== versions"; { rustc -V; cargo -V; git rev-parse HEAD; } | tee "$OUT/versions.txt"

echo "== release build (repo profile: opt3 fat-LTO cgu1 strip panic=abort)"
/usr/bin/time -f "%e s wall, %M KB maxrss" cargo build --release 2> "$OUT/build_warm.txt" || true
tail -2 "$OUT/build_warm.txt"
BIN="$ROOT/target/release/telegram-bot-api-rs"
stat -c %s "$BIN" > "$OUT/binary_size_bytes.txt"
echo "binary bytes: $(cat "$OUT/binary_size_bytes.txt")"

if [ -z "${SKIP_COLD_BUILD:-}" ]; then
  echo "== clean release build (sccache OFF, fresh target dir)"
  T="$(mktemp -d "${TMPDIR:-/tmp}/bench-cold.XXXXXX")"
  ( export CARGO_TARGET_DIR="$T/target" CARGO_BUILD_BUILD_DIR="$T/build" RUSTC_WRAPPER= CARGO_BUILD_RUSTC_WRAPPER=
    /usr/bin/time -f "%e s wall, %U s user, %S s sys, %M KB maxrss" cargo build --release 2> "$OUT/build_cold.txt" )
  tail -2 "$OUT/build_cold.txt"; rm -rf "$T"
fi

echo "== cargo tree / machete"
cargo tree | wc -l > "$OUT/tree_lines.txt"
cargo tree --prefix none --edges normal | sort -u | wc -l > "$OUT/tree_unique.txt"
grep -c '^name = ' Cargo.lock > "$OUT/lock_packages.txt"
cargo machete > "$OUT/machete.txt" 2>&1 || true
cat "$OUT/tree_lines.txt" "$OUT/tree_unique.txt" "$OUT/machete.txt"

echo "== bloat (symbol-preserving build of the committed source, separate target dir)"
if [ -d "$B/src" ]; then
  ( cd "$B/src" && export CARGO_TARGET_DIR="$B/target" CARGO_BUILD_BUILD_DIR="$B/cargo-build" CARGO_PROFILE_RELEASE_STRIP=false CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
    cargo bloat --release --crates -n 25 > "$OUT/bloat_crates.txt" 2>&1
    cargo bloat --release -n 30 > "$OUT/bloat_functions.txt" 2>&1 )
  head -30 "$OUT/bloat_crates.txt"
fi

echo "== clippy --all-targets"
touch src/main.rs
cargo clippy --all-targets 2> "$OUT/clippy_raw.txt" >/dev/null || true
python3 "$ROOT/bench/clippy_summary.py" "$OUT/clippy_raw.txt" > "$OUT/clippy_summary.txt"
cat "$OUT/clippy_summary.txt"

echo "== cargo test"
/usr/bin/time -f "%e s wall" cargo test > "$OUT/test_raw.txt" 2> "$OUT/test_time.txt" || true
grep -E "^test result" "$OUT/test_raw.txt" | tee "$OUT/test_summary.txt"
tail -1 "$OUT/test_time.txt"

echo "== LOC"
git ls-files 'src/*.rs' | xargs wc -l | sort -rn > "$OUT/loc.txt"
awk '$1>800 && $2!="total"{print $2, $1}' "$OUT/loc.txt" > "$OUT/loc_over_800.txt"
cat "$OUT/loc_over_800.txt"
