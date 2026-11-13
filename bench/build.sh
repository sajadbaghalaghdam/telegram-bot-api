#!/usr/bin/env bash
# Builds (1) the mock upstream and (2) a *bench binary* of the server from the current working tree.
#
# The server is pointed at the mock with the regular --upstream-url option (see lib.sh), so no patched
# source is needed. The bench binary uses the repository's release profile plus symbols (strip=false,
# debug=line-tables-only) so perf/heaptrack can resolve functions; this does not change code generation.
# It gets its own target dir so the repository's target/ is untouched.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="${BENCH_BUILD_DIR:-$ROOT/bench/.build}"
mkdir -p "$B"

( cd "$ROOT/bench/mock-upstream" && cargo build --release --offline -q 2>&1 | tail -3 ) || cargo build --release -q --manifest-path "$ROOT/bench/mock-upstream/Cargo.toml"

git -C "$ROOT" rev-parse HEAD > "$B/source_commit"
export CARGO_TARGET_DIR="$B/target" CARGO_BUILD_BUILD_DIR="$B/cargo-build"
export CARGO_PROFILE_RELEASE_STRIP=false CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
( cd "$ROOT" && cargo build --release 2>&1 | tail -3 )
cp "$B/target/release/telegram-bot-api-rs" "$B/server-bench"
cp "$ROOT/bench/mock-upstream/target/release/mock-upstream" "$B/mock-upstream"
ls -l "$B/server-bench" "$B/mock-upstream"
