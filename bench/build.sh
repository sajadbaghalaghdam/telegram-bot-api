#!/usr/bin/env bash
# Builds (1) the mock upstream and (2) a *bench binary* of the server.
#
# The server hard-codes https://api.telegram.org as upstream (src/client/engine.rs) and offers no
# option to change it, so a one-line patch (bench/patches/upstream-url-env.patch: read
# BENCH_UPSTREAM_BASE_URL at startup) is applied to a COPY of the committed source (git archive HEAD).
# src/ in the repository is never modified. The bench binary is built with the release profile
# plus symbols (strip=false, debug=line-tables-only) so perf/heaptrack can resolve functions;
# this does not change code generation.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="${BENCH_BUILD_DIR:-$ROOT/bench/.build}"
mkdir -p "$B"

( cd "$ROOT/bench/mock-upstream" && cargo build --release --offline -q 2>&1 | tail -3 ) || cargo build --release -q --manifest-path "$ROOT/bench/mock-upstream/Cargo.toml"

rm -rf "$B/src"; mkdir -p "$B/src"
git -C "$ROOT" archive HEAD | tar -x -C "$B/src"
( cd "$B/src" && patch -p1 --quiet < "$ROOT/bench/patches/upstream-url-env.patch" )
git -C "$ROOT" rev-parse HEAD > "$B/source_commit"

# separate target + build dir so the repository's own target/ is untouched
export CARGO_TARGET_DIR="$B/target" CARGO_BUILD_BUILD_DIR="$B/cargo-build"
export CARGO_PROFILE_RELEASE_STRIP=false CARGO_PROFILE_RELEASE_DEBUG=line-tables-only
( cd "$B/src" && cargo build --release 2>&1 | tail -3 )
cp "$B/target/release/telegram-bot-api-rs" "$B/server-bench"
cp "$ROOT/bench/mock-upstream/target/release/mock-upstream" "$B/mock-upstream"
ls -l "$B/server-bench" "$B/mock-upstream"
