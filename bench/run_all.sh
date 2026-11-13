#!/usr/bin/env bash
# One-shot baseline: build, static analysis, then the runtime suites; prints a summary.
#
#   bench/run_all.sh                    # everything (~25-30 min on 8 cores)
#   SUITES="idle load" bench/run_all.sh # subset (static idle load profile heap webhook files)
#   SKIP_BUILD=1                        # reuse bench/.build
#   SKIP_COLD_BUILD=1                   # skip the clean-build timing inside the static suite
#   REPS=3 DUR=15 SUSTAINED=60          # load-suite knobs
#
# Raw outputs: bench/results/<UTC timestamp>/ (gitignored); summary.md is written next to them.
# Needs: cargo, oha, perf (+sudo), heaptrack, cargo-bloat, cargo-machete, python3, curl.
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STAMP="${STAMP:-$(date -u +%Y%m%dT%H%M%SZ)}"
RES="$HERE/results/$STAMP"; mkdir -p "$RES"
SUITES="${SUITES:-static idle load profile heap webhook files}"

{
  echo "date: $(date -u)"; echo "kernel: $(uname -srm)"; echo "cpu: $(lscpu | sed -n 's/^Model name: *//p')"
  echo "cores: $(nproc)"; echo "mem: $(free -m | awk '/Mem:/{print $2" MB"}')"
  echo "commit: $(git -C "$HERE/.." rev-parse HEAD)"; echo "rustc: $(rustc -V)"; echo "oha: $(oha --version)"
  echo "load average at start: $(cut -d' ' -f1-3 /proc/loadavg)"
} | tee "$RES/environment.txt"

[ -n "${SKIP_BUILD:-}" ] || "$HERE/build.sh" 2>&1 | tee "$RES/build.log"

for s in $SUITES; do
  echo; echo "######## suite: $s  (load avg $(cut -d' ' -f1 /proc/loadavg))"
  "$HERE/$s.sh" "$RES" 2>&1 | tee "$RES/$s.log"
done

python3 "$HERE/summarize.py" "$RES" > "$RES/summary.md"
echo; echo "######## SUMMARY ($RES/summary.md)"; cat "$RES/summary.md"
