#!/usr/bin/env bash
# Shared helpers for the runtime suites. Source this file; set RES (results dir) first.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
B="${BENCH_BUILD_DIR:-$ROOT/bench/.build}"
PORT_SRV="${PORT_SRV:-18080}"
PORT_MOCK="${PORT_MOCK:-18081}"
TOKEN="123456789:AAbenchbenchbenchbenchbenchbenchbenc"
SRV_URL="http://127.0.0.1:$PORT_SRV"
MOCK_URL="http://127.0.0.1:$PORT_MOCK"
SCRATCH="${BENCH_SCRATCH:-${TMPDIR:-/tmp}/bench-scratch}"
mkdir -p "$SCRATCH"
SRV_PID=""; MOCK_PID=""; SAMPLERS=()

wait_port() { # port, timeout-seconds
  local end=$((SECONDS + ${2:-10}))
  until (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; do
    [ $SECONDS -ge $end ] && { echo "port $1 never opened" >&2; return 1; }
    sleep 0.002
  done
}

start_mock() { # [env VAR=..]; extra args
  "$B/mock-upstream" --port "$PORT_MOCK" --threads "${MOCK_THREADS:-2}" >/dev/null 2>&1 &
  MOCK_PID=$!
  wait_port "$PORT_MOCK" 10
}

# start_server <wrapper-or-empty> [server args...]; sets SRV_PID and STARTUP_MS
start_server() {
  local wrap="$1"; shift
  DATA="$SCRATCH/data"; rm -rf "$DATA"; mkdir -p "$DATA"
  local t0; t0=$(date +%s%N)
  $wrap "$B/server-bench" --upstream-url "$MOCK_URL" --api-id 12345 --api-hash 0123456789abcdef \
    -p "$PORT_SRV" -d "$DATA" -t temp "$@" >"$SCRATCH/server.log" 2>&1 &
  wait_port "$PORT_SRV" 20 || return 1
  STARTUP_MS=$(( ($(date +%s%N) - t0) / 1000000 ))
  sleep 0.2
  SRV_PID=$(pgrep -x server-bench | head -1)   # exact comm match: also finds the child under heaptrack
}

stop_server() {
  [ -n "$SRV_PID" ] && kill "$SRV_PID" 2>/dev/null
  # wait for the port to be released and the process to vanish
  local i=0; while pgrep -x server-bench >/dev/null && [ $i -lt 100 ]; do sleep 0.1; i=$((i+1)); done
  pkill -9 -x server-bench 2>/dev/null; SRV_PID=""
}
stop_mock() { [ -n "$MOCK_PID" ] && kill "$MOCK_PID" 2>/dev/null; wait "$MOCK_PID" 2>/dev/null; MOCK_PID=""; }
cleanup() { stop_server; stop_mock; for p in "${SAMPLERS[@]:-}"; do kill "$p" 2>/dev/null; done; }
trap cleanup EXIT

# proc_stat <pid>: "rss_kb vsz_kb threads fds utime_ticks stime_ticks"
proc_stat() {
  local p=$1
  awk -v fds="$(ls /proc/$p/fd 2>/dev/null | wc -l)" '
    /^VmRSS:/{r=$2} /^VmSize:/{v=$2} /^Threads:/{t=$2} END{printf "%s %s %s %s ", r, v, t, fds}' /proc/$p/status
  awk '{printf "%s %s\n", $14, $15}' /proc/$p/stat
}

# sampler <pid> <interval> <outfile>: one line per sample "epoch rss vsz threads fds utime stime"
sampler() {
  ( while kill -0 "$1" 2>/dev/null; do
      echo "$(date +%s.%N) $(proc_stat "$1")" >> "$3"
      sleep "$2"
    done ) &
  SAMPLERS+=($!)
}
stop_samplers() { for p in "${SAMPLERS[@]:-}"; do kill "$p" 2>/dev/null; wait "$p" 2>/dev/null; done; SAMPLERS=(); }

# run_oha <name> <concurrency> <seconds> <oha args...>   -> $RES/<name>.oha.json + .srv.tsv + .mock.tsv
run_oha() {
  local name=$1 c=$2 dur=$3; shift 3
  : > "$RES/$name.srv.tsv"; : > "$RES/$name.mock.tsv"
  sampler "$SRV_PID" 1 "$RES/$name.srv.tsv"
  sampler "$MOCK_PID" 1 "$RES/$name.mock.tsv"
  oha -z "${dur}s" -c "$c" --no-tui --output-format json "$@" > "$RES/$name.oha.json" 2> "$RES/$name.oha.err"
  stop_samplers
}
