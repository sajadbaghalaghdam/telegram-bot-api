#!/usr/bin/env bash
# CPU profile (perf) of the server during a sendMessage load. Needs sudo for perf (perf_event_paranoid).
# usage: profile.sh <results-dir>
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RES="${1:?results dir}/profile"; mkdir -p "$RES"
python3 -c "import json;json.dump({'chat_id':1001,'text':'hello'},open('$SCRATCH/small.json','w'))"

# sudo (kernel symbols need it), drop comments/blank lines, squeeze the padding perf adds
rep() { sudo perf report "$@" 2>/dev/null | grep -v '^#' | grep -v '^$' | tr -s ' ' | cut -c1-220; }

start_mock; start_server "" || exit 1
oha -z 3s -c 32 --no-tui "$SRV_URL/bot$TOKEN/getMe" >/dev/null 2>&1

oha -z 40s -c 32 --no-tui --output-format json -m POST -H "content-type: application/json" -D "$SCRATCH/small.json" \
  "$SRV_URL/bot$TOKEN/sendMessage" > "$RES/oha_during_profile.json" &
OHA=$!
sleep 4
echo "== perf stat (10 s)"
sudo perf stat -p "$SRV_PID" -e task-clock,context-switches,cpu-migrations,page-faults,cycles,instructions,branches,branch-misses -- sleep 10 2> "$RES/perf_stat.txt"
cat "$RES/perf_stat.txt"

echo "== perf record (frame-pointer callgraph, flat profile, 10 s @ 999 Hz)"
sudo perf record -F 999 -g -p "$SRV_PID" -o "$SCRATCH/perf.data" -- sleep 10 >/dev/null 2>&1
rep -i "$SCRATCH/perf.data" --no-children --stdio --sort symbol -g none | head -60 > "$RES/top_symbols.txt"
rep -i "$SCRATCH/perf.data" --no-children --stdio --sort comm -g none | head -15 > "$RES/by_thread.txt"
rep -i "$SCRATCH/perf.data" --no-children --stdio --sort dso -g none | head -15 > "$RES/by_dso.txt"

echo "== perf record (DWARF callgraph, 5 s @ 499 Hz) for the inclusive (children) view"
sudo perf record -F 499 --call-graph dwarf,16384 -p "$SRV_PID" -o "$SCRATCH/perf_dwarf.data" -- sleep 5 >/dev/null 2>&1
rep -i "$SCRATCH/perf_dwarf.data" --children --stdio --sort symbol -g none | head -600 > "$RES/top_inclusive_all.txt"
# user-space view: the kernel/syscall frames dominate the raw inclusive list
grep -E 'telegram_bot_api|reqwest|hyper|serde|axum|tokio::|url::|malloc|_int_free|lll_lock|rusqlite|sqlite' "$RES/top_inclusive_all.txt" | head -50 > "$RES/top_inclusive.txt"
wait "$OHA"
sudo rm -f "$SCRATCH/perf.data" "$SCRATCH/perf_dwarf.data"
head -25 "$RES/top_symbols.txt"; cat "$RES/by_thread.txt"
