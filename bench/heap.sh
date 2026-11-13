#!/usr/bin/env bash
# Allocation count / peak heap per request with heaptrack: the slope between two request counts
# removes the fixed startup cost. usage: heap.sh <results-dir>   env: HEAP_NS ("5000 25000")
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RES="${1:?results dir}/heap"; mkdir -p "$RES"
python3 -c "import json;json.dump({'chat_id':1001,'text':'hello'},open('$SCRATCH/small.json','w'))"
start_mock
for n in ${HEAP_NS:-5000 25000}; do
  rm -f "$SCRATCH"/ht_$n.*
  start_server "heaptrack -o $SCRATCH/ht_$n" || exit 1
  # one warm-up request, then exactly n measured requests
  curl -s -o /dev/null -XPOST -H 'content-type: application/json' -d '{"chat_id":1,"text":"w"}' "$SRV_URL/bot$TOKEN/sendMessage"
  oha -n "$n" -c 32 --no-tui --output-format json -m POST -H "content-type: application/json" -D "$SCRATCH/small.json" \
      "$SRV_URL/bot$TOKEN/sendMessage" > "$RES/oha_$n.json"
  sleep 1; stop_server; sleep 3
  f=$(ls "$SCRATCH"/ht_$n.* 2>/dev/null | head -1)
  heaptrack_print -f "$f" -F "$SCRATCH/folded_$n.txt" > "$SCRATCH/ht_print_$n.txt" 2>&1
  grep -E "^(calls to allocation functions|temporary memory allocations|peak heap memory|peak RSS|total memory leaked)" "$SCRATCH/ht_print_$n.txt" | tee "$RES/heaptrack_${n}_summary.txt"
  python3 "$(dirname "${BASH_SOURCE[0]}")/heap_top.py" "$SCRATCH/folded_$n.txt" 20 > "$RES/heap_top_$n.txt"
  rm -f "$f"
done
stop_mock
