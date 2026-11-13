#!/usr/bin/env bash
# Allocations per delivered webhook update with heaptrack: the slope between two update counts
# removes the fixed start-up cost (same method as heap.sh, but the load is setWebhook + the mock's
# seeded getUpdates instead of sendMessage). usage: heap_webhook.sh <results-dir>
# env: HEAP_NS ("5000 25000"), WH_CHATS (200)
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RES="${1:?results dir}/heap_webhook"; mkdir -p "$RES"
start_mock
for n in ${HEAP_NS:-5000 25000}; do
  rm -f "$SCRATCH"/htw_$n.*
  start_server "heaptrack -o $SCRATCH/htw_$n" --local || exit 1
  curl -s "$MOCK_URL/__mock/seed?n=$n&chats=${WH_CHATS:-200}" >/dev/null; curl -s "$MOCK_URL/__mock/reset" >/dev/null
  curl -s -XPOST -H 'content-type: application/json' -d "{\"url\":\"http://127.0.0.1:$PORT_MOCK/hook\"}" "$SRV_URL/bot$TOKEN/setWebhook" >/dev/null
  end=$((SECONDS + 120)); d=0
  while [ $SECONDS -lt $end ]; do
    d=$(curl -s "$MOCK_URL/__mock/stats" | sed -n 's/.*"hook":\([0-9]*\).*/\1/p'); [ "${d:-0}" -ge "$n" ] && break; sleep 0.1
  done
  sleep 1; stop_server; sleep 3
  f=$(ls "$SCRATCH"/htw_$n.* 2>/dev/null | head -1)
  heaptrack_print -f "$f" -F "$SCRATCH/folded_w$n.txt" > "$SCRATCH/htw_print_$n.txt" 2>&1
  echo "n=$n delivered=$d" | tee "$RES/heaptrack_${n}_summary.txt"
  grep -E "^(calls to allocation functions|temporary memory allocations|peak heap memory|peak RSS)" "$SCRATCH/htw_print_$n.txt" | tee -a "$RES/heaptrack_${n}_summary.txt"
  python3 "$(dirname "${BASH_SOURCE[0]}")/heap_top.py" "$SCRATCH/folded_w$n.txt" 20 > "$RES/heap_top_$n.txt"
  rm -f "$f"
done
stop_mock
