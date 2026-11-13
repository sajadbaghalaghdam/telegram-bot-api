#!/usr/bin/env bash
# Local webhook delivery: the mock serves N seeded updates via getUpdates and receives the
# deliveries on /hook. Measures updates/s (setWebhook -> last delivery) and RSS.
# usage: webhook.sh <results-dir>    env: WH_N (50000), WH_CHATS (200), WH_N1 (3000: single-chat serial case)
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RES="${1:?results dir}/webhook"; mkdir -p "$RES"
OUT="$RES/webhook.tsv"
echo -e "case\tupdates\tchats\tdelivered\tseconds\tupdates_per_s\trss_kb_before\trss_kb_hwm\trss_kb_after10s\tthreads_after\tfds_after\tcpu_s" > "$OUT"
start_mock
hooks() { curl -s "$MOCK_URL/__mock/stats" | sed -n 's/.*"hook":\([0-9]*\).*/\1/p'; }

run_case() { # name n chats
  local name=$1 n=$2 chats=$3
  start_server "" --local || exit 1
  curl -s "$MOCK_URL/__mock/seed?n=$n&chats=$chats" >/dev/null; curl -s "$MOCK_URL/__mock/reset" >/dev/null
  read -r rss0 _ <<< "$(proc_stat "$SRV_PID")"; read -r _ _ _ _ u0 s0 <<< "$(proc_stat "$SRV_PID")"
  : > "$RES/$name.srv.tsv"; sampler "$SRV_PID" 0.25 "$RES/$name.srv.tsv"
  local t0 t1 d=0; t0=$(date +%s.%N)
  curl -s -XPOST -H 'content-type: application/json' -d "{\"url\":\"http://127.0.0.1:$PORT_MOCK/hook\"}" "$SRV_URL/bot$TOKEN/setWebhook" > "$RES/$name.setwebhook.json"
  local end=$((SECONDS + 120))
  while [ $SECONDS -lt $end ]; do d=$(hooks); [ "${d:-0}" -ge "$n" ] && break; sleep 0.05; done
  t1=$(date +%s.%N)
  stop_samplers
  read -r _ _ _ _ u1 s1 <<< "$(proc_stat "$SRV_PID")"
  sleep 10
  read -r rssA _ thr fds _ <<< "$(proc_stat "$SRV_PID")"
  curl -s "$SRV_URL/bot$TOKEN/getWebhookInfo" > "$RES/$name.webhookinfo.json"
  local peak; peak=$(awk "/VmHWM/{print \$2}" /proc/$SRV_PID/status)   # kernel high-water mark
  python3 - "$name" "$n" "$chats" "$d" "$t0" "$t1" "$rss0" "$peak" "$rssA" "$thr" "$fds" "$(( (u1+s1)-(u0+s0) ))" >> "$OUT" <<'PY'
import sys
name,n,ch,d,t0,t1,r0,pk,ra,thr,fds,ticks=sys.argv[1:]
sec=float(t1)-float(t0)
print("\t".join([name,n,ch,d,f"{sec:.2f}",f"{float(d)/sec:.0f}",r0,pk,ra,thr,fds,f"{int(ticks)/100:.2f}"]))
PY
  stop_server
}
run_case many_chats "${WH_N:-50000}" "${WH_CHATS:-200}"
run_case single_chat "${WH_N1:-3000}" 1
stop_mock
cat "$OUT"
