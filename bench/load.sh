#!/usr/bin/env bash
# Load tests through the mock upstream with oha.
# usage: load.sh <results-dir>     env: REPS (3), DUR (15 s per case), SUSTAINED (60 s), CONCS ("1 32 256")
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RES="${1:?results dir}/load"; mkdir -p "$RES"
REPS="${REPS:-3}"; DUR="${DUR:-15}"; SUSTAINED="${SUSTAINED:-60}"; CONCS="${CONCS:-1 32 256}"

python3 - "$SCRATCH" <<'PY'
import json,sys
d=sys.argv[1]
json.dump({"chat_id":1001,"text":"hello"},open(d+"/small.json","w"))
json.dump({"chat_id":1001,"text":"x"*50000,"parse_mode":"HTML"},open(d+"/big50k.json","w"))
PY
JH=(-H "content-type: application/json")
scen_args() { # sets SA (array) for a scenario
  case "$1" in
    getMe)       SA=("$SRV_URL/bot$TOKEN/getMe") ;;
    sendMessage) SA=(-m POST "${JH[@]}" -D "$SCRATCH/small.json" "$SRV_URL/bot$TOKEN/sendMessage") ;;
    big50k)      SA=(-m POST "${JH[@]}" -D "$SCRATCH/big50k.json" "$SRV_URL/bot$TOKEN/sendMessage") ;;
  esac
}

start_mock
echo "== mock alone (is the mock a bottleneck?)"
for c in 32 256; do
  oha -z 5s -c $c --no-tui --output-format json -m POST "${JH[@]}" -D "$SCRATCH/small.json" "$MOCK_URL/bot$TOKEN/sendMessage" > "$RES/mock_direct_c$c.oha.json"
done

for rep in $(seq 1 "$REPS"); do
  echo "== rep $rep"
  start_server "" || exit 1
  oha -z 3s -c 32 --no-tui "$SRV_URL/bot$TOKEN/getMe" >/dev/null 2>&1   # warm-up (also fills the getMe cache)
  for scen in getMe sendMessage big50k; do
    for c in $CONCS; do
      scen_args "$scen"
      run_oha "r${rep}_${scen}_c${c}" "$c" "$DUR" "${SA[@]}"
      echo "  rep$rep $scen c=$c done"
    done
  done
  stop_server
done

echo "== unique tokens getMe (cache/DB/map growth)"
start_server "" || exit 1
python3 - "$SCRATCH" "$PORT_SRV" <<'PY'
import random,string,sys
random.seed(1)
with open(sys.argv[1]+"/uniq_urls.txt","w") as f:   # 6000 distinct tokens > session cap (2048) and active-bot cap (4096)
    for i in range(6000):
        f.write("http://127.0.0.1:%s/bot%d:AA%s/getMe\n"%(sys.argv[2],100000000+i,"".join(random.choices(string.ascii_letters+string.digits,k=30))))
PY
run_oha "uniq_getMe_c32" 32 "$DUR" --urls-from-file "$SCRATCH/uniq_urls.txt"
read -r rss vsz thr fds _ <<< "$(proc_stat "$SRV_PID")"; echo -e "rss_kb\t$rss\tthreads\t$thr\tfds\t$fds" > "$RES/uniq_getMe_after.tsv"
du -sk "$DATA" > "$RES/uniq_getMe_datadir_kb.txt"; ls -l "$DATA" >> "$RES/uniq_getMe_datadir_kb.txt"
stop_server

echo "== sustained ${SUSTAINED}s sendMessage c=32, RSS sampled every 5 s, then 20 s idle"
start_server "" || exit 1
oha -z 3s -c 32 --no-tui "$SRV_URL/bot$TOKEN/getMe" >/dev/null 2>&1
: > "$RES/sustained.srv.tsv"; : > "$RES/sustained.mock.tsv"
sampler "$SRV_PID" 5 "$RES/sustained.srv.tsv"; sampler "$MOCK_PID" 5 "$RES/sustained.mock.tsv"
oha -z "${SUSTAINED}s" -c 32 --no-tui --output-format json -m POST "${JH[@]}" -D "$SCRATCH/small.json" "$SRV_URL/bot$TOKEN/sendMessage" > "$RES/sustained.oha.json"
sleep 20
stop_samplers
stop_server
stop_mock
echo "load suite done"
