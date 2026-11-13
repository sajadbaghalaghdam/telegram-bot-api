#!/usr/bin/env bash
# Idle runtime: startup time, RSS/VSZ/threads/fds after 10 s idle, idle CPU. 3 independent starts.
# usage: idle.sh <results-dir>
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RES="${1:?results dir}/idle"; mkdir -p "$RES"
OUT="$RES/idle.tsv"
echo -e "run\tstartup_ms\trss_kb_t1s\trss_kb_t10s\tvsz_kb\tthreads\tfds\tidle_cpu_ticks_10s" > "$OUT"
start_mock
for i in 1 2 3; do
  start_server "" || exit 1
  sleep 1; read -r rss1 _ <<< "$(proc_stat "$SRV_PID")"
  read -r _ _ _ _ u0 s0 <<< "$(proc_stat "$SRV_PID")"
  sleep 10
  read -r rss vsz thr fds u1 s1 <<< "$(proc_stat "$SRV_PID")"
  echo -e "$i\t$STARTUP_MS\t$rss1\t$rss\t$vsz\t$thr\t$fds\t$(( (u1+s1)-(u0+s0) ))" >> "$OUT"
  [ "$i" = 1 ] && { cp /proc/$SRV_PID/status "$RES/proc_status.txt"; ls -l /proc/$SRV_PID/fd > "$RES/fds.txt"; }
  stop_server
done
cat "$OUT"
