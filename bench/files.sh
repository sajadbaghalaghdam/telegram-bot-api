#!/usr/bin/env bash
# File path: getFile and file download (64 KiB file served by the mock), local and non-local mode.
# MTProto up/download (> 20 MB / 50 MB) needs real Telegram DCs and is NOT measurable here.
# usage: files.sh <results-dir>    env: DUR (10)
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
RES="${1:?results dir}/files"; mkdir -p "$RES"; DUR="${FILE_DUR:-10}"
export FILE_BYTES=65536
start_mock
for mode in local nonlocal; do
  flags=(); [ "$mode" = local ] && flags=(--local)
  start_server "" "${flags[@]}" || exit 1
  # first getFile (local mode: downloads the file from the mock to disk)
  curl -s -o "$RES/${mode}_getFile_first.json" -w '%{time_total}\n' "$SRV_URL/bot$TOKEN/getFile?file_id=BQACAgIAAxkBAAIBQ2" > "$RES/${mode}_getFile_first_time.txt"
  cp "$RES/${mode}_getFile_first.json" /dev/null
  run_oha "${mode}_getFile_c32" 32 "$DUR" "$SRV_URL/bot$TOKEN/getFile?file_id=BQACAgIAAxkBAAIBQ2"
  run_oha "${mode}_download_c32" 32 "$DUR" "$SRV_URL/file/bot$TOKEN/documents/file_1.bin"
  curl -s -o /dev/null -w '%{http_code} %{size_download}\n' "$SRV_URL/file/bot$TOKEN/documents/file_1.bin" > "$RES/${mode}_download_check.txt"
  du -sk "$DATA" > "$RES/${mode}_datadir_kb.txt"
  stop_server
done
stop_mock
head -c 400 "$RES"/*_getFile_first.json; echo; cat "$RES"/*_download_check.txt
