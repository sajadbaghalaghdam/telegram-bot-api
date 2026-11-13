#!/usr/bin/env python3
"""Top allocation sites by call count from a heaptrack flamegraph-folded file
(`heaptrack_print -f <file> -F folded.txt`). Each stack is attributed to its innermost frame that
belongs to the app or a request-path library; counts are allocation calls.
usage: heap_top.py <folded.txt> [N]"""
import collections
import re
import sys

top = int(sys.argv[2]) if len(sys.argv) > 2 else 25
pat = re.compile(r"telegram_bot_api_rs|reqwest|hyper|axum|serde_json|url::|tower|rusqlite|dashmap|tokio::|http::|bytes::")
agg = collections.Counter()
tot = 0
for line in open(sys.argv[1]):
    stack, _, n = line.rstrip("\n").rpartition(" ")
    n = int(n)
    tot += n
    frames = [f.split(" (")[0] for f in stack.split(";") if f]
    hit = next((f for f in reversed(frames) if pat.search(f)), "(other/unattributed)")
    agg[hit[:120]] += n
print(f"total allocation calls: {tot}")
for k, v in agg.most_common(top):
    print(f"{v:>9} {100 * v / tot:5.1f}%  {k}")
