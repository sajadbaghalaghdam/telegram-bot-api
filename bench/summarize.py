#!/usr/bin/env python3
"""Builds summary.md from a bench results directory. usage: summarize.py <results-dir>"""
import glob
import json
import os
import re
import statistics as st
import sys

R = sys.argv[1]
out = []


def p(s=""):
    out.append(s)


def rd(path, default=""):
    try:
        return open(path).read().strip()
    except OSError:
        return default


def med(v):
    return st.median(v) if v else float("nan")


def tsv(path):
    rows = []
    try:
        for line in open(path):
            f = line.split()
            if len(f) == 7:
                rows.append([float(x) for x in f])
    except OSError:
        pass
    return rows


def cpu_cores(rows):
    """Average CPU cores used between first and last sample."""
    if len(rows) < 2:
        return float("nan")
    dt = rows[-1][0] - rows[0][0]
    ticks = (rows[-1][5] + rows[-1][6]) - (rows[0][5] + rows[0][6])
    return ticks / 100.0 / dt if dt > 0 else float("nan")


def oha(path):
    try:
        d = json.load(open(path))
    except Exception:
        return None
    sc = d.get("statusCodeDistribution", {})
    bad = sum(v for k, v in sc.items() if not k.startswith("2"))
    ed = d.get("errorDistribution", {})
    bad += sum(v for k, v in ed.items() if "deadline" not in k)
    ok = sum(v for k, v in sc.items() if k.startswith("2"))
    lp = d.get("latencyPercentiles", {})
    return dict(rps=d["summary"]["requestsPerSec"], p50=(lp.get("p50") or 0) * 1000, p99=(lp.get("p99") or 0) * 1000,
                p999=(lp.get("p99.9") or 0) * 1000, bad=bad, ok=ok, bps=d["summary"].get("sizePerSec", 0))


# ---------------------------------------------------------------- static
S = R + "/static"
if os.path.isdir(S):
    p("## Static")
    p()
    p(f"- release binary (repo profile): {int(rd(S + '/binary_size_bytes.txt', '0')):,} bytes")
    p(f"- `cargo build --release` with sccache + deps cached (not a clean build): {rd(S + '/build_warm.txt').splitlines()[-1] if rd(S + '/build_warm.txt') else 'n/a'}")
    if os.path.exists(S + "/build_cold.txt"):
        p(f"- clean `cargo build --release` (sccache off): {rd(S + '/build_cold.txt').splitlines()[-1]}")
    p(f"- `cargo tree | wc -l`: {rd(S + '/tree_lines.txt')}; unique normal deps: {rd(S + '/tree_unique.txt')}; Cargo.lock packages: {rd(S + '/lock_packages.txt')}")
    p(f"- cargo machete: `{rd(S + '/machete.txt').replace(chr(10), ' | ')[:300]}`")
    p(f"- tests: `{rd(S + '/test_summary.txt').replace(chr(10), ' | ')}`; {rd(S + '/test_time.txt').splitlines()[-1] if rd(S + '/test_time.txt') else ''}")
    p("- clippy --all-targets: " + rd(S + "/clippy_summary.txt").replace("\n", "; "))
    p("- files > 800 lines: " + (", ".join(rd(S + "/loc_over_800.txt").replace("\n", ",").split(",")) or "none"))
    p()

# ---------------------------------------------------------------- idle
f = R + "/idle/idle.tsv"
if os.path.exists(f):
    rows = [l.split("\t") for l in open(f).read().strip().splitlines()[1:]]
    p("## Idle (3 independent starts, 10 s idle)")
    p()
    p("| metric | min | median | max |")
    p("|---|---|---|---|")
    names = ["startup ms", "RSS kB @1s", "RSS kB @10s", "VSZ kB", "threads", "open fds", "idle CPU ticks/10s"]
    for i, n in enumerate(names):
        v = [float(r[i + 1]) for r in rows]
        p(f"| {n} | {min(v):.0f} | {med(v):.0f} | {max(v):.0f} |")
    p()

# ---------------------------------------------------------------- load
L = R + "/load"
if os.path.isdir(L):
    p("## Load (oha through the mock upstream)")
    p()
    for c in (32, 256):
        o = oha(f"{L}/mock_direct_c{c}.oha.json")
        if o:
            p(f"- mock alone, c={c}: {o['rps']:.0f} req/s, p99 {o['p99']:.2f} ms (ceiling, shares the same 8 cores with oha)")
    p()
    p("| scenario | conc | req/s min | req/s median | p50 ms (med) | p99 ms (med) | errors | server CPU cores (med) | mock CPU cores (med) | server RSS MB end (med) |")
    p("|---|---|---|---|---|---|---|---|---|---|")
    for scen in ("getMe", "sendMessage", "big50k"):
        for c in (1, 32, 256):
            res = [oha(x) for x in sorted(glob.glob(f"{L}/r*_{scen}_c{c}.oha.json"))]
            res = [r for r in res if r]
            if not res:
                continue
            cpu = [cpu_cores(tsv(x)) for x in sorted(glob.glob(f"{L}/r*_{scen}_c{c}.srv.tsv"))]
            mcpu = [cpu_cores(tsv(x)) for x in sorted(glob.glob(f"{L}/r*_{scen}_c{c}.mock.tsv"))]
            rss = [tsv(x)[-1][1] / 1024 for x in sorted(glob.glob(f"{L}/r*_{scen}_c{c}.srv.tsv")) if tsv(x)]
            p(f"| {scen} | {c} | {min(r['rps'] for r in res):.0f} | {med([r['rps'] for r in res]):.0f} | "
              f"{med([r['p50'] for r in res]):.2f} | {med([r['p99'] for r in res]):.2f} | {sum(r['bad'] for r in res)} | "
              f"{med(cpu):.2f} | {med(mcpu):.2f} | {med(rss):.1f} |")
    p()
    p(f"(n = {len(glob.glob(L + '/r*_getMe_c1.oha.json'))} repetitions per cell; errors = non-2xx + transport errors summed over all repetitions)")
    p()
    u = oha(L + "/uniq_getMe_c32.oha.json")
    if u:
        p(f"- getMe cycling over 6000 distinct tokens (c=32; > 2048-session cap, so entries are evicted and re-fetched from upstream + SQLite save): {u['rps']:.0f} req/s, "
          f"p99 {u['p99']:.1f} ms, errors {u['bad']}; after: {rd(L + '/uniq_getMe_after.tsv')}; data dir: {rd(L + '/uniq_getMe_datadir_kb.txt').splitlines()[0] if rd(L + '/uniq_getMe_datadir_kb.txt') else ''} kB")
    s = tsv(L + "/sustained.srv.tsv")
    o = oha(L + "/sustained.oha.json")
    if s and o:
        t0 = s[0][0]
        p()
        p(f"### Sustained sendMessage c=32 ({o['rps']:.0f} req/s, p99 {o['p99']:.2f} ms, errors {o['bad']}); sampled every 5 s, then idle")
        p()
        p("| t (s) | RSS MB | threads | fds |")
        p("|---|---|---|---|")
        for r in s:
            p(f"| {r[0] - t0:.0f} | {r[1] / 1024:.1f} | {r[3]:.0f} | {r[4]:.0f} |")
    p()

# ---------------------------------------------------------------- profile
P = R + "/profile"
if os.path.isdir(P):
    p("## Profile (perf, sendMessage c=32)")
    p()
    p("```")
    p(rd(P + "/perf_stat.txt")[:1800])
    p("```")
    p("Top self-time symbols:")
    p("```")
    p("\n".join(rd(P + "/top_symbols.txt").splitlines()[:25]))
    p("```")
    p("By thread:")
    p("```")
    p("\n".join(rd(P + "/by_thread.txt").splitlines()[:8]))
    p("```")
    p()

# ---------------------------------------------------------------- heap
H = R + "/heap"
if os.path.isdir(H):
    p("## Allocations (heaptrack)")
    p()

    def num(path, pat):
        m = re.search(pat, rd(path))
        return int(m.group(1).replace(",", "")) if m else None

    pts = {}
    for f in glob.glob(H + "/heaptrack_*_summary.txt"):
        n = int(re.search(r"heaptrack_(\d+)_summary", f).group(1))
        pts[n] = (num(f, r"calls to allocation functions: ([\d,]+)"), num(f, r"temporary memory allocations: ([\d,]+)"))
        txt = rd(f)
        p(f"- N={n}: " + txt.replace("\n", "; "))
    ns = sorted(pts)
    if len(ns) >= 2 and None not in pts[ns[-1]] and None not in pts[ns[-2]]:
        a = (pts[ns[-1]][0] - pts[ns[-2]][0]) / (ns[-1] - ns[-2])
        t = (pts[ns[-1]][1] - pts[ns[-2]][1]) / (ns[-1] - ns[-2])
        p()
        p(f"**Allocations per sendMessage request (slope N={ns[-2]}..{ns[-1]}): {a:.1f}** (of which temporary: {t:.1f})")
    p()

# ---------------------------------------------------------------- webhook
f = R + "/webhook/webhook.tsv"
if os.path.exists(f):
    p("## Webhook delivery (local mode, mock getUpdates -> mock /hook)")
    p()
    rows = [l.split("\t") for l in open(f).read().strip().splitlines()]
    p("| " + " | ".join(rows[0]) + " |")
    p("|" + "---|" * len(rows[0]))
    for r in rows[1:]:
        p("| " + " | ".join(r) + " |")
    p()

# ---------------------------------------------------------------- files
F = R + "/files"
if os.path.isdir(F):
    p("## Files (64 KiB file from the mock, c=32)")
    p()
    p("| case | req/s | MB/s | p50 ms | p99 ms | errors | server CPU cores |")
    p("|---|---|---|---|---|---|---|")
    for x in sorted(glob.glob(F + "/*_c32.oha.json")):
        n = os.path.basename(x).replace(".oha.json", "")
        o = oha(x)
        if o:
            p(f"| {n} | {o['rps']:.0f} | {o['bps'] / 1e6:.1f} | {o['p50']:.2f} | {o['p99']:.2f} | {o['bad']} | {cpu_cores(tsv(F + '/' + n + '.srv.tsv')):.2f} |")
    p()

print("\n".join(out))
