# Performance baseline

Measured on commit `67c2155` (branch `audit-fixes`), **before any optimisation work**. Every later
change should be judged by re-running `bench/run_all.sh` and comparing against the tables below.
Raw outputs of this baseline are committed under `bench/results/baseline/` (`summary.md` is the
generated table set; the rest are the oha JSON files, `/proc` samples, perf and heaptrack output).

## 1. Environment

| | |
|---|---|
| CPU / RAM | AMD EPYC 7313P (VM), 8 vCPUs, 3.4 GB RAM, kernel 7.0.0-43 |
| Toolchain | rustc/cargo 1.100.0, oha 1.16.0, perf 7.0.14, heaptrack |
| Release profile | opt-level 3, fat LTO, codegen-units 1, strip, panic=abort (unchanged) |
| Machine state | **shared**: other agents were building concurrently (load average 2.2 at start, 1.5-2.3 between suites). oha, the mock and the server all run on the same 8 vCPUs over loopback. Absolute numbers are therefore indicative; the 3 repetitions per cell are reported as min / median. |
| Server flags | `--api-id 12345 --api-hash ... -p 18080 -d <tmp>`; default `--verbosity 0`, i.e. **logging is off**, so tracing cost is not part of the baseline. `--local` only for the webhook and file suites. |

## 2. Methodology (and an important deviation)

* **The upstream URL is hard-coded.** `src/client/engine.rs` sets `upstream_base_url:
  "https://api.telegram.org"`; there is no CLI option or env var (tests overwrite the field
  directly), and `--proxy` only applies to webhook delivery unless `TELEGRAM_UPSTREAM_PROXY` is set,
  and still needs TLS. Since `src/` must not change, `bench/build.sh` builds a **copy** of the
  committed source (`git archive HEAD`) with the one-line patch `bench/patches/upstream-url-env.patch`
  (read `BENCH_UPSTREAM_BASE_URL` at start-up, otherwise unchanged) and with symbols
  (`strip=false`, `debug=line-tables-only`; no codegen change). All runtime numbers come from that
  binary. Static numbers (size, build time, clippy, tests) come from the unmodified repo.
  *Optimisation candidate #0: a real `--upstream-url` option, which makes this benchmark patch-free.*
* **Mock upstream** (`bench/mock-upstream`, ~150 lines of axum, 2 threads): answers getMe,
  sendMessage, getUpdates (stateful, offset/limit aware, long-polls when empty), getFile, file
  download, setWebhook/deleteWebhook/any method, and receives webhook deliveries on `/hook`.
  It is plain HTTP/1.1, so **TLS to upstream (rustls/ring) is not in the baseline**; real
  deployments pay that on every upstream call and on connection setup.
  Not a bottleneck: oha straight at the mock reaches 100k req/s (c=32) / 123k req/s (c=256), more than
  2x the server's best relayed rate; the mock used ~0.9-1.1 cores while the server used ~4.7-5.
* **Load**: `oha -z 15s` through server -> mock, c = 1 / 32 / 256, 3 repetitions each, fresh server
  per repetition, 3 s warm-up. Server RSS/threads/fds/CPU sampled every second from `/proc`; CPU
  "cores" = (utime+stime)/elapsed. Errors = non-2xx + transport errors (deadline aborts excluded).
* **Profiles**: `perf record -F 999 -g` (flat self-time) and a DWARF call-graph run, both on the
  sendMessage c=32 load; `perf stat`. The VM exposes no hardware instruction/branch counters
  (they read 0), only cycles and software events.
* **Allocations**: `heaptrack` on the same server, N=5000 and N=25000 sendMessage requests (c=32);
  allocations/request is the slope, which removes start-up cost.

Run it: `bench/run_all.sh` (about 25 min; build + static + idle + load + profile + heap + webhook +
files, prints the summary, writes `bench/results/<UTC stamp>/`). Subsets: `SUITES="load heap"`,
`SKIP_BUILD=1`, `SKIP_COLD_BUILD=1`, `REPS=`, `DUR=`, `SUSTAINED=`. Needs sudo for perf.

## 3. Baseline

### 3.1 Static

| metric | value |
|---|---|
| Release binary (repo profile) | 7,712,376 bytes (7.4 MiB) |
| Clean `cargo build --release` (sccache off) | 98.7 s wall, 184 s user, 673 MB peak RSS |
| Build with sccache and deps cached | 47 s wall |
| `cargo tree \| wc -l` / unique normal deps / Cargo.lock packages | 420 / 213 / 219 |
| `cargo machete` | no unused dependencies |
| `cargo test` | 114 passed, 0 failed; 2.7 s test run, 5.9 s wall incl. incremental build |
| `cargo clippy --all-targets` | **91 warnings** (CONTRIBUTING.md requires clean): `collapsible_if` 54, `io_other_error` 11, `manual_is_multiple_of` 9, `chunks_exact_to_as_chunks` 4, `too_many_arguments` 3, `manual_repeat_n` 3, `needless_borrows_for_generic_args` 2, `manual_div_ceil` 2, `manual_range_contains` 1, `bool_comparison` 1, `op_ref` 1 |
| Source | 11,908 lines in `src/`; files > 800 lines: `client/engine.rs` 2082, `client/webhook.rs` 1982, `mtproto/uploader.rs` 1527 |

`cargo bloat --release --crates` (.text, 5.7 MiB with symbols build): [Unknown] 1.6 MiB (28%, mostly bundled
SQLite C), std 688 KiB, telegram_bot_api_rs 526 KiB, rustls 351 KiB, ring 284 KiB, tokio 275 KiB,
clap_builder 223 KiB, axum 150 KiB, reqwest 148 KiB, **regex_syntax 124 KiB + regex_automata 114 KiB**
(pulled in by tracing-subscriber `env-filter`), hyper_util 120 KiB, libsqlite3_sys 114 KiB.
Largest functions: `async_main` closure 67 KiB, `sqlite3VdbeExec` 52 KiB, `universal_call_inner` 51 KiB
(full list in `bench/results/baseline/static/bloat_functions.txt`).

### 3.2 Idle (3 independent starts)

| metric | min / median / max |
|---|---|
| Start-up to first accepted connection | 11 / 12 / 18 ms |
| RSS after 1 s / 10 s | 9.0 / 9.0 / 9.1 MB; 9.1 / 9.2 / 9.2 MB |
| VSZ | 229,828 kB |
| Threads / open fds | 10 / 21 (8 tokio workers + main + watchdog; 21 fds = 5 SQLite connections x2-3 files, sockets, epoll) |
| CPU in 10 s idle | < 1 tick (below measurement resolution) |

### 3.3 Load through the mock (3 repetitions, 15 s each)

| scenario | conc | req/s min | req/s median | p50 ms | p99 ms | errors | server CPU cores | mock CPU cores | server RSS MB (end) |
|---|---|---|---|---|---|---|---|---|---|
| getMe (served from cache) | 1 | 13,184 | 13,662 | 0.07 | 0.12 | 0 | 0.48 | 0.00 | 11.1 |
| getMe | 32 | 112,293 | 118,617 | 0.22 | 1.02 | 0 | 2.86 | 0.00 | 12.0 |
| getMe | 256 | 171,961 | 181,573 | 1.23 | 4.14 | 0 | 3.99 | 0.00 | 19.8 |
| sendMessage (small JSON, relayed) | 1 | 4,694 | 4,785 | 0.20 | 0.37 | 0 | 0.67 | 0.25 | 12.2 |
| sendMessage | 32 | 44,929 | 46,021 | 0.64 | 1.66 | 0 | 4.71 | 0.91 | 15.7 |
| sendMessage | 256 | 47,756 | 47,966 | 5.05 | 11.38 | 0 | 5.07 | 0.93 | 36.5 |
| sendMessage 50 KB body | 1 | 4,093 | 4,163 | 0.23 | 0.38 | 0 | 0.69 | 0.23 | 19.2 |
| 50 KB body | 32 | 36,482 | 37,217 | 0.80 | 1.91 | 0 | 4.65 | 0.98 | 23.0 |
| 50 KB body | 256 | 37,413 | 37,821 | 6.40 | 14.67 | 0 | 4.85 | 1.12 | 67.2 |

Throughput saturates around c=32 (the machine, not the server's queueing, is the limit: 4.7-5 of 8
cores are used by the server and oha/mock use the rest). Relayed sendMessage costs about
**~100 us of server CPU per request** (4.71 cores / 46 k req/s), of which about half is kernel TCP
work (two loopback legs).

Other load cases:

* getMe cycling over 6,000 distinct tokens (c=32, exceeds the 2,048-session cap, so entries are evicted
  and re-fetched from upstream, with a SQLite write each time): 51,078 req/s, p99 2.9 ms, 0 errors;
  afterwards RSS 20 MB, **47 threads**, 52 fds; SQLite files 5.9 MB.
* Sustained sendMessage, c=32, 60 s, 46,043 req/s, p99 1.64 ms, 0 errors; RSS sampled every 5 s:
  13.0 -> 14.6 -> 14.4 ... 14.9 / 15.1 / 14.8 / 14.6 MB at t=60, then **12.7 MB** after the load ends.
  Threads 28 during ramp-up, 10 from t=10 s. fds 85 under load, 47 after (idle pooled upstream sockets).
  **No growth: flat within +-0.5 MB for 60 s.**

### 3.4 CPU profile (sendMessage c=32, `perf`)

`perf stat` (10 s): 48.3 s task-clock (4.8 cores), 132.7 G cycles, **350 k context switches (~35 k/s,
about 0.75 per request)**, 2.9 k cpu-migrations, 2.2 k page faults. Instructions/branches unavailable (VM).

By DSO: **kernel 47%** (TCP send/receive, softirq, futex, scheduler; `writev` alone is 27% inclusive),
**server binary 37%**, **libc 15%** (almost entirely malloc/free), vdso 0.5%.
By thread: 100% `tokio-rt-worker` (watchdog thread 0.00%).

Top self-time symbols (full top 60 in `bench/results/baseline/profile/top_symbols.txt`):

| % | symbol | where |
|---|---|---|
| 4.65 | `native_queued_spin_lock_slowpath` | kernel (socket/queue locks) |
| 3.09 + 2.19 + 1.95 + 1.86 + 1.04 + 0.89 + 0.63 | `_int_malloc`, `malloc`, `cfree`, `_int_free_merge_chunk`, `_int_free_chunk`, `__libc_malloc2`, `unlink_chunk` | glibc allocator = **11.7% self** |
| 1.57 | `axum::serve::handle_connection::{closure#3}` | axum connection task |
| 1.26 | `hyper_util ... Client<reqwest::Connector>::send_request` | outbound relay (src/client/engine.rs `universal_call_inner`) |
| 0.89 | `axum::middleware::from_fn::FromFn<telemetry_middleware>` | src/server.rs `telemetry_middleware` |
| 0.73 | `reqwest::RequestBuilder::send` | outbound relay |
| 0.66 | `drop_glue<AppState>` | five-`Arc` `AppState` cloned and dropped per request (middleware + extractor) |
| 0.65+0.64+0.58+0.29 | `url::Parser::after_double_slash`, `Host::parse_cow`, `parse_path`, `ParseOptions::parse` | reqwest re-parsing the `format!`-built upstream URL: **~2.2%** |
| 0.45+0.33+0.30+0.24 | `serde_json Value::deserialize`, `parse_str`, `skip_to_escape`, `BTreeMap<String,Value>` drop | full-`Value` parse of every upstream reply: **~1.3%** (`generic_post_handler`, `note_upstream_reply`) |
| 0.53 / 0.38 / 0.30 | `generic_post_handler`, `universal_call_inner`, `universal_call` closures | src/handlers/generic.rs, src/client/engine.rs |
| 0.34 | `reqwest::Connector::clone` | per-request client clone inside reqwest |

Inclusive (DWARF run): `free` 9.2% and `malloc` 8.1% inclusive, of which **`__lll_lock_wake_private`
7.7% and `__lll_lock_wait_private` 4.8%**: glibc arena-lock contention (see 3.6).

### 3.5 Allocations (heaptrack)

| N requests | allocation calls | temporary allocs | peak heap | live at exit |
|---|---|---|---|---|
| 5,000 | 621,773 | 48,167 | 3.86 MB | 1.99 MB |
| 25,000 | 3,081,943 | 241,319 | 4.09 MB | 2.16 MB |

**123.0 allocations per relayed sendMessage request** (~9.7 of them short-lived "temporary"
allocations). Peak heap is a flat ~4 MB (the RSS growth under load is socket buffers and glibc
arenas, not live objects). Attribution, from the folded heaptrack stacks (inlining makes this
approximate; `bench/results/baseline/heap/heap_top_25000.txt`):

* ~60% (~74 allocs/req) inside the `generic_post_handler` async block, which inlines
  `universal_call` / `universal_call_inner`: `Path<(String,String)>` extraction, `Query<HashMap>`,
  `to_string()` of the content type, `to_ascii_lowercase()` of the method (1/req, confirmed),
  `format!` URL, building the reqwest request, **parsing the upstream reply into
  `serde_json::Value` (~11-15 allocs/req: BTreeMap nodes and Strings) just to read `ok` /
  `error_code`**, and again in `note_upstream_reply`.
* ~6% axum `clone_box` of the route/middleware stack per request and ~4% boxed-service calls.
* ~5% `url::parse_url` (+0.8% `into_url`): reqwest re-parsing the URL per request.
* ~1.6% `telemetry_middleware`.

### 3.6 Webhook delivery (`--local`, mock getUpdates -> mock `/hook`)

| case | updates | delivered | time | updates/s | RSS before -> HWM -> after | CPU time |
|---|---|---|---|---|---|---|
| 200 chats (parallel queues) | 50,000 | 50,000 | 1.72 s | **29,001** | 9.0 -> 17.4 -> 16.2 MB | 5.6 s (~113 us/update) |
| 1 chat (serial FIFO) | 3,000 | 3,000 | 1.37 s | 2,197 | 9.0 -> 12.2 -> 11.3 MB | 1.2 s |

Threads 10 after; fds 86 after (up to `max_connections` keep-alive sockets to the webhook, 23 with one chat).
`getWebhookInfo` reports `pending_update_count: 0` afterwards. Serial throughput (~0.45 ms per
update per chat, i.e. one round-trip) is by design (per-chat FIFO, as in C++). Not CPU-profiled.

### 3.7 Files (64 KiB file from the mock, c=32, 10 s)

| case | req/s | MB/s | p50 / p99 ms | errors | server CPU cores |
|---|---|---|---|---|---|
| `--local` getFile (cache hit after the first) | 86,923 | 23 | 0.30 / 1.78 | 0 | 5.06 |
| `--local` file download from disk (`/file/bot.../documents/file_1.bin`) | 25,100 | 1,645 | 1.19 / 3.21 | 0 | 5.14 |
| non-local getFile (relayed every time) | 41,225 | 6 | 0.70 / 2.12 | 0 | 4.73 |
| non-local download (streamed from upstream) | 12,077 | 791 | 2.57 / 5.00 | 0 | 3.29 |

First `--local` getFile (which downloads the file to disk): 2.2 ms; non-local: 1.0 ms.

## 4. Findings / suspicious items

1. **No leak, runaway task or fd growth observed.** 60 s sustained: RSS flat (13-15 MB); after the load
   RSS drops to 12.7 MB, threads return to 10, fds to 47 (pooled idle sockets, bounded by
   `pool_max_idle_per_host(64)`).
2. **glibc arena contention.** `main.rs` calls `mallopt(M_ARENA_MAX, 4)` but the runtime has 8 worker
   threads; `__lll_lock_wake/wait_private` is ~12% inclusive, malloc+free is ~17% inclusive (9.2% free + 8.1% malloc), 0.75
   context switches per request. Allocation count (123/req) is the multiplier.
3. **`watchdog` calls `malloc_trim(0)` every 10 s.** This is what produces the 14.4 -> 12.7 MB RSS step
   after load; it takes arena locks on a live server. Its cost (latency blips) is not isolated by this
   baseline.
4. **Blocking-pool thread growth.** The distinct-token load pushed the process to 47 threads (tokio
   `spawn_blocking` for SQLite saves), unbounded by config (tokio default 512), and each getMe refresh
   does a SQLite write (`bots` table grows by one row per token that upstream accepts; no eviction;
   5.9 MB for 6,000 tokens).
5. **Whole-reply JSON parse on the hot path** (`generic_post_handler` + `note_upstream_reply`) and per-request
   URL building/parsing: ~3.5% self CPU and ~20-25 allocs/req for information that is a few bytes.
6. **Webhook path** clones the polled update array (`classify_poll` does `a.clone()`) and re-serialises each
   `Value` (`serde_json::to_vec(u)`) per update; 113 us CPU/update.
7. 50 KB bodies cost 19% throughput vs the small body at c=32 and double RSS at c=256 (67 vs 36 MB);
   dominated by kernel copy of 2x50 KB per request; copies in user space not separately measured.
8. Quality gate: clippy is not clean (91 warnings) and three files exceed 800 lines, contrary to CONTRIBUTING.md.

## 5. Optimisation candidates, ranked by measured impact

| # | candidate | evidence (baseline) | expected effect |
|---|---|---|---|
| 1 | Cut allocations on the relay path (123/req): typed/scan-only reply inspection instead of `serde_json::Value`, `&str` for method/token/content-type, no `to_ascii_lowercase`, `Path` borrowing, `Bytes` pass-through | 60% of allocs in `generic_post_handler`; malloc/free 11.7% self, ~17% inclusive | largest: allocator + lock contention is the biggest user-space block (up to ~15% CPU) |
| 2 | Allocator arena policy: `M_ARENA_MAX` >= worker threads (or a per-thread-cache allocator), re-evaluate `malloc_trim` every 10 s | 12% inclusive in arena lock wait/wake, 35 k context switches/s | direct: 5-12% CPU, fewer context switches. Needs an A/B run; RSS headroom is large (15 MB) |
| 3 | Pre-parse the upstream base URL once; build per-request URL without `format!` + reparse (or reuse `Url` + `path_segments_mut`) | url crate ~2.2% self, ~6 allocs/req | ~2-3% CPU, -6 allocs/req |
| 4 | Collapse `AppState` into one `Arc`, make telemetry a cheap tower layer / drop per-request state clones; avoid per-request axum `clone_box` | `drop_glue<AppState>` 0.66%, telemetry 0.89%, ~7-10 allocs/req in clone_box | ~1.5-2% CPU, -8..10 allocs/req |
| 5 | Add `--upstream-url` (or env) option and a CI-runnable benchmark | benchmark needs a patched copy | makes every number above patch-free and gates regressions |
| 6 | Bound blocking threads / dedicated SQLite writer thread; skip the DB write when the cached profile is unchanged | 47 threads, 1 write per token refresh | resource hygiene; fewer threads; no throughput change expected |
| 7 | Webhook: deliver the raw update slice instead of `Vec<Value>` clone + re-serialise | 113 us CPU/update, 29 k updates/s | est. 20-40% less CPU per update (unmeasured, needs a perf run on the webhook suite) |
| 8 | Binary size: drop `tracing-subscriber/env-filter` (regex stack ~240 KiB, 4.3% of .text) if RUST_LOG support is not required; evaluate `opt-level="s"` only with measurements | bloat report | ~0.25 MB (3%) |
| 9 | Hygiene: fix the 91 clippy warnings (54 are trivial `collapsible_if`), split the three >800-line files (engine.rs 2082, webhook.rs 1982, uploader.rs 1527) | static section | no runtime effect; required by CONTRIBUTING.md |

Impact estimates are from profile shares, not from experiments; each must be confirmed by re-running
the suite (the commit message of an optimisation should quote before/after from `summary.md`).

## 6. Not measured / limitations

* **TLS to upstream** (real traffic is https): handshake and rustls/ring costs are absent; mock is plain HTTP.
* **MTProto** upload/download (> 20/50 MB, `uploader.rs`, `downloader.rs`, `handshake.rs`) needs real
  Telegram DCs and an authorised account: not measurable here. Large-file RSS (O(1) streaming claim) unverified.
* Real Telegram latency, rate limits, multipart uploads of small/medium files, `sendMediaGroup` / local-path
  rewriting (JSON bodies in `--local` mode are additionally scanned for `/`), stats server (`-s`), `--filter`.
* Logging enabled (`-v 3+`): tracing/fmt cost not in the baseline (default verbosity 0 = off).
* Hardware counters (IPC, cache and branch misses): not exposed by this VM.
* Webhook path has no perf/heaptrack profile; webhook delivery targets are a local plaintext mock
  (no TLS, no 410/429 retry behaviour).
* The machine was shared; repeat runs show ~+-5% between repetitions (see min vs median in 3.3),
  and results depend on oha and the mock competing for the same 8 vCPUs.
* Runtime binary is the patched copy with debug symbols (44 MB on disk); only static numbers use the
  repository's stripped 7.7 MB binary.

## 7. Round 4A results (HTTP relay core: `src/client/*` except webhook, `src/handlers`, `src/server.rs`)

Branch `worktree-agent-a008174bab5de4129` (13 commits on top of `a91bf81`). Same scenarios as
section 3, same bench/ scripts, mock upstream on plain HTTP. The machine was heavily shared during this round (load
average 5-10, other agents building), so **absolute req/s varied +-20% between runs**. Therefore:

* allocation counts (heaptrack slope, N=5,000 and 25,000 sendMessage c=32) are the reliable metric;
* CPU per request (server `utime+stime` / requests, interleaved A/B runs of base and final binary,
  c=32, 8 s each, 4-5 pairs) is the reliable *throughput* metric; differences below about +-4% are noise here.

### 7.1 Allocations per relayed sendMessage (heaptrack slope)

| step | allocs/request |
|---|---|
| baseline (`67c2155`) | 123.0 |
| engine split + `RelayCall`, allocation-free method matching | 122.0 |
| typed minimal reply scan instead of `serde_json::Value` (handler, `note_upstream_reply`, logOut) | 96.0 |
| single-`Arc` `AppState` | 96.0 (neutral) |
| `RawPathParams`, lazy query map, borrowed content type, alloc-free token validation | 91.0 |
| local-file blocking-pool work, atomic getFile download, body bounds, manager single-refresh | 91.0 |
| *(tried and reverted)* upstream URL pre-parsed once, `Url` clone + `set_path` | 89.0, no CPU change |

### 7.2 Final binary vs baseline binary (interleaved A/B, median of runs; `user_us`/`sys_us` = server CPU per request)

| scenario (c=32) | baseline | final | verdict |
|---|---|---|---|
| sendMessage, CPU per request (5 pairs) | 102.9 us (user 59.2, sys 43.8) | 104.8 us (user 59.9, sys 44.5) | within noise |
| sendMessage, p99 | 1.65-3.67 ms | 1.58-3.89 ms | within noise |
| getMe from cache (4 pairs) | 25.2 us/req | 25.8 us/req | within noise |
| sendMessage 50 KB body (4 pairs) | 125.5 us/req, RSS 17-19.5 MB | 127.5 us/req, RSS 19.3-20.0 MB | within noise |
| `--local` file download 64 KiB (3 pairs) | 183-200 us/req | 194-199 us/req | within noise |
| `--local` getFile cache hit (3 pairs) | 57-62 us/req | 55-57 us/req | -4% (marginal) |
| RSS after sendMessage load | 14.4-14.6 MB | 14.6-15.0 MB | +0.3 MB |

**Conclusion of the round:** the relay core is now split into small modules, has 26% fewer allocations per request and
several resource-safety fixes (below), but **no measurable CPU or throughput gain**: user-space CPU per relayed request
stayed at ~59 us. The profile in section 3.4 attributed ~12% to malloc/free; removing a quarter of the allocations is
below what this shared machine can resolve. What remains per request (about 42 of the 91 allocations) is inside
reqwest/hyper's client path (request/header maps, URL clone, pool checkout, response body), axum's per-request
`clone_box` and the `from_fn` telemetry future, plus ~44 us of kernel TCP time; none of that is reachable from the
files owned by this round. The levers with a real expected effect are outside it: `M_ARENA_MAX` / `malloc_trim` in
`main.rs`/`watchdog.rs` (candidate #2), and the `Mutex` ring in `stats.rs` that `telemetry_middleware` takes twice per request.

### 7.3 Behaviour/resource fixes made on the way (each has a regression test)

* Large multipart upload spool file leaked on any mid-stream error or client disconnect; now an RAII guard
  (`aborted_large_multipart_upload_leaves_no_temp_file`).
* Remote-URL -> MTProto path uploaded a truncated file when the download stream broke; now an error.
* `--local` getFile persisted truncated downloads (errors ignored) and raced on the target path; now temp file + rename
  (`local_mode_interrupted_download_is_neither_served_nor_cached`).
* Request bodies were buffered without a bound; now 64 MiB (413 / 400), large-upload text fields 1 MiB.
* Blocking `std::fs` calls on runtime workers (`resolve_local_path`, `resolve_file_on_disk`, getFile) moved to the
  blocking pool, one hop per file served (no throughput change, see 7.2).
* Expired getMe session: every concurrent request refreshed it from upstream; now exactly one does.
