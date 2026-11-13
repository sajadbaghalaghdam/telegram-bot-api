## Static

- release binary (repo profile): 7,712,376 bytes
- `cargo build --release` with sccache + deps cached (not a clean build): 47.07 s wall, 672904 KB maxrss
- clean `cargo build --release` (sccache off): 98.68 s wall, 184.44 s user, 16.77 s sys, 672828 KB maxrss
- `cargo tree | wc -l`: 420; unique normal deps: 213; Cargo.lock packages: 219
- cargo machete: `Analyzing dependencies of crates in this directory... | cargo-machete didn't find any unused dependencies in this directory. Good job! | Done!`
- tests: `test result: ok. 114 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 2.74s`; 5.87 s wall
- clippy --all-targets: warnings_total 91; clippy::collapsible_if 54; clippy::io_other_error 11; clippy::manual_is_multiple_of 9; clippy::chunks_exact_to_as_chunks 4; clippy::too_many_arguments 3; clippy::manual_repeat_n 3; clippy::needless_borrows_for_generic_args 2; clippy::manual_div_ceil 2; clippy::manual_range_contains 1; clippy::bool_comparison 1; clippy::op_ref 1
- files > 800 lines: src/client/engine.rs 2082, src/client/webhook.rs 1982, src/mtproto/uploader.rs 1527

## Idle (3 independent starts, 10 s idle)

| metric | min | median | max |
|---|---|---|---|
| startup ms | 11 | 12 | 18 |
| RSS kB @1s | 9008 | 9028 | 9064 |
| RSS kB @10s | 9128 | 9180 | 9208 |
| VSZ kB | 229828 | 229828 | 229828 |
| threads | 10 | 10 | 10 |
| open fds | 21 | 21 | 21 |
| idle CPU ticks/10s | 0 | 0 | 0 |

## Load (oha through the mock upstream)

- mock alone, c=32: 99568 req/s, p99 0.61 ms (ceiling, shares the same 8 cores with oha)
- mock alone, c=256: 123095 req/s, p99 3.90 ms (ceiling, shares the same 8 cores with oha)

| scenario | conc | req/s min | req/s median | p50 ms (med) | p99 ms (med) | errors | server CPU cores (med) | mock CPU cores (med) | server RSS MB end (med) |
|---|---|---|---|---|---|---|---|---|---|
| getMe | 1 | 13184 | 13662 | 0.07 | 0.12 | 0 | 0.48 | 0.00 | 11.1 |
| getMe | 32 | 112293 | 118617 | 0.22 | 1.02 | 0 | 2.86 | 0.00 | 12.0 |
| getMe | 256 | 171961 | 181573 | 1.23 | 4.14 | 0 | 3.99 | 0.00 | 19.8 |
| sendMessage | 1 | 4694 | 4785 | 0.20 | 0.37 | 0 | 0.67 | 0.25 | 12.2 |
| sendMessage | 32 | 44929 | 46021 | 0.64 | 1.66 | 0 | 4.71 | 0.91 | 15.7 |
| sendMessage | 256 | 47756 | 47966 | 5.05 | 11.38 | 0 | 5.07 | 0.93 | 36.5 |
| big50k | 1 | 4093 | 4163 | 0.23 | 0.38 | 0 | 0.69 | 0.23 | 19.2 |
| big50k | 32 | 36482 | 37217 | 0.80 | 1.91 | 0 | 4.65 | 0.98 | 23.0 |
| big50k | 256 | 37413 | 37821 | 6.40 | 14.67 | 0 | 4.85 | 1.12 | 67.2 |

(n = 3 repetitions per cell; errors = non-2xx + transport errors summed over all repetitions)

- getMe cycling over 6000 distinct tokens (c=32; > 2048-session cap, so entries are evicted and re-fetched from upstream + SQLite save): 51078 req/s, p99 2.9 ms, errors 0; after: rss_kb	20112	threads	47	fds	52; data dir: 5948	/tmp/scratch/bs/data kB

### Sustained sendMessage c=32 (46043 req/s, p99 1.64 ms, errors 0); sampled every 5 s, then idle

| t (s) | RSS MB | threads | fds |
|---|---|---|---|
| 0 | 13.0 | 28 | 53 |
| 5 | 14.6 | 28 | 85 |
| 10 | 14.4 | 10 | 85 |
| 15 | 14.4 | 10 | 85 |
| 20 | 14.4 | 10 | 85 |
| 25 | 14.9 | 10 | 85 |
| 30 | 14.9 | 10 | 85 |
| 35 | 14.8 | 10 | 85 |
| 40 | 15.1 | 10 | 85 |
| 45 | 14.8 | 10 | 85 |
| 50 | 14.9 | 10 | 85 |
| 55 | 14.8 | 10 | 85 |
| 60 | 14.6 | 10 | 47 |
| 65 | 14.6 | 10 | 47 |
| 70 | 12.7 | 10 | 47 |
| 75 | 12.7 | 10 | 47 |
| 80 | 12.7 | 10 | 47 |

## Profile (perf, sendMessage c=32)

```
Performance counter stats for process id '264330':

         48,282.20 msec task-clock                                                            
           350,404      context-switches                                                      
             2,854      cpu-migrations                                                        
             2,224      page-faults                                                           
   132,703,670,879      cycles                                                                  (75.06%)
                 0      instructions                                                            (74.92%)
                 0      branches                                                                (75.15%)
                 0      branch-misses                                                           (74.88%)

      10.005430526 seconds time elapsed
```
Top self-time symbols:
```
4.65% [k] native_queued_spin_lock_slowpath - - 
 3.09% [.] _int_malloc - - 
 2.19% [.] malloc - - 
 1.95% [.] cfree@GLIBC_2.2.5 - - 
 1.86% [.] _int_free_merge_chunk - - 
 1.79% [k] native_write_msr - - 
 1.57% [.] axum::serve::handle_connection::<tokio::net::tcp::listener::TcpListener, axum::routing::Router, axum::routing::Router>::{closure#0}::{closure#3} - - 
 1.26% [.] <hyper_util::client::legacy::client::Client<reqwest::connect::Connector, reqwest::async_impl::body::Body>>::send_request::{closure#0} - - 
 1.16% [.] __memmove_avx_unaligned_erms - - 
 1.04% [.] _int_free_chunk - - 
 0.92% [k] native_read_msr - - 
 0.89% [.] __libc_malloc2 - - 
 0.89% [.] <axum::middleware::from_fn::FromFn<telegram_bot_api_rs::server::telemetry_middleware, telegram_bot_api_rs::server::AppState, axum::routing::route::Route, (axum::extract::state::State<telegram_bot_api_rs::serve
 0.82% [.] <futures_util::future::future::Map<futures_util::future::try_future::MapErr<hyper::client::conn::http1::upgrades::UpgradeableConnection<reqwest::connect::sealed::Conn, reqwest::async_impl::body::Body>, <hyper_
 0.73% [.] <reqwest::async_impl::request::RequestBuilder>::send - - 
 0.67% [.] tokio::runtime::task::raw::poll::<tokio::runtime::blocking::task::BlockingTask<<tokio::runtime::scheduler::multi_thread::worker::Launch>::launch::{closure#0}>, tokio::runtime::blocking::schedule::BlockingSched
 0.66% [.] core::ptr::drop_glue::<telegram_bot_api_rs::server::AppState> - - 
 0.65% [.] <url::parser::Parser>::after_double_slash - - 
 0.64% [.] __memmove_avx_unaligned - - 
 0.64% [.] <url::host::Host<alloc::borrow::Cow<str>>>::parse_cow - - 
 0.63% [.] unlink_chunk.isra.0 - - 
 0.61% [.] <hyper::proto::h1::dispatch::Dispatcher<hyper::proto::h1::dispatch::Server<hyper_util::service::glue::TowerToHyperService<tower::util::map_request::MapRequest<axum::routing::Router, axum::serve::handle_connect
 0.58% [k] tcp_clean_rtx_queue - - 
 0.58% [.] <url::parser::Parser>::parse_path - - 
 0.57% [k] nft_do_chain - - 
```
By thread:
```
100.00% tokio-rt-worker
 0.00% watchdog
```

## Allocations (heaptrack)

- N=5000: calls to allocation functions: 621773 (29369/s); temporary memory allocations: 48167 (2275/s); peak heap memory consumption: 3.86M; peak RSS (including heaptrack overhead): 18.97M; total memory leaked: 1.99M
- N=25000: calls to allocation functions: 3081943 (115502/s); temporary memory allocations: 241319 (9043/s); peak heap memory consumption: 4.09M; peak RSS (including heaptrack overhead): 19.56M; total memory leaked: 2.16M

**Allocations per sendMessage request (slope N=5000..25000): 123.0** (of which temporary: 9.7)

## Webhook delivery (local mode, mock getUpdates -> mock /hook)

| case | updates | chats | delivered | seconds | updates_per_s | rss_kb_before | rss_kb_hwm | rss_kb_after10s | threads_after | fds_after | cpu_s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| many_chats | 50000 | 200 | 50000 | 1.72 | 29001 | 9048 | 17440 | 16192 | 10 | 86 | 5.63 |
| single_chat | 3000 | 1 | 3000 | 1.37 | 2197 | 9048 | 12204 | 11252 | 10 | 23 | 1.23 |

## Files (64 KiB file from the mock, c=32)

| case | req/s | MB/s | p50 ms | p99 ms | errors | server CPU cores |
|---|---|---|---|---|---|---|
| local_download_c32 | 25100 | 1644.8 | 1.19 | 3.21 | 0 | 5.14 |
| local_getFile_c32 | 86923 | 23.3 | 0.30 | 1.78 | 0 | 5.06 |
| nonlocal_download_c32 | 12077 | 791.3 | 2.57 | 5.00 | 0 | 3.29 |
| nonlocal_getFile_c32 | 41225 | 5.6 | 0.70 | 2.12 | 0 | 4.73 |

