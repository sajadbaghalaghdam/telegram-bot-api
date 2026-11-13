# Changelog

## 1.1.0

Parity audit against the official C++ `tdlib/telegram-bot-api`, bug fixes, and a performance and
structure pass. Checked with 179 unit/integration tests against mock servers and a smoke test of the
release binary; the MTProto paths have **not** been verified against live Telegram servers yet.

### Behaviour changes to know about
- **`--verbosity` now follows C++:** the default is `0`, which logs nothing. Use `-v 2`/`-v 3` (or
  `RUST_LOG`) to see warnings/info. `--log` with `-v 0` prints a one-line hint on stderr.
- **`RUST_LOG`** accepts `level` and `target=level` (span/field directives are no longer supported).
  An empty `RUST_LOG` is ignored in favour of `-v`.
- **Token validation matches C++:** malformed tokens return `401 Unauthorized: invalid token specified`;
  `--filter` mismatches return `421 Misdirected Request: forbidden token specified`.
- **`-s 0` disables the stats server and `-p 0` is rejected** (as in C++). `/stats` is served only on the
  dedicated `--http-stat-port`, no longer on the API port.
- **Graceful shutdown:** SIGTERM/SIGINT stops accepting, waits up to 10 s for open requests, flushes
  storage and logs, exits 0. The bundled systemd unit now uses `KillSignal=SIGTERM`, `TimeoutStopSec=15s`.
- **Webhooks (`--local`):** only genuinely local webhook URLs are run by the built-in worker and restored
  at startup; cloud webhooks are proxied untouched (including form/multipart `setWebhook` and
  `certificate`) and are never deleted by a restart. Delivery now waits for a 2xx before confirming
  updates, retries with the C++ backoff schedule, honours `Retry-After` and `max_connections`, and keeps
  per-chat ordering without one failing chat blocking the others.
- **HTTP/1.1 only** (like C++); request bodies are buffered up to 64 MiB (413 above), multipart uploads
  over 49 MB are streamed to the MTProto path as before.
- New option **`--upstream-url`** / `TELEGRAM_UPSTREAM_URL` (default `https://api.telegram.org`).

### Fixed
- Local-mode file serving could read files outside the bot's directory (path traversal / symlinks).
- `getMe` was served from a never-expiring cache and skipped authorisation; sessions now expire and
  revalidate (one refresh per expiry).
- `getUpdates` while a webhook is active now returns `409 Conflict`; error codes and `Retry-After` are
  propagated instead of being flattened to 400.
- `close` no longer stops webhook delivery; `logOut` relays upstream and cleans up only on success;
  `/test/...` routes relay to the test DC and validate tokens.
- Large uploads: correct media type for video/audio/voice/animation/video note (was always a document),
  2000 MiB cap, truncated downloads are never uploaded or cached, spool files are removed on every error
  path, fail-fast upload workers.
- MTProto: DH parameter validation (safe prime, `g` residue, range checks), `new_nonce_hash1` check,
  strict packet/header checks, factorisation edge cases (no hang/panic), bounded retries and timeouts,
  constant-time `msg_key` comparison, DC migration caps, malformed `gzip_packed` can no longer crash
  the process.
- Concurrent downloads of one file no longer corrupt each other's partial file.
- Startup: validation happens before any directory is created, directories are probed, `--temp-dir` is
  resolved against `--files-dir`, bind/IP errors are fatal and readable, watchdog actually fires.
- `--log` file output with size-based rotation (`<log>.old`), `SIGUSR1` reopens the log, `SIGHUP` ignored.
- Storage: write errors are logged, schema migrations are checked, writes go through one batching
  writer thread, stale rows (unused for 30 days) are purged from `bots`/`file_cache` (webhooks never).

### Performance / structure
- Webhook delivery: 32k to 50k updates/s, allocations per update 135 to 50 (shared test machine).
- MTProto: in-place IGE and pooled buffers; upload/download allocations per part 18 to 3-4, peak
  memory flat in file size (20 MiB upload, 9 MiB download).
- Malloc arenas = worker threads, blocking pool capped at 16, `malloc_trim` kept: +22% to +40% relay
  throughput, 6,000-token load went from 47 to 11 threads.
- Smaller binary (7.1 MB), `rsa`/`num-traits`/`regex` stacks removed, no `clippy`/build warnings.
- `engine.rs` split into focused modules; `webhook/` and `mtproto/` are module directories.
- See `BENCHMARKS.md` and `bench/` for the methodology and numbers.

### Known gaps (see `CPP_VS_RUST_COMPARISON.md`)
- No per-bot `/stats` blocks or `?id=`/`?v=`; no per-chat send cap; `close` does not return the
  "already closed" 400/10-minute 429; MTProto uploads return synthetic `file_id`/`file_unique_id`;
  `-u`, `-g`, `-c`, CPU-affinity flags and `--memory-verbosity` are accepted but have no effect.
- Cached-key MTProto worker connections reuse the session setup without `initConnection`
  (unchanged from 1.0.x; TDLib sends it per session) - verify with a live large-file test.
