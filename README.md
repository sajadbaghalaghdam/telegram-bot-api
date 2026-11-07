# Telegram Bot API Server — Pure Rust Edition

[![License: BSL-1.0](https://img.shields.io/badge/License-BSL%201.0-blue.svg)](LICENSE)
[![Rust Edition](https://img.shields.io/badge/Rust-2024%20Edition-orange.svg)](Cargo.toml)
[![Bot API](https://img.shields.io/badge/Bot%20API-9.5-0088cc.svg)](https://core.telegram.org/bots/api)
[![Build Instructions](https://img.shields.io/badge/Build-Interactive%20Generator-success.svg)](https://sajadbaghalaghdam.github.io/telegram-bot-api/build.html)

A high-performance, ultra-low-memory **Telegram Bot API Server** written in **100% pure Rust**, designed as a **100% CLI-compatible drop-in replacement** for the official C++ [`tdlib/telegram-bot-api`](https://github.com/tdlib/telegram-bot-api).

---

## Why Use This Implementation?

Developers running self-hosted `telegram-bot-api` instances in production commonly face three bottlenecks with the official C++ TDLib server:

1. **High Memory Consumption (`500 MB – 1+ GB` RSS)**: The C++ server instantiates a full stateful TDLib actor hierarchy, in-memory dialog/message caches, and binlog buffers for every active bot token (`+8.5 MiB to +90 MiB` per bot in production).
2. **Heavy Build & Deployment Dependencies**: Compiling the C++ server requires `CMake`, `gperf`, `OpenSSL`, `zlib`, a C++17 toolchain, and **`3.5+ GB` of free RAM** for 15–30 minutes just to compile TDLib's generated TL schemas.
3. **Heap Fragmentation Under Heavy Multipart / Media Traffic**: Frequent large file uploads and multipart payloads leave dirty pages in the allocator unless manually tuned.

### What This Rust Rewrite Changes

* **15–30 MB Production RAM Footprint**: Uses a shared asynchronous Tokio runtime (`axum` + `reqwest`/`rustls`), a bounded lock-free L1 cache (`DashMap`, 4,096 entries max), and a single shared **SQLite WAL** persistence engine (`PRAGMA synchronous=NORMAL`, `mmap_size=0`).
* **O(1) Constant-Memory File Streaming**: Streams local files (`--local`) and `multipart/form-data` uploads up to `2000 MB` in fixed **`64 KB` chunks** (`tokio_util::io::ReaderStream`) with **zero heap growth**, paired with automatic periodic `malloc_trim(0)` on Linux.
* **Zero-Downtime Migration from C++**: Automatically parses existing TDLib `webhooks_db.binlog` files inside `--dir` on startup, migrates registered bot tokens and webhook URLs into SQLite, synchronizes update offsets, and continues delivering webhook updates without requiring bots to re-register via `setWebhook`.
* **18-Second Compilation, Single `5.8 MB` Binary**: Statically bundles TLS (`rustls`) and SQLite (`rusqlite` bundled)—no external `OpenSSL` or `gperf` required.

---

## Benchmarks: Official C++ (`v10.3`) vs. Pure Rust (`v9.5`)

### 1. Live Production Telemetry (Linux x86_64, 11 Active Bots, ~286 req/sec)

Measured on the same production server (`systemd` service, 11 active high-traffic bots using local webhooks and `--local` file path delivery):

| Metric | Official C++ (`telegram-bot-api`) | Pure Rust (`telegram-bot-api-rs`) | Improvement |
| :--- | :---: | :---: | :---: |
| **Resident Memory (RSS)** | `988.1 MiB` (`13.0%` system RAM) | **`15.4 MiB – 29.7 MiB`** (`0.1% – 0.3%` system RAM) | **~33x – 64x lower RAM** |
| **Binary Size (Stripped Release)** | `32.45 MiB` (`34,030,592 B`) | **`5.84 MiB`** (`6,123,520 B`) | **5.56x smaller** |
| **Clean Build Time (Release)** | `~18 min` (`> 3.5 GB` RAM required) | **`17.8 sec`** (`< 400 MB` RAM required) | **~60x faster build** |

### 2. Controlled Head-to-Head Microbenchmarks

Measured on an isolated host with identical CLI flags (`--local --http-port ... --http-stat-port ...`), 64 concurrent connections (`c = 64`), and nanosecond-resolution timers:

| Category | Official C++ (`v10.3`) | Pure Rust (`v9.5`) | Delta |
| :--- | :---: | :---: | :---: |
| **Cold Startup to Ready Socket** | `17.54 ms` (`17,536,100 ns`) | **`11.28 ms`** (`11,281,600 ns`) | **1.55x faster** |
| **Idle Working Set (RSS)** | `14.57 MiB` (`15,278,080 B`) | **`9.52 MiB`** (`9,981,952 B`) | **1.53x lower** |
| **Idle Private Memory** | `10.30 MiB` (`10,805,248 B`) | **`4.48 MiB`** (`4,698,112 B`) | **2.30x lower** |
| **Memory Growth per Active Bot** | `+8.55 MiB` RSS per bot | **`< 1 KiB`** per bot (shared pool) | **O(1) multi-bot scaling** |
| **`getMe` Throughput (`c=64`)** | `25,278.41 RPS` (`p50: 2.41 ms`) | **`31,261.97 RPS`** (`p50: 1.93 ms`, `min: 94.6 µs`) | **+23.67% higher RPS** |
| **`/stats` Endpoint (`c=64`)** | `11,536.22 RPS` (`p50: 5.34 ms`) | **`31,980.38 RPS`** (`p50: 1.91 ms`) | **2.77x higher RPS** |
| **64 MiB Local File Stream (`--local`)** | `1,222+ MB/s` | **`1,222.59 MB/s`** (`10.256 Gbps`, `0 B` heap delta) | **Zero heap growth** |

---

## Pre-Built Optimized Binaries

Pre-compiled release binaries (`lto = "fat"`, `codegen-units = 1`, `strip = true`, `panic = "abort"`) are available on the [**Releases**](https://github.com/sajadbaghalaghdam/telegram-bot-api/releases) page for multiple CPU architectures:

| Target Platform / CPU Architecture | Binary Asset | Recommended Use Case |
| :--- | :--- | :--- |
| **Linux x86_64 (Generic)** | `telegram-bot-api-linux-x86_64` | Compatible with all 64-bit Intel/AMD Linux servers (glibc 2.31+) |
| **Linux x86_64-v3 (AVX2 / SIMD)** | `telegram-bot-api-linux-x86_64-v3-avx2` | Maximum throughput for Intel Haswell+ (2013+) and AMD EPYC / Ryzen servers |
| **Linux ARM64 (`aarch64`)** | `telegram-bot-api-linux-arm64` | AWS Graviton, Oracle Cloud Ampere A1, Hetzner ARM64, Raspberry Pi 4/5 |
| **Windows x86_64 (MSVC)** | `telegram-bot-api-windows-x86_64.exe` | 64-bit Windows Server & Windows 10/11 |

---

## Build Instructions

Use the interactive [**Build Instructions Generator**](https://sajadbaghalaghdam.github.io/telegram-bot-api/build.html) to get exact copy-paste commands for Ubuntu, Debian, Alpine, CentOS/Rocky/AlmaLinux, Fedora, Arch Linux, macOS, FreeBSD, or Windows.

### Quick Build on Linux (Optimized for Host CPU)

```bash
# 1. Install Rust toolchain (if not already installed)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# 2. Clone and build with native CPU instructions + fat LTO
git clone https://github.com/sajadbaghalaghdam/telegram-bot-api.git
cd telegram-bot-api
RUSTFLAGS="-C target-cpu=native" cargo build --release

# 3. Install as drop-in replacement
sudo install -m 755 target/release/telegram-bot-api-rs /usr/local/bin/telegram-bot-api
```

---

## Drop-In CLI Compatibility (All 24 Official Flags)

Every CLI flag and environment variable from the official C++ `telegram-bot-api` is supported identically—no changes to your `systemd` unit or Docker command line are needed:

```text
Usage: telegram-bot-api-rs [OPTIONS]

Options:
      --api-id <API_ID>                          Application identifier for Telegram API access [env: TELEGRAM_API_ID=]
      --api-hash <API_HASH>                      Application identifier hash for Telegram API access [env: TELEGRAM_API_HASH=]
      --local                                    Allow the Bot API server to serve local requests [env: TELEGRAM_LOCAL=]
  -p, --http-port <PORT>                         HTTP listening port [default: 8081]
  -s, --http-stat-port <STAT_PORT>               HTTP statistics port [default: 0]
  -d, --dir <DIR>                                Server working directory [default: .]
  -t, --temp-dir <TEMP_DIR>                      Directory for storing HTTP server temporary files [default: /tmp]
      --files-dir <FILES_DIR>                    Directory for storing persistent bot files (defaults to --dir)
      --filter < remainder/modulo >              Accept only bots with 'bot_user_id % modulo == remainder'
  -c, --max-connections <MAX_CONNECTIONS>        Default value of maximum webhook connections per bot [default: 40]
      --max-webhook-connections <MAX>            Alias for --max-connections
      --cpu-affinity <MASK>                      CPU affinity bitmask for worker threads
      --main-thread-affinity <MASK>              CPU affinity bitmask for main thread
      --proxy <PROXY>                            Proxy for outgoing Telegram requests (ip:port:user:pass, http://..., socks5://...)
      --http-ip-address <IP>                     Local IP address for HTTP connections [default: 0.0.0.0]
      --http-stat-ip-address <IP>                Local IP address for HTTP statistics connections [default: 0.0.0.0]
  -l, --log <FILE>                               Path to the file where the log will be written
  -v, --verbosity <LEVEL>                        Log verbosity level (0=fatal, 1=error, 2=warning, 3=info, 4=debug, 5+=trace) [default: 1]
      --memory-verbosity <LEVEL>                 Memory log verbosity level [default: 3]
      --log-max-file-size <BYTES>                Maximum size of the log file in bytes before rotation [default: 2000000000]
  -u, --username <USERNAME>                      Effective user name to switch to after binding privileged ports
  -g, --groupname <GROUPNAME>                    Effective group name to switch to after binding privileged ports
  -h, --help                                     Print help
  -V, --version                                  Print version
```

See [`CPP_VS_RUST_COMPARISON.md`](CPP_VS_RUST_COMPARISON.md) for a complete side-by-side comparison of all 24 CLI options and internal runtime behaviors.

---

## Production `systemd` Service Configuration

To migrate an existing C++ `telegram-bot-api` service to the Rust binary with ultra-low memory overhead:

```ini
[Unit]
Description=Telegram Bot API Server (Pure Rust)
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/root/tgApi
Environment="MALLOC_ARENA_MAX=2"
Environment="MALLOC_MMAP_THRESHOLD_=65536"
Environment="MALLOC_TRIM_THRESHOLD_=65536"
ExecStart=/usr/local/bin/telegram-bot-api --api-id=YOUR_API_ID --api-hash=YOUR_API_HASH --http-port=8080 --dir=/root/tgApi --local
Restart=always
RestartSec=3
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

---

## License

Distributed under the **Boost Software License 1.0** (`BSL-1.0`), matching the official [`tdlib/telegram-bot-api`](https://github.com/tdlib/telegram-bot-api/blob/master/LICENSE_1_0.txt). See [`LICENSE`](LICENSE) for full text.
