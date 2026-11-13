# Telegram Bot API Server — Pure Rust Edition

[![Release](https://img.shields.io/github/v/release/sajadbaghalaghdam/telegram-bot-api?color=blue&label=version)](https://github.com/sajadbaghalaghdam/telegram-bot-api/releases)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Rust Edition](https://img.shields.io/badge/Rust-2024%20Edition-orange.svg)](Cargo.toml)
[![Build & Release](https://github.com/sajadbaghalaghdam/telegram-bot-api/actions/workflows/release.yml/badge.svg)](https://github.com/sajadbaghalaghdam/telegram-bot-api/actions)

An ultra-fast, high-performance, and ultra-low-memory **Telegram Bot API Server** written from the ground up in **100% pure Rust**, featuring an integrated **MTProto 2.0 streaming pipeline** for seamless large file uploads and downloads up to **2000 MB (2 GB)** in local mode.

---

## Key Features

* **Ultra-Low Memory Footprint (15–30 MB RSS)**: Runs in only 15–30 MB of RAM under real-world multi-bot production workloads (compared to ~1 GB in legacy servers).
* **Native MTProto 2.0 Pipeline (Up to 2000 MB)**: Direct streaming to and from Telegram Cloud Data Centers with 512 KB pipelined chunks, Diffie-Hellman key exchange, AES-256-IGE packet cipher, and automatic DC migration (`FILE_MIGRATE_X`).
* **Zero 20MB Cloud Restrictions in Local Mode**: Both `sendDocument` and `getFile` stream files up to 2000 MB with constant $O(1)$ RAM usage, bypassing cloud limitations.
* **Pure Rust & Zero C++ Toolchain Hassle**: Built with Tokio, Axum, and embedded SQLite WAL caching. No CMake, no gperf, no external OpenSSL or heavy C++ build environments needed.
* **Production-Grade SQLite WAL Storage**: High-speed local cache for file metadata, webhook registrations, and bot statistics.
* **Drop-in Compatible**: Compatible with all standard Telegram Bot API clients and libraries (Python, Node.js, Go, PHP, C#, etc.).

---

## Performance & Memory Comparison

| Metric | Legacy Server | Pure Rust Server (`telegram-bot-api-rs`) | Advantage |
| :--- | :---: | :---: | :---: |
| **Production RAM (RSS)** | `~988 MB` | **`15 MB – 30 MB`** | **~33x – 64x lower memory** |
| **2GB File Upload Peak RAM** | High / Unbounded | **`~52 MB`** ($O(1)$ constant) | **Predictable memory footprint** |
| **Idle Private Memory** | `~10.8 MB` | **`~4.5 MB`** | **2.3x lower idle footprint** |
| **Memory Growth per Active Bot** | `+8.5 MB` per bot | **`< 1 KB`** per bot (shared async pool) | **Infinite multi-bot scalability** |
| **Clean Build Time** | `~18 minutes` (>3.5 GB RAM) | **`< 40 seconds`** (<500 MB RAM) | **~30x faster builds** |
| **Binary Size** | `~34 MB` | **`~6.9 MB`** (LTO Stripped) | **5x smaller binary** |

---

## Quick Start: 1-Line Production Installer (Linux)

You can install the binary, configure credentials, and set up a production-ready `systemd` service with a single command:

```bash
curl -fsSL https://raw.githubusercontent.com/sajadbaghalaghdam/telegram-bot-api/master/install.sh | sudo bash
```

The script automatically:
1. Detects your CPU architecture (`x86_64`, `x86_64 AVX2`, or `aarch64` ARM64).
2. Downloads the latest verified release binary to `/usr/local/bin/telegram-bot-api`.
3. Creates a dedicated data directory (`/var/lib/telegram-bot-api`).
4. Generates and starts a managed `systemd` service (`telegram-bot-api.service`).

---

## Manual Service Setup (`systemd`)

If you prefer to configure `systemd` manually:

1. Download the pre-built binary for your platform from [**Releases**](https://github.com/sajadbaghalaghdam/telegram-bot-api/releases) and place it in `/usr/local/bin/telegram-bot-api`.
2. Make it executable:
   ```bash
   sudo chmod +x /usr/local/bin/telegram-bot-api
   ```
3. Create `/etc/systemd/system/telegram-bot-api.service`:
   ```ini
   [Unit]
   Description=Telegram Bot API Server (Pure Rust)
   After=network.target

   [Service]
   Type=simple
   User=root
   WorkingDirectory=/var/lib/telegram-bot-api
   ExecStart=/usr/local/bin/telegram-bot-api --api-id=YOUR_API_ID --api-hash=YOUR_API_HASH --http-port=8080 --dir=/var/lib/telegram-bot-api --local
   Restart=always
   RestartSec=2s
   LimitNOFILE=65535

   [Install]
   WantedBy=multi-user.target
   ```
4. Enable and start:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now telegram-bot-api
   ```

---

## Pre-Compiled Multi-Architecture Binaries

Fat LTO, stripped pre-compiled binaries are provided for every release on the [**Releases**](https://github.com/sajadbaghalaghdam/telegram-bot-api/releases) page:

| Architecture / OS | Binary Filename | Description |
| :--- | :--- | :--- |
| **Linux x86_64 (glibc)** | `telegram-bot-api-rs-x86_64-unknown-linux-gnu` | Standard 64-bit Linux (Ubuntu, Debian, CentOS, AlmaLinux, Rocky) |
| **Linux x86_64 (AVX2)** | `telegram-bot-api-rs-x86_64-unknown-linux-gnu-avx2` | Optimized for Intel Haswell+ / AMD Zen+ AVX2 instructions |
| **Linux x86_64 (musl Static)** | `telegram-bot-api-rs-x86_64-unknown-linux-musl` | **Zero-dependency fully static binary** (Alpine, Docker scratch, any Linux) |
| **Linux ARM64 / AArch64** | `telegram-bot-api-rs-aarch64-unknown-linux-gnu` | AWS Graviton, Oracle Ampere A1, Raspberry Pi 4/5, Docker ARM |
| **Windows x86_64** | `telegram-bot-api-rs-x86_64-pc-windows-msvc.exe` | 64-bit Windows executable |
| **Windows x86_64 (AVX2)** | `telegram-bot-api-rs-x86_64-pc-windows-msvc-avx2.exe` | Optimized for Windows x86-64 with AVX2 instruction sets |

All releases include cryptographic SHA-256 hashes in `SHA256SUMS.txt`.

---

## Building From Source

Prerequisites: A standard Rust toolchain (2024 edition / Rust 1.85+).

```bash
# 1. Clone the repository
git clone https://github.com/sajadbaghalaghdam/telegram-bot-api.git
cd telegram-bot-api

# 2. Build release binary with full optimizations
cargo build --release

# 3. Binary will be available at:
./target/release/telegram-bot-api-rs --help
```

---

## CLI Options

```text
Usage: telegram-bot-api [OPTIONS]

Options:
      --api-id <API_ID>                          Application identifier for Telegram API access [env: TELEGRAM_API_ID=]
      --api-hash <API_HASH>                      Application identifier hash for Telegram API access [env: TELEGRAM_API_HASH=]
      --local                                    Enable local mode (streams files directly from disk, removes 20MB limit)
      --dir <DIR>                                Working directory for storing data [default: .]
      --files-dir <FILES_DIR>                    Directory for storing downloaded files
      --temp-dir <TEMP_DIR>                      Directory for storing temporary files
      --http-port <PORT>                         HTTP listening port [default: 8081]
      --http-ip-address <IP>                     HTTP IP address [default: 0.0.0.0]
      --http-stat-port <PORT>                    HTTP statistics listening port (0 or unset disables)
      --log <PATH>                               Path to log file
  -v, --verbosity <LEVEL>                        Log verbosity (0=off, 1=error, 2=warn, 3=info, 4=debug, 5+=trace) [default: 0]; RUST_LOG (non-empty) overrides
  -h, --help                                     Print help
  -V, --version                                  Print version
```

---

## License

This project is licensed under the [MIT License](LICENSE).
