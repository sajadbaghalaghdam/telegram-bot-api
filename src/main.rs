//! Telegram Bot API Server in Pure Rust matching official tdlib/telegram-bot-api specifications
pub mod client;
pub mod config;
pub mod error;
pub mod handlers;
pub mod logging;
pub mod models;
pub mod mtproto;
pub mod query;
pub mod server;
pub mod stats;
pub mod storage;
pub mod watchdog;

use std::path::PathBuf;
use std::sync::Arc;
use clap::Parser;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::client::{ClientManager, TelegramEngine};
use crate::config::ServerConfig;
use crate::server::{create_router, AppState};
use crate::stats::ServerStats;
use crate::storage::StorageEngine;
use crate::watchdog::Watchdog;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure glibc allocator BEFORE spawning any Tokio threads so thread arenas stay strictly bounded
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    unsafe {
        unsafe extern "C" {
            fn mallopt(param: i32, value: i32) -> i32;
        }
        const M_TRIM_THRESHOLD: i32 = -1;
        const M_MMAP_THRESHOLD: i32 = -3;
        const M_ARENA_MAX: i32 = -8;
        let _ = mallopt(M_ARENA_MAX, 4);
        let _ = mallopt(M_MMAP_THRESHOLD, 131072);
        let _ = mallopt(M_TRIM_THRESHOLD, 65536);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::parse();

    let level_str = config.log_level();

    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| level_str.into());

    // Resolve directories first: relative --log / --temp-dir are interpreted against --files-dir (as in C++).
    let raw_dir = PathBuf::from(&config.dir);
    if let Err(e) = config::ensure_writable_dir(&raw_dir, "--dir") {
        eprintln!("{e}");
        std::process::exit(1);
    }
    let effective_dir = std::fs::canonicalize(&raw_dir)?;
    let effective_files_dir = match config.files_dir.as_deref() {
        Some(p) => {
            let pb = PathBuf::from(p);
            if let Err(e) = config::ensure_writable_dir(&pb, "--files-dir") {
                eprintln!("{e}");
                std::process::exit(1);
            }
            std::fs::canonicalize(&pb)?
        }
        None => effective_dir.clone(),
    };
    let effective_temp_dir = ServerConfig::resolve_in(&effective_files_dir, &config.temp_dir);
    if let Err(e) = config::ensure_writable_dir(&effective_temp_dir, "--temp-dir") {
        eprintln!("{e}");
        std::process::exit(1);
    }

    let registry = tracing_subscriber::registry().with(filter);
    match config.log_file.as_deref() {
        Some(lf) => {
            let path = ServerConfig::resolve_in(&effective_files_dir, lf);
            match logging::RotatingFile::open(&path, config.log_max_file_size) {
                Ok(w) => registry
                    .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(w))
                    .init(),
                Err(e) => {
                    eprintln!("Can't open log file \"{}\": {e}", path.display());
                    std::process::exit(1);
                }
            }
        }
        None => registry.with(tracing_subscriber::fmt::layer()).init(),
    }

    if config.api_id <= 0 || config.api_hash.trim().is_empty() {
        eprintln!("You must provide valid api-id and api-hash obtained at https://my.telegram.org");
        std::process::exit(1);
    }

    let token_filter = match config.parsed_filter() {
        Ok(f) => f,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(1);
        }
    };

    let addr = config.address()?;
    tracing::info!(
        "Starting Telegram Bot API Server v{} on {} (Local mode: {}, Dir: {}, Files Dir: {})",
        env!("CARGO_PKG_VERSION"),
        addr,
        config.local_mode,
        effective_dir.display(),
        effective_files_dir.display()
    );

    let storage = StorageEngine::new(&effective_dir)?;
    tracing::info!("SQLite WAL + L1 DashMap Storage Engine initialized in {}", effective_dir.display());

    let proxy_url = config.normalized_proxy();
    if let Some(ref p) = proxy_url {
        tracing::info!("Configured outgoing HTTP proxy: {}", p);
    }

    let engine = Arc::new(
        TelegramEngine::with_options(
            config.local_mode,
            effective_files_dir,
            effective_temp_dir,
            storage,
            proxy_url.as_deref(),
            token_filter,
            config.effective_max_webhook_connections(),
        )
        .with_api_credentials(config.api_id, config.api_hash.clone()),
    );

    // Automatically restore any active webhooks from webhooks_db.binlog and SQLite WAL
    engine.restore_webhooks().await;

    let client_manager = Arc::new(ClientManager::new(Arc::clone(&engine)));
    let stats = Arc::new(ServerStats::new());
    stats.set_active_bots(engine.active_bots_count());

    let watchdog = Watchdog::new(60);
    watchdog.clone().spawn_monitor();

    let state = AppState {
        config: Arc::new(config.clone()),
        engine: Arc::clone(&engine),
        client_manager: Arc::clone(&client_manager),
        stats: Arc::clone(&stats),
        watchdog,
    };

    // If dedicated --http-stat-port is specified, launch dedicated stats listener on --http-stat-ip-address
    if let Some(stat_addr) = config.stat_address() {
        let stat_addr = match stat_addr {
            Ok(a) => a,
            Err(e) => {
                eprintln!("Invalid --http-stat-ip-address \"{}\": {e}", config.http_stat_ip_address);
                std::process::exit(1);
            }
        };
        let stat_listener = match tokio::net::TcpListener::bind(stat_addr).await {
            Ok(l) => l,
            Err(e) => {
                eprintln!("Can't bind statistics server to {stat_addr}: {e}");
                std::process::exit(1);
            }
        };
        let stat_stats = Arc::clone(&stats);
        let stat_engine = Arc::clone(&engine);
        let stat_cm = Arc::clone(&client_manager);
        tokio::spawn(async move {
            let s1 = stat_stats.clone();
            let e1 = stat_engine.clone();
            let c1 = stat_cm.clone();
            let s2 = stat_stats.clone();
            let e2 = stat_engine.clone();
            let c2 = stat_cm.clone();
            let stat_app = axum::Router::new()
                .route(
                    "/",
                    axum::routing::get(move || {
                        let s = s1.clone();
                        let e = e1.clone();
                        let c = c1.clone();
                        async move {
                            s.set_active_bots(e.active_bots_count().max(c.active_count()));
                            (
                                [("content-type", "text/plain")],
                                s.to_report(),
                            )
                        }
                    }),
                )
                .route(
                    "/stats",
                    axum::routing::get(move || {
                        let s = s2.clone();
                        let e = e2.clone();
                        let c = c2.clone();
                        async move {
                            s.set_active_bots(e.active_bots_count().max(c.active_count()));
                            (
                                [("content-type", "text/plain")],
                                s.to_report(),
                            )
                        }
                    }),
                );
            tracing::info!("Dedicated statistics server listening on http://{}", stat_addr);
            if let Err(e) = axum::serve(stat_listener, stat_app).await {
                tracing::error!("Statistics server stopped: {e}");
            }
        });
    }

    let app = create_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Server listening on http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
