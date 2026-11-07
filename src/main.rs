//! Telegram Bot API Server in Pure Rust matching official tdlib/telegram-bot-api specifications
pub mod client;
pub mod config;
pub mod error;
pub mod handlers;
pub mod models;
pub mod mtproto;
pub mod query;
pub mod server;
pub mod stats;
pub mod storage;
pub mod watchdog;
pub mod webhook;

use std::path::{Path, PathBuf};
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
    #[cfg(target_os = "linux")]
    unsafe {
        unsafe extern "C" {
            fn mallopt(param: i32, value: i32) -> i32;
        }
        const M_TRIM_THRESHOLD: i32 = -1;
        const M_MMAP_THRESHOLD: i32 = -3;
        const M_ARENA_MAX: i32 = -8;
        let _ = mallopt(M_ARENA_MAX, 2);
        let _ = mallopt(M_MMAP_THRESHOLD, 32768);
        let _ = mallopt(M_TRIM_THRESHOLD, 32768);
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    rt.block_on(async_main())
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    let config = ServerConfig::parse();

    let level_str = match config.verbosity {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| level_str.into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

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
    let effective_files_dir = config.files_dir.as_deref().unwrap_or(&config.dir);

    tracing::info!(
        "Starting Telegram Bot API Server v9.5 on {} (Local mode: {}, Dir: {}, Files Dir: {})",
        addr,
        config.local_mode,
        config.dir,
        effective_files_dir
    );

    let storage = StorageEngine::new(Path::new(&config.dir))?;
    tracing::info!("SQLite WAL + L1 DashMap Storage Engine initialized in {}", config.dir);

    let proxy_url = config.normalized_proxy();
    if let Some(ref p) = proxy_url {
        tracing::info!("Configured outgoing HTTP proxy: {}", p);
    }

    let engine = Arc::new(TelegramEngine::with_options(
        config.local_mode,
        PathBuf::from(effective_files_dir),
        PathBuf::from(&config.temp_dir),
        storage,
        proxy_url.as_deref(),
        token_filter,
        config.effective_max_webhook_connections(),
    ));

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
    if let Some(Ok(stat_addr)) = config.stat_address() {
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
                                [("content-type", "text/plain; charset=utf-8")],
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
                                [("content-type", "text/plain; charset=utf-8")],
                                s.to_report(),
                            )
                        }
                    }),
                );
            if let Ok(listener) = tokio::net::TcpListener::bind(stat_addr).await {
                tracing::info!("Dedicated statistics server listening on http://{}", stat_addr);
                let _ = axum::serve(listener, stat_app).await;
            }
        });
    }

    let app = create_router(state);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!("Server listening on http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
