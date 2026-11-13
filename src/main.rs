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

/// Prints a startup error the way C++ does (message on stderr, exit code 1).
fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("{msg}");
    std::process::exit(1);
}

async fn async_main() -> Result<(), Box<dyn std::error::Error>> {
    // Clap already rejected malformed ports / IP addresses while parsing.
    let config = ServerConfig::parse();

    // C++ validates options before touching the filesystem (telegram-bot-api.cpp:304-321).
    if let Err(e) = config.validate() {
        fail(e);
    }
    let token_filter = config.parsed_filter().unwrap_or_else(|e| fail(e));
    let addr = config.address();

    // RUST_LOG overrides -v only when set to a non-empty value.
    let filter = match std::env::var("RUST_LOG") {
        Ok(v) if !v.trim().is_empty() => tracing_subscriber::EnvFilter::try_new(&v)
            .unwrap_or_else(|e| fail(format!("Invalid RUST_LOG value: {e}"))),
        _ => tracing_subscriber::EnvFilter::new(config.log_level()),
    };

    // Resolve directories first: relative --log / --temp-dir are interpreted against --files-dir (as in C++).
    let raw_dir = PathBuf::from(&config.dir);
    if let Err(e) = config::ensure_writable_dir(&raw_dir, "--dir", "working") {
        fail(e);
    }
    let effective_dir = std::fs::canonicalize(&raw_dir)
        .unwrap_or_else(|e| fail(format!("Invalid working directory specified: {e}")));
    let effective_files_dir = match config.files_dir.as_deref() {
        Some(p) => {
            let pb = PathBuf::from(p);
            if let Err(e) = config::ensure_writable_dir(&pb, "--files-dir", "files") {
                fail(e);
            }
            std::fs::canonicalize(&pb).unwrap_or_else(|e| fail(format!("Invalid files directory specified: {e}")))
        }
        None => effective_dir.clone(),
    };
    let effective_temp_dir = ServerConfig::resolve_in(&effective_files_dir, &config.temp_dir);
    if let Err(e) = config::ensure_writable_dir(&effective_temp_dir, "--temp-dir", "temporary") {
        fail(e);
    }
    let registry = tracing_subscriber::registry().with(filter);
    // Handle kept for SIGUSR1 reopen; None when logging to stderr.
    let mut log_writer: Option<logging::RotatingFile> = None;
    match config.log_file.as_deref() {
        Some(lf) => {
            let path = ServerConfig::resolve_in(&effective_files_dir, lf);
            match logging::RotatingFile::open(&path, config.log_max_file_size) {
                Ok(w) => {
                    registry
                        .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(w.clone()))
                        .init();
                    log_writer = Some(w);
                    let rust_log_set = std::env::var("RUST_LOG").map(|v| !v.trim().is_empty()).unwrap_or(false);
                    if config.verbosity <= 0 && !rust_log_set {
                        eprintln!("Note: --verbosity 0 logs nothing, so the log file will stay empty; use -v 3 (or higher) to log.");
                    }
                }
                Err(e) => {
                    eprintln!("Can't open log file \"{}\": {e}", path.display());
                    std::process::exit(1);
                }
            }
        }
        None => registry.with(tracing_subscriber::fmt::layer()).init(),
    }

    // Like C++ telegram-bot-api.cpp: SIGUSR1 is always handled (reopens the log file if there is one,
    // otherwise it is a no-op) and SIGHUP is ignored; neither may terminate the process.
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::user_defined1()) {
            Ok(mut sig) => {
                tokio::spawn(async move {
                    while sig.recv().await.is_some() {
                        if let Some(w) = &log_writer {
                            if let Err(e) = w.reopen() {
                                eprintln!("Failed to reopen log file: {e}");
                            }
                        }
                    }
                });
            }
            Err(e) => eprintln!("Failed to install SIGUSR1 handler: {e}"),
        }
        match signal(SignalKind::hangup()) {
            Ok(mut sig) => {
                tokio::spawn(async move { while sig.recv().await.is_some() {} });
            }
            Err(e) => eprintln!("Failed to ignore SIGHUP: {e}"),
        }
    }

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

    // Bind both listeners before restoring webhooks so a port conflict fails before any side effects.
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| fail(format!("Can't bind HTTP server to {addr}: {e}")));
    let stat_listener = match config.stat_address() {
        Some(stat_addr) => Some(
            tokio::net::TcpListener::bind(stat_addr)
                .await
                .unwrap_or_else(|e| fail(format!("Can't bind statistics server to {stat_addr}: {e}"))),
        ),
        None => None,
    };

    // Automatically restore any active webhooks from webhooks_db.binlog and SQLite WAL
    engine.restore_webhooks().await;

    let client_manager = Arc::new(ClientManager::new(Arc::clone(&engine)));
    let stats = Arc::new(ServerStats::new());

    let watchdog = Watchdog::new(60);
    watchdog.clone().spawn_monitor();

    let state = AppState {
        config: Arc::new(config.clone()),
        engine: Arc::clone(&engine),
        client_manager: Arc::clone(&client_manager),
        stats: Arc::clone(&stats),
        watchdog,
    };

    // Dedicated statistics server (-s != 0). Like C++ HttpStatConnection it answers any path and method.
    if let Some(stat_listener) = stat_listener {
        let st = (Arc::clone(&stats), Arc::clone(&engine), Arc::clone(&client_manager));
        let stat_app = axum::Router::new().fallback(move || {
            let (s, e, c) = st.clone();
            async move { ([("content-type", "text/plain")], handlers::stats::render_report(&e, &c, &s)) }
        });
        if let Ok(a) = stat_listener.local_addr() {
            tracing::info!("Dedicated statistics server listening on http://{}", a);
        }
        tokio::spawn(async move {
            if let Err(e) = axum::serve(stat_listener, stat_app).await {
                tracing::error!("Statistics server stopped: {e}");
            }
        });
    }

    let app = create_router(state);
    tracing::info!("Server listening on http://{}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
