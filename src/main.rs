//! Telegram Bot API Server in Pure Rust matching official tdlib/telegram-bot-api specifications
pub mod client;
pub mod config;
pub mod error;
pub mod handlers;
pub mod logging;
pub mod models;
pub mod mtproto;
pub mod server;
pub mod stats;
pub mod storage;
pub mod watchdog;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use clap::Parser;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use crate::client::{ClientManager, TelegramEngine};
use crate::config::ServerConfig;
use crate::server::{create_router, AppState};
use crate::stats::ServerStats;
use crate::storage::StorageEngine;
use crate::watchdog::Watchdog;

/// Upper bound for Tokio's blocking pool (`spawn_blocking`, `tokio::fs`, SQLite reads); the default is 512.
/// Writes use the dedicated SQLite writer thread, so this only has to cover file I/O and the 4 SQLite readers.
const MAX_BLOCKING_THREADS: usize = 16;
/// How long open connections (long polls, keep-alive) get to finish after SIGTERM/SIGINT.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(10);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workers = std::thread::available_parallelism().map_or(1, |n| n.get());
    configure_allocator(workers);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .max_blocking_threads(MAX_BLOCKING_THREADS)
        .enable_all()
        .build()?;

    let result = rt.block_on(async_main());
    // Do not wait for stray blocking tasks (e.g. a SQLite read) longer than this.
    rt.shutdown_timeout(Duration::from_secs(2));
    result
}

/// Configures the glibc allocator BEFORE any Tokio thread exists.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn configure_allocator(workers: usize) {
    unsafe extern "C" {
        fn mallopt(param: i32, value: i32) -> i32;
    }
    const M_TRIM_THRESHOLD: i32 = -1;
    const M_MMAP_THRESHOLD: i32 = -3;
    const M_ARENA_MAX: i32 = -8;
    // One arena per worker: with fewer arenas than threads, workers contend on arena locks
    // (see BENCHMARKS.md, Round 4D: M_ARENA_MAX 4 vs 8 workers).
    let arenas = i32::try_from(workers).unwrap_or(i32::MAX);
    // SAFETY: plain libc setters called once at start-up, before other threads are created.
    unsafe {
        let _ = mallopt(M_ARENA_MAX, arenas);
        let _ = mallopt(M_MMAP_THRESHOLD, 131072);
        let _ = mallopt(M_TRIM_THRESHOLD, 65536);
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn configure_allocator(_workers: usize) {}

/// Prints a startup error the way C++ does (message on stderr, exit code 1).
fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("{msg}");
    std::process::exit(1);
}

/// Working, files and temporary directories, all created, writable and canonical.
struct Dirs {
    work: PathBuf,
    files: PathBuf,
    temp: PathBuf,
}

/// Resolves directories first: relative --log / --temp-dir are interpreted against --files-dir (as in C++).
fn prepare_dirs(config: &ServerConfig) -> Dirs {
    let raw_dir = PathBuf::from(&config.dir);
    if let Err(e) = config::ensure_writable_dir(&raw_dir, "--dir", "working") {
        fail(e);
    }
    let work = std::fs::canonicalize(&raw_dir)
        .unwrap_or_else(|e| fail(format!("Invalid working directory specified: {e}")));
    let files = match config.files_dir.as_deref() {
        Some(p) => {
            let pb = PathBuf::from(p);
            if let Err(e) = config::ensure_writable_dir(&pb, "--files-dir", "files") {
                fail(e);
            }
            std::fs::canonicalize(&pb).unwrap_or_else(|e| fail(format!("Invalid files directory specified: {e}")))
        }
        None => work.clone(),
    };
    let temp = ServerConfig::resolve_in(&files, &config.temp_dir);
    if let Err(e) = config::ensure_writable_dir(&temp, "--temp-dir", "temporary") {
        fail(e);
    }
    Dirs { work, files, temp }
}

/// Installs the global tracing subscriber. Returns the log-file handle (kept for SIGUSR1 reopen and the
/// final flush), or `None` when logging to stderr.
fn init_logging(
    config: &ServerConfig,
    files_dir: &Path,
    filter: tracing_subscriber::filter::Targets,
) -> Option<logging::RotatingFile> {
    let registry = tracing_subscriber::registry().with(filter);
    let Some(lf) = config.log_file.as_deref() else {
        registry.with(tracing_subscriber::fmt::layer()).init();
        return None;
    };
    let path = ServerConfig::resolve_in(files_dir, lf);
    match logging::RotatingFile::open(&path, config.log_max_file_size) {
        Ok(w) => {
            registry
                .with(tracing_subscriber::fmt::layer().with_ansi(false).with_writer(w.clone()))
                .init();
            let rust_log_set = std::env::var("RUST_LOG").map(|v| !v.trim().is_empty()).unwrap_or(false);
            if config.verbosity <= 0 && !rust_log_set {
                eprintln!("Note: --verbosity 0 logs nothing, so the log file will stay empty; use -v 3 (or higher) to log.");
            }
            Some(w)
        }
        Err(e) => fail(format!("Can't open log file \"{}\": {e}", path.display())),
    }
}

/// Like C++ telegram-bot-api.cpp: SIGUSR1 is always handled (reopens the log file if there is one,
/// otherwise it is a no-op) and SIGHUP is ignored; neither may terminate the process.
/// The tasks live in `tasks`, so shutdown can cancel them.
#[cfg(unix)]
fn spawn_signal_tasks(tasks: &mut JoinSet<()>, log_writer: Option<logging::RotatingFile>) {
    use tokio::signal::unix::{signal, SignalKind};
    match signal(SignalKind::user_defined1()) {
        Ok(mut sig) => {
            tasks.spawn(async move {
                while sig.recv().await.is_some() {
                    if let Some(Err(e)) = log_writer.as_ref().map(|w| w.reopen()) {
                        eprintln!("Failed to reopen log file: {e}");
                    }
                }
            });
        }
        Err(e) => eprintln!("Failed to install SIGUSR1 handler: {e}"),
    }
    match signal(SignalKind::hangup()) {
        Ok(mut sig) => {
            tasks.spawn(async move { while sig.recv().await.is_some() {} });
        }
        Err(e) => eprintln!("Failed to ignore SIGHUP: {e}"),
    }
}

#[cfg(not(unix))]
fn spawn_signal_tasks(_tasks: &mut JoinSet<()>, _log_writer: Option<logging::RotatingFile>) {}

/// Resolves on the first SIGTERM or SIGINT.
async fn shutdown_signal() {
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(e) => {
                eprintln!("Failed to install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        r = tokio::signal::ctrl_c() => {
            if let Err(e) = r {
                eprintln!("Failed to listen for SIGINT: {e}");
                std::future::pending::<()>().await;
            }
        }
        () = term => {}
    }
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
    let rust_log = std::env::var("RUST_LOG").ok();
    let filter = logging::build_filter(rust_log.as_deref(), config.log_level())
        .unwrap_or_else(|e| fail(format!("Invalid RUST_LOG value: {e}")));

    let dirs = prepare_dirs(&config);
    let log_writer = init_logging(&config, &dirs.files, filter);

    // Every background task is owned by this set and aborted on shutdown.
    let mut background = JoinSet::new();
    spawn_signal_tasks(&mut background, log_writer.clone());

    tracing::info!(
        "Starting Telegram Bot API Server v{} on {} (Local mode: {}, Dir: {}, Files Dir: {})",
        env!("CARGO_PKG_VERSION"),
        addr,
        config.local_mode,
        dirs.work.display(),
        dirs.files.display()
    );

    let storage = StorageEngine::new(&dirs.work)?;
    tracing::info!("SQLite WAL + L1 DashMap Storage Engine initialized in {}", dirs.work.display());

    let proxy_url = config.normalized_proxy();
    if let Some(ref p) = proxy_url {
        tracing::info!("Configured outgoing HTTP proxy: {}", p);
    }

    let mut engine = TelegramEngine::with_options(
        config.local_mode,
        dirs.files,
        dirs.temp,
        storage,
        proxy_url.as_deref(),
        token_filter,
        config.effective_max_webhook_connections(),
    )
    .with_api_credentials(config.api_id, config.api_hash.clone());
    if config.upstream_url != config::DEFAULT_UPSTREAM_URL {
        tracing::info!("Using upstream Bot API at {}", config.upstream_url);
    }
    engine.upstream_base_url.clone_from(&config.upstream_url);
    let engine = Arc::new(engine);

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
    {
        // CPU usage is derived from samples taken once a second (C++ ServerCpuStat::update).
        let stats = Arc::clone(&stats);
        background.spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                stats.sample_cpu();
            }
        });
    }

    let watchdog = Watchdog::new(60);
    let _watchdog_guard = watchdog.clone().spawn_monitor();

    let shutdown = CancellationToken::new();

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
        let stop = shutdown.clone().cancelled_owned();
        background.spawn(async move {
            if let Err(e) = axum::serve(stat_listener, stat_app).with_graceful_shutdown(stop).await {
                tracing::error!("Statistics server stopped: {e}");
            }
        });
    }

    let state = AppState {
        config: Arc::new(config),
        engine: Arc::clone(&engine),
        client_manager,
        stats,
        watchdog,
    };
    let app = create_router(state);
    tracing::info!("Server listening on http://{}", addr);

    let mut serve = std::pin::pin!(
        axum::serve(listener, app).with_graceful_shutdown(shutdown.clone().cancelled_owned()).into_future()
    );
    let served = tokio::select! {
        r = &mut serve => r,
        () = shutdown_signal() => {
            tracing::info!("Shutdown signal received: no longer accepting connections");
            shutdown.cancel();
            // Open long polls / keep-alive connections get a bounded time to finish; a second signal skips the wait.
            tokio::select! {
                r = &mut serve => r,
                () = tokio::time::sleep(SHUTDOWN_GRACE) => {
                    tracing::warn!("Connections still open after {SHUTDOWN_GRACE:?}; closing them");
                    Ok(())
                }
                () = shutdown_signal() => Ok(()),
            }
        }
    };

    // Stop everything that outlives the listener, then make sure pending SQLite writes are on disk.
    shutdown.cancel();
    background.abort_all();
    for worker in engine.webhook_workers.iter() {
        worker.value().abort();
    }
    engine.storage.flush().await;
    tracing::info!("Server stopped");
    if let Some(mut w) = log_writer {
        let _ = std::io::Write::flush(&mut w);
    }
    served?;
    Ok(())
}
