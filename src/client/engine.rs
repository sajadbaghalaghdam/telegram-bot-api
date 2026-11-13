//! The engine: shared state and construction. Behaviour lives in sibling modules, each adding an
//! `impl TelegramEngine` block for one responsibility:
//! `token` (validation, seen bots), `profile` (getMe), `files` (local files, getFile, serving),
//! `upload` (MTProto/multipart upload glue), `webhook_api` (webhook methods), `relay` (universal call).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use dashmap::DashMap;
use reqwest::Client;
use tokio::task::AbortHandle;

use crate::storage::StorageEngine;

pub use super::webhook_api::is_local_webhook_url;

/// 64KB chunk size for O(1) constant-memory file streaming
pub(super) const STREAM_BUFFER_SIZE: usize = 65536;

/// Core engine handling upstream communication, local file streaming, and SQLite WAL caching.
pub struct TelegramEngine {
    pub client: Client,
    pub webhook_client: Client,
    pub upstream_base_url: String,
    pub local_mode: bool,
    pub dir: PathBuf,
    pub temp_dir: PathBuf,
    pub storage: Arc<StorageEngine>,
    pub webhook_workers: Arc<DashMap<String, AbortHandle>>,
    /// Runtime state (pending count, last error) of running local webhook workers.
    pub webhook_states: Arc<DashMap<String, Arc<super::webhook::WebhookState>>>,
    pub set_webhook_times: DashMap<String, std::time::Instant>,
    pub proxy_url: Option<String>,
    pub active_bots: Arc<DashMap<String, ()>>,
    pub token_filter: Option<(u64, u64)>,
    pub default_max_webhook_connections: i32,
    pub api_id: i32,
    pub api_hash: String,
}

impl TelegramEngine {
    pub fn new(
        local_mode: bool,
        dir: PathBuf,
        temp_dir: PathBuf,
        storage: Arc<StorageEngine>,
    ) -> Self {
        Self::with_options(local_mode, dir, temp_dir, storage, None, None, if local_mode { 100 } else { 40 })
    }

    pub fn with_api_credentials(mut self, api_id: i32, api_hash: String) -> Self {
        self.api_id = api_id;
        self.api_hash = api_hash;
        self
    }

    pub fn with_options(
        local_mode: bool,
        dir: PathBuf,
        temp_dir: PathBuf,
        storage: Arc<StorageEngine>,
        proxy_url: Option<&str>,
        token_filter: Option<(u64, u64)>,
        default_max_webhook_connections: i32,
    ) -> Self {
        let abs_dir = std::fs::canonicalize(&dir).unwrap_or_else(|_| {
            std::env::current_dir()
                .map(|cwd| cwd.join(&dir))
                .unwrap_or(dir)
        });

        let mut client_builder = Client::builder()
            .pool_max_idle_per_host(64)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(300));

        let mut webhook_builder = Client::builder()
            .pool_max_idle_per_host(64)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(60));

        if let Some(p) = proxy_url {
            if let Ok(proxy) = reqwest::Proxy::all(p) {
                webhook_builder = webhook_builder.proxy(proxy);
            }
            if std::env::var("TELEGRAM_UPSTREAM_PROXY").is_ok() {
                if let Ok(proxy) = reqwest::Proxy::all(p) {
                    client_builder = client_builder.proxy(proxy);
                }
            }
        }

        let client = client_builder
            .build()
            .expect("Failed to initialize HTTP client");

        let webhook_client = webhook_builder
            .build()
            .expect("Failed to initialize webhook HTTP client");

        Self {
            client,
            webhook_client,
            upstream_base_url: "https://api.telegram.org".to_string(),
            local_mode,
            dir: abs_dir,
            temp_dir,
            storage,
            webhook_workers: Arc::new(DashMap::new()),
            webhook_states: Arc::new(DashMap::new()),
            set_webhook_times: DashMap::new(),
            proxy_url: proxy_url.map(|s| s.to_string()),
            active_bots: Arc::new(DashMap::with_capacity(64)),
            token_filter,
            default_max_webhook_connections,
            api_id: 0,
            api_hash: String::new(),
        }
    }
}
