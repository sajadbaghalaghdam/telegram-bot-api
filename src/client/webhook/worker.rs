//! The per-bot worker: polls upstream, feeds the queues, and owns the registry entries.
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::{Notify, Semaphore};
use tokio::task::JoinSet;

use super::ctx::Ctx;
use super::delivery::run_queue;
use super::poll::{classify_poll, poll_error_text, PollOutcome};
use super::queue::{queue_id, Inner, Item};
use super::retry::poll_backoff;
use super::state::{rand_unit, WebhookState};
use crate::client::engine::TelegramEngine;

/// Poll interval while updates are loaded but unfinished (upstream answers instantly then).
const BUSY_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Minimum spacing of deleteWebhook repairs after a "webhook is active" conflict.
const CONFLICT_FIX_INTERVAL: Duration = Duration::from_secs(30);

fn build_client(engine: &TelegramEngine, url: &str, ip_address: Option<&str>) -> Client {
    if let (Some(ip), Ok(u)) = (ip_address.filter(|s| !s.is_empty()), reqwest::Url::parse(url)) {
        if engine.proxy_url.is_some() {
            tracing::warn!("webhook ip_address {ip} is ignored: the webhook is reached through a proxy");
        } else if let (Ok(ip), Some(host), Some(port)) = (ip.parse::<IpAddr>(), u.host_str(), u.port_or_known_default()) {
            let b = Client::builder()
                .tcp_nodelay(true)
                .timeout(Duration::from_secs(60))
                .resolve(host, SocketAddr::new(ip, port));
            if let Ok(c) = b.build() {
                return c;
            }
        }
    }
    engine.webhook_client.clone()
}

/// Serializes registry changes (`webhook_states` + `webhook_workers`) of start/stop/exit.
static REGISTRY_LOCK: Mutex<()> = Mutex::new(());

fn registry_lock() -> MutexGuard<'static, ()> {
    REGISTRY_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

struct ExitGuard {
    engine: Arc<TelegramEngine>,
    token: String,
    generation: u64,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        // Remove the registry entries only if they still belong to this worker generation.
        let _l = registry_lock();
        let removed = self
            .engine
            .webhook_states
            .remove_if(&self.token, |_, s| s.generation == self.generation)
            .is_some();
        if removed {
            self.engine.webhook_workers.remove(&self.token);
        }
    }
}

pub struct WorkerConfig {
    pub token: String,
    pub url: String,
    pub secret_token: Option<String>,
    pub allowed_updates: Option<Vec<String>>,
    pub drop_pending_updates: bool,
    pub max_connections: i32,
    pub ip_address: Option<String>,
}

/// Waits until an update finished, the webhook got closed, or `d` elapsed.
async fn wait_progress(ctx: &Ctx, d: Duration) {
    tokio::select! {
        _ = ctx.progress.notified() => {}
        _ = ctx.closed_notify.notified() => {}
        _ = tokio::time::sleep(d) => {}
    }
}

/// True while this worker is still the registered one for its token.
fn is_current(engine: &TelegramEngine, token: &str, generation: u64) -> bool {
    engine.webhook_states.get(token).is_some_and(|s| s.generation == generation)
}

async fn run_worker(engine: Arc<TelegramEngine>, cfg: WorkerConfig, state: Arc<WebhookState>) {
    let _guard = ExitGuard {
        engine: Arc::clone(&engine),
        token: cfg.token.clone(),
        generation: state.generation,
    };
    let mut base_offset: i64 = 0;

    if cfg.drop_pending_updates {
        let init_url = format!("{}/bot{}/getUpdates?offset=-1&limit=1&timeout=0", engine.upstream_base_url, cfg.token);
        if let Ok(resp) = engine.client.get(&init_url).send().await
            && let Ok(json) = resp.json::<Value>().await
                && let Some(uid) = json
                    .get("result")
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.last())
                    .and_then(|u| u.get("update_id"))
                    .and_then(|v| v.as_i64())
                {
                    base_offset = uid + 1;
                }
    }

    let ctx = Arc::new(Ctx {
        engine: Arc::clone(&engine),
        token: cfg.token.clone(),
        url: cfg.url.clone(),
        secret: cfg.secret_token.clone(),
        client: build_client(&engine, &cfg.url, cfg.ip_address.as_deref()),
        sem: Arc::new(Semaphore::new(cfg.max_connections.max(1) as usize)),
        state: Arc::clone(&state),
        first_410: Mutex::new(None),
        closed: AtomicBool::new(false),
        inner: Mutex::new(Inner::new(base_offset)),
        progress: Notify::new(),
        closed_notify: Notify::new(),
    });
    // Queue tasks live in this set: they are aborted together with the worker.
    let mut tasks: JoinSet<()> = JoinSet::new();
    // C++ bounds the loaded updates to 2 * max_connections.
    let cap = (cfg.max_connections.max(1) as usize).saturating_mul(2);

    let poll_url = format!("{}/bot{}/getUpdates", engine.upstream_base_url, cfg.token);
    let mut fails: u32 = 0;
    let mut last_conflict_fix: Option<Instant> = None;
    loop {
        while tasks.try_join_next().is_some() {}
        if ctx.closed.load(Ordering::Relaxed) {
            tracing::warn!("Closing webhook of a bot because of HTTP 410 errors");
            if is_current(&engine, &cfg.token, state.generation) {
                engine.storage.delete_webhook(&cfg.token).await;
            }
            break;
        }

        let page = {
            let g = ctx.lock();
            g.poll_window(cap).map(|(o, l)| (o, l, g.window.occupancy() > 0))
        };
        let Some((offset, limit, busy)) = page else {
            wait_progress(&ctx, BUSY_POLL_INTERVAL).await;
            continue;
        };

        // Long polling only helps while nothing is outstanding: upstream answers instantly when
        // it still holds unconfirmed updates >= offset.
        let mut body = serde_json::json!({ "offset": offset, "limit": limit, "timeout": if busy { 0 } else { 20 } });
        if let Some(ref allowed) = cfg.allowed_updates {
            body["allowed_updates"] = serde_json::to_value(allowed).unwrap_or(Value::Null);
        }
        let poll = async {
            match engine.client.post(&poll_url).json(&body).send().await {
                Ok(resp) => Ok(resp.json::<Value>().await.ok()),
                Err(e) => Err(e.without_url().to_string()),
            }
        };
        let res = tokio::select! {
            r = poll => r,
            _ = ctx.closed_notify.notified() => continue,
        };
        let (json, transport_err) = match res {
            Ok(j) => (j, None),
            Err(e) => (None, Some(e)),
        };

        match classify_poll(json.as_ref()) {
            PollOutcome::Updates(updates) => {
                fails = 0;
                let mut to_spawn = Vec::new();
                let mut added = 0usize;
                {
                    let mut g = ctx.lock();
                    for u in &updates {
                        let Some(id) = u.get("update_id").and_then(|v| v.as_i64()) else { continue };
                        let item = Item {
                            id,
                            body: Bytes::from(serde_json::to_vec(u).unwrap_or_default()),
                            first_seen: Instant::now(),
                        };
                        let qid = queue_id(u, id);
                        if let Some(spawn) = g.admit(qid, item) {
                            added += 1;
                            if spawn {
                                to_spawn.push(qid);
                            }
                        }
                    }
                    ctx.sync_pending(&g);
                }
                for qid in to_spawn {
                    tasks.spawn(run_queue(Arc::clone(&ctx), qid));
                }
                if added == 0 && busy {
                    wait_progress(&ctx, BUSY_POLL_INTERVAL).await;
                }
            }
            PollOutcome::Fatal => {
                let msg = poll_error_text(json.as_ref(), None);
                tracing::warn!("Stopping webhook worker: {msg}");
                state.record_error(&msg);
                // Do not restart a dead bot's webhook on every boot.
                if is_current(&engine, &cfg.token, state.generation) {
                    engine.storage.delete_webhook(&cfg.token).await;
                }
                break;
            }
            outcome => {
                let msg = poll_error_text(json.as_ref(), transport_err.as_deref());
                tracing::warn!("webhook poll error: {msg}");
                state.record_error(&msg);
                fails = fails.saturating_add(1);
                let d = match outcome {
                    PollOutcome::Retry(Some(s)) => Duration::from_secs_f64(s as f64 + rand_unit()),
                    PollOutcome::WebhookActive => {
                        // A cloud webhook reappeared upstream (e.g. set by another client).
                        if last_conflict_fix.is_none_or(|t| t.elapsed() >= CONFLICT_FIX_INTERVAL) {
                            last_conflict_fix = Some(Instant::now());
                            match engine.delete_upstream_webhook(&cfg.token, false).await {
                                Ok(()) => tracing::warn!("deleted the cloud webhook that blocked getUpdates"),
                                Err(e) => tracing::warn!("deleteWebhook after getUpdates conflict failed: {e}"),
                            }
                        }
                        poll_backoff(fails, rand_unit())
                    }
                    _ => poll_backoff(fails, rand_unit()),
                };
                tokio::time::sleep(d).await;
            }
        }
    }
}

impl TelegramEngine {
    /// Starts or replaces the polling -> webhook delivery worker for a bot (local webhooks only).
    pub fn start_webhook_worker(self: &Arc<Self>, cfg: WorkerConfig) {
        let token = cfg.token.clone();
        // Held for the whole registration: concurrent starts for one token cannot orphan a worker,
        // and a worker that exits early waits (in its ExitGuard) until it is registered.
        let _l = registry_lock();
        self.stop_worker_locked(&token);
        if cfg.url.is_empty() {
            return;
        }
        self.record_active_bot(&token);
        let state = Arc::new(WebhookState::new(cfg.max_connections));
        self.webhook_states.insert(token.clone(), Arc::clone(&state));
        let engine = Arc::clone(self);
        let handle = tokio::spawn(run_worker(engine, cfg, state));
        self.webhook_workers.insert(token, handle.abort_handle());
    }

    fn stop_worker_locked(&self, token: &str) {
        self.webhook_states.remove(token);
        if let Some((_, handle)) = self.webhook_workers.remove(token) {
            handle.abort();
        }
    }

    pub fn stop_webhook_worker(&self, token: &str) {
        let _l = registry_lock();
        self.stop_worker_locked(token);
    }

}
