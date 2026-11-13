//! Local webhook delivery: a long-polling worker per bot that pushes updates to a locally
//! reachable webhook URL (see `is_local_webhook_url`). Cloud (non-local) webhooks are never
//! handled here; they are proxied to the upstream Bot API untouched.
//!
//! Semantics follow the C++ `WebhookActor`:
//! * an update is only confirmed (getUpdates offset advanced) after the webhook answered 2xx
//!   (or the update was dropped because it is older than the drop timeout);
//! * failures are retried with exponential backoff (`next_retry`), honoring `Retry-After`;
//! * updates of one chat (queue) are delivered strictly in order, different chats in parallel,
//!   bounded by `max_connections`;
//! * a 2xx reply that is a JSON object with a `method` field is executed as a Bot API call.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use super::engine::{is_local_webhook_url, TelegramEngine};
use crate::error::TelegramError;

/// C++ WEBHOOK_MAX_RESEND_TIMEOUT
pub const MAX_RESEND_TIMEOUT: u32 = 60;
/// C++ WEBHOOK_DROP_TIMEOUT (23h): continuous 410 closes the webhook; also the lifetime of an update.
pub const DROP_TIMEOUT_SECS: u64 = 60 * 60 * 23;
/// C++ MAX_RETRY_AFTER
pub const MAX_RETRY_AFTER: u32 = 3600;
/// Local mode allows up to 100000 connections in C++ (`get_webhook_max_connections`).
pub const MAX_CONNECTIONS_LOCAL: i32 = 100_000;

static GENERATION: AtomicU64 = AtomicU64::new(1);

/// Runtime state of one worker, shared with getWebhookInfo.
pub struct WebhookState {
    pub generation: u64,
    pub max_connections: i32,
    /// Updates fetched from upstream and not yet delivered/dropped.
    pub pending: AtomicI64,
    pub last_error_date: AtomicI64,
    pub last_error_message: Mutex<Option<String>>,
}

impl WebhookState {
    fn new(max_connections: i32) -> Self {
        Self {
            generation: GENERATION.fetch_add(1, Ordering::Relaxed),
            max_connections,
            pending: AtomicI64::new(0),
            last_error_date: AtomicI64::new(0),
            last_error_message: Mutex::new(None),
        }
    }

    fn record_error(&self, msg: &str) {
        self.last_error_date.store(unix_now(), Ordering::Relaxed);
        if let Ok(mut m) = self.last_error_message.lock() {
            *m = Some(msg.chars().take(512).collect());
        }
    }

    pub fn snapshot(&self) -> (i64, Option<i64>, Option<String>) {
        let d = self.last_error_date.load(Ordering::Relaxed);
        let m = self.last_error_message.lock().ok().and_then(|m| m.clone());
        (self.pending.load(Ordering::Relaxed).max(0), if d > 0 { Some(d) } else { None }, m)
    }
}

fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn rand_range(lo: u32, hi: u32) -> u32 {
    use rand::Rng;
    rand::thread_rng().gen_range(lo..=hi)
}

fn rand_unit() -> f64 {
    use rand::Rng;
    rand::thread_rng().gen_range(0.0..1.0)
}

// ---------------------------------------------------------------------------------------------
// Pure logic
// ---------------------------------------------------------------------------------------------

/// C++ `on_update_error` delay computation. Returns `(next_delay, effective_delay)` in seconds.
/// `max_timeout_rand` must be a random value in `MAX_RESEND_TIMEOUT..=2*MAX_RESEND_TIMEOUT`.
/// The very first failure without Retry-After is retried immediately (as in C++).
pub fn next_retry(delay: u32, fail_count: u32, retry_after: u32, max_timeout_rand: u32) -> (u32, u32) {
    let retry_after = retry_after.min(MAX_RETRY_AFTER);
    let mut next_delay = delay;
    let mut effective = retry_after;
    if retry_after == 0 && fail_count > 0 {
        next_delay = max_timeout_rand.min(delay.saturating_mul(2));
        effective = next_delay;
    }
    (next_delay, effective)
}

/// True when waiting `next_effective_delay` more would exceed the update's lifetime.
pub fn should_drop(elapsed_secs: u64, next_effective_delay: u32) -> bool {
    elapsed_secs + next_effective_delay as u64 > DROP_TIMEOUT_SECS
}

/// C++: close the webhook after 410 responses lasting longer than the drop timeout.
pub fn close_after_410(first_410_elapsed_secs: u64) -> bool {
    first_410_elapsed_secs > DROP_TIMEOUT_SECS
}

/// Backoff for failed upstream polls: 2^fails seconds capped at 60, with jitter (factor 0.5..=1.0).
pub fn poll_backoff(consecutive_fails: u32, unit_rand: f64) -> Duration {
    let base = (1u64 << consecutive_fails.min(6)).min(60) as f64;
    let jitter = 0.5 + 0.5 * unit_rand.clamp(0.0, 1.0);
    Duration::from_secs_f64((base * jitter).max(0.5))
}

/// Secret token: at most 256 characters of `[A-Za-z0-9_-]` (empty means "no secret").
pub fn validate_secret_token(s: &str) -> Result<(), &'static str> {
    if s.len() > 256 {
        return Err("secret token is too long");
    }
    if !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err("secret token contains illegal characters");
    }
    Ok(())
}

/// Clamps like C++ `get_integer_arg(.., default, 1, max)`.
pub fn clamp_max_connections(requested: Option<i32>, default: i32, local_mode: bool) -> i32 {
    let max = if local_mode { MAX_CONNECTIONS_LOCAL } else { 100 };
    requested.unwrap_or(default).clamp(1, max)
}

/// Only locally delivered webhooks are restored/run by this process.
pub fn should_restore(local_mode: bool, url: &str) -> bool {
    local_mode && !url.is_empty() && is_local_webhook_url(url)
}

/// URL must be parseable http/https with a host.
pub fn validate_webhook_url(url: &str) -> Result<reqwest::Url, &'static str> {
    let u = reqwest::Url::parse(url).map_err(|_| "invalid webhook URL specified")?;
    if (u.scheme() != "http" && u.scheme() != "https") || u.host_str().is_none() {
        return Err("unsupported URL protocol");
    }
    Ok(u)
}

fn id_of(v: &Value) -> Option<i64> {
    v.get("id").and_then(|x| x.as_i64())
}

/// Ordering key of an update: its chat (or sender when there is no chat).
pub fn queue_key(update: &Value) -> Option<i64> {
    let obj = update.as_object()?;
    for (k, v) in obj {
        if k == "update_id" || !v.is_object() {
            continue;
        }
        if let Some(id) = v.get("chat").and_then(id_of) {
            return Some(id);
        }
        if let Some(id) = v.get("message").and_then(|m| m.get("chat")).and_then(id_of) {
            return Some(id);
        }
        if let Some(id) = v.get("from").and_then(id_of) {
            return Some(id);
        }
    }
    None
}

/// Groups update indices by queue, keeping arrival order inside a group; updates without a
/// key each get their own group. Groups are ordered by first appearance.
pub fn group_by_queue(updates: &[Value]) -> Vec<Vec<usize>> {
    let mut groups: Vec<Vec<usize>> = Vec::new();
    let mut index: HashMap<i64, usize> = HashMap::new();
    for (i, u) in updates.iter().enumerate() {
        match queue_key(u) {
            Some(k) => {
                if let Some(&g) = index.get(&k) {
                    groups[g].push(i);
                } else {
                    index.insert(k, groups.len());
                    groups.push(vec![i]);
                }
            }
            None => groups.push(vec![i]),
        }
    }
    groups
}

/// C++ WebhookActor.cpp:620: a reply with a `method` other than delete/setWebhook/close/logOut/get*.
pub fn reply_method(body: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let m = v.get("method")?.as_str()?.to_ascii_lowercase();
    if m.is_empty() || matches!(m.as_str(), "deletewebhook" | "setwebhook" | "close" | "logout") || m.starts_with("get") {
        return None;
    }
    Some(m)
}

/// Result of one upstream poll.
#[derive(Debug, PartialEq)]
pub enum PollOutcome {
    Updates(Vec<Value>),
    /// Token invalid / bot gone: stop the worker.
    Fatal,
    /// Wait this long, then poll again (None = use exponential backoff).
    Retry(Option<u32>),
}

pub fn classify_poll(body: Option<&Value>) -> PollOutcome {
    let Some(json) = body else {
        return PollOutcome::Retry(None);
    };
    if json.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        return match json.get("result").and_then(|v| v.as_array()) {
            Some(a) => PollOutcome::Updates(a.clone()),
            None => PollOutcome::Retry(None),
        };
    }
    match json.get("error_code").and_then(|v| v.as_i64()).unwrap_or(0) {
        401 | 404 => PollOutcome::Fatal,
        429 => {
            let ra = json
                .get("parameters")
                .and_then(|p| p.get("retry_after"))
                .and_then(|v| v.as_u64())
                .unwrap_or(1)
                .clamp(1, MAX_RETRY_AFTER as u64) as u32;
            PollOutcome::Retry(Some(ra))
        }
        _ => PollOutcome::Retry(None),
    }
}

// ---------------------------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------------------------

struct SendError {
    msg: String,
    status: Option<u16>,
    retry_after: u32,
}

struct Ctx {
    engine: Arc<TelegramEngine>,
    token: String,
    url: String,
    secret: Option<String>,
    client: Client,
    sem: Arc<Semaphore>,
    state: Arc<WebhookState>,
    first_410: Mutex<Option<Instant>>,
    closed: AtomicBool,
}

enum Delivery {
    Delivered,
    Dropped,
    Closed,
}

async fn send_one(ctx: &Ctx, body: Bytes) -> Result<Bytes, SendError> {
    let mut req = ctx.client.post(&ctx.url).header("content-type", "application/json");
    if let Some(ref s) = ctx.secret {
        if !s.is_empty() {
            req = req.header("X-Telegram-Bot-Api-Secret-Token", s);
        }
    }
    let resp = req.body(body).send().await.map_err(|e| SendError {
        msg: format!("Connection error: {}", e.without_url()),
        status: None,
        retry_after: 0,
    })?;
    let status = resp.status();
    let retry_after = resp
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(0);
    if status.is_success() {
        // Cap the amount we read from the reply; only small JSON method calls matter.
        let bytes = resp.bytes().await.unwrap_or_default();
        return Ok(if bytes.len() > 1 << 20 { Bytes::new() } else { bytes });
    }
    Err(SendError {
        msg: format!(
            "Wrong response from the webhook: {} {}",
            status.as_u16(),
            status.canonical_reason().unwrap_or("")
        ),
        status: Some(status.as_u16()),
        retry_after,
    })
}

async fn deliver(ctx: &Ctx, body: Bytes) -> Delivery {
    let started = Instant::now();
    let mut delay = 1u32;
    let mut fails = 0u32;
    loop {
        if ctx.closed.load(Ordering::Relaxed) {
            return Delivery::Closed;
        }
        let out = {
            let _permit = ctx.sem.acquire().await;
            send_one(ctx, body.clone()).await
        };
        match out {
            Ok(reply) => {
                if let Ok(mut f) = ctx.first_410.lock() {
                    *f = None;
                }
                if let Some(method) = reply_method(&reply) {
                    // Result of the method call is ignored, as in C++ (promise has no listener).
                    let _ = ctx
                        .engine
                        .universal_call(
                            &ctx.token,
                            &method,
                            &HashMap::new(),
                            None,
                            Some("application/json"),
                            reply,
                        )
                        .await;
                }
                return Delivery::Delivered;
            }
            Err(e) => {
                ctx.state.record_error(&e.msg);
                if e.status == Some(410) {
                    let close = {
                        let mut f = ctx.first_410.lock().unwrap_or_else(|p| p.into_inner());
                        match *f {
                            None => {
                                *f = Some(Instant::now());
                                false
                            }
                            Some(t) => close_after_410(t.elapsed().as_secs()),
                        }
                    };
                    if close {
                        ctx.closed.store(true, Ordering::Relaxed);
                        return Delivery::Closed;
                    }
                } else if let Ok(mut f) = ctx.first_410.lock() {
                    *f = None;
                }
                let (nd, eff) = next_retry(delay, fails, e.retry_after, rand_range(MAX_RESEND_TIMEOUT, MAX_RESEND_TIMEOUT * 2));
                if should_drop(started.elapsed().as_secs(), eff) {
                    tracing::warn!("Dropping update for bot webhook: {}", e.msg);
                    return Delivery::Dropped;
                }
                delay = nd;
                fails += 1;
                if eff > 0 {
                    tokio::time::sleep(Duration::from_secs(eff as u64)).await;
                }
            }
        }
    }
}

/// Delivers one batch; returns true if the webhook got closed (410 for too long).
async fn deliver_batch(ctx: &Arc<Ctx>, updates: Vec<Value>) -> bool {
    let groups = group_by_queue(&updates);
    let bodies: Vec<Bytes> = updates
        .iter()
        .map(|u| Bytes::from(serde_json::to_vec(u).unwrap_or_default()))
        .collect();
    let mut set = JoinSet::new();
    for g in groups {
        let ctx = Arc::clone(ctx);
        let items: Vec<Bytes> = g.iter().map(|&i| bodies[i].clone()).collect();
        set.spawn(async move {
            for body in items {
                let r = deliver(&ctx, body).await;
                ctx.state.pending.fetch_sub(1, Ordering::Relaxed);
                if matches!(r, Delivery::Closed) {
                    return;
                }
            }
        });
    }
    while set.join_next().await.is_some() {}
    ctx.closed.load(Ordering::Relaxed)
}

fn build_client(engine: &TelegramEngine, url: &str, ip_address: Option<&str>) -> Client {
    if let (Some(ip), Ok(u)) = (ip_address.filter(|s| !s.is_empty()), reqwest::Url::parse(url)) {
        if let (Ok(ip), Some(host), Some(port)) = (ip.parse::<IpAddr>(), u.host_str(), u.port_or_known_default()) {
            let mut b = Client::builder()
                .tcp_nodelay(true)
                .timeout(Duration::from_secs(60))
                .resolve(host, SocketAddr::new(ip, port));
            if let Some(p) = engine.proxy_url.as_deref().and_then(|p| reqwest::Proxy::all(p).ok()) {
                b = b.proxy(p);
            }
            if let Ok(c) = b.build() {
                return c;
            }
        }
    }
    engine.webhook_client.clone()
}

struct ExitGuard {
    engine: Arc<TelegramEngine>,
    token: String,
    generation: u64,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        // Remove the registry entries only if they still belong to this worker generation.
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

async fn run_worker(engine: Arc<TelegramEngine>, cfg: WorkerConfig, state: Arc<WebhookState>) {
    let _guard = ExitGuard {
        engine: Arc::clone(&engine),
        token: cfg.token.clone(),
        generation: state.generation,
    };
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
    });
    let mut next_offset: i64 = 0;

    if cfg.drop_pending_updates {
        let init_url = format!("{}/bot{}/getUpdates?offset=-1&limit=1&timeout=0", engine.upstream_base_url, cfg.token);
        if let Ok(resp) = engine.client.get(&init_url).send().await {
            if let Ok(json) = resp.json::<Value>().await {
                if let Some(uid) = json
                    .get("result")
                    .and_then(|v| v.as_array())
                    .and_then(|a| a.last())
                    .and_then(|u| u.get("update_id"))
                    .and_then(|v| v.as_i64())
                {
                    next_offset = uid + 1;
                }
            }
        }
    }

    let poll_url = format!("{}/bot{}/getUpdates", engine.upstream_base_url, cfg.token);
    let mut fails: u32 = 0;
    loop {
        let mut body = serde_json::json!({ "offset": next_offset, "limit": 100, "timeout": 20 });
        if let Some(ref allowed) = cfg.allowed_updates {
            body["allowed_updates"] = serde_json::to_value(allowed).unwrap_or(Value::Null);
        }
        let json: Option<Value> = match engine.client.post(&poll_url).json(&body).send().await {
            Ok(resp) => resp.json::<Value>().await.ok(),
            Err(_) => None,
        };
        match classify_poll(json.as_ref()) {
            PollOutcome::Updates(updates) => {
                fails = 0;
                if updates.is_empty() {
                    continue;
                }
                let max_uid = updates
                    .iter()
                    .filter_map(|u| u.get("update_id").and_then(|v| v.as_i64()))
                    .max();
                state.pending.store(updates.len() as i64, Ordering::Relaxed);
                let closed = deliver_batch(&ctx, updates).await;
                state.pending.store(0, Ordering::Relaxed);
                if closed {
                    tracing::warn!("Closing webhook of a bot because of HTTP 410 errors");
                    engine.storage.delete_webhook(&cfg.token).await;
                    break;
                }
                // Offset advances only now: every update of the batch was delivered or dropped.
                if let Some(m) = max_uid {
                    next_offset = next_offset.max(m + 1);
                }
            }
            PollOutcome::Fatal => break,
            PollOutcome::Retry(after) => {
                fails = fails.saturating_add(1);
                let d = match after {
                    Some(s) => Duration::from_secs_f64(s as f64 + rand_unit()),
                    None => poll_backoff(fails, rand_unit()),
                };
                if let Some(ref j) = json {
                    if let Some(desc) = j.get("description").and_then(|v| v.as_str()) {
                        tracing::debug!("webhook poll error: {desc}");
                    }
                }
                tokio::time::sleep(d).await;
            }
        }
    }
}

impl TelegramEngine {
    /// Starts or replaces the polling -> webhook delivery worker for a bot (local webhooks only).
    pub fn start_webhook_worker(self: &Arc<Self>, cfg: WorkerConfig) {
        let token = cfg.token.clone();
        self.stop_webhook_worker(&token);
        if cfg.url.is_empty() {
            return;
        }
        self.record_active_bot(&token);
        let state = Arc::new(WebhookState::new(cfg.max_connections));
        let generation = state.generation;
        self.webhook_states.insert(token.clone(), Arc::clone(&state));
        let engine = Arc::clone(self);
        let handle = tokio::spawn(run_worker(engine, cfg, state));
        self.webhook_workers.insert(token.clone(), handle.abort_handle());
        if handle.is_finished() {
            // Worker already exited before registering itself: clean up its stale entry.
            if self
                .webhook_states
                .remove_if(&token, |_, s| s.generation == generation)
                .is_some()
            {
                self.webhook_workers.remove(&token);
            }
        }
    }

    pub fn stop_webhook_worker(&self, token: &str) {
        self.webhook_states.remove(token);
        if let Some((_, handle)) = self.webhook_workers.remove(token) {
            handle.abort();
        }
    }

    /// Restores locally delivered webhooks from storage. Cloud webhooks are left alone.
    pub async fn restore_webhooks(self: &Arc<Self>) {
        let binlog_path = self.dir.join("webhooks_db.binlog");
        if binlog_path.exists() {
            if let Ok(bytes) = tokio::fs::read(&binlog_path).await {
                for (token, url) in Self::parse_webhooks_binlog(&bytes) {
                    // Import only local webhooks, and never overwrite an existing stored record.
                    if should_restore(self.local_mode, &url) && self.storage.get_webhook(&token).await.is_none() {
                        self.storage
                            .save_webhook(&token, &url, None, self.default_max_webhook_connections, None, None)
                            .await;
                    }
                }
            }
        }

        let saved = self.storage.get_all_webhooks().await;
        let mut count = 0;
        for (token, record) in saved {
            if !should_restore(self.local_mode, &record.url) {
                continue;
            }
            count += 1;
            self.start_webhook_worker(WorkerConfig {
                token,
                url: record.url,
                secret_token: record.secret_token,
                allowed_updates: record.allowed_updates,
                drop_pending_updates: false,
                max_connections: clamp_max_connections(Some(record.max_connections), self.default_max_webhook_connections, true),
                ip_address: record.ip_address,
            });
        }
        if count > 0 {
            tracing::info!("Restored {} local webhook configurations from SQLite WAL", count);
        }
    }

    /// 1 second minimum between setWebhook calls of one bot (C++ `next_allowed_set_webhook_time_`).
    pub fn check_set_webhook_throttle(&self, token: &str) -> Result<(), TelegramError> {
        let now = Instant::now();
        let mut entry = self.set_webhook_times.entry(token.to_string()).or_insert(now - Duration::from_secs(2));
        if now < *entry {
            return Err(TelegramError::flood_wait(1));
        }
        *entry = now + Duration::from_secs(1);
        drop(entry);
        if self.set_webhook_times.len() > 8192 {
            self.set_webhook_times.retain(|_, t| *t > now);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn retry_schedule_matches_cpp() {
        // first failure: immediate retry
        assert_eq!(next_retry(1, 0, 0, 90), (1, 0));
        // then doubling until the random cap
        assert_eq!(next_retry(1, 1, 0, 90), (2, 2));
        assert_eq!(next_retry(2, 2, 0, 90), (4, 4));
        assert_eq!(next_retry(64, 6, 0, 90), (90, 90));
        assert_eq!(next_retry(60, 7, 0, 60), (60, 60));
        // retry_after wins and is clamped
        assert_eq!(next_retry(8, 3, 30, 90), (8, 30));
        assert_eq!(next_retry(8, 0, 999_999, 90), (8, 3600));
    }

    #[test]
    fn drop_rules() {
        assert!(!should_drop(10, 60));
        assert!(should_drop(DROP_TIMEOUT_SECS, 1));
        assert!(!close_after_410(100));
        assert!(close_after_410(DROP_TIMEOUT_SECS + 1));
    }

    #[test]
    fn poll_backoff_bounds() {
        assert!(poll_backoff(1, 0.0) >= Duration::from_secs(1));
        assert!(poll_backoff(1, 1.0) <= Duration::from_secs(2));
        assert!(poll_backoff(30, 1.0) <= Duration::from_secs(60));
        assert!(poll_backoff(30, 0.0) >= Duration::from_secs(30));
        assert!(poll_backoff(0, 0.0) >= Duration::from_millis(500));
    }

    #[test]
    fn secret_token() {
        assert!(validate_secret_token("abc_DEF-123").is_ok());
        assert!(validate_secret_token("").is_ok());
        assert!(validate_secret_token(&"a".repeat(256)).is_ok());
        assert!(validate_secret_token(&"a".repeat(257)).is_err());
        assert!(validate_secret_token("bad token").is_err());
        assert!(validate_secret_token("bad!").is_err());
    }

    #[test]
    fn max_connections_clamp() {
        assert_eq!(clamp_max_connections(None, 40, false), 40);
        assert_eq!(clamp_max_connections(Some(0), 40, false), 1);
        assert_eq!(clamp_max_connections(Some(-5), 40, true), 1);
        assert_eq!(clamp_max_connections(Some(5000), 40, false), 100);
        assert_eq!(clamp_max_connections(Some(5000), 40, true), 5000);
    }

    #[test]
    fn restore_filter() {
        assert!(should_restore(true, "http://127.0.0.1:8080/hook"));
        assert!(!should_restore(true, "https://example.com/hook"));
        assert!(!should_restore(true, "http://example.com/hook"));
        assert!(!should_restore(true, ""));
        assert!(!should_restore(false, "http://127.0.0.1:8080/hook"));
    }

    #[test]
    fn url_validation() {
        assert!(validate_webhook_url("https://example.com/x").is_ok());
        assert!(validate_webhook_url("http://127.0.0.1:1/").is_ok());
        assert!(validate_webhook_url("ftp://example.com/").is_err());
        assert!(validate_webhook_url("not a url").is_err());
    }

    #[test]
    fn queue_ordering_groups() {
        let ups = vec![
            json!({"update_id":1,"message":{"chat":{"id":10}}}),
            json!({"update_id":2,"message":{"chat":{"id":20}}}),
            json!({"update_id":3,"edited_message":{"chat":{"id":10}}}),
            json!({"update_id":4,"callback_query":{"from":{"id":5},"message":{"chat":{"id":20}}}}),
            json!({"update_id":5,"inline_query":{"from":{"id":7}}}),
            json!({"update_id":6,"poll":{"id":"x"}}),
            json!({"update_id":7,"poll":{"id":"y"}}),
        ];
        let g = group_by_queue(&ups);
        assert_eq!(g, vec![vec![0, 2], vec![1, 3], vec![4], vec![5], vec![6]]);
    }

    #[test]
    fn reply_methods() {
        assert_eq!(reply_method(br#"{"method":"sendMessage","chat_id":1,"text":"x"}"#).as_deref(), Some("sendmessage"));
        assert_eq!(reply_method(br#"{"method":"getMe"}"#), None);
        assert_eq!(reply_method(br#"{"method":"deleteWebhook"}"#), None);
        assert_eq!(reply_method(b"OK"), None);
        assert_eq!(reply_method(b""), None);
        assert_eq!(reply_method(br#"{"ok":true}"#), None);
    }

    #[test]
    fn poll_classification() {
        assert_eq!(classify_poll(None), PollOutcome::Retry(None));
        assert_eq!(classify_poll(Some(&json!({"ok":true,"result":[]}))), PollOutcome::Updates(vec![]));
        assert_eq!(classify_poll(Some(&json!({"ok":false,"error_code":401}))), PollOutcome::Fatal);
        assert_eq!(classify_poll(Some(&json!({"ok":false,"error_code":404}))), PollOutcome::Fatal);
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":429,"parameters":{"retry_after":7}}))),
            PollOutcome::Retry(Some(7))
        );
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":429,"parameters":{"retry_after":99999}}))),
            PollOutcome::Retry(Some(3600))
        );
        assert_eq!(classify_poll(Some(&json!({"ok":false,"error_code":409}))), PollOutcome::Retry(None));
        assert_eq!(classify_poll(Some(&json!({"ok":true}))), PollOutcome::Retry(None));
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    type Log = Arc<Mutex<Vec<String>>>;

    async fn read_request(sock: &mut tokio::net::TcpStream) -> Option<(String, Vec<u8>)> {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            let n = sock.read(&mut tmp).await.ok()?;
            if n == 0 {
                return None;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&buf[..pos]).to_string();
                let cl = head
                    .lines()
                    .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length:").map(|v| v.trim().parse::<usize>().unwrap_or(0)))
                    .unwrap_or(0);
                while buf.len() < pos + 4 + cl {
                    let n = sock.read(&mut tmp).await.ok()?;
                    if n == 0 {
                        break;
                    }
                    buf.extend_from_slice(&tmp[..n]);
                }
                return Some((head, buf[pos + 4..].to_vec()));
            }
        }
    }

    async fn respond(sock: &mut tokio::net::TcpStream, status: &str, body: &str) {
        let r = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        let _ = sock.write_all(r.as_bytes()).await;
        let _ = sock.shutdown().await;
    }

    fn test_engine(name: &str, up_port: u16) -> (Arc<TelegramEngine>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("{name}_{}", std::process::id()));
        let storage = crate::storage::StorageEngine::new(&dir).unwrap();
        let mut engine = TelegramEngine::new(true, dir.clone(), dir.clone(), storage);
        engine.upstream_base_url = format!("http://127.0.0.1:{up_port}");
        (Arc::new(engine), dir)
    }

    fn cfg(token: &str, url: String) -> WorkerConfig {
        WorkerConfig {
            token: token.to_string(),
            url,
            secret_token: None,
            allowed_updates: None,
            drop_pending_updates: false,
            max_connections: 5,
            ip_address: None,
        }
    }

    #[tokio::test]
    async fn offset_advances_only_after_webhook_2xx() {
        let log: Log = Arc::new(Mutex::new(Vec::new()));

        // Mock webhook: 500 first, then 200.
        let wh = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let wh_port = wh.local_addr().unwrap().port();
        let wlog = log.clone();
        tokio::spawn(async move {
            let mut n = 0;
            loop {
                let (mut s, _) = wh.accept().await.unwrap();
                let Some((_, body)) = read_request(&mut s).await else { continue };
                n += 1;
                wlog.lock().unwrap().push(format!("wh:{}", String::from_utf8_lossy(&body)));
                if n == 1 {
                    respond(&mut s, "500 Internal Server Error", "boom").await;
                } else {
                    respond(&mut s, "200 OK", "").await;
                }
            }
        });

        // Mock upstream: update 7 for offset 0; empty afterwards.
        let up = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_port = up.local_addr().unwrap().port();
        let ulog = log.clone();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = up.accept().await.unwrap();
                let ulog = ulog.clone();
                tokio::spawn(async move {
                    let Some((_, body)) = read_request(&mut s).await else { return };
                    let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let off = v.get("offset").and_then(|o| o.as_i64()).unwrap_or(-99);
                    ulog.lock().unwrap().push(format!("poll:{off}"));
                    if off == 0 {
                        respond(&mut s, "200 OK", r#"{"ok":true,"result":[{"update_id":7,"message":{"chat":{"id":1},"text":"hi"}}]}"#).await;
                    } else {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        respond(&mut s, "200 OK", r#"{"ok":true,"result":[]}"#).await;
                    }
                });
            }
        });

        let (engine, dir) = test_engine("wh_it", up_port);
        let token = "123456:ABCDEF".to_string();
        engine.start_webhook_worker(cfg(&token, format!("http://127.0.0.1:{wh_port}/hook")));

        let mut seen = Vec::new();
        for _ in 0..100 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            seen = log.lock().unwrap().clone();
            if seen.iter().any(|e| e == "poll:8") {
                break;
            }
        }
        // Order: poll:0, webhook(500), webhook(200, same body), only then poll:8.
        assert!(seen.len() >= 4, "{seen:?}");
        assert_eq!(seen[0], "poll:0");
        assert!(seen[1].starts_with("wh:") && seen[1] == seen[2], "same update twice: {seen:?}");
        assert_eq!(seen[3], "poll:8", "offset must advance only after the 200: {seen:?}");

        // getWebhookInfo-visible state: error recorded, nothing pending.
        let st = engine.webhook_states.get(&token).unwrap().clone();
        let (pending, date, msg) = st.snapshot();
        assert_eq!(pending, 0);
        assert!(date.is_some());
        assert!(msg.unwrap().contains("500"));
        engine.stop_webhook_worker(&token);
        assert!(!engine.webhook_workers.contains_key(&token));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn set_webhook_throttle_one_second() {
        let (engine, dir) = test_engine("wh_thr", 1);
        assert!(engine.check_set_webhook_throttle("1:a").is_ok());
        match engine.check_set_webhook_throttle("1:a") {
            Err(TelegramError::ApiError { code: 429, parameters, .. }) => {
                assert_eq!(parameters.unwrap().retry_after, Some(1));
            }
            other => panic!("expected 429, got {other:?}"),
        }
        assert!(engine.check_set_webhook_throttle("2:b").is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn worker_exit_on_401_clears_registry() {
        let up = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up_port = up.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = up.accept().await.unwrap();
                tokio::spawn(async move {
                    let _ = read_request(&mut s).await;
                    respond(&mut s, "401 Unauthorized", r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#).await;
                });
            }
        });
        let (engine, dir) = test_engine("wh_401", up_port);
        engine.start_webhook_worker(cfg("123456:XYZ", "http://127.0.0.1:9/hook".into()));
        for _ in 0..100 {
            if !engine.webhook_workers.contains_key("123456:XYZ") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(!engine.webhook_workers.contains_key("123456:XYZ"));
        assert!(!engine.webhook_states.contains_key("123456:XYZ"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
