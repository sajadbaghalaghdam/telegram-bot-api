//! Local webhook delivery: a long-polling worker per bot that pushes updates to a locally
//! reachable webhook URL (see `is_local_webhook_url`). Cloud (non-local) webhooks are never
//! handled here; they are proxied to the upstream Bot API untouched.
//!
//! Semantics follow the C++ `WebhookActor`:
//! * the worker polls upstream continuously; every update goes into the FIFO queue of its chat
//!   (queue key), each queue has its own delivery task and retry timer, so one failing chat never
//!   blocks the others; at most `max_connections` requests are in flight and at most
//!   `2 * max_connections` updates are loaded at once;
//! * the getUpdates offset is a watermark: it only confirms updates below the lowest id that is
//!   not yet delivered/dropped (`Window`), so delivery stays at-least-once. Upstream re-sends
//!   everything >= offset, so already loaded ids are skipped. Known limitation: one poll page is
//!   100 updates, so once a stuck update has 99 finished successors behind it polling pauses until
//!   it is delivered or dropped;
//! * failures are retried with exponential backoff (`next_retry`), honoring `Retry-After`; an
//!   update is dropped 23h after it was first seen (the timer restarts on process restart);
//! * a 2xx reply that is a JSON object with a `method` field is executed as a Bot API call, in a
//!   detached task (result ignored, never delays delivery);
//! * `pending_update_count` of getWebhookInfo counts updates loaded locally and not yet finished;
//!   the backlog still held by upstream is not included.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Client;
use serde_json::Value;
use tokio::sync::{Notify, Semaphore};
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

/// URL must be parseable http/https with a host. Like C++ `parse_url(url, Protocol::Https)` a
/// scheme-less URL is retried with `https://` prepended. Returns the URL string to use.
pub fn normalize_webhook_url(url: &str) -> Result<String, &'static str> {
    let candidate = if url.contains("://") { url.to_string() } else { format!("https://{url}") };
    let u = reqwest::Url::parse(&candidate).map_err(|_| "invalid webhook URL specified")?;
    if (u.scheme() != "http" && u.scheme() != "https") || u.host_str().is_none() {
        return Err("unsupported URL protocol");
    }
    Ok(candidate)
}

/// URL must be parseable http/https with a host (scheme-less URLs get `https://`).
pub fn validate_webhook_url(url: &str) -> Result<reqwest::Url, &'static str> {
    let n = normalize_webhook_url(url)?;
    reqwest::Url::parse(&n).map_err(|_| "invalid webhook URL specified")
}

/// C++ WebhookActor: "Invalid IP address specified".
pub fn validate_ip_address(ip: &str) -> Result<(), &'static str> {
    ip.parse::<IpAddr>().map(|_| ()).map_err(|_| "invalid IP address specified")
}

// ---------------------------------------------------------------------------------------------
// setWebhook argument parsing (C++ reads `query->arg(..)` regardless of the content type)
// ---------------------------------------------------------------------------------------------

/// The original HTTP request, forwarded unchanged when the webhook is set on the cloud server.
pub struct RawForward<'a> {
    pub query: Option<&'a str>,
    pub content_type: Option<&'a str>,
    pub body: &'a Bytes,
}

fn percent_decode_form(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let h = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
                match h {
                    Some(v) => {
                        out.push(v);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_urlencoded(body: &[u8], out: &mut HashMap<String, String>) {
    for pair in String::from_utf8_lossy(body).split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode_form(k), percent_decode_form(v));
    }
}

fn json_scalar_to_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn find_sub(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from > hay.len() {
        return None;
    }
    hay[from..].windows(needle.len()).position(|w| w == needle).map(|p| p + from)
}

fn header_param(disposition: &str, key: &str) -> Option<String> {
    for part in disposition.split(';').skip(1) {
        if let Some((k, v)) = part.trim().split_once('=')
            && k.trim().eq_ignore_ascii_case(key) {
                return Some(v.trim().trim_matches('"').to_string());
            }
    }
    None
}

/// Parses text fields of a multipart/form-data body (file parts such as `certificate` are skipped).
fn parse_multipart(body: &[u8], boundary: &str, out: &mut HashMap<String, String>) -> Result<(), String> {
    const BAD: &str = "can't parse multipart/form-data request body";
    let delim = format!("--{boundary}").into_bytes();
    let mut next_delim = b"\r\n".to_vec();
    next_delim.extend_from_slice(&delim);
    let mut pos = find_sub(body, &delim, 0).ok_or(BAD)? + delim.len();
    loop {
        if body[pos..].starts_with(b"--") {
            return Ok(());
        }
        if !body[pos..].starts_with(b"\r\n") {
            return Err(BAD.into());
        }
        pos += 2;
        let (head_end, body_start) = if body[pos..].starts_with(b"\r\n") {
            (pos, pos + 2)
        } else {
            let e = find_sub(body, b"\r\n\r\n", pos).ok_or(BAD)?;
            (e, e + 4)
        };
        let head = String::from_utf8_lossy(&body[pos..head_end]).into_owned();
        let end = find_sub(body, &next_delim, body_start).ok_or(BAD)?;
        let content = &body[body_start..end];
        let disposition = head
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-disposition:"))
            .map(|l| l.to_string());
        if let Some(d) = disposition
            && let Some(name) = header_param(&d, "name")
                && header_param(&d, "filename").is_none() {
                    out.insert(name, String::from_utf8_lossy(content).into_owned());
                }
        pos = end + next_delim.len();
        if pos > body.len() {
            return Err(BAD.into());
        }
    }
}

/// Merges the URL query and the request body (JSON, form-urlencoded or multipart) into one
/// argument map, the way C++ `Query::arg` sees them. Body values win over query values.
/// A body that cannot be parsed is an error (never silently treated as "no arguments").
pub fn parse_request_args(
    query: &HashMap<String, String>,
    content_type: Option<&str>,
    body: &[u8],
) -> Result<HashMap<String, String>, String> {
    let mut args = query.clone();
    if body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(args);
    }
    let ct = content_type.unwrap_or("").trim();
    let ct_lower = ct.to_ascii_lowercase();
    let starts_obj = body.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'{');
    if ct_lower.starts_with("multipart/form-data") {
        let boundary = header_param(&format!("x;{ct}"), "boundary").filter(|b| !b.is_empty());
        let boundary = boundary.ok_or_else(|| "multipart/form-data boundary is missing".to_string())?;
        parse_multipart(body, &boundary, &mut args)?;
    } else if ct_lower.starts_with("application/x-www-form-urlencoded") {
        parse_urlencoded(body, &mut args);
    } else if ct_lower.contains("json") || (ct_lower.is_empty() && starts_obj) {
        let v: Value = serde_json::from_slice(body).map_err(|_| "can't parse JSON object request body".to_string())?;
        let obj = v.as_object().ok_or_else(|| "JSON request body must be an object".to_string())?;
        for (k, v) in obj {
            if let Some(s) = json_scalar_to_string(v) {
                args.insert(k.clone(), s);
            }
        }
    } else if ct_lower.is_empty() {
        parse_urlencoded(body, &mut args);
    } else {
        return Err("unsupported request Content-Type".to_string());
    }
    Ok(args)
}

/// C++ `to_bool`: trimmed, case-insensitive "true" / "yes" / "1".
pub fn arg_to_bool(s: &str) -> bool {
    matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "yes" | "1")
}

/// Builds the setWebhook parameters from the merged argument map.
pub fn set_webhook_request_from_args(args: &HashMap<String, String>) -> Result<crate::models::SetWebhookRequest, String> {
    let allowed_updates = match args.get("allowed_updates").map(|s| s.trim()).filter(|s| !s.is_empty()) {
        None => None,
        Some(s) => Some(
            serde_json::from_str::<Vec<String>>(s).map_err(|_| "can't parse allowed_updates as a JSON array of strings".to_string())?,
        ),
    };
    Ok(crate::models::SetWebhookRequest {
        url: args.get("url").cloned().unwrap_or_default(),
        ip_address: args.get("ip_address").cloned(),
        max_connections: args.get("max_connections").and_then(|v| v.trim().parse::<i32>().ok()),
        allowed_updates,
        drop_pending_updates: args.get("drop_pending_updates").map(|v| arg_to_bool(v)),
        secret_token: args.get("secret_token").cloned(),
    })
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

/// Identity of a delivery queue. Updates of one chat are delivered strictly in order; updates
/// without any chat/sender get a queue of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueId {
    Chat(i64),
    Solo(i64),
}

pub fn queue_id(update: &Value, update_id: i64) -> QueueId {
    queue_key(update).map(QueueId::Chat).unwrap_or(QueueId::Solo(update_id))
}

/// Upper bound of `limit` of one getUpdates call (Bot API).
pub const POLL_LIMIT: usize = 100;

/// Tracks which update ids are loaded and which are finished (delivered or dropped), so that the
/// getUpdates offset only ever confirms updates below the lowest id that is still unfinished
/// (a watermark): a slow or failing chat can never cause an update of another chat to be lost,
/// and nothing is confirmed upstream before it was delivered (at-least-once).
#[derive(Debug)]
pub struct Window {
    /// Everything below this id is finished and confirmed.
    base: i64,
    max_seen: i64,
    unfinished: BTreeSet<i64>,
    /// Finished ids above the watermark (they are re-sent by upstream until confirmed).
    finished: BTreeSet<i64>,
}

impl Window {
    pub fn new(base: i64) -> Self {
        Self { base, max_seen: base - 1, unfinished: BTreeSet::new(), finished: BTreeSet::new() }
    }

    /// The getUpdates `offset` that is safe to send.
    pub fn offset(&self) -> i64 {
        self.unfinished.first().copied().unwrap_or(self.max_seen + 1).max(self.base)
    }

    pub fn is_known(&self, id: i64) -> bool {
        id < self.offset() || self.unfinished.contains(&id) || self.finished.contains(&id)
    }

    /// Registers a freshly loaded update; false if it was already seen (upstream re-sends
    /// everything that is not confirmed yet).
    pub fn add(&mut self, id: i64) -> bool {
        if self.is_known(id) {
            return false;
        }
        self.unfinished.insert(id);
        self.max_seen = self.max_seen.max(id);
        true
    }

    pub fn finish(&mut self, id: i64) {
        if self.unfinished.remove(&id) {
            self.finished.insert(id);
        }
        self.base = self.offset();
        self.finished = self.finished.split_off(&self.base);
    }

    /// Loaded and not yet delivered/dropped.
    pub fn unfinished(&self) -> usize {
        self.unfinished.len()
    }

    /// Ids upstream will return again on the next poll (everything at or above the offset).
    pub fn occupancy(&self) -> usize {
        self.unfinished.len() + self.finished.len()
    }
}

struct Item {
    id: i64,
    body: Bytes,
    /// When this update was first seen; the drop timeout is measured from here, per update.
    first_seen: Instant,
}

/// Scheduler state: the offset window plus the per-queue FIFOs. A queue key exists exactly while
/// its task is running.
struct Inner {
    window: Window,
    queues: HashMap<QueueId, VecDeque<Item>>,
}

impl Inner {
    fn new(base: i64) -> Self {
        Self { window: Window::new(base), queues: HashMap::new() }
    }

    /// None: duplicate. Some(true): a new queue was created and its task must be spawned.
    fn admit(&mut self, qid: QueueId, item: Item) -> Option<bool> {
        if !self.window.add(item.id) {
            return None;
        }
        let spawn = !self.queues.contains_key(&qid);
        self.queues.entry(qid).or_default().push_back(item);
        Some(spawn)
    }

    /// Next item of a queue; removes the (empty) queue, which tells `admit` to spawn a new task.
    fn next(&mut self, qid: QueueId) -> Option<Item> {
        let item = self.queues.get_mut(&qid)?.pop_front();
        if item.is_none() {
            self.queues.remove(&qid);
        }
        item
    }

    /// `(offset, limit)` for the next poll, or None while the window is full: at most `cap`
    /// updates may be loaded at once, and since upstream re-sends everything >= offset, the
    /// window (unfinished + finished-above-watermark) must stay below one poll page.
    fn poll_window(&self, cap: usize) -> Option<(i64, usize)> {
        let room = cap.saturating_sub(self.window.unfinished());
        let occ = self.window.occupancy();
        if room == 0 || occ >= POLL_LIMIT {
            return None;
        }
        Some((self.window.offset(), POLL_LIMIT.min(occ + room)))
    }
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
    /// 409 because a cloud webhook is still set upstream: it has to be deleted again.
    WebhookActive,
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
        409 if json
            .get("description")
            .and_then(|v| v.as_str())
            .is_some_and(|d| d.to_ascii_lowercase().contains("webhook is active")) =>
        {
            PollOutcome::WebhookActive
        }
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

/// Human readable reason of a failed poll, for logs and `getWebhookInfo.last_error_message`.
pub fn poll_error_text(body: Option<&Value>, transport_error: Option<&str>) -> String {
    if let Some(e) = transport_error {
        return format!("getUpdates failed: {e}");
    }
    match body {
        Some(j) => {
            let code = j.get("error_code").and_then(|v| v.as_i64()).unwrap_or(0);
            let desc = j.get("description").and_then(|v| v.as_str()).unwrap_or("unexpected response");
            format!("getUpdates failed: {code} {desc}")
        }
        None => "getUpdates failed: unreadable response".to_string(),
    }
}

// ---------------------------------------------------------------------------------------------
// Delivery
// ---------------------------------------------------------------------------------------------

/// Hard cap of the webhook reply body we are willing to read (only small JSON method calls matter).
pub const MAX_REPLY_BYTES: usize = 1 << 20;
/// Poll interval while updates are loaded but unfinished (upstream answers instantly then).
const BUSY_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Minimum spacing of deleteWebhook repairs after a "webhook is active" conflict.
const CONFLICT_FIX_INTERVAL: Duration = Duration::from_secs(30);

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
    inner: Mutex<Inner>,
    /// Signalled whenever an update finished (the watermark may have advanced).
    progress: Notify,
    closed_notify: Notify,
}

impl Ctx {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn sync_pending(&self, inner: &Inner) {
        self.state.pending.store(inner.window.unfinished() as i64, Ordering::Relaxed);
    }

    fn finish(&self, id: i64) {
        {
            let mut g = self.lock();
            g.window.finish(id);
            self.sync_pending(&g);
        }
        self.progress.notify_one();
    }
}

enum Delivery {
    Delivered,
    Dropped,
    Closed,
}

/// Reads a successful reply, giving up (empty result) as soon as it exceeds `cap` bytes.
async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Bytes {
    if resp.content_length().is_some_and(|l| l > cap as u64) {
        return Bytes::new();
    }
    let mut buf: Vec<u8> = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(c)) => {
                if buf.len() + c.len() > cap {
                    return Bytes::new();
                }
                buf.extend_from_slice(&c);
            }
            Ok(None) => break,
            Err(_) => return Bytes::new(),
        }
    }
    Bytes::from(buf)
}

async fn send_one(ctx: &Ctx, body: Bytes) -> Result<Bytes, SendError> {
    let mut req = ctx.client.post(&ctx.url).header("content-type", "application/json");
    if let Some(ref s) = ctx.secret
        && !s.is_empty() {
            req = req.header("X-Telegram-Bot-Api-Secret-Token", s);
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
        return Ok(read_capped(resp, MAX_REPLY_BYTES).await);
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

/// Delivers one update, retrying with backoff (the retry timer belongs to the update's queue;
/// other queues are unaffected). The drop timeout counts from the update's own first-seen time.
async fn deliver(ctx: &Arc<Ctx>, item: &Item) -> Delivery {
    let mut delay = 1u32;
    let mut fails = 0u32;
    loop {
        if ctx.closed.load(Ordering::Relaxed) {
            return Delivery::Closed;
        }
        let out = {
            let _permit = ctx.sem.acquire().await;
            send_one(ctx, item.body.clone()).await
        };
        match out {
            Ok(reply) => {
                if let Ok(mut f) = ctx.first_410.lock() {
                    *f = None;
                }
                if let Some(method) = reply_method(&reply) {
                    // Fire and forget, as C++ WebhookActor.cpp:620-631: the result is ignored and
                    // must never delay the delivery of the next update.
                    let engine = Arc::clone(&ctx.engine);
                    let token = ctx.token.clone();
                    tokio::spawn(async move {
                        let _ = engine
                            .universal_call(&token, &method, &HashMap::new(), None, Some("application/json"), reply)
                            .await;
                    });
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
                if should_drop(item.first_seen.elapsed().as_secs(), eff) {
                    tracing::warn!("Dropping update {} for bot webhook: {}", item.id, e.msg);
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

/// Drains one queue in FIFO order; the queue's retries never hold up any other queue.
async fn run_queue(ctx: Arc<Ctx>, qid: QueueId) {
    loop {
        let item = ctx.lock().next(qid);
        let Some(item) = item else { return };
        match deliver(&ctx, &item).await {
            Delivery::Closed => {
                ctx.closed_notify.notify_one();
                return;
            }
            Delivery::Delivered | Delivery::Dropped => ctx.finish(item.id),
        }
    }
}

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

/// True when the throttle map may be swept again (at most once per second).
fn sweep_due(last_ms: u64, now_ms: u64) -> bool {
    now_ms.saturating_sub(last_ms) >= 1000
}

static LAST_SWEEP_MS: AtomicU64 = AtomicU64::new(0);

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

    /// Restores locally delivered webhooks from storage. Cloud webhooks are left alone.
    pub async fn restore_webhooks(self: &Arc<Self>) {
        let binlog_path = self.dir.join("webhooks_db.binlog");
        if binlog_path.exists()
            && let Ok(bytes) = tokio::fs::read(&binlog_path).await {
                for (token, url) in Self::parse_webhooks_binlog(&bytes) {
                    // Import only local webhooks, and never overwrite an existing stored record.
                    if should_restore(self.local_mode, &url) && self.storage.get_webhook(&token).await.is_none() {
                        self.storage
                            .save_webhook(&token, &url, None, self.default_max_webhook_connections, None, None)
                            .await;
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
        // Sweep expired entries, but at most once per second so a flood of tokens cannot make
        // every call scan the whole map.
        if self.set_webhook_times.len() > 1024 {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let last = LAST_SWEEP_MS.load(Ordering::Relaxed);
            if sweep_due(last, now_ms)
                && LAST_SWEEP_MS.compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok()
            {
                self.set_webhook_times.retain(|_, t| *t > now);
            }
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
    fn scheme_less_url_gets_https() {
        assert_eq!(normalize_webhook_url("example.com/hook").unwrap(), "https://example.com/hook");
        assert_eq!(normalize_webhook_url("localhost:8080/x").unwrap(), "https://localhost:8080/x");
        assert_eq!(normalize_webhook_url("http://127.0.0.1:1/").unwrap(), "http://127.0.0.1:1/");
        assert!(normalize_webhook_url("ftp://example.com/").is_err());
        assert!(normalize_webhook_url("").is_err());
        assert!(normalize_webhook_url("a b").is_err());
    }

    #[test]
    fn ip_address_validation() {
        assert!(validate_ip_address("1.2.3.4").is_ok());
        assert!(validate_ip_address("::1").is_ok());
        assert!(validate_ip_address("example.com").is_err());
        assert!(validate_ip_address("999.1.1.1").is_err());
    }

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn args_from_query_only() {
        let a = parse_request_args(&q(&[("url", "http://127.0.0.1:1/h")]), None, b"").unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "http://127.0.0.1:1/h");
        assert_eq!(r.max_connections, None);
    }

    #[test]
    fn args_from_json_body() {
        let body = br#"{"url":"http://127.0.0.1:1/h","max_connections":7,"drop_pending_updates":true,"secret_token":"s_1","allowed_updates":["message","edited_message"],"ip_address":"127.0.0.1"}"#;
        let a = parse_request_args(&q(&[]), Some("application/json; charset=utf-8"), body).unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "http://127.0.0.1:1/h");
        assert_eq!(r.max_connections, Some(7));
        assert_eq!(r.drop_pending_updates, Some(true));
        assert_eq!(r.secret_token.as_deref(), Some("s_1"));
        assert_eq!(r.allowed_updates, Some(vec!["message".into(), "edited_message".into()]));
        assert_eq!(r.ip_address.as_deref(), Some("127.0.0.1"));
    }

    #[test]
    fn args_from_form_urlencoded_body() {
        let body = b"url=http%3A%2F%2F127.0.0.1%3A1%2Fh%3Fa%3Db&max_connections=9&drop_pending_updates=yes&secret_token=abc&allowed_updates=%5B%22message%22%5D";
        let a = parse_request_args(&q(&[]), Some("application/x-www-form-urlencoded"), body).unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "http://127.0.0.1:1/h?a=b");
        assert_eq!(r.max_connections, Some(9));
        assert_eq!(r.drop_pending_updates, Some(true));
        assert_eq!(r.secret_token.as_deref(), Some("abc"));
        assert_eq!(r.allowed_updates, Some(vec!["message".into()]));
        // no content type at all: curl -d style bodies are form data
        let a = parse_request_args(&q(&[]), None, b"url=http%3A%2F%2Fx.y%2F&a+b=c+d").unwrap();
        assert_eq!(a["url"], "http://x.y/");
        assert_eq!(a["a b"], "c d");
    }

    #[test]
    fn args_from_multipart_body_skips_certificate() {
        let b = "XyZ";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"url\"\r\n\r\nhttps://example.com/hook\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"certificate\"; filename=\"cert.pem\"\r\nContent-Type: application/x-pem-file\r\n\r\n-----BEGIN CERTIFICATE-----\r\nAAA\r\n-----END CERTIFICATE-----\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"max_connections\"\r\n\r\n12\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"allowed_updates\"\r\n\r\n[\"message\"]\r\n\
             --{b}--\r\n"
        );
        let ct = format!("multipart/form-data; boundary=\"{b}\"");
        let a = parse_request_args(&q(&[("secret_token", "fromquery")]), Some(&ct), body.as_bytes()).unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "https://example.com/hook");
        assert_eq!(r.max_connections, Some(12));
        assert_eq!(r.secret_token.as_deref(), Some("fromquery"));
        assert_eq!(r.allowed_updates, Some(vec!["message".into()]));
        assert!(!a.contains_key("certificate"));
    }

    #[test]
    fn unparsable_bodies_are_errors_not_empty_args() {
        assert!(parse_request_args(&q(&[]), Some("application/json"), b"{not json").is_err());
        assert!(parse_request_args(&q(&[]), Some("application/json"), b"[1,2]").is_err());
        assert!(parse_request_args(&q(&[]), Some("multipart/form-data"), b"--x\r\n").is_err());
        assert!(parse_request_args(&q(&[]), Some("multipart/form-data; boundary=x"), b"garbage").is_err());
        assert!(parse_request_args(&q(&[]), Some("text/plain"), b"url=x").is_err());
        let bad = q(&[("allowed_updates", "not json")]);
        assert!(set_webhook_request_from_args(&bad).is_err());
    }

    #[test]
    fn body_overrides_query_and_bool_parsing() {
        let a = parse_request_args(&q(&[("url", "http://q/")]), Some("application/json"), br#"{"url":"http://b/"}"#).unwrap();
        assert_eq!(a["url"], "http://b/");
        assert!(arg_to_bool(" TRUE ") && arg_to_bool("1") && arg_to_bool("yes"));
        assert!(!arg_to_bool("false") && !arg_to_bool(""));
    }

    fn item(id: i64) -> Item {
        Item { id, body: Bytes::from(id.to_string()), first_seen: Instant::now() }
    }

    #[test]
    fn queue_ids() {
        let u = |v: Value| {
            let id = v["update_id"].as_i64().unwrap();
            queue_id(&v, id)
        };
        assert_eq!(u(json!({"update_id":1,"message":{"chat":{"id":10}}})), QueueId::Chat(10));
        assert_eq!(u(json!({"update_id":3,"edited_message":{"chat":{"id":10}}})), QueueId::Chat(10));
        assert_eq!(u(json!({"update_id":4,"callback_query":{"from":{"id":5},"message":{"chat":{"id":20}}}})), QueueId::Chat(20));
        assert_eq!(u(json!({"update_id":5,"inline_query":{"from":{"id":7}}})), QueueId::Chat(7));
        assert_eq!(u(json!({"update_id":6,"poll":{"id":"x"}})), QueueId::Solo(6));
    }

    #[test]
    fn queue_is_fifo_and_independent_per_chat() {
        let mut inner = Inner::new(0);
        let (a, b) = (QueueId::Chat(10), QueueId::Chat(20));
        assert_eq!(inner.admit(a, item(1)), Some(true));
        assert_eq!(inner.admit(b, item(2)), Some(true));
        assert_eq!(inner.admit(a, item(3)), Some(false));
        assert_eq!(inner.admit(a, item(4)), Some(false));
        // duplicates (upstream re-sends unconfirmed updates) are ignored
        assert_eq!(inner.admit(a, item(3)), None);
        // chat a is strictly ordered, chat b is not held up behind it
        assert_eq!(inner.next(a).unwrap().id, 1);
        assert_eq!(inner.next(b).unwrap().id, 2);
        assert_eq!(inner.next(a).unwrap().id, 3);
        assert_eq!(inner.next(a).unwrap().id, 4);
        assert!(inner.next(a).is_none());
        // an emptied queue is gone, so the next admit spawns a fresh task
        assert_eq!(inner.admit(a, item(5)), Some(true));
    }

    #[test]
    fn offset_is_a_watermark() {
        let mut w = Window::new(0);
        assert_eq!(w.offset(), 0);
        for id in [5, 6, 7, 8] {
            assert!(w.add(id));
        }
        assert!(!w.add(6));
        // nothing finished: confirm nothing beyond the first unfinished id
        assert_eq!(w.offset(), 5);
        // later updates finishing does not advance past the stuck one (no loss on crash)
        w.finish(6);
        w.finish(8);
        assert_eq!(w.offset(), 5);
        assert!(!w.add(6) && !w.add(8), "finished-but-unconfirmed ids are not redelivered");
        w.finish(5);
        assert_eq!(w.offset(), 7, "5 and 6 are done, 7 is still pending");
        w.finish(7);
        assert_eq!(w.offset(), 9);
        assert_eq!(w.occupancy(), 0);
        // ids below the offset are never admitted again
        assert!(!w.add(8) && !w.add(1));
        assert!(w.add(9));
    }

    #[test]
    fn window_respects_initial_offset_and_bounds() {
        let mut w = Window::new(100);
        assert_eq!(w.offset(), 100);
        assert!(!w.add(99));
        assert!(w.add(100));
        assert_eq!(w.unfinished(), 1);
        let mut inner = Inner::new(0);
        // cap = 2 * max_connections: room shrinks as updates are loaded
        assert_eq!(inner.poll_window(4), Some((0, 4)));
        for id in 1..=4 {
            inner.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(inner.poll_window(4), None, "loaded updates reached 2*max_connections");
        inner.window.finish(1);
        assert_eq!(inner.poll_window(4), Some((2, 4)));
        // limit counts ids upstream re-sends (everything >= offset)
        let mut big = Inner::new(0);
        for id in 1..=99 {
            big.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(big.poll_window(1000), Some((1, 100)));
        big.admit(QueueId::Solo(100), item(100));
        assert_eq!(big.poll_window(1000), None, "one poll page is full");
    }

    #[test]
    fn sweep_is_rate_limited() {
        assert!(!sweep_due(10_000, 10_500));
        assert!(sweep_due(10_000, 11_000));
        assert!(!sweep_due(5_000, 4_000));
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
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":409,"description":"Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first"}))),
            PollOutcome::WebhookActive
        );
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":409,"description":"Conflict: terminated by other getUpdates request"}))),
            PollOutcome::Retry(None)
        );
        assert!(poll_error_text(Some(&json!({"error_code":502,"description":"Bad Gateway"})), None).contains("502 Bad Gateway"));
        assert!(poll_error_text(None, Some("boom")).contains("boom"));
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
        // The offset never goes past the update before the webhook answered 2xx: the polls in
        // between (re-fetching the unconfirmed update) stay at <= 7, and poll:8 follows the 200.
        assert_eq!(seen[0], "poll:0");
        let whs: Vec<usize> = (0..seen.len()).filter(|&i| seen[i].starts_with("wh:")).collect();
        assert_eq!(whs.len(), 2, "500 then 200 for the same update: {seen:?}");
        assert_eq!(seen[whs[0]], seen[whs[1]], "same update twice: {seen:?}");
        let p8 = seen.iter().position(|e| e == "poll:8").unwrap();
        assert!(p8 > whs[1], "offset must advance only after the 200: {seen:?}");
        assert!(seen[..p8].iter().filter_map(|e| e.strip_prefix("poll:")).all(|o| o.parse::<i64>().unwrap() <= 7), "{seen:?}");

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

    type Recorded = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

    /// Upstream mock: records (request head, path, body) and answers via `f(path) -> (status, body)`.
    async fn mock_upstream(rec: Recorded, f: impl Fn(&str) -> (&'static str, String) + Send + Sync + 'static) -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        let f = Arc::new(f);
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                let (rec, f) = (rec.clone(), f.clone());
                tokio::spawn(async move {
                    let Some((head, body)) = read_request(&mut s).await else { return };
                    let path = head.lines().next().unwrap_or("").split(' ').nth(1).unwrap_or("").to_string();
                    rec.lock().unwrap().push((head, path.clone(), body));
                    if path.contains("/getUpdates") {
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    let (st, b) = f(&path);
                    respond(&mut s, st, &b).await;
                });
            }
        });
        port
    }

    fn ok_upstream(path: &str) -> (&'static str, String) {
        if path.contains("/getUpdates") {
            ("200 OK", r#"{"ok":true,"result":[]}"#.to_string())
        } else {
            ("200 OK", r#"{"ok":true,"result":true}"#.to_string())
        }
    }

    #[tokio::test]
    async fn cloud_set_webhook_forwards_original_request_for_every_body_type() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), ok_upstream).await;
        let (engine, dir) = test_engine("wh_cloud", port);

        let multipart = "--B1\r\nContent-Disposition: form-data; name=\"url\"\r\n\r\nhttps://example.com/h\r\n--B1\r\nContent-Disposition: form-data; name=\"certificate\"; filename=\"c.pem\"\r\n\r\nPEMDATA\r\n--B1--\r\n";
        let cases: Vec<(&str, Option<&str>, Vec<u8>)> = vec![
            ("111:a", Some("application/json"), br#"{"url":"https://example.com/h","max_connections":3}"#.to_vec()),
            ("222:b", Some("application/x-www-form-urlencoded"), b"url=https%3A%2F%2Fexample.com%2Fh&max_connections=3".to_vec()),
            ("333:c", Some("multipart/form-data; boundary=B1"), multipart.as_bytes().to_vec()),
        ];
        for (token, ct, body) in &cases {
            let out = engine
                .universal_call(token, "setWebhook", &HashMap::new(), Some("x=1"), *ct, Bytes::from(body.clone()))
                .await
                .unwrap();
            assert!(String::from_utf8_lossy(&out).contains("\"ok\":true"));
        }
        let seen = rec.lock().unwrap().clone();
        assert_eq!(seen.len(), 3, "{seen:?}");
        for ((token, ct, body), (head, path, got)) in cases.iter().zip(seen.iter()) {
            assert_eq!(path, &format!("/bot{token}/setWebhook?x=1"));
            assert!(head.to_ascii_lowercase().contains(&format!("content-type: {}", ct.unwrap().to_ascii_lowercase())), "{head}");
            assert_eq!(got, body, "body must be forwarded byte for byte");
        }
        // the certificate part survives (it is only dropped by our own arg parser)
        assert!(String::from_utf8_lossy(&seen[2].2).contains("PEMDATA"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn local_set_webhook_accepts_form_and_multipart_bodies() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), ok_upstream).await;
        let (engine, dir) = test_engine("wh_local_forms", port);
        let multipart = "--B1\r\nContent-Disposition: form-data; name=\"url\"\r\n\r\nhttp://127.0.0.1:9/h\r\n--B1\r\nContent-Disposition: form-data; name=\"max_connections\"\r\n\r\n4\r\n--B1--\r\n";
        let cases: Vec<(&str, &str, Vec<u8>)> = vec![
            ("444:a", "application/x-www-form-urlencoded", b"url=http%3A%2F%2F127.0.0.1%3A9%2Fh&max_connections=4".to_vec()),
            ("555:b", "multipart/form-data; boundary=B1", multipart.as_bytes().to_vec()),
        ];
        for (token, ct, body) in cases {
            engine
                .universal_call(token, "setWebhook", &HashMap::new(), None, Some(ct), Bytes::from(body))
                .await
                .unwrap();
            let rec_wh = engine.storage.get_webhook(token).await.expect("stored");
            assert_eq!(rec_wh.url, "http://127.0.0.1:9/h");
            assert_eq!(rec_wh.max_connections, 4);
            assert!(engine.webhook_workers.contains_key(token));
            engine.stop_webhook_worker(token);
        }
        assert!(rec.lock().unwrap().iter().any(|(_, p, _)| p.contains("/bot444:a/deleteWebhook")));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unparsable_body_never_touches_upstream_or_local_state() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), ok_upstream).await;
        let (engine, dir) = test_engine("wh_badbody", port);
        engine.storage.save_webhook("666:a", "http://127.0.0.1:9/h", None, 40, None, None).await;
        let r = engine
            .universal_call("666:a", "setWebhook", &HashMap::new(), None, Some("application/json"), Bytes::from_static(b"{oops"))
            .await;
        assert!(matches!(r, Err(TelegramError::BadRequest(_))), "{r:?}");
        assert!(engine.storage.get_webhook("666:a").await.is_some());
        assert!(rec.lock().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn local_set_webhook_fails_when_upstream_delete_fails() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), |_| {
            ("401 Unauthorized", r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#.to_string())
        })
        .await;
        let (engine, dir) = test_engine("wh_delfail", port);
        let r = engine
            .universal_call(
                "777:a",
                "setWebhook",
                &HashMap::new(),
                None,
                Some("application/json"),
                Bytes::from_static(br#"{"url":"http://127.0.0.1:9/h"}"#),
            )
            .await;
        assert!(matches!(r, Err(TelegramError::ApiError { code: 401, .. })), "{r:?}");
        assert!(engine.storage.get_webhook("777:a").await.is_none());
        assert!(!engine.webhook_workers.contains_key("777:a"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn invalid_ip_and_secret_rules() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), ok_upstream).await;
        let (engine, dir) = test_engine("wh_ip", port);
        let call = |token: &'static str, body: &'static [u8]| {
            let engine = engine.clone();
            async move {
                engine
                    .universal_call(token, "setWebhook", &HashMap::new(), None, Some("application/json"), Bytes::from_static(body))
                    .await
            }
        };
        let r = call("881:a", br#"{"url":"https://example.com/h","ip_address":"not-an-ip"}"#).await;
        assert!(matches!(r, Err(TelegramError::BadRequest(_))), "{r:?}");
        // bad secret with a url is rejected, but ignored when the webhook is being deleted
        let r = call("882:a", br#"{"url":"https://example.com/h","secret_token":"bad token"}"#).await;
        assert!(matches!(r, Err(TelegramError::BadRequest(_))), "{r:?}");
        let r = call("883:a", br#"{"url":"","secret_token":"bad token"}"#).await;
        assert!(r.is_ok(), "{r:?}");
        // scheme-less url is accepted (https:// assumed) and forwarded
        let r = call("884:a", br#"{"url":"example.com/h"}"#).await;
        assert!(r.is_ok(), "{r:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Upstream that serves a fixed update list like the real getUpdates: everything with
    /// `update_id >= offset`, at most `limit`, instantly when non-empty, else after a short wait.
    async fn serving_upstream(log: Log, updates: Vec<Value>) -> u16 {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                let (log, updates) = (log.clone(), updates.clone());
                tokio::spawn(async move {
                    let Some((_, body)) = read_request(&mut s).await else { return };
                    let v: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                    let off = v["offset"].as_i64().unwrap_or(0);
                    let limit = v["limit"].as_u64().unwrap_or(100) as usize;
                    log.lock().unwrap().push(format!("poll:{off}"));
                    let res: Vec<&Value> = updates.iter().filter(|u| u["update_id"].as_i64().unwrap() >= off).take(limit).collect();
                    if res.is_empty() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    respond(&mut s, "200 OK", &serde_json::json!({"ok": true, "result": res}).to_string()).await;
                });
            }
        });
        port
    }

    async fn wait_for(log: &Log, what: &str, pred: impl Fn(&[String]) -> bool) -> Vec<String> {
        for _ in 0..200 {
            let seen = log.lock().unwrap().clone();
            if pred(&seen) {
                return seen;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timeout waiting for {what}: {:?}", log.lock().unwrap());
    }

    #[tokio::test]
    async fn poisoned_chat_does_not_block_other_chats() {
        let log: Log = Arc::new(Mutex::new(Vec::new()));
        // Webhook: chat 1 fails twice (500) and then recovers; everything else is 200.
        let wh = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let wh_port = wh.local_addr().unwrap().port();
        let wlog = log.clone();
        tokio::spawn(async move {
            let mut chat1_calls = 0;
            loop {
                let (mut s, _) = wh.accept().await.unwrap();
                let Some((_, body)) = read_request(&mut s).await else { continue };
                let v: Value = serde_json::from_slice(&body).unwrap();
                let id = v["update_id"].as_i64().unwrap();
                let chat = v["message"]["chat"]["id"].as_i64().unwrap();
                if chat == 1 {
                    chat1_calls += 1;
                    if chat1_calls <= 2 {
                        wlog.lock().unwrap().push(format!("wh:{id}:500"));
                        respond(&mut s, "500 Internal Server Error", "no").await;
                        continue;
                    }
                }
                wlog.lock().unwrap().push(format!("wh:{id}:200"));
                respond(&mut s, "200 OK", "").await;
            }
        });
        let msg = |id: i64, chat: i64| serde_json::json!({"update_id": id, "message": {"chat": {"id": chat}, "text": "x"}});
        let up_port = serving_upstream(log.clone(), vec![msg(1, 1), msg(2, 2), msg(3, 2), msg(4, 3), msg(5, 2)]).await;

        let (engine, dir) = test_engine("wh_poison", up_port);
        let token = "123456:POISON".to_string();
        engine.start_webhook_worker(cfg(&token, format!("http://127.0.0.1:{wh_port}/hook")));

        // Chats 2 and 3 are fully delivered while chat 1 is still failing.
        let seen = wait_for(&log, "other chats delivered", |l| {
            ["wh:4:200", "wh:5:200"].iter().all(|w| l.iter().any(|e| e == w))
        })
        .await;
        assert!(!seen.iter().any(|e| e == "wh:1:200"), "chat 1 must still be failing: {seen:?}");
        // ordering inside chat 2: 2, 3, 5
        let order: Vec<&str> = seen
            .iter()
            .filter(|e| e.starts_with("wh:2:") || e.starts_with("wh:3:") || e.starts_with("wh:5:"))
            .map(|e| e.as_str())
            .collect();
        assert_eq!(order, ["wh:2:200", "wh:3:200", "wh:5:200"], "{seen:?}");
        // the watermark holds at the stuck update 1: nothing beyond it was confirmed upstream
        assert!(
            seen.iter().filter_map(|e| e.strip_prefix("poll:")).all(|o| o.parse::<i64>().unwrap() <= 1),
            "offset advanced past an undelivered update: {seen:?}"
        );

        // After the retry (2 s backoff) succeeded, the watermark jumps past everything.
        let seen = wait_for(&log, "offset 6", |l| l.iter().any(|e| e == "poll:6")).await;
        let ok1 = seen.iter().position(|e| e == "wh:1:200").expect("chat 1 recovered");
        let p6 = seen.iter().position(|e| e == "poll:6").unwrap();
        assert!(ok1 < p6);
        // re-sent unconfirmed updates were not delivered twice
        for id in 2..=5 {
            assert_eq!(seen.iter().filter(|e| **e == format!("wh:{id}:200")).count(), 1, "update {id} delivered once: {seen:?}");
        }
        let (pending, _, _) = engine.webhook_states.get(&token).unwrap().snapshot();
        assert_eq!(pending, 0);
        engine.stop_webhook_worker(&token);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Webhook mock answering 200 with `reply` for every request.
    async fn reply_webhook(reply: Vec<u8>) -> u16 {
        let wh = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = wh.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = wh.accept().await.unwrap();
                let reply = reply.clone();
                tokio::spawn(async move {
                    let _ = read_request(&mut s).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                        reply.len()
                    );
                    let _ = s.write_all(head.as_bytes()).await;
                    let _ = s.write_all(&reply).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        port
    }

    /// Delivers one update to a webhook answering with `reply`; returns the upstream paths seen.
    async fn run_reply_case(name: &str, reply: Vec<u8>) -> Vec<String> {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let first = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let up = mock_upstream(rec.clone(), move |path| {
            if path.contains("/getUpdates") && first.swap(false, Ordering::SeqCst) {
                return ("200 OK", r#"{"ok":true,"result":[{"update_id":1,"message":{"chat":{"id":1}}}]}"#.to_string());
            }
            ok_upstream(path)
        })
        .await;
        let wh_port = reply_webhook(reply).await;
        let (engine, dir) = test_engine(name, up);
        let token = "123456:REPLY";
        engine.start_webhook_worker(cfg(token, format!("http://127.0.0.1:{wh_port}/hook")));
        // update 1 delivered => a poll with offset 2 follows
        for _ in 0..100 {
            if rec.lock().unwrap().iter().any(|(_, _, b)| String::from_utf8_lossy(b).contains("\"offset\":2")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        engine.stop_webhook_worker(token);
        let paths = rec.lock().unwrap().iter().map(|(_, p, _)| p.clone()).collect();
        let _ = std::fs::remove_dir_all(&dir);
        paths
    }

    #[tokio::test]
    async fn webhook_reply_method_is_executed_in_the_background() {
        let paths = run_reply_case("wh_reply_ok", br#"{"method":"sendMessage","chat_id":1,"text":"hi"}"#.to_vec()).await;
        assert!(paths.iter().any(|p| p.to_ascii_lowercase().contains("/sendmessage")), "{paths:?}");
        assert!(paths.iter().any(|p| p.contains("/getUpdates")));
    }

    #[tokio::test]
    async fn oversized_webhook_reply_is_ignored() {
        let mut big = br#"{"method":"sendMessage","chat_id":1,"text":""#.to_vec();
        big.extend(std::iter::repeat_n(b'a', MAX_REPLY_BYTES + 4096));
        big.extend_from_slice(br#""}"#);
        let paths = run_reply_case("wh_reply_big", big).await;
        assert!(paths.iter().any(|p| p.contains("/getUpdates")));
        assert!(!paths.iter().any(|p| p.to_ascii_lowercase().contains("/sendmessage")), "oversized reply must not be executed: {paths:?}");
    }

    #[tokio::test]
    async fn poll_errors_are_recorded_and_conflict_reissues_delete_webhook() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), |path| {
            if path.contains("/getUpdates") {
                (
                    "409 Conflict",
                    r#"{"ok":false,"error_code":409,"description":"Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first"}"#.to_string(),
                )
            } else {
                ok_upstream(path)
            }
        })
        .await;
        let (engine, dir) = test_engine("wh_409", port);
        let token = "123456:CONF";
        engine.start_webhook_worker(cfg(token, "http://127.0.0.1:9/hook".into()));
        let count_deletes = || rec.lock().unwrap().iter().filter(|(_, p, _)| p.contains("/deleteWebhook")).count();
        for _ in 0..100 {
            if count_deletes() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(count_deletes(), 1, "deleteWebhook must be re-issued");
        let (_, date, msg) = engine.webhook_states.get(token).unwrap().snapshot();
        assert!(date.is_some());
        assert!(msg.unwrap().contains("409"));
        // rate limited: more polls happen but there is no second repair within the interval
        tokio::time::sleep(Duration::from_millis(2500)).await;
        assert_eq!(count_deletes(), 1);
        engine.stop_webhook_worker(token);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn fatal_poll_error_deletes_stored_record() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), |path| {
            if path.contains("/getUpdates") {
                ("401 Unauthorized", r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#.to_string())
            } else {
                ok_upstream(path)
            }
        })
        .await;
        let (engine, dir) = test_engine("wh_fatal", port);
        let token = "123456:DEAD";
        engine.storage.save_webhook(token, "http://127.0.0.1:9/hook", None, 40, None, None).await;
        engine.start_webhook_worker(cfg(token, "http://127.0.0.1:9/hook".into()));
        for _ in 0..100 {
            if !engine.webhook_workers.contains_key(token) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(engine.storage.get_webhook(token).await.is_none(), "dead bot must not be restored on every boot");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_starts_leave_exactly_one_worker() {
        let rec: Recorded = Arc::new(Mutex::new(Vec::new()));
        let port = mock_upstream(rec.clone(), ok_upstream).await;
        let (engine, dir) = test_engine("wh_race", port);
        let token = "123456:RACE";
        let mut hs = Vec::new();
        for _ in 0..16 {
            let e = engine.clone();
            hs.push(tokio::spawn(async move { e.start_webhook_worker(cfg(token, "http://127.0.0.1:9/hook".into())) }));
        }
        for h in hs {
            h.await.unwrap();
        }
        assert_eq!(engine.webhook_workers.len(), 1);
        assert_eq!(engine.webhook_states.len(), 1);
        tokio::time::sleep(Duration::from_millis(500)).await;
        // Every loser was aborted: a single poll loop makes one request per ~200ms mock delay.
        let before = rec.lock().unwrap().len();
        tokio::time::sleep(Duration::from_millis(1000)).await;
        let polls = rec.lock().unwrap().len() - before;
        assert!(polls <= 8, "orphaned workers keep polling: {polls} polls/s");
        engine.stop_webhook_worker(token);
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
