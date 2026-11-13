//! The per-bot worker: polls upstream, feeds the per-chat queues and reacts to poll errors.
//! Everything it spawns (queue tasks, the reply executor) lives in its `JoinSet`, so ending or
//! aborting the worker cancels all of it.
use std::net::{IpAddr, SocketAddr};
use std::ops::Range;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use reqwest::Client;
use serde::Deserialize;
use tokio::task::JoinSet;

use super::ctx::Ctx;
use super::delivery::{run_queue, Target};
use super::poll::{classify_poll, poll_error_text, PollOutcome, PollReply, Poller};
use super::queue::Item;
use super::registry::{is_current, ExitGuard};
use super::reply::ReplySink;
use super::retry::poll_backoff;
use super::state::{rand_unit, WebhookState};
use super::update::{queue_id, update_id};
use crate::client::engine::TelegramEngine;

/// Poll interval while updates are loaded but unfinished (upstream answers instantly then).
const BUSY_POLL_INTERVAL: Duration = Duration::from_secs(1);
/// Minimum spacing of deleteWebhook repairs after a "webhook is active" conflict.
const CONFLICT_FIX_INTERVAL: Duration = Duration::from_secs(30);
/// Server-side long-poll timeout (seconds) while nothing is outstanding.
const LONG_POLL_SECS: u32 = 20;

pub struct WorkerConfig {
    pub token: String,
    pub url: String,
    pub secret_token: Option<String>,
    pub allowed_updates: Option<Vec<String>>,
    pub drop_pending_updates: bool,
    pub max_connections: i32,
    pub ip_address: Option<String>,
}

/// The HTTP client used for delivery: the engine's shared one (connection pool, proxy), or a
/// dedicated one when the webhook has a fixed `ip_address` (not possible through a proxy).
fn delivery_client(engine: &TelegramEngine, url: &str, ip_address: Option<&str>) -> Client {
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

/// drop_pending_updates: the offset just past the newest update upstream holds.
async fn offset_after_pending(engine: &TelegramEngine, token: &str) -> Option<i64> {
    #[derive(Deserialize)]
    struct Reply {
        result: Vec<Last>,
    }
    #[derive(Deserialize)]
    struct Last {
        update_id: i64,
    }
    let url = format!("{}/bot{token}/getUpdates?offset=-1&limit=1&timeout=0", engine.upstream_base_url);
    let body = engine.client.get(&url).send().await.ok()?.bytes().await.ok()?;
    let reply: Reply = serde_json::from_slice(&body).ok()?;
    reply.result.last().map(|u| u.update_id + 1)
}

/// Waits until an update finished, the webhook got closed, or `d` elapsed.
async fn wait_progress(ctx: &Ctx, d: Duration) {
    tokio::select! {
        _ = ctx.progress.notified() => {}
        _ = ctx.closed_notify.notified() => {}
        _ = tokio::time::sleep(d) => {}
    }
}

/// Loads the updates of one poll page that are not known yet and spawns a task for every queue
/// that became non-empty. Each update is the exact byte slice of the reply (no copy); returns how
/// many were new.
fn admit_page(ctx: &Arc<Ctx>, tasks: &mut JoinSet<()>, page: &Bytes, ranges: &[Range<usize>]) -> usize {
    let now = Instant::now();
    let mut to_spawn = Vec::new();
    let mut added = 0usize;
    {
        let mut sched = ctx.lock();
        for r in ranges {
            let raw = &page[r.clone()];
            // Upstream re-sends everything >= offset: skip known ids before looking at the rest.
            let Some(id) = update_id(raw) else { continue };
            if sched.is_known(id) {
                continue;
            }
            let qid = queue_id(raw, id);
            if let Some(spawn) = sched.admit(qid, Item { id, body: page.slice(r.clone()), first_seen: now }) {
                added += 1;
                if spawn {
                    to_spawn.push(qid);
                }
            }
        }
        ctx.sync_pending(&sched);
    }
    for qid in to_spawn {
        tasks.spawn(run_queue(Arc::clone(ctx), qid));
    }
    added
}

pub(super) async fn run_worker(engine: Arc<TelegramEngine>, cfg: WorkerConfig, state: Arc<WebhookState>) {
    let WorkerConfig { token, url, secret_token, allowed_updates, drop_pending_updates, max_connections, ip_address } = cfg;
    let token: Arc<str> = Arc::from(token);
    let _guard = ExitGuard { engine: Arc::clone(&engine), token: Arc::clone(&token), generation: state.generation };

    let client = delivery_client(&engine, &url, ip_address.as_deref());
    let Some(target) = Target::new(client, &url, secret_token.as_deref()) else {
        tracing::warn!("webhook worker not started: unusable webhook URL or secret token");
        state.record_error("unusable webhook URL or secret token");
        return;
    };
    let base_offset = if drop_pending_updates { offset_after_pending(&engine, &token).await.unwrap_or(0) } else { 0 };

    let max_connections = max_connections.max(1) as usize;
    // Queue tasks and the reply executor live in this set: they are aborted together with the worker.
    let mut tasks: JoinSet<()> = JoinSet::new();
    let replies = ReplySink::start(&mut tasks, Arc::clone(&engine), Arc::clone(&token));
    let ctx = Arc::new(Ctx::new(target, replies, Arc::clone(&state), max_connections, base_offset));
    // C++ bounds the loaded updates to 2 * max_connections.
    let cap = max_connections.saturating_mul(2);
    let poller = Poller::new(engine.client.clone(), &engine.upstream_base_url, &token, allowed_updates.as_deref());

    let mut fails: u32 = 0;
    let mut last_conflict_fix: Option<Instant> = None;
    loop {
        while let Some(r) = tasks.try_join_next() {
            if let Err(e) = r
                && e.is_panic()
            {
                tracing::error!("webhook task panicked: {e}");
            }
        }
        if ctx.closed.load(Ordering::Relaxed) {
            tracing::warn!("Closing webhook of a bot because of HTTP 410 errors");
            if is_current(&engine, &token, state.generation) {
                engine.storage.delete_webhook(&token).await;
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
        let timeout = if busy { 0 } else { LONG_POLL_SECS };
        let res = tokio::select! {
            r = poller.fetch(offset, limit, timeout) => r,
            _ = ctx.closed_notify.notified() => continue,
        };
        let (body, transport_err) = match res {
            Ok(b) => (b, None),
            Err(e) => (Bytes::new(), Some(e)),
        };
        let reply = PollReply::parse(&body);

        match classify_poll(reply.as_ref(), &body) {
            PollOutcome::Updates(ranges) => {
                fails = 0;
                let added = admit_page(&ctx, &mut tasks, &body, &ranges);
                if added == 0 && busy {
                    wait_progress(&ctx, BUSY_POLL_INTERVAL).await;
                }
            }
            PollOutcome::Fatal => {
                let msg = poll_error_text(reply.as_ref(), None);
                tracing::warn!("Stopping webhook worker: {msg}");
                state.record_error(&msg);
                // Do not restart a dead bot's webhook on every boot.
                if is_current(&engine, &token, state.generation) {
                    engine.storage.delete_webhook(&token).await;
                }
                break;
            }
            outcome => {
                let msg = poll_error_text(reply.as_ref(), transport_err.as_deref());
                tracing::warn!("webhook poll error: {msg}");
                state.record_error(&msg);
                fails = fails.saturating_add(1);
                let d = match outcome {
                    PollOutcome::Retry(Some(s)) => Duration::from_secs_f64(s as f64 + rand_unit()),
                    PollOutcome::WebhookActive => {
                        // A cloud webhook reappeared upstream (e.g. set by another client).
                        if last_conflict_fix.is_none_or(|t| t.elapsed() >= CONFLICT_FIX_INTERVAL) {
                            last_conflict_fix = Some(Instant::now());
                            match engine.delete_upstream_webhook(&token, false).await {
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
