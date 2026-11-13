//! HTTP delivery of one update to the webhook URL, with retries.
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::ctx::Ctx;
use super::queue::{Item, QueueId};
use super::reply::reply_method;
use super::retry::{close_after_410, next_retry, should_drop, MAX_RESEND_TIMEOUT};
use super::state::rand_range;
/// Hard cap of the webhook reply body we are willing to read (only small JSON method calls matter).
pub(super) const MAX_REPLY_BYTES: usize = 1 << 20;
struct SendError {
    msg: String,
    status: Option<u16>,
    retry_after: u32,
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
pub(super) async fn run_queue(ctx: Arc<Ctx>, qid: QueueId) {
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

