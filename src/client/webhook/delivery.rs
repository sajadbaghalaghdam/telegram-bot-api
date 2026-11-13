//! HTTP delivery of one update to the webhook URL, with retries.
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use reqwest::header::{HeaderName, HeaderValue, CONTENT_TYPE, RETRY_AFTER};
use reqwest::{Client, Url};

use super::ctx::Ctx;
use super::queue::Item;
use super::reply::reply_method;
use super::retry::{next_retry, should_drop, MAX_RESEND_TIMEOUT};
use super::state::rand_range;
use super::update::QueueId;

/// Hard cap of the webhook reply body we are willing to read (only small JSON method calls matter).
pub(super) const MAX_REPLY_BYTES: usize = 1 << 20;

const SECRET_HEADER: HeaderName = HeaderName::from_static("x-telegram-bot-api-secret-token");

/// Where updates go: parsed once per worker, so a delivery does no URL parsing and clones neither
/// the URL nor the secret text.
pub(super) struct Target {
    client: Client,
    url: Url,
    secret: Option<HeaderValue>,
}

impl Target {
    /// None when `url` is not a valid URL or the secret is not a valid header value.
    pub(super) fn new(client: Client, url: &str, secret: Option<&str>) -> Option<Self> {
        let secret = match secret.filter(|s| !s.is_empty()) {
            Some(s) => {
                let mut v = HeaderValue::from_str(s).ok()?;
                v.set_sensitive(true);
                Some(v)
            }
            None => None,
        };
        Some(Self { client, url: Url::parse(url).ok()?, secret })
    }

    /// POSTs one update; on 2xx returns the (size-capped) reply body.
    async fn post(&self, body: Bytes) -> Result<Bytes, SendError> {
        let mut req = self.client.post(self.url.clone()).header(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(s) = &self.secret {
            req = req.header(SECRET_HEADER, s.clone());
        }
        let resp = req.body(body).send().await.map_err(|e| SendError {
            msg: format!("Connection error: {}", e.without_url()),
            status: None,
            retry_after: 0,
        })?;
        let status = resp.status();
        let retry_after = resp
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u32>().ok())
            .unwrap_or(0);
        if status.is_success() {
            return Ok(read_capped(resp, MAX_REPLY_BYTES).await);
        }
        Err(SendError {
            msg: format!("Wrong response from the webhook: {} {}", status.as_u16(), status.canonical_reason().unwrap_or("")),
            status: Some(status.as_u16()),
            retry_after,
        })
    }
}

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

/// Reads a successful reply, giving up (empty result) as soon as it exceeds `cap` bytes. A reply
/// that arrives in one chunk (the usual tiny one) is returned without copying.
async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Bytes {
    match resp.content_length() {
        Some(0) => return Bytes::new(),
        Some(l) if l > cap as u64 => return Bytes::new(),
        _ => {}
    }
    let first = match resp.chunk().await {
        Ok(Some(c)) if c.len() <= cap => c,
        _ => return Bytes::new(),
    };
    let mut joined: Option<Vec<u8>> = None;
    loop {
        match resp.chunk().await {
            Ok(None) => return joined.map_or(first, Bytes::from),
            Ok(Some(c)) => {
                let buf = joined.get_or_insert_with(|| first.to_vec());
                if buf.len() + c.len() > cap {
                    return Bytes::new();
                }
                buf.extend_from_slice(&c);
            }
            Err(_) => return Bytes::new(),
        }
    }
}

/// Delivers one update, retrying with backoff (the retry timer belongs to the update's queue;
/// other queues are unaffected). The drop timeout counts from the update's own first-seen time.
async fn deliver(ctx: &Ctx, item: &Item) -> Delivery {
    let mut delay = 1u32;
    let mut fails = 0u32;
    loop {
        if ctx.closed.load(Ordering::Relaxed) {
            return Delivery::Closed;
        }
        let out = {
            let _permit = ctx.permits.acquire().await;
            ctx.target.post(item.body.clone()).await
        };
        match out {
            Ok(reply) => {
                ctx.note_delivered();
                if let Some(method) = reply_method(&reply) {
                    ctx.replies.submit(method, reply);
                }
                return Delivery::Delivered;
            }
            Err(e) => {
                ctx.state.record_error(&e.msg);
                if ctx.note_failure(e.status == Some(410)) {
                    return Delivery::Closed;
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
    let mut next = ctx.first(qid);
    while let Some(item) = next {
        match deliver(&ctx, &item).await {
            Delivery::Closed => {
                ctx.closed_notify.notify_one();
                return;
            }
            Delivery::Delivered | Delivery::Dropped => next = ctx.finish_and_next(qid, item.id),
        }
    }
}
