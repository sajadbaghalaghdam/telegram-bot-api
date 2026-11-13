//! Webhook replies that carry a Bot API method call (C++ WebhookActor.cpp:620-631).
//!
//! The call is executed outside the delivery path (its result is ignored and must never delay
//! the next update), but not unbounded: calls go through a bounded channel to one executor task
//! that runs at most [`MAX_CONCURRENT_CALLS`] of them at a time. The executor lives in the
//! worker's `JoinSet`, so stopping the worker cancels it together with every call in flight.
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::client::engine::TelegramEngine;

/// Replies waiting for a free executor slot; beyond this they are dropped (with a warning).
const QUEUE_CAPACITY: usize = 256;
/// Reply calls running at the same time.
const MAX_CONCURRENT_CALLS: usize = 32;

/// A reply with a `method` other than delete/setWebhook/close/logOut/get*: the lowercased method.
pub(super) fn reply_method(body: &[u8]) -> Option<String> {
    #[derive(Deserialize)]
    struct Reply<'a> {
        #[serde(borrow)]
        method: Option<Cow<'a, str>>,
    }
    // Most replies are empty or "OK": skip the parser unless it can be a JSON object.
    if body.iter().find(|b| !b.is_ascii_whitespace()) != Some(&b'{') {
        return None;
    }
    let m = serde_json::from_slice::<Reply>(body).ok()?.method?.to_ascii_lowercase();
    if m.is_empty() || matches!(m.as_str(), "deletewebhook" | "setwebhook" | "close" | "logout") || m.starts_with("get") {
        return None;
    }
    Some(m)
}

struct Call {
    method: String,
    body: Bytes,
}

/// Handle of a worker's reply executor.
pub(super) struct ReplySink {
    tx: mpsc::Sender<Call>,
    dropped: AtomicU64,
}

impl ReplySink {
    /// Starts the executor inside `tasks` (the worker's task set).
    pub(super) fn start(tasks: &mut JoinSet<()>, engine: Arc<TelegramEngine>, token: Arc<str>) -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        tasks.spawn(run_executor(engine, token, rx));
        Self { tx, dropped: AtomicU64::new(0) }
    }

    /// Queues a call; when the executor is saturated the call is dropped, like an update that
    /// does not fit into a full queue.
    pub(super) fn submit(&self, method: String, body: Bytes) {
        if self.tx.try_send(Call { method, body }).is_err() {
            let n = self.dropped.fetch_add(1, Ordering::Relaxed);
            if n.is_power_of_two() || n == 0 {
                tracing::warn!("webhook reply method calls are not keeping up: {} dropped so far", n + 1);
            }
        }
    }
}

async fn run_executor(engine: Arc<TelegramEngine>, token: Arc<str>, mut rx: mpsc::Receiver<Call>) {
    let mut calls: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            // Reap finished calls first so the slot count below is accurate.
            Some(_) = calls.join_next(), if !calls.is_empty() => {}
            call = rx.recv(), if calls.len() < MAX_CONCURRENT_CALLS => {
                let Some(Call { method, body }) = call else { break };
                let (engine, token) = (Arc::clone(&engine), Arc::clone(&token));
                calls.spawn(async move {
                    let _ = engine.universal_call(&token, &method, &HashMap::new(), None, Some("application/json"), body).await;
                });
            }
        }
    }
    // The worker is gone: dropping `calls` aborts whatever is still running.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_methods() {
        assert_eq!(reply_method(br#"{"method":"sendMessage","chat_id":1,"text":"x"}"#).as_deref(), Some("sendmessage"));
        assert_eq!(reply_method(br#"  {"method":"sendMessage"}"#).as_deref(), Some("sendmessage"));
        assert_eq!(reply_method(br#"{"method":"sendMessage"}"#).as_deref(), Some("sendmessage"));
        assert_eq!(reply_method(br#"{"method":"getMe"}"#), None);
        assert_eq!(reply_method(br#"{"method":"deleteWebhook"}"#), None);
        assert_eq!(reply_method(br#"{"method":"setWebhook"}"#), None);
        assert_eq!(reply_method(br#"{"method":"LogOut"}"#), None);
        assert_eq!(reply_method(br#"{"method":""}"#), None);
        assert_eq!(reply_method(br#"{"method":5}"#), None);
        assert_eq!(reply_method(b"OK"), None);
        assert_eq!(reply_method(b""), None);
        assert_eq!(reply_method(br#"{"ok":true}"#), None);
        assert_eq!(reply_method(br#"["method"]"#), None);
    }
}
