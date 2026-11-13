//! State shared by the worker loop and its queue tasks.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Instant;

use reqwest::Client;
use tokio::sync::{Notify, Semaphore};

use super::queue::Inner;
use super::state::WebhookState;
use crate::client::engine::TelegramEngine;

pub(super) struct Ctx {
    pub(super) engine: Arc<TelegramEngine>,
    pub(super) token: String,
    pub(super) url: String,
    pub(super) secret: Option<String>,
    pub(super) client: Client,
    pub(super) sem: Arc<Semaphore>,
    pub(super) state: Arc<WebhookState>,
    pub(super) first_410: Mutex<Option<Instant>>,
    pub(super) closed: AtomicBool,
    pub(super) inner: Mutex<Inner>,
    /// Signalled whenever an update finished (the watermark may have advanced).
    pub(super) progress: Notify,
    pub(super) closed_notify: Notify,
}

impl Ctx {
    pub(super) fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn sync_pending(&self, inner: &Inner) {
        self.state.pending.store(inner.window.unfinished() as i64, Ordering::Relaxed);
    }

    pub(super) fn finish(&self, id: i64) {
        {
            let mut g = self.lock();
            g.window.finish(id);
            self.sync_pending(&g);
        }
        self.progress.notify_one();
    }
}

