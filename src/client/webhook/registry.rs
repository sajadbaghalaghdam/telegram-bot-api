//! The engine's registry of running workers (`webhook_states` + `webhook_workers`) and its
//! start / stop / exit protocol.
use std::sync::{Arc, Mutex, MutexGuard};

use super::state::WebhookState;
use super::worker::{run_worker, WorkerConfig};
use crate::client::engine::TelegramEngine;

/// Serializes registry changes of start/stop/exit. Only held for short synchronous sections,
/// never across an `.await`.
static REGISTRY_LOCK: Mutex<()> = Mutex::new(());

fn registry_lock() -> MutexGuard<'static, ()> {
    REGISTRY_LOCK.lock().unwrap_or_else(|p| p.into_inner())
}

/// Removes a worker's registry entries when it ends for any reason (error, 401, 410, abort).
pub(super) struct ExitGuard {
    pub(super) engine: Arc<TelegramEngine>,
    pub(super) token: Arc<str>,
    pub(super) generation: u64,
}

impl Drop for ExitGuard {
    fn drop(&mut self) {
        // Remove the registry entries only if they still belong to this worker generation.
        let _l = registry_lock();
        let removed = self
            .engine
            .webhook_states
            .remove_if(&*self.token, |_, s| s.generation == self.generation)
            .is_some();
        if removed {
            self.engine.webhook_workers.remove(&*self.token);
        }
    }
}

/// True while this worker is still the registered one for its token.
pub(super) fn is_current(engine: &TelegramEngine, token: &str, generation: u64) -> bool {
    engine.webhook_states.get(token).is_some_and(|s| s.generation == generation)
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

    /// Stops the worker of a bot. Dropping the aborted worker cancels all its queue tasks, reply
    /// calls and in-flight requests.
    pub fn stop_webhook_worker(&self, token: &str) {
        let _l = registry_lock();
        self.stop_worker_locked(token);
    }
}
