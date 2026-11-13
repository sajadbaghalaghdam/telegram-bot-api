//! Runtime state of one webhook worker, shared with getWebhookInfo.
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Mutex;

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
    pub(super) fn new(max_connections: i32) -> Self {
        Self {
            generation: GENERATION.fetch_add(1, Ordering::Relaxed),
            max_connections,
            pending: AtomicI64::new(0),
            last_error_date: AtomicI64::new(0),
            last_error_message: Mutex::new(None),
        }
    }

    pub(super) fn record_error(&self, msg: &str) {
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

pub(super) fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

pub(super) fn rand_range(lo: u32, hi: u32) -> u32 {
    use rand::Rng;
    rand::thread_rng().gen_range(lo..=hi)
}

pub(super) fn rand_unit() -> f64 {
    use rand::Rng;
    rand::thread_rng().gen_range(0.0..1.0)
}
