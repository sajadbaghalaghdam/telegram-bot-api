//! Pure retry / drop / backoff rules (C++ `WebhookActor` constants and `on_update_error`).
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// C++ WEBHOOK_MAX_RESEND_TIMEOUT
pub const MAX_RESEND_TIMEOUT: u32 = 60;
/// C++ WEBHOOK_DROP_TIMEOUT (23h): continuous 410 closes the webhook; also the lifetime of an update.
pub const DROP_TIMEOUT_SECS: u64 = 60 * 60 * 23;
/// C++ MAX_RETRY_AFTER
pub const MAX_RETRY_AFTER: u32 = 3600;

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

/// Tracks the run of consecutive 410 replies of a webhook without taking a lock (every delivery
/// reports its outcome here): the webhook is closed once 410s have lasted longer than
/// [`DROP_TIMEOUT_SECS`].
pub struct GoneTracker {
    started: Instant,
    /// Milliseconds since `started`, plus one, of the first 410 of the current run; 0 = no run.
    first_ms: AtomicU64,
}

impl GoneTracker {
    pub fn new() -> Self {
        Self { started: Instant::now(), first_ms: AtomicU64::new(0) }
    }

    /// Any non-410 outcome ends the run.
    pub fn delivered(&self) {
        if self.first_ms.load(Ordering::Relaxed) != 0 {
            self.first_ms.store(0, Ordering::Relaxed);
        }
    }

    /// Records a failed attempt; true when the webhook has to be closed.
    pub fn failed(&self, is_410: bool) -> bool {
        if !is_410 {
            self.delivered();
            return false;
        }
        self.failed_at(self.started.elapsed().as_millis() as u64)
    }

    fn failed_at(&self, now_ms: u64) -> bool {
        match self.first_ms.compare_exchange(0, now_ms + 1, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => false,
            Err(first) => close_after_410((now_ms + 1).saturating_sub(first) / 1000),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn gone_tracker_closes_after_the_drop_timeout_only() {
        let t = GoneTracker::new();
        assert!(!t.failed_at(0), "first 410 starts the run");
        assert!(!t.failed_at(60_000));
        assert!(!t.failed_at(DROP_TIMEOUT_SECS * 1000), "exactly the timeout is not enough");
        assert!(t.failed_at(DROP_TIMEOUT_SECS * 1000 + 2000));
        // a successful delivery (or any other error) ends the run
        t.delivered();
        assert!(!t.failed_at(DROP_TIMEOUT_SECS * 1000 + 5000));
        assert!(!t.failed(false));
        assert!(!t.failed_at(DROP_TIMEOUT_SECS * 1000 + 9000));
    }
}
