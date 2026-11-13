//! setWebhook throttle: 1 second minimum between calls of one bot.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use crate::client::engine::TelegramEngine;
use crate::error::TelegramError;

/// True when the throttle map may be swept again (at most once per second).
fn sweep_due(last_ms: u64, now_ms: u64) -> bool {
    now_ms.saturating_sub(last_ms) >= 1000
}

static LAST_SWEEP_MS: AtomicU64 = AtomicU64::new(0);

impl TelegramEngine {
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

    #[test]
    fn sweep_is_rate_limited() {
        assert!(!sweep_due(10_000, 10_500));
        assert!(sweep_due(10_000, 11_000));
        assert!(!sweep_due(5_000, 4_000));
    }
}
