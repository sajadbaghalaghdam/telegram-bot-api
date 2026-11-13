//! setWebhook throttle: 1 second minimum between calls of one bot.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use crate::client::engine::TelegramEngine;
use crate::error::TelegramError;

/// Minimum spacing of setWebhook calls of one bot.
const SPACING: Duration = Duration::from_secs(1);
/// Above this many entries expired ones are swept (at most once per second).
const SWEEP_ABOVE: usize = 1024;
/// Hard bound of the map: entries live one second, so this many distinct bots within one second
/// is not legitimate traffic; further new bots are throttled until entries expire.
const MAX_ENTRIES: usize = 65_536;

/// True when the throttle map may be swept again (at most once per second).
fn sweep_due(last_ms: u64, now_ms: u64) -> bool {
    now_ms.saturating_sub(last_ms) >= 1000
}

/// Process-wide: there is one engine, and a sweep is cheap enough to share the rate limit.
static LAST_SWEEP_MS: AtomicU64 = AtomicU64::new(0);

fn sweep(times: &DashMap<String, Instant>, now: Instant) {
    times.retain(|_, until| *until > now);
}

/// Throttle decision for `token`: records the call when allowed.
fn check(times: &DashMap<String, Instant>, token: &str, now: Instant) -> Result<(), TelegramError> {
    if times.len() >= SWEEP_ABOVE {
        // Sweep, but at most once per second so a flood of tokens cannot make every call scan
        // the whole map.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let last = LAST_SWEEP_MS.load(Ordering::Relaxed);
        if sweep_due(last, now_ms) && LAST_SWEEP_MS.compare_exchange(last, now_ms, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            sweep(times, now);
        }
        if times.len() >= MAX_ENTRIES && !times.contains_key(token) {
            return Err(TelegramError::flood_wait(1));
        }
    }
    match times.entry(token.to_string()) {
        Entry::Occupied(mut e) => {
            if now < *e.get() {
                return Err(TelegramError::flood_wait(1));
            }
            e.insert(now + SPACING);
        }
        Entry::Vacant(v) => {
            v.insert(now + SPACING);
        }
    }
    Ok(())
}

impl TelegramEngine {
    /// 1 second minimum between setWebhook calls of one bot (C++ `next_allowed_set_webhook_time_`).
    pub fn check_set_webhook_throttle(&self, token: &str) -> Result<(), TelegramError> {
        check(&self.set_webhook_times, token, Instant::now())
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

    #[test]
    fn sweep_removes_only_expired_entries() {
        let times = DashMap::new();
        let now = Instant::now();
        times.insert("old".to_string(), now);
        times.insert("fresh".to_string(), now + Duration::from_secs(1));
        sweep(&times, now + Duration::from_millis(10));
        assert!(!times.contains_key("old"));
        assert!(times.contains_key("fresh"));
    }

    #[test]
    fn map_is_hard_bounded() {
        let times = DashMap::new();
        let now = Instant::now();
        // entries that cannot expire during the test
        for i in 0..MAX_ENTRIES {
            times.insert(format!("{i}:x"), now + Duration::from_secs(3600));
        }
        assert!(check(&times, "new:bot", now).is_err(), "a new bot must be refused while the map is full");
        assert_eq!(times.len(), MAX_ENTRIES);
        // known bots are still judged by their own timestamp
        assert!(check(&times, "5:x", now).is_err());
        times.insert("6:x".to_string(), now - Duration::from_secs(1));
        assert!(check(&times, "6:x", now).is_ok());
    }
}
