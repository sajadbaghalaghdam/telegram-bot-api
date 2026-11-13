use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::sleep;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

/// Async Watchdog monitoring health, responsiveness, and proactive OS page reclamation
pub struct Watchdog {
    last_kick_time: AtomicI64,
    timeout_secs: i64,
}

impl Watchdog {
    pub fn new(timeout_secs: i64) -> Arc<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        Arc::new(Self {
            last_kick_time: AtomicI64::new(now),
            timeout_secs,
        })
    }

    /// Kicks the watchdog to reset the timer
    #[inline]
    pub fn kick(&self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        self.last_kick_time.store(now, Ordering::Relaxed);
    }

    /// Seconds since the last kick.
    fn idle_secs(&self) -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        now - self.last_kick_time.load(Ordering::Relaxed)
    }

    /// Spawns the liveness heartbeat (a task on the Tokio runtime that kicks every second) and an
    /// independent OS-thread monitor. If the runtime stops scheduling tasks (blocked workers), the
    /// heartbeat stops and the monitor thread reports it. Also trims the heap periodically.
    pub fn spawn_monitor(self: Arc<Self>) {
        let beat = Arc::clone(&self);
        tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(1)).await;
                beat.kick();
            }
        });

        let _ = std::thread::Builder::new().name("watchdog".into()).spawn(move || {
            let mut ticks = 0u64;
            let mut alerted = false;
            loop {
                std::thread::sleep(Duration::from_secs(5));
                let idle = self.idle_secs();
                if idle > self.timeout_secs {
                    // Log once per stall; the next kick re-arms.
                    if !alerted {
                        tracing::error!(
                            "Watchdog timeout expired after {} seconds: event loop is unresponsive",
                            idle
                        );
                        alerted = true;
                    }
                } else {
                    alerted = false;
                }

                // Proactively release unused heap pages back to the Linux kernel every ~10 s
                ticks += 1;
                #[cfg(all(target_os = "linux", target_env = "gnu"))]
                if ticks % 2 == 0 {
                    unsafe {
                        let _ = malloc_trim(0);
                    }
                }
                #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
                let _ = ticks;
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_grows_until_kicked() {
        let w = Watchdog::new(60);
        w.last_kick_time.store(w.last_kick_time.load(Ordering::Relaxed) - 100, Ordering::Relaxed);
        assert!(w.idle_secs() >= 100);
        w.kick();
        assert!(w.idle_secs() <= 1);
    }
}
