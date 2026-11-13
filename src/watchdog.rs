use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::time::sleep;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

/// Async Watchdog monitoring health, responsiveness, and proactive OS page reclamation.
/// Uses a monotonic clock (`Instant`), so wall-clock jumps (NTP, manual changes) cannot cause false alarms.
pub struct Watchdog {
    start: Instant,
    /// Milliseconds since `start` at the last kick.
    last_kick_ms: AtomicU64,
    timeout_secs: i64,
}

impl Watchdog {
    pub fn new(timeout_secs: i64) -> Arc<Self> {
        Arc::new(Self {
            start: Instant::now(),
            last_kick_ms: AtomicU64::new(0),
            timeout_secs,
        })
    }

    /// Kicks the watchdog to reset the timer
    #[inline]
    pub fn kick(&self) {
        self.last_kick_ms.store(self.start.elapsed().as_millis() as u64, Ordering::Relaxed);
    }

    /// Seconds since the last kick.
    fn idle_secs(&self) -> i64 {
        let now = self.start.elapsed().as_millis() as u64;
        (now.saturating_sub(self.last_kick_ms.load(Ordering::Relaxed)) / 1000) as i64
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

        let spawned = std::thread::Builder::new().name("watchdog".into()).spawn(move || {
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
        if let Err(e) = spawned {
            tracing::error!("Failed to spawn watchdog thread, stall detection and heap trimming are disabled: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn idle_grows_until_kicked() {
        let w = Watchdog::new(60);
        std::thread::sleep(Duration::from_millis(1100));
        assert!(w.idle_secs() >= 1);
        w.kick();
        assert_eq!(w.idle_secs(), 0);
    }
}
