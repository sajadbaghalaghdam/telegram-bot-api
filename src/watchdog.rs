use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;

#[cfg(all(target_os = "linux", target_env = "gnu"))]
unsafe extern "C" {
    fn malloc_trim(pad: usize) -> i32;
}

/// Async Watchdog monitoring health, responsiveness and proactive OS page reclamation.
/// Uses a monotonic clock (`Instant`), so wall-clock jumps (NTP, manual changes) cannot cause false alarms.
pub struct Watchdog {
    start: Instant,
    /// Milliseconds since `start` at the last kick.
    last_kick_ms: AtomicU64,
    timeout_secs: i64,
}

/// Owns the watchdog's background work (heartbeat task + monitor thread, which also trims the heap); dropping it stops both.
pub struct MonitorGuard {
    heartbeat: JoinHandle<()>,
    stop: Arc<(Mutex<bool>, Condvar)>,
}

impl Drop for MonitorGuard {
    fn drop(&mut self) {
        self.heartbeat.abort();
        *self.stop.0.lock().unwrap_or_else(|e| e.into_inner()) = true;
        self.stop.1.notify_all();
    }
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
    /// heartbeat stops and the monitor thread reports it. The thread also returns unused heap pages to the OS
    /// every ~10 s (measured: RSS 18 MB instead of 40 MB after a load burst, no throughput or latency cost,
    /// see BENCHMARKS.md Round 4D). Both end when the returned guard is dropped.
    pub fn spawn_monitor(self: Arc<Self>) -> MonitorGuard {
        let beat = Arc::clone(&self);
        let heartbeat = tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            loop {
                tick.tick().await;
                beat.kick();
            }
        });

        let stop = Arc::new((Mutex::new(false), Condvar::new()));
        let thread_stop = Arc::clone(&stop);
        let spawned = std::thread::Builder::new().name("watchdog".into()).spawn(move || {
            let mut alerted = false;
            let mut ticks = 0u64;
            let (flag, cv) = &*thread_stop;
            let mut stopped = flag.lock().unwrap_or_else(|e| e.into_inner());
            loop {
                // Interruptible 5 s sleep: wakes immediately when the guard is dropped.
                let (g, _) = cv
                    .wait_timeout_while(stopped, Duration::from_secs(5), |s| !*s)
                    .unwrap_or_else(|e| e.into_inner());
                stopped = g;
                if *stopped {
                    return;
                }
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

                // Release unused heap pages back to the Linux kernel every ~10 s.
                ticks += 1;
                #[cfg(all(target_os = "linux", target_env = "gnu"))]
                if ticks.is_multiple_of(2) {
                    // SAFETY: malloc_trim has no preconditions.
                    let _ = unsafe { malloc_trim(0) };
                }
                #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
                let _ = ticks;
            }
        });
        if let Err(e) = spawned {
            tracing::error!("Failed to spawn watchdog thread, stall detection and heap trimming are disabled: {e}");
        }
        MonitorGuard { heartbeat, stop }
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

    #[tokio::test]
    async fn dropping_the_guard_stops_heartbeat_and_thread() {
        let w = Watchdog::new(60);
        let guard = Arc::clone(&w).spawn_monitor();
        let stop = Arc::clone(&guard.stop);
        let hb_finished = guard.heartbeat.abort_handle();
        drop(guard);
        // The monitor thread wakes from its condvar wait and exits, releasing its clone of `stop`
        // and of the watchdog (only the test's references remain).
        let deadline = Instant::now() + Duration::from_secs(2);
        while Arc::strong_count(&stop) > 1 || Arc::strong_count(&w) > 1 {
            assert!(Instant::now() < deadline, "monitor thread did not exit promptly");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::task::yield_now().await;
        assert!(hb_finished.is_finished());
    }
}
