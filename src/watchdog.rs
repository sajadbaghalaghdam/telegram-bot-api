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

    /// Spawns the watchdog monitoring & memory trim loop
    pub fn spawn_monitor(self: Arc<Self>) {
        tokio::spawn(async move {
            let check_interval = Duration::from_secs(10);
            loop {
                sleep(check_interval).await;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs() as i64;
                let last = self.last_kick_time.load(Ordering::Relaxed);

                if now - last > self.timeout_secs {
                    tracing::warn!(
                        "Watchdog alert: Event loop has been unresponsive for {} seconds (timeout: {}s)",
                        now - last,
                        self.timeout_secs
                    );
                }
                self.last_kick_time.store(now, Ordering::Relaxed);

                // Proactively release unused heap pages back to the Linux kernel (MADV_DONTNEED)
                #[cfg(all(target_os = "linux", target_env = "gnu"))]
                unsafe {
                    let _ = malloc_trim(0);
                }
            }
        });
    }
}
