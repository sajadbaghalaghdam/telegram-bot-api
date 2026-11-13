use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

/// Cache-line padded atomic counter (64 bytes aligned) to prevent CPU False Sharing
#[repr(align(64))]
pub struct CacheAlignedAtomicU64(AtomicU64);

impl CacheAlignedAtomicU64 {
    pub fn new(val: u64) -> Self {
        Self(AtomicU64::new(val))
    }

    #[inline(always)]
    pub fn fetch_add(&self, val: u64, order: Ordering) -> u64 {
        self.0.fetch_add(val, order)
    }

    #[inline(always)]
    pub fn load(&self, order: Ordering) -> u64 {
        self.0.load(order)
    }
}

#[repr(align(64))]
pub struct CacheAlignedAtomicUsize(AtomicUsize);

impl CacheAlignedAtomicUsize {
    pub fn new(val: usize) -> Self {
        Self(AtomicUsize::new(val))
    }

    #[inline(always)]
    pub fn store(&self, val: usize, order: Ordering) {
        self.0.store(val, order);
    }

    #[inline(always)]
    pub fn load(&self, order: Ordering) -> usize {
        self.0.load(order)
    }
}

/// High-performance lock-free server statistics matching official C++ Stats.h/Stats.cpp
/// Features cache-line padding to eliminate False Sharing under high concurrency.
pub struct ServerStats {
    pub start_time: Instant,
    pub total_requests: CacheAlignedAtomicU64,
    pub ok_requests: CacheAlignedAtomicU64,
    pub error_requests: CacheAlignedAtomicU64,
    pub bytes_received: CacheAlignedAtomicU64,
    pub bytes_sent: CacheAlignedAtomicU64,
    pub active_bots: CacheAlignedAtomicUsize,
}

impl Default for ServerStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ServerStats {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            total_requests: CacheAlignedAtomicU64::new(0),
            ok_requests: CacheAlignedAtomicU64::new(0),
            error_requests: CacheAlignedAtomicU64::new(0),
            bytes_received: CacheAlignedAtomicU64::new(0),
            bytes_sent: CacheAlignedAtomicU64::new(0),
            active_bots: CacheAlignedAtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn record_request(&self, bytes_in: u64) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes_in, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_response(&self, is_ok: bool, bytes_out: u64) {
        if is_ok {
            self.ok_requests.fetch_add(1, Ordering::Relaxed);
        } else {
            self.error_requests.fetch_add(1, Ordering::Relaxed);
        }
        self.bytes_sent.fetch_add(bytes_out, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn set_active_bots(&self, count: usize) {
        self.active_bots.store(count, Ordering::Relaxed);
    }

    pub fn uptime_seconds(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    /// Generates the report in the C++ `/stats` style: tab-separated `key\tvalue` rows.
    /// Per-bot sections and time-windowed (5sec/1min/1hour) values are not tracked by this port.
    pub fn to_report(&self) -> String {
        let uptime = self.uptime_seconds() as u64;
        let total = self.total_requests.load(Ordering::Relaxed);
        let ok = self.ok_requests.load(Ordering::Relaxed);
        let err = self.error_requests.load(Ordering::Relaxed);
        let bin = self.bytes_received.load(Ordering::Relaxed);
        let bout = self.bytes_sent.load(Ordering::Relaxed);
        let bots = self.active_bots.load(Ordering::Relaxed);

        format!(
            "uptime\t{uptime}\n\
            bot_count\t{bots}\n\
            active_bot_count\t{bots}\n\
            total_requests\t{total}\n\
            successful_requests\t{ok}\n\
            error_requests\t{err}\n\
            bytes_received\t{bin}\n\
            bytes_sent\t{bout}\n"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_is_tab_separated() {
        let s = ServerStats::new();
        s.record_request(10);
        s.record_response(true, 20);
        s.set_active_bots(3);
        let r = s.to_report();
        assert!(r.contains("\nbot_count\t3\n"));
        assert!(r.contains("total_requests\t1\n"));
        assert!(r.contains("bytes_sent\t20\n"));
        assert!(r.lines().all(|l| l.split('\t').count() == 2));
    }
}
