use std::sync::atomic::{AtomicU64, Ordering};
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

/// High-performance lock-free server statistics matching official C++ Stats.h/Stats.cpp
/// Features cache-line padding to eliminate False Sharing under high concurrency.
pub struct ServerStats {
    pub start_time: Instant,
    pub total_requests: CacheAlignedAtomicU64,
    pub ok_requests: CacheAlignedAtomicU64,
    pub error_requests: CacheAlignedAtomicU64,
    pub bytes_received: CacheAlignedAtomicU64,
    pub bytes_sent: CacheAlignedAtomicU64,
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

    pub fn uptime_seconds(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    /// Generates the report in the layout of C++ `ClientManager::get_stats` (ClientManager.cpp:232-236):
    /// the `DURATION\tinf\t5sec\t1min\t1hour` header, then tab-separated `key\tvalue` rows, `uptime` as a float.
    /// Only lifetime values exist in this port, so rows carry a single value. Missing vs C++: CPU rows,
    /// buffer/webhook/network-query counters, per-bot blocks and the 5sec/1min/1hour windows.
    pub fn to_report(&self, bot_count: usize, active_bot_count: usize) -> String {
        let uptime = self.uptime_seconds();
        let total = self.total_requests.load(Ordering::Relaxed);
        let ok = self.ok_requests.load(Ordering::Relaxed);
        let err = self.error_requests.load(Ordering::Relaxed);
        let bin = self.bytes_received.load(Ordering::Relaxed);
        let bout = self.bytes_sent.load(Ordering::Relaxed);

        let mut out = format!(
            "DURATION\tinf\t5sec\t1min\t1hour\n\
            uptime\t{uptime}\n\
            bot_count\t{bot_count}\n\
            active_bot_count\t{active_bot_count}\n"
        );
        out.push_str(&mem_lines());
        out.push_str(&format!(
            "total_requests\t{total}\n\
            successful_requests\t{ok}\n\
            error_requests\t{err}\n\
            bytes_received\t{bin}\n\
            bytes_sent\t{bout}\n"
        ));
        out
    }
}

/// `rss`/`vm`/`rss_peak`/`vm_peak` rows from /proc/self/status (Linux only; empty elsewhere).
fn mem_lines() -> String {
    std::fs::read_to_string("/proc/self/status")
        .map(|s| parse_mem_lines(&s))
        .unwrap_or_default()
}

fn parse_mem_lines(status: &str) -> String {
    let mut out = String::new();
    for (field, key) in [("VmRSS", "rss"), ("VmSize", "vm"), ("VmHWM", "rss_peak"), ("VmPeak", "vm_peak")] {
        let prefix = format!("{field}:");
        if let Some(line) = status.lines().find(|l| l.starts_with(&prefix)) {
            let kb: f64 = line[prefix.len()..].trim().trim_end_matches("kB").trim().parse().unwrap_or(0.0);
            out.push_str(&format!("{key}\t{:.1}MiB\n", kb / 1024.0));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn report_has_cpp_header_and_keys() {
        let s = ServerStats::new();
        s.record_request(10);
        s.record_response(true, 20);
        let r = s.to_report(3, 2);
        let mut lines = r.lines();
        assert_eq!(lines.next().unwrap(), "DURATION\tinf\t5sec\t1min\t1hour");
        let up = lines.next().unwrap();
        assert!(up.starts_with("uptime\t") && up[7..].parse::<f64>().is_ok());
        assert_eq!(lines.next().unwrap(), "bot_count\t3");
        assert_eq!(lines.next().unwrap(), "active_bot_count\t2");
        assert!(r.contains("total_requests\t1\n"));
        assert!(r.contains("bytes_sent\t20\n"));
        assert!(r.lines().skip(1).all(|l| l.split('\t').count() == 2));
    }

    #[test]
    fn mem_parsing() {
        let s = "VmPeak:\t  2048 kB\nVmSize:\t 1024 kB\nVmHWM:\t 512 kB\nVmRSS:\t 256 kB\n";
        assert_eq!(parse_mem_lines(s), "rss\t0.2MiB\nvm\t1.0MiB\nrss_peak\t0.5MiB\nvm_peak\t2.0MiB\n");
    }
}
