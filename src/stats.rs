use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
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

/// Server-wide counters, using the key names of C++ `ServerBotStat::as_vector` (Stats.cpp).
const KEYS: [&str; 6] = [
    "request_count",
    "request_bytes",
    "response_count",
    "response_count_ok",
    "response_count_error",
    "response_bytes",
];
const REQUEST_COUNT: usize = 0;
const REQUEST_BYTES: usize = 1;
const RESPONSE_COUNT: usize = 2;
const RESPONSE_OK: usize = 3;
const RESPONSE_ERROR: usize = 4;
const RESPONSE_BYTES: usize = 5;

/// Window columns after `inf`, as in C++ (`DESCR` / `DURATIONS` in Stats.h).
const WINDOWS: [(&str, u64); 3] = [("5sec", 5), ("1min", 60), ("1hour", 3600)];
const RING_SECS: usize = 3600;

#[derive(Clone, Copy, Default)]
struct Slot {
    sec: u64,
    v: [u64; 6],
}

/// Ring of per-second buckets covering the last hour; resolution is one second.
struct Ring(Vec<Slot>);

impl Ring {
    fn new() -> Self {
        Self(vec![Slot::default(); RING_SECS])
    }

    fn add(&mut self, now_sec: u64, idx: usize, amount: u64) {
        let slot = &mut self.0[(now_sec % RING_SECS as u64) as usize];
        if slot.sec != now_sec {
            *slot = Slot { sec: now_sec, v: [0; 6] };
        }
        slot.v[idx] += amount;
    }

    /// Sum of events in the last `window` seconds (the current, partial second included).
    fn sum(&self, now_sec: u64, window: u64) -> [u64; 6] {
        let mut out = [0u64; 6];
        for slot in &self.0 {
            if slot.sec <= now_sec && slot.sec + window > now_sec {
                for (o, v) in out.iter_mut().zip(slot.v) {
                    *o += v;
                }
            }
        }
        out
    }
}

#[derive(Clone, Copy)]
struct CpuSample {
    /// Seconds since server start.
    t: f64,
    user_ticks: u64,
    system_ticks: u64,
    total_ticks: u64,
}

struct CpuHistory {
    first: Option<CpuSample>,
    recent: VecDeque<CpuSample>,
}

/// High-performance server statistics matching official C++ Stats.h/Stats.cpp: lifetime counters are
/// lock-free atomics (cache-line padded against false sharing); the 5sec/1min/1hour windows come from a
/// per-second ring behind a short mutex.
pub struct ServerStats {
    pub start_time: Instant,
    request_count: CacheAlignedAtomicU64,
    request_bytes: CacheAlignedAtomicU64,
    response_count: CacheAlignedAtomicU64,
    response_count_ok: CacheAlignedAtomicU64,
    response_count_error: CacheAlignedAtomicU64,
    response_bytes: CacheAlignedAtomicU64,
    ring: Mutex<Ring>,
    cpu: Mutex<CpuHistory>,
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
            request_count: CacheAlignedAtomicU64::new(0),
            request_bytes: CacheAlignedAtomicU64::new(0),
            response_count: CacheAlignedAtomicU64::new(0),
            response_count_ok: CacheAlignedAtomicU64::new(0),
            response_count_error: CacheAlignedAtomicU64::new(0),
            response_bytes: CacheAlignedAtomicU64::new(0),
            ring: Mutex::new(Ring::new()),
            cpu: Mutex::new(CpuHistory { first: None, recent: VecDeque::new() }),
        }
    }

    fn now_sec(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }

    pub fn record_request(&self, bytes_in: u64) {
        self.record_request_at(self.now_sec(), bytes_in);
    }

    pub fn record_response(&self, is_ok: bool, bytes_out: u64) {
        self.record_response_at(self.now_sec(), is_ok, bytes_out);
    }

    fn record_request_at(&self, now_sec: u64, bytes_in: u64) {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        self.request_bytes.fetch_add(bytes_in, Ordering::Relaxed);
        let mut r = self.ring.lock().unwrap_or_else(|e| e.into_inner());
        r.add(now_sec, REQUEST_COUNT, 1);
        r.add(now_sec, REQUEST_BYTES, bytes_in);
    }

    fn record_response_at(&self, now_sec: u64, is_ok: bool, bytes_out: u64) {
        self.response_count.fetch_add(1, Ordering::Relaxed);
        self.response_bytes.fetch_add(bytes_out, Ordering::Relaxed);
        let ok_idx = if is_ok { RESPONSE_OK } else { RESPONSE_ERROR };
        if is_ok {
            self.response_count_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            self.response_count_error.fetch_add(1, Ordering::Relaxed);
        }
        let mut r = self.ring.lock().unwrap_or_else(|e| e.into_inner());
        r.add(now_sec, RESPONSE_COUNT, 1);
        r.add(now_sec, ok_idx, 1);
        r.add(now_sec, RESPONSE_BYTES, bytes_out);
    }

    pub fn uptime_seconds(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    /// Takes one CPU sample from /proc (Linux only; a no-op elsewhere). Meant to be called about once a
    /// second, like C++ `ServerCpuStat::update`.
    pub fn sample_cpu(&self) {
        let Some((user_ticks, system_ticks, total_ticks)) = read_cpu_ticks() else { return };
        let s = CpuSample { t: self.uptime_seconds(), user_ticks, system_ticks, total_ticks };
        let mut h = self.cpu.lock().unwrap_or_else(|e| e.into_inner());
        if h.first.is_none() {
            h.first = Some(s);
        }
        h.recent.push_back(s);
        while h.recent.front().is_some_and(|f| s.t - f.t > 3600.0 + 5.0) {
            h.recent.pop_front();
        }
    }

    /// Generates the report in the layout of C++ `ClientManager::get_stats` (ClientManager.cpp:232-258):
    /// the `DURATION\tinf\t5sec\t1min\t1hour` header, then tab-separated rows. `uptime`, `bot_count`,
    /// `active_bot_count` and memory rows carry one value; CPU and counter rows carry four values
    /// (inf, 5sec, 1min, 1hour) with counters as per-second rates, like C++ `normalize`.
    /// Missing vs C++: buffer_memory, active_* rows, request_file_*/request_max_bytes/update_count
    /// counters and per-bot blocks.
    pub fn to_report(&self, bot_count: usize, active_bot_count: usize) -> String {
        let uptime = self.uptime_seconds();
        let mut out = format!(
            "DURATION\tinf\t5sec\t1min\t1hour\n\
            uptime\t{uptime}\n\
            bot_count\t{bot_count}\n\
            active_bot_count\t{active_bot_count}\n"
        );
        out.push_str(&mem_lines());
        out.push_str(&self.cpu_rows(uptime));
        out.push_str(&self.counter_rows(self.now_sec(), uptime));
        out
    }

    /// Counter rows: `key\tinf\t5sec\t1min\t1hour`, each a per-second rate over `min(window, uptime)`.
    fn counter_rows(&self, now_sec: u64, uptime: f64) -> String {
        let totals = [
            self.request_count.load(Ordering::Relaxed),
            self.request_bytes.load(Ordering::Relaxed),
            self.response_count.load(Ordering::Relaxed),
            self.response_count_ok.load(Ordering::Relaxed),
            self.response_count_error.load(Ordering::Relaxed),
            self.response_bytes.load(Ordering::Relaxed),
        ];
        let windows: Vec<([u64; 6], f64)> = {
            let ring = self.ring.lock().unwrap_or_else(|e| e.into_inner());
            WINDOWS.iter().map(|&(_, w)| (ring.sum(now_sec, w), (w as f64).min(uptime))).collect()
        };
        let rate = |count: u64, secs: f64| if secs > 0.0 { count as f64 / secs } else { 0.0 };
        let mut out = String::new();
        for (i, key) in KEYS.iter().enumerate() {
            out.push_str(&format!("{key}\t{}", rate(totals[i], uptime)));
            for (sums, secs) in &windows {
                out.push_str(&format!("\t{}", rate(sums[i], *secs)));
            }
            out.push('\n');
        }
        out
    }

    /// `total_cpu` / `user_cpu` / `system_cpu` rows with the four duration columns (C++ `CpuStat::as_vector`).
    fn cpu_rows(&self, now: f64) -> String {
        let h = self.cpu.lock().unwrap_or_else(|e| e.into_inner());
        let Some(last) = h.recent.back().copied() else { return String::new() };
        let mut cols: Vec<[String; 3]> = Vec::with_capacity(4);
        cols.push(cpu_percentages(h.first, Some(last)));
        for &(_, w) in &WINDOWS {
            let first = h.recent.iter().find(|s| now - s.t <= w as f64).copied();
            cols.push(cpu_percentages(first, Some(last)));
        }
        ["total_cpu", "user_cpu", "system_cpu"]
            .iter()
            .enumerate()
            .map(|(row, key)| format!("{key}\t{}\n", cols.iter().map(|c| c[row].as_str()).collect::<Vec<_>>().join("\t")))
            .collect()
    }
}

/// Mirrors C++ `CpuStat::as_vector`: "UNKNOWN" unless two samples with advancing total ticks exist;
/// otherwise percent of one core (100% * hardware threads * ticks / total ticks).
fn cpu_percentages(first: Option<CpuSample>, last: Option<CpuSample>) -> [String; 3] {
    let unknown = || ["UNKNOWN".to_string(), "UNKNOWN".to_string(), "UNKNOWN".to_string()];
    let (Some(f), Some(l)) = (first, last) else { return unknown() };
    if f.total_ticks >= l.total_ticks {
        return unknown();
    }
    let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
    let total = (l.total_ticks - f.total_ticks) as f64;
    let user = l.user_ticks.saturating_sub(f.user_ticks) as f64;
    let system = l.system_ticks.saturating_sub(f.system_ticks) as f64;
    let pct = |t: f64| format!("{}%", t / total * 100.0 * cores);
    [pct(user + system), pct(user), pct(system)]
}

/// `(utime, stime)` in clock ticks from the contents of /proc/self/stat (fields 14 and 15; the command
/// name in parentheses may contain spaces, so parsing starts after the last `)`).
fn parse_proc_self_stat(s: &str) -> Option<(u64, u64)> {
    let rest = &s[s.rfind(')')? + 1..];
    let mut f = rest.split_whitespace();
    // After the ')' the first field is `state` (field 3); utime/stime are fields 14/15.
    let utime = f.nth(11)?.parse().ok()?;
    let stime = f.next()?.parse().ok()?;
    Some((utime, stime))
}

/// Sum of all CPU time columns of the aggregate `cpu` line of /proc/stat (user..steal; guest time is
/// already counted in user).
fn parse_proc_stat_total(s: &str) -> Option<u64> {
    let line = s.lines().find(|l| l.starts_with("cpu "))?;
    Some(line.split_whitespace().skip(1).take(8).filter_map(|v| v.parse::<u64>().ok()).sum())
}

fn read_cpu_ticks() -> Option<(u64, u64, u64)> {
    let (u, s) = parse_proc_self_stat(&std::fs::read_to_string("/proc/self/stat").ok()?)?;
    let total = parse_proc_stat_total(&std::fs::read_to_string("/proc/stat").ok()?)?;
    Some((u, s, total))
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
        s.sample_cpu();
        let r = s.to_report(3, 2);
        let mut lines = r.lines();
        assert_eq!(lines.next().unwrap(), "DURATION\tinf\t5sec\t1min\t1hour");
        let up = lines.next().unwrap();
        assert!(up.starts_with("uptime\t") && up[7..].parse::<f64>().is_ok());
        assert_eq!(lines.next().unwrap(), "bot_count\t3");
        assert_eq!(lines.next().unwrap(), "active_bot_count\t2");
        for key in KEYS {
            let row = r.lines().find(|l| l.starts_with(&format!("{key}\t"))).unwrap_or_else(|| panic!("{key}"));
            assert_eq!(row.split('\t').count(), 5, "{key}: key + inf/5sec/1min/1hour");
        }
        assert!(!r.contains("total_requests") && !r.contains("bytes_sent"));
    }

    #[test]
    fn window_rates() {
        let s = ServerStats::new();
        // 10 requests of 100 bytes at t=100s, 2 ok + 1 error responses at t=100s, 1 request at t=3000s.
        for _ in 0..10 {
            s.record_request_at(100, 100);
        }
        s.record_response_at(100, true, 50);
        s.record_response_at(100, true, 50);
        s.record_response_at(100, false, 10);
        s.record_request_at(3000, 1);

        // At t=102s only the t=100 events fall inside every window that is >= 3 s wide.
        let rows = s.counter_rows(102, 200.0);
        let cols = |key: &str| -> Vec<f64> {
            rows.lines()
                .find(|l| l.starts_with(&format!("{key}\t")))
                .unwrap()
                .split('\t')
                .skip(1)
                .map(|v| v.parse().unwrap())
                .collect()
        };
        // inf: lifetime totals / uptime (11 requests / 200 s); 5sec: 10 / 5; 1min: 10 / 60; 1hour: 10 / 200
        // (the t=3000 event lies in the future of this synthetic clock and is excluded from windows).
        let rc = cols("request_count");
        assert!((rc[0] - 11.0 / 200.0).abs() < 1e-9);
        assert!((rc[1] - 2.0).abs() < 1e-9);
        assert!((rc[2] - 10.0 / 60.0).abs() < 1e-9);
        assert!((rc[3] - 10.0 / 200.0).abs() < 1e-9);
        assert!((cols("response_count")[1] - 3.0 / 5.0).abs() < 1e-9);
        assert!((cols("response_count_ok")[1] - 2.0 / 5.0).abs() < 1e-9);
        assert!((cols("response_count_error")[1] - 1.0 / 5.0).abs() < 1e-9);
        assert!((cols("response_bytes")[2] - 110.0 / 60.0).abs() < 1e-9);
        assert!((cols("request_bytes")[1] - 1000.0 / 5.0).abs() < 1e-9);

        // Events age out of the short windows: at t=110 the 5sec and 1min... 5sec is empty, 1min still holds them.
        let rows = s.counter_rows(110, 200.0);
        let rc: Vec<f64> = rows.lines().next().unwrap().split('\t').skip(1).map(|v| v.parse().unwrap()).collect();
        assert_eq!(rc[1], 0.0);
        assert!((rc[2] - 10.0 / 60.0).abs() < 1e-9);
        // ...and out of the 1min window at t=160 (but not the 1hour one).
        let rows = s.counter_rows(160, 200.0);
        let rc: Vec<f64> = rows.lines().next().unwrap().split('\t').skip(1).map(|v| v.parse().unwrap()).collect();
        assert_eq!(rc[2], 0.0);
        assert!(rc[3] > 0.0);
    }

    #[test]
    fn young_server_divides_by_uptime_not_window() {
        let s = ServerStats::new();
        s.record_request_at(1, 0);
        s.record_request_at(1, 0);
        let rows = s.counter_rows(2, 2.0);
        let rc: Vec<f64> = rows.lines().next().unwrap().split('\t').skip(1).map(|v| v.parse().unwrap()).collect();
        assert!(rc.iter().all(|v| (*v - 1.0).abs() < 1e-9), "{rc:?}");
    }

    #[test]
    fn ring_slot_is_reused_after_an_hour() {
        let mut r = Ring::new();
        r.add(5, REQUEST_COUNT, 3);
        r.add(5 + RING_SECS as u64, REQUEST_COUNT, 1);
        assert_eq!(r.sum(5 + RING_SECS as u64, 3600)[REQUEST_COUNT], 1);
    }

    #[test]
    fn cpu_parsing() {
        let stat = "1234 (my (weird) name) S 1 2 3 4 5 6 7 8 9 10 111 222 13 14 20 0 1 0 100 1000 100";
        assert_eq!(parse_proc_self_stat(stat), Some((111, 222)));
        assert_eq!(parse_proc_self_stat("garbage"), None);
        let all = "cpu  10 20 30 400 5 6 7 8 9 10\ncpu0 1 1 1 1 1 1 1 1 1 1\n";
        assert_eq!(parse_proc_stat_total(all), Some(10 + 20 + 30 + 400 + 5 + 6 + 7 + 8));
        assert_eq!(parse_proc_stat_total("intr 1\n"), None);
    }

    #[test]
    fn cpu_percentages_and_unknown() {
        let a = CpuSample { t: 0.0, user_ticks: 0, system_ticks: 0, total_ticks: 0 };
        let b = CpuSample { t: 1.0, user_ticks: 30, system_ticks: 10, total_ticks: 400 };
        assert_eq!(cpu_percentages(Some(a), Some(a))[0], "UNKNOWN");
        assert_eq!(cpu_percentages(None, Some(b))[0], "UNKNOWN");
        let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1) as f64;
        let p = cpu_percentages(Some(a), Some(b));
        assert_eq!(p[0], format!("{}%", 40.0 / 400.0 * 100.0 * cores));
        assert_eq!(p[1], format!("{}%", 30.0 / 400.0 * 100.0 * cores));
    }

    #[test]
    fn cpu_rows_have_four_columns() {
        let s = ServerStats::new();
        assert_eq!(s.cpu_rows(1.0), "");
        s.sample_cpu();
        s.sample_cpu();
        let rows = s.cpu_rows(s.uptime_seconds());
        if !rows.is_empty() {
            assert_eq!(rows.lines().count(), 3);
            assert!(rows.lines().all(|l| l.split('\t').count() == 5));
        }
    }

    #[test]
    fn mem_parsing() {
        let s = "VmPeak:\t  2048 kB\nVmSize:\t 1024 kB\nVmHWM:\t 512 kB\nVmRSS:\t 256 kB\n";
        assert_eq!(parse_mem_lines(s), "rss\t0.2MiB\nvm\t1.0MiB\nrss_peak\t0.5MiB\nvm_peak\t2.0MiB\n");
    }
}
