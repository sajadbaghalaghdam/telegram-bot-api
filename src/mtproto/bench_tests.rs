//! Ignored micro / end-to-end benchmarks for the MTProto layer (no extra dependencies).
//!
//! Run (release, one at a time, on the shared machine):
//! ```text
//! flock /tmp/bench.lock cargo test --release bench_ -- --ignored --nocapture --test-threads=1
//! ```
//! `bench_e2e_*` talk to the in-process mock DC of `mock_dc` over loopback (`Profile::Bench`):
//! it speaks the server side of the encrypted channel with an independent reference IGE, so its
//! cost does not change when the production code is optimised. The mock threads are excluded from allocation counts
//! and from the reported client CPU time.
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use super::media::MediaAttrs;
use super::mock_dc::{self, encode_file_id, pattern, profile, Profile, CHUNK, MOCK_THREAD};
use super::uploader::upload_and_send_large_file_typed;

// ---------------------------------------------------------------- counting allocator

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn excluded() -> bool {
    MOCK_THREAD.try_with(|e| e.get()).unwrap_or(true)
}

fn note_alloc(size: usize) {
    ALLOCS.fetch_add(1, Ordering::Relaxed);
    BYTES.fetch_add(size as u64, Ordering::Relaxed);
    let live = LIVE.fetch_add(size, Ordering::Relaxed) + size;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        if !excluded() {
            note_alloc(l.size());
        }
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        if !excluded() {
            LIVE.fetch_sub(l.size().min(LIVE.load(Ordering::Relaxed)), Ordering::Relaxed);
        }
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        if !excluded() {
            note_alloc(l.size());
        }
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        if !excluded() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            if new > l.size() {
                BYTES.fetch_add((new - l.size()) as u64, Ordering::Relaxed);
                let live = LIVE.fetch_add(new - l.size(), Ordering::Relaxed) + (new - l.size());
                PEAK.fetch_max(live, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub((l.size() - new).min(LIVE.load(Ordering::Relaxed)), Ordering::Relaxed);
            }
        }
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

#[derive(Clone, Copy, Debug)]
struct AllocSnap {
    allocs: u64,
    bytes: u64,
}

fn snap() -> AllocSnap {
    AllocSnap { allocs: ALLOCS.load(Ordering::Relaxed), bytes: BYTES.load(Ordering::Relaxed) }
}

fn reset_peak() {
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
}

// ---------------------------------------------------------------- measurement helpers

/// CPU seconds (user+sys) of every thread of this process except the mock DC threads.
fn client_cpu_secs() -> f64 {
    let mut ticks = 0u64;
    for t in std::fs::read_dir("/proc/self/task").unwrap().flatten() {
        let name = std::fs::read_to_string(t.path().join("comm")).unwrap_or_default();
        if name.starts_with("mockdc") {
            continue;
        }
        let stat = std::fs::read_to_string(t.path().join("stat")).unwrap_or_default();
        if let Some(rest) = stat.rsplit_once(") ").map(|x| x.1) {
            let f: Vec<&str> = rest.split(' ').collect();
            ticks += f[11].parse::<u64>().unwrap_or(0) + f[12].parse::<u64>().unwrap_or(0);
        }
    }
    ticks as f64 / 100.0
}

fn rss_kib() -> u64 {
    let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    s.lines()
        .find(|l| l.starts_with("VmHWM:"))
        .and_then(|l| l.split_whitespace().nth(1)?.parse().ok())
        .unwrap_or(0)
}

struct Run {
    secs: f64,
    cpu: f64,
    allocs: u64,
    bytes: u64,
    peak_live: usize,
}

fn measured<R>(f: impl FnOnce() -> R) -> (R, Run) {
    let (a0, c0) = (snap(), client_cpu_secs());
    reset_peak();
    let base_live = LIVE.load(Ordering::Relaxed);
    let t = Instant::now();
    let r = f();
    let secs = t.elapsed().as_secs_f64();
    let (a1, c1) = (snap(), client_cpu_secs());
    (
        r,
        Run {
            secs,
            cpu: c1 - c0,
            allocs: a1.allocs - a0.allocs,
            bytes: a1.bytes - a0.bytes,
            peak_live: PEAK.load(Ordering::Relaxed).saturating_sub(base_live),
        },
    )
}

fn report(name: &str, mb: f64, runs: &[Run], units: f64, unit_name: &str) {
    let best = runs.iter().min_by(|a, b| a.secs.total_cmp(&b.secs)).unwrap();
    println!(
        "BENCH {name}: min {:.3}s  {:.1} MB/s  min-cpu {:.2}s  allocs/{unit_name} {:.2}  alloc-bytes/{unit_name} {:.0}  peak-live {:.2} MiB  (runs: {})",
        best.secs,
        mb / best.secs,
        runs.iter().map(|r| r.cpu).fold(f64::MAX, f64::min),
        best.allocs as f64 / units,
        best.bytes as f64 / units,
        best.peak_live as f64 / 1048576.0,
        runs.iter().map(|r| format!("{:.3}", r.secs)).collect::<Vec<_>>().join(" ")
    );
}

// ---------------------------------------------------------------- micro benchmarks

#[test]
#[ignore]
fn bench_ige() {
    let key = [0x42u8; 32];
    let iv = [0x17u8; 32];
    let data = pattern(512 * 1024);
    let iters = 200usize;
    let mb = (iters * data.len()) as f64 / 1048576.0;

    let mut runs = Vec::new();
    for _ in 0..3 {
        let ((), r) = measured(|| {
            for _ in 0..iters {
                std::hint::black_box(super::crypto::aes_256_ige_encrypt(&data, &key, &iv).unwrap());
            }
        });
        runs.push(r);
    }
    report("ige_encrypt_512K", mb, &runs, iters as f64, "call");

    let enc = super::crypto::aes_256_ige_encrypt(&data, &key, &iv).unwrap();
    let mut runs = Vec::new();
    for _ in 0..3 {
        let ((), r) = measured(|| {
            for _ in 0..iters {
                std::hint::black_box(super::crypto::aes_256_ige_decrypt(&enc, &key, &iv).unwrap());
            }
        });
        runs.push(r);
    }
    report("ige_decrypt_512K", mb, &runs, iters as f64, "call");
    assert_eq!(super::crypto::aes_256_ige_decrypt(&enc, &key, &iv).unwrap(), data);

    // In-place variants (what the packet path uses).
    use super::crypto::Aes256Ige;
    let mut buf = data.clone();
    let mut runs = Vec::new();
    for _ in 0..3 {
        let ((), r) = measured(|| {
            for _ in 0..iters {
                Aes256Ige::encrypt_in_place(&mut buf, &key, &iv).unwrap();
            }
        });
        runs.push(r);
    }
    report("ige_encrypt_inplace_512K", mb, &runs, iters as f64, "call");
    let mut runs = Vec::new();
    for _ in 0..3 {
        let ((), r) = measured(|| {
            for _ in 0..iters {
                Aes256Ige::decrypt_in_place(&mut buf, &key, &iv).unwrap();
            }
        });
        runs.push(r);
    }
    report("ige_decrypt_inplace_512K", mb, &runs, iters as f64, "call");
}

#[test]
#[ignore]
fn bench_packet() {
    let st = profile(Profile::Bench);
    let (key, kid) = (st.auth_key, st.auth_key_id);
    for (label, size, iters) in [("512K", 512 * 1024usize, 200usize), ("200B", 200, 100_000)] {
        let body = pattern(size);
        let mb = (iters * size) as f64 / 1048576.0;
        let mut runs = Vec::new();
        let mut out = Vec::new();
        for _ in 0..3 {
            let ((), r) = measured(|| {
                for i in 0..iters {
                    let header = super::packet::OutgoingHeader { server_salt: 1, session_id: 2, msg_id: 4 * i as i64 + 4, seq_no: 1 };
                    super::packet::pack_encrypted_frame(&mut out, &key, kid, &header, &body).unwrap();
                    std::hint::black_box(&out);
                }
            });
            runs.push(r);
        }
        report(&format!("pack_{label}"), mb, &runs, iters as f64, "msg");
        println!("BENCH pack_{label}: {:.0} msgs/s", iters as f64 / runs.iter().map(|r| r.secs).fold(f64::MAX, f64::min));

        // Server-direction packet for unpack (x = 8 keys), built with the reference code.
        let mut frame = Vec::new();
        mock_dc::server_frame(st, 2, 101, &body, &mut frame);
        let packet = frame[4..].to_vec();
        let mut runs = Vec::new();
        let mut work = Vec::new();
        for _ in 0..3 {
            let ((), r) = measured(|| {
                for _ in 0..iters {
                    work.clear();
                    work.extend_from_slice(&packet); // the receive buffer is refilled from the socket
                    let u = super::packet::unpack_encrypted_in_place(&key, kid, 2, &mut work).unwrap();
                    assert_eq!(u.msg_id & 1, 1);
                    std::hint::black_box(&work[u.data]);
                }
            });
            runs.push(r);
        }
        report(&format!("unpack_{label}"), mb, &runs, iters as f64, "msg");
        println!("BENCH unpack_{label}: {:.0} msgs/s", iters as f64 / runs.iter().map(|r| r.secs).fold(f64::MAX, f64::min));
    }
}

// ---------------------------------------------------------------- end-to-end (loopback mock DC)

const MOCK_DOWNLOAD_SIZE: u64 = 128 * 1024 * 1024 + 12345;

fn make_file(size: usize) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("mtproto_bench_{}.bin", std::process::id()));
    mock_dc::write_pattern_file(&path, size);
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_e2e_upload() {
    mock_dc::start();
    let st = profile(Profile::Bench);
    let size = 128 * 1024 * 1024usize;
    let path = make_file(size);
    let mut runs = Vec::new();
    for _ in 0..3 {
        st.uploaded_bytes.store(0, Ordering::Relaxed);
        let conns0 = mock_dc::total_accepted();
        let (a0, c0) = (snap(), client_cpu_secs());
        reset_peak();
        let base_live = LIVE.load(Ordering::Relaxed);
        let t = Instant::now();
        let res = upload_and_send_large_file_typed(1, "hash", st.bot_token, "-12345", None, &path, "", None, MediaAttrs::default())
            .await
            .unwrap();
        let secs = t.elapsed().as_secs_f64();
        let (a1, c1) = (snap(), client_cpu_secs());
        assert_eq!(res.message_id, 777);
        assert_eq!(res.doc_id, 4242);
        assert_eq!(st.uploaded_bytes.load(Ordering::Relaxed), size as u64);
        println!("BENCH e2e_upload: connections opened {}", mock_dc::total_accepted() - conns0);
        runs.push(Run {
            secs,
            cpu: c1 - c0,
            allocs: a1.allocs - a0.allocs,
            bytes: a1.bytes - a0.bytes,
            peak_live: PEAK.load(Ordering::Relaxed).saturating_sub(base_live),
        });
    }
    std::fs::remove_file(&path).ok();
    report("e2e_upload_128MiB", size as f64 / 1048576.0, &runs, (size / CHUNK) as f64, "part");
    println!("BENCH e2e_upload: process VmHWM {} KiB (includes the 128 MiB-file page cache? no: RSS only)", rss_kib());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_e2e_download() {
    mock_dc::start();
    let st = profile(Profile::Bench);
    let file_id = encode_file_id(4, MOCK_DOWNLOAD_SIZE as i64);
    let dir = std::env::temp_dir().join(format!("mtproto_bench_dl_{}", std::process::id()));
    let mut runs = Vec::new();
    for _ in 0..3 {
        std::fs::remove_dir_all(&dir).ok();
        let (a0, c0) = (snap(), client_cpu_secs());
        reset_peak();
        let base_live = LIVE.load(Ordering::Relaxed);
        let t = Instant::now();
        let (path, total) = super::downloader::download_large_file_via_mtproto(1, "hash", st.bot_token, &file_id, &dir)
            .await
            .unwrap();
        let secs = t.elapsed().as_secs_f64();
        let (a1, c1) = (snap(), client_cpu_secs());
        let peak = PEAK.load(Ordering::Relaxed).saturating_sub(base_live);
        assert_eq!(total, MOCK_DOWNLOAD_SIZE);
        let data = std::fs::read(&path).unwrap();
        assert_eq!(data.len() as u64, MOCK_DOWNLOAD_SIZE);
        for (i, c) in data.chunks(CHUNK).enumerate() {
            assert!(c.iter().all(|&b| b == i as u8), "chunk {i} corrupted");
        }
        runs.push(Run {
            secs,
            cpu: c1 - c0,
            allocs: a1.allocs - a0.allocs,
            bytes: a1.bytes - a0.bytes,
            peak_live: peak,
        });
    }
    std::fs::remove_dir_all(&dir).ok();
    report(
        "e2e_download_128MiB",
        MOCK_DOWNLOAD_SIZE as f64 / 1048576.0,
        &runs,
        MOCK_DOWNLOAD_SIZE.div_ceil(CHUNK as u64) as f64,
        "part",
    );
}
