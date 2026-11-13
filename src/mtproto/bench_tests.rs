//! Ignored micro / end-to-end benchmarks for the MTProto layer (no extra dependencies).
//!
//! Run (release, one at a time, on the shared machine):
//! ```text
//! flock /tmp/bench.lock cargo test --release bench_ -- --ignored --nocapture --test-threads=1
//! ```
//! `bench_e2e_*` talk to an in-process mock DC (`MockDc`) over loopback: it speaks the server
//! side of the encrypted channel with an independent reference IGE, so its cost does not change
//! when the production code is optimised. The mock threads are excluded from allocation counts
//! and from the reported client CPU time.
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;
use std::time::Instant;

use aes::cipher::{generic_array::GenericArray, BlockDecrypt, BlockEncrypt, KeyInit};
use aes::Aes256;
use sha2::{Digest, Sha256};

use super::auth::{cache_auth, CachedBotAuth, TEST_DC_ADDR};
use super::crypto::derive_keys;
use super::media::MediaAttrs;
use super::uploader::upload_and_send_large_file_typed;

// ---------------------------------------------------------------- counting allocator

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static EXCLUDE: Cell<bool> = const { Cell::new(false) };
}

fn excluded() -> bool {
    EXCLUDE.try_with(|e| e.get()).unwrap_or(true)
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

// ---------------------------------------------------------------- reference IGE (independent of production code)

fn ref_ige_encrypt(pt: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Vec<u8> {
    let c = Aes256::new(GenericArray::from_slice(key));
    let mut out = vec![0u8; pt.len()];
    let (mut pc, mut pp) = ([0u8; 16], [0u8; 16]);
    pc.copy_from_slice(&iv[..16]);
    pp.copy_from_slice(&iv[16..]);
    for (p, o) in pt.as_chunks::<16>().0.iter().zip(out.as_chunks_mut::<16>().0.iter_mut()) {
        let mut b = GenericArray::clone_from_slice(p.as_slice());
        for i in 0..16 {
            b[i] ^= pc[i];
        }
        c.encrypt_block(&mut b);
        for i in 0..16 {
            b[i] ^= pp[i];
        }
        o.copy_from_slice(&b);
        pc = *o;
        pp.copy_from_slice(p.as_slice());
    }
    out
}

fn ref_ige_decrypt(ct: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Vec<u8> {
    let c = Aes256::new(GenericArray::from_slice(key));
    let mut out = vec![0u8; ct.len()];
    let (mut pc, mut pp) = ([0u8; 16], [0u8; 16]);
    pc.copy_from_slice(&iv[..16]);
    pp.copy_from_slice(&iv[16..]);
    for (x, o) in ct.as_chunks::<16>().0.iter().zip(out.as_chunks_mut::<16>().0.iter_mut()) {
        let mut b = GenericArray::clone_from_slice(x.as_slice());
        for i in 0..16 {
            b[i] ^= pp[i];
        }
        c.decrypt_block(&mut b);
        for i in 0..16 {
            b[i] ^= pc[i];
        }
        o.copy_from_slice(&b);
        pc = *x;
        pp = *o;
    }
    out
}

// ---------------------------------------------------------------- mock DC

fn auth_key() -> [u8; 256] {
    let mut k = [0u8; 256];
    for (i, b) in k.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(7).wrapping_add(3);
    }
    k
}

fn auth_key_id() -> i64 {
    let h = sha1::Sha1::digest(auth_key());
    i64::from_le_bytes(h[12..20].try_into().unwrap())
}

const MOCK_DOWNLOAD_SIZE: u64 = 128 * 1024 * 1024 + 12345;
const CHUNK: usize = 512 * 1024;

static MOCK_UPLOADED_BYTES: AtomicU64 = AtomicU64::new(0);
static MOCK_UPLOADED_PARTS: AtomicU64 = AtomicU64::new(0);

fn tl_bytes(out: &mut Vec<u8>, data_len: usize, fill: impl FnOnce(&mut Vec<u8>)) {
    let header = if data_len <= 253 {
        out.push(data_len as u8);
        1
    } else {
        out.push(254);
        out.extend_from_slice(&(data_len as u32).to_le_bytes()[..3]);
        4
    };
    fill(out);
    let pad = (4 - (header + data_len) % 4) % 4;
    out.extend(std::iter::repeat_n(0u8, pad));
}

/// Encrypts a server -> client message (x = 8 keys) into `out` as a full intermediate frame.
fn server_frame(session_id: i64, msg_id: i64, body: &[u8], out: &mut Vec<u8>) {
    let key = auth_key();
    let mut pt = Vec::with_capacity(32 + body.len() + 32);
    pt.extend_from_slice(&1i64.to_le_bytes());
    pt.extend_from_slice(&session_id.to_le_bytes());
    pt.extend_from_slice(&msg_id.to_le_bytes());
    pt.extend_from_slice(&1i32.to_le_bytes());
    pt.extend_from_slice(&(body.len() as u32).to_le_bytes());
    pt.extend_from_slice(body);
    let pad = 12 + (16 - (pt.len() + 12) % 16) % 16;
    pt.extend(std::iter::repeat_n(0xabu8, pad));
    let mut h = Sha256::new();
    h.update(&key[96..128]);
    h.update(&pt);
    let mut msg_key = [0u8; 16];
    msg_key.copy_from_slice(&h.finalize()[8..24]);
    let (k, iv) = derive_keys(&key, &msg_key, false);
    let enc = ref_ige_encrypt(&pt, &k, &iv);
    out.clear();
    out.extend_from_slice(&((24 + enc.len()) as u32).to_le_bytes());
    out.extend_from_slice(&auth_key_id().to_le_bytes());
    out.extend_from_slice(&msg_key);
    out.extend_from_slice(&enc);
}

fn serve(mut s: TcpStream) {
    EXCLUDE.with(|e| e.set(true));
    s.set_nodelay(true).ok();
    let mut tag = [0u8; 4];
    if s.read_exact(&mut tag).is_err() {
        return;
    }
    let key = auth_key();
    let (mut frame, mut out, mut body) = (Vec::new(), Vec::new(), Vec::new());
    let mut counter = 0i64;
    loop {
        let mut len = [0u8; 4];
        if s.read_exact(&mut len).is_err() {
            return;
        }
        let len = u32::from_le_bytes(len) as usize;
        frame.resize(len, 0);
        if s.read_exact(&mut frame).is_err() {
            return;
        }
        let msg_key: [u8; 16] = frame[8..24].try_into().unwrap();
        let (k, iv) = derive_keys(&key, &msg_key, true);
        // IGE decryption is sequential from the start, so a prefix can be decrypted alone.
        let n = ((len - 24).min(if len < 8192 { len } else { 96 })) & !15;
        let pt = ref_ige_decrypt(&frame[24..24 + n], &k, &iv);
        let session_id = i64::from_le_bytes(pt[8..16].try_into().unwrap());
        let req_msg_id = i64::from_le_bytes(pt[16..24].try_into().unwrap());
        let data_len = u32::from_le_bytes(pt[28..32].try_into().unwrap()) as usize;
        let req = &pt[32..];
        let ctor = u32::from_le_bytes(req[..4].try_into().unwrap());

        body.clear();
        body.extend_from_slice(&0xf35c6d01u32.to_le_bytes());
        body.extend_from_slice(&req_msg_id.to_le_bytes());
        match ctor {
            0x62d6b459 => continue, // msgs_ack: no reply
            0x330e77f => {
                // messages.sendMedia: random_id is the last long of the body.
                let rid = &req[data_len - 8..data_len];
                body.extend_from_slice(&0x4e90bfd6u32.to_le_bytes());
                body.extend_from_slice(&777i32.to_le_bytes());
                body.extend_from_slice(rid);
                body.extend_from_slice(&0x8fd4c4d8u32.to_le_bytes());
                body.extend_from_slice(&0i32.to_le_bytes());
                body.extend_from_slice(&4242i64.to_le_bytes());
                body.extend_from_slice(&99i64.to_le_bytes());
                tl_bytes(&mut body, 2, |o| o.extend_from_slice(b"ab"));
                body.extend_from_slice(&1_700_000_000i32.to_le_bytes());
                body.extend_from_slice(&[0u8; 40]);
            }
            0xbe5335be => {
                // upload.getFile: ... offset:long limit:int (location without file reference)
                let offset = u64::from_le_bytes(req[36..44].try_into().unwrap());
                let limit = i32::from_le_bytes(req[44..48].try_into().unwrap()) as u64;
                let n = MOCK_DOWNLOAD_SIZE.saturating_sub(offset).min(limit) as usize;
                body.extend_from_slice(&0x096a18d5u32.to_le_bytes());
                body.extend_from_slice(&0i32.to_le_bytes());
                body.extend_from_slice(&0i32.to_le_bytes());
                let fill = (offset / CHUNK as u64) as u8;
                tl_bytes(&mut body, n, |o| o.extend(std::iter::repeat_n(fill, n)));
            }
            0xde7b673d | 0xb304a621 => {
                // saveBigFilePart: ctor file_id part [total] bytes -> bytes header after 16/20 bytes.
                let at = if ctor == 0xde7b673d { 20 } else { 16 };
                let blen = if req[at] == 254 {
                    u32::from_le_bytes([req[at + 1], req[at + 2], req[at + 3], 0]) as usize
                } else {
                    req[at] as usize
                };
                MOCK_UPLOADED_BYTES.fetch_add(blen as u64, Ordering::Relaxed);
                MOCK_UPLOADED_PARTS.fetch_add(1, Ordering::Relaxed);
                body.extend_from_slice(&0x997275b5u32.to_le_bytes());
            }
            _ => body.extend_from_slice(&0x997275b5u32.to_le_bytes()),
        }
        counter += 1;
        server_frame(session_id, (1i64 << 33) | (counter * 4 + 1), &body, &mut out);
        if s.write_all(&out).is_err() {
            return;
        }
    }
}

fn mock_dc() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        TEST_DC_ADDR.set(l.local_addr().unwrap().to_string()).unwrap();
        std::thread::Builder::new()
            .name("mockdc-accept".into())
            .spawn(move || {
                EXCLUDE.with(|e| e.set(true));
                for s in l.incoming().flatten() {
                    std::thread::Builder::new().name("mockdc-conn".into()).spawn(move || serve(s)).ok();
                }
            })
            .unwrap();
    });
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
        "BENCH {name}: min {:.3}s  {:.1} MB/s  cpu {:.2}s  allocs/{unit_name} {:.2}  alloc-bytes/{unit_name} {:.0}  peak-live {:.2} MiB  (runs: {})",
        best.secs,
        mb / best.secs,
        best.cpu,
        best.allocs as f64 / units,
        best.bytes as f64 / units,
        best.peak_live as f64 / 1048576.0,
        runs.iter().map(|r| format!("{:.3}", r.secs)).collect::<Vec<_>>().join(" ")
    );
}

// ---------------------------------------------------------------- micro benchmarks

fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(|i| (i.wrapping_mul(31) >> 3) as u8).collect()
}

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
    let key = auth_key();
    let kid = auth_key_id();
    for (label, size, iters) in [("512K", 512 * 1024usize, 200usize), ("200B", 200, 100_000)] {
        let body = pattern(size);
        let mb = (iters * size) as f64 / 1048576.0;
        let mut runs = Vec::new();
        for _ in 0..3 {
            let ((), r) = measured(|| {
                for i in 0..iters {
                    std::hint::black_box(
                        super::packet::pack_encrypted_message(&key, kid, 1, 2, 4 * i as i64 + 4, 1, &body).unwrap(),
                    );
                }
            });
            runs.push(r);
        }
        report(&format!("pack_{label}"), mb, &runs, iters as f64, "msg");
        println!("BENCH pack_{label}: {:.0} msgs/s", iters as f64 / runs.iter().map(|r| r.secs).fold(f64::MAX, f64::min));

        // Server-direction packet for unpack (x = 8 keys), built with the reference code.
        let mut frame = Vec::new();
        server_frame(2, 101, &body, &mut frame);
        let packet = frame[4..].to_vec();
        let mut runs = Vec::new();
        for _ in 0..3 {
            let ((), r) = measured(|| {
                for _ in 0..iters {
                    let (id, _seq, data) = super::packet::unpack_encrypted_message(&key, kid, 2, &packet).unwrap();
                    assert_eq!(id & 1, 1);
                    std::hint::black_box(data);
                }
            });
            runs.push(r);
        }
        report(&format!("unpack_{label}"), mb, &runs, iters as f64, "msg");
        println!("BENCH unpack_{label}: {:.0} msgs/s", iters as f64 / runs.iter().map(|r| r.secs).fold(f64::MAX, f64::min));
    }
}

// ---------------------------------------------------------------- end-to-end (loopback mock DC)

fn make_file(size: usize) -> std::path::PathBuf {
    let path = std::env::temp_dir().join(format!("mtproto_bench_{}.bin", std::process::id()));
    let block = pattern(1 << 20);
    let mut f = std::fs::File::create(&path).unwrap();
    let mut left = size;
    while left > 0 {
        let n = left.min(block.len());
        f.write_all(&block[..n]).unwrap();
        left -= n;
    }
    path
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_e2e_upload() {
    mock_dc();
    let size = 128 * 1024 * 1024usize;
    let path = make_file(size);
    cache_auth("bench-token", CachedBotAuth { dc_id: 1, auth_key: auth_key(), server_salt: 1, time_offset: 0 });
    let mut runs = Vec::new();
    for _ in 0..3 {
        MOCK_UPLOADED_BYTES.store(0, Ordering::Relaxed);
        MOCK_UPLOADED_PARTS.store(0, Ordering::Relaxed);
        let (a0, c0) = (snap(), client_cpu_secs());
        reset_peak();
        let base_live = LIVE.load(Ordering::Relaxed);
        let t = Instant::now();
        let res = upload_and_send_large_file_typed(1, "hash", "bench-token", "-12345", None, &path, "", None, MediaAttrs::default())
            .await
            .unwrap();
        let secs = t.elapsed().as_secs_f64();
        let (a1, c1) = (snap(), client_cpu_secs());
        assert_eq!(res.message_id, 777);
        assert_eq!(res.doc_id, 4242);
        assert_eq!(MOCK_UPLOADED_BYTES.load(Ordering::Relaxed), size as u64);
        assert_eq!(MOCK_UPLOADED_PARTS.load(Ordering::Relaxed), (size / CHUNK) as u64);
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

fn encode_file_id(dc: i32, doc_id: i64) -> String {
    let mut raw = Vec::new();
    raw.extend_from_slice(&5i32.to_le_bytes()); // document
    raw.extend_from_slice(&dc.to_le_bytes());
    raw.extend_from_slice(&doc_id.to_le_bytes());
    raw.extend_from_slice(&0x1122334455667788i64.to_le_bytes());
    // zero run-length encoding (inverse of `zero_decode`)
    let mut enc = Vec::new();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == 0 {
            let mut n = 0;
            while i < raw.len() && raw[i] == 0 && n < 255 {
                n += 1;
                i += 1;
            }
            enc.extend_from_slice(&[0, n as u8]);
        } else {
            enc.push(raw[i]);
            i += 1;
        }
    }
    enc.push(2); // PERSISTENT_ID_VERSION_OLD
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut s = String::new();
    for c in enc.chunks(3) {
        let n = c.iter().fold(0u32, |a, &b| (a << 8) | b as u32) << (8 * (3 - c.len()));
        for k in 0..=c.len() {
            s.push(T[((n >> (18 - 6 * k)) & 63) as usize] as char);
        }
    }
    s
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn bench_e2e_download() {
    mock_dc();
    cache_auth("bench-token", CachedBotAuth { dc_id: 4, auth_key: auth_key(), server_salt: 1, time_offset: 0 });
    let file_id = encode_file_id(4, 0x0102030405060708);
    let dir = std::env::temp_dir().join(format!("mtproto_bench_dl_{}", std::process::id()));
    let mut runs = Vec::new();
    for _ in 0..3 {
        std::fs::remove_dir_all(&dir).ok();
        let (a0, c0) = (snap(), client_cpu_secs());
        reset_peak();
        let base_live = LIVE.load(Ordering::Relaxed);
        let t = Instant::now();
        let (path, total) = super::downloader::download_large_file_via_mtproto(1, "hash", "bench-token", &file_id, &dir)
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
