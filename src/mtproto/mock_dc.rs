//! Test-only in-process mock Telegram DC speaking the server side of the encrypted MTProto
//! channel (intermediate transport), with an independent reference AES-IGE.
//!
//! The production code is pointed at it through `auth::TEST_DC_ADDR` and pre-seeded auth keys
//! (no DH handshake). Three auth keys select the behaviour:
//! * [`Profile::Bench`]: minimal work per request (only the first 96 bytes are decrypted), so
//!   benchmarks measure the client;
//! * [`Profile::Verify`]: full decryption, `msg_key` check and upload content check;
//! * [`Profile::Flaky`]: like `Verify`, but the first request of each connection is answered
//!   with `bad_server_salt` and the first request re-sent with the new salt with `FLOOD_WAIT_1`.
//!
//! Special download id: [`FAIL_AFTER_TWO_CHUNKS`] is a huge file that answers `LOCATION_INVALID`
//! for offsets >= 2 chunks; any other `doc_id` is the file size in bytes. Chunk `i` of a download is filled with the
//! byte `i as u8`; upload part data must equal [`pattern_byte`] of the absolute offset.
use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use aes::cipher::{generic_array::GenericArray, BlockDecrypt, BlockEncrypt, KeyInit};
use aes::Aes256;
use sha2::{Digest, Sha256};

use super::auth::{cache_auth, CachedBotAuth, TEST_DC_ADDR};
use super::crypto::derive_keys;

pub const CHUNK: usize = 512 * 1024;

/// `doc_id` of a download whose chunks from index 2 on fail (see module docs).
pub const FAIL_AFTER_TWO_CHUNKS: i64 = 1 << 40;

thread_local! {
    /// Threads that must not be counted by the benchmark allocator.
    pub static MOCK_THREAD: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Profile {
    Bench,
    Verify,
    Flaky,
}

/// Per-profile state observable by tests.
pub struct ProfileState {
    pub profile: Profile,
    pub auth_key: [u8; 256],
    pub auth_key_id: i64,
    pub bot_token: &'static str,
    /// Upload part indexes seen with correct content.
    pub parts: Mutex<HashSet<u32>>,
    /// Protocol violations detected by the mock (must stay empty).
    pub errors: Mutex<Vec<String>>,
    pub uploaded_bytes: AtomicU64,
    /// Requests received with the salt from `bad_server_salt` (Flaky profile).
    pub salted_requests: AtomicU64,
    pub floods_sent: AtomicU64,
}

fn make_key(seed: u8) -> [u8; 256] {
    let mut k = [0u8; 256];
    for (i, b) in k.iter_mut().enumerate() {
        *b = (i as u8).wrapping_mul(7).wrapping_add(seed);
    }
    k
}

fn state(profile: Profile, seed: u8, token: &'static str) -> ProfileState {
    let auth_key = make_key(seed);
    let h = sha1::Sha1::digest(auth_key);
    ProfileState {
        profile,
        auth_key,
        auth_key_id: i64::from_le_bytes(h[12..20].try_into().unwrap()),
        bot_token: token,
        parts: Mutex::new(HashSet::new()),
        errors: Mutex::new(Vec::new()),
        uploaded_bytes: AtomicU64::new(0),
        salted_requests: AtomicU64::new(0),
        floods_sent: AtomicU64::new(0),
    }
}

pub fn profile(p: Profile) -> &'static ProfileState {
    static STATES: OnceLock<[ProfileState; 3]> = OnceLock::new();
    let s = STATES.get_or_init(|| {
        [state(Profile::Bench, 3, "mock-bench"), state(Profile::Verify, 11, "mock-verify"), state(Profile::Flaky, 29, "mock-flaky")]
    });
    &s[p as usize]
}

/// Deterministic upload content.
pub fn pattern_byte(i: usize) -> u8 {
    (i.wrapping_mul(31) >> 3) as u8
}

pub fn pattern(n: usize) -> Vec<u8> {
    (0..n).map(pattern_byte).collect()
}

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
        pp = *p;
    }
    out
}

pub fn ref_ige_decrypt(ct: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Vec<u8> {
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
    out.extend(std::iter::repeat_n(0u8, (4 - (header + data_len) % 4) % 4));
}

/// Encrypts a server -> client message (x = 8 keys) into `out` as a full intermediate frame.
pub fn server_frame(st: &ProfileState, session_id: i64, msg_id: i64, body: &[u8], out: &mut Vec<u8>) {
    let key = &st.auth_key;
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
    let (k, iv) = derive_keys(key, &msg_key, false);
    let enc = ref_ige_encrypt(&pt, &k, &iv);
    out.clear();
    out.extend_from_slice(&((24 + enc.len()) as u32).to_le_bytes());
    out.extend_from_slice(&st.auth_key_id.to_le_bytes());
    out.extend_from_slice(&msg_key);
    out.extend_from_slice(&enc);
}

fn rpc_result_header(req_msg_id: i64) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&0xf35c6d01u32.to_le_bytes());
    body.extend_from_slice(&req_msg_id.to_le_bytes());
    body
}

fn rpc_error_body(req_msg_id: i64, code: i32, msg: &str) -> Vec<u8> {
    let mut body = rpc_result_header(req_msg_id);
    body.extend_from_slice(&0x2144ca19u32.to_le_bytes());
    body.extend_from_slice(&code.to_le_bytes());
    tl_bytes(&mut body, msg.len(), |o| o.extend_from_slice(msg.as_bytes()));
    body
}

fn record_error(st: &ProfileState, msg: String) {
    st.errors.lock().unwrap().push(msg);
}

/// Checks an `upload.saveBigFilePart` / `saveFilePart` body against the deterministic pattern.
fn verify_part(st: &ProfileState, ctor: u32, body: &[u8]) {
    let part = u32::from_le_bytes(body[12..16].try_into().unwrap());
    // saveBigFilePart: ctor file_id part total bytes; saveFilePart: ctor file_id part bytes
    let mut cur = &body[if ctor == 0xde7b673d { 20 } else { 16 }..];
    let Ok(data) = super::tl::read_bytes(&mut cur) else {
        return record_error(st, format!("part {part}: malformed bytes field"));
    };
    if !cur.is_empty() {
        record_error(st, format!("part {part}: trailing data after bytes"));
    }
    let base = part as usize * CHUNK;
    if let Some(bad) = data.iter().enumerate().position(|(i, &b)| b != pattern_byte(base + i)) {
        return record_error(st, format!("part {part}: wrong content at byte {bad}"));
    }
    st.uploaded_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);
    st.parts.lock().unwrap().insert(part);
}

/// Decrements the open-connection counter on every exit path of `serve`.
struct OpenGuard;

impl Drop for OpenGuard {
    fn drop(&mut self) {
        OPEN_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
    }
}

fn serve(mut s: TcpStream) {
    let _open = OpenGuard;
    MOCK_THREAD.with(|e| e.set(true));
    s.set_nodelay(true).ok();
    let mut tag = [0u8; 4];
    if s.read_exact(&mut tag).is_err() {
        return;
    }
    let (mut frame, mut out) = (Vec::new(), Vec::new());
    let mut counter = 0i64;
    let mut requests = 0u32;
    let mut st: Option<&'static ProfileState> = None;
    let mut new_salt = 0i64;
    let mut flooded = false;
    loop {
        let mut len = [0u8; 4];
        if s.read_exact(&mut len).is_err() {
            break;
        }
        let len = u32::from_le_bytes(len) as usize;
        frame.resize(len, 0);
        if s.read_exact(&mut frame).is_err() {
            break;
        }
        let key_id = i64::from_le_bytes(frame[..8].try_into().unwrap());
        if st.is_none() {
            let found = [Profile::Bench, Profile::Verify, Profile::Flaky].into_iter().map(profile).find(|p| p.auth_key_id == key_id);
            let Some(found) = found else { break };
            st = Some(found);
        }
        let st = st.unwrap();
        let verify = st.profile != Profile::Bench;
        let msg_key: [u8; 16] = frame[8..24].try_into().unwrap();
        let (k, iv) = derive_keys(&st.auth_key, &msg_key, true);
        // IGE decryption is sequential from the start, so a prefix can be decrypted alone.
        let n = if verify || len < 8192 { len - 24 } else { 96 } & !15;
        let pt = ref_ige_decrypt(&frame[24..24 + n], &k, &iv);
        if verify {
            let mut h = Sha256::new();
            h.update(&st.auth_key[88..120]);
            h.update(&pt);
            if h.finalize()[8..24] != msg_key {
                record_error(st, "client msg_key mismatch".into());
            }
            if !pt.len().is_multiple_of(16) {
                record_error(st, "client plaintext not block aligned".into());
            }
        }
        let session_id = i64::from_le_bytes(pt[8..16].try_into().unwrap());
        let req_msg_id = i64::from_le_bytes(pt[16..24].try_into().unwrap());
        let data_len = u32::from_le_bytes(pt[28..32].try_into().unwrap()) as usize;
        if verify {
            let padding = pt.len().saturating_sub(32 + data_len);
            if !(12..=27).contains(&padding) || req_msg_id & 3 != 0 {
                record_error(st, format!("bad client header: padding {padding} msg_id {req_msg_id}"));
            }
        }
        let req = &pt[32..];
        let ctor = u32::from_le_bytes(req[..4].try_into().unwrap());
        if ctor == 0x62d6b459 {
            continue; // msgs_ack: no reply
        }
        requests += 1;
        counter += 1;
        let srv_msg_id = (1i64 << 33) | (counter * 4 + 1);

        if st.profile == Profile::Flaky && requests == 1 {
            // bad_server_salt#edab447b bad_msg_id bad_msg_seqno error_code new_server_salt
            new_salt = 0x5eed_5a17;
            let mut b = Vec::new();
            b.extend_from_slice(&0xedab447bu32.to_le_bytes());
            b.extend_from_slice(&req_msg_id.to_le_bytes());
            b.extend_from_slice(&1i32.to_le_bytes());
            b.extend_from_slice(&48i32.to_le_bytes());
            b.extend_from_slice(&new_salt.to_le_bytes());
            server_frame(st, session_id, srv_msg_id, &b, &mut out);
            if s.write_all(&out).is_err() {
                break;
            }
            continue;
        }
        let salted = new_salt != 0 && i64::from_le_bytes(pt[0..8].try_into().unwrap()) == new_salt;
        if salted {
            st.salted_requests.fetch_add(1, Ordering::Relaxed);
        }
        if st.profile == Profile::Flaky && salted && !flooded {
            flooded = true;
            st.floods_sent.fetch_add(1, Ordering::Relaxed);
            let b = rpc_error_body(req_msg_id, 420, "FLOOD_WAIT_1");
            server_frame(st, session_id, srv_msg_id, &b, &mut out);
            if s.write_all(&out).is_err() {
                break;
            }
            continue;
        }

        let mut body = rpc_result_header(req_msg_id);
        match ctor {
            0x330e77f => {
                // messages.sendMedia: random_id is the last long of the body.
                body.extend_from_slice(&0x4e90bfd6u32.to_le_bytes());
                body.extend_from_slice(&777i32.to_le_bytes());
                body.extend_from_slice(&req[data_len - 8..data_len]);
                body.extend_from_slice(&0x8fd4c4d8u32.to_le_bytes());
                body.extend_from_slice(&0i32.to_le_bytes());
                body.extend_from_slice(&4242i64.to_le_bytes());
                body.extend_from_slice(&99i64.to_le_bytes());
                tl_bytes(&mut body, 2, |o| o.extend_from_slice(b"ab"));
                body.extend_from_slice(&1_700_000_000i32.to_le_bytes());
                body.extend_from_slice(&[0u8; 40]);
            }
            0xbe5335be => {
                // upload.getFile: flags loc_ctor id access_hash file_ref thumb offset limit
                let doc_id = u64::from_le_bytes(req[12..20].try_into().unwrap());
                let offset = u64::from_le_bytes(req[36..44].try_into().unwrap());
                let limit = i32::from_le_bytes(req[44..48].try_into().unwrap()) as u64;
                if doc_id == FAIL_AFTER_TWO_CHUNKS as u64 && offset >= 2 * CHUNK as u64 {
                    body = rpc_error_body(req_msg_id, 400, "LOCATION_INVALID");
                } else {
                    let n = doc_id.saturating_sub(offset).min(limit) as usize;
                    body.extend_from_slice(&0x096a18d5u32.to_le_bytes());
                    body.extend_from_slice(&0i32.to_le_bytes());
                    body.extend_from_slice(&0i32.to_le_bytes());
                    let fill = (offset / CHUNK as u64) as u8;
                    tl_bytes(&mut body, n, |o| o.extend(std::iter::repeat_n(fill, n)));
                }
            }
            0xde7b673d | 0xb304a621 => {
                if verify {
                    verify_part(st, ctor, &req[..data_len]);
                } else {
                    let at = if ctor == 0xde7b673d { 20 } else { 16 };
                    let blen = if req[at] == 254 {
                        u32::from_le_bytes([req[at + 1], req[at + 2], req[at + 3], 0]) as usize
                    } else {
                        req[at] as usize
                    };
                    st.uploaded_bytes.fetch_add(blen as u64, Ordering::Relaxed);
                }
                body.extend_from_slice(&0x997275b5u32.to_le_bytes());
            }
            _ => body.extend_from_slice(&0x997275b5u32.to_le_bytes()),
        }
        server_frame(st, session_id, srv_msg_id, &body, &mut out);
        if s.write_all(&out).is_err() {
            break;
        }
    }
}

/// TCP connections currently open on the mock (counted at accept, before the first frame).
static OPEN_CONNECTIONS: AtomicI64 = AtomicI64::new(0);

/// TCP connections accepted since process start.
static TOTAL_ACCEPTED: AtomicU64 = AtomicU64::new(0);

pub fn total_accepted() -> u64 {
    TOTAL_ACCEPTED.load(Ordering::Relaxed)
}

pub fn open_connections() -> i64 {
    OPEN_CONNECTIONS.load(Ordering::Relaxed)
}

/// Starts the mock DC (once per process) and seeds the auth caches of all profiles for DC 1
/// and DC 4 so no handshake is needed.
pub fn start() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        TEST_DC_ADDR.set(l.local_addr().unwrap().to_string()).unwrap();
        std::thread::Builder::new()
            .name("mockdc-accept".into())
            .spawn(move || {
                MOCK_THREAD.with(|e| e.set(true));
                for s in l.incoming().flatten() {
                    OPEN_CONNECTIONS.fetch_add(1, Ordering::Relaxed);
                    TOTAL_ACCEPTED.fetch_add(1, Ordering::Relaxed);
                    let spawned = std::thread::Builder::new().name("mockdc-conn".into()).spawn(move || serve(s));
                    if spawned.is_err() {
                        OPEN_CONNECTIONS.fetch_sub(1, Ordering::Relaxed);
                    }
                }
            })
            .unwrap();
        for p in [Profile::Bench, Profile::Verify, Profile::Flaky] {
            let st = profile(p);
            for dc_id in [1, 4] {
                cache_auth(st.bot_token, CachedBotAuth { dc_id, auth_key: st.auth_key, server_salt: 1, time_offset: 0 });
            }
        }
    });
}

/// Builds a Bot API `file_id` (version 2 layout) for a document on `dc` with the given doc id.
pub fn encode_file_id(dc: i32, doc_id: i64) -> String {
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

/// Writes `size` bytes of [`pattern_byte`] data (by absolute offset) to `path`.
pub fn write_pattern_file(path: &std::path::Path, size: usize) {
    let mut f = std::fs::File::create(path).unwrap();
    let mut buf = Vec::with_capacity(1 << 20);
    let mut off = 0;
    while off < size {
        let n = (size - off).min(1 << 20);
        buf.clear();
        buf.extend((off..off + n).map(pattern_byte));
        f.write_all(&buf).unwrap();
        off += n;
    }
}
