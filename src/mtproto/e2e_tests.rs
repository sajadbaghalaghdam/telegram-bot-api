//! End-to-end correctness tests of the upload/download pipelines against the in-process mock DC
//! (`mock_dc`): the mock decrypts every client frame with an independent reference IGE, checks
//! `msg_key`, padding and the exact file content of each uploaded part, and encrypts download
//! chunks the same way. Also pins resource hygiene: no leaked sockets, tasks or `.part` files on
//! cancellation and error paths.
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::downloader::download_large_file_via_mtproto;
use super::media::{MediaAttrs, MediaKind};
use super::mock_dc::{self, encode_file_id, profile, write_pattern_file, Profile, ProfileState, CHUNK, FAIL_AFTER_TWO_CHUNKS};
use super::uploader::upload_and_send_large_file_typed;

fn scratch_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("mtproto_e2e_{}_{}", std::process::id(), name));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn upload(st: &ProfileState, path: &Path, attrs: MediaAttrs) -> std::io::Result<super::uploader::UploadedMediaResult> {
    upload_and_send_large_file_typed(1, "hash", st.bot_token, "-12345", None, path, "cap", None, attrs).await
}

fn assert_clean(st: &ProfileState) {
    let errors = st.errors.lock().unwrap();
    assert!(errors.is_empty(), "mock detected protocol violations: {:?}", *errors);
}

/// Waits until every connection the client opened has been closed (and fully processed by the
/// mock), proving that no sockets are leaked.
async fn wait_for_no_connections() {
    for _ in 0..100 {
        if mock_dc::open_connections() == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("{} mock connections still open (leaked sockets)", mock_dc::open_connections());
}

/// Starts a test that inspects per-profile counters: serialised, with a clean slate.
async fn exclusive(p: Profile) -> (tokio::sync::MutexGuard<'static, ()>, &'static ProfileState) {
    mock_dc::start();
    let guard = profile_lock().lock().await;
    wait_for_no_connections().await;
    let st = profile(p);
    st.parts.lock().unwrap().clear();
    st.errors.lock().unwrap().clear();
    for c in [&st.uploaded_bytes, &st.salted_requests, &st.floods_sent] {
        c.store(0, Ordering::Relaxed);
    }
    (guard, st)
}

/// Connections the mock accepted since `since` (a [`mock_dc::total_accepted`] snapshot).
fn connections_since(since: u64) -> u64 {
    mock_dc::total_accepted() - since
}

/// Serialises tests that look at per-profile counters.
fn profile_lock() -> &'static tokio::sync::Mutex<()> {
    static L: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    L.get_or_init(|| tokio::sync::Mutex::new(()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn big_upload_sends_every_part_with_correct_content_and_reuses_connections() {
    let (_g, st) = exclusive(Profile::Verify).await;
    let dir = scratch_dir("big_upload");
    let path = dir.join("big.bin");
    // 20 full parts + 5 bytes: a big file (saveBigFilePart) with a short, padded last part.
    let size = 10 * 1024 * 1024 + 5;
    write_pattern_file(&path, size);
    let parts = size.div_ceil(CHUNK);
    let conns0 = mock_dc::total_accepted();

    let res = upload(st, &path, MediaAttrs { kind: MediaKind::Video, duration: 3, width: 4, height: 5, supports_streaming: true })
        .await
        .unwrap();
    assert_eq!((res.message_id, res.doc_id, res.access_hash, res.file_size), (777, 4242, 99, size as i64));
    assert_eq!(st.parts.lock().unwrap().len(), parts);
    assert_eq!(st.uploaded_bytes.load(Ordering::Relaxed), size as u64);
    assert_clean(st);
    // 8 workers (the control connection is reused as the first one, another is reused for sendMedia).
    assert_eq!(connections_since(conns0), 8);
    wait_for_no_connections().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn small_upload_uses_save_file_part_and_single_connection() {
    let (_g, st) = exclusive(Profile::Verify).await;
    let dir = scratch_dir("small_upload");
    let path = dir.join("small.bin");
    let size = CHUNK + 1000; // 2 parts, not big
    write_pattern_file(&path, size);
    let conns0 = mock_dc::total_accepted();
    let res = upload(st, &path, MediaAttrs::default()).await.unwrap();
    assert_eq!(res.message_id, 777);
    assert_eq!(st.parts.lock().unwrap().len(), 2);
    assert_clean(st);
    assert_eq!(connections_since(conns0), 2); // 2 parts -> 2 workers
    wait_for_no_connections().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_survives_bad_server_salt_and_flood_wait() {
    let (_g, st) = exclusive(Profile::Flaky).await;
    let dir = scratch_dir("flaky_upload");
    let path = dir.join("f.bin");
    let size = 6 * CHUNK + 17;
    write_pattern_file(&path, size);
    let res = upload(st, &path, MediaAttrs::default()).await.unwrap();
    assert_eq!(res.message_id, 777);
    assert_eq!(st.parts.lock().unwrap().len(), 7);
    assert!(st.salted_requests.load(Ordering::Relaxed) > 0, "client never adopted the new salt");
    assert!(st.floods_sent.load(Ordering::Relaxed) > 0, "flood wait path not exercised");
    assert_clean(st);
    wait_for_no_connections().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upload_of_missing_or_empty_file_fails_cleanly() {
    mock_dc::start();
    let st = profile(Profile::Verify);
    let dir = scratch_dir("bad_upload");
    assert!(upload(st, &dir.join("nope.bin"), MediaAttrs::default()).await.is_err());
    let empty = dir.join("empty.bin");
    std::fs::write(&empty, b"").unwrap();
    assert!(upload(st, &empty, MediaAttrs::default()).await.is_err());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_upload_closes_all_connections() {
    let (_g, _st) = exclusive(Profile::Verify).await;
    let dir = scratch_dir("cancel_upload");
    let path = dir.join("c.bin");
    write_pattern_file(&path, 48 * 1024 * 1024);
    let task = tokio::spawn({
        let path = path.clone();
        async move { upload(profile(Profile::Verify), &path, MediaAttrs::default()).await }
    });
    // Wait until workers are connected, then drop the whole upload.
    for _ in 0..100 {
        if mock_dc::open_connections() >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    task.abort();
    let _ = task.await;
    wait_for_no_connections().await;
    std::fs::remove_dir_all(&dir).ok();
}

async fn download(st: &ProfileState, doc_id: i64, dir: &Path) -> std::io::Result<(PathBuf, u64)> {
    download_large_file_via_mtproto(1, "hash", st.bot_token, &encode_file_id(4, doc_id), dir).await
}

fn assert_chunks(path: &Path, size: usize) {
    let data = std::fs::read(path).unwrap();
    assert_eq!(data.len(), size);
    for (i, c) in data.chunks(CHUNK).enumerate() {
        assert!(c.iter().all(|&b| b == i as u8), "chunk {i} corrupted");
    }
}

fn no_part_files(dir: &Path) -> bool {
    walk(dir).iter().all(|p| !p.to_string_lossy().ends_with(".part"))
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(walk(&p));
            } else {
                out.push(p);
            }
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_round_trips_for_edge_sizes() {
    let (_g, st) = exclusive(Profile::Verify).await;
    let dir = scratch_dir("download");
    // < 1 chunk, exactly 1 chunk, 1 chunk + 1 byte, exact multiple, many chunks with a short tail.
    for size in [1usize, CHUNK - 1, CHUNK, CHUNK + 1, 4 * CHUNK, 37 * CHUNK + 4321] {
        let (path, total) = download(st, size as i64, &dir).await.unwrap();
        assert_eq!(total, size as u64, "size {size}");
        assert_chunks(&path, size);
    }
    assert!(no_part_files(&dir));
    assert_clean(st);
    wait_for_no_connections().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_download_removes_part_file_and_closes_connections() {
    let (_g, st) = exclusive(Profile::Verify).await;
    let dir = scratch_dir("download_fail");
    let res = download(st, FAIL_AFTER_TWO_CHUNKS, &dir).await;
    let err = res.expect_err("later chunks answer LOCATION_INVALID");
    assert!(err.to_string().contains("LOCATION_INVALID"), "{err}");
    assert!(no_part_files(&dir), "`.part` left behind: {:?}", walk(&dir));
    wait_for_no_connections().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn download_with_unknown_bot_fails_instead_of_hanging() {
    let (_g, _st) = exclusive(Profile::Verify).await;
    let dir = scratch_dir("download_noauth");
    // No cached auth for this token: the client attempts a DH handshake, which the mock refuses.
    let res = download_large_file_via_mtproto(1, "hash", "unknown-token", &encode_file_id(4, 9), &dir).await;
    assert!(res.is_err());
    assert!(no_part_files(&dir));
    wait_for_no_connections().await;
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelled_download_cleans_part_file_and_connections() {
    let (_g, _st) = exclusive(Profile::Verify).await;
    let dir = scratch_dir("download_cancel");
    let task = tokio::spawn({
        let dir = dir.clone();
        async move { download(profile(Profile::Verify), 400 * CHUNK as i64, &dir).await }
    });
    let mut part_seen = false;
    for _ in 0..200 {
        part_seen = !no_part_files(&dir);
        if mock_dc::open_connections() >= 3 && part_seen {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(part_seen, "download never created its .part file; the test would prove nothing");
    task.abort();
    let _ = task.await;
    wait_for_no_connections().await;
    assert!(no_part_files(&dir), "`.part` file left behind after cancellation: {:?}", walk(&dir));
    std::fs::remove_dir_all(&dir).ok();
}
