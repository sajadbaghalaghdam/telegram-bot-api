use bytes::BytesMut;
use std::io::{Error, ErrorKind};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::{self, OpenOptions};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};

use super::auth::CachedBotAuth;
use super::connection::MtProtoConnection;
use super::rpc_error::parse_migrate_dc;
use super::file_id::{decode_file_id, extension_for_file_type, DecodedFileLocation};
use super::tl::{read_bytes, read_int, write_bytes, write_int, write_long, write_string};

/// 512 KB download chunk size (maximum MTProto chunk size evenly dividing 1MB)
const CHUNK_SIZE: usize = 512 * 1024;

/// Number of parallel worker connections for high-speed file downloads
const NUM_DOWNLOAD_WORKERS: usize = 6;

/// Builds an upload.getFile query for MTProto 2.0
fn build_get_file_query(loc: &DecodedFileLocation, offset: i64, limit: i32) -> BytesMut {
    let mut q = BytesMut::with_capacity(128);
    // upload.getFile#be5335be flags:# precise:flags.0?true cdn_supported:flags.1?true location:InputFileLocation offset:long limit:int = upload.File
    write_int(&mut q, 0xbe5335beu32 as i32);
    write_int(&mut q, 1); // flags: precise = true (bit 0)

    if loc.file_type == 2 {
        // inputPhotoFileLocation#40181ffe id:long access_hash:long file_reference:bytes thumb_size:string
        write_int(&mut q, 0x40181ffeu32 as i32);
        write_long(&mut q, loc.doc_id);
        write_long(&mut q, loc.access_hash);
        write_bytes(&mut q, &loc.file_reference);
        write_string(&mut q, "");
    } else {
        // inputDocumentFileLocation#bad07584 id:long access_hash:long file_reference:bytes thumb_size:string
        write_int(&mut q, 0xbad07584u32 as i32);
        write_long(&mut q, loc.doc_id);
        write_long(&mut q, loc.access_hash);
        write_bytes(&mut q, &loc.file_reference);
        write_string(&mut q, "");
    }

    write_long(&mut q, offset);
    write_int(&mut q, limit);
    q
}

/// Parses an upload.file#096a18d5 response and returns the chunk bytes (borrowed, no copy).
fn parse_upload_file_chunk(resp: &[u8]) -> Result<&[u8], Error> {
    if resp.len() < 12 {
        return Ok(&[]);
    }
    let tl = |e: super::tl::TLError| Error::new(ErrorKind::InvalidData, e.to_string());
    let mut cur = resp;
    let ctor = read_int(&mut cur).map_err(tl)? as u32;

    if ctor == 0x096a18d5 {
        let _file_type = read_int(&mut cur).map_err(tl)?;
        let _mtime = read_int(&mut cur).map_err(tl)?;
        read_bytes(&mut cur).map_err(tl)
    } else {
        Err(Error::new(
            ErrorKind::InvalidData,
            format!("Unexpected upload.getFile response constructor: 0x{:08x}", ctor),
        ))
    }
}

/// True when an RPC/transport error message indicates a stale/dropped authorization that a
/// fresh auth key may fix: transport error -404 ("MTProto transport error -404",
/// "MTProto transport error: -404", "Transport error code: -404"), "early eof" or AUTH_KEY_*,
/// case-insensitively. Other transport codes (e.g. -429 flood, -444 invalid DC) must not
/// trigger a refresh.
fn is_auth_refresh_error(msg: &str) -> bool {
    let m = msg.to_ascii_lowercase();
    if m.contains("early eof") || m.contains("auth_key") {
        return true;
    }
    m.contains("transport error")
        && m.match_indices("-404").any(|(i, _)| !m[i + 4..].starts_with(|c: char| c.is_ascii_digit()))
}

/// Whether a failed getFile should be treated as "past end of file". The file size is not known
/// when downloading, so only `OFFSET_INVALID` counts; an empty or short chunk is the other EOF signal.
fn is_past_eof_error(msg: &str) -> bool {
    msg.contains("OFFSET_INVALID")
}

/// Decides the index of the final part from what the writer has seen: a short chunk is
/// authoritative; otherwise the lowest empty/EOF marker minus one (workers race past the end, so
/// several markers may arrive in any order).
fn final_part_index(short_part: Option<usize>, min_empty_marker: Option<usize>) -> Option<usize> {
    short_part.or_else(|| min_empty_marker.and_then(|m| m.checked_sub(1)))
}

/// Upper bound on the number of parts of one download (4 GiB), so a server that never signals
/// EOF cannot make us write forever.
const MAX_DOWNLOAD_PARTS: usize = 8192;

/// Re-attempts of one getFile (on a fresh connection) before the download fails.
const MAX_PART_ATTEMPTS: u32 = 5;

/// Chunk buffers in flight (being filled by workers, queued, or being written). This bounds the
/// download's memory to `CHUNK_BUFFERS x 512 KiB` plus one receive buffer per connection.
const CHUNK_BUFFERS: usize = NUM_DOWNLOAD_WORKERS * 2;

/// Deletes the `.part` file on drop unless the download was committed, so every error and
/// cancellation path (including a dropped future) cleans up.
struct PartFileGuard {
    path: PathBuf,
    committed: bool,
}

impl Drop for PartFileGuard {
    fn drop(&mut self) {
        if !self.committed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Aborts the wrapped task when dropped (a plain `JoinHandle` detaches instead).
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Recycles chunk buffers between the download workers and the disk writer.
struct ChunkPool {
    free: std::sync::Mutex<Vec<Vec<u8>>>,
    permits: Arc<Semaphore>,
}

impl ChunkPool {
    fn new() -> Arc<Self> {
        Arc::new(Self { free: std::sync::Mutex::new(Vec::new()), permits: Arc::new(Semaphore::new(CHUNK_BUFFERS)) })
    }

    fn take(&self) -> Vec<u8> {
        let recycled = self.free.lock().unwrap_or_else(|p| p.into_inner()).pop();
        recycled.unwrap_or_else(|| Vec::with_capacity(CHUNK_SIZE))
    }

    fn put(&self, buf: Vec<u8>) {
        self.free.lock().unwrap_or_else(|p| p.into_inner()).push(buf);
    }
}

/// A downloaded part on its way to the writer; the permit is released once it is written.
struct Chunk {
    part_idx: usize,
    /// Empty = "past EOF" marker.
    data: Vec<u8>,
    is_last: bool,
    _permit: OwnedSemaphorePermit,
}

/// Writes parts at their offsets and decides the final length once all workers are done.
async fn run_writer(
    file: Arc<std::fs::File>,
    mut rx: mpsc::Receiver<Chunk>,
    pool: Arc<ChunkPool>,
    first_chunk_len: u64,
) -> Result<u64, Error> {
    let mut received_parts = std::collections::HashSet::new();
    received_parts.insert(0usize); // part 0 is already written
    let mut short_part: Option<usize> = None;
    let mut min_empty_marker: Option<usize> = None;
    let mut max_written_bytes = first_chunk_len;
    let mut final_total_bytes = None;

    while let Some(chunk) = rx.recv().await {
        let Chunk { part_idx, data, is_last, _permit } = chunk;
        if data.is_empty() {
            // Empty EOF marker: part_idx is past EOF, so part_idx - 1 was the final part.
            // Several workers may report markers out of order; keep the lowest.
            min_empty_marker = Some(min_empty_marker.map_or(part_idx, |m| m.min(part_idx)));
            continue;
        }

        let offset = (part_idx * CHUNK_SIZE) as u64;
        let end_offset = offset + data.len() as u64;
        let file = Arc::clone(&file);
        let data = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, Error> {
            file.write_all_at(&data, offset)?;
            Ok(data)
        })
        .await
        .map_err(Error::other)??;
        pool.put(data);
        received_parts.insert(part_idx);

        max_written_bytes = max_written_bytes.max(end_offset);
        if is_last {
            short_part = Some(part_idx);
            final_total_bytes = Some(end_offset);
        }
    }

    let Some(last_idx) = final_part_index(short_part, min_empty_marker) else {
        return Err(Error::new(
            ErrorKind::UnexpectedEof,
            "Incomplete download: connection ended without reaching EOF",
        ));
    };
    // Contiguity verification: assert that every part from 0 to last_idx was received without gaps
    for part in 0..=last_idx {
        if !received_parts.contains(&part) {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Incomplete download: chunk {} missing from range 0..={}", part, last_idx),
            ));
        }
    }
    let total_len = final_total_bytes.unwrap_or(max_written_bytes);
    tokio::task::spawn_blocking(move || file.set_len(total_len)).await.map_err(Error::other)??;
    Ok(total_len)
}

/// Shared state of the download workers.
struct WorkerCtx {
    loc: DecodedFileLocation,
    auth: CachedBotAuth,
    api_id: i32,
    next_part: AtomicUsize,
    eof_reached: AtomicBool,
    pool: Arc<ChunkPool>,
    tx: mpsc::Sender<Chunk>,
}

/// Outcome of fetching one part.
enum PartFetch {
    /// Data copied into the pooled buffer.
    Data,
    /// The part is at or past the end of the file.
    Eof,
}

/// Fetches part `part_idx` into `buf` (cleared first), retrying on a fresh connection.
async fn fetch_part(
    ctx: &WorkerCtx,
    conn: &mut MtProtoConnection,
    part_idx: usize,
    buf: &mut Vec<u8>,
) -> Result<PartFetch, Error> {
    let q = build_get_file_query(&ctx.loc, (part_idx * CHUNK_SIZE) as i64, CHUNK_SIZE as i32);
    let mut attempt = 0u32;
    loop {
        let res = conn
            .rpc_call_with(&q, |body| {
                let chunk = parse_upload_file_chunk(body)?;
                buf.clear();
                buf.extend_from_slice(chunk);
                Ok(())
            })
            .await;
        match res {
            Ok(()) if buf.is_empty() => return Ok(PartFetch::Eof),
            Ok(()) => return Ok(PartFetch::Data),
            Err(e) => {
                let msg = e.to_string();
                tracing::warn!("[dl] part {} attempt {} error: {}", part_idx, attempt, msg);
                if is_past_eof_error(&msg) {
                    return Ok(PartFetch::Eof);
                }
                attempt += 1;
                // Flood waits up to 3 s were already slept through inside rpc_call_with.
                if MtProtoConnection::parse_flood_wait(&msg).is_some()
                    || msg.contains("LOCATION_INVALID")
                    || msg.contains("FILE_REFERENCE_EXPIRED")
                    || attempt >= MAX_PART_ATTEMPTS
                {
                    return Err(e);
                }
                tokio::time::sleep(Duration::from_millis(50 * (1 << (attempt - 1)))).await;
                if let Ok(new_conn) = MtProtoConnection::connect_with_auth(ctx.auth, ctx.api_id).await {
                    *conn = new_conn;
                }
            }
        }
    }
}

/// One download connection: claims part indexes from the shared counter until EOF.
async fn run_worker(ctx: Arc<WorkerCtx>, first_conn: Option<MtProtoConnection>) -> Result<(), Error> {
    let mut conn = match first_conn {
        Some(c) => c,
        None => MtProtoConnection::connect_with_auth(ctx.auth, ctx.api_id).await?,
    };
    loop {
        if ctx.eof_reached.load(Ordering::Relaxed) {
            return Ok(());
        }
        let part_idx = ctx.next_part.fetch_add(1, Ordering::Relaxed);
        if part_idx >= MAX_DOWNLOAD_PARTS {
            return Err(Error::new(ErrorKind::InvalidData, "Download exceeds the maximum supported file size"));
        }
        // Back-pressure: a chunk buffer is needed before asking the server for data.
        let Ok(permit) = Arc::clone(&ctx.pool.permits).acquire_owned().await else {
            return Ok(());
        };
        let mut buf = ctx.pool.take();
        match fetch_part(&ctx, &mut conn, part_idx, &mut buf).await? {
            PartFetch::Eof => {
                ctx.eof_reached.store(true, Ordering::Relaxed);
                ctx.pool.put(buf);
                let marker = Chunk { part_idx, data: Vec::new(), is_last: true, _permit: permit };
                let _ = ctx.tx.send(marker).await;
                return Ok(());
            }
            PartFetch::Data => {
                let is_last = buf.len() < CHUNK_SIZE;
                if is_last {
                    ctx.eof_reached.store(true, Ordering::Relaxed);
                }
                if ctx.tx.send(Chunk { part_idx, data: buf, is_last, _permit: permit }).await.is_err() {
                    return Ok(()); // writer is gone; its error is reported by the caller
                }
                if is_last {
                    return Ok(());
                }
            }
        }
    }
}

/// Downloads a large file (up to 2000 MB / 2 GB) directly from Telegram MTProto 2.0 servers
/// using multi-worker parallel chunk streaming with constant O(1) RAM.
pub async fn download_large_file_via_mtproto(
    api_id: i32,
    api_hash: &str,
    bot_token: &str,
    file_id_str: &str,
    destination_dir: &Path,
) -> Result<(PathBuf, u64), Error> {
    let loc = decode_file_id(file_id_str).ok_or_else(|| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("Failed to decode MTProto location from file_id: {}", file_id_str),
        )
    })?;

    let ext = extension_for_file_type(loc.file_type);
    let subfolder = match loc.file_type {
        2 => "photos",
        3 => "voice",
        4 => "videos",
        8 => "stickers",
        9 => "music",
        10 => "animations",
        12 => "video_notes",
        _ => "documents",
    };

    let target_dir = destination_dir.join(subfolder);
    fs::create_dir_all(&target_dir).await?;

    let file_name = format!("file_{}.{}", loc.doc_id, ext);
    let target_path = target_dir.join(&file_name);
    // Unique per call: two concurrent downloads of the same file must not truncate or unlink each
    // other's partial file (the first to finish renames its own over the target).
    let part_path = target_dir.join(format!(".{}.{:016x}.part", file_name, rand::random::<u64>()));

    // If already fully downloaded and non-empty, return cached disk file
    if let Ok(meta) = fs::metadata(&target_path).await
        && meta.len() > 0
    {
        return Ok((target_path, meta.len()));
    }

    // 1. Connect and authorize bot to the target DC where the file resides
    tracing::info!("[dl] start file_type={} dc={} doc_id={}", loc.file_type, loc.dc_id, loc.doc_id);
    let t_start = std::time::Instant::now();
    let mut main_conn = MtProtoConnection::connect_and_authorize(loc.dc_id, api_id, api_hash, bot_token).await?;
    tracing::info!("[dl] authorized on dc={} in {:?}", main_conn.dc_id, t_start.elapsed());

    // 2. Fetch the first 512 KB chunk (part 0, offset 0) on the primary connection.
    // This immediately handles any DC migration (FILE_MIGRATE_X) and verifies authentication.
    let q0 = build_get_file_query(&loc, 0, CHUNK_SIZE as i32);
    let mut refreshed_auth = false;
    let mut migrations = 0u32;
    let mut flood_retries = 0u32;
    let chunk0 = loop {
        match main_conn.rpc_call_with(&q0, |body| parse_upload_file_chunk(body).map(<[u8]>::to_vec)).await {
            Ok(c) => break c,
            Err(e) => {
                let err_msg = e.to_string();
                tracing::warn!("[dl] chunk0 error on dc={}: {}", main_conn.dc_id, err_msg);
                if let Some(target_dc) = parse_migrate_dc(&err_msg) {
                    migrations += 1;
                    if migrations > 3 {
                        return Err(Error::other(format!("Too many DC migrations: {}", err_msg)));
                    }
                    main_conn = MtProtoConnection::connect_and_authorize(target_dc, api_id, api_hash, bot_token).await?;
                    continue;
                }
                if let Some(wait_secs) = MtProtoConnection::parse_flood_wait(&err_msg) {
                    flood_retries += 1;
                    if wait_secs <= 3 && flood_retries <= MAX_PART_ATTEMPTS {
                        tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                        continue;
                    }
                    tracing::warn!("[dl] dc={} flood wait {}s -> fast-fail (429)", main_conn.dc_id, wait_secs);
                    return Err(e);
                }
                // If cached auth failed or dropped connection, refresh authorization once
                if !refreshed_auth && is_auth_refresh_error(&err_msg) {
                    refreshed_auth = true;
                    // Refresh on the DC we are currently on (may differ from loc.dc_id after a migration)
                    let cur_dc = main_conn.dc_id;
                    tracing::info!("[dl] refreshing stale MTProto authorization on dc={}", cur_dc);
                    main_conn = MtProtoConnection::connect_and_authorize_fresh(cur_dc, api_id, api_hash, bot_token).await?;
                    continue;
                }
                return Err(e);
            }
        }
    };

    if chunk0.is_empty() {
        // Create an empty file at target destination
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&target_path)
            .await?;
        return Ok((target_path, 0));
    }

    // From here on the `.part` file is removed on every exit path unless committed.
    let mut part_guard = PartFileGuard { path: part_path.clone(), committed: false };
    let out_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&part_path)
        .await?
        .into_std()
        .await;
    let out_file = Arc::new(out_file);
    {
        let (file, first) = (Arc::clone(&out_file), chunk0.clone());
        tokio::task::spawn_blocking(move || file.write_all_at(&first, 0)).await.map_err(Error::other)??;
    }

    // If file is smaller than 512 KB, download is completely finished in 1 round trip!
    if chunk0.len() < CHUNK_SIZE {
        drop(out_file);
        fs::rename(&part_path, &target_path).await?;
        part_guard.committed = true;
        return Ok((target_path, chunk0.len() as u64));
    }

    // 3. For files > 512 KB, share the authenticated auth_key with parallel worker connections.
    let shared_auth = main_conn.cached_auth();

    // 4. Multi-worker parallel download pipeline: workers stream 512 KB chunks into pooled
    // buffers and hand them to one disk writer; at most CHUNK_BUFFERS chunks exist at a time.
    let pool = ChunkPool::new();
    let (tx, rx) = mpsc::channel::<Chunk>(CHUNK_BUFFERS);
    let writer = AbortOnDrop(tokio::spawn(run_writer(
        Arc::clone(&out_file),
        rx,
        Arc::clone(&pool),
        chunk0.len() as u64,
    )));
    drop(chunk0);
    drop(out_file);

    let ctx = Arc::new(WorkerCtx {
        loc,
        auth: shared_auth,
        api_id,
        next_part: AtomicUsize::new(1),
        eof_reached: AtomicBool::new(false),
        pool,
        tx,
    });
    let mut worker_set = tokio::task::JoinSet::new();
    worker_set.spawn(run_worker(Arc::clone(&ctx), Some(main_conn)));
    for _ in 1..NUM_DOWNLOAD_WORKERS {
        worker_set.spawn(run_worker(Arc::clone(&ctx), None));
    }
    // The workers hold the only senders from here on, so the writer ends when they finish.
    drop(ctx);

    // Fail fast: on the first worker error, abort the remaining workers immediately.
    let mut first_err = None;
    while let Some(joined) = worker_set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_err = Some(e);
            }
            Err(e) if e.is_cancelled() => {}
            Err(e) => {
                first_err = Some(Error::other(format!("Worker panicked: {e}")));
            }
        }
        if first_err.is_some() {
            worker_set.abort_all();
            // Wait for the aborted workers (cancellation errors are expected) before cleanup.
            while worker_set.join_next().await.is_some() {}
            break;
        }
    }

    if let Some(e) = first_err {
        // `writer` (AbortOnDrop) and `part_guard` clean up on return.
        return Err(e);
    }

    let mut writer = writer;
    let total_downloaded = match (&mut writer.0).await {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => return Err(e),
        Err(e) => return Err(Error::other(format!("Writer panicked: {e}"))),
    };

    // Atomically move verified complete file to target destination
    fs::rename(&part_path, &target_path).await?;
    part_guard.committed = true;

    let secs = t_start.elapsed().as_secs_f64().max(0.001);
    tracing::info!(
        "[dl] done {} bytes in {:.2}s ({:.2} MB/s)",
        total_downloaded,
        secs,
        total_downloaded as f64 / 1_048_576.0 / secs
    );

    Ok((target_path, total_downloaded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_refresh_matches_real_error_texts() {
        assert!(is_auth_refresh_error("MTProto transport error -404"));
        assert!(is_auth_refresh_error("Transport error code: -404"));
        assert!(is_auth_refresh_error("early eof"));
        assert!(is_auth_refresh_error("AUTH_KEY_UNREGISTERED"));
        assert!(is_auth_refresh_error("MTProto transport error: -404"));
        assert!(!is_auth_refresh_error("FLOOD_WAIT_5"));
        assert!(!is_auth_refresh_error("MTProto transport error -429"));
        assert!(!is_auth_refresh_error("MTProto transport error: -444"));
        assert!(!is_auth_refresh_error("Transport error code: -4040"));
        assert!(!is_auth_refresh_error("transport error"));
    }

    #[test]
    fn past_eof_detection() {
        assert!(is_past_eof_error("RPC error OFFSET_INVALID"));
        assert!(!is_past_eof_error("BAD_REQUEST"));
    }

    #[test]
    fn final_part_from_short_chunk_or_lowest_marker() {
        // Exact multiple of the chunk size: only empty markers, arriving out of order.
        assert_eq!(final_part_index(None, Some(5)), Some(4));
        assert_eq!(final_part_index(Some(4), Some(7)), Some(4));
        assert_eq!(final_part_index(None, None), None);
        assert_eq!(final_part_index(None, Some(0)), None);
    }
}
