//! Parallel `upload.saveFilePart` / `upload.saveBigFilePart` workers.
//!
//! Memory is bounded by construction: each of the (at most) `UPLOAD_WORKERS` workers owns
//! `PIPELINE_DEPTH` reusable part buffers of about `PART_SIZE` bytes plus the connection's
//! send buffer, so peak usage is `workers x (PIPELINE_DEPTH + 1) x 512 KiB`, independent of the
//! file size. A file part travels: `pread` straight into the TL query buffer -> frame buffer
//! (copy + in-place encryption) -> socket.
use std::fs::File;
use std::io::Error;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::auth::CachedBotAuth;
use super::connection::MtProtoConnection;
use super::upload_progress;
use super::tl::{bytes_padding, write_bytes_header, write_int, write_long};

/// 512 KB (maximum MTProto file part size, evenly divides 1 MB)
pub const PART_SIZE: usize = 512 * 1024;

/// Number of parallel MTProto upload connections for big files (matching TDLib SessionMultiProxy)
const UPLOAD_WORKERS: usize = 8;

/// Number of pipelined 512 KB parts per worker batch
const PIPELINE_DEPTH: usize = 4;

/// Re-attempts of a failed batch (on a fresh connection) before the upload fails.
const MAX_BATCH_RETRIES: u64 = 5;

/// Files above this size use `upload.saveBigFilePart` / `inputFileBig`.
pub const BIG_FILE_THRESHOLD: usize = 10 * 1024 * 1024;

/// boolTrue#997275b5
const BOOL_TRUE: u32 = 0x997275b5;

/// Number of `PART_SIZE` parts needed for `file_size` bytes.
pub fn part_count(file_size: usize) -> usize {
    file_size.div_ceil(PART_SIZE)
}

/// Everything the part workers need to know about the transfer.
pub struct PartUpload<'a> {
    pub path: &'a Path,
    pub file_size: usize,
    pub file_id: i64,
    pub auth: CachedBotAuth,
    pub api_id: i32,
}

impl PartUpload<'_> {
    fn total_parts(&self) -> usize {
        part_count(self.file_size)
    }

    pub fn is_big(&self) -> bool {
        self.file_size > BIG_FILE_THRESHOLD
    }
}

/// Immutable per-transfer data shared by all workers.
struct Shared {
    file: File,
    file_size: usize,
    file_id: i64,
    total_parts: usize,
    is_big: bool,
    auth: CachedBotAuth,
    api_id: i32,
    next_part: AtomicUsize,
    /// Bytes stored so far, for `getUploadProgress`.
    progress: Arc<upload_progress::Entry>,
}

/// Writes the TL query for part `part_idx` into `q` (cleared first) and returns it. The file
/// data is read directly into the query buffer.
async fn build_part_query(shared: &Arc<Shared>, mut q: Vec<u8>, part_idx: usize) -> Result<Vec<u8>, Error> {
    let offset = part_idx * PART_SIZE;
    let chunk_len = PART_SIZE.min(shared.file_size - offset);

    q.clear();
    if shared.is_big {
        // upload.saveBigFilePart#de7b673d file_id:long file_part:int file_total_parts:int bytes:bytes = Bool
        write_int(&mut q, 0xde7b673du32 as i32);
        write_long(&mut q, shared.file_id);
        write_int(&mut q, part_idx as i32);
        write_int(&mut q, shared.total_parts as i32);
    } else {
        // upload.saveFilePart#b304a621 file_id:long file_part:int bytes:bytes = Bool
        write_int(&mut q, 0xb304a621u32 as i32);
        write_long(&mut q, shared.file_id);
        write_int(&mut q, part_idx as i32);
    }
    let header_len = write_bytes_header(&mut q, chunk_len);
    let data_at = q.len();
    q.resize(data_at + chunk_len, 0);

    let shared = Arc::clone(shared);
    let mut q = tokio::task::spawn_blocking(move || -> Result<Vec<u8>, Error> {
        super::file_at::read_exact_at(&shared.file, &mut q[data_at..], offset as u64)?;
        Ok(q)
    })
    .await
    .map_err(Error::other)??;

    q.resize(q.len() + bytes_padding(header_len, chunk_len), 0);
    Ok(q)
}

fn expect_bool_true(body: &[u8]) -> Result<(), Error> {
    let ctor = body.first_chunk::<4>().map(|b| u32::from_le_bytes(*b)).unwrap_or(0);
    if ctor != BOOL_TRUE {
        return Err(Error::other(format!("upload part returned non-true result (0x{:08x})", ctor)));
    }
    Ok(())
}

/// One upload connection: pulls part indexes from the shared counter, `PIPELINE_DEPTH` at a time.
async fn run_worker(shared: Arc<Shared>, mut conn: MtProtoConnection) -> Result<MtProtoConnection, Error> {
    let mut slots: Vec<Vec<u8>> = (0..PIPELINE_DEPTH).map(|_| Vec::new()).collect();
    let mut ready: Vec<Vec<u8>> = Vec::with_capacity(PIPELINE_DEPTH);

    loop {
        // Fill the pipeline: each slot buffer is reused for every batch of this worker.
        ready.clear();
        let mut batch_bytes = 0u64;
        for slot in slots.iter_mut() {
            let part_idx = shared.next_part.fetch_add(1, Ordering::Relaxed);
            if part_idx >= shared.total_parts {
                break;
            }
            let q = build_part_query(&shared, std::mem::take(slot), part_idx).await?;
            batch_bytes += PART_SIZE.min(shared.file_size - part_idx * PART_SIZE) as u64;
            ready.push(q);
        }
        if ready.is_empty() {
            return Ok(conn);
        }

        let mut retry_count = 0u64;
        loop {
            match conn.rpc_call_batch_with(&ready, |_, body| expect_bool_true(body)).await {
                Ok(()) => {
                    shared.progress.add(batch_bytes);
                    break;
                }
                Err(e) => {
                    retry_count += 1;
                    if retry_count > MAX_BATCH_RETRIES {
                        return Err(e);
                    }
                    tracing::warn!("[upload] batch failed (attempt {retry_count}): {e}");
                    tokio::time::sleep(Duration::from_millis(500 * retry_count)).await;
                    match MtProtoConnection::connect_with_auth(shared.auth, shared.api_id).await {
                        Ok(new_conn) => conn = new_conn,
                        Err(e) => tracing::warn!("[upload] reconnect failed: {e}"),
                    }
                }
            }
        }

        // Hand the buffers back to their slots for the next batch.
        for (slot, q) in slots.iter_mut().zip(ready.drain(..)) {
            *slot = q;
        }
    }
}

/// Uploads every part of the file over up to `UPLOAD_WORKERS` connections sharing the bot's
/// `auth_key` (zero extra DH handshakes). `first_conn` (already authorized) serves as the first
/// worker, so a single-part upload opens no extra connection. Fails fast: on the first worker
/// error the remaining workers are aborted and awaited. Returns one still-open connection for
/// the final `messages.sendMedia`.
pub async fn upload_parts(plan: &PartUpload<'_>, first_conn: MtProtoConnection) -> Result<MtProtoConnection, Error> {
    let total_parts = plan.total_parts();
    let file = tokio::fs::File::open(plan.path).await?.into_std().await;
    // Listed for `getUploadProgress` until this returns (done, failed or dropped).
    let tracking = upload_progress::start(plan.path, plan.file_size as u64);
    let shared = Arc::new(Shared {
        file,
        file_size: plan.file_size,
        file_id: plan.file_id,
        total_parts,
        is_big: plan.is_big(),
        auth: plan.auth,
        api_id: plan.api_id,
        next_part: AtomicUsize::new(0),
        progress: tracking.entry(),
    });
    let num_workers = UPLOAD_WORKERS.min(total_parts.max(1));

    let mut worker_set = tokio::task::JoinSet::new();
    worker_set.spawn(run_worker(Arc::clone(&shared), first_conn));
    for _ in 1..num_workers {
        let shared = Arc::clone(&shared);
        worker_set.spawn(async move {
            let conn = MtProtoConnection::connect_with_auth(shared.auth, shared.api_id).await?;
            run_worker(shared, conn).await
        });
    }

    // Fail fast: on the first worker error, abort the remaining workers instead of letting
    // them keep uploading parts of a doomed file.
    let mut first_err: Option<Error> = None;
    let mut spare_conn = None;
    while let Some(joined) = worker_set.join_next().await {
        match joined {
            Ok(Ok(conn)) => {
                spare_conn.get_or_insert(conn);
            }
            Ok(Err(e)) => first_err = Some(e),
            Err(e) if e.is_cancelled() => {}
            Err(e) => {
                first_err = Some(Error::other(format!("Upload worker panicked: {}", e)));
            }
        }
        if first_err.is_some() {
            tracing::error!("[upload] a worker failed, aborting remaining workers");
            worker_set.abort_all();
            while worker_set.join_next().await.is_some() {}
            break;
        }
    }
    match (first_err, spare_conn) {
        (Some(e), _) => Err(e),
        (None, Some(conn)) => Ok(conn),
        (None, None) => Err(Error::other("no upload worker finished")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_true_check() {
        assert!(expect_bool_true(&BOOL_TRUE.to_le_bytes()).is_ok());
        assert!(expect_bool_true(&0xbc799737u32.to_le_bytes()).is_err()); // boolFalse
        assert!(expect_bool_true(&[]).is_err());
    }

    #[tokio::test]
    async fn part_query_layout_matches_tl_bytes_encoding() {
        let dir = std::env::temp_dir().join(format!("mtproto_parts_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("f.bin");
        let size = PART_SIZE + 5; // second part is 5 bytes: needs padding
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        std::fs::write(&path, &data).unwrap();

        let shared = Arc::new(Shared {
            file: File::open(&path).unwrap(),
            file_size: size,
            file_id: 99,
            total_parts: 2,
            is_big: true,
            auth: CachedBotAuth { dc_id: 1, auth_key: [0; 256], server_salt: 0, time_offset: 0 },
            api_id: 1,
            next_part: AtomicUsize::new(0),
            progress: upload_progress::start(&path, size as u64).entry(),
        });
        for (idx, expect) in [(0usize, &data[..PART_SIZE]), (1, &data[PART_SIZE..])] {
            let q = build_part_query(&shared, vec![1, 2, 3], idx).await.unwrap(); // stale content is cleared
            assert_eq!(q.len() % 4, 0);
            let mut cur = q.as_slice();
            assert_eq!(super::super::tl::read_int(&mut cur).unwrap() as u32, 0xde7b673d);
            assert_eq!(super::super::tl::read_long(&mut cur).unwrap(), 99);
            assert_eq!(super::super::tl::read_int(&mut cur).unwrap(), idx as i32);
            assert_eq!(super::super::tl::read_int(&mut cur).unwrap(), 2);
            assert_eq!(super::super::tl::read_bytes(&mut cur).unwrap(), expect);
            assert!(cur.is_empty());
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
