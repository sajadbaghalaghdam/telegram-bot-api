//! Parallel `upload.saveFilePart` / `upload.saveBigFilePart` workers.
use bytes::BytesMut;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::auth::CachedBotAuth;
use super::connection::MtProtoConnection;
use super::tl::{write_bytes, write_int, write_long};

/// 512 KB (maximum MTProto file part size, evenly divides 1 MB)
pub const PART_SIZE: usize = 512 * 1024;

/// Number of parallel MTProto upload connections for big files (matching TDLib SessionMultiProxy)
const UPLOAD_WORKERS: usize = 8;

/// Number of pipelined 512 KB parts per worker batch
const PIPELINE_DEPTH: usize = 4;

/// Files above this size use `upload.saveBigFilePart` / `inputFileBig`.
pub const BIG_FILE_THRESHOLD: usize = 10 * 1024 * 1024;

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

/// Uploads every part of the file over up to `UPLOAD_WORKERS` connections sharing the bot's
/// `auth_key` (zero extra DH handshakes). Fails fast: on the first worker error the remaining
/// workers are aborted and awaited.
pub async fn upload_parts(plan: &PartUpload<'_>) -> Result<(), Error> {
    let total_parts = plan.total_parts();
    let num_workers = UPLOAD_WORKERS.min(total_parts.max(1));
    let next_part = Arc::new(AtomicUsize::new(0));

    let mut worker_set = tokio::task::JoinSet::new();
    for _worker_idx in 0..num_workers {
        let next_part = Arc::clone(&next_part);
        let path = plan.path.to_path_buf();
        let (file_size, file_id, shared_auth, api_id, is_big) =
            (plan.file_size, plan.file_id, plan.auth, plan.api_id, plan.is_big());

        worker_set.spawn(async move {
            let mut worker_conn = MtProtoConnection::connect_with_auth(shared_auth, api_id).await?;
            let mut file = File::open(&path).await?;
            let mut part_buf = vec![0u8; PART_SIZE];
            let mut batch_queries = Vec::with_capacity(PIPELINE_DEPTH);

            loop {
                batch_queries.clear();
                for _ in 0..PIPELINE_DEPTH {
                    let part_idx = next_part.fetch_add(1, Ordering::Relaxed);
                    if part_idx >= total_parts {
                        break;
                    }
                    let offset = part_idx * PART_SIZE;
                    let chunk_len = PART_SIZE.min(file_size - offset);

                    file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                    file.read_exact(&mut part_buf[..chunk_len]).await?;

                    let mut q = BytesMut::with_capacity(chunk_len + 32);
                    if is_big {
                        // upload.saveBigFilePart#de7b673d file_id:long file_part:int file_total_parts:int bytes:bytes = Bool
                        write_int(&mut q, 0xde7b673du32 as i32);
                        write_long(&mut q, file_id);
                        write_int(&mut q, part_idx as i32);
                        write_int(&mut q, total_parts as i32);
                        write_bytes(&mut q, &part_buf[..chunk_len]);
                    } else {
                        // upload.saveFilePart#b304a621 file_id:long file_part:int bytes:bytes = Bool
                        write_int(&mut q, 0xb304a621u32 as i32);
                        write_long(&mut q, file_id);
                        write_int(&mut q, part_idx as i32);
                        write_bytes(&mut q, &part_buf[..chunk_len]);
                    }
                    batch_queries.push(q);
                }

                if batch_queries.is_empty() {
                    break;
                }

                let mut retry_count = 0;
                let responses = loop {
                    match worker_conn.rpc_call_batch(&batch_queries).await {
                        Ok(resps) => break resps,
                        Err(e) => {
                            retry_count += 1;
                            if retry_count > 5 {
                                return Err(e);
                            }
                            tokio::time::sleep(Duration::from_millis(500 * retry_count)).await;
                            if let Ok(new_conn) = MtProtoConnection::connect_with_auth(shared_auth, api_id).await {
                                worker_conn = new_conn;
                            }
                        }
                    }
                };
                for resp in responses {
                    // boolTrue#997275b5
                    let bool_ctor = if resp.len() >= 4 {
                        u32::from_le_bytes(resp[0..4].try_into().unwrap())
                    } else {
                        0
                    };
                    if bool_ctor != 0x997275b5 {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("upload part returned non-true result (0x{:08x})", bool_ctor),
                        ));
                    }
                }
            }

            Ok::<(), Error>(())
        });
    }

    // Fail fast: on the first worker error, abort the remaining workers instead of letting
    // them keep uploading parts of a doomed file.
    let mut first_err: Option<Error> = None;
    while let Some(joined) = worker_set.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(e)) => first_err = Some(e),
            Err(e) if e.is_cancelled() => {}
            Err(e) => {
                first_err = Some(Error::new(ErrorKind::Other, format!("Upload worker panicked: {}", e)));
            }
        }
        if first_err.is_some() {
            tracing::error!("[upload] a worker failed, aborting remaining workers");
            worker_set.abort_all();
            while worker_set.join_next().await.is_some() {}
            break;
        }
    }
    first_err.map_or(Ok(()), Err)
}
