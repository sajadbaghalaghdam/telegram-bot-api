use bytes::BytesMut;
use std::io::{Error, ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::connection::MtProtoConnection;
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

/// Parses an upload.file#096a18d5 response chunk
fn parse_upload_file_chunk(resp: &[u8]) -> Result<Vec<u8>, Error> {
    if resp.len() < 12 {
        return Ok(Vec::new());
    }
    let mut cur = resp;
    let ctor = read_int(&mut cur).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))? as u32;

    if ctor == 0x096a18d5 {
        let _file_type = read_int(&mut cur).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        let _mtime = read_int(&mut cur).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        let chunk_data = read_bytes(&mut cur).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        Ok(chunk_data.to_vec())
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
    let part_path = target_dir.join(format!(".{}.part", file_name));

    // If already fully downloaded and non-empty, return cached disk file
    if target_path.exists() {
        if let Ok(meta) = fs::metadata(&target_path).await {
            if meta.len() > 0 {
                return Ok((target_path, meta.len()));
            }
        }
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
    let resp0 = loop {
        match main_conn.rpc_call(&q0).await {
            Ok(r) => break r,
            Err(e) => {
                let err_msg = e.to_string();
                tracing::warn!("[dl] chunk0 error on dc={}: {}", main_conn.dc_id, err_msg);
                if let Some(target_dc) = MtProtoConnection::parse_migrate_dc(&err_msg) {
                    migrations += 1;
                    if migrations > 3 {
                        return Err(Error::new(ErrorKind::Other, format!("Too many DC migrations: {}", err_msg)));
                    }
                    main_conn = MtProtoConnection::connect_and_authorize(target_dc, api_id, api_hash, bot_token).await?;
                    continue;
                }
                if let Some(wait_secs) = MtProtoConnection::parse_flood_wait(&err_msg) {
                    if wait_secs <= 3 {
                        tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                        continue;
                    }
                    tracing::warn!("[dl] dc={} flood wait {}s -> fast-fail (429)", main_conn.dc_id, wait_secs);
                    return Err(e);
                }
                // If cached auth failed or dropped connection, refresh authorization once
                if !refreshed_auth && (is_auth_refresh_error(&err_msg)) {
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

    let chunk0 = parse_upload_file_chunk(&resp0)?;
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

    let mut out_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&part_path)
        .await?;

    out_file.write_all(&chunk0).await?;

    // If file is smaller than 512 KB, download is completely finished in 1 round trip!
    if chunk0.len() < CHUNK_SIZE {
        out_file.flush().await?;
        drop(out_file);
        fs::rename(&part_path, &target_path).await?;
        return Ok((target_path, chunk0.len() as u64));
    }

    // 3. For files > 512 KB, extract the authenticated auth_key from main_conn
        let shared_auth = main_conn.cached_auth();
    drop(main_conn);

    // 4. Multi-worker parallel download pipeline:
    // Parallel TCP worker connections stream remaining 512 KB chunks at full network capacity.
    // Chunks are transferred over an asynchronous mpsc channel to a dedicated async disk writer.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(usize, Vec<u8>, bool)>(32);

    let writer_handle = tokio::spawn(async move {
        let mut received_parts = std::collections::HashSet::new();
        received_parts.insert(0); // part 0 is already written
        let mut short_part: Option<usize> = None;
        let mut min_empty_marker: Option<usize> = None;
        let mut max_written_bytes = chunk0.len() as u64;
        let mut final_total_bytes = None;

        while let Some((part_idx, data, is_last)) = rx.recv().await {
            if data.is_empty() {
                // Empty EOF marker: part_idx is past EOF, so part_idx - 1 was the final part.
                // Several workers may report markers out of order; keep the lowest.
                min_empty_marker = Some(min_empty_marker.map_or(part_idx, |m| m.min(part_idx)));
                continue;
            }

            let offset = (part_idx * CHUNK_SIZE) as u64;
            out_file.seek(SeekFrom::Start(offset)).await?;
            out_file.write_all(&data).await?;
            received_parts.insert(part_idx);

            let end_offset = offset + data.len() as u64;
            if end_offset > max_written_bytes {
                max_written_bytes = end_offset;
            }

            if is_last {
                short_part = Some(part_idx);
                final_total_bytes = Some(end_offset);
            }
        }

        if let Some(last_idx) = final_part_index(short_part, min_empty_marker) {
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
            out_file.set_len(total_len).await?;
            out_file.flush().await?;
            Ok::<u64, Error>(total_len)
        } else {
            Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Incomplete download: connection ended without reaching EOF",
            ))
        }
    });

    let next_part = Arc::new(AtomicUsize::new(1));
    let eof_reached = Arc::new(AtomicBool::new(false));
    let mut worker_set = tokio::task::JoinSet::new();

    for _ in 0..NUM_DOWNLOAD_WORKERS {
        let tx_clone = tx.clone();
        let next_part_clone = Arc::clone(&next_part);
        let eof_clone = Arc::clone(&eof_reached);
        let loc_clone = loc.clone();

        worker_set.spawn(async move {
            let mut conn = MtProtoConnection::connect_with_auth(shared_auth, api_id).await?;
            loop {
                if eof_clone.load(Ordering::Relaxed) {
                    break;
                }
                let part_idx = next_part_clone.fetch_add(1, Ordering::Relaxed);
                let offset = (part_idx * CHUNK_SIZE) as i64;
                let q = build_get_file_query(&loc_clone, offset, CHUNK_SIZE as i32);

                let mut chunk_res = None;
                for attempt in 0..5 {
                    match conn.rpc_call(&q).await {
                        Ok(r) => {
                            chunk_res = Some(r);
                            break;
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            tracing::warn!("[dl] part {} attempt {} error: {}", part_idx, attempt, msg);
                            if is_past_eof_error(&msg) {
                                eof_clone.store(true, Ordering::Relaxed);
                                let _ = tx_clone.send((part_idx, Vec::new(), true)).await;
                                return Ok(());
                            }
                            if let Some(wait_secs) = MtProtoConnection::parse_flood_wait(&msg) {
                                if wait_secs <= 3 {
                                    tokio::time::sleep(std::time::Duration::from_secs(wait_secs)).await;
                                    continue;
                                }
                                return Err(e);
                            }
                            if msg.contains("LOCATION_INVALID") || msg.contains("FILE_REFERENCE_EXPIRED") {
                                return Err(e);
                            }
                            if attempt == 4 {
                                return Err(e);
                            }
                            let backoff = std::time::Duration::from_millis(50 * (1 << attempt));
                            tokio::time::sleep(backoff).await;
                            if let Ok(new_c) = MtProtoConnection::connect_with_auth(shared_auth, api_id).await {
                                conn = new_c;
                            }
                        }
                    }
                }

                let resp = match chunk_res {
                    Some(r) => r,
                    None => break,
                };

                let chunk_data = parse_upload_file_chunk(&resp)?;
                if chunk_data.is_empty() {
                    eof_clone.store(true, Ordering::Relaxed);
                    let _ = tx_clone.send((part_idx, Vec::new(), true)).await;
                    break;
                }

                let len = chunk_data.len();
                let is_last = len < CHUNK_SIZE;

                if tx_clone.send((part_idx, chunk_data, is_last)).await.is_err() {
                    break;
                }

                if is_last {
                    eof_clone.store(true, Ordering::Relaxed);
                    break;
                }
            }
            Ok::<(), Error>(())
        });
    }

    drop(tx);

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
                first_err = Some(Error::new(ErrorKind::Other, format!("Worker panicked: {e}")));
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
        writer_handle.abort();
        let _ = writer_handle.await; // Cancelled is expected; ensures the file handle is closed
        let _ = fs::remove_file(&part_path).await;
        return Err(e);
    }

    let total_downloaded = match writer_handle.await {
        Ok(Ok(t)) => t,
        Ok(Err(e)) => {
            let _ = fs::remove_file(&part_path).await;
            return Err(e);
        }
        Err(e) => {
            let _ = fs::remove_file(&part_path).await;
            return Err(Error::new(ErrorKind::Other, format!("Writer panicked: {e}")));
        }
    };

    // Atomically move verified complete file to target destination
    fs::rename(&part_path, &target_path).await?;

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
