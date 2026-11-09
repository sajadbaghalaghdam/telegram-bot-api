use bytes::BytesMut;
use std::io::{Error, ErrorKind, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};

use super::tl::{read_bytes, read_int, read_long, write_bytes, write_int, write_long, write_string};
use super::uploader::{CachedBotAuth, MtProtoConnection};

/// 512 KB download chunk size (maximum MTProto chunk size evenly dividing 1MB)
const CHUNK_SIZE: usize = 512 * 1024;

/// Number of parallel worker connections for high-speed file downloads
const NUM_DOWNLOAD_WORKERS: usize = 6;

#[derive(Debug, Clone)]
pub struct DecodedFileLocation {
    pub file_type: i32,
    pub dc_id: i32,
    pub doc_id: i64,
    pub access_hash: i64,
    pub file_reference: Vec<u8>,
}

/// Pure-Rust base64url decoder without external dependencies
pub fn base64url_decode(input: &str) -> Option<Vec<u8>> {
    let mut table = [255u8; 256];
    for (i, &b) in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".iter().enumerate() {
        table[b as usize] = i as u8;
    }
    table[b'+' as usize] = 62;
    table[b'/' as usize] = 63;

    let bytes = input.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity((bytes.len() * 3) / 4);
    let mut buf = 0u32;
    let mut bits = 0;

    for &b in bytes {
        let v = table[b as usize];
        if v == 255 {
            continue;
        }
        buf = (buf << 6) | (v as u32);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

/// Pure-Rust zero run-length decoder (matching TDLib zero_decode in misc.cpp:231)
pub fn zero_decode(data: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(data.len() * 2);
    let mut i = 0;
    while i < data.len() {
        let c = data[i];
        if c == 0 && i + 1 < data.len() {
            let cnt = data[i + 1] as usize;
            buf.resize(buf.len() + cnt, 0);
            i += 2;
        } else {
            buf.push(c);
            i += 1;
        }
    }
    buf
}

/// Decodes Telegram Bot API persistent `file_id` (v4/v2) directly to MTProto location
/// Translated from TDLib td/telegram/files/FileManager.cpp:from_persistent_id_v23 & FileLocation.hpp:FullRemoteFileLocation::parse
pub fn decode_file_id(file_id: &str) -> Option<DecodedFileLocation> {
    let raw = base64url_decode(file_id)?;
    if raw.len() < 2 {
        return None;
    }

    let version = raw[raw.len() - 1];
    let sub = if version == 4 {
        if raw.len() < 3 {
            return None;
        }
        &raw[..raw.len() - 2] // strip PERSISTENT_ID_VERSION (4) and sub_version
    } else if version == 2 {
        &raw[..raw.len() - 1] // strip PERSISTENT_ID_VERSION_OLD (2)
    } else {
        return None;
    };

    let decoded = zero_decode(sub);
    if decoded.len() < 24 {
        return None;
    }

    let mut cursor = decoded.as_slice();
    let raw_type = read_int(&mut cursor).ok()?;
    let is_web = (raw_type & (1 << 24)) != 0;
    if is_web {
        return None;
    }
    let has_file_ref = (raw_type & (1 << 25)) != 0;
    let file_type = raw_type & !(1 << 24) & !(1 << 25);

    let dc_id = read_int(&mut cursor).ok()?;
    let file_reference = if has_file_ref {
        read_bytes(&mut cursor).ok()?.to_vec()
    } else {
        Vec::new()
    };

    let doc_id = read_long(&mut cursor).ok()?;
    let access_hash = read_long(&mut cursor).ok()?;

    Some(DecodedFileLocation {
        file_type,
        dc_id,
        doc_id,
        access_hash,
        file_reference,
    })
}

/// Suggested extension for file types (from td/telegram/files/FileType.cpp)
pub fn extension_for_file_type(file_type: i32) -> &'static str {
    match file_type {
        0 => "jpg",   // Thumbnail
        1 => "jpg",   // ProfilePhoto
        2 => "jpg",   // Photo
        3 => "ogg",   // VoiceNote
        4 => "mp4",   // Video
        5 => "bin",   // Document
        8 => "webp",  // Sticker
        9 => "mp3",   // Audio
        10 => "mp4",  // Animation
        12 => "mp4",  // VideoNote
        _ => "bin",
    }
}

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
    let mut main_conn = MtProtoConnection::connect_and_authorize(loc.dc_id, api_id, api_hash, bot_token).await?;

    // 2. Fetch the first 512 KB chunk (part 0, offset 0) on the primary connection.
    // This immediately handles any DC migration (FILE_MIGRATE_X) and verifies authentication.
    let q0 = build_get_file_query(&loc, 0, CHUNK_SIZE as i32);
    let resp0 = loop {
        match main_conn.rpc_call(&q0).await {
            Ok(r) => break r,
            Err(e) => {
                let err_msg = e.to_string();
                if let Some(target_dc) = MtProtoConnection::parse_migrate_dc(&err_msg) {
                    main_conn = MtProtoConnection::connect_and_authorize(target_dc, api_id, api_hash, bot_token).await?;
                    continue;
                }
                if let Some(wait_secs) = MtProtoConnection::parse_flood_wait(&err_msg) {
                    tokio::time::sleep(std::time::Duration::from_secs(wait_secs.max(1))).await;
                    continue;
                }
                return Err(e);
            }
        }
    };

    let chunk0 = parse_upload_file_chunk(&resp0)?;
    if chunk0.is_empty() {
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
    let home_dc = main_conn.dc_id;
    let shared_auth = CachedBotAuth {
        dc_id: home_dc,
        auth_key: *main_conn.session.auth_key(),
        server_salt: main_conn.session.server_salt,
        time_offset: main_conn.session.time_offset,
    };
    drop(main_conn);

    // 4. Multi-worker parallel download pipeline:
    // Parallel TCP worker connections stream remaining 512 KB chunks at full network capacity.
    // Chunks are transferred over an asynchronous mpsc channel to a dedicated async disk writer.
    let (tx, mut rx) = tokio::sync::mpsc::channel::<(usize, Vec<u8>)>(32);

    let writer_handle = tokio::spawn(async move {
        let mut total = chunk0.len() as u64;
        while let Some((part_idx, data)) = rx.recv().await {
            let offset = (part_idx * CHUNK_SIZE) as u64;
            out_file.seek(SeekFrom::Start(offset)).await?;
            out_file.write_all(&data).await?;
            let end_offset = offset + data.len() as u64;
            if end_offset > total {
                total = end_offset;
            }
        }
        out_file.flush().await?;
        out_file.set_len(total).await?;
        Ok::<u64, Error>(total)
    });

    let next_part = Arc::new(AtomicUsize::new(1));
    let eof_reached = Arc::new(AtomicBool::new(false));
    let mut worker_handles = Vec::with_capacity(NUM_DOWNLOAD_WORKERS);

    for _ in 0..NUM_DOWNLOAD_WORKERS {
        let tx_clone = tx.clone();
        let next_part_clone = Arc::clone(&next_part);
        let eof_clone = Arc::clone(&eof_reached);
        let loc_clone = loc.clone();

        worker_handles.push(tokio::spawn(async move {
            let mut conn = MtProtoConnection::connect_with_auth(shared_auth, api_id).await?;
            loop {
                if eof_clone.load(Ordering::Relaxed) {
                    break;
                }
                let part_idx = next_part_clone.fetch_add(1, Ordering::Relaxed);
                let offset = (part_idx * CHUNK_SIZE) as i64;
                let q = build_get_file_query(&loc_clone, offset, CHUNK_SIZE as i32);

                let mut chunk_res = None;
                for attempt in 0..3 {
                    match conn.rpc_call(&q).await {
                        Ok(r) => {
                            chunk_res = Some(r);
                            break;
                        }
                        Err(e) => {
                            let msg = e.to_string();
                            if msg.contains("OFFSET_INVALID") || msg.contains("LOCATION_INVALID") {
                                eof_clone.store(true, Ordering::Relaxed);
                                return Ok(());
                            }
                            if attempt == 2 {
                                return Err(e);
                            }
                            if let Ok(new_c) = MtProtoConnection::connect_with_auth(shared_auth, api_id).await {
                                conn = new_c;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
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
                    break;
                }

                let len = chunk_data.len();
                if tx_clone.send((part_idx, chunk_data)).await.is_err() {
                    break;
                }

                if len < CHUNK_SIZE {
                    eof_clone.store(true, Ordering::Relaxed);
                    break;
                }
            }
            Ok::<(), Error>(())
        }));
    }

    drop(tx);

    let mut first_err = None;
    for wh in &mut worker_handles {
        match wh.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if first_err.is_none() {
                    first_err = Some(e);
                }
            }
            Err(e) => {
                if first_err.is_none() {
                    first_err = Some(Error::new(ErrorKind::Other, format!("Worker panicked: {e}")));
                }
            }
        }
    }

    if let Some(e) = first_err {
        writer_handle.abort();
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

    Ok((target_path, total_downloaded))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_real_file_id() {
        let file_id = "CQACAgQAAxUAAWrwTHZ-JOvriRzYeCZRX_Tm9mRMAAIgFgAC4uN5UTBHbKrumCZNPQQ";
        let loc = decode_file_id(file_id).expect("Should decode file_id");
        assert_eq!(loc.dc_id, 4);
        assert_eq!(loc.doc_id, 5870974149056271904);
        assert_eq!(loc.access_hash, 5559298940864317232);
        assert_eq!(loc.file_type, 9); // Audio
        assert_eq!(extension_for_file_type(loc.file_type), "mp3");
    }
}
