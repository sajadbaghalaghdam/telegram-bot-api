//! Bot API `file_id` decoding (base64url + zero-RLE -> MTProto file location).
use super::tl::{read_bytes, read_int, read_long};

#[derive(Debug, Clone)]
pub struct DecodedFileLocation {
    pub file_type: i32,
    pub dc_id: i32,
    pub doc_id: i64,
    pub access_hash: i64,
    pub file_reference: Vec<u8>,
}

/// Pure-Rust base64url decoder without external dependencies
fn base64url_decode(input: &str) -> Option<Vec<u8>> {
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
fn zero_decode(data: &[u8]) -> Vec<u8> {
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
