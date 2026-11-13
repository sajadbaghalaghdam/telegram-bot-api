//! Bot API `file_id` decoding (base64url + zero-RLE -> MTProto file location) and encoding of
//! the `file_id`/`file_unique_id` of a document this server sent over MTProto.
use super::tl::{read_bytes, read_int, read_long, write_bytes};

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

/// TDLib `FileType` values of the documents this server sends (td/telegram/files/FileType.h).
pub mod file_type {
    pub const VOICE_NOTE: i32 = 3;
    pub const VIDEO: i32 = 4;
    pub const DOCUMENT: i32 = 5;
    pub const AUDIO: i32 = 9;
    pub const ANIMATION: i32 = 10;
    pub const VIDEO_NOTE: i32 = 13;
}

/// TDLib's `Version::Next - 1` (TDLib reads every earlier version too).
const TDLIB_VERSION: u8 = 61;
const PERSISTENT_ID_VERSION: u8 = 4;
const FILE_REFERENCE_FLAG: i32 = 1 << 25;
/// `FileUniqueIdType::Document`.
const UNIQUE_DOCUMENT: i32 = 2;

/// The Bot API `file_id` of a sent document (TDLib `FullRemoteFileLocation`, persistent id v4):
/// `type|FILE_REFERENCE_FLAG, dc_id, file_reference (TL bytes), id, access_hash`, zero-run
/// encoded, then the TDLib version byte and 4, base64url without padding.
pub fn encode_document_file_id(file_type: i32, dc_id: i32, id: i64, access_hash: i64, file_reference: &[u8]) -> String {
    let mut raw = bytes::BytesMut::with_capacity(32 + file_reference.len());
    raw.extend_from_slice(&(file_type | FILE_REFERENCE_FLAG).to_le_bytes());
    raw.extend_from_slice(&dc_id.to_le_bytes());
    write_bytes(&mut raw, file_reference);
    raw.extend_from_slice(&id.to_le_bytes());
    raw.extend_from_slice(&access_hash.to_le_bytes());
    let mut out = zero_encode(&raw);
    out.push(TDLIB_VERSION);
    out.push(PERSISTENT_ID_VERSION);
    base64url_encode(&out)
}

/// The Bot API `file_unique_id` of a document: `FileUniqueIdType::Document` and the id, zero-run
/// encoded, base64url (the `AgAD...` ids of the cloud server).
pub fn encode_document_unique_id(id: i64) -> String {
    let mut raw = Vec::with_capacity(12);
    raw.extend_from_slice(&UNIQUE_DOCUMENT.to_le_bytes());
    raw.extend_from_slice(&id.to_le_bytes());
    base64url_encode(&zero_encode(&raw))
}

/// TDLib zero_encode (misc.cpp): each run of up to 250 zero bytes becomes `0, run length`.
fn zero_encode(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(data.len() + 8);
    let mut i = 0;
    while i < data.len() {
        out.push(data[i]);
        if data[i] == 0 {
            let mut cnt = 1usize;
            while cnt < 250 && i + cnt < data.len() && data[i + cnt] == 0 {
                cnt += 1;
            }
            out.push(cnt as u8);
            i += cnt;
        } else {
            i += 1;
        }
    }
    out
}

/// base64url without padding.
fn base64url_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], chunk.get(1).copied().unwrap_or(0), chunk.get(2).copied().unwrap_or(0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        for k in 0..=chunk.len() {
            out.push(ALPHABET[((n >> (18 - 6 * k)) & 63) as usize] as char);
        }
    }
    out
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

    #[test]
    fn encoded_document_file_id_round_trips() {
        let reference = [0u8, 0, 7, 9, 0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13];
        let id = encode_document_file_id(file_type::VIDEO, 4, 5_870_974_149_056_271_904, -42, &reference);
        let loc = decode_file_id(&id).expect("decodes");
        assert_eq!(loc.file_type, file_type::VIDEO);
        assert_eq!(loc.dc_id, 4);
        assert_eq!(loc.doc_id, 5_870_974_149_056_271_904);
        assert_eq!(loc.access_hash, -42);
        assert_eq!(loc.file_reference, reference);
    }

    #[test]
    fn real_file_id_re_encodes_identically() {
        // A cloud Bot API file_id, decoded and encoded again: the same bytes except the
        // TDLib version byte (second to last).
        let real = "CQACAgQAAxUAAWrwTHZ-JOvriRzYeCZRX_Tm9mRMAAIgFgAC4uN5UTBHbKrumCZNPQQ";
        let loc = decode_file_id(real).unwrap();
        let again = encode_document_file_id(loc.file_type, loc.dc_id, loc.doc_id, loc.access_hash, &loc.file_reference);
        let (a, b) = (base64url_decode(real).unwrap(), base64url_decode(&again).unwrap());
        assert_eq!(a.len(), b.len());
        assert_eq!(a[..a.len() - 2], b[..b.len() - 2]);
        assert_eq!(a[a.len() - 1], b[b.len() - 1]);
    }

    #[test]
    fn unique_id_has_the_cloud_prefix() {
        assert!(encode_document_unique_id(5_870_974_149_056_271_904).starts_with("AgAD"));
    }

    #[test]
    fn base64url_matches_known_values() {
        assert_eq!(base64url_encode(b""), "");
        assert_eq!(base64url_encode(b"f"), "Zg");
        assert_eq!(base64url_encode(b"fo"), "Zm8");
        assert_eq!(base64url_encode(b"foo"), "Zm9v");
        assert_eq!(base64url_encode(&[0xfb, 0xff]), "-_8");
    }
}
