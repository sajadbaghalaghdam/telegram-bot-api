use bytes::{Buf, BufMut};
use std::str;
use thiserror::Error;

const MAX_TL_LENGTH: usize = 1024 * 1024;

#[derive(Debug, Error, PartialEq)]
pub enum TLError {
    #[error("Unexpected EOF while reading data")]
    UnexpectedEof,
    #[error("Invalid UTF-8 sequence")]
    InvalidUtf8(#[from] str::Utf8Error),
    #[error("Reserved length marker 0xff in TL string")]
    InvalidLengthMarker,
    #[error("Data length {0} exceeds maximum allowed limit")]
    TooLarge(usize),
}

pub fn write_int(buf: &mut impl BufMut, val: i32) {
    buf.put_i32_le(val);
}

pub fn write_long(buf: &mut impl BufMut, val: i64) {
    buf.put_i64_le(val);
}

pub fn write_int128(buf: &mut impl BufMut, val: &[u8; 16]) {
    buf.put_slice(val);
}

pub fn write_int256(buf: &mut impl BufMut, val: &[u8; 32]) {
    buf.put_slice(val);
}

/// Writes the TL `bytes` length prefix for a payload of `len` bytes and returns its size
/// (1 byte for `len <= 253`, otherwise `0xfe` + 3 length bytes).
pub fn write_bytes_header(buf: &mut impl BufMut, len: usize) -> usize {
    if len <= 253 {
        buf.put_u8(len as u8);
        1
    } else {
        buf.put_u8(254);
        buf.put_slice(&(len as u32).to_le_bytes()[0..3]);
        4
    }
}

/// Zero padding that follows a TL `bytes` payload so the whole field is 4-byte aligned.
pub fn bytes_padding(header_len: usize, len: usize) -> usize {
    (4 - ((header_len + len) % 4)) % 4
}

pub fn write_bytes(buf: &mut impl BufMut, data: &[u8]) {
    let header_len = write_bytes_header(buf, data.len());
    buf.put_slice(data);
    buf.put_bytes(0, bytes_padding(header_len, data.len()));
}

pub fn write_string(buf: &mut impl BufMut, s: &str) {
    write_bytes(buf, s.as_bytes());
}

pub fn read_int(cursor: &mut &[u8]) -> Result<i32, TLError> {
    if cursor.len() < 4 {
        return Err(TLError::UnexpectedEof);
    }
    Ok(cursor.get_i32_le())
}

pub fn read_long(cursor: &mut &[u8]) -> Result<i64, TLError> {
    if cursor.len() < 8 {
        return Err(TLError::UnexpectedEof);
    }
    Ok(cursor.get_i64_le())
}

pub fn read_int128(cursor: &mut &[u8]) -> Result<[u8; 16], TLError> {
    if cursor.len() < 16 {
        return Err(TLError::UnexpectedEof);
    }
    let mut res = [0u8; 16];
    res.copy_from_slice(&cursor[..16]);
    *cursor = &cursor[16..];
    Ok(res)
}

pub fn read_int256(cursor: &mut &[u8]) -> Result<[u8; 32], TLError> {
    if cursor.len() < 32 {
        return Err(TLError::UnexpectedEof);
    }
    let mut res = [0u8; 32];
    res.copy_from_slice(&cursor[..32]);
    *cursor = &cursor[32..];
    Ok(res)
}

pub fn read_bytes<'a>(cursor: &mut &'a [u8]) -> Result<&'a [u8], TLError> {
    if cursor.is_empty() {
        return Err(TLError::UnexpectedEof);
    }

    let first_byte = cursor[0];
    if first_byte == 255 {
        // 255 is a reserved length marker in the TL string encoding.
        return Err(TLError::InvalidLengthMarker);
    }
    let (len, header_len) = if first_byte < 254 {
        let l = first_byte as usize;
        *cursor = &cursor[1..];
        (l, 1usize)
    } else {
        if cursor.len() < 4 {
            return Err(TLError::UnexpectedEof);
        }
        let mut len_buf = [0u8; 4];
        len_buf[0..3].copy_from_slice(&cursor[1..4]);
        let l = u32::from_le_bytes(len_buf) as usize;
        *cursor = &cursor[4..];
        (l, 4usize)
    };

    if len > MAX_TL_LENGTH {
        return Err(TLError::TooLarge(len));
    }

    if cursor.len() < len {
        return Err(TLError::UnexpectedEof);
    }

    let data = &cursor[..len];
    *cursor = &cursor[len..];

    let padding = (4 - ((header_len + len) % 4)) % 4;
    if cursor.len() < padding {
        return Err(TLError::UnexpectedEof);
    }
    *cursor = &cursor[padding..];

    Ok(data)
}

pub fn read_string<'a>(cursor: &mut &'a [u8]) -> Result<&'a str, TLError> {
    let bytes = read_bytes(cursor)?;
    str::from_utf8(bytes).map_err(TLError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_int_roundtrip() {
        let mut buf = BytesMut::new();
        let val = -123456789;
        write_int(&mut buf, val);

        let mut cursor = buf.as_ref();
        assert_eq!(read_int(&mut cursor).unwrap(), val);
    }

    #[test]
    fn test_long_roundtrip() {
        let mut buf = BytesMut::new();
        let val = -987654321012345678i64;
        write_long(&mut buf, val);

        let mut cursor = buf.as_ref();
        assert_eq!(read_long(&mut cursor).unwrap(), val);
    }

    #[test]
    fn test_string_short_roundtrip() {
        let mut buf = BytesMut::new();
        let val = "Hello Rust!";
        write_string(&mut buf, val);

        let mut cursor = buf.as_ref();
        assert_eq!(read_string(&mut cursor).unwrap(), val);
    }

    #[test]
    fn test_string_long_roundtrip() {
        let mut buf = BytesMut::new();
        let val = "A".repeat(300);
        write_string(&mut buf, &val);

        let mut cursor = buf.as_ref();
        assert_eq!(read_string(&mut cursor).unwrap(), val);
    }

    #[test]
    fn test_bytes_padding_roundtrip() {
        let mut buf = BytesMut::new();
        let val = b"hello";
        write_bytes(&mut buf, val);
        assert_eq!(buf.len() % 4, 0);

        let mut cursor = buf.as_ref();
        assert_eq!(read_bytes(&mut cursor).unwrap(), val);
    }

    #[test]
    fn test_header_and_padding_helpers_match_write_bytes() {
        for len in [0usize, 1, 3, 4, 253, 254, 255, 300, 512 * 1024] {
            let data = vec![0xabu8; len];
            let mut whole = Vec::new();
            write_bytes(&mut whole, &data);
            let mut parts = Vec::new();
            let h = write_bytes_header(&mut parts, len);
            parts.extend_from_slice(&data);
            parts.resize(parts.len() + bytes_padding(h, len), 0);
            assert_eq!(whole, parts, "len {len}");
            assert_eq!(whole.len() % 4, 0);
        }
    }

    #[test]
    fn test_read_bytes_rejects_255_marker() {
        let data = [255u8, 1, 0, 0, 0, 0, 0, 0];
        let mut cursor = &data[..];
        assert_eq!(read_bytes(&mut cursor), Err(TLError::InvalidLengthMarker));
    }
}
