use bytes::{Buf, BufMut, BytesMut};
use std::str;
use thiserror::Error;

const MAX_TL_LENGTH: usize = 1024 * 1024;

#[derive(Debug, Error, PartialEq)]
pub enum TLError {
    #[error("Unexpected EOF while reading data")]
    UnexpectedEof,
    #[error("Invalid UTF-8 sequence")]
    InvalidUtf8(#[from] str::Utf8Error),
    #[error("Data length {0} exceeds maximum allowed limit")]
    TooLarge(usize),
}

pub fn write_int(buf: &mut BytesMut, val: i32) {
    buf.put_i32_le(val);
}

pub fn write_long(buf: &mut BytesMut, val: i64) {
    buf.put_i64_le(val);
}

pub fn write_int128(buf: &mut BytesMut, val: &[u8; 16]) {
    buf.put_slice(val);
}

pub fn write_int256(buf: &mut BytesMut, val: &[u8; 32]) {
    buf.put_slice(val);
}

pub fn write_bytes(buf: &mut BytesMut, data: &[u8]) {
    let len = data.len();
    let header_len = if len <= 253 {
        buf.put_u8(len as u8);
        buf.put_slice(data);
        1
    } else {
        buf.put_u8(254);
        let len_bytes = (len as u32).to_le_bytes();
        buf.put_slice(&len_bytes[0..3]);
        buf.put_slice(data);
        4
    };

    let padding = (4 - ((header_len + len) % 4)) % 4;
    if padding > 0 {
        buf.put_bytes(0, padding);
    }
}

pub fn write_string(buf: &mut BytesMut, s: &str) {
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
}
