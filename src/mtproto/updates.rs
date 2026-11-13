//! Parsing of the `Updates` response to `messages.sendMedia`.
use std::time::{SystemTime, UNIX_EPOCH};

use super::tl::{read_bytes, read_int};

pub fn parse_send_media_updates(buf: &[u8], random_id: i64) -> (i32, i64, i64, i64) {
    let mut message_id = 1i32;
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut date = now_secs;
    let mut doc_id = rand::random::<i64>().abs();
    let mut access_hash = 0i64;

    let rand_bytes = random_id.to_le_bytes();
    for i in 0..buf.len().saturating_sub(16) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        if ctor == 0x4e90bfd6 && buf[i + 8..i + 16] == rand_bytes {
            message_id = i32::from_le_bytes(buf[i + 4..i + 8].try_into().unwrap());
            break;
        }
    }

    for i in 0..buf.len().saturating_sub(32) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        if ctor == 0x8fd4c4d8 {
            doc_id = i64::from_le_bytes(buf[i + 8..i + 16].try_into().unwrap());
            access_hash = i64::from_le_bytes(buf[i + 16..i + 24].try_into().unwrap());
            let mut cur = &buf[i + 24..];
            if read_bytes(&mut cur).is_ok()
                && let Ok(d) = read_int(&mut cur)
                && d > 1_600_000_000
            {
                date = d as i64;
            }
            break;
        }
    }

    (message_id, date, doc_id, access_hash)
}
