//! Reads what `messages.sendMedia` answered: the sent message's id and date, and its document.
use std::time::{SystemTime, UNIX_EPOCH};

use super::tl::{read_bytes, read_int};

/// What a `messages.sendMedia` answer tells about the sent message and its document.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SentMedia {
    /// 1 when the answer did not carry the id (matched by `random_id`).
    pub message_id: i32,
    pub date: i64,
    pub doc_id: i64,
    /// 0 when no document was found in the answer.
    pub access_hash: i64,
    /// Needed in a usable Bot API `file_id`; empty when no document was found.
    pub file_reference: Vec<u8>,
}

/// Scans a `messages.sendMedia` answer for `updateMessageID` (matched by `random_id`) and the
/// first `document#8fd4c4d8`.
pub fn parse_send_media_updates(buf: &[u8], random_id: i64) -> SentMedia {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut sent = SentMedia {
        message_id: 1,
        date: now_secs,
        doc_id: rand::random::<i64>().abs(),
        ..SentMedia::default()
    };

    let rand_bytes = random_id.to_le_bytes();
    for i in 0..buf.len().saturating_sub(16) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        if ctor == 0x4e90bfd6 && buf[i + 8..i + 16] == rand_bytes {
            sent.message_id = i32::from_le_bytes(buf[i + 4..i + 8].try_into().unwrap());
            break;
        }
    }

    // document#8fd4c4d8 flags:# id:long access_hash:long file_reference:bytes date:int ...
    for i in 0..buf.len().saturating_sub(32) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        if ctor == 0x8fd4c4d8 {
            sent.doc_id = i64::from_le_bytes(buf[i + 8..i + 16].try_into().unwrap());
            sent.access_hash = i64::from_le_bytes(buf[i + 16..i + 24].try_into().unwrap());
            let mut cur = &buf[i + 24..];
            if let Ok(file_reference) = read_bytes(&mut cur) {
                sent.file_reference = file_reference.to_vec();
                if let Ok(d) = read_int(&mut cur)
                    && d > 1_600_000_000
                {
                    sent.date = d as i64;
                }
            }
            break;
        }
    }

    sent
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtproto::tl::{write_bytes, write_int, write_long};
    use bytes::BytesMut;

    #[test]
    fn reads_message_id_and_document() {
        let random_id = 0x1122_3344_5566_7788i64;
        let mut buf = BytesMut::new();
        write_int(&mut buf, 0x12345678); // some outer constructor
        write_int(&mut buf, 0x4e90bfd6u32 as i32); // updateMessageID id:int random_id:long
        write_int(&mut buf, 4242);
        write_long(&mut buf, random_id);
        write_int(&mut buf, 0x8fd4c4d8u32 as i32); // document
        write_int(&mut buf, 0); // flags
        write_long(&mut buf, 987_654_321);
        write_long(&mut buf, -55);
        write_bytes(&mut buf, &[1, 2, 3, 0, 5]);
        write_int(&mut buf, 1_763_000_000);
        write_long(&mut buf, 0); // padding so the scan window covers the document
        let sent = parse_send_media_updates(&buf, random_id);
        assert_eq!(
            sent,
            SentMedia {
                message_id: 4242,
                date: 1_763_000_000,
                doc_id: 987_654_321,
                access_hash: -55,
                file_reference: vec![1, 2, 3, 0, 5],
            }
        );
    }
}
