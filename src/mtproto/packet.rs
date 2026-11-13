use crate::mtproto::crypto::{Aes256Ige, derive_keys};
use rand::RngCore;
use sha2::{Digest, Sha256};
use std::ops::Range;
use thiserror::Error;

/// MTProto 2.0 Packet constants
const AUTH_KEY_ID_SIZE: usize = 8;
const MSG_KEY_SIZE: usize = 16;
const AES_BLOCK_SIZE: usize = 16;
const MIN_PADDING: usize = 12;
/// salt + session_id + msg_id + seq_no + data length
const INNER_HEADER_SIZE: usize = 32;
/// Intermediate-transport length prefix.
const FRAME_LEN_SIZE: usize = 4;
/// Offset of the encrypted part inside a frame built by [`pack_encrypted_frame`].
const ENCRYPTED_AT: usize = FRAME_LEN_SIZE + AUTH_KEY_ID_SIZE + MSG_KEY_SIZE;

#[derive(Error, Debug)]
pub enum PacketError {
    #[error("Packet is too short to be a valid MTProto message")]
    PacketTooShort,
    #[error("Authentication key ID mismatch")]
    AuthKeyIdMismatch,
    #[error("Message key integrity check failed")]
    IntegrityCheckFailed,
    #[error("Decrypted plaintext is shorter than the declared data length")]
    InvalidPayloadLength,
    #[error("Session ID mismatch")]
    SessionIdMismatch,
    #[error("Invalid server message header (msg_id or length)")]
    InvalidHeader,
    #[error("Transport error code: {0}")]
    TransportError(i32),
    #[error("Crypto error: {0}")]
    Crypto(String),
}

/// Header fields of a client -> server message.
pub struct OutgoingHeader {
    pub server_salt: i64,
    pub session_id: i64,
    pub msg_id: i64,
    pub seq_no: i32,
}

/// Packs and encrypts a message according to MTProto 2.0 specification
/// (translated from Transport::calc_message_key2 & write_crypto_impl in td/mtproto/Transport.cpp).
///
/// The complete intermediate-transport frame `[len:u32][auth_key_id][msg_key][encrypted]` is
/// built in `out` (cleared first, capacity reused) and encrypted in place, so the caller can
/// write it to the socket as-is: no intermediate plaintext/ciphertext/packet buffers.
pub fn pack_encrypted_frame(
    out: &mut Vec<u8>,
    auth_key: &[u8; 256],
    auth_key_id: i64,
    header: &OutgoingHeader,
    message_data: &[u8],
) -> Result<(), PacketError> {
    let current_len = INNER_HEADER_SIZE + message_data.len();
    // Pad to the AES block size with at least MIN_PADDING random bytes.
    let padding_len = MIN_PADDING + (AES_BLOCK_SIZE - ((current_len + MIN_PADDING) % AES_BLOCK_SIZE)) % AES_BLOCK_SIZE;
    let total = ENCRYPTED_AT + current_len + padding_len;

    out.clear();
    out.reserve(total);
    out.extend_from_slice(&((total - FRAME_LEN_SIZE) as u32).to_le_bytes());
    out.extend_from_slice(&auth_key_id.to_le_bytes());
    out.extend_from_slice(&[0u8; MSG_KEY_SIZE]); // msg_key, filled below
    out.extend_from_slice(&header.server_salt.to_le_bytes());
    out.extend_from_slice(&header.session_id.to_le_bytes());
    out.extend_from_slice(&header.msg_id.to_le_bytes());
    out.extend_from_slice(&header.seq_no.to_le_bytes());
    out.extend_from_slice(&(message_data.len() as u32).to_le_bytes());
    out.extend_from_slice(message_data);
    out.resize(total, 0);
    rand::thread_rng().fill_bytes(&mut out[ENCRYPTED_AT + current_len..]);

    // msg_key_large = SHA256(substr(auth_key, 88 + 0, 32) + plaintext)
    // msg_key = substr(msg_key_large, 8, 16)
    let mut hasher = Sha256::new();
    hasher.update(&auth_key[88..120]);
    hasher.update(&out[ENCRYPTED_AT..]);
    let hash_result = hasher.finalize();
    let mut msg_key = [0u8; MSG_KEY_SIZE];
    msg_key.copy_from_slice(&hash_result[8..24]);
    out[FRAME_LEN_SIZE + AUTH_KEY_ID_SIZE..ENCRYPTED_AT].copy_from_slice(&msg_key);

    let (aes_key, aes_iv) = derive_keys(auth_key, &msg_key, true);
    Aes256Ige::encrypt_in_place(&mut out[ENCRYPTED_AT..], &aes_key, &aes_iv)
        .map_err(|e| PacketError::Crypto(e.to_string()))?;
    Ok(())
}

/// A decrypted server message: header fields plus the location of the body inside the buffer
/// that was decrypted in place.
pub struct Unpacked {
    pub msg_id: i64,
    pub seq_no: i32,
    pub data: Range<usize>,
}

/// Decrypts an MTProto 2.0 encrypted message **in place** and validates it. On success the
/// message body is `packet[unpacked.data]`; on error the buffer contents are unspecified.
pub fn unpack_encrypted_in_place(
    auth_key: &[u8; 256],
    auth_key_id: i64,
    session_id: i64,
    packet: &mut [u8],
) -> Result<Unpacked, PacketError> {
    if packet.len() == 4 {
        let code = i32::from_le_bytes(packet[0..4].try_into().unwrap());
        return Err(PacketError::TransportError(code));
    }
    if packet.len() < AUTH_KEY_ID_SIZE + MSG_KEY_SIZE + INNER_HEADER_SIZE {
        return Err(PacketError::PacketTooShort);
    }

    let recv_auth_key_id = i64::from_le_bytes(packet[0..8].try_into().unwrap());
    if recv_auth_key_id != auth_key_id {
        return Err(PacketError::AuthKeyIdMismatch);
    }

    let mut msg_key = [0u8; MSG_KEY_SIZE];
    msg_key.copy_from_slice(&packet[8..24]);

    let (aes_key, aes_iv) = derive_keys(auth_key, &msg_key, false);

    let encrypted_part = &mut packet[AUTH_KEY_ID_SIZE + MSG_KEY_SIZE..];
    if !encrypted_part.len().is_multiple_of(16) {
        return Err(PacketError::InvalidPayloadLength);
    }
    Aes256Ige::decrypt_in_place(encrypted_part, &aes_key, &aes_iv)
        .map_err(|e| PacketError::Crypto(e.to_string()))?;
    let plaintext: &[u8] = encrypted_part;

    // Verify msg_key: substr(SHA256(substr(auth_key, 88 + 8, 32) + plaintext), 8, 16)
    let mut hasher = Sha256::new();
    hasher.update(&auth_key[96..128]);
    hasher.update(plaintext);
    let hash = hasher.finalize();
    if !constant_time_eq(&hash[8..24], &msg_key) {
        return Err(PacketError::IntegrityCheckFailed);
    }

    if plaintext.len() < INNER_HEADER_SIZE {
        return Err(PacketError::InvalidPayloadLength);
    }

    let recv_session_id = i64::from_le_bytes(plaintext[8..16].try_into().unwrap());
    if recv_session_id != session_id {
        return Err(PacketError::SessionIdMismatch);
    }

    let msg_id = i64::from_le_bytes(plaintext[16..24].try_into().unwrap());
    let seq_no = i32::from_le_bytes(plaintext[24..28].try_into().unwrap());
    let data_len = u32::from_le_bytes(plaintext[28..32].try_into().unwrap()) as usize;

    // Server-originated message ids are odd; the body is 4-byte aligned and
    // followed by 12..=1024 bytes of padding (MTProto 2.0).
    if msg_id & 1 == 0 {
        return Err(PacketError::InvalidHeader);
    }
    if !data_len.is_multiple_of(4) || plaintext.len() < INNER_HEADER_SIZE + data_len {
        return Err(PacketError::InvalidPayloadLength);
    }
    let padding = plaintext.len() - INNER_HEADER_SIZE - data_len;
    if !(MIN_PADDING..=1024).contains(&padding) {
        return Err(PacketError::InvalidPayloadLength);
    }

    let start = AUTH_KEY_ID_SIZE + MSG_KEY_SIZE + INNER_HEADER_SIZE;
    Ok(Unpacked { msg_id, seq_no, data: start..start + data_len })
}

/// Compares two equal-length byte strings without early exit.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 256] = [7u8; 256];

    /// Builds a server -> client packet by hand (x = 8 keys) and unpacks it.
    fn roundtrip(session: i64, msg_id: i64, data: &[u8]) -> Result<(i64, i32, Vec<u8>), PacketError> {
        let mut pt = Vec::new();
        pt.extend_from_slice(&1i64.to_le_bytes());
        pt.extend_from_slice(&session.to_le_bytes());
        pt.extend_from_slice(&msg_id.to_le_bytes());
        pt.extend_from_slice(&1i32.to_le_bytes());
        pt.extend_from_slice(&(data.len() as u32).to_le_bytes());
        pt.extend_from_slice(data);
        while !pt.len().is_multiple_of(16) || pt.len() < 32 + data.len() + 12 {
            pt.push(0xab);
        }
        let mut h = Sha256::new();
        h.update(&KEY[96..128]);
        h.update(&pt);
        let mut msg_key = [0u8; 16];
        msg_key.copy_from_slice(&h.finalize()[8..24]);
        let (k, iv) = derive_keys(&KEY, &msg_key, false);
        Aes256Ige::encrypt_in_place(&mut pt, &k, &iv).unwrap();
        let mut pkt = 42i64.to_le_bytes().to_vec();
        pkt.extend_from_slice(&msg_key);
        pkt.extend_from_slice(&pt);
        let u = unpack_encrypted_in_place(&KEY, 42, 99, &mut pkt)?;
        Ok((u.msg_id, u.seq_no, pkt[u.data].to_vec()))
    }

    #[test]
    fn unpack_accepts_valid() {
        let (id, seq, d) = roundtrip(99, 101, b"abcd").unwrap();
        assert_eq!((id, seq, d.as_slice()), (101, 1, &b"abcd"[..]));
    }

    #[test]
    fn unpack_rejects_bad_session_and_msg_id() {
        assert!(matches!(roundtrip(98, 101, b"abcd"), Err(PacketError::SessionIdMismatch)));
        assert!(matches!(roundtrip(99, 100, b"abcd"), Err(PacketError::InvalidHeader)));
    }

    #[test]
    fn unpack_rejects_unaligned_length() {
        assert!(matches!(roundtrip(99, 101, b"abc"), Err(PacketError::InvalidPayloadLength)));
    }

    #[test]
    fn unpack_rejects_tampered_ciphertext_and_wrong_key_id() {
        let mut pkt = vec![0u8; 24 + 48];
        pkt[..8].copy_from_slice(&42i64.to_le_bytes());
        assert!(matches!(
            unpack_encrypted_in_place(&KEY, 42, 99, &mut pkt.clone()),
            Err(PacketError::IntegrityCheckFailed)
        ));
        assert!(matches!(
            unpack_encrypted_in_place(&KEY, 43, 99, &mut pkt),
            Err(PacketError::AuthKeyIdMismatch)
        ));
        assert!(matches!(
            unpack_encrypted_in_place(&KEY, 42, 99, &mut (-404i32).to_le_bytes()),
            Err(PacketError::TransportError(-404))
        ));
    }

    /// Server-side view of a client frame: decrypt with x = 0 keys and check every field.
    #[test]
    fn pack_frame_layout_and_decryption() {
        for len in [0usize, 4, 20, 1000, 512 * 1024] {
            let data: Vec<u8> = (0..len).map(|i| (i * 7 + 1) as u8).collect();
            let header = OutgoingHeader { server_salt: 11, session_id: 22, msg_id: 44, seq_no: 5 };
            let mut frame = vec![0xeeu8; 3]; // garbage must be cleared
            pack_encrypted_frame(&mut frame, &KEY, 42, &header, &data).unwrap();

            let total = u32::from_le_bytes(frame[..4].try_into().unwrap()) as usize;
            assert_eq!(total, frame.len() - 4);
            assert_eq!(i64::from_le_bytes(frame[4..12].try_into().unwrap()), 42);
            let msg_key: [u8; 16] = frame[12..28].try_into().unwrap();
            let (k, iv) = derive_keys(&KEY, &msg_key, true);
            let mut pt = frame[28..].to_vec();
            assert!(pt.len().is_multiple_of(16));
            Aes256Ige::decrypt_in_place(&mut pt, &k, &iv).unwrap();
            let mut h = Sha256::new();
            h.update(&KEY[88..120]);
            h.update(&pt);
            assert_eq!(&h.finalize()[8..24], &msg_key);
            assert_eq!(i64::from_le_bytes(pt[0..8].try_into().unwrap()), 11);
            assert_eq!(i64::from_le_bytes(pt[8..16].try_into().unwrap()), 22);
            assert_eq!(i64::from_le_bytes(pt[16..24].try_into().unwrap()), 44);
            assert_eq!(i32::from_le_bytes(pt[24..28].try_into().unwrap()), 5);
            assert_eq!(u32::from_le_bytes(pt[28..32].try_into().unwrap()) as usize, len);
            assert_eq!(&pt[32..32 + len], data.as_slice());
            let padding = pt.len() - 32 - len;
            assert!((12..=27).contains(&padding), "padding {padding}");
        }
    }
}
