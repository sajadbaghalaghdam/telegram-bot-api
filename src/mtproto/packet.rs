use crate::mtproto::crypto::{aes_256_ige_decrypt, aes_256_ige_encrypt, derive_keys};
use rand::RngCore;
use sha2::{Digest, Sha256};
use thiserror::Error;

/// MTProto 2.0 Packet constants
const AUTH_KEY_ID_SIZE: usize = 8;
const MSG_KEY_SIZE: usize = 16;
const AES_BLOCK_SIZE: usize = 16;
const MIN_PADDING: usize = 12;

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

/// Packs and encrypts a message according to MTProto 2.0 specification
/// (translated from Transport::calc_message_key2 & write_crypto_impl in td/mtproto/Transport.cpp).
pub fn pack_encrypted_message(
    auth_key: &[u8; 256],
    auth_key_id: i64,
    server_salt: i64,
    session_id: i64,
    msg_id: i64,
    seq_no: i32,
    message_data: &[u8],
) -> Result<Vec<u8>, PacketError> {
    let header_size = 32;
    let current_len = header_size + message_data.len();

    let mut padding_len = (AES_BLOCK_SIZE - ((current_len + MIN_PADDING) % AES_BLOCK_SIZE)) % AES_BLOCK_SIZE + MIN_PADDING;
    if (current_len + padding_len) % AES_BLOCK_SIZE != 0 {
        padding_len += AES_BLOCK_SIZE - ((current_len + padding_len) % AES_BLOCK_SIZE);
    }

    let mut plaintext = Vec::with_capacity(current_len + padding_len);
    plaintext.extend_from_slice(&server_salt.to_le_bytes());
    plaintext.extend_from_slice(&session_id.to_le_bytes());
    plaintext.extend_from_slice(&msg_id.to_le_bytes());
    plaintext.extend_from_slice(&seq_no.to_le_bytes());
    plaintext.extend_from_slice(&(message_data.len() as u32).to_le_bytes());
    plaintext.extend_from_slice(message_data);
    plaintext.resize(current_len + padding_len, 0);
    rand::thread_rng().fill_bytes(&mut plaintext[current_len..]);

    // msg_key_large = SHA256(substr(auth_key, 88 + 0, 32) + plaintext)
    // msg_key = substr(msg_key_large, 8, 16)
    let mut hasher = Sha256::new();
    hasher.update(&auth_key[88..120]);
    hasher.update(&plaintext);
    let hash_result = hasher.finalize();
    let mut msg_key = [0u8; 16];
    msg_key.copy_from_slice(&hash_result[8..24]);

    let (aes_key, aes_iv) = derive_keys(auth_key, &msg_key, true);

    let encrypted_data = aes_256_ige_encrypt(&plaintext, &aes_key, &aes_iv)
        .map_err(|e| PacketError::Crypto(e.to_string()))?;

    let mut packet = Vec::with_capacity(AUTH_KEY_ID_SIZE + MSG_KEY_SIZE + encrypted_data.len());
    packet.extend_from_slice(&auth_key_id.to_le_bytes());
    packet.extend_from_slice(&msg_key);
    packet.extend_from_slice(&encrypted_data);

    Ok(packet)
}

/// Unpacks and decrypts an MTProto 2.0 encrypted message.
pub fn unpack_encrypted_message(
    auth_key: &[u8; 256],
    auth_key_id: i64,
    session_id: i64,
    packet: &[u8],
) -> Result<(i64, i32, Vec<u8>), PacketError> {
    if packet.len() == 4 {
        let code = i32::from_le_bytes(packet[0..4].try_into().unwrap());
        return Err(PacketError::TransportError(code));
    }
    if packet.len() < AUTH_KEY_ID_SIZE + MSG_KEY_SIZE + 32 {
        return Err(PacketError::PacketTooShort);
    }

    let recv_auth_key_id = i64::from_le_bytes(packet[0..8].try_into().unwrap());
    if recv_auth_key_id != auth_key_id {
        return Err(PacketError::AuthKeyIdMismatch);
    }

    let mut msg_key = [0u8; 16];
    msg_key.copy_from_slice(&packet[8..24]);

    let (aes_key, aes_iv) = derive_keys(auth_key, &msg_key, false);

    if (packet.len() - 24) % 16 != 0 {
        return Err(PacketError::InvalidPayloadLength);
    }
    let encrypted_part = &packet[24..];
    let plaintext = aes_256_ige_decrypt(encrypted_part, &aes_key, &aes_iv)
        .map_err(|e| PacketError::Crypto(e.to_string()))?;

    // Verify msg_key: substr(SHA256(substr(auth_key, 88 + 8, 32) + plaintext), 8, 16)
    let mut hasher = Sha256::new();
    hasher.update(&auth_key[96..128]);
    hasher.update(&plaintext);
    let hash = hasher.finalize();
    if &hash[8..24] != msg_key {
        return Err(PacketError::IntegrityCheckFailed);
    }

    if plaintext.len() < 32 {
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
    if data_len % 4 != 0 || plaintext.len() < 32 + data_len {
        return Err(PacketError::InvalidPayloadLength);
    }
    let padding = plaintext.len() - 32 - data_len;
    if !(MIN_PADDING..=1024).contains(&padding) {
        return Err(PacketError::InvalidPayloadLength);
    }

    let message_data = plaintext[32..32 + data_len].to_vec();
    Ok((msg_id, seq_no, message_data))
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 256] = [7u8; 256];

    fn roundtrip(session: i64, msg_id: i64, data: &[u8]) -> Result<(i64, i32, Vec<u8>), PacketError> {
        // Server->client direction uses x=8 keys, so craft the packet by hand.
        let mut pt = Vec::new();
        pt.extend_from_slice(&1i64.to_le_bytes());
        pt.extend_from_slice(&session.to_le_bytes());
        pt.extend_from_slice(&msg_id.to_le_bytes());
        pt.extend_from_slice(&1i32.to_le_bytes());
        pt.extend_from_slice(&(data.len() as u32).to_le_bytes());
        pt.extend_from_slice(data);
        while pt.len() % 16 != 0 || pt.len() < 32 + data.len() + 12 {
            pt.push(0xab);
        }
        let mut h = Sha256::new();
        h.update(&KEY[96..128]);
        h.update(&pt);
        let mut msg_key = [0u8; 16];
        msg_key.copy_from_slice(&h.finalize()[8..24]);
        let (k, iv) = derive_keys(&KEY, &msg_key, false);
        let enc = aes_256_ige_encrypt(&pt, &k, &iv).unwrap();
        let mut pkt = 42i64.to_le_bytes().to_vec();
        pkt.extend_from_slice(&msg_key);
        pkt.extend_from_slice(&enc);
        unpack_encrypted_message(&KEY, 42, 99, &pkt)
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
}
