use crate::mtproto::crypto::{aes_256_ige_decrypt, aes_256_ige_encrypt, derive_keys};
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
    #[error("Crypto error: {0}")]
    Crypto(String),
}

/// Packs and encrypts a message according to MTProto 2.0 specification.
pub fn pack_encrypted_message(
    auth_key: &[u8; 256],
    auth_key_id: i64,
    server_salt: i64,
    session_id: i64,
    msg_id: i64,
    seq_no: i32,
    message_data: &[u8],
) -> Result<Vec<u8>, PacketError> {
    // 1. Calculate exact padding length
    let header_size = 32;
    let current_len = header_size + message_data.len();
    
    let mut padding_len = AES_BLOCK_SIZE - (current_len % AES_BLOCK_SIZE);
    if padding_len < MIN_PADDING {
        padding_len += ((MIN_PADDING - padding_len + AES_BLOCK_SIZE - 1) / AES_BLOCK_SIZE) * AES_BLOCK_SIZE;
    }

    // 2. Construct Plaintext Payload
    let mut plaintext = Vec::with_capacity(current_len + padding_len);
    plaintext.extend_from_slice(&server_salt.to_le_bytes());
    plaintext.extend_from_slice(&session_id.to_le_bytes());
    plaintext.extend_from_slice(&msg_id.to_le_bytes());
    plaintext.extend_from_slice(&seq_no.to_le_bytes());
    plaintext.extend_from_slice(&(message_data.len() as u32).to_le_bytes());
    plaintext.extend_from_slice(message_data);
    plaintext.resize(current_len + padding_len, 0);

    // 3. Calculate msg_key
    let mut hasher = Sha256::new();
    hasher.update(&auth_key[88..120]);
    hasher.update(&plaintext);
    let hash_result = hasher.finalize();
    let mut msg_key = [0u8; 16];
    msg_key.copy_from_slice(&hash_result[0..MSG_KEY_SIZE]);

    // 4. Derive AES keys (is_client = true)
    let (aes_key, aes_iv) = derive_keys(auth_key, &msg_key, true);

    // 5. Encrypt
    let encrypted_data = aes_256_ige_encrypt(&plaintext, &aes_key, &aes_iv)
        .map_err(|e| PacketError::Crypto(e.to_string()))?;

    // 6. Assemble final packet: auth_key_id (8) + msg_key (16) + encrypted_data
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
    packet: &[u8],
) -> Result<(i64, i32, Vec<u8>), PacketError> {
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

    let plaintext = aes_256_ige_decrypt(&packet[24..], &aes_key, &aes_iv)
        .map_err(|e| PacketError::Crypto(e.to_string()))?;

    // Verify msg_key
    let mut hasher = Sha256::new();
    hasher.update(&auth_key[96..128]);
    hasher.update(&plaintext);
    let hash = hasher.finalize();
    if &hash[0..16] != msg_key {
        return Err(PacketError::IntegrityCheckFailed);
    }

    if plaintext.len() < 32 {
        return Err(PacketError::InvalidPayloadLength);
    }

    let msg_id = i64::from_le_bytes(plaintext[16..24].try_into().unwrap());
    let seq_no = i32::from_le_bytes(plaintext[24..28].try_into().unwrap());
    let data_len = u32::from_le_bytes(plaintext[28..32].try_into().unwrap()) as usize;

    if plaintext.len() < 32 + data_len {
        return Err(PacketError::InvalidPayloadLength);
    }

    let message_data = plaintext[32..32 + data_len].to_vec();
    Ok((msg_id, seq_no, message_data))
}
