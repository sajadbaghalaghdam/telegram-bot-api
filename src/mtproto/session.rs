use sha1::{Digest, Sha1};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("System time went backwards")]
    TimeRegression,
}

/// Represents a secure MTProto session state for a single client connection.
/// Translated from td/mtproto/AuthData.cpp & DhHandshake.cpp.
#[derive(Debug)]
pub struct MtProtoSession {
    auth_key: [u8; 256],
    auth_key_id: i64,
    pub(crate) server_salt: i64,
    pub(crate) session_id: i64,
    pub(crate) time_offset: i64,
    seq_no: i32,
    last_msg_id: i64,
}

impl MtProtoSession {
    pub fn with_time_offset(auth_key: [u8; 256], server_salt: i64, time_offset: i64) -> Self {
        // Calculate auth_key_id: lower 64 bits (bytes 12..20) of SHA1(auth_key)
        // Matches DhHandshake::calc_key_id: as<int64>(auth_key_sha1.raw + 12)
        let mut hasher = Sha1::new();
        hasher.update(auth_key);
        let hash = hasher.finalize();

        let auth_key_id = i64::from_le_bytes(
            hash[12..20].try_into().expect("SHA1 hash is 20 bytes"),
        );

        let session_id = rand::random::<i64>();

        Self {
            auth_key,
            auth_key_id,
            server_salt,
            session_id,
            time_offset,
            seq_no: 0,
            last_msg_id: 0,
        }
    }

    pub fn auth_key(&self) -> &[u8; 256] {
        &self.auth_key
    }

    pub fn auth_key_id(&self) -> i64 {
        self.auth_key_id
    }

    /// Generates a strictly increasing message ID divisible by 4 (client-to-server).
    /// Matches AuthData::next_message_id in td/mtproto/AuthData.cpp.
    pub fn generate_msg_id(&mut self) -> Result<i64, SessionError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SessionError::TimeRegression)?;

        let secs = (now.as_secs() as i64).saturating_add(self.time_offset);
        let nanos = now.subsec_nanos() as i64;

        let mut msg_id = ((secs << 32) + ((nanos << 2) / 10)) & !3i64;

        if msg_id <= self.last_msg_id {
            msg_id = (self.last_msg_id + 4) & !3i64;
        }

        self.last_msg_id = msg_id;
        Ok(msg_id)
    }

    /// Returns the MTProto sequence number (`seq_no_ | 1` if content-related, advancing by 2).
    /// Matches AuthData::next_seq_no in td/mtproto/AuthData.h.
    pub fn next_seq_no(&mut self, is_content_related: bool) -> i32 {
        let mut res = self.seq_no;
        if is_content_related {
            res |= 1;
            self.seq_no = self.seq_no.wrapping_add(2);
        }
        res
    }
}
