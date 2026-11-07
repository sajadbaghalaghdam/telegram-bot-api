#![allow(dead_code)]
use sha1::{Digest, Sha1};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SessionError {
    #[error("System time went backwards")]
    TimeRegression,
}

/// Represents a secure MTProto session state for a single client connection.
/// Designed for minimal memory footprint and high-performance state transitions.
/// 
/// Note: This struct is intentionally NOT Clone to prevent expensive copying of 
/// the 256-byte auth_key and to ensure state consistency. Use Arc<Mutex<MtProtoSession>> 
/// for shared access.
#[derive(Debug)]
pub struct MtProtoSession {
    auth_key: [u8; 256],
    auth_key_id: i64,
    pub(crate) server_salt: i64,
    pub(crate) session_id: i64,
    seq_no: i32,
    last_msg_id: i64,
}

impl MtProtoSession {
    /// Creates a new MTProto session.
    /// 
    /// # Arguments
    /// * `auth_key` - The 256-byte shared secret.
    /// * `server_salt` - The salt provided by the server during the handshake.
    pub fn new(auth_key: [u8; 256], server_salt: i64) -> Self {
        // Calculate auth_key_id: lower 64 bits (LE) of SHA1(auth_key)
        let mut hasher = Sha1::new();
        hasher.update(auth_key);
        let hash = hasher.finalize();
        
        // Optimization: Direct conversion from slice to i64 using try_into
        let auth_key_id = i64::from_le_bytes(
            hash[0..8].try_into().expect("SHA1 hash must be at least 8 bytes")
        );

        // Generate a random session_id for this connection
        let session_id = rand::random::<i64>();

        Self {
            auth_key,
            auth_key_id,
            server_salt,
            session_id,
            seq_no: 0,
            last_msg_id: 0,
        }
    }

    /// Returns the authentication key.
    pub fn auth_key(&self) -> &[u8; 256] {
        &self.auth_key
    }

    /// Returns the authentication key ID.
    pub fn auth_key_id(&self) -> i64 {
        self.auth_key_id
    }

    /// Generates a strictly increasing message ID.
    /// 
    /// Formula: (unix_timestamp_seconds * 2^32) + (nanoseconds / 4)
    /// Ensures the ID is strictly greater than the previous one and is odd for client messages.
    pub fn generate_msg_id(&mut self) -> Result<i64, SessionError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| SessionError::TimeRegression)?;

        let secs = now.as_secs() as i64;
        let nanos = now.subsec_nanos() as i64;

        // MTProto msg_id construction
        let mut msg_id = (secs << 32) + (nanos / 4);

        // Ensure strictly increasing
        if msg_id <= self.last_msg_id {
            msg_id = self.last_msg_id + 1;
        }

        // Ensure msg_id is odd for client-to-server messages
        if msg_id % 2 == 0 {
            msg_id += 1;
        }

        self.last_msg_id = msg_id;
        Ok(msg_id)
    }

    /// Returns the current sequence number and increments it if the message is content-related.
    /// 
    /// # Arguments
    /// * `is_content_related` - If true, the sequence number is incremented for the next message.
    pub fn next_seq_no(&mut self, is_content_related: bool) -> i32 {
        let current = self.seq_no;
        if is_content_related {
            // Sequence numbers wrap around at 2^31 - 1 (signed i32)
            self.seq_no = self.seq_no.wrapping_add(1);
        }
        current
    }

    /// Getter for the current sequence number without incrementing.
    pub fn current_seq_no(&self) -> i32 {
        self.seq_no
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_initialization() {
        let key = [0u8; 256];
        let salt = 123456789;
        let session = MtProtoSession::new(key, salt);
        
        assert_eq!(session.server_salt, salt);
        // SHA1 of 256 zeros starts with 0x7c 0x1d ...
        // 0x1d7c... in LE i64 should be consistent
        assert_ne!(session.auth_key_id(), 0);
    }

    #[test]
    fn test_msg_id_monotonicity() {
        let mut session = MtProtoSession::new([0u8; 256], 0);
        let id1 = session.generate_msg_id().unwrap();
        let id2 = session.generate_msg_id().unwrap();
        
        assert!(id2 > id1);
        assert_eq!(id1 % 2, 1);
        assert_eq!(id2 % 2, 1);
    }

    #[test]
    fn test_seq_no_logic() {
        let mut session = MtProtoSession::new([0u8; 256], 0);
        
        // 1. Content related: returns 0, state becomes 1
        assert_eq!(session.next_seq_no(true), 0);
        
        // 2. Content related: returns 1, state becomes 2
        assert_eq!(session.next_seq_no(true), 1);
        
        // 3. Not content related: returns 2, state stays 2
        assert_eq!(session.next_seq_no(false), 2);
        
        // 4. Not content related: returns 2, state stays 2
        assert_eq!(session.next_seq_no(false), 2);
        
        // 5. Content related: returns 2, state becomes 3
        assert_eq!(session.next_seq_no(true), 2);
        
        // Final check: state should now be 3
        assert_eq!(session.current_seq_no(), 3);
    }
}