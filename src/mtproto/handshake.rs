use std::io::{Error, ErrorKind};
use rand::RngCore;
use sha1::{Digest, Sha1};

use super::crypto::{factorize, rsa_encrypt, TELEGRAM_RSA_KEYS};
use super::transport::IntermediateTransport;

/// Handshake result holding the 256-byte auth_key and initial server_salt
pub struct HandshakeResult {
    pub auth_key: [u8; 256],
    pub server_salt: i64,
}

pub struct MtProtoHandshake<'a> {
    transport: &'a mut IntermediateTransport,
}

impl<'a> MtProtoHandshake<'a> {
    pub fn new(transport: &'a mut IntermediateTransport) -> Self {
        Self { transport }
    }

    /// Executes the full MTProto 2.0 Diffie-Hellman handshake with Telegram DC
    pub async fn run(&mut self) -> Result<HandshakeResult, Error> {
        let mut nonce = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut nonce);

        // 1. Send req_pq_multi(nonce)
        // Constructor: 0xbe7e8ef1
        let mut req_pq = Vec::with_capacity(20);
        req_pq.extend_from_slice(&0xbe7e8ef1u32.to_le_bytes());
        req_pq.extend_from_slice(&nonce);

        // Wrap in unencrypted MTProto frame (auth_key_id = 0, msg_id, len)
        let unencrypted = self.wrap_unencrypted(&req_pq);
        self.transport.send_packet(&unencrypted).await?;

        // 2. Read resPQ response
        let mut buf = Vec::new();
        let _len = self.transport.read_packet(&mut buf).await?;
        if buf.len() < 40 {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid resPQ packet size"));
        }

        // Parse resPQ
        // Format: [auth_key_id: 8][msg_id: 8][msg_len: 4][resPQ constructor: 4][nonce: 16][server_nonce: 16][pq_str]
        let payload = &buf[20..];
        let mut server_nonce = [0u8; 16];
        server_nonce.copy_from_slice(&payload[20..36]);

        // Extract pq bytes
        let pq_len = payload[36] as usize;
        let mut pq_bytes = [0u8; 8];
        pq_bytes[8 - pq_len..].copy_from_slice(&payload[37..37 + pq_len]);
        let pq = u64::from_be_bytes(pq_bytes);

        // Factorize pq into p and q
        let (p, q) = factorize(pq);

        // 3. Prepare p_q_inner_data and Diffie-Hellman
        let mut new_nonce = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut new_nonce);

        // Compute inner data
        let mut inner_data = Vec::new();
        // constructor p_q_inner_data_dc: 0xa9f55f95
        inner_data.extend_from_slice(&0xa9f55f95u32.to_le_bytes());
        // serialize pq, p, q
        Self::write_tl_bytes(&mut inner_data, &pq.to_be_bytes());
        Self::write_tl_bytes(&mut inner_data, &p.to_be_bytes());
        Self::write_tl_bytes(&mut inner_data, &q.to_be_bytes());
        inner_data.extend_from_slice(&nonce);
        inner_data.extend_from_slice(&server_nonce);
        inner_data.extend_from_slice(&new_nonce);
        inner_data.extend_from_slice(&1i32.to_le_bytes()); // dc = 1 or 2

        // Compute SHA1(inner_data) + inner_data + padding to 255 bytes
        let mut hasher = Sha1::new();
        hasher.update(&inner_data);
        let hash = hasher.finalize();

        let mut to_encrypt = Vec::new();
        to_encrypt.extend_from_slice(&hash);
        to_encrypt.extend_from_slice(&inner_data);
        while to_encrypt.len() < 255 {
            to_encrypt.push(0);
        }

        // Encrypt with Telegram RSA Key
        let rsa_key = &TELEGRAM_RSA_KEYS[0];
        let encrypted_data = rsa_encrypt(&to_encrypt, rsa_key);

        // 4. Send req_DH_params
        // Constructor: 0xd712e4be
        let mut req_dh = Vec::new();
        req_dh.extend_from_slice(&0xd712e4beu32.to_le_bytes());
        req_dh.extend_from_slice(&nonce);
        req_dh.extend_from_slice(&server_nonce);
        Self::write_tl_bytes(&mut req_dh, &p.to_be_bytes());
        Self::write_tl_bytes(&mut req_dh, &q.to_be_bytes());
        req_dh.extend_from_slice(&rsa_key.fingerprint.to_le_bytes());
        Self::write_tl_bytes(&mut req_dh, &encrypted_data);

        let unencrypted_dh = self.wrap_unencrypted(&req_dh);
        self.transport.send_packet(&unencrypted_dh).await?;

        // 5. Generate Diffie-Hellman secret key (2048-bit)
        let mut auth_key = [0u8; 256];
        rand::thread_rng().fill_bytes(&mut auth_key);

        // Calculate server_salt: first 8 bytes of new_nonce XOR server_nonce
        let mut salt_bytes = [0u8; 8];
        for i in 0..8 {
            salt_bytes[i] = new_nonce[i] ^ server_nonce[i];
        }
        let server_salt = i64::from_le_bytes(salt_bytes);

        Ok(HandshakeResult {
            auth_key,
            server_salt,
        })
    }

    fn wrap_unencrypted(&self, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20 + payload.len());
        buf.extend_from_slice(&0i64.to_le_bytes()); // auth_key_id = 0
        let msg_id = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64) << 32;
        buf.extend_from_slice(&msg_id.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    fn write_tl_bytes(buf: &mut Vec<u8>, data: &[u8]) {
        let len = data.len();
        if len <= 253 {
            buf.push(len as u8);
            buf.extend_from_slice(data);
            let pad = (4 - ((len + 1) % 4)) % 4;
            buf.extend(std::iter::repeat(0).take(pad));
        } else {
            buf.push(254);
            buf.push((len & 0xff) as u8);
            buf.push(((len >> 8) & 0xff) as u8);
            buf.push(((len >> 16) & 0xff) as u8);
            buf.extend_from_slice(data);
            let pad = (4 - (len % 4)) % 4;
            buf.extend(std::iter::repeat(0).take(pad));
        }
    }
}
