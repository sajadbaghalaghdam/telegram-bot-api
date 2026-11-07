use bytes::BytesMut;
use num_bigint::BigUint;
use rand::RngCore;
use sha1::{Digest, Sha1};
use std::io::{Error, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};

use super::crypto::{
    aes_256_ige_decrypt, aes_256_ige_encrypt, derive_tmp_keys, factorize, rsa_pad_encrypt,
    TELEGRAM_RSA_KEYS,
};
use super::tl::{read_bytes, read_int, read_int128, read_long, write_bytes, write_int, write_int128, write_int256, write_long};
use super::transport::IntermediateTransport;

/// Handshake result holding the 256-byte auth_key, initial server_salt, and server time offset.
pub struct HandshakeResult {
    pub auth_key: [u8; 256],
    pub server_salt: i64,
    pub time_offset: i64,
}

pub struct MtProtoHandshake<'a> {
    transport: &'a mut IntermediateTransport,
    dc_id: i32,
}

impl<'a> MtProtoHandshake<'a> {
    pub fn new(transport: &'a mut IntermediateTransport) -> Self {
        Self { transport, dc_id: 2 }
    }

    pub fn with_dc(transport: &'a mut IntermediateTransport, dc_id: i32) -> Self {
        Self { transport, dc_id }
    }

    /// Executes the full 6-step MTProto 2.0 Diffie-Hellman handshake with Telegram DC
    /// (translated from td/mtproto/Handshake.cpp & DhHandshake.cpp).
    pub async fn run(&mut self) -> Result<HandshakeResult, Error> {
        let mut nonce = [0u8; 16];
        rand::thread_rng().fill_bytes(&mut nonce);

        // 1. Send req_pq_multi#be7e8ef1(nonce)
        let mut req_pq = BytesMut::with_capacity(20);
        write_int(&mut req_pq, 0xbe7e8ef1u32 as i32);
        write_int128(&mut req_pq, &nonce);

        let unencrypted = self.wrap_unencrypted(&req_pq);
        self.transport.send_packet(&unencrypted).await?;

        // 2. Read resPQ#05162463 response
        let mut buf = Vec::new();
        self.transport.read_packet(&mut buf).await?;
        let payload = Self::unwrap_unencrypted(&buf)?;

        let mut cursor = payload;
        let res_pq_ctor = read_int(&mut cursor).map_err(Self::tl_err)? as u32;
        if res_pq_ctor != 0x05162463 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unexpected resPQ constructor: 0x{:08x}", res_pq_ctor),
            ));
        }

        let resp_nonce = read_int128(&mut cursor).map_err(Self::tl_err)?;
        if resp_nonce != nonce {
            return Err(Error::new(ErrorKind::InvalidData, "resPQ nonce mismatch"));
        }
        let server_nonce = read_int128(&mut cursor).map_err(Self::tl_err)?;
        let pq_slice = read_bytes(&mut cursor).map_err(Self::tl_err)?;
        if pq_slice.is_empty() || pq_slice.len() > 8 {
            return Err(Error::new(ErrorKind::InvalidData, "Invalid pq length"));
        }
        let mut pq_be = [0u8; 8];
        pq_be[8 - pq_slice.len()..].copy_from_slice(pq_slice);
        let pq = u64::from_be_bytes(pq_be);

        let vec_ctor = read_int(&mut cursor).map_err(Self::tl_err)? as u32;
        if vec_ctor != 0x1cb5c415 {
            return Err(Error::new(ErrorKind::InvalidData, "Expected vector#1cb5c415 in resPQ"));
        }
        let fp_count = read_int(&mut cursor).map_err(Self::tl_err)?;
        let mut matched_key = None;
        for _ in 0..fp_count {
            let fp = read_long(&mut cursor).map_err(Self::tl_err)?;
            if matched_key.is_none() {
                for k in TELEGRAM_RSA_KEYS {
                    if k.fingerprint == fp {
                        matched_key = Some(k);
                        break;
                    }
                }
            }
        }
        let rsa_key = matched_key.unwrap_or(&TELEGRAM_RSA_KEYS[0]);

        // Factorize pq into p and q (p <= q) and strip leading zero bytes (matching as_big_endian_string in tdutils/crypto.cpp)
        let (p, q) = factorize(pq);
        let p_be = p.to_be_bytes();
        let q_be = q.to_be_bytes();
        let p_start = p_be.iter().position(|&b| b != 0).unwrap_or(7);
        let q_start = q_be.iter().position(|&b| b != 0).unwrap_or(7);
        let p_bytes = &p_be[p_start..];
        let q_bytes = &q_be[q_start..];

        // 3. Prepare p_q_inner_data_dc#a9f55f95 and RSA_PAD
        let mut new_nonce = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut new_nonce);

        let mut inner_data = BytesMut::with_capacity(144);
        write_int(&mut inner_data, 0xa9f55f95u32 as i32);
        write_bytes(&mut inner_data, pq_slice);
        write_bytes(&mut inner_data, p_bytes);
        write_bytes(&mut inner_data, q_bytes);
        write_int128(&mut inner_data, &nonce);
        write_int128(&mut inner_data, &server_nonce);
        write_int256(&mut inner_data, &new_nonce);
        write_int(&mut inner_data, self.dc_id);

        let encrypted_data = rsa_pad_encrypt(&inner_data, rsa_key);

        // 4. Send req_DH_params#d712e4be
        let mut req_dh = BytesMut::with_capacity(360);
        write_int(&mut req_dh, 0xd712e4beu32 as i32);
        write_int128(&mut req_dh, &nonce);
        write_int128(&mut req_dh, &server_nonce);
        write_bytes(&mut req_dh, p_bytes);
        write_bytes(&mut req_dh, q_bytes);
        write_long(&mut req_dh, rsa_key.fingerprint);
        write_bytes(&mut req_dh, &encrypted_data);

        let unencrypted_dh = self.wrap_unencrypted(&req_dh);
        self.transport.send_packet(&unencrypted_dh).await?;

        // 5. Read server_DH_params_ok#d0e8075c
        self.transport.read_packet(&mut buf).await?;
        let dh_payload = Self::unwrap_unencrypted(&buf)?;
        let mut dh_cursor = dh_payload;

        let dh_ctor = read_int(&mut dh_cursor).map_err(Self::tl_err)? as u32;
        if dh_ctor != 0xd0e8075c {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unexpected server_DH_params constructor: 0x{:08x}", dh_ctor),
            ));
        }
        let dh_nonce = read_int128(&mut dh_cursor).map_err(Self::tl_err)?;
        let dh_server_nonce = read_int128(&mut dh_cursor).map_err(Self::tl_err)?;
        if dh_nonce != nonce || dh_server_nonce != server_nonce {
            return Err(Error::new(ErrorKind::InvalidData, "server_DH_params nonce mismatch"));
        }
        let encrypted_answer = read_bytes(&mut dh_cursor).map_err(Self::tl_err)?;
        if encrypted_answer.len() % 16 != 0 {
            return Err(Error::new(ErrorKind::InvalidData, "encrypted_answer not 16-byte aligned"));
        }

        let (tmp_aes_key, tmp_aes_iv) = derive_tmp_keys(&server_nonce, &new_nonce);
        let answer = aes_256_ige_decrypt(encrypted_answer, &tmp_aes_key, &tmp_aes_iv)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        if answer.len() < 36 {
            return Err(Error::new(ErrorKind::InvalidData, "Decrypted DH answer too short"));
        }
        let answer_sha1 = &answer[0..20];
        let mut inner_cursor = &answer[20..];

        let inner_ctor = read_int(&mut inner_cursor).map_err(Self::tl_err)? as u32;
        if inner_ctor != 0xb5890dba {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Expected server_DH_inner_data#b5890dba, got 0x{:08x}", inner_ctor),
            ));
        }
        let inner_nonce = read_int128(&mut inner_cursor).map_err(Self::tl_err)?;
        let inner_server_nonce = read_int128(&mut inner_cursor).map_err(Self::tl_err)?;
        if inner_nonce != nonce || inner_server_nonce != server_nonce {
            return Err(Error::new(ErrorKind::InvalidData, "server_DH_inner_data nonce mismatch"));
        }
        let g = read_int(&mut inner_cursor).map_err(Self::tl_err)?;
        let dh_prime_bytes = read_bytes(&mut inner_cursor).map_err(Self::tl_err)?;
        let g_a_bytes = read_bytes(&mut inner_cursor).map_err(Self::tl_err)?;
        let server_time = read_int(&mut inner_cursor).map_err(Self::tl_err)?;

        let pad_len = inner_cursor.len();
        if pad_len >= 16 {
            return Err(Error::new(ErrorKind::InvalidData, "Excessive padding in server_DH_inner_data"));
        }
        let inner_len = answer.len() - 20 - pad_len;
        let mut h = Sha1::new();
        h.update(&answer[20..20 + inner_len]);
        if h.finalize().as_slice() != answer_sha1 {
            return Err(Error::new(ErrorKind::InvalidData, "server_DH_inner_data SHA1 mismatch"));
        }

        // Compute Diffie-Hellman g_b = g^b mod p and auth_key = g_a^b mod p
        let mut b_raw = [0u8; 256];
        rand::thread_rng().fill_bytes(&mut b_raw);
        let b = BigUint::from_bytes_be(&b_raw);
        let prime = BigUint::from_bytes_be(dh_prime_bytes);
        let g_big = BigUint::from(g as u32);
        let g_a = BigUint::from_bytes_be(g_a_bytes);

        let g_b_vec = g_big.modpow(&b, &prime).to_bytes_be();
        let mut g_b_padded = [0u8; 256];
        g_b_padded[256 - g_b_vec.len()..].copy_from_slice(&g_b_vec);

        let g_ab_vec = g_a.modpow(&b, &prime).to_bytes_be();
        let mut auth_key = [0u8; 256];
        auth_key[256 - g_ab_vec.len()..].copy_from_slice(&g_ab_vec);

        // 6. Send set_client_DH_params#f5045f1f
        let mut client_inner = BytesMut::with_capacity(320);
        write_int(&mut client_inner, 0x6643b654u32 as i32);
        write_int128(&mut client_inner, &nonce);
        write_int128(&mut client_inner, &server_nonce);
        write_long(&mut client_inner, 0); // retry_id = 0
        write_bytes(&mut client_inner, &g_b_padded);

        let mut client_hasher = Sha1::new();
        client_hasher.update(&client_inner);
        let client_sha1 = client_hasher.finalize();

        let unpadded_len = 20 + client_inner.len();
        let padded_len = (unpadded_len + 15) & !15;
        let mut client_plain = vec![0u8; padded_len];
        client_plain[0..20].copy_from_slice(&client_sha1);
        client_plain[20..unpadded_len].copy_from_slice(&client_inner);
        rand::thread_rng().fill_bytes(&mut client_plain[unpadded_len..]);

        let client_encrypted = aes_256_ige_encrypt(&client_plain, &tmp_aes_key, &tmp_aes_iv)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        let mut set_dh = BytesMut::with_capacity(64 + client_encrypted.len());
        write_int(&mut set_dh, 0xf5045f1fu32 as i32);
        write_int128(&mut set_dh, &nonce);
        write_int128(&mut set_dh, &server_nonce);
        write_bytes(&mut set_dh, &client_encrypted);

        let unencrypted_set_dh = self.wrap_unencrypted(&set_dh);
        self.transport.send_packet(&unencrypted_set_dh).await?;

        // 7. Read dh_gen_ok#3bcbf734
        self.transport.read_packet(&mut buf).await?;
        let gen_payload = Self::unwrap_unencrypted(&buf)?;
        let mut gen_cursor = gen_payload;
        let gen_ctor = read_int(&mut gen_cursor).map_err(Self::tl_err)? as u32;
        if gen_ctor != 0x3bcbf734 {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                format!("DH generation failed with constructor: 0x{:08x}", gen_ctor),
            ));
        }
        let gen_nonce = read_int128(&mut gen_cursor).map_err(Self::tl_err)?;
        let gen_server_nonce = read_int128(&mut gen_cursor).map_err(Self::tl_err)?;
        if gen_nonce != nonce || gen_server_nonce != server_nonce {
            return Err(Error::new(ErrorKind::InvalidData, "dh_gen_ok nonce mismatch"));
        }

        let mut salt_bytes = [0u8; 8];
        for i in 0..8 {
            salt_bytes[i] = new_nonce[i] ^ server_nonce[i];
        }
        let server_salt = i64::from_le_bytes(salt_bytes);

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let time_offset = (server_time as i64) - now_secs;

        Ok(HandshakeResult {
            auth_key,
            server_salt,
            time_offset,
        })
    }

    fn wrap_unencrypted(&self, payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20 + payload.len());
        buf.extend_from_slice(&0i64.to_le_bytes()); // auth_key_id = 0
        let msg_id = ((SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64)
            << 32)
            & !3i64;
        buf.extend_from_slice(&msg_id.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(payload);
        buf
    }

    fn unwrap_unencrypted(buf: &[u8]) -> Result<&[u8], Error> {
        if buf.len() == 4 {
            let err_code = i32::from_le_bytes(buf[0..4].try_into().unwrap());
            return Err(Error::new(
                ErrorKind::ConnectionRefused,
                format!("MTProto transport error: {}", err_code),
            ));
        }
        if buf.len() < 20 {
            return Err(Error::new(ErrorKind::InvalidData, "Unencrypted packet shorter than 20 bytes"));
        }
        let auth_key_id = i64::from_le_bytes(buf[0..8].try_into().unwrap());
        if auth_key_id != 0 {
            return Err(Error::new(ErrorKind::InvalidData, "Expected auth_key_id = 0 during handshake"));
        }
        let len = u32::from_le_bytes(buf[16..20].try_into().unwrap()) as usize;
        if buf.len() < 20 + len {
            return Err(Error::new(ErrorKind::InvalidData, "Unencrypted payload truncated"));
        }
        Ok(&buf[20..20 + len])
    }

    fn tl_err(e: super::tl::TLError) -> Error {
        Error::new(ErrorKind::InvalidData, e.to_string())
    }
}
