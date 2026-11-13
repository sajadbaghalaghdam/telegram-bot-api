use num_bigint::BigUint;
use rand::RngCore;
use sha2::{Digest, Sha256};

use super::ige::aes_256_ige_encrypt;

/// Telegram Production RSA Public Key for MTProto 2.0 Handshake
pub struct TelegramRsaKey {
    pub fingerprint: i64,
    pub n: &'static str,
    pub e: u32,
}

/// Official Telegram Production Public Keys (from td/telegram/net/PublicRsaKeySharedMain.cpp)
pub static TELEGRAM_RSA_KEYS: &[TelegramRsaKey] = &[
    TelegramRsaKey {
        // Primary TDLib Main RSA Public Key (0xd09d1d85de64fd85)
        fingerprint: -3414540481677951611,
        n: "e8bb3305c0b52c6cf2afdf7637313489e63e05268e5badb601af417786472e5f93b85438968e20e6729a301c0afc121bf7151f834436f7fda680847a66bf64accec78ee21c0b316f0edafe2f41908da7bd1f4a5107638eeb67040ace472a14f90d9f7c2b7def99688ba3073adb5750bb02964902a359fe745d8170e36876d4fd8a5d41b2a76cbff9a13267eb9580b2d06d10357448d20d9da2191cb5d8c93982961cdfdeda629e37f1fb09a0722027696032fe61ed663db7a37f6f263d370f69db53a0dc0a1748bdaaff6209d5645485e6e001d1953255757e4b8e42813347b11da6ab500fd0ace7e6dfa3736199ccaf9397ed0745a427dcfa6cd67bcb1acff3",
        e: 65537,
    },
    TelegramRsaKey {
        fingerprint: -4344795326620583105, // 0xc3b42c042e0e3b3f
        n: "c150023e2f70db7985ded064759cfecf0af328e69a41da94ffb29293f3ff6308b01b45dd8d36ca3d8883ec92757da5ac0e673ecdd3718029815076bd23fb04b275f3f22f877ff235777e7ee5e0780f38fbb7df4ac7614a5e2023f251977261fa9724e9d078e9b3b7ff6a0e69e57e404121a804435f4324e95f3122684124a197",
        e: 65537,
    },
    TelegramRsaKey {
        fingerprint: -2771036070624869502, // 0xd995644a69622d82
        n: "c2c51b9cfb0cf4c64360e527ba32c9183be7173b9e4a30560738d8f344bf429b9f0d7e68c07641328906be383a17e0892c2daff20c15112fa7d10526eefb4081efbcab72583d8a43f80c657a7d43f05db17d23d8c11e74fec8b746a51d8b9e6e44b93b4f535804b40be155e82b7941fa44ec52479e09968a514d3a242c7decfb",
        e: 65537,
    },
];

/// Raw RSA (m^e mod n); unused outside tests.
#[cfg(test)]
fn rsa_encrypt(data: &[u8], key: &TelegramRsaKey) -> Result<Vec<u8>, String> {
    let n = BigUint::parse_bytes(key.n.as_bytes(), 16).expect("Valid hex string for RSA modulus");
    let e = BigUint::from(key.e);
    let m = BigUint::from_bytes_be(data);
    if m >= n {
        return Err("RSA input value must be smaller than the modulus".to_string());
    }

    let c = m.modpow(&e, &n);
    let mut bytes = c.to_bytes_be();

    if bytes.len() < 256 {
        let mut padded = vec![0u8; 256 - bytes.len()];
        padded.extend(bytes);
        bytes = padded;
    }

    Ok(bytes)
}

/// MTProto 2.0 RSA_PAD encryption (translated from td/mtproto/Handshake.cpp:135-158)
pub fn rsa_pad_encrypt(data: &[u8], key: &TelegramRsaKey) -> Result<[u8; 256], String> {
    if data.len() > 144 {
        return Err(format!("p_q_inner_data is {} bytes, exceeds 144", data.len()));
    }

    let n = BigUint::parse_bytes(key.n.as_bytes(), 16).expect("Valid hex string for RSA modulus");
    let e = BigUint::from(key.e);

    let mut data_padded = [0u8; 192];
    data_padded[..data.len()].copy_from_slice(data);
    rand::thread_rng().fill_bytes(&mut data_padded[data.len()..]);

    let mut data_reversed = data_padded;
    data_reversed.reverse();

    loop {
        let mut temp_key = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut temp_key);

        // data_with_hash = reverse(data_padded) || SHA256(temp_key || data_padded)
        let mut hasher = Sha256::new();
        hasher.update(&temp_key);
        hasher.update(&data_padded);
        let hash = hasher.finalize();

        let mut data_with_hash = [0u8; 224];
        data_with_hash[..192].copy_from_slice(&data_reversed);
        data_with_hash[192..224].copy_from_slice(&hash);

        let zero_iv = [0u8; 32];
        let aes_encrypted = aes_256_ige_encrypt(&data_with_hash, &temp_key, &zero_iv)
            .expect("224 is divisible by 16");

        let mut hasher2 = Sha256::new();
        hasher2.update(&aes_encrypted);
        let hash2 = hasher2.finalize();

        let mut key_aes_encrypted = [0u8; 256];
        for i in 0..32 {
            key_aes_encrypted[i] = temp_key[i] ^ hash2[i];
        }
        key_aes_encrypted[32..256].copy_from_slice(&aes_encrypted);

        let x = BigUint::from_bytes_be(&key_aes_encrypted);
        if x < n {
            let y = x.modpow(&e, &n);
            let y_bytes = y.to_bytes_be();
            let mut out = [0u8; 256];
            let offset = 256 - y_bytes.len();
            out[offset..].copy_from_slice(&y_bytes);
            return Ok(out);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rsa_encrypt_rejects_value_ge_modulus() {
        let key = &TELEGRAM_RSA_KEYS[0];
        let n = BigUint::parse_bytes(key.n.as_bytes(), 16).unwrap();
        assert!(rsa_encrypt(&n.to_bytes_be(), key).is_err());
        assert!(rsa_encrypt(&[0xff; 256], key).is_err());
        assert_eq!(rsa_encrypt(&[1, 2, 3], key).unwrap().len(), 256);
    }

    #[test]
    fn rsa_pad_rejects_oversized_input() {
        assert!(rsa_pad_encrypt(&[0u8; 145], &TELEGRAM_RSA_KEYS[0]).is_err());
        assert!(rsa_pad_encrypt(&[0u8; 144], &TELEGRAM_RSA_KEYS[0]).is_ok());
    }
}
