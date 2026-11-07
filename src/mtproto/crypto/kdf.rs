use sha2::{Digest, Sha256};

/// MTProto 2.0 Key Derivation Function (KDF).
pub fn derive_keys(auth_key: &[u8; 256], msg_key: &[u8; 16], is_client: bool) -> ([u8; 32], [u8; 32]) {
    let x = if is_client { 0 } else { 8 };

    // sha256_a = SHA256(msg_key + auth_key[x .. x + 36])
    let mut hasher_a = Sha256::new();
    hasher_a.update(msg_key);
    hasher_a.update(&auth_key[x..x + 36]);
    let sha256_a = hasher_a.finalize();

    // sha256_b = SHA256(auth_key[x + 40 .. x + 76] + msg_key)
    let mut hasher_b = Sha256::new();
    hasher_b.update(&auth_key[x + 40..x + 76]);
    hasher_b.update(msg_key);
    let sha256_b = hasher_b.finalize();

    let mut aes_key = [0u8; 32];
    let mut aes_iv = [0u8; 32];

    // aes_key (32 bytes) = sha256_a[0..8] + sha256_b[8..24] + sha256_a[24..32]
    aes_key[0..8].copy_from_slice(&sha256_a[0..8]);
    aes_key[8..24].copy_from_slice(&sha256_b[8..24]);
    aes_key[24..32].copy_from_slice(&sha256_a[24..32]);

    // aes_iv (32 bytes) = sha256_b[0..8] + sha256_a[8..24] + sha256_b[24..32]
    aes_iv[0..8].copy_from_slice(&sha256_b[0..8]);
    aes_iv[8..24].copy_from_slice(&sha256_a[8..24]);
    aes_iv[24..32].copy_from_slice(&sha256_b[24..32]);

    (aes_key, aes_iv)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_keys_deterministic() {
        let auth_key = [0u8; 256];
        let msg_key = [0u8; 16];

        let res1 = derive_keys(&auth_key, &msg_key, true);
        let res2 = derive_keys(&auth_key, &msg_key, true);

        assert_eq!(res1, res2, "KDF must be deterministic");
    }

    #[test]
    fn test_derive_keys_direction_difference() {
        let mut auth_key = [0u8; 256];
        for (i, b) in auth_key.iter_mut().enumerate() {
            *b = i as u8;
        }
        let msg_key = [1u8; 16];

        let client_to_server = derive_keys(&auth_key, &msg_key, true);
        let server_to_client = derive_keys(&auth_key, &msg_key, false);

        assert_ne!(client_to_server, server_to_client, "Keys must differ based on direction");
    }

    #[test]
    fn test_buffer_lengths() {
        let auth_key = [0u8; 256];
        let msg_key = [0u8; 16];
        let (key, iv) = derive_keys(&auth_key, &msg_key, true);

        assert_eq!(key.len(), 32);
        assert_eq!(iv.len(), 32);
    }
}
