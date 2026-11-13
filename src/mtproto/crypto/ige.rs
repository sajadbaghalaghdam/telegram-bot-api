use aes::cipher::{BlockDecrypt, BlockEncrypt, KeyInit, generic_array::GenericArray};
use aes::Aes256;
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum IgeError {
    #[error("Input length must be a multiple of 16 bytes")]
    InvalidLength,
}

/// AES-256-IGE (Infinite Garble Extension) implementation for MTProto 2.0.
///
/// Encryption: C_i = AES_encrypt(P_i ^ C_{i-1}) ^ P_{i-1}
/// Decryption: P_i = AES_decrypt(C_i ^ P_{i-1}) ^ C_{i-1}
///
/// IV is 32 bytes: [iv1 (16 bytes), iv2 (16 bytes)]
/// C_0 = iv1, P_0 = iv2
///
/// Both directions are inherently sequential (each block depends on the previous result), so
/// the work is done in place on the caller's buffer with no allocation and no per-block copies
/// beyond the two 16-byte chaining registers.
pub struct Aes256Ige;

#[inline(always)]
fn xor16(a: &mut [u8; 16], b: &[u8; 16]) {
    for (x, y) in a.iter_mut().zip(b) {
        *x ^= *y;
    }
}

impl Aes256Ige {
    /// Encrypts `buf` in place. Its length must be a multiple of 16.
    pub fn encrypt_in_place(buf: &mut [u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<(), IgeError> {
        if !buf.len().is_multiple_of(16) {
            return Err(IgeError::InvalidLength);
        }
        let cipher = Aes256::new(GenericArray::from_slice(key));
        let mut prev_cipher: [u8; 16] = iv[..16].try_into().expect("16 bytes");
        let mut prev_plain: [u8; 16] = iv[16..].try_into().expect("16 bytes");

        for block in buf.as_chunks_mut::<16>().0 {
            let plain = *block;
            let mut x = plain;
            xor16(&mut x, &prev_cipher); // P_i ^ C_{i-1}
            let mut g = GenericArray::from(x);
            cipher.encrypt_block(&mut g);
            let mut c: [u8; 16] = g.into();
            xor16(&mut c, &prev_plain); // ... ^ P_{i-1}
            *block = c;
            prev_cipher = c;
            prev_plain = plain;
        }
        Ok(())
    }

    /// Decrypts `buf` in place. Its length must be a multiple of 16.
    pub fn decrypt_in_place(buf: &mut [u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<(), IgeError> {
        if !buf.len().is_multiple_of(16) {
            return Err(IgeError::InvalidLength);
        }
        let cipher = Aes256::new(GenericArray::from_slice(key));
        let mut prev_cipher: [u8; 16] = iv[..16].try_into().expect("16 bytes");
        let mut prev_plain: [u8; 16] = iv[16..].try_into().expect("16 bytes");

        for block in buf.as_chunks_mut::<16>().0 {
            let c = *block;
            let mut x = c;
            xor16(&mut x, &prev_plain); // C_i ^ P_{i-1}
            let mut g = GenericArray::from(x);
            cipher.decrypt_block(&mut g);
            let mut p: [u8; 16] = g.into();
            xor16(&mut p, &prev_cipher); // ... ^ C_{i-1}
            *block = p;
            prev_cipher = c;
            prev_plain = p;
        }
        Ok(())
    }

    /// Encrypts `plaintext` into a new buffer (convenience for the cold handshake path).
    pub fn encrypt(plaintext: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<Vec<u8>, IgeError> {
        let mut out = plaintext.to_vec();
        Self::encrypt_in_place(&mut out, key, iv)?;
        Ok(out)
    }

    /// Decrypts `ciphertext` into a new buffer (convenience for the cold handshake path).
    pub fn decrypt(ciphertext: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<Vec<u8>, IgeError> {
        let mut out = ciphertext.to_vec();
        Self::decrypt_in_place(&mut out, key, iv)?;
        Ok(out)
    }
}

/// Wrapper functions returning Result to prevent DoS via panics
pub fn aes_256_ige_encrypt(plaintext: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<Vec<u8>, IgeError> {
    Aes256Ige::encrypt(plaintext, key, iv)
}

pub fn aes_256_ige_decrypt(ciphertext: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<Vec<u8>, IgeError> {
    Aes256Ige::decrypt(ciphertext, key, iv)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Straightforward textbook IGE used as an oracle for the in-place implementation.
    fn reference_encrypt(pt: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Vec<u8> {
        let cipher = Aes256::new(GenericArray::from_slice(key));
        let mut out = Vec::new();
        let mut prev_c = iv[..16].to_vec();
        let mut prev_p = iv[16..].to_vec();
        for p in pt.chunks(16) {
            let mut b = GenericArray::clone_from_slice(p);
            for (x, y) in b.iter_mut().zip(&prev_c) {
                *x ^= y;
            }
            cipher.encrypt_block(&mut b);
            for (x, y) in b.iter_mut().zip(&prev_p) {
                *x ^= y;
            }
            out.extend_from_slice(&b);
            prev_c = b.to_vec();
            prev_p = p.to_vec();
        }
        out
    }

    #[test]
    fn matches_reference_and_roundtrips_in_place() {
        let key: [u8; 32] = std::array::from_fn(|i| (i * 5 + 1) as u8);
        let iv: [u8; 32] = std::array::from_fn(|i| (255 - i * 3) as u8);
        for len in [0usize, 16, 32, 48, 1024, 4096 + 16] {
            let data: Vec<u8> = (0..len).map(|i| (i * 131 + 7) as u8).collect();
            let expected = reference_encrypt(&data, &key, &iv);
            let mut buf = data.clone();
            Aes256Ige::encrypt_in_place(&mut buf, &key, &iv).unwrap();
            assert_eq!(buf, expected, "encrypt len {len}");
            assert_eq!(aes_256_ige_encrypt(&data, &key, &iv).unwrap(), expected);
            Aes256Ige::decrypt_in_place(&mut buf, &key, &iv).unwrap();
            assert_eq!(buf, data, "decrypt len {len}");
        }
    }

    #[test]
    fn test_ige_roundtrip() {
        let key = [0u8; 32];
        let iv = [0u8; 32];
        let data = b"This is a secret message that is 48 bytes long!!";

        let encrypted = aes_256_ige_encrypt(data, &key, &iv).expect("Encryption failed");
        let decrypted = aes_256_ige_decrypt(&encrypted, &key, &iv).expect("Decryption failed");

        assert_eq!(data, decrypted.as_slice());
    }

    #[test]
    fn test_ige_empty() {
        let key = [0u8; 32];
        let iv = [0u8; 32];
        let data = b"";

        let encrypted = aes_256_ige_encrypt(data, &key, &iv).expect("Encryption failed");
        let decrypted = aes_256_ige_decrypt(&encrypted, &key, &iv).expect("Decryption failed");

        assert_eq!(data, decrypted.as_slice());
    }

    #[test]
    fn test_ige_invalid_length() {
        let key = [0u8; 32];
        let iv = [0u8; 32];
        let data = b"Not a multiple of 16";

        let result = aes_256_ige_encrypt(data, &key, &iv);
        assert_eq!(result, Err(IgeError::InvalidLength));
        let mut buf = data.to_vec();
        assert_eq!(Aes256Ige::decrypt_in_place(&mut buf, &key, &iv), Err(IgeError::InvalidLength));
    }
}
