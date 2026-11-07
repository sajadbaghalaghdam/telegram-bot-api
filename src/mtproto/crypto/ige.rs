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
pub struct Aes256Ige;

impl Aes256Ige {
    /// Encrypts plaintext using AES-256-IGE.
    /// Plaintext length must be a multiple of 16.
    pub fn encrypt(plaintext: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<Vec<u8>, IgeError> {
        if plaintext.len() % 16 != 0 {
            return Err(IgeError::InvalidLength);
        }

        let cipher = Aes256::new(GenericArray::from_slice(key));
        let mut ciphertext = vec![0u8; plaintext.len()];
        
        let mut prev_cipher = &iv[0..16];
        let mut prev_plain = &iv[16..32];

        // Use chunks_exact and chunks_mut to avoid bounds checking and repeated allocations
        let mut plain_chunks = plaintext.chunks_exact(16);
        let mut cipher_chunks = ciphertext.chunks_exact_mut(16);

        while let (Some(p_chunk), Some(c_chunk)) = (plain_chunks.next(), cipher_chunks.next()) {
            let mut block = GenericArray::clone_from_slice(p_chunk);
            
            // Step 1: P_i ^ C_{i-1}
            for (b, pc) in block.iter_mut().zip(prev_cipher.iter()) {
                *b ^= *pc;
            }

            // Step 2: AES_encrypt(...)
            cipher.encrypt_block(&mut block);

            // Step 3: ... ^ P_{i-1}
            for (b, pp) in block.iter_mut().zip(prev_plain.iter()) {
                *b ^= *pp;
            }

            c_chunk.copy_from_slice(&block);
            
            // Update state for next block using slices to avoid Vec slicing overhead
            prev_cipher = c_chunk;
            prev_plain = p_chunk;
        }

        Ok(ciphertext)
    }

    /// Decrypts ciphertext using AES-256-IGE.
    /// Ciphertext length must be a multiple of 16.
    pub fn decrypt(ciphertext: &[u8], key: &[u8; 32], iv: &[u8; 32]) -> Result<Vec<u8>, IgeError> {
        if ciphertext.len() % 16 != 0 {
            return Err(IgeError::InvalidLength);
        }

        let cipher = Aes256::new(GenericArray::from_slice(key));
        let mut plaintext = vec![0u8; ciphertext.len()];

        let mut prev_cipher = &iv[0..16];
        let mut prev_plain = &iv[16..32];

        let mut cipher_chunks = ciphertext.chunks_exact(16);
        let mut plain_chunks = plaintext.chunks_exact_mut(16);

        while let (Some(c_chunk), Some(p_chunk)) = (cipher_chunks.next(), plain_chunks.next()) {
            let mut block = GenericArray::clone_from_slice(c_chunk);

            // Step 1: C_i ^ P_{i-1}
            for (b, pp) in block.iter_mut().zip(prev_plain.iter()) {
                *b ^= *pp;
            }

            // Step 2: AES_decrypt(...)
            cipher.decrypt_block(&mut block);

            // Step 3: ... ^ C_{i-1}
            for (b, pc) in block.iter_mut().zip(prev_cipher.iter()) {
                *b ^= *pc;
            }

            p_chunk.copy_from_slice(&block);

            // Update state for next block
            prev_cipher = c_chunk;
            prev_plain = p_chunk;
        }

        Ok(plaintext)
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
    }
}