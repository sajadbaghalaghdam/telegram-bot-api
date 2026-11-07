use num_bigint::BigUint;

/// Telegram Production RSA Public Key for MTProto 2.0 Handshake
pub struct TelegramRsaKey {
    pub fingerprint: i64,
    pub n: &'static str,
    pub e: u32,
}

/// Official Telegram Production Public Keys (DC2 / DC4)
pub static TELEGRAM_RSA_KEYS: &[TelegramRsaKey] = &[
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

/// Encrypt data with Telegram RSA key (m^e mod n)
pub fn rsa_encrypt(data: &[u8], key: &TelegramRsaKey) -> Vec<u8> {
    let n = BigUint::parse_bytes(key.n.as_bytes(), 16).expect("Valid hex string for RSA modulus");
    let e = BigUint::from(key.e);
    let m = BigUint::from_bytes_be(data);

    let c = m.modpow(&e, &n);
    let mut bytes = c.to_bytes_be();

    // Pad to 256 bytes (RSA 2048-bit output)
    if bytes.len() < 256 {
        let mut padded = vec![0u8; 256 - bytes.len()];
        padded.extend(bytes);
        bytes = padded;
    }

    bytes
}
