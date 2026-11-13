//! Native MTProto 2.0 client used for large file up/downloads (up to 2000 MB).
//!
//! Layering, bottom to top:
//! * wire: `tl` (serialisation), `crypto` (IGE, KDF, RSA, factorize), `packet` (encrypt/decrypt
//!   frames in place), `transport` (intermediate transport with timeouts), `session` (ids, seq_no);
//! * `handshake`: DH key exchange; `auth`: per-bot auth-key caches, bot login, cross-DC import;
//! * `connection`: `MtProtoConnection` RPC / pipelined batch primitives; `rpc_error`, `gzip`;
//! * building blocks: `peer` (InputPeer), `media` (sendMedia TL), `updates` (response parsing),
//!   `file_id` (Bot API file_id decoding);
//! * transfers: `upload_parts` (part workers) + `uploader` (entry point); `downloader`.
pub mod auth;
#[cfg(test)]
mod bench_tests;
pub mod connection;
pub mod crypto;
pub mod downloader;
pub mod file_at;
#[cfg(test)]
mod e2e_tests;
pub mod file_id;
pub mod gzip;
pub mod handshake;
pub mod media;
#[cfg(test)]
mod mock_dc;
pub mod packet;
pub mod peer;
pub mod rpc_error;
pub mod session;
pub mod tl;
pub mod transport;
pub mod updates;
pub mod upload_parts;
pub mod upload_progress;
pub mod uploader;

pub use downloader::download_large_file_via_mtproto;
pub use file_id::{decode_file_id, DecodedFileLocation};
pub use uploader::{upload_and_send_large_file_typed, MediaAttrs, MediaKind, MtProtoConnection, UploadedMediaResult};
