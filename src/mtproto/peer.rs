//! InputPeer resolution (chat_id / @username -> `InputPeer*` TL) and access_hash extraction.
use bytes::BytesMut;
use std::io::{Error, ErrorKind};

use super::connection::MtProtoConnection;
use super::tl::{read_int, read_long, write_int, write_long, write_string};

/// Resolves an InputPeer TL object for the target chat_id or username.
/// Translated from td/telegram/UserManager.cpp & ChatManager.cpp.
pub async fn resolve_input_peer(
    conn: &mut MtProtoConnection,
    chat_id_str: &str,
    hint_username: Option<&str>,
) -> Result<BytesMut, Error> {
    let trimmed = chat_id_str.trim();
    if let Some(uname) = trimmed.strip_prefix('@').or(hint_username) {
        let clean_uname = uname.trim_start_matches('@');
        if !clean_uname.is_empty() {
            if let Ok(peer) = resolve_username_mtproto(conn, clean_uname).await {
                return Ok(peer);
            }
        }
    }

    let chat_id: i64 = trimmed.parse().map_err(|_| {
        Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid chat_id: {}", chat_id_str),
        )
    })?;

    if chat_id > 0 {
        let mut req = BytesMut::with_capacity(32);
        write_int(&mut req, 0x0d91a548u32 as i32); // users.getUsers#d91a548
        write_int(&mut req, 0x1cb5c415u32 as i32); // vector#1cb5c415
        write_int(&mut req, 1);
        write_int(&mut req, 0xf21158c6u32 as i32); // inputUser#f21158c6
        write_long(&mut req, chat_id);
        write_long(&mut req, 0);

        let resp = conn.rpc_call(&req).await.map_err(|e| {
            Error::new(ErrorKind::NotFound, format!("Failed to resolve user {}: {}", chat_id, e))
        })?;
        // Bots may address users/channels with access_hash 0 (TDLib does the same).
        let access_hash = extract_user_access_hash(&resp, chat_id).unwrap_or_else(|| {
            tracing::warn!("[upload] access_hash for user {} not found in response, using 0", chat_id);
            0
        });

        let mut peer = BytesMut::with_capacity(20);
        write_int(&mut peer, 0xdde8a54cu32 as i32); // inputPeerUser#dde8a54c
        write_long(&mut peer, chat_id);
        write_long(&mut peer, access_hash);
        Ok(peer)
    } else if chat_id < -1_000_000_000_000 {
        let channel_id = -1_000_000_000_000 - chat_id;
        let mut req = BytesMut::with_capacity(32);
        write_int(&mut req, 0x0a7f6bbbu32 as i32); // channels.getChannels#a7f6bbb
        write_int(&mut req, 0x1cb5c415u32 as i32); // vector#1cb5c415
        write_int(&mut req, 1);
        write_int(&mut req, 0xf35aec28u32 as i32); // inputChannel#f35aec28
        write_long(&mut req, channel_id);
        write_long(&mut req, 0);

        let resp = conn.rpc_call(&req).await.map_err(|e| {
            Error::new(ErrorKind::NotFound, format!("Failed to resolve channel {}: {}", channel_id, e))
        })?;
        let access_hash = extract_channel_access_hash(&resp, channel_id).unwrap_or_else(|| {
            tracing::warn!("[upload] access_hash for channel {} not found in response, using 0", channel_id);
            0
        });

        let mut peer = BytesMut::with_capacity(20);
        write_int(&mut peer, 0x27bcbbfcu32 as i32); // inputPeerChannel#27bcbbfc
        write_long(&mut peer, channel_id);
        write_long(&mut peer, access_hash);
        Ok(peer)
    } else {
        let group_id = -chat_id;
        let mut peer = BytesMut::with_capacity(12);
        write_int(&mut peer, 0x35a95cb9u32 as i32); // inputPeerChat#35a95cb9
        write_long(&mut peer, group_id);
        Ok(peer)
    }
}

async fn resolve_username_mtproto(
    conn: &mut MtProtoConnection,
    username: &str,
) -> Result<BytesMut, Error> {
    let mut req = BytesMut::with_capacity(64);
    write_int(&mut req, 0x725afbbcu32 as i32);
    write_int(&mut req, 0); // flags = 0
    write_string(&mut req, username);

    let resp = conn.rpc_call(&req).await?;
    let mut cur = resp.as_slice();
    let ctor = read_int(&mut cur).map_err(tl_err)? as u32;
    if ctor != 0x7f077ad9 {
        return Err(Error::new(ErrorKind::NotFound, "contacts.resolveUsername failed"));
    }
    let peer_ctor = read_int(&mut cur).map_err(tl_err)? as u32;
    let peer_id = read_long(&mut cur).map_err(tl_err)?;

    match peer_ctor {
        0x59511722 => {
            let ah = extract_user_access_hash(&resp, peer_id).unwrap_or_else(|| {
                tracing::warn!("[upload] access_hash for resolved user {} not found, using 0", peer_id);
                0
            });
            let mut peer = BytesMut::with_capacity(20);
            write_int(&mut peer, 0xdde8a54cu32 as i32);
            write_long(&mut peer, peer_id);
            write_long(&mut peer, ah);
            Ok(peer)
        }
        0xa2a5371e => {
            let ah = extract_channel_access_hash(&resp, peer_id).unwrap_or_else(|| {
                tracing::warn!("[upload] access_hash for resolved channel {} not found, using 0", peer_id);
                0
            });
            let mut peer = BytesMut::with_capacity(20);
            write_int(&mut peer, 0x27bcbbfcu32 as i32);
            write_long(&mut peer, peer_id);
            write_long(&mut peer, ah);
            Ok(peer)
        }
        0x36c6019a => {
            let mut peer = BytesMut::with_capacity(12);
            write_int(&mut peer, 0x35a95cb9u32 as i32);
            write_long(&mut peer, peer_id);
            Ok(peer)
        }
        _ => Err(Error::new(ErrorKind::InvalidData, "Unknown Peer constructor")),
    }
}

fn extract_user_access_hash(buf: &[u8], target_user_id: i64) -> Option<i64> {
    let id_bytes = target_user_id.to_le_bytes();
    for i in 0..buf.len().saturating_sub(28) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().ok()?);
        if (ctor == 0x31774388 || ctor == 0xb1b8cc83 || ctor == 0x20b1422)
            && buf[i + 12..i + 20] == id_bytes
        {
            let flags = i32::from_le_bytes(buf[i + 4..i + 8].try_into().ok()?);
            if (flags & 1) != 0 {
                let ah = i64::from_le_bytes(buf[i + 20..i + 28].try_into().ok()?);
                return Some(ah);
            }
        }
    }
    None
}

fn extract_channel_access_hash(buf: &[u8], target_channel_id: i64) -> Option<i64> {
    let id_bytes = target_channel_id.to_le_bytes();
    for i in 0..buf.len().saturating_sub(28) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().ok()?);
        if (ctor == 0x1c32b11c || ctor == 0xd49f34c6) && buf[i + 12..i + 20] == id_bytes {
            let flags = i32::from_le_bytes(buf[i + 4..i + 8].try_into().ok()?);
            if (flags & (1 << 13)) != 0 {
                let ah = i64::from_le_bytes(buf[i + 20..i + 28].try_into().ok()?);
                return Some(ah);
            }
        }
    }
    None
}

fn tl_err(e: super::tl::TLError) -> Error {
    Error::new(ErrorKind::InvalidData, e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_user_hash_requires_flag_and_id() {
        let mut b = Vec::new();
        b.extend_from_slice(&0x31774388u32.to_le_bytes());
        b.extend_from_slice(&1i32.to_le_bytes()); // flags: access_hash present
        b.extend_from_slice(&0i32.to_le_bytes()); // flags2
        b.extend_from_slice(&42i64.to_le_bytes());
        b.extend_from_slice(&777i64.to_le_bytes());
        b.extend_from_slice(&[0u8; 16]);
        assert_eq!(extract_user_access_hash(&b, 42), Some(777));
        assert_eq!(extract_user_access_hash(&b, 43), None);
    }
}
