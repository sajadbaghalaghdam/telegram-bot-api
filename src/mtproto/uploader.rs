use bytes::{BufMut, BytesMut};
use dashmap::DashMap;
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::handshake::MtProtoHandshake;
use super::packet::{pack_encrypted_message, unpack_encrypted_message, PacketError};
use super::session::MtProtoSession;
use super::tl::{read_bytes, read_int, read_long, read_string, write_bytes, write_int, write_long, write_string};
use super::transport::IntermediateTransport;

/// MTProto Layer matching TDLib production deployment
const MTPROTO_LAYER: i32 = 225;

/// 512 KB (maximum MTProto file part size, evenly divides 1 MB)
const PART_SIZE: usize = 512 * 1024;

/// Number of parallel MTProto upload connections for big files (matching TDLib SessionMultiProxy)
const UPLOAD_WORKERS: usize = 8;

/// Number of pipelined 512 KB parts per worker batch
const PIPELINE_DEPTH: usize = 4;

#[derive(Clone, Copy)]
pub struct CachedBotAuth {
    pub dc_id: i32,
    pub auth_key: [u8; 256],
    pub server_salt: i64,
    pub time_offset: i64,
}

static BOT_AUTH_CACHE: OnceLock<DashMap<String, CachedBotAuth>> = OnceLock::new();

fn auth_cache() -> &'static DashMap<String, CachedBotAuth> {
    BOT_AUTH_CACHE.get_or_init(DashMap::new)
}

/// Production DC addresses (from td/telegram/net/ConnectionCreator.cpp)
pub fn dc_address(dc_id: i32) -> &'static str {
    match dc_id {
        1 => "149.154.175.50:443",
        2 => "149.154.167.51:443",
        3 => "149.154.175.100:443",
        4 => "149.154.167.91:443",
        5 => "149.154.171.5:443",
        _ => "149.154.167.51:443",
    }
}

/// Result of uploading and sending a media file over MTProto
#[derive(Debug, Clone)]
pub struct UploadedMediaResult {
    pub message_id: i32,
    pub date: i64,
    pub doc_id: i64,
    pub access_hash: i64,
    pub file_size: i64,
    pub file_name: String,
    pub mime_type: String,
    pub dc_id: i32,
}

/// Active MTProto 2.0 Connection (translated from td/mtproto/SessionConnection.cpp)
pub struct MtProtoConnection {
    pub dc_id: i32,
    pub transport: IntermediateTransport,
    pub session: MtProtoSession,
    read_buf: Vec<u8>,
    pending_acks: Vec<i64>,
    inited_connection: bool,
    api_id: i32,
}

impl MtProtoConnection {
    /// Connects to the specified DC and performs the 6-step Diffie-Hellman handshake.
    pub async fn connect(dc_id: i32, api_id: i32) -> Result<Self, Error> {
        let addr = dc_address(dc_id);
        let mut transport = IntermediateTransport::connect(addr).await?;
        let hs = MtProtoHandshake::with_dc(&mut transport, dc_id).run().await?;
        let session = MtProtoSession::with_time_offset(hs.auth_key, hs.server_salt, hs.time_offset);

        Ok(Self {
            dc_id,
            transport,
            session,
            read_buf: Vec::with_capacity(64 * 1024),
            pending_acks: Vec::with_capacity(16),
            inited_connection: false,
            api_id,
        })
    }

    /// Opens an additional parallel TCP connection sharing the bot's authorized `auth_key`
    /// with a fresh `session_id` (matching TDLib's SessionMultiProxy upload workers).
    pub async fn connect_with_auth(auth: CachedBotAuth, api_id: i32) -> Result<Self, Error> {
        let addr = dc_address(auth.dc_id);
        let transport = IntermediateTransport::connect(addr).await?;
        let session = MtProtoSession::with_time_offset(auth.auth_key, auth.server_salt, auth.time_offset);

        Ok(Self {
            dc_id: auth.dc_id,
            transport,
            session,
            read_buf: Vec::with_capacity(64 * 1024),
            pending_acks: Vec::with_capacity(16),
            inited_connection: false,
            api_id,
        })
    }

    /// Connects to Telegram and authorizes the bot via `invokeWithLayer(initConnection(auth.importBotAuthorization))`
    /// with automatic DC migration (`USER_MIGRATE_X` / `FILE_MIGRATE_X`) and in-memory `auth_key` caching.
    pub async fn connect_and_authorize(
        initial_dc: i32,
        api_id: i32,
        api_hash: &str,
        bot_token: &str,
    ) -> Result<Self, Error> {
        if let Some(cached) = auth_cache().get(bot_token).map(|r| *r.value()) {
            if let Ok(conn) = Self::connect_with_auth(cached, api_id).await {
                return Ok(conn);
            }
        }

        let mut dc_id = initial_dc;
        for _attempt in 0..5 {
            let mut conn = Self::connect(dc_id, api_id).await?;
            let auth_query = Self::build_init_and_bot_auth(api_id, api_hash, bot_token);
            conn.inited_connection = true;
            match conn.rpc_call(&auth_query).await {
                Ok(_resp) => {
                    auth_cache().insert(
                        bot_token.to_string(),
                        CachedBotAuth {
                            dc_id: conn.dc_id,
                            auth_key: *conn.session.auth_key(),
                            server_salt: conn.session.server_salt,
                            time_offset: conn.session.time_offset,
                        },
                    );
                    return Ok(conn);
                }
                Err(e) => {
                    let msg = e.to_string();
                    if let Some(target_dc) = Self::parse_migrate_dc(&msg) {
                        dc_id = target_dc;
                        continue;
                    }
                    if let Some(wait_secs) = Self::parse_flood_wait(&msg) {
                        tokio::time::sleep(Duration::from_secs(wait_secs.max(1))).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
        Err(Error::new(
            ErrorKind::Other,
            "Exceeded maximum attempts during bot authorization",
        ))
    }

    fn parse_migrate_dc(err_msg: &str) -> Option<i32> {
        for prefix in ["USER_MIGRATE_", "FILE_MIGRATE_", "PHONE_MIGRATE_", "NETWORK_MIGRATE_"] {
            if let Some(idx) = err_msg.find(prefix) {
                let rest = &err_msg[idx + prefix.len()..];
                let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
                if let Ok(dc) = digits.parse::<i32>() {
                    return Some(dc);
                }
            }
        }
        None
    }

    fn parse_flood_wait(err_msg: &str) -> Option<u64> {
        if let Some(idx) = err_msg.find("FLOOD_WAIT_") {
            let rest = &err_msg[idx + "FLOOD_WAIT_".len()..];
            let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
            if let Ok(secs) = digits.parse::<u64>() {
                return Some(secs);
            }
            return Some(1);
        }
        None
    }

    /// Wraps an inner TL query in `invokeWithLayer#da9b0d0d(layer, initConnection#c1cd5ea9(..., query))`
    pub fn wrap_with_init_connection(api_id: i32, inner_query: &[u8]) -> BytesMut {
        let mut buf = BytesMut::with_capacity(128 + inner_query.len());
        write_int(&mut buf, 0xda9b0d0du32 as i32);
        write_int(&mut buf, MTPROTO_LAYER);
        write_int(&mut buf, 0xc1cd5ea9u32 as i32);
        write_int(&mut buf, 0); // flags = 0
        write_int(&mut buf, api_id);
        write_string(&mut buf, "Telegram Bot API");
        write_string(&mut buf, "Linux");
        write_string(&mut buf, "9.5.0");
        write_string(&mut buf, "en");
        write_string(&mut buf, "");
        write_string(&mut buf, "en");
        buf.put_slice(inner_query);
        buf
    }

    /// Builds `invokeWithLayer#da9b0d0d(layer, initConnection#c1cd5ea9(..., auth.importBotAuthorization#67a3ff2c))`
    /// Translated from td/telegram/net/MtprotoHeader.cpp & AuthManager.cpp.
    pub fn build_init_and_bot_auth(api_id: i32, api_hash: &str, bot_token: &str) -> BytesMut {
        let mut inner = BytesMut::with_capacity(128);
        // auth.importBotAuthorization#67a3ff2c
        write_int(&mut inner, 0x67a3ff2cu32 as i32);
        write_int(&mut inner, 0); // flags = 0
        write_int(&mut inner, api_id);
        write_string(&mut inner, api_hash);
        write_string(&mut inner, bot_token);
        Self::wrap_with_init_connection(api_id, &inner)
    }

    /// Executes a single MTProto RPC call, handling `bad_server_salt`, `bad_msg_notification`,
    /// `new_session_created`, `msg_container`, `FLOOD_WAIT_X`, and `msgs_ack`.
    pub async fn rpc_call(&mut self, query: &[u8]) -> Result<Vec<u8>, Error> {
        let effective_query: BytesMut = if !self.inited_connection {
            self.inited_connection = true;
            Self::wrap_with_init_connection(self.api_id, query)
        } else {
            let mut b = BytesMut::with_capacity(query.len());
            b.put_slice(query);
            b
        };

        let mut req_msg_id = self.send_encrypted(&effective_query, true).await?;

        loop {
            self.flush_acks().await?;
            self.transport.read_packet(&mut self.read_buf).await?;

            let (srv_msg_id, srv_seq_no, payload) = unpack_encrypted_message(
                self.session.auth_key(),
                self.session.auth_key_id(),
                &self.read_buf,
            )
            .map_err(|e| match e {
                PacketError::TransportError(code) => Error::new(
                    ErrorKind::PermissionDenied,
                    format!("MTProto transport error {}", code),
                ),
                other => Error::new(ErrorKind::InvalidData, other.to_string()),
            })?;

            if (srv_seq_no & 1) == 1 {
                self.pending_acks.push(srv_msg_id);
            }

            let mut results = Vec::new();
            let mut retry_needed = false;
            self.process_incoming_payload(&payload, srv_msg_id, &mut results, &mut retry_needed)?;

            if retry_needed {
                req_msg_id = self.send_encrypted(&effective_query, true).await?;
                continue;
            }

            for (ans_req_id, res) in results {
                if ans_req_id == req_msg_id {
                    self.flush_acks().await?;
                    match res {
                        Ok(data) => return Ok(data),
                        Err(e) => {
                            let msg = e.to_string();
                            if let Some(wait_secs) = Self::parse_flood_wait(&msg) {
                                tokio::time::sleep(Duration::from_secs(wait_secs.max(1))).await;
                                req_msg_id = self.send_encrypted(&effective_query, true).await?;
                                break;
                            }
                            return Err(e);
                        }
                    }
                }
            }
        }
    }

    /// Pipelined batch RPC call: sends multiple `upload.saveBigFilePart` requests back-to-back
    /// before reading their responses, maximizing throughput over high-bandwidth links.
    pub async fn rpc_call_batch(&mut self, queries: &[BytesMut]) -> Result<Vec<Vec<u8>>, Error> {
        if queries.is_empty() {
            return Ok(Vec::new());
        }

        let mut expected_ids = Vec::with_capacity(queries.len());
        for (idx, q) in queries.iter().enumerate() {
            if idx == 0 && !self.inited_connection {
                self.inited_connection = true;
                let wrapped = Self::wrap_with_init_connection(self.api_id, q);
                let msg_id = self.send_encrypted(&wrapped, true).await?;
                expected_ids.push(msg_id);
            } else {
                let msg_id = self.send_encrypted(q, true).await?;
                expected_ids.push(msg_id);
            }
        }
        self.transport.flush().await?;

        let mut collected: Vec<Option<Vec<u8>>> = vec![None; queries.len()];
        let mut remaining = queries.len();

        while remaining > 0 {
            self.flush_acks().await?;
            self.transport.read_packet(&mut self.read_buf).await?;

            let (srv_msg_id, srv_seq_no, payload) = unpack_encrypted_message(
                self.session.auth_key(),
                self.session.auth_key_id(),
                &self.read_buf,
            )
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

            if (srv_seq_no & 1) == 1 {
                self.pending_acks.push(srv_msg_id);
            }

            let mut results = Vec::new();
            let mut retry_needed = false;
            self.process_incoming_payload(&payload, srv_msg_id, &mut results, &mut retry_needed)?;

            if retry_needed {
                for (i, slot) in collected.iter().enumerate() {
                    if slot.is_none() {
                        expected_ids[i] = self.send_encrypted(&queries[i], true).await?;
                    }
                }
                self.transport.flush().await?;
                continue;
            }

            for (ans_req_id, res) in results {
                match res {
                    Ok(data) => {
                        if let Some(pos) = expected_ids.iter().position(|&id| id == ans_req_id) {
                            if collected[pos].is_none() {
                                collected[pos] = Some(data);
                                remaining -= 1;
                            }
                        }
                    }
                    Err(e) => {
                        let msg = e.to_string();
                        if let Some(wait_secs) = Self::parse_flood_wait(&msg) {
                            tokio::time::sleep(Duration::from_secs(wait_secs.max(1))).await;
                            if let Some(pos) = expected_ids.iter().position(|&id| id == ans_req_id) {
                                expected_ids[pos] = self.send_encrypted(&queries[pos], true).await?;
                                self.transport.flush().await?;
                            }
                        } else {
                            return Err(e);
                        }
                    }
                }
            }
        }

        self.flush_acks().await?;
        Ok(collected.into_iter().map(|x| x.unwrap_or_default()).collect())
    }

    async fn send_encrypted(&mut self, data: &[u8], is_content_related: bool) -> Result<i64, Error> {
        let msg_id = self
            .session
            .generate_msg_id()
            .map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
        let seq_no = self.session.next_seq_no(is_content_related);
        let packet = pack_encrypted_message(
            self.session.auth_key(),
            self.session.auth_key_id(),
            self.session.server_salt,
            self.session.session_id,
            msg_id,
            seq_no,
            data,
        )
        .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        self.transport.send_packet(&packet).await?;
        Ok(msg_id)
    }

    async fn flush_acks(&mut self) -> Result<(), Error> {
        if self.pending_acks.is_empty() {
            return Ok(());
        }
        // msgs_ack#62d6b459 msg_ids:Vector<long>
        let mut ack_buf = BytesMut::with_capacity(8 + self.pending_acks.len() * 8);
        write_int(&mut ack_buf, 0x62d6b459u32 as i32);
        write_int(&mut ack_buf, 0x1cb5c415u32 as i32); // vector#1cb5c415
        write_int(&mut ack_buf, self.pending_acks.len() as i32);
        for &id in &self.pending_acks {
            write_long(&mut ack_buf, id);
        }
        self.pending_acks.clear();
        let _ = self.send_encrypted(&ack_buf, false).await?;
        Ok(())
    }

    fn process_incoming_payload(
        &mut self,
        payload: &[u8],
        srv_msg_id: i64,
        results: &mut Vec<(i64, Result<Vec<u8>, Error>)>,
        retry_needed: &mut bool,
    ) -> Result<(), Error> {
        if payload.len() < 4 {
            return Ok(());
        }
        let mut cursor = payload;
        let ctor = read_int(&mut cursor).map_err(Self::tl_err)? as u32;

        match ctor {
            // msg_container#73f1f8dc messages:vector<%Message>
            0x73f1f8dc => {
                let count = read_int(&mut cursor).map_err(Self::tl_err)?;
                for _ in 0..count {
                    if cursor.len() < 16 {
                        break;
                    }
                    let inner_msg_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                    let inner_seqno = read_int(&mut cursor).map_err(Self::tl_err)?;
                    let inner_bytes = read_int(&mut cursor).map_err(Self::tl_err)? as usize;
                    if cursor.len() < inner_bytes {
                        break;
                    }
                    let inner_body = &cursor[..inner_bytes];
                    cursor = &cursor[inner_bytes..];
                    if (inner_seqno & 1) == 1 {
                        self.pending_acks.push(inner_msg_id);
                    }
                    self.process_incoming_payload(inner_body, inner_msg_id, results, retry_needed)?;
                }
            }
            // rpc_result#f35c6d01 req_msg_id:long result:Object
            0xf35c6d01 => {
                let req_msg_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                if cursor.len() >= 4 {
                    let mut inner_cursor = cursor;
                    let inner_ctor = read_int(&mut inner_cursor).map_err(Self::tl_err)? as u32;
                    if inner_ctor == 0x2144ca19 {
                        // rpc_error#2144ca19 error_code:int error_message:string
                        let code = read_int(&mut inner_cursor).map_err(Self::tl_err)?;
                        let msg = read_string(&mut inner_cursor).map_err(Self::tl_err)?;
                        results.push((
                            req_msg_id,
                            Err(Error::new(
                                ErrorKind::Other,
                                format!("MTProto RPC Error {}: {}", code, msg),
                            )),
                        ));
                        return Ok(());
                    } else if inner_ctor == 0x3072cfa1 {
                        // gzip_packed#3072cfa1 packed_data:string
                        let gz_bytes = read_bytes(&mut inner_cursor).map_err(Self::tl_err)?;
                        let unpacked = gunzip_pure_rust(gz_bytes).unwrap_or_else(|_| gz_bytes.to_vec());
                        if unpacked.len() >= 4 {
                            let mut u_cur = unpacked.as_slice();
                            let u_ctor = read_int(&mut u_cur).unwrap_or(0) as u32;
                            if u_ctor == 0x2144ca19 {
                                let code = read_int(&mut u_cur).unwrap_or(500);
                                let msg = read_string(&mut u_cur).unwrap_or("Unknown RPC error");
                                results.push((
                                    req_msg_id,
                                    Err(Error::new(
                                        ErrorKind::Other,
                                        format!("MTProto RPC Error {}: {}", code, msg),
                                    )),
                                ));
                                return Ok(());
                            }
                        }
                        results.push((req_msg_id, Ok(unpacked)));
                        return Ok(());
                    }
                }
                results.push((req_msg_id, Ok(cursor.to_vec())));
            }
            // bad_server_salt#edab447b bad_msg_id:long bad_msg_seqno:int error_code:int new_server_salt:long
            0xedab447b => {
                let _bad_msg_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                let _bad_seqno = read_int(&mut cursor).map_err(Self::tl_err)?;
                let _err_code = read_int(&mut cursor).map_err(Self::tl_err)?;
                let new_salt = read_long(&mut cursor).map_err(Self::tl_err)?;
                self.session.server_salt = new_salt;
                *retry_needed = true;
            }
            // bad_msg_notification#a7eff811 bad_msg_id:long bad_msg_seqno:int error_code:int
            0xa7eff811 => {
                let _bad_msg_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                let _bad_seqno = read_int(&mut cursor).map_err(Self::tl_err)?;
                let err_code = read_int(&mut cursor).map_err(Self::tl_err)?;
                if err_code == 16 || err_code == 17 {
                    let srv_secs = srv_msg_id >> 32;
                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    self.session.time_offset = srv_secs - now_secs;
                    *retry_needed = true;
                }
            }
            // new_session_created#9ec20908 first_msg_id:long unique_id:long server_salt:long
            0x9ec20908 => {
                let _first_msg_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                let _unique_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                let server_salt = read_long(&mut cursor).map_err(Self::tl_err)?;
                self.session.server_salt = server_salt;
            }
            // gzip_packed#3072cfa1
            0x3072cfa1 => {
                let gz_bytes = read_bytes(&mut cursor).map_err(Self::tl_err)?;
                if let Ok(unpacked) = gunzip_pure_rust(gz_bytes) {
                    self.process_incoming_payload(&unpacked, srv_msg_id, results, retry_needed)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn tl_err(e: super::tl::TLError) -> Error {
        Error::new(ErrorKind::InvalidData, e.to_string())
    }
}

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

        let mut access_hash = 0i64;
        if let Ok(resp) = conn.rpc_call(&req).await {
            if let Some(ah) = extract_user_access_hash(&resp, chat_id) {
                access_hash = ah;
            }
        }

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

        let mut access_hash = 0i64;
        if let Ok(resp) = conn.rpc_call(&req).await {
            if let Some(ah) = extract_channel_access_hash(&resp, channel_id) {
                access_hash = ah;
            }
        }

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
    let ctor = read_int(&mut cur).map_err(MtProtoConnection::tl_err)? as u32;
    if ctor != 0x7f077ad9 {
        return Err(Error::new(ErrorKind::NotFound, "contacts.resolveUsername failed"));
    }
    let peer_ctor = read_int(&mut cur).map_err(MtProtoConnection::tl_err)? as u32;
    let peer_id = read_long(&mut cur).map_err(MtProtoConnection::tl_err)?;

    match peer_ctor {
        0x59511722 => {
            let ah = extract_user_access_hash(&resp, peer_id).unwrap_or(0);
            let mut peer = BytesMut::with_capacity(20);
            write_int(&mut peer, 0xdde8a54cu32 as i32);
            write_long(&mut peer, peer_id);
            write_long(&mut peer, ah);
            Ok(peer)
        }
        0xa2a5371e => {
            let ah = extract_channel_access_hash(&resp, peer_id).unwrap_or(0);
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

/// Uploads a local file (up to 2000 MB) and sends it to the target peer over MTProto 2.0
/// using parallel worker connections sharing the bot's authorized `auth_key` (matching TDLib's SessionMultiProxy)
/// and `upload.saveBigFilePart#de7b673d` + `messages.sendMedia#330e77f`.
pub async fn upload_and_send_large_file(
    api_id: i32,
    api_hash: &str,
    bot_token: &str,
    chat_id_str: &str,
    hint_username: Option<&str>,
    file_path: &Path,
    caption: &str,
    custom_file_name: Option<&str>,
) -> Result<UploadedMediaResult, Error> {
    let metadata = tokio::fs::metadata(file_path).await?;
    let file_size = metadata.len() as usize;
    if file_size == 0 {
        return Err(Error::new(ErrorKind::InvalidInput, "Cannot upload an empty file"));
    }

    let file_name = custom_file_name
        .map(|s| s.to_string())
        .or_else(|| {
            file_path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "document.bin".to_string());

    let mime_type = mime_guess::from_path(&file_name)
        .first_or_octet_stream()
        .to_string();

    // 1. Connect primary MTProto control session and authenticate bot (only 1 DH handshake + 1 importBotAuthorization!)
    let mut main_conn = MtProtoConnection::connect_and_authorize(2, api_id, api_hash, bot_token).await?;
    let home_dc = main_conn.dc_id;

    // 2. Resolve target InputPeer before starting the heavy upload
    let input_peer = resolve_input_peer(&mut main_conn, chat_id_str, hint_username).await?;

    let shared_auth = CachedBotAuth {
        dc_id: home_dc,
        auth_key: *main_conn.session.auth_key(),
        server_salt: main_conn.session.server_salt,
        time_offset: main_conn.session.time_offset,
    };
    auth_cache().insert(bot_token.to_string(), shared_auth);

    // 3. Calculate 512 KB part layout (translated from FileUploader.cpp)
    let total_parts = (file_size + PART_SIZE - 1) / PART_SIZE;
    let is_big = file_size > 10 * 1024 * 1024;
    let file_id: i64 = rand::random::<i64>();

    let num_workers = UPLOAD_WORKERS.min(total_parts.max(1));
    let next_part = Arc::new(AtomicUsize::new(0));
    let path_buf = file_path.to_path_buf();

    let mut handles = Vec::with_capacity(num_workers);
    for _worker_idx in 0..num_workers {
        let next_part_clone = Arc::clone(&next_part);
        let path_clone = path_buf.clone();

        handles.push(tokio::spawn(async move {
            // Share the already-authorized auth_key with a unique session_id (zero extra DH handshakes or auth calls!)
            let mut worker_conn = MtProtoConnection::connect_with_auth(shared_auth, api_id).await?;
            let mut file = File::open(&path_clone).await?;
            let mut part_buf = vec![0u8; PART_SIZE];
            let mut batch_queries = Vec::with_capacity(PIPELINE_DEPTH);

            loop {
                batch_queries.clear();
                for _ in 0..PIPELINE_DEPTH {
                    let part_idx = next_part_clone.fetch_add(1, Ordering::Relaxed);
                    if part_idx >= total_parts {
                        break;
                    }
                    let offset = part_idx * PART_SIZE;
                    let chunk_len = PART_SIZE.min(file_size - offset);

                    file.seek(std::io::SeekFrom::Start(offset as u64)).await?;
                    file.read_exact(&mut part_buf[..chunk_len]).await?;

                    let mut q = BytesMut::with_capacity(chunk_len + 32);
                    if is_big {
                        // upload.saveBigFilePart#de7b673d file_id:long file_part:int file_total_parts:int bytes:bytes = Bool
                        write_int(&mut q, 0xde7b673du32 as i32);
                        write_long(&mut q, file_id);
                        write_int(&mut q, part_idx as i32);
                        write_int(&mut q, total_parts as i32);
                        write_bytes(&mut q, &part_buf[..chunk_len]);
                    } else {
                        // upload.saveFilePart#b304a621 file_id:long file_part:int bytes:bytes = Bool
                        write_int(&mut q, 0xb304a621u32 as i32);
                        write_long(&mut q, file_id);
                        write_int(&mut q, part_idx as i32);
                        write_bytes(&mut q, &part_buf[..chunk_len]);
                    }
                    batch_queries.push(q);
                }

                if batch_queries.is_empty() {
                    break;
                }

                let responses = worker_conn.rpc_call_batch(&batch_queries).await?;
                for resp in responses {
                    if resp.len() >= 4 {
                        let bool_ctor = u32::from_le_bytes(resp[0..4].try_into().unwrap());
                        // boolTrue#997275b5
                        if bool_ctor != 0x997275b5 {
                            return Err(Error::new(
                                ErrorKind::Other,
                                format!("upload.saveBigFilePart returned false (0x{:08x})", bool_ctor),
                            ));
                        }
                    }
                }
            }

            Ok::<(), Error>(())
        }));
    }

    for h in handles {
        h.await
            .map_err(|e| Error::new(ErrorKind::Other, format!("Upload worker panicked: {}", e)))??;
    }

    // 4. Construct messages.sendMedia#330e77f with inputMediaUploadedDocument#37c9330
    // Translated from td/telegram/DocumentsManager.cpp:718 & MessagesManager.cpp:1592
    let random_id: i64 = rand::random::<i64>();
    let mut send_req = BytesMut::with_capacity(512);

    // messages.sendMedia#330e77f
    write_int(&mut send_req, 0x0330e77fu32 as i32);
    write_int(&mut send_req, 0); // flags = 0
    send_req.put_slice(&input_peer);

    // inputMediaUploadedDocument#37c9330
    write_int(&mut send_req, 0x037c9330u32 as i32);
    write_int(&mut send_req, 1 << 4); // flags: force_file (bit 4)
    if is_big {
        // inputFileBig#fa4f0bb5 id:long parts:int name:string
        write_int(&mut send_req, 0xfa4f0bb5u32 as i32);
        write_long(&mut send_req, file_id);
        write_int(&mut send_req, total_parts as i32);
        write_string(&mut send_req, &file_name);
    } else {
        // inputFile#f52ff27f id:long parts:int name:string md5_checksum:string
        write_int(&mut send_req, 0xf52ff27fu32 as i32);
        write_long(&mut send_req, file_id);
        write_int(&mut send_req, total_parts as i32);
        write_string(&mut send_req, &file_name);
        write_string(&mut send_req, "");
    }
    write_string(&mut send_req, &mime_type);

    // attributes: Vector<DocumentAttribute> = [documentAttributeFilename#15590068]
    write_int(&mut send_req, 0x1cb5c415u32 as i32); // vector#1cb5c415
    write_int(&mut send_req, 1);
    write_int(&mut send_req, 0x15590068u32 as i32); // documentAttributeFilename#15590068
    write_string(&mut send_req, &file_name);

    // message (caption) & random_id
    write_string(&mut send_req, caption);
    write_long(&mut send_req, random_id);

    let send_resp = main_conn.rpc_call(&send_req).await?;

    // 5. Parse message_id and document metadata from Updates response
    let (message_id, date, doc_id, access_hash) = parse_send_media_updates(&send_resp, random_id);

    Ok(UploadedMediaResult {
        message_id,
        date,
        doc_id,
        access_hash,
        file_size: file_size as i64,
        file_name,
        mime_type,
        dc_id: home_dc,
    })
}

fn parse_send_media_updates(buf: &[u8], random_id: i64) -> (i32, i64, i64, i64) {
    let mut message_id = 1i32;
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let mut date = now_secs;
    let mut doc_id = rand::random::<i64>().abs();
    let mut access_hash = 0i64;

    let rand_bytes = random_id.to_le_bytes();
    for i in 0..buf.len().saturating_sub(16) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        if ctor == 0x4e90bfd6 && buf[i + 8..i + 16] == rand_bytes {
            message_id = i32::from_le_bytes(buf[i + 4..i + 8].try_into().unwrap());
            break;
        }
    }

    for i in 0..buf.len().saturating_sub(32) {
        let ctor = u32::from_le_bytes(buf[i..i + 4].try_into().unwrap());
        if ctor == 0x8fd4c4d8 {
            doc_id = i64::from_le_bytes(buf[i + 8..i + 16].try_into().unwrap());
            access_hash = i64::from_le_bytes(buf[i + 16..i + 24].try_into().unwrap());
            let mut cur = &buf[i + 24..];
            if read_bytes(&mut cur).is_ok() {
                if let Ok(d) = read_int(&mut cur) {
                    if d > 1_600_000_000 {
                        date = d as i64;
                    }
                }
            }
            break;
        }
    }

    (message_id, date, doc_id, access_hash)
}

/// Pure-Rust RFC 1952 / RFC 1951 gunzip decompressor for `gzip_packed#3072cfa1` MTProto frames.
fn gunzip_pure_rust(data: &[u8]) -> Result<Vec<u8>, Error> {
    if data.len() < 18 || data[0] != 0x1f || data[1] != 0x8b || data[2] != 0x08 {
        return Err(Error::new(ErrorKind::InvalidData, "Invalid gzip header"));
    }
    let flg = data[3];
    let mut pos = 10;
    if (flg & 4) != 0 {
        if pos + 2 > data.len() {
            return Err(Error::new(ErrorKind::UnexpectedEof, "Truncated FEXTRA"));
        }
        let xlen = u16::from_le_bytes([data[pos], data[pos + 1]]) as usize;
        pos += 2 + xlen;
    }
    if (flg & 8) != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        pos += 1;
    }
    if (flg & 16) != 0 {
        while pos < data.len() && data[pos] != 0 {
            pos += 1;
        }
        pos += 1;
    }
    if (flg & 2) != 0 {
        pos += 2;
    }
    if pos >= data.len().saturating_sub(8) {
        return Err(Error::new(ErrorKind::UnexpectedEof, "Empty deflate body"));
    }
    inflate_raw(&data[pos..data.len() - 8])
}

fn inflate_raw(input: &[u8]) -> Result<Vec<u8>, Error> {
    struct BitReader<'a> {
        data: &'a [u8],
        byte_pos: usize,
        bit_pos: u8,
    }
    impl<'a> BitReader<'a> {
        fn read_bits(&mut self, n: u8) -> Result<u32, Error> {
            let mut val = 0u32;
            for i in 0..n {
                if self.byte_pos >= self.data.len() {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Deflate EOF"));
                }
                let bit = ((self.data[self.byte_pos] >> self.bit_pos) & 1) as u32;
                val |= bit << i;
                self.bit_pos += 1;
                if self.bit_pos == 8 {
                    self.bit_pos = 0;
                    self.byte_pos += 1;
                }
            }
            Ok(val)
        }
        fn align_byte(&mut self) {
            if self.bit_pos > 0 {
                self.bit_pos = 0;
                self.byte_pos += 1;
            }
        }
    }

    struct HuffTable {
        counts: [u16; 16],
        symbols: Vec<u16>,
    }
    impl HuffTable {
        fn from_lengths(lengths: &[u8]) -> Self {
            let mut counts = [0u16; 16];
            for &l in lengths {
                if l > 0 && (l as usize) < 16 {
                    counts[l as usize] += 1;
                }
            }
            let mut offsets = [0u16; 16];
            let mut total = 0u16;
            for bits in 1..16 {
                offsets[bits] = total;
                total += counts[bits];
            }
            let mut symbols = vec![0u16; total as usize];
            for (sym, &l) in lengths.iter().enumerate() {
                if l > 0 && (l as usize) < 16 {
                    let idx = offsets[l as usize] as usize;
                    symbols[idx] = sym as u16;
                    offsets[l as usize] += 1;
                }
            }
            Self { counts, symbols }
        }
        fn decode(&self, br: &mut BitReader<'_>) -> Result<u16, Error> {
            let mut code = 0i32;
            let mut first = 0i32;
            let mut index = 0i32;
            for len in 1..16 {
                code |= br.read_bits(1)? as i32;
                let count = self.counts[len] as i32;
                if code - count < first {
                    return Ok(self.symbols[(index + (code - first)) as usize]);
                }
                index += count;
                first += count;
                first <<= 1;
                code <<= 1;
            }
            Err(Error::new(ErrorKind::InvalidData, "Invalid Huffman code"))
        }
    }

    const LEN_BASE: [u16; 29] = [
        3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115,
        131, 163, 195, 227, 258,
    ];
    const LEN_EXTRA: [u8; 29] = [
        0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
    ];
    const DIST_BASE: [u16; 30] = [
        1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
        2049, 3073, 4097, 6145, 8193, 12289, 16385, 24577,
    ];
    const DIST_EXTRA: [u8; 30] = [
        0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12,
        13, 13,
    ];
    const CL_ORDER: [usize; 19] = [
        16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
    ];

    let mut br = BitReader {
        data: input,
        byte_pos: 0,
        bit_pos: 0,
    };
    let mut out = Vec::with_capacity(input.len() * 2);

    loop {
        let bfinal = br.read_bits(1)?;
        let btype = br.read_bits(2)?;
        match btype {
            0 => {
                br.align_byte();
                if br.byte_pos + 4 > br.data.len() {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Stored block header EOF"));
                }
                let len = u16::from_le_bytes([br.data[br.byte_pos], br.data[br.byte_pos + 1]]) as usize;
                br.byte_pos += 4;
                if br.byte_pos + len > br.data.len() {
                    return Err(Error::new(ErrorKind::UnexpectedEof, "Stored block data EOF"));
                }
                out.extend_from_slice(&br.data[br.byte_pos..br.byte_pos + len]);
                br.byte_pos += len;
            }
            1 | 2 => {
                let (lit_table, dist_table) = if btype == 1 {
                    let mut ll = vec![0u8; 288];
                    ll[0..144].fill(8);
                    ll[144..256].fill(9);
                    ll[256..280].fill(7);
                    ll[280..288].fill(8);
                    let dl = vec![5u8; 32];
                    (HuffTable::from_lengths(&ll), HuffTable::from_lengths(&dl))
                } else {
                    let hlit = br.read_bits(5)? as usize + 257;
                    let hdist = br.read_bits(5)? as usize + 1;
                    let hclen = br.read_bits(4)? as usize + 4;
                    let mut cl_lens = [0u8; 19];
                    for i in 0..hclen {
                        cl_lens[CL_ORDER[i]] = br.read_bits(3)? as u8;
                    }
                    let cl_table = HuffTable::from_lengths(&cl_lens);
                    let mut lens = Vec::with_capacity(hlit + hdist);
                    while lens.len() < hlit + hdist {
                        let sym = cl_table.decode(&mut br)?;
                        match sym {
                            0..=15 => lens.push(sym as u8),
                            16 => {
                                let prev = *lens.last().unwrap_or(&0);
                                let rep = br.read_bits(2)? as usize + 3;
                                lens.extend(std::iter::repeat(prev).take(rep));
                            }
                            17 => {
                                let rep = br.read_bits(3)? as usize + 3;
                                lens.extend(std::iter::repeat(0).take(rep));
                            }
                            18 => {
                                let rep = br.read_bits(7)? as usize + 11;
                                lens.extend(std::iter::repeat(0).take(rep));
                            }
                            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid CL sym")),
                        }
                    }
                    (
                        HuffTable::from_lengths(&lens[..hlit]),
                        HuffTable::from_lengths(&lens[hlit..]),
                    )
                };

                loop {
                    let sym = lit_table.decode(&mut br)? as usize;
                    if sym < 256 {
                        out.push(sym as u8);
                    } else if sym == 256 {
                        break;
                    } else {
                        let len_idx = sym - 257;
                        let length = LEN_BASE[len_idx] as usize
                            + br.read_bits(LEN_EXTRA[len_idx])? as usize;
                        let dist_sym = dist_table.decode(&mut br)? as usize;
                        let dist = DIST_BASE[dist_sym] as usize
                            + br.read_bits(DIST_EXTRA[dist_sym])? as usize;
                        if dist > out.len() {
                            return Err(Error::new(ErrorKind::InvalidData, "Invalid LZ77 distance"));
                        }
                        for _ in 0..length {
                            let b = out[out.len() - dist];
                            out.push(b);
                        }
                    }
                }
            }
            _ => return Err(Error::new(ErrorKind::InvalidData, "Invalid deflate block type")),
        }
        if bfinal != 0 {
            break;
        }
    }
    Ok(out)
}
