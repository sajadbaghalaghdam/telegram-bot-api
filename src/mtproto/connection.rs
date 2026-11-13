//! `MtProtoConnection`: one TCP connection + encrypted MTProto session with RPC call/batch primitives.
use bytes::{BufMut, BytesMut};
use std::io::{Error, ErrorKind};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::auth::{dc_address, CachedBotAuth};
use super::gzip::gunzip_pure_rust;
use super::handshake::MtProtoHandshake;
use super::packet::{pack_encrypted_message, unpack_encrypted_message, PacketError};
use super::session::MtProtoSession;
use super::tl::{read_bytes, read_int, read_long, read_string, write_int, write_long, write_string};
use super::transport::IntermediateTransport;

/// MTProto Layer matching TDLib production deployment
const MTPROTO_LAYER: i32 = 225;

/// Active MTProto 2.0 Connection (translated from td/mtproto/SessionConnection.cpp)
pub struct MtProtoConnection {
    pub dc_id: i32,
    pub transport: IntermediateTransport,
    pub session: MtProtoSession,
    read_buf: Vec<u8>,
    pending_acks: Vec<i64>,
    pub(super) inited_connection: bool,
    api_id: i32,
}

impl MtProtoConnection {
    /// Snapshot of this connection's authorization, shareable with worker connections.
    pub fn cached_auth(&self) -> CachedBotAuth {
        CachedBotAuth {
            dc_id: self.dc_id,
            auth_key: *self.session.auth_key(),
            server_salt: self.session.server_salt,
            time_offset: self.session.time_offset,
        }
    }

    /// See [`super::rpc_error::parse_migrate_dc`].
    pub fn parse_migrate_dc(err_msg: &str) -> Option<i32> {
        super::rpc_error::parse_migrate_dc(err_msg)
    }

    /// See [`super::rpc_error::parse_flood_wait`].
    pub fn parse_flood_wait(err_msg: &str) -> Option<u64> {
        super::rpc_error::parse_flood_wait(err_msg)
    }

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
            inited_connection: true, // Auth key is already initialized & authorized with Bot credentials
            api_id,
        })
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
                self.session.session_id,
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
                            if let Some(wait_secs) = super::rpc_error::parse_flood_wait(&msg) {
                                if wait_secs <= 3 {
                                    tracing::warn!("[rpc] dc={} {} -> sleeping {}s", self.dc_id, msg, wait_secs);
                                    tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                                    req_msg_id = self.send_encrypted(&effective_query, true).await?;
                                    break;
                                }
                                tracing::warn!("[rpc] dc={} {} -> fast-fail (429)", self.dc_id, msg);
                                return Err(e);
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
                self.session.session_id,
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
                        if let Some(wait_secs) = super::rpc_error::parse_flood_wait(&msg) {
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
                        return Err(Error::new(ErrorKind::InvalidData, "Truncated msg_container"));
                    }
                    let inner_msg_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                    let inner_seqno = read_int(&mut cursor).map_err(Self::tl_err)?;
                    let inner_bytes = read_int(&mut cursor).map_err(Self::tl_err)? as usize;
                    if cursor.len() < inner_bytes {
                        return Err(Error::new(ErrorKind::InvalidData, "msg_container inner message truncated"));
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
                        let unpacked = match gunzip_pure_rust(gz_bytes) {
                            Ok(u) => u,
                            Err(e) => {
                                tracing::error!("[mtproto] gzip_packed rpc_result decompression failed: {}", e);
                                results.push((
                                    req_msg_id,
                                    Err(Error::new(
                                        ErrorKind::InvalidData,
                                        format!("gzip_packed decompression failed: {}", e),
                                    )),
                                ));
                                return Ok(());
                            }
                        };
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
                let unpacked = gunzip_pure_rust(gz_bytes).map_err(|e| {
                    tracing::error!("[mtproto] gzip_packed decompression failed: {}", e);
                    Error::new(ErrorKind::InvalidData, format!("gzip_packed decompression failed: {}", e))
                })?;
                self.process_incoming_payload(&unpacked, srv_msg_id, results, retry_needed)?;
            }
            // pong#347773c5 msg_id:long ping_id:long
            0x347773c5 => {
                let _pong_msg_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                let ping_id = read_long(&mut cursor).map_err(Self::tl_err)?;
                results.push((ping_id, Ok(cursor.to_vec())));
            }
            _ => {}
        }
        Ok(())
    }

    fn tl_err(e: super::tl::TLError) -> Error {
        Error::new(ErrorKind::InvalidData, e.to_string())
    }
}
