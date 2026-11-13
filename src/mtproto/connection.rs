//! `MtProtoConnection`: one TCP connection + encrypted MTProto session with RPC call/batch primitives.
//!
//! The receive path decrypts each packet **in place** in a per-connection buffer and hands
//! RPC results to the caller as borrowed slices of that buffer (`rpc_call_with`,
//! `rpc_call_batch_with`); the send path builds, encrypts and frames each message in one
//! reusable buffer. After connection setup no per-message heap allocation is needed.
use bytes::{BufMut, BytesMut};
use std::io::{Error, ErrorKind};
use std::ops::Range;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use super::auth::{dc_address, CachedBotAuth};
use super::gzip::gunzip_pure_rust;
use super::handshake::MtProtoHandshake;
use super::packet::{pack_encrypted_frame, unpack_encrypted_in_place, OutgoingHeader, PacketError};
use super::rpc_error::parse_flood_wait;
use super::session::MtProtoSession;
use super::tl::{read_bytes, read_int, read_long, read_string, write_int, write_long, write_string, TLError};
use super::transport::IntermediateTransport;

/// MTProto Layer matching TDLib production deployment
const MTPROTO_LAYER: i32 = 225;

/// Re-sends of a request after `bad_server_salt` / `bad_msg_notification` before giving up.
const MAX_RESENDS: u32 = 8;
/// Re-sends of a request after `FLOOD_WAIT_x` before giving up.
const MAX_FLOOD_RETRIES: u32 = 5;
/// Longest `FLOOD_WAIT_x` a single RPC sleeps through; longer waits fail fast (HTTP 429 upstream).
const SINGLE_RPC_MAX_FLOOD_WAIT_SECS: u64 = 3;
/// Longest `FLOOD_WAIT_x` a pipelined batch (file parts) sleeps through.
pub const BATCH_MAX_FLOOD_WAIT_SECS: u64 = 60;
/// Upper bound for the size of a decompressed `gzip_packed` payload.
const MAX_GUNZIP_OUTPUT: usize = 16 * 1024 * 1024;
/// Nesting limit for `msg_container` / `gzip_packed`.
const MAX_DISPATCH_DEPTH: u32 = 8;

const CTOR_RPC_RESULT: u32 = 0xf35c6d01;
const CTOR_RPC_ERROR: u32 = 0x2144ca19;
const CTOR_GZIP_PACKED: u32 = 0x3072cfa1;
const CTOR_MSG_CONTAINER: u32 = 0x73f1f8dc;
const CTOR_BAD_SERVER_SALT: u32 = 0xedab447b;
const CTOR_BAD_MSG_NOTIFICATION: u32 = 0xa7eff811;
const CTOR_NEW_SESSION_CREATED: u32 = 0x9ec20908;

fn tl_err(e: TLError) -> Error {
    Error::new(ErrorKind::InvalidData, e.to_string())
}

fn rpc_error(code: i32, msg: &str) -> Error {
    Error::other(format!("MTProto RPC Error {}: {}", code, msg))
}

/// Outcome for one request message id, found while dispatching an incoming packet.
enum Reply {
    /// `rpc_result` payload: byte range inside the connection's receive buffer (zero copy).
    Body(Range<usize>),
    /// Payload that had to be materialised (gunzipped).
    Owned(Vec<u8>),
    Error(Error),
}

/// Borrowed view of the connection state that incoming-message dispatch mutates.
struct Inbound<'a> {
    session: &'a mut MtProtoSession,
    pending_acks: &'a mut Vec<i64>,
    replies: &'a mut Vec<(i64, Reply)>,
    /// A salt / clock correction requires re-sending the in-flight requests.
    retry: bool,
}

impl Inbound<'_> {
    /// Interprets `buf[range]` (one server message body); `Reply::Body` ranges are absolute
    /// offsets into `buf`.
    fn dispatch(&mut self, buf: &[u8], range: Range<usize>, srv_msg_id: i64, depth: u32) -> Result<(), Error> {
        if range.len() < 4 {
            return Ok(());
        }
        if depth > MAX_DISPATCH_DEPTH {
            return Err(Error::new(ErrorKind::InvalidData, "MTProto message nesting too deep"));
        }
        let end = range.end;
        let mut cursor = &buf[range];
        let ctor = read_int(&mut cursor).map_err(tl_err)? as u32;

        match ctor {
            // msg_container#73f1f8dc messages:vector<%Message>
            CTOR_MSG_CONTAINER => {
                let count = read_int(&mut cursor).map_err(tl_err)?;
                for _ in 0..count {
                    if cursor.len() < 16 {
                        return Err(Error::new(ErrorKind::InvalidData, "Truncated msg_container"));
                    }
                    let inner_msg_id = read_long(&mut cursor).map_err(tl_err)?;
                    let inner_seqno = read_int(&mut cursor).map_err(tl_err)?;
                    let inner_bytes = read_int(&mut cursor).map_err(tl_err)? as usize;
                    if cursor.len() < inner_bytes {
                        return Err(Error::new(ErrorKind::InvalidData, "msg_container inner message truncated"));
                    }
                    let inner_start = end - cursor.len();
                    cursor = &cursor[inner_bytes..];
                    if (inner_seqno & 1) == 1 {
                        self.pending_acks.push(inner_msg_id);
                    }
                    self.dispatch(buf, inner_start..inner_start + inner_bytes, inner_msg_id, depth + 1)?;
                }
            }
            // rpc_result#f35c6d01 req_msg_id:long result:Object
            CTOR_RPC_RESULT => {
                let req_msg_id = read_long(&mut cursor).map_err(tl_err)?;
                let result_start = end - cursor.len();
                if cursor.len() >= 4 {
                    let mut inner_cursor = cursor;
                    let inner_ctor = read_int(&mut inner_cursor).map_err(tl_err)? as u32;
                    if inner_ctor == CTOR_RPC_ERROR {
                        // rpc_error#2144ca19 error_code:int error_message:string
                        let code = read_int(&mut inner_cursor).map_err(tl_err)?;
                        let msg = read_string(&mut inner_cursor).map_err(tl_err)?;
                        self.replies.push((req_msg_id, Reply::Error(rpc_error(code, msg))));
                        return Ok(());
                    } else if inner_ctor == CTOR_GZIP_PACKED {
                        // gzip_packed#3072cfa1 packed_data:string
                        let gz_bytes = read_bytes(&mut inner_cursor).map_err(tl_err)?;
                        let unpacked = match gunzip_pure_rust(gz_bytes, MAX_GUNZIP_OUTPUT) {
                            Ok(u) => u,
                            Err(e) => {
                                tracing::error!("[mtproto] gzip_packed rpc_result decompression failed: {}", e);
                                self.replies.push((
                                    req_msg_id,
                                    Reply::Error(Error::new(
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
                            if u_ctor == CTOR_RPC_ERROR {
                                let code = read_int(&mut u_cur).unwrap_or(500);
                                let msg = read_string(&mut u_cur).unwrap_or("Unknown RPC error");
                                self.replies.push((req_msg_id, Reply::Error(rpc_error(code, msg))));
                                return Ok(());
                            }
                        }
                        self.replies.push((req_msg_id, Reply::Owned(unpacked)));
                        return Ok(());
                    }
                }
                self.replies.push((req_msg_id, Reply::Body(result_start..end)));
            }
            // bad_server_salt#edab447b bad_msg_id:long bad_msg_seqno:int error_code:int new_server_salt:long
            CTOR_BAD_SERVER_SALT => {
                let _bad_msg_id = read_long(&mut cursor).map_err(tl_err)?;
                let _bad_seqno = read_int(&mut cursor).map_err(tl_err)?;
                let _err_code = read_int(&mut cursor).map_err(tl_err)?;
                let new_salt = read_long(&mut cursor).map_err(tl_err)?;
                self.session.server_salt = new_salt;
                self.retry = true;
            }
            // bad_msg_notification#a7eff811 bad_msg_id:long bad_msg_seqno:int error_code:int
            CTOR_BAD_MSG_NOTIFICATION => {
                let bad_msg_id = read_long(&mut cursor).map_err(tl_err)?;
                let _bad_seqno = read_int(&mut cursor).map_err(tl_err)?;
                let err_code = read_int(&mut cursor).map_err(tl_err)?;
                if err_code == 16 || err_code == 17 {
                    // msg_id too low / too high: resynchronise the clock and re-send.
                    let srv_secs = srv_msg_id >> 32;
                    let now_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs() as i64;
                    self.session.time_offset = srv_secs - now_secs;
                    self.retry = true;
                } else {
                    // Any other code means the server will never answer this request: fail it
                    // now instead of waiting for the read timeout.
                    self.replies.push((
                        bad_msg_id,
                        Reply::Error(Error::new(
                            ErrorKind::InvalidData,
                            format!("MTProto bad_msg_notification error_code {err_code}"),
                        )),
                    ));
                }
            }
            // new_session_created#9ec20908 first_msg_id:long unique_id:long server_salt:long
            CTOR_NEW_SESSION_CREATED => {
                let _first_msg_id = read_long(&mut cursor).map_err(tl_err)?;
                let _unique_id = read_long(&mut cursor).map_err(tl_err)?;
                let server_salt = read_long(&mut cursor).map_err(tl_err)?;
                self.session.server_salt = server_salt;
            }
            // gzip_packed#3072cfa1
            CTOR_GZIP_PACKED => {
                let gz_bytes = read_bytes(&mut cursor).map_err(tl_err)?;
                let unpacked = gunzip_pure_rust(gz_bytes, MAX_GUNZIP_OUTPUT).map_err(|e| {
                    tracing::error!("[mtproto] gzip_packed decompression failed: {}", e);
                    Error::new(ErrorKind::InvalidData, format!("gzip_packed decompression failed: {}", e))
                })?;
                // Replies found inside refer to `unpacked`, so materialise them.
                let mut inner_replies = Vec::new();
                let mut inner = Inbound {
                    session: &mut *self.session,
                    pending_acks: &mut *self.pending_acks,
                    replies: &mut inner_replies,
                    retry: false,
                };
                inner.dispatch(&unpacked, 0..unpacked.len(), srv_msg_id, depth + 1)?;
                self.retry |= inner.retry;
                for (id, reply) in inner_replies {
                    let reply = match reply {
                        Reply::Body(r) => Reply::Owned(unpacked[r].to_vec()),
                        other => other,
                    };
                    self.replies.push((id, reply));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Active MTProto 2.0 Connection (translated from td/mtproto/SessionConnection.cpp)
pub struct MtProtoConnection {
    pub dc_id: i32,
    transport: IntermediateTransport,
    session: MtProtoSession,
    /// Last received packet, decrypted in place.
    recv_buf: Vec<u8>,
    /// Last sent frame (built and encrypted in place).
    send_buf: Vec<u8>,
    /// Scratch for `msgs_ack` bodies.
    ack_buf: Vec<u8>,
    pending_acks: Vec<i64>,
    /// Replies found in the last dispatched packet (reused).
    replies: Vec<(i64, Reply)>,
    pub(super) inited_connection: bool,
    api_id: i32,
}

impl MtProtoConnection {
    fn new(dc_id: i32, transport: IntermediateTransport, session: MtProtoSession, inited: bool, api_id: i32) -> Self {
        Self {
            dc_id,
            transport,
            session,
            recv_buf: Vec::with_capacity(4096),
            send_buf: Vec::new(),
            ack_buf: Vec::new(),
            pending_acks: Vec::with_capacity(16),
            replies: Vec::with_capacity(4),
            inited_connection: inited,
            api_id,
        }
    }

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
        parse_flood_wait(err_msg)
    }

    /// Connects to the specified DC and performs the 6-step Diffie-Hellman handshake.
    pub async fn connect(dc_id: i32, api_id: i32) -> Result<Self, Error> {
        let addr = dc_address(dc_id);
        let mut transport = IntermediateTransport::connect(addr).await?;
        let hs = MtProtoHandshake::with_dc(&mut transport, dc_id).run().await?;
        let session = MtProtoSession::with_time_offset(hs.auth_key, hs.server_salt, hs.time_offset);
        Ok(Self::new(dc_id, transport, session, false, api_id))
    }

    /// Opens an additional parallel TCP connection sharing the bot's authorized `auth_key`
    /// with a fresh `session_id` (matching TDLib's SessionMultiProxy upload workers).
    pub async fn connect_with_auth(auth: CachedBotAuth, api_id: i32) -> Result<Self, Error> {
        let addr = dc_address(auth.dc_id);
        let transport = IntermediateTransport::connect(addr).await?;
        let session = MtProtoSession::with_time_offset(auth.auth_key, auth.server_salt, auth.time_offset);
        // Auth key is already initialized & authorized with Bot credentials.
        Ok(Self::new(auth.dc_id, transport, session, true, api_id))
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

    /// Executes a single MTProto RPC call and returns a copy of the result body.
    /// Prefer [`Self::rpc_call_with`] for large results.
    pub async fn rpc_call(&mut self, query: &[u8]) -> Result<Vec<u8>, Error> {
        self.rpc_call_with(query, |body| Ok(body.to_vec())).await
    }

    /// Executes a single MTProto RPC call, handling `bad_server_salt`, `bad_msg_notification`,
    /// `new_session_created`, `msg_container`, `FLOOD_WAIT_X`, and `msgs_ack`. `on_result` is
    /// invoked with the result body borrowed from the receive buffer (no copy).
    pub async fn rpc_call_with<R>(
        &mut self,
        query: &[u8],
        on_result: impl FnOnce(&[u8]) -> Result<R, Error>,
    ) -> Result<R, Error> {
        let wrapped;
        let effective: &[u8] = if self.inited_connection {
            query
        } else {
            self.inited_connection = true;
            wrapped = Self::wrap_with_init_connection(self.api_id, query);
            &wrapped
        };

        let mut req_msg_id = self.send_encrypted(effective, true).await?;
        let (mut resends, mut flood_retries) = (0u32, 0u32);

        loop {
            self.flush_acks().await?;
            if self.recv_and_dispatch().await? {
                resends += 1;
                if resends > MAX_RESENDS {
                    return Err(Error::other("MTProto request re-sent too many times (salt/clock corrections)"));
                }
                req_msg_id = self.send_encrypted(effective, true).await?;
                continue;
            }

            let Some(pos) = self.replies.iter().position(|(id, _)| *id == req_msg_id) else {
                continue;
            };
            let (_, reply) = self.replies.swap_remove(pos);
            self.flush_acks().await?;
            match reply {
                Reply::Body(range) => return on_result(&self.recv_buf[range]),
                Reply::Owned(data) => return on_result(&data),
                Reply::Error(e) => {
                    let msg = e.to_string();
                    if let Some(wait_secs) = parse_flood_wait(&msg) {
                        if wait_secs <= SINGLE_RPC_MAX_FLOOD_WAIT_SECS && flood_retries < MAX_FLOOD_RETRIES {
                            flood_retries += 1;
                            tracing::warn!("[rpc] dc={} {} -> sleeping {}s", self.dc_id, msg, wait_secs);
                            tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                            req_msg_id = self.send_encrypted(effective, true).await?;
                            continue;
                        }
                        tracing::warn!("[rpc] dc={} {} -> fast-fail (429)", self.dc_id, msg);
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Pipelined batch RPC call: sends all `queries` back-to-back before reading their
    /// responses (e.g. `upload.saveBigFilePart`), maximizing throughput over high-latency links.
    /// `on_result(index, body)` is called once per query with the result body borrowed from the
    /// receive buffer. Any non-flood RPC error aborts the batch.
    pub async fn rpc_call_batch_with<Q: AsRef<[u8]>>(
        &mut self,
        queries: &[Q],
        mut on_result: impl FnMut(usize, &[u8]) -> Result<(), Error>,
    ) -> Result<(), Error> {
        if queries.is_empty() {
            return Ok(());
        }

        let wrapped_first = if self.inited_connection {
            None
        } else {
            self.inited_connection = true;
            Some(Self::wrap_with_init_connection(self.api_id, queries[0].as_ref()))
        };
        let query_at = |i: usize| -> &[u8] {
            match (&wrapped_first, i) {
                (Some(w), 0) => w,
                _ => queries[i].as_ref(),
            }
        };

        let mut expected_ids = Vec::with_capacity(queries.len());
        for i in 0..queries.len() {
            expected_ids.push(self.send_encrypted(query_at(i), true).await?);
        }

        let mut done = vec![false; queries.len()];
        let mut remaining = queries.len();
        let (mut resends, mut flood_retries) = (0u32, 0u32);

        while remaining > 0 {
            self.flush_acks().await?;
            if self.recv_and_dispatch().await? {
                resends += 1;
                if resends > MAX_RESENDS {
                    return Err(Error::other("MTProto batch re-sent too many times (salt/clock corrections)"));
                }
                for i in 0..queries.len() {
                    if !done[i] {
                        expected_ids[i] = self.send_encrypted(query_at(i), true).await?;
                    }
                }
                continue;
            }

            let mut replies = std::mem::take(&mut self.replies);
            for (id, reply) in replies.drain(..) {
                let Some(pos) = expected_ids.iter().position(|&e| e == id) else {
                    continue;
                };
                if done[pos] {
                    continue;
                }
                match reply {
                    Reply::Body(range) => on_result(pos, &self.recv_buf[range])?,
                    Reply::Owned(data) => on_result(pos, &data)?,
                    Reply::Error(e) => {
                        let msg = e.to_string();
                        match parse_flood_wait(&msg) {
                            Some(wait_secs)
                                if wait_secs <= BATCH_MAX_FLOOD_WAIT_SECS && flood_retries < MAX_FLOOD_RETRIES =>
                            {
                                flood_retries += 1;
                                tracing::warn!("[rpc] dc={} {} -> sleeping {}s", self.dc_id, msg, wait_secs);
                                tokio::time::sleep(Duration::from_secs(wait_secs.max(1))).await;
                                expected_ids[pos] = self.send_encrypted(query_at(pos), true).await?;
                                continue;
                            }
                            _ => return Err(e),
                        }
                    }
                }
                done[pos] = true;
                remaining -= 1;
            }
            self.replies = replies;
        }

        self.flush_acks().await?;
        Ok(())
    }

    /// Reads one packet, decrypts it in place and dispatches its content into `self.replies`.
    /// Returns `true` when the in-flight requests must be re-sent (salt / clock correction).
    async fn recv_and_dispatch(&mut self) -> Result<bool, Error> {
        self.transport.read_packet(&mut self.recv_buf).await?;

        let unpacked = unpack_encrypted_in_place(
            self.session.auth_key(),
            self.session.auth_key_id(),
            self.session.session_id,
            &mut self.recv_buf,
        )
        .map_err(|e| match e {
            PacketError::TransportError(code) => {
                Error::new(ErrorKind::PermissionDenied, format!("MTProto transport error {}", code))
            }
            other => Error::new(ErrorKind::InvalidData, other.to_string()),
        })?;

        if (unpacked.seq_no & 1) == 1 {
            self.pending_acks.push(unpacked.msg_id);
        }

        self.replies.clear();
        let mut inbound = Inbound {
            session: &mut self.session,
            pending_acks: &mut self.pending_acks,
            replies: &mut self.replies,
            retry: false,
        };
        inbound.dispatch(&self.recv_buf, unpacked.data, unpacked.msg_id, 0)?;
        Ok(inbound.retry)
    }

    async fn send_encrypted(&mut self, data: &[u8], is_content_related: bool) -> Result<i64, Error> {
        let msg_id = self.session.generate_msg_id().map_err(Error::other)?;
        let seq_no = self.session.next_seq_no(is_content_related);
        let header = OutgoingHeader {
            server_salt: self.session.server_salt,
            session_id: self.session.session_id,
            msg_id,
            seq_no,
        };
        pack_encrypted_frame(&mut self.send_buf, self.session.auth_key(), self.session.auth_key_id(), &header, data)
            .map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        self.transport.send_frame(&self.send_buf).await?;
        Ok(msg_id)
    }

    async fn flush_acks(&mut self) -> Result<(), Error> {
        if self.pending_acks.is_empty() {
            return Ok(());
        }
        // msgs_ack#62d6b459 msg_ids:Vector<long>
        let mut ack_buf = std::mem::take(&mut self.ack_buf);
        ack_buf.clear();
        write_int(&mut ack_buf, 0x62d6b459u32 as i32);
        write_int(&mut ack_buf, 0x1cb5c415u32 as i32); // vector#1cb5c415
        write_int(&mut ack_buf, self.pending_acks.len() as i32);
        for &id in &self.pending_acks {
            write_long(&mut ack_buf, id);
        }
        self.pending_acks.clear();
        let sent = self.send_encrypted(&ack_buf, false).await;
        self.ack_buf = ack_buf;
        sent.map(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inbound_parts() -> (MtProtoSession, Vec<i64>, Vec<(i64, Reply)>) {
        (MtProtoSession::new([1u8; 256], 5), Vec::new(), Vec::new())
    }

    fn rpc_result(req_id: i64, result: &[u8]) -> Vec<u8> {
        let mut v = CTOR_RPC_RESULT.to_le_bytes().to_vec();
        v.extend_from_slice(&req_id.to_le_bytes());
        v.extend_from_slice(result);
        v
    }

    #[test]
    fn dispatch_rpc_result_is_zero_copy_range() {
        let (mut s, mut acks, mut replies) = inbound_parts();
        let body = [0x97u8, 0x72, 0x75, 0x99]; // boolTrue
        let msg = rpc_result(77, &body);
        let mut ib = Inbound { session: &mut s, pending_acks: &mut acks, replies: &mut replies, retry: false };
        ib.dispatch(&msg, 0..msg.len(), 9, 0).unwrap();
        assert!(!ib.retry);
        match &replies[0] {
            (77, Reply::Body(r)) => assert_eq!(&msg[r.clone()], &body),
            _ => panic!("expected Body reply"),
        }
    }

    #[test]
    fn dispatch_container_error_salt_and_bad_msg() {
        let (mut s, mut acks, mut replies) = inbound_parts();

        // rpc_error inside a container with an odd seqno (must be acked).
        let mut err = CTOR_RPC_ERROR.to_le_bytes().to_vec();
        err.extend_from_slice(&420i32.to_le_bytes());
        write_string(&mut err, "FLOOD_WAIT_7");
        let inner = rpc_result(5, &err);
        let mut c = CTOR_MSG_CONTAINER.to_le_bytes().to_vec();
        c.extend_from_slice(&1i32.to_le_bytes());
        c.extend_from_slice(&101i64.to_le_bytes());
        c.extend_from_slice(&1i32.to_le_bytes());
        c.extend_from_slice(&(inner.len() as i32).to_le_bytes());
        c.extend_from_slice(&inner);
        {
            let mut ib = Inbound { session: &mut s, pending_acks: &mut acks, replies: &mut replies, retry: false };
            ib.dispatch(&c, 0..c.len(), 9, 0).unwrap();
        }
        assert_eq!(acks, vec![101]);
        match replies.pop().unwrap() {
            (5, Reply::Error(e)) => assert_eq!(parse_flood_wait(&e.to_string()), Some(7)),
            _ => panic!("expected rpc error"),
        }

        // bad_server_salt updates the salt and asks for a resend.
        let mut m = CTOR_BAD_SERVER_SALT.to_le_bytes().to_vec();
        m.extend_from_slice(&1i64.to_le_bytes());
        m.extend_from_slice(&1i32.to_le_bytes());
        m.extend_from_slice(&48i32.to_le_bytes());
        m.extend_from_slice(&0x1234i64.to_le_bytes());
        let mut ib = Inbound { session: &mut s, pending_acks: &mut acks, replies: &mut replies, retry: false };
        ib.dispatch(&m, 0..m.len(), 9, 0).unwrap();
        assert!(ib.retry);
        assert_eq!(s.server_salt, 0x1234);

        // bad_msg_notification with a non-clock code fails that request instead of hanging.
        let mut m = CTOR_BAD_MSG_NOTIFICATION.to_le_bytes().to_vec();
        m.extend_from_slice(&42i64.to_le_bytes());
        m.extend_from_slice(&1i32.to_le_bytes());
        m.extend_from_slice(&33i32.to_le_bytes());
        let mut ib = Inbound { session: &mut s, pending_acks: &mut acks, replies: &mut replies, retry: false };
        ib.dispatch(&m, 0..m.len(), 9, 0).unwrap();
        assert!(!ib.retry);
        assert!(matches!(replies.pop(), Some((42, Reply::Error(_)))));
    }

    #[test]
    fn dispatch_rejects_excessive_nesting() {
        let (mut s, mut acks, mut replies) = inbound_parts();
        let mut msg = CTOR_MSG_CONTAINER.to_le_bytes().to_vec();
        msg.extend_from_slice(&0i32.to_le_bytes());
        let mut ib = Inbound { session: &mut s, pending_acks: &mut acks, replies: &mut replies, retry: false };
        assert!(ib.dispatch(&msg, 0..msg.len(), 9, MAX_DISPATCH_DEPTH + 1).is_err());
    }
}
