//! Bot authorization: per-bot auth-key/home-DC caches, DC addresses, home-DC login and cross-DC export/import.
use bytes::BytesMut;
use dashmap::DashMap;
use std::io::{Error, ErrorKind};
use std::sync::OnceLock;
use std::time::Duration;

use super::connection::MtProtoConnection;
use super::tl::{read_bytes, read_int, read_long, write_bytes, write_int, write_long, write_string};

#[derive(Clone, Copy)]
pub struct CachedBotAuth {
    pub dc_id: i32,
    pub auth_key: [u8; 256],
    pub server_salt: i64,
    pub time_offset: i64,
}

static BOT_AUTH_CACHE: OnceLock<DashMap<String, CachedBotAuth>> = OnceLock::new();
static BOT_HOME_DC: OnceLock<DashMap<String, i32>> = OnceLock::new();

fn auth_cache() -> &'static DashMap<String, CachedBotAuth> {
    BOT_AUTH_CACHE.get_or_init(DashMap::new)
}

fn home_dc_cache() -> &'static DashMap<String, i32> {
    BOT_HOME_DC.get_or_init(DashMap::new)
}

/// Remembers `auth` as the authorization for `bot_token` on `auth.dc_id`.
pub(super) fn cache_auth(bot_token: &str, auth: CachedBotAuth) {
    auth_cache().insert(format!("{bot_token}:{}", auth.dc_id), auth);
}

/// Clears cached MTProto authorization when a bot closes or logs out
pub fn invalidate_bot_auth(token: &str) {
    let prefix = format!("{token}:");
    auth_cache().retain(|k, _| !k.starts_with(&prefix) && k != token);
    home_dc_cache().remove(token);
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

impl MtProtoConnection {
    /// Removes cached MTProto authorization for a specific bot and DC
    pub fn invalidate_cached_auth(bot_token: &str, dc_id: i32) {
        let key = format!("{bot_token}:{dc_id}");
        auth_cache().remove(&key);
    }

    /// Connects to and authorizes the bot on its Home DC (where bot token was registered).
    pub async fn connect_home_dc(
        api_id: i32,
        api_hash: &str,
        bot_token: &str,
    ) -> Result<Self, Error> {
        let home_dc = home_dc_cache().get(bot_token).map(|v| *v.value()).unwrap_or(1);
        let cache_key = format!("{bot_token}:{home_dc}");
        if let Some(cached) = auth_cache().get(&cache_key).map(|r| *r.value()) {
            if let Ok(conn) = Self::connect_with_auth(cached, api_id).await {
                return Ok(conn);
            }
        }

        let mut dc_id = home_dc;
        for _attempt in 0..5 {
            let mut conn = Self::connect(dc_id, api_id).await?;
            let auth_query = Self::build_init_and_bot_auth(api_id, api_hash, bot_token);
            conn.inited_connection = true;
            match conn.rpc_call(&auth_query).await {
                Ok(_resp) => {
                    home_dc_cache().insert(bot_token.to_string(), conn.dc_id);
                    auth_cache().insert(format!("{bot_token}:{}", conn.dc_id), conn.cached_auth());
                    return Ok(conn);
                }
                Err(e) => {
                    let msg = e.to_string();
                    tracing::warn!("[auth] importBotAuthorization on dc={} failed: {}", dc_id, msg);
                    if let Some(target_dc) = super::rpc_error::parse_migrate_dc(&msg) {
                        dc_id = target_dc;
                        continue;
                    }
                    if let Some(wait_secs) = super::rpc_error::parse_flood_wait(&msg) {
                        if wait_secs <= 3 {
                            tokio::time::sleep(Duration::from_secs(wait_secs)).await;
                            continue;
                        }
                        return Err(e);
                    }
                    return Err(e);
                }
            }
        }
        Err(Error::new(
            ErrorKind::Other,
            "Exceeded maximum attempts during bot authorization on home DC",
        ))
    }

    /// Connects to Telegram and authorizes the bot via `invokeWithLayer(initConnection(auth.importBotAuthorization))`
    /// bypassing in-memory cache to ensure a fresh session and handshake.
    /// If target DC is different from bot's Home DC, performs MTProto cross-DC authorization:
    /// 1. Exports authorization from Home DC via `auth.exportAuthorization(target_dc)`
    /// 2. Imports authorization to target DC via `auth.importAuthorization(id, bytes)`
    pub async fn connect_and_authorize_fresh(
        target_dc: i32,
        api_id: i32,
        api_hash: &str,
        bot_token: &str,
    ) -> Result<Self, Error> {
        Self::invalidate_cached_auth(bot_token, target_dc);

        let mut home_conn = Self::connect_home_dc(api_id, api_hash, bot_token).await?;
        let home_dc = home_conn.dc_id;

        if target_dc == home_dc {
            return Ok(home_conn);
        }

        // Target DC is different from Home DC -> Export authorization from Home DC and Import to target DC
        tracing::info!("[auth] Exporting authorization from home dc={} for target dc={}", home_dc, target_dc);
        let mut q_export = BytesMut::with_capacity(32);
        write_int(&mut q_export, 0xe5bfffcd_u32 as i32); // auth.exportAuthorization#e5bfffcd dc_id:int
        write_int(&mut q_export, target_dc);

        let export_resp = home_conn.rpc_call(&q_export).await?;
        let mut cur = export_resp.as_slice();
        let ctor = read_int(&mut cur).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))? as u32;
        if ctor != 0xb434e2b8 {
            return Err(Error::new(
                ErrorKind::InvalidData,
                format!("Unexpected exportAuthorization constructor: 0x{:08x}", ctor),
            ));
        }
        let auth_id = read_long(&mut cur).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;
        let auth_bytes = read_bytes(&mut cur).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        tracing::info!("[auth] Importing authorization to target dc={} (id={})", target_dc, auth_id);
        let mut target_conn = Self::connect(target_dc, api_id).await?;
        let mut q_import = BytesMut::with_capacity(64 + auth_bytes.len());
        write_int(&mut q_import, 0xa57a7dad_u32 as i32); // auth.importAuthorization#a57a7dad id:long bytes:bytes
        write_long(&mut q_import, auth_id);
        write_bytes(&mut q_import, auth_bytes);

        target_conn.rpc_call(&q_import).await?;

        let cache_key = format!("{bot_token}:{target_dc}");
        auth_cache().insert(cache_key, target_conn.cached_auth());

        Ok(target_conn)
    }

    /// Connects to Telegram and authorizes the bot for target DC,
    /// using cached auth if available, or executing fresh (cross-DC) authorization.
    pub async fn connect_and_authorize(
        target_dc: i32,
        api_id: i32,
        api_hash: &str,
        bot_token: &str,
    ) -> Result<Self, Error> {
        let cache_key = format!("{bot_token}:{target_dc}");
        if let Some(cached) = auth_cache().get(&cache_key).map(|r| *r.value()) {
            if let Ok(conn) = Self::connect_with_auth(cached, api_id).await {
                return Ok(conn);
            }
        }

        Self::connect_and_authorize_fresh(target_dc, api_id, api_hash, bot_token).await
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_import_auth_tl_encoding() {
        let mut q_export = BytesMut::with_capacity(32);
        write_int(&mut q_export, 0xe5bfffcd_u32 as i32);
        write_int(&mut q_export, 4);

        assert_eq!(q_export.len(), 8);
        let mut cur = q_export.as_ref();
        assert_eq!(read_int(&mut cur).unwrap(), 0xe5bfffcd_u32 as i32);
        assert_eq!(read_int(&mut cur).unwrap(), 4);

        let mut q_import = BytesMut::with_capacity(64);
        let sample_bytes = b"sample_exported_token_bytes";
        write_int(&mut q_import, 0xa57a7dad_u32 as i32);
        write_long(&mut q_import, 1234567890123456_i64);
        write_bytes(&mut q_import, sample_bytes);

        let mut cur_import = q_import.as_ref();
        assert_eq!(read_int(&mut cur_import).unwrap(), 0xa57a7dad_u32 as i32);
        assert_eq!(read_long(&mut cur_import).unwrap(), 1234567890123456_i64);
        assert_eq!(read_bytes(&mut cur_import).unwrap(), sample_bytes);
    }
}
