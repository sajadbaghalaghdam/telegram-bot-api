//! The universal relay: every Bot API method that is not answered locally is forwarded to the
//! upstream Bot API; local-file uploads (`--local`) are rewritten to multipart / MTProto.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use reqwest::multipart::Form;
use serde_json::Value;
use tokio::fs;

use super::engine::TelegramEngine;
use crate::error::{TelegramError, TelegramResult};
use crate::models::ApiResponse;

/// Media field names in Telegram Bot API that may reference a local file path in `--local` mode
const MEDIA_FIELDS: &[&str] = &[
    "photo",
    "video",
    "document",
    "audio",
    "voice",
    "animation",
    "video_note",
    "sticker",
    "thumbnail",
    "certificate",
];

impl TelegramEngine {
    /// Universal call supporting JSON (with local file path auto-streaming), Multipart passthrough, Form-urlencoded, and Query params
    pub async fn universal_call(
        self: &Arc<Self>,
        token: &str,
        method: &str,
        query_map: &std::collections::HashMap<String, String>,
        query_str: Option<&str>,
        content_type: Option<&str>,
        body_bytes: Bytes,
    ) -> TelegramResult<Bytes> {
        self.check_token_filter(token)?;
        let reply = self
            .universal_call_inner(token, method, query_map, query_str, content_type, body_bytes)
            .await;
        self.note_upstream_reply(token, &reply);
        reply
    }

    async fn universal_call_inner(
        self: &Arc<Self>,
        token: &str,
        method: &str,
        query_map: &std::collections::HashMap<String, String>,
        query_str: Option<&str>,
        content_type: Option<&str>,
        body_bytes: Bytes,
    ) -> TelegramResult<Bytes> {
        let method_lower = method.to_ascii_lowercase();

        if method_lower == "getupdates" && self.webhook_workers.contains_key(token) {
            return Err(TelegramError::Conflict(
                "can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first".to_string(),
            ));
        }

        // 1. Handle setWebhook / deleteWebhook / getWebhookInfo / getFile specially
        if method_lower == "setwebhook" {
            // Query + JSON + form-urlencoded + multipart are merged into one argument map.
            let args = super::webhook::parse_request_args(query_map, content_type, &body_bytes)
                .map_err(TelegramError::BadRequest)?;
            let req = super::webhook::set_webhook_request_from_args(&args)
                .map_err(TelegramError::BadRequest)?;
            let raw = super::webhook::RawForward {
                query: query_str,
                content_type,
                body: &body_bytes,
            };
            let ok = self.set_webhook_raw(token, &req, Some(raw)).await?;
            if !ok {
                return Err(TelegramError::BadRequest("Failed to set webhook".to_string()));
            }
            return Ok(Bytes::from_static(
                br#"{"ok":true,"result":true,"description":"Webhook was set"}"#,
            ));
        }

        if method_lower == "close" {
            // C++ re-creates the client on the next request; webhook delivery must keep running.
            crate::mtproto::uploader::invalidate_bot_auth(token);
            return Ok(Bytes::from_static(
                br#"{"ok":true,"result":true,"description":"Bot instance closed"}"#,
            ));
        }

        if method_lower == "logout" {
            // Relay the upstream result; only tear down local state once upstream confirmed logOut.
            let url = format!("{}/bot{token}/logOut", self.upstream_base_url);
            let out = self
                .client
                .post(&url)
                .send()
                .await
                .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
                .bytes()
                .await
                .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
            let ok = serde_json::from_slice::<Value>(&out)
                .ok()
                .and_then(|j| j.get("ok").and_then(|v| v.as_bool()))
                .unwrap_or(false);
            if ok {
                self.stop_webhook_worker(token);
                self.storage.delete_webhook(token).await;
                crate::mtproto::uploader::invalidate_bot_auth(token);
            }
            return Ok(out);
        }

        if method_lower == "deletewebhook" {
            let drop_pending = query_map
                .get("drop_pending_updates")
                .and_then(|v| v.parse::<bool>().ok())
                .or_else(|| {
                    if !body_bytes.is_empty() {
                        serde_json::from_slice::<Value>(&body_bytes)
                            .ok()
                            .and_then(|j| j.get("drop_pending_updates").and_then(|v| v.as_bool()))
                    } else {
                        None
                    }
                })
                .unwrap_or(false);

            let _ = self.delete_webhook(token, drop_pending).await?;
            return Ok(Bytes::from_static(
                br#"{"ok":true,"result":true,"description":"Webhook was deleted"}"#,
            ));
        }

        if method_lower == "getwebhookinfo" {
            let info = self.get_webhook_info(token).await?;
            let resp = ApiResponse::success(info);
            let vec = serde_json::to_vec(&resp).unwrap_or_default();
            return Ok(Bytes::from(vec));
        }

        if method_lower == "getfile" {
            let mut file_id = query_map.get("file_id").cloned().unwrap_or_default();
            if file_id.is_empty() && !body_bytes.is_empty() {
                if let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) {
                    if let Some(fid) = json.get("file_id").and_then(|v| v.as_str()) {
                        file_id = fid.to_string();
                    }
                }
                if file_id.is_empty() {
                    if let Ok(body_str) = std::str::from_utf8(&body_bytes) {
                        for pair in body_str.split('&') {
                            if let Some((k, v)) = pair.split_once('=') {
                                if k == "file_id" {
                                    file_id = v.to_string();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if !file_id.is_empty() {
                match self.get_file(token, &file_id).await {
                    Ok(file) => {
                        let resp = ApiResponse::success(file);
                        let vec = serde_json::to_vec(&resp).unwrap_or_default();
                        return Ok(Bytes::from(vec));
                    }
                    Err(e) => {
                        let err_json = match e {
                            TelegramError::ApiError { code, description, parameters } => serde_json::json!({
                                "ok": false,
                                "error_code": code,
                                "description": description,
                                "parameters": parameters
                            }),
                            _ => serde_json::json!({
                                "ok": false,
                                "error_code": 400,
                                "description": e.to_string()
                            }),
                        };
                        return Ok(Bytes::from(serde_json::to_vec(&err_json).unwrap_or_default()));
                    }
                }
            }
        }

        // 2. Build upstream URL with optional query string
        let url = match query_str {
            Some(q) if !q.is_empty() => {
                format!("{}/bot{token}/{method}?{q}", self.upstream_base_url)
            }
            _ => format!("{}/bot{token}/{method}", self.upstream_base_url),
        };

        let ct = content_type.unwrap_or("");

        // 3. If JSON body in local_mode, fast-check whether this method and payload can possibly contain a local file path
        let may_have_local_media = self.local_mode
            && !body_bytes.is_empty()
            && (ct.starts_with("application/json") || body_bytes.starts_with(b"{"))
            && (body_bytes.contains(&b'/') || body_bytes.contains(&b'\\'))
            && !matches!(
                method_lower.as_str(),
                "sendmessage"
                    | "editmessagetext"
                    | "editmessagecaption"
                    | "editmessagereplymarkup"
                    | "deletemessage"
                    | "deletemessages"
                    | "forwardmessage"
                    | "forwardmessages"
                    | "copymessage"
                    | "copymessages"
                    | "answercallbackquery"
                    | "answerinlinequery"
                    | "getchat"
                    | "getchatmember"
                    | "getchatmembercount"
                    | "getchatadministrators"
                    | "banchatmember"
                    | "unbanchatmember"
                    | "restrictchatmember"
                    | "sendchataction"
            );

        if may_have_local_media {
            if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&body_bytes) {
                if method_lower == "sendmediagroup" {
                    if let Some(Value::Array(items)) = map.get("media") {
                        let mut new_items = items.clone();
                        let mut parts_to_add: Vec<(String, PathBuf)> = Vec::new();
                        for (idx, it) in new_items.iter_mut().enumerate() {
                            if let Some(m_str) = it.get("media").and_then(|v| v.as_str()) {
                                if let Some(local_p) = self.resolve_local_path(m_str) {
                                    let attach_name = format!("file_{idx}");
                                    it["media"] = Value::String(format!("attach://{attach_name}"));
                                    parts_to_add.push((attach_name, local_p));
                                }
                            }
                        }
                        if !parts_to_add.is_empty() {
                            let mut form = Form::new();
                            for (attach_name, local_p) in parts_to_add {
                                let part = Self::build_streaming_part(&local_p, "media.bin").await?;
                                form = form.part(attach_name, part);
                            }
                            form = form.text("media", serde_json::to_string(&new_items).unwrap_or_default());
                            for (k, v) in map {
                                if k == "media" || v.is_null() {
                                    continue;
                                }
                                let val_str = match v {
                                    Value::String(s) => s,
                                    Value::Bool(b) => b.to_string(),
                                    Value::Number(n) => n.to_string(),
                                    other => serde_json::to_string(&other).unwrap_or_default(),
                                };
                                form = form.text(k, val_str);
                            }
                            let resp = self.client.post(&url).multipart(form).send().await
                                .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
                            let out = resp.bytes().await
                                .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
                            return Ok(out);
                        }
                    }
                }

                if method_lower == "editmessagemedia" {
                    if let Some(Value::Object(mut item)) = map.get("media").cloned() {
                        if let Some(m_str) = item.get("media").and_then(|v| v.as_str()) {
                            if let Some(local_p) = self.resolve_local_path(m_str) {
                                item.insert("media".to_string(), Value::String("attach://file_edit".to_string()));
                                let part = Self::build_streaming_part(&local_p, "media.bin").await?;
                                let mut form = Form::new().part("file_edit", part);
                                form = form.text("media", serde_json::to_string(&item).unwrap_or_default());
                                for (k, v) in map {
                                    if k == "media" || v.is_null() {
                                        continue;
                                    }
                                    let val_str = match v {
                                        Value::String(s) => s,
                                        Value::Bool(b) => b.to_string(),
                                        Value::Number(n) => n.to_string(),
                                        other => serde_json::to_string(&other).unwrap_or_default(),
                                    };
                                    form = form.text(k, val_str);
                                }
                                let resp = self.client.post(&url).multipart(form).send().await
                                    .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
                                let out = resp.bytes().await
                                    .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
                                return Ok(out);
                            }
                        }
                    }
                }

                let mut local_media_field: Option<(String, PathBuf)> = None;
                for &field_name in MEDIA_FIELDS {
                    if let Some(Value::String(s)) = map.get(field_name) {
                        if let Some(local_p) = self.resolve_local_path(s) {
                            local_media_field = Some((field_name.to_string(), local_p));
                            break;
                        } else if s.starts_with("http://") || s.starts_with("https://") {
                            let chat_id_str = map
                                .get("chat_id")
                                .map(|v| match v {
                                    Value::String(s) => s.clone(),
                                    Value::Number(n) => n.to_string(),
                                    _ => String::new(),
                                })
                                .or_else(|| query_map.get("chat_id").cloned())
                                .unwrap_or_default();
                            let caption = map
                                .get("caption")
                                .and_then(|v| v.as_str())
                                .or_else(|| query_map.get("caption").map(|s| s.as_str()))
                                .unwrap_or("");
                            if let Some(res) = self
                                .maybe_upload_large_remote_url_mtproto(token, method, &chat_id_str, s, caption)
                                .await
                            {
                                return res;
                            }
                        }
                    }
                }

                if let Some((media_key, local_path)) = local_media_field {
                    let file_len = fs::metadata(&local_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);

                    if file_len > 49 * 1024 * 1024 {
                        let chat_id_str = map
                            .get("chat_id")
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                Value::Number(n) => n.to_string(),
                                _ => String::new(),
                            })
                            .or_else(|| query_map.get("chat_id").cloned())
                            .unwrap_or_default();
                        let caption = map
                            .get("caption")
                            .and_then(|v| v.as_str())
                            .or_else(|| query_map.get("caption").map(|s| s.as_str()))
                            .unwrap_or("");
                        return self
                            .upload_local_file_mtproto(
                                token,
                                method,
                                &chat_id_str,
                                &local_path,
                                caption,
                                None,
                                Some(&map),
                            )
                            .await;
                    }

                    let part = Self::build_streaming_part(&local_path, "file.bin").await?;
                    let mut form = Form::new().part(media_key.clone(), part);
                    for (k, v) in map {
                        if k == media_key || v.is_null() {
                            continue;
                        }
                        let val_str = match v {
                            Value::String(s) => s,
                            Value::Bool(b) => b.to_string(),
                            Value::Number(n) => n.to_string(),
                            other => serde_json::to_string(&other).unwrap_or_default(),
                        };
                        form = form.text(k, val_str);
                    }
                    let resp = self
                        .client
                        .post(&url)
                        .multipart(form)
                        .send()
                        .await
                        .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
                    let out = resp
                        .bytes()
                        .await
                        .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
                    return Ok(out);
                }
            }
        }

        // 3b. Check query_map for local media paths or large remote URLs in --local mode
        if self.local_mode && !query_map.is_empty() {
            for &field_name in MEDIA_FIELDS {
                if let Some(val) = query_map.get(field_name) {
                    let chat_id_str = query_map.get("chat_id").cloned().unwrap_or_default();
                    let caption = query_map.get("caption").map(|s| s.as_str()).unwrap_or("");
                    if let Some(local_path) = self.resolve_local_path(val) {
                        let file_len = fs::metadata(&local_path)
                            .await
                            .map(|m| m.len())
                            .unwrap_or(0);
                        if file_len > 49 * 1024 * 1024 {
                            return self
                                .upload_local_file_mtproto(
                                    token,
                                    method,
                                    &chat_id_str,
                                    &local_path,
                                    caption,
                                    None,
                                    None,
                                )
                                .await;
                        }
                    } else if val.starts_with("http://") || val.starts_with("https://") {
                        if let Some(res) = self
                            .maybe_upload_large_remote_url_mtproto(token, method, &chat_id_str, val, caption)
                            .await
                        {
                            return res;
                        }
                    }
                }
            }
        }

        // 4. Direct passthrough (preserves multipart/form-data, application/json, form-urlencoded, or empty body)
        let req_builder = if body_bytes.is_empty() && ct.is_empty() {
            self.client.post(&url)
        } else if !ct.is_empty() {
            self.client
                .post(&url)
                .header("content-type", ct)
                .body(body_bytes)
        } else {
            self.client
                .post(&url)
                .header("content-type", "application/json")
                .body(body_bytes)
        };

        let resp = req_builder
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
        let out = resp
            .bytes()
            .await
            .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
        Ok(out)
    }
}

#[cfg(test)]
mod active_bot_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Mock upstream: ok for token "1:good", otherwise a 401 JSON error.
    async fn mock_upstream() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buf[..n]).to_string();
                    let body = if head.contains("/bot1:good/") {
                        r#"{"ok":true,"result":true}"#
                    } else {
                        r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#
                    };
                    let r = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(r.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn only_upstream_accepted_tokens_are_counted() {
        let port = mock_upstream().await;
        let dir = std::env::temp_dir().join(format!("activebot_{}", std::process::id()));
        let storage = crate::storage::StorageEngine::new(&dir).unwrap();
        let mut e = TelegramEngine::new(false, dir.clone(), dir.clone(), storage);
        e.upstream_base_url = format!("http://127.0.0.1:{port}");
        let e = Arc::new(e);
        let none = std::collections::HashMap::new();

        for i in 2..12 {
            let tok = format!("{i}:fake");
            let r = e.universal_call(&tok, "getChat", &none, None, None, Bytes::new()).await.unwrap();
            assert!(String::from_utf8_lossy(&r).contains("401"));
        }
        assert_eq!(e.active_bots.len(), 0, "fake tokens must not be recorded");

        e.universal_call("1:good", "getChat", &none, None, None, Bytes::new()).await.unwrap();
        assert_eq!(e.active_bots.len(), 1);

        // A later 401 for a recorded token removes it again.
        e.record_active_bot("7:revoked");
        assert_eq!(e.active_bots.len(), 2);
        e.universal_call("7:revoked", "getChat", &none, None, None, Bytes::new()).await.unwrap();
        assert_eq!(e.active_bots.len(), 1);

        // get_me with a bad token errors and is not counted.
        assert!(e.get_me("9:bad").await.is_err());
        assert_eq!(e.active_bots.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
