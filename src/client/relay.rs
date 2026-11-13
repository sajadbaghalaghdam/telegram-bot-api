//! The universal relay: every Bot API method that is not answered locally is forwarded to the
//! upstream Bot API. Methods with local semantics (webhooks, close/logOut, getFile) are handled
//! here; `--local` file uploads are rewritten by `local_media`.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;

use super::engine::TelegramEngine;
use super::reply::{Relayed, ReplyHead};
use super::upstream::send_bytes;
use super::util::ascii_lower;
use crate::error::{TelegramError, TelegramResult};
use crate::models::ApiResponse;

/// One call of the universal relay.
pub struct RelayCall<'a> {
    pub token: &'a str,
    pub method: &'a str,
    /// Decoded query parameters.
    pub query_map: &'a HashMap<String, String>,
    /// The query string exactly as received (forwarded verbatim).
    pub query: Option<&'a str>,
    pub content_type: Option<&'a str>,
    pub body: Bytes,
}

/// The Bot API methods the relay treats specially (names are matched case-insensitively).
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Method {
    GetUpdates,
    SetWebhook,
    Close,
    LogOut,
    DeleteWebhook,
    GetWebhookInfo,
    GetFile,
    SendMediaGroup,
    EditMessageMedia,
    /// Methods whose parameters never reference a local file, so `--local` body scanning is skipped.
    NoLocalMedia,
    Other,
}

impl Method {
    fn of(name: &str) -> Self {
        let mut buf = [0u8; 32];
        match ascii_lower(name, &mut buf) {
            "getupdates" => Self::GetUpdates,
            "setwebhook" => Self::SetWebhook,
            "close" => Self::Close,
            "logout" => Self::LogOut,
            "deletewebhook" => Self::DeleteWebhook,
            "getwebhookinfo" => Self::GetWebhookInfo,
            "getfile" => Self::GetFile,
            "sendmediagroup" => Self::SendMediaGroup,
            "editmessagemedia" => Self::EditMessageMedia,
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
            | "sendchataction" => Self::NoLocalMedia,
            _ => Self::Other,
        }
    }
}

impl TelegramEngine {
    /// Universal call supporting JSON (with local file path auto-streaming), Multipart passthrough, Form-urlencoded, and Query params
    pub async fn universal_call(
        self: &Arc<Self>,
        token: &str,
        method: &str,
        query_map: &HashMap<String, String>,
        query_str: Option<&str>,
        content_type: Option<&str>,
        body_bytes: Bytes,
    ) -> TelegramResult<Bytes> {
        let relayed = self
            .relay(RelayCall {
                token,
                method,
                query_map,
                query: query_str,
                content_type,
                body: body_bytes,
            })
            .await?;
        Ok(relayed.body)
    }

    /// Validates the token, executes the call and keeps the seen-bots map in sync with the answer.
    pub async fn relay(self: &Arc<Self>, call: RelayCall<'_>) -> TelegramResult<Relayed> {
        let token = call.token;
        self.check_token_filter(token)?;
        let reply = self.relay_inner(call).await.map(Relayed::new);
        self.note_upstream_reply(token, &reply);
        reply
    }

    async fn relay_inner(self: &Arc<Self>, call: RelayCall<'_>) -> TelegramResult<Bytes> {
        let method = Method::of(call.method);

        if method == Method::GetUpdates && self.webhook_workers.contains_key(call.token) {
            return Err(TelegramError::Conflict(
                "can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first".to_string(),
            ));
        }

        match method {
            Method::SetWebhook => return self.relay_set_webhook(&call).await,
            Method::Close => {
                // C++ re-creates the client on the next request; webhook delivery must keep running.
                crate::mtproto::uploader::invalidate_bot_auth(call.token);
                return Ok(Bytes::from_static(
                    br#"{"ok":true,"result":true,"description":"Bot instance closed"}"#,
                ));
            }
            Method::LogOut => return self.relay_log_out(call.token).await,
            Method::DeleteWebhook => return self.relay_delete_webhook(&call).await,
            Method::GetWebhookInfo => {
                let info = self.get_webhook_info(call.token).await?;
                return Ok(json_bytes(&ApiResponse::success(info)));
            }
            Method::GetFile => {
                if let Some(reply) = self.relay_get_file(&call).await {
                    return Ok(reply);
                }
            }
            _ => {}
        }

        let url = self.upstream_url(call.token, call.method, call.query);

        if self.local_mode
            && let Some(res) = self.relay_local_media(&call, method, &url).await
        {
            return res;
        }

        self.forward(&url, call.content_type, call.body).await
    }

    /// setWebhook: query + JSON + form-urlencoded + multipart are merged into one argument map.
    async fn relay_set_webhook(self: &Arc<Self>, call: &RelayCall<'_>) -> TelegramResult<Bytes> {
        let args = super::webhook::parse_request_args(call.query_map, call.content_type, &call.body)
            .map_err(TelegramError::BadRequest)?;
        let req = super::webhook::set_webhook_request_from_args(&args).map_err(TelegramError::BadRequest)?;
        let raw = super::webhook::RawForward {
            query: call.query,
            content_type: call.content_type,
            body: &call.body,
        };
        let ok = self.set_webhook_raw(call.token, &req, Some(raw)).await?;
        if !ok {
            return Err(TelegramError::BadRequest("Failed to set webhook".to_string()));
        }
        Ok(Bytes::from_static(
            br#"{"ok":true,"result":true,"description":"Webhook was set"}"#,
        ))
    }

    /// logOut: relay the upstream result; only tear down local state once upstream confirmed it.
    async fn relay_log_out(&self, token: &str) -> TelegramResult<Bytes> {
        let url = self.upstream_url(token, "logOut", None);
        let out = send_bytes(self.client.post(url)).await?;
        if ReplyHead::parse(&out).ok == Some(true) {
            self.stop_webhook_worker(token);
            self.storage.delete_webhook(token).await;
            crate::mtproto::uploader::invalidate_bot_auth(token);
        }
        Ok(out)
    }

    async fn relay_delete_webhook(&self, call: &RelayCall<'_>) -> TelegramResult<Bytes> {
        let drop_pending = call
            .query_map
            .get("drop_pending_updates")
            .and_then(|v| v.parse::<bool>().ok())
            .or_else(|| {
                if call.body.is_empty() {
                    return None;
                }
                serde_json::from_slice::<serde_json::Value>(&call.body)
                    .ok()
                    .and_then(|j| j.get("drop_pending_updates").and_then(|v| v.as_bool()))
            })
            .unwrap_or(false);

        let _ = self.delete_webhook(call.token, drop_pending).await?;
        Ok(Bytes::from_static(
            br#"{"ok":true,"result":true,"description":"Webhook was deleted"}"#,
        ))
    }

    /// getFile with a `file_id` (query, JSON body or form body). Errors are answered as a JSON body
    /// (status derived by the caller); `None` when there is no file_id, so the call is relayed as is.
    async fn relay_get_file(&self, call: &RelayCall<'_>) -> Option<Bytes> {
        let file_id = file_id_of(call)?;
        let json = match self.get_file(call.token, &file_id).await {
            Ok(file) => json_bytes(&ApiResponse::success(file)),
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
                json_bytes(&err_json)
            }
        };
        Some(json)
    }

    /// Direct passthrough (preserves multipart/form-data, application/json, form-urlencoded, or empty body).
    pub(super) async fn forward(
        &self,
        url: &str,
        content_type: Option<&str>,
        body: Bytes,
    ) -> TelegramResult<Bytes> {
        let ct = content_type.unwrap_or("");
        let req_builder = if body.is_empty() && ct.is_empty() {
            self.client.post(url)
        } else if !ct.is_empty() {
            self.client.post(url).header("content-type", ct).body(body)
        } else {
            self.client
                .post(url)
                .header("content-type", "application/json")
                .body(body)
        };
        send_bytes(req_builder).await
    }

    /// Upstream URL of a bot method: `<base>/bot<token>/<method>[?query]`.
    pub(super) fn upstream_url(&self, token: &str, method: &str, query: Option<&str>) -> String {
        match query {
            Some(q) if !q.is_empty() => format!("{}/bot{token}/{method}?{q}", self.upstream_base_url),
            _ => format!("{}/bot{token}/{method}", self.upstream_base_url),
        }
    }
}

/// `file_id` of a getFile call: query parameter first, then JSON body, then form body.
fn file_id_of(call: &RelayCall<'_>) -> Option<String> {
    let mut file_id = call.query_map.get("file_id").cloned().unwrap_or_default();
    if file_id.is_empty() && !call.body.is_empty() {
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&call.body)
            && let Some(fid) = json.get("file_id").and_then(|v| v.as_str())
        {
            file_id = fid.to_string();
        }
        if file_id.is_empty()
            && let Ok(body_str) = std::str::from_utf8(&call.body)
        {
            file_id = body_str
                .split('&')
                .filter_map(|pair| pair.split_once('='))
                .find(|(k, _)| *k == "file_id")
                .map(|(_, v)| v.to_string())
                .unwrap_or_default();
        }
    }
    (!file_id.is_empty()).then_some(file_id)
}

fn json_bytes<T: serde::Serialize>(v: &T) -> Bytes {
    Bytes::from(serde_json::to_vec(v).unwrap_or_default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn method_names_are_case_insensitive() {
        assert!(Method::of("SetWebhook") == Method::SetWebhook);
        assert!(Method::of("LOGOUT") == Method::LogOut);
        assert!(Method::of("sendMessage") == Method::NoLocalMedia);
        assert!(Method::of("sendPhoto") == Method::Other);
        assert!(Method::of("a-very-long-method-name-that-does-not-fit-the-buffer") == Method::Other);
    }

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
        let none = HashMap::new();

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
