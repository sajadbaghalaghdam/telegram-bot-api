use std::collections::HashMap;
use axum::{
    body::Bytes,
    extract::{FromRequest, Multipart, Path, Query, RawQuery, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use tokio::io::AsyncWriteExt;
use crate::client::relay::RelayCall;
use crate::client::upload::LargeUpload;
use crate::error::{TelegramError, TelegramResult};
use crate::server::AppState;

/// The multipart field carrying the media of a send method (None: unknown method, the first file
/// part is used).
fn large_upload_media_param(method: &str) -> Option<&'static str> {
    match method.to_ascii_lowercase().as_str() {
        "sendphoto" => Some("photo"),
        "sendvideo" => Some("video"),
        "senddocument" => Some("document"),
        "sendaudio" => Some("audio"),
        "sendvoice" => Some("voice"),
        "sendanimation" => Some("animation"),
        "sendvideonote" => Some("video_note"),
        _ => None,
    }
}

fn is_media_field(media_param: Option<&str>, field_name: &str) -> bool {
    media_param.is_none_or(|m| m == field_name)
}

/// A non-2xx upstream answer that is not a Bot API JSON object becomes a proper JSON error with
/// the upstream status as `error_code`; Bot API error bodies pass through untouched.
fn relay_status_error(status: u16, body: &[u8]) -> Option<TelegramError> {
    if (200..300).contains(&status) {
        return None;
    }
    let is_api_json = serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .is_some_and(|j| j.get("ok").is_some());
    if is_api_json {
        return None;
    }
    Some(TelegramError::from_http_status(status, &format!("upstream returned HTTP {status}")))
}

/// Handles any official Telegram Bot API POST or GET method dynamically,
/// supporting JSON (with local file path auto-streaming), large Multipart (>50MB up to 2GB via MTProto 2.0),
/// Form-urlencoded, and Query params.
pub async fn generic_post_handler(
    Path((token, method)): Path<(String, String)>,
    State(state): State<AppState>,
    Query(query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
    req: Request,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    state.engine.check_token_filter(clean_token)?;

    if method.eq_ignore_ascii_case("getme") {
        let session = state.client_manager.get_or_create(clean_token).await?;
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            session.me_json_bytes.clone(),
        )
            .into_response());
    }

    let ct = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let content_len = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // If a large (> 49 MB up to 2000 MB) multipart/form-data upload arrives, stream it directly to a temp file
    // in O(1) memory and upload it via Pure-Rust MTProto 2.0 (upload.saveBigFilePart + messages.sendMedia)
    if ct
        .as_deref()
        .map(|c| c.starts_with("multipart/form-data"))
        .unwrap_or(false)
        && content_len > 49 * 1024 * 1024
    {
        let mut multipart = Multipart::from_request(req, &state)
            .await
            .map_err(|e| TelegramError::BadRequest(format!("Invalid multipart stream: {e}")))?;

        let _ = tokio::fs::create_dir_all(&state.engine.temp_dir).await;
        let temp_path = state
            .engine
            .temp_dir
            .join(format!("mtproto_upload_{}.tmp", rand::random::<u64>()));

        let mut chat_id_str = query_map.get("chat_id").cloned().unwrap_or_default();
        let mut caption = query_map.get("caption").cloned().unwrap_or_default();
        let mut orig_file_name: Option<String> = None;
        let mut extra_fields = serde_json::Map::new();
        let mut has_temp_file = false;

        // Only the method's media field (e.g. `video`) is streamed to the large-file path; other
        // file parts (`thumbnail`, ...) are skipped so they cannot overwrite the media.
        let media_param = large_upload_media_param(&method);

        while let Some(mut field) = multipart
            .next_field()
            .await
            .map_err(|e| TelegramError::BadRequest(format!("Multipart field error: {e}")))?
        {
            let name = field.name().unwrap_or("").to_string();
            if let Some(fname) = field.file_name().map(|f| f.to_string()) {
                if has_temp_file || !is_media_field(media_param, &name) {
                    continue;
                }
                orig_file_name = Some(fname);
                let mut f = tokio::fs::File::create(&temp_path)
                    .await
                    .map_err(|e| TelegramError::Internal(format!("Temp file create error: {e}")))?;
                has_temp_file = true;
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|e| TelegramError::BadRequest(format!("Multipart chunk error: {e}")))?
                {
                    f.write_all(&chunk)
                        .await
                        .map_err(|e| TelegramError::Internal(format!("Temp write error: {e}")))?;
                }
                f.flush()
                    .await
                    .map_err(|e| TelegramError::Internal(format!("Temp flush error: {e}")))?;
            } else if name == "chat_id" {
                if let Ok(txt) = field.text().await {
                    chat_id_str = txt;
                }
            } else if name == "caption" {
                if let Ok(txt) = field.text().await {
                    caption = txt;
                }
            } else if !name.is_empty()
                && let Ok(txt) = field.text().await {
                    if let Ok(val) = serde_json::from_str::<serde_json::Value>(&txt) {
                        extra_fields.insert(name, val);
                    } else {
                        extra_fields.insert(name, serde_json::Value::String(txt));
                    }
                }
        }

        if has_temp_file {
            let res = state
                .engine
                .upload_local_file_mtproto(LargeUpload {
                    token: clean_token,
                    method: &method,
                    chat_id: &chat_id_str,
                    path: &temp_path,
                    caption: &caption,
                    file_name: orig_file_name.as_deref(),
                    extra_fields: Some(&extra_fields),
                })
                .await;
            let _ = tokio::fs::remove_file(&temp_path).await;
            let out = res?;
            return Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                out,
            )
                .into_response());
        }
        return Err(TelegramError::BadRequest(
            "No file part found in large multipart upload".to_string(),
        ));
    }

    let body = Bytes::from_request(req, &state)
        .await
        .map_err(|e| TelegramError::BadRequest(format!("Failed to read request body: {e}")))?;

    let relayed = state
        .engine
        .relay(RelayCall {
            token: clean_token,
            method: &method,
            query_map: &query_map,
            query: raw_query.as_deref(),
            content_type: ct.as_deref(),
            body,
        })
        .await?;

    let (status, retry_after) = match relayed.head.failure() {
        Some((code, retry_after)) => (StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST), retry_after),
        None => (StatusCode::OK, None),
    };
    let out = relayed.body;

    if status == StatusCode::OK
        && (method.eq_ignore_ascii_case("close") || method.eq_ignore_ascii_case("logout"))
    {
        state.client_manager.close(clean_token);
    }

    let mut resp = (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        out,
    )
        .into_response();

    if let Some(ra) = retry_after
        && let Ok(val) = ra.to_string().parse() {
            resp.headers_mut().insert(header::RETRY_AFTER, val);
        }

    Ok(resp)
}

/// Handles any official Telegram Bot API GET method dynamically
pub async fn generic_get_handler(
    Path((token, method)): Path<(String, String)>,
    State(state): State<AppState>,
    Query(query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
    req: Request,
) -> TelegramResult<Response> {
    generic_post_handler(
        Path((token, method)),
        State(state),
        Query(query_map),
        RawQuery(raw_query),
        req,
    )
    .await
}

/// Handles any official Telegram Bot API POST method for the Test DC environment (/test/bot<token>/<method>)
pub async fn generic_post_test_handler(
    Path((token, method)): Path<(String, String)>,
    State(state): State<AppState>,
    Query(_query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
    req: Request,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    state.engine.check_token_filter(clean_token)?;
    let ct = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let body_bytes = axum::body::to_bytes(req.into_body(), 100 * 1024 * 1024)
        .await
        .map_err(|e| TelegramError::Internal(format!("Failed reading request body: {e}")))?;

    // Same configured upstream (and proxy-aware client) as every other relayed call.
    let test_url = format!("{}/test/bot{clean_token}/{method}", state.engine.upstream_base_url);
    let mut req_builder = match raw_query {
        Some(ref q) if !q.is_empty() => state.engine.client.post(format!("{test_url}?{q}")),
        _ => state.engine.client.post(&test_url),
    };
    if let Some(ref c) = ct {
        req_builder = req_builder.header(header::CONTENT_TYPE, c);
    }
    let resp = req_builder
        .body(body_bytes)
        .send()
        .await
        .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;

    let status = resp.status().as_u16();
    let retry_after = resp.headers().get(header::RETRY_AFTER).cloned();
    let out = resp
        .bytes()
        .await
        .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;

    if let Some(err) = relay_status_error(status, &out) {
        return Err(err);
    }

    // Pass the upstream status through (Bot API errors carry their own error_code in the body).
    let mut response = (
        StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
        [(header::CONTENT_TYPE, "application/json")],
        out,
    )
        .into_response();
    if let Some(ra) = retry_after {
        response.headers_mut().insert(header::RETRY_AFTER, ra);
    }
    Ok(response)
}

/// Handles any official Telegram Bot API GET method for the Test DC environment
pub async fn generic_get_test_handler(
    Path((token, method)): Path<(String, String)>,
    State(state): State<AppState>,
    Query(query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
    req: Request,
) -> TelegramResult<Response> {
    generic_post_test_handler(
        Path((token, method)),
        State(state),
        Query(query_map),
        RawQuery(raw_query),
        req,
    )
    .await
}

/// Fallback handler returning official Telegram Bot API 404 JSON for unknown routes/methods
pub async fn fallback_404_handler() -> Response {
    (
        StatusCode::NOT_FOUND,
        [(header::CONTENT_TYPE, "application/json")],
        r#"{"ok":false,"error_code":404,"description":"Not Found: method not found"}"#,
    )
        .into_response()
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::{ClientManager, TelegramEngine};
    use clap::Parser;
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn media_param_per_method() {
        assert_eq!(large_upload_media_param("sendVideo"), Some("video"));
        assert_eq!(large_upload_media_param("sendvideonote"), Some("video_note"));
        assert_eq!(large_upload_media_param("sendDocument"), Some("document"));
        assert_eq!(large_upload_media_param("sendMessage"), None);
    }

    #[test]
    fn only_the_media_field_is_streamed() {
        let p = large_upload_media_param("sendVideo");
        assert!(is_media_field(p, "video"));
        assert!(!is_media_field(p, "thumbnail"));
        assert!(!is_media_field(p, "cover"));
        // unknown method: any file part qualifies (the first one wins in the handler)
        assert!(is_media_field(None, "file"));
    }

    #[test]
    fn upstream_status_is_mapped_to_json_errors() {
        assert!(relay_status_error(200, br#"{"ok":true}"#).is_none());
        // Bot API error bodies pass through untouched (status kept by the caller)
        assert!(relay_status_error(400, br#"{"ok":false,"error_code":400,"description":"x"}"#).is_none());
        // HTML from a gateway becomes a JSON error with the right code
        assert!(matches!(
            relay_status_error(502, b"<html>Bad Gateway</html>"),
            Some(TelegramError::ApiError { code: 502, .. })
        ));
        assert!(matches!(relay_status_error(404, b""), Some(TelegramError::NotFound(_))));
        assert!(matches!(relay_status_error(401, b"nope"), Some(TelegramError::Unauthorized(_))));
    }

    /// HTTP server answering every request with `status` / `body`; records request paths.
    async fn canned_upstream(status: &'static str, body: &'static str, paths: Arc<std::sync::Mutex<Vec<String>>>) -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                let paths = paths.clone();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 8192];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buf[..n]).to_string();
                    paths.lock().unwrap().push(head.split(' ').nth(1).unwrap_or("").to_string());
                    let r = format!(
                        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nRetry-After: 7\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(r.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        port
    }

    async fn call_test_dc(status: &'static str, body: &'static str) -> (Response, Vec<String>) {
        let paths = Arc::new(std::sync::Mutex::new(Vec::new()));
        let port = canned_upstream(status, body, paths.clone()).await;
        let dir = std::env::temp_dir().join(format!("generic_test_dc_{}_{}", std::process::id(), port));
        let storage = crate::storage::StorageEngine::new(&dir).unwrap();
        let mut engine = TelegramEngine::new(false, dir.clone(), dir.clone(), storage);
        engine.upstream_base_url = format!("http://127.0.0.1:{port}");
        let engine = Arc::new(engine);
        let state = AppState {
            config: Arc::new(crate::config::ServerConfig::parse_from(["bot"])),
            engine: engine.clone(),
            client_manager: Arc::new(ClientManager::new(engine)),
            stats: Arc::new(crate::stats::ServerStats::new()),
            watchdog: crate::watchdog::Watchdog::new(60),
        };
        let req = Request::builder().method("POST").body(axum::body::Body::from("x=1")).unwrap();
        let resp = generic_post_test_handler(
            Path(("bot123456:AAH-abcdefghijklmnopqrstuvwxyz012345".to_string(), "getMe".to_string())),
            State(state),
            Query(HashMap::new()),
            RawQuery(Some("a=b".to_string())),
            req,
        )
        .await
        .unwrap_or_else(|e| e.into_response());
        let _ = std::fs::remove_dir_all(&dir);
        let seen = paths.lock().unwrap().clone();
        (resp, seen)
    }

    #[tokio::test]
    async fn test_dc_relay_uses_configured_upstream_and_passes_status() {
        let (resp, paths) = call_test_dc("200 OK", r#"{"ok":true,"result":1}"#).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(paths, ["/test/bot123456:AAH-abcdefghijklmnopqrstuvwxyz012345/getMe?a=b"]);

        let (resp, _) = call_test_dc(
            "429 Too Many Requests",
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 7","parameters":{"retry_after":7}}"#,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(resp.headers().get(header::RETRY_AFTER).unwrap(), "7");

        let (resp, _) = call_test_dc("502 Bad Gateway", "<html>oops</html>").await;
        assert_eq!(resp.status().as_u16(), 502);
        let body = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let j: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(j["ok"], false);
        assert_eq!(j["error_code"], 502);
    }
}
