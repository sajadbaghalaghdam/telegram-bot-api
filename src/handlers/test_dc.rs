//! Test DC environment routes (`/test/bot<token>/<method>` and `/test/file/bot<token>/<path>`),
//! matching the official HttpServer.h. Always relayed to the configured upstream.

use axum::{
    extract::{RawPathParams, Request, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};

use super::generic::{json_response, token_and_method};
use crate::error::{TelegramError, TelegramResult};
use crate::server::SharedState;

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

/// Handles any official Telegram Bot API method for the Test DC environment (POST and GET).
pub async fn relay_test_handler(
    State(state): State<SharedState>,
    params: RawPathParams,
    req: Request,
) -> TelegramResult<Response> {
    let (token, method) = token_and_method(&params);
    let token = token.strip_prefix("bot").unwrap_or(token);
    state.engine.check_token_filter(token)?;

    let (parts, body) = req.into_parts();
    let body = axum::body::to_bytes(body, 100 * 1024 * 1024)
        .await
        .map_err(|e| TelegramError::Internal(format!("Failed reading request body: {e}")))?;

    // Same configured upstream (and proxy-aware client) as every other relayed call.
    let base = &state.engine.upstream_base_url;
    let url = match parts.uri.query() {
        Some(q) if !q.is_empty() => format!("{base}/test/bot{token}/{method}?{q}"),
        _ => format!("{base}/test/bot{token}/{method}"),
    };
    let mut req_builder = state.engine.client.post(url);
    if let Some(ct) = parts.headers.get(header::CONTENT_TYPE) {
        req_builder = req_builder.header(header::CONTENT_TYPE, ct);
    }
    let resp = req_builder
        .body(body)
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
    let mut response = json_response(StatusCode::from_u16(status).unwrap_or(StatusCode::OK), out);
    if let Some(ra) = retry_after {
        response.headers_mut().insert(header::RETRY_AFTER, ra);
    }
    Ok(response)
}

/// Serve file content for Test DC environment: GET /test/file/bot{token}/{*file_path}
pub async fn serve_test_file(
    State(state): State<SharedState>,
    params: axum::extract::RawPathParams,
) -> TelegramResult<Response> {
    let (mut token, mut file_path) = ("", "");
    for (name, value) in &params {
        match name {
            "token" => token = value,
            "file_path" => file_path = value,
            _ => {}
        }
    }
    let token = token.strip_prefix("bot").unwrap_or(token);
    state.engine.check_token_filter(token)?;
    let dl_url = format!("{}/test/file/bot{token}/{file_path}", state.engine.upstream_base_url);
    let resp = state
        .engine
        .client
        .get(&dl_url)
        .send()
        .await
        .map_err(|e| TelegramError::Internal(format!("File download network error: {e}")))?;

    if !resp.status().is_success() {
        return Err(TelegramError::from_http_status(
            resp.status().as_u16(),
            &format!("File download failed: status {}", resp.status()),
        ));
    }

    let mut resp_headers = HeaderMap::new();
    if let Some(ct) = resp.headers().get(header::CONTENT_TYPE) {
        resp_headers.insert(header::CONTENT_TYPE, ct.clone());
    }
    if let Some(cl) = resp.headers().get(header::CONTENT_LENGTH) {
        resp_headers.insert(header::CONTENT_LENGTH, cl.clone());
    }

    let body = axum::body::Body::from_stream(resp.bytes_stream());
    Ok((resp_headers, body).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
