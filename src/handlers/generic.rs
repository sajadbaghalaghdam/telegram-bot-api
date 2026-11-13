use std::collections::HashMap;

use axum::{
    body::Body,
    extract::{Query, RawPathParams, Request, State},
    http::{header, HeaderValue, StatusCode, Uri},
    response::Response,
};

use super::large_upload;
use crate::client::relay::RelayCall;
use crate::error::{TelegramError, TelegramResult};
use crate::server::SharedState;

/// `(token, method)` from the route captures, borrowed (no allocation). The token still carries an
/// optional `bot` prefix.
pub(super) fn token_and_method(params: &RawPathParams) -> (&str, &str) {
    let (mut token, mut method) = ("", "");
    for (name, value) in params {
        match name {
            "token" => token = value,
            "method" => method = value,
            _ => {}
        }
    }
    (token, method)
}

/// Response with the given status and a JSON body.
pub(super) fn json_response(status: StatusCode, body: impl Into<Body>) -> Response {
    let mut resp = Response::new(body.into());
    *resp.status_mut() = status;
    resp.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
    resp
}

/// Decoded query parameters (empty, without allocating, when there is no query string).
fn query_map(uri: &Uri) -> TelegramResult<HashMap<String, String>> {
    if uri.query().is_none_or(str::is_empty) {
        return Ok(HashMap::new());
    }
    Query::<HashMap<String, String>>::try_from_uri(uri)
        .map(|q| q.0)
        .map_err(|e| TelegramError::BadRequest(format!("Failed to deserialize query string: {e}")))
}

/// Handles any official Telegram Bot API POST or GET method dynamically,
/// supporting JSON (with local file path auto-streaming), large Multipart (>50MB up to 2GB via MTProto 2.0),
/// Form-urlencoded, and Query params.
pub async fn relay_handler(
    State(state): State<SharedState>,
    params: RawPathParams,
    req: Request,
) -> TelegramResult<Response> {
    let (token, method) = token_and_method(&params);
    let token = token.strip_prefix("bot").unwrap_or(token);
    state.engine.check_token_filter(token)?;

    if method.eq_ignore_ascii_case("getme") {
        return super::bot::me_response(&state, token).await;
    }

    let (parts, body) = req.into_parts();
    let query_map = query_map(&parts.uri)?;
    let content_type = parts.headers.get(header::CONTENT_TYPE).and_then(|v| v.to_str().ok());

    if large_upload::applies(content_type, &parts.headers) {
        let req = Request::from_parts(parts, body);
        return large_upload::relay(&state, token, method, &query_map, req).await;
    }

    let body = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| TelegramError::BadRequest(format!("Failed to read request body: {e}")))?;

    let relayed = state
        .engine
        .relay(RelayCall {
            token,
            method,
            query_map: &query_map,
            query: parts.uri.query(),
            content_type,
            body,
        })
        .await?;

    let (status, retry_after) = match relayed.head.failure() {
        Some((code, retry_after)) => (StatusCode::from_u16(code).unwrap_or(StatusCode::BAD_REQUEST), retry_after),
        None => (StatusCode::OK, None),
    };

    if status == StatusCode::OK && (method.eq_ignore_ascii_case("close") || method.eq_ignore_ascii_case("logout")) {
        state.client_manager.close(token);
    }

    let mut resp = json_response(status, relayed.body);
    if let Some(ra) = retry_after
        && let Ok(val) = HeaderValue::try_from(ra.to_string())
    {
        resp.headers_mut().insert(header::RETRY_AFTER, val);
    }
    Ok(resp)
}

/// Fallback handler returning official Telegram Bot API 404 JSON for unknown routes/methods
pub async fn fallback_404_handler() -> Response {
    json_response(
        StatusCode::NOT_FOUND,
        r#"{"ok":false,"error_code":404,"description":"Not Found: method not found"}"#,
    )
}
