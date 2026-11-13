use std::collections::HashMap;

use axum::{
    extract::{RawPathParams, State},
    http::StatusCode,
    response::Response,
};
use bytes::Bytes;

use super::generic::{json_response, token_and_method};
use crate::client::relay::RelayCall;
use crate::error::TelegramResult;
use crate::server::SharedState;

/// Serves the short-lived cached `getMe` answer of a (validated, `bot`-less) token.
pub(super) async fn me_response(state: &SharedState, token: &str) -> TelegramResult<Response> {
    let session = state.client_manager.get_or_create(token).await?;
    Ok(json_response(StatusCode::OK, session.me_json_bytes.clone()))
}

/// `getMe` fast path: validates the token (401/421) and serves a short-lived cached response.
pub async fn get_me(State(state): State<SharedState>, params: RawPathParams) -> TelegramResult<Response> {
    let (token, _) = token_and_method(&params);
    let token = token.strip_prefix("bot").unwrap_or(token);
    state.engine.check_token_filter(token)?;
    me_response(&state, token).await
}

/// Delegates to the universal call path so close/logOut share validation, upstream relay and cleanup.
async fn lifecycle_call(state: &SharedState, params: &RawPathParams, method: &str) -> TelegramResult<Response> {
    let (token, _) = token_and_method(params);
    let token = token.strip_prefix("bot").unwrap_or(token);
    let relayed = state
        .engine
        .relay(RelayCall {
            token,
            method,
            query_map: &HashMap::new(),
            query: None,
            content_type: None,
            body: Bytes::new(),
        })
        .await?;
    let failure_code = (relayed.head.ok == Some(false))
        .then(|| relayed.head.error_code.and_then(|c| u64::try_from(c).ok()))
        .flatten();
    if failure_code.is_none() {
        state.client_manager.close(token);
    }
    let status = failure_code
        .and_then(|c| StatusCode::from_u16(c as u16).ok())
        .unwrap_or(StatusCode::OK);
    Ok(json_response(status, relayed.body))
}

pub async fn close(State(state): State<SharedState>, params: RawPathParams) -> TelegramResult<Response> {
    lifecycle_call(&state, &params, "close").await
}

pub async fn log_out(State(state): State<SharedState>, params: RawPathParams) -> TelegramResult<Response> {
    lifecycle_call(&state, &params, "logOut").await
}

/// The local path a `getUploadProgress` call names: its `path` query parameter, else the `path`
/// of a JSON body; a `file://` prefix is dropped.
fn upload_path(query: &HashMap<String, String>, body: &[u8]) -> Option<String> {
    let path = query.get("path").cloned().or_else(|| {
        serde_json::from_slice::<serde_json::Value>(body)
            .ok()?
            .get("path")?
            .as_str()
            .map(str::to_owned)
    })?;
    Some(path.strip_prefix("file://").map(str::to_owned).unwrap_or(path))
}

/// `getUploadProgress` (an extension of the Bot API): how far the large-file upload of the local
/// file `path` is, while a `send*` request for it is open. 404 when no upload of it is running
/// (not started yet, or finished): a client polls and treats that as "no news".
pub async fn get_upload_progress(
    State(state): State<SharedState>,
    params: RawPathParams,
    axum::extract::Query(query): axum::extract::Query<HashMap<String, String>>,
    body: Bytes,
) -> TelegramResult<Response> {
    let (token, _) = token_and_method(&params);
    let token = token.strip_prefix("bot").unwrap_or(token);
    state.engine.check_token_filter(token)?;
    // A live bot session only: the token is checked like any other call's.
    state.client_manager.get_or_create(token).await?;
    let progress = upload_path(&query, &body).and_then(|path| crate::mtproto::upload_progress::snapshot(&path));
    Ok(match progress {
        Some((done, total)) => json_response(
            StatusCode::OK,
            serde_json::json!({"ok": true, "result": {"done_bytes": done, "total_bytes": total}}).to_string(),
        ),
        None => json_response(
            StatusCode::NOT_FOUND,
            r#"{"ok":false,"error_code":404,"description":"Not Found: no upload of this file is running"}"#,
        ),
    })
}

#[cfg(test)]
mod upload_progress_tests {
    use super::*;

    #[test]
    fn the_path_comes_from_the_query_or_the_body() {
        let query = HashMap::from([("path".to_owned(), "file:///a/b.mp4".to_owned())]);
        assert_eq!(upload_path(&query, b"").as_deref(), Some("/a/b.mp4"));
        assert_eq!(upload_path(&HashMap::new(), br#"{"path":"/a/c.mp4"}"#).as_deref(), Some("/a/c.mp4"));
        assert_eq!(upload_path(&HashMap::new(), b"nope"), None);
    }
}
