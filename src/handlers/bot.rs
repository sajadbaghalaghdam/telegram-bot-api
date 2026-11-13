use axum::{
    extract::{Path, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use crate::error::TelegramResult;
use crate::server::AppState;

/// `getMe` fast path: validates the token (401/421) and serves a short-lived cached response.
pub async fn get_me(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    state.engine.check_token_filter(clean_token)?;
    let session = state.client_manager.get_or_create(clean_token).await?;
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        session.me_json_bytes.clone(),
    )
        .into_response())
}

/// Delegates to the universal call path so close/logOut share validation, upstream relay and cleanup.
async fn lifecycle_call(state: &AppState, token: &str, method: &str) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(token);
    let out = state
        .engine
        .universal_call(
            clean_token,
            method,
            &std::collections::HashMap::new(),
            None,
            None,
            bytes::Bytes::new(),
        )
        .await?;
    let failure_code = serde_json::from_slice::<serde_json::Value>(&out)
        .ok()
        .filter(|j| j.get("ok").and_then(|v| v.as_bool()) == Some(false))
        .and_then(|j| j.get("error_code").and_then(|v| v.as_u64()));
    if failure_code.is_none() {
        state.client_manager.close(clean_token);
    }
    let status = failure_code
        .and_then(|c| StatusCode::from_u16(c as u16).ok())
        .unwrap_or(StatusCode::OK);
    Ok((status, [(header::CONTENT_TYPE, "application/json")], out).into_response())
}

pub async fn close(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Response> {
    lifecycle_call(&state, &token, "close").await
}

pub async fn log_out(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Response> {
    lifecycle_call(&state, &token, "logOut").await
}
