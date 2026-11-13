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
