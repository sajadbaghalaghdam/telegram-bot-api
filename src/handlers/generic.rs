use std::collections::HashMap;
use axum::{
    body::Bytes,
    extract::{Path, Query, RawQuery, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use crate::error::TelegramResult;
use crate::server::AppState;

/// Handles any official Telegram Bot API POST or GET method dynamically,
/// supporting JSON (with local file path auto-streaming), Multipart, Form-urlencoded, and Query params.
pub async fn generic_post_handler(
    Path((token, method)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);

    if method.eq_ignore_ascii_case("getme") {
        let session = state.client_manager.get_or_create(clean_token).await?;
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            session.me_json_bytes.clone(),
        )
            .into_response());
    }

    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    let q_str = raw_query.as_deref();

    let out = state
        .engine
        .universal_call(clean_token, &method, &query_map, q_str, ct, body)
        .await?;

    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        out,
    )
        .into_response())
}

/// Handles any official Telegram Bot API GET method dynamically
pub async fn generic_get_handler(
    Path((token, method)): Path<(String, String)>,
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
) -> TelegramResult<Response> {
    generic_post_handler(
        Path((token, method)),
        State(state),
        headers,
        Query(query_map),
        RawQuery(raw_query),
        Bytes::new(),
    )
    .await
}
