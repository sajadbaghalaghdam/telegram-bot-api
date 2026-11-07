use axum::{
    extract::{Path, State},
    http::header,
    response::{IntoResponse, Response},
    Json,
};
use crate::error::TelegramResult;
use crate::models::ApiResponse;
use crate::server::AppState;

/// Zero-allocation fast path for `getMe` using pre-serialized Arc-backed `Bytes`
pub async fn get_me(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let session = state.client_manager.get_or_create(clean_token).await?;
    Ok((
        [(header::CONTENT_TYPE, "application/json")],
        session.me_json_bytes.clone(),
    )
        .into_response())
}

pub async fn close(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Json<ApiResponse<bool>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    state.client_manager.close(clean_token);
    Ok(Json(ApiResponse::success(true)))
}

pub async fn log_out(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Json<ApiResponse<bool>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    state.client_manager.close(clean_token);
    Ok(Json(ApiResponse::success(true)))
}
