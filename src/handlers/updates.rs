use axum::{
    extract::{Path, State},
    Json,
};
use crate::error::TelegramResult;
use crate::models::{
    ApiResponse, GetUpdatesRequest, SetWebhookRequest, Update, WebhookInfo,
};
use crate::server::AppState;

pub async fn get_updates(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<GetUpdatesRequest>,
) -> TelegramResult<Json<ApiResponse<Vec<Update>>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let updates = state.engine.get_updates(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(updates)))
}

pub async fn set_webhook(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SetWebhookRequest>,
) -> TelegramResult<Json<ApiResponse<bool>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let success = state.engine.set_webhook(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(success)))
}

pub async fn delete_webhook(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Json<ApiResponse<bool>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let success = state.engine.delete_webhook(clean_token).await?;
    Ok(Json(ApiResponse::success(success)))
}

pub async fn get_webhook_info(
    Path(token): Path<String>,
    State(state): State<AppState>,
) -> TelegramResult<Json<ApiResponse<WebhookInfo>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let info = state.engine.get_webhook_info(clean_token).await?;
    Ok(Json(ApiResponse::success(info)))
}
