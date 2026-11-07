use axum::{
    extract::{Path, State},
    Json,
};
use crate::error::{TelegramError, TelegramResult};
use crate::models::{
    ApiResponse, DeleteMessageRequest, EditMessageTextRequest, Message, SendMessageRequest,
};
use crate::server::AppState;

/// Handler for sendMessage with all official parameters
pub async fn send_message(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SendMessageRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    
    if payload.text.trim().is_empty() {
        return Err(TelegramError::BadRequest("Text must not be empty".to_string()));
    }

    let msg = state.engine.send_message(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

/// Handler for editMessageText
pub async fn edit_message_text(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<EditMessageTextRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    
    if payload.text.trim().is_empty() {
        return Err(TelegramError::BadRequest("Text must not be empty".to_string()));
    }

    let msg = state.engine.edit_message_text(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

/// Handler for deleteMessage
pub async fn delete_message(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<DeleteMessageRequest>,
) -> TelegramResult<Json<ApiResponse<bool>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let success = state.engine.delete_message(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(success)))
}
