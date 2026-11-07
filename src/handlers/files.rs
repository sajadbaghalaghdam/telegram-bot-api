use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue},
    response::{IntoResponse, Response},
    Json,
};
use crate::error::TelegramResult;
use crate::models::{
    ApiResponse, File, GetFileRequest, Message, SendAnimationRequest, SendAudioRequest,
    SendDocumentRequest, SendPhotoRequest, SendVideoRequest, SendVoiceRequest,
};
use crate::server::AppState;

pub async fn send_photo(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SendPhotoRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let msg = state.engine.send_photo(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

pub async fn send_document(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SendDocumentRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let msg = state.engine.send_document(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

pub async fn send_audio(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SendAudioRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let msg = state.engine.send_audio(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

pub async fn send_video(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SendVideoRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let msg = state.engine.send_video(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

pub async fn send_voice(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SendVoiceRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let msg = state.engine.send_voice(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

pub async fn send_animation(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<SendAnimationRequest>,
) -> TelegramResult<Json<ApiResponse<Message>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let msg = state.engine.send_animation(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(msg)))
}

pub async fn get_file(
    Path(token): Path<String>,
    State(state): State<AppState>,
    Json(payload): Json<GetFileRequest>,
) -> TelegramResult<Json<ApiResponse<File>>> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let file = state.engine.get_file(clean_token, &payload).await?;
    Ok(Json(ApiResponse::success(file)))
}

/// Serve file content with O(1) memory streaming: GET /file/bot{token}/{*file_path}
pub async fn serve_file(
    Path((token, file_path)): Path<(String, String)>,
    State(state): State<AppState>,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let (body, content_len) = state
        .engine
        .open_file_stream(clean_token, &file_path)
        .await?;

    let content_type = mime_guess::from_path(&file_path)
        .first_or_octet_stream()
        .as_ref()
        .to_string();

    let mut headers = HeaderMap::new();
    if let Ok(ct) = HeaderValue::from_str(&content_type) {
        headers.insert(header::CONTENT_TYPE, ct);
    }
    if let Some(len) = content_len {
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    }

    Ok((headers, body).into_response())
}
