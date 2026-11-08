use axum::{
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use tokio::fs::File as TokioFile;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;
use crate::error::{TelegramError, TelegramResult};
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

/// Serve file content with O(1) memory streaming and HTTP Range support (matching C++ HttpFileConnection)
pub async fn serve_file(
    req_headers: HeaderMap,
    Path((token, file_path)): Path<(String, String)>,
    State(state): State<AppState>,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let content_type = mime_guess::from_path(&file_path)
        .first_or_octet_stream()
        .as_ref()
        .to_string();

    let mut resp_headers = HeaderMap::new();
    if let Ok(ct) = HeaderValue::from_str(&content_type) {
        resp_headers.insert(header::CONTENT_TYPE, ct);
    }
    resp_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    // Enforce 20MB download limit in non-local mode matching C++ Client.cpp:17042
    if !state.config.local_mode {
        if let Some(local_path) = state.engine.resolve_file_on_disk(clean_token, &file_path) {
            if let Ok(meta) = tokio::fs::metadata(&local_path).await {
                if meta.len() > 20 * 1024 * 1024 {
                    return Err(TelegramError::BadRequest("Bad Request: file is too big".to_string()));
                }
            }
        }
    }

    // 1. If file is on local disk in local mode, handle Range requests directly
    if let Some(local_path) = state.engine.resolve_file_on_disk(clean_token, &file_path) {
        if let Ok(mut f) = TokioFile::open(&local_path).await {
            if let Ok(meta) = f.metadata().await {
                let total_size = meta.len();

                if let Some(range_header) = req_headers.get(header::RANGE) {
                    if let Ok(range_str) = range_header.to_str() {
                        if let Some(spec) = range_str.strip_prefix("bytes=") {
                            let parts: Vec<&str> = spec.split('-').collect();
                            if parts.len() == 2 {
                                let p0 = parts[0].trim();
                                let p1 = parts[1].trim();

                                let (start, end) = if p0.is_empty() {
                                    // Suffix range: bytes=-500 (last 500 bytes)
                                    let suffix: u64 = p1.parse().unwrap_or(0);
                                    let s = total_size.saturating_sub(suffix);
                                    (s, total_size.saturating_sub(1))
                                } else if p1.is_empty() {
                                    // Prefix range: bytes=500- (from 500 to end)
                                    let s: u64 = p0.parse().unwrap_or(0);
                                    (s, total_size.saturating_sub(1))
                                } else {
                                    // Explicit range: bytes=start-end
                                    let s: u64 = p0.parse().unwrap_or(0);
                                    let e: u64 = p1.parse().unwrap_or(total_size.saturating_sub(1));
                                    (s, e.min(total_size.saturating_sub(1)))
                                };

                                if start > end || start >= total_size {
                                    // RFC 7233: 416 Range Not Satisfiable
                                    let mut err_headers = HeaderMap::new();
                                    err_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                                    if let Ok(cr) = HeaderValue::from_str(&format!("bytes */{}", total_size)) {
                                        err_headers.insert(header::CONTENT_RANGE, cr);
                                    }
                                    return Ok((StatusCode::RANGE_NOT_SATISFIABLE, err_headers, "").into_response());
                                }

                                let length = end - start + 1;
                                if f.seek(std::io::SeekFrom::Start(start)).await.is_ok() {
                                    if let Ok(cr) = HeaderValue::from_str(&format!(
                                        "bytes {}-{}/{}",
                                        start, end, total_size
                                    )) {
                                        resp_headers.insert(header::CONTENT_RANGE, cr);
                                    }
                                    resp_headers.insert(
                                        header::CONTENT_LENGTH,
                                        HeaderValue::from(length),
                                    );
                                    let stream =
                                        ReaderStream::with_capacity(f.take(length), 64 * 1024);
                                    let body = axum::body::Body::from_stream(stream);
                                    return Ok((
                                        StatusCode::PARTIAL_CONTENT,
                                        resp_headers,
                                        body,
                                    )
                                        .into_response());
                                }
                            }
                        }
                    }
                }

                resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(total_size));
                let stream = ReaderStream::with_capacity(f, 64 * 1024);
                let body = axum::body::Body::from_stream(stream);
                return Ok((resp_headers, body).into_response());
            }
        }
    }

    // 2. Fall back to upstream file stream
    let (body, content_len) = state
        .engine
        .open_file_stream(clean_token, &file_path)
        .await?;

    if !state.config.local_mode {
        if let Some(len) = content_len {
            if len > 20 * 1024 * 1024 {
                return Err(TelegramError::BadRequest("Bad Request: file is too big".to_string()));
            }
        }
    }

    if let Some(len) = content_len {
        resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    }

    Ok((resp_headers, body).into_response())
}

/// Serve file content for Test DC environment: GET /test/file/bot{token}/{*file_path}
pub async fn serve_test_file(
    _req_headers: HeaderMap,
    Path((token, file_path)): Path<(String, String)>,
    State(state): State<AppState>,
) -> TelegramResult<Response> {
    let clean_token = token.strip_prefix("bot").unwrap_or(&token);
    let dl_url = format!("https://api.telegram.org/test/file/bot{clean_token}/{file_path}");
    let resp = state
        .engine
        .client
        .get(&dl_url)
        .send()
        .await
        .map_err(|e| crate::error::TelegramError::Internal(format!("File download network error: {e}")))?;

    if !resp.status().is_success() {
        return Err(crate::error::TelegramError::BadRequest(format!(
            "File download failed: status {}",
            resp.status()
        )));
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
