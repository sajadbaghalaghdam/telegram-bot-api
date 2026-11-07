use std::collections::HashMap;
use axum::{
    body::Bytes,
    extract::{FromRequest, Multipart, Path, Query, RawQuery, Request, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use tokio::io::AsyncWriteExt;
use crate::error::{TelegramError, TelegramResult};
use crate::server::AppState;

/// Handles any official Telegram Bot API POST or GET method dynamically,
/// supporting JSON (with local file path auto-streaming), large Multipart (>50MB up to 2GB via MTProto 2.0),
/// Form-urlencoded, and Query params.
pub async fn generic_post_handler(
    Path((token, method)): Path<(String, String)>,
    State(state): State<AppState>,
    Query(query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
    req: Request,
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

    let ct = req
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let content_len = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    // If a large (> 49 MB up to 2000 MB) multipart/form-data upload arrives, stream it directly to a temp file
    // in O(1) memory and upload it via Pure-Rust MTProto 2.0 (upload.saveBigFilePart + messages.sendMedia)
    if ct
        .as_deref()
        .map(|c| c.starts_with("multipart/form-data"))
        .unwrap_or(false)
        && content_len > 49 * 1024 * 1024
    {
        let mut multipart = Multipart::from_request(req, &state)
            .await
            .map_err(|e| TelegramError::BadRequest(format!("Invalid multipart stream: {e}")))?;

        let _ = tokio::fs::create_dir_all(&state.engine.temp_dir).await;
        let temp_path = state
            .engine
            .temp_dir
            .join(format!("mtproto_upload_{}.tmp", rand::random::<u64>()));

        let mut chat_id_str = query_map.get("chat_id").cloned().unwrap_or_default();
        let mut caption = query_map.get("caption").cloned().unwrap_or_default();
        let mut orig_file_name: Option<String> = None;
        let mut has_temp_file = false;

        while let Some(mut field) = multipart
            .next_field()
            .await
            .map_err(|e| TelegramError::BadRequest(format!("Multipart field error: {e}")))?
        {
            let name = field.name().unwrap_or("").to_string();
            if let Some(fname) = field.file_name() {
                orig_file_name = Some(fname.to_string());
                let mut f = tokio::fs::File::create(&temp_path)
                    .await
                    .map_err(|e| TelegramError::Internal(format!("Temp file create error: {e}")))?;
                while let Some(chunk) = field
                    .chunk()
                    .await
                    .map_err(|e| TelegramError::BadRequest(format!("Multipart chunk error: {e}")))?
                {
                    f.write_all(&chunk)
                        .await
                        .map_err(|e| TelegramError::Internal(format!("Temp write error: {e}")))?;
                }
                f.flush()
                    .await
                    .map_err(|e| TelegramError::Internal(format!("Temp flush error: {e}")))?;
                has_temp_file = true;
            } else if name == "chat_id" {
                if let Ok(txt) = field.text().await {
                    chat_id_str = txt;
                }
            } else if name == "caption" {
                if let Ok(txt) = field.text().await {
                    caption = txt;
                }
            }
        }

        if has_temp_file {
            let res = state
                .engine
                .upload_local_file_mtproto(
                    clean_token,
                    &chat_id_str,
                    &temp_path,
                    &caption,
                    orig_file_name.as_deref(),
                )
                .await;
            let _ = tokio::fs::remove_file(&temp_path).await;
            let out = res?;
            return Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                out,
            )
                .into_response());
        }
        return Err(TelegramError::BadRequest(
            "No file part found in large multipart upload".to_string(),
        ));
    }

    let body = Bytes::from_request(req, &state)
        .await
        .map_err(|e| TelegramError::BadRequest(format!("Failed to read request body: {e}")))?;

    let q_str = raw_query.as_deref();
    let out = state
        .engine
        .universal_call(clean_token, &method, &query_map, q_str, ct.as_deref(), body)
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
    Query(query_map): Query<HashMap<String, String>>,
    RawQuery(raw_query): RawQuery,
    req: Request,
) -> TelegramResult<Response> {
    generic_post_handler(
        Path((token, method)),
        State(state),
        Query(query_map),
        RawQuery(raw_query),
        req,
    )
    .await
}
