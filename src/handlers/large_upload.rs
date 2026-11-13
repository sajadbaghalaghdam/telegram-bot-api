//! Large multipart uploads (> 49 MB, up to 2000 MB): the media part is spooled to a temp file in
//! O(1) memory and uploaded through MTProto (`upload.saveBigFilePart` + `messages.sendMedia`).

use std::collections::HashMap;

use axum::{
    extract::{FromRequest, Multipart, Request},
    http::{header, HeaderMap, StatusCode},
    response::Response,
};
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt;

use super::generic::json_response;
use crate::client::upload::LargeUpload;
use crate::client::util::TempFile;
use crate::error::{TelegramError, TelegramResult};
use crate::server::SharedState;

/// Bodies above this size take the MTProto path.
const THRESHOLD: u64 = 49 * 1024 * 1024;

/// Whether a request is a multipart upload large enough for the MTProto path.
pub fn applies(content_type: Option<&str>, headers: &HeaderMap) -> bool {
    content_type.is_some_and(|c| c.starts_with("multipart/form-data"))
        && headers
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
            > THRESHOLD
}

/// The multipart field carrying the media of a send method (None: unknown method, the first file
/// part is used).
fn media_param(method: &str) -> Option<&'static str> {
    let mut buf = [0u8; 32];
    match crate::client::util::ascii_lower(method, &mut buf) {
        "sendphoto" => Some("photo"),
        "sendvideo" => Some("video"),
        "senddocument" => Some("document"),
        "sendaudio" => Some("audio"),
        "sendvoice" => Some("voice"),
        "sendanimation" => Some("animation"),
        "sendvideonote" => Some("video_note"),
        _ => None,
    }
}

fn is_media_field(media_param: Option<&str>, field_name: &str) -> bool {
    media_param.is_none_or(|m| m == field_name)
}

/// What the multipart body carried besides the media file.
struct Received {
    chat_id: String,
    caption: String,
    file_name: Option<String>,
    extra_fields: Map<String, Value>,
}

/// Receives the multipart stream: the media part goes to `temp`, text fields are collected.
/// Returns `None` when the body had no file part.
async fn receive(
    mut multipart: Multipart,
    temp: &TempFile,
    media_param: Option<&str>,
    query: &HashMap<String, String>,
) -> TelegramResult<Option<Received>> {
    let mut got = Received {
        chat_id: query.get("chat_id").cloned().unwrap_or_default(),
        caption: query.get("caption").cloned().unwrap_or_default(),
        file_name: None,
        extra_fields: Map::new(),
    };
    let mut has_file = false;

    while let Some(mut field) = multipart
        .next_field()
        .await
        .map_err(|e| TelegramError::BadRequest(format!("Multipart field error: {e}")))?
    {
        let name = field.name().unwrap_or("").to_string();
        if let Some(file_name) = field.file_name().map(str::to_string) {
            // Only the method's media field (e.g. `video`) is streamed; other file parts
            // (`thumbnail`, ...) are skipped so they cannot overwrite the media.
            if has_file || !is_media_field(media_param, &name) {
                continue;
            }
            got.file_name = Some(file_name);
            let mut f = tokio::fs::File::create(temp.path())
                .await
                .map_err(|e| TelegramError::Internal(format!("Temp file create error: {e}")))?;
            has_file = true;
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
        } else if name == "chat_id" {
            if let Ok(txt) = field.text().await {
                got.chat_id = txt;
            }
        } else if name == "caption" {
            if let Ok(txt) = field.text().await {
                got.caption = txt;
            }
        } else if !name.is_empty()
            && let Ok(txt) = field.text().await
        {
            let val = serde_json::from_str::<Value>(&txt).unwrap_or(Value::String(txt));
            got.extra_fields.insert(name, val);
        }
    }
    Ok(has_file.then_some(got))
}

/// Spools the upload to a temp file and sends it through MTProto. The temp file is removed on every
/// exit path, including errors while receiving and the client disconnecting mid-upload.
pub async fn relay(
    state: &SharedState,
    token: &str,
    method: &str,
    query: &HashMap<String, String>,
    req: Request,
) -> TelegramResult<Response> {
    let multipart = Multipart::from_request(req, state)
        .await
        .map_err(|e| TelegramError::BadRequest(format!("Invalid multipart stream: {e}")))?;

    let _ = tokio::fs::create_dir_all(&state.engine.temp_dir).await;
    let temp = TempFile::new(
        state
            .engine
            .temp_dir
            .join(format!("mtproto_upload_{}.tmp", rand::random::<u64>())),
    );

    let Some(got) = receive(multipart, &temp, media_param(method), query).await? else {
        return Err(TelegramError::BadRequest(
            "No file part found in large multipart upload".to_string(),
        ));
    };

    let out = state
        .engine
        .upload_local_file_mtproto(LargeUpload {
            token,
            method,
            chat_id: &got.chat_id,
            path: temp.path(),
            caption: &got.caption,
            file_name: got.file_name.as_deref(),
            extra_fields: Some(&got.extra_fields),
        })
        .await?;
    Ok(json_response(StatusCode::OK, out))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_param_per_method() {
        assert_eq!(media_param("sendVideo"), Some("video"));
        assert_eq!(media_param("sendvideonote"), Some("video_note"));
        assert_eq!(media_param("sendDocument"), Some("document"));
        assert_eq!(media_param("sendMessage"), None);
    }

    #[test]
    fn only_the_media_field_is_streamed() {
        let p = media_param("sendVideo");
        assert!(is_media_field(p, "video"));
        assert!(!is_media_field(p, "thumbnail"));
        assert!(!is_media_field(p, "cover"));
        // unknown method: any file part qualifies (the first one wins)
        assert!(is_media_field(None, "file"));
    }

    #[test]
    fn only_large_multipart_applies() {
        let mut h = HeaderMap::new();
        h.insert(header::CONTENT_LENGTH, "60000000".parse().unwrap());
        assert!(applies(Some("multipart/form-data; boundary=x"), &h));
        assert!(!applies(Some("application/json"), &h));
        assert!(!applies(None, &h));
        h.insert(header::CONTENT_LENGTH, "1000".parse().unwrap());
        assert!(!applies(Some("multipart/form-data; boundary=x"), &h));
        assert!(!applies(Some("multipart/form-data"), &HeaderMap::new()));
    }
}
