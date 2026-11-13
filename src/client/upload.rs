//! Uploads: MTProto large-file glue, remote-URL -> MTProto, streaming multipart parts, media attributes.

use std::path::Path;

use bytes::Bytes;
use reqwest::multipart::Part;
use serde_json::Value;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use super::engine::{TelegramEngine, STREAM_BUFFER_SIZE};
use super::util::TempFile;
use crate::error::{TelegramError, TelegramResult};
use crate::models::File;
use crate::mtproto::{MediaAttrs, MediaKind};

/// One large-file upload request (send* method with a local file > 49 MB).
pub struct LargeUpload<'a> {
    pub token: &'a str,
    pub method: &'a str,
    pub chat_id: &'a str,
    pub path: &'a Path,
    pub caption: &'a str,
    /// File name to report instead of the path's own (set for temp files).
    pub file_name: Option<&'a str>,
    /// The other request parameters, echoed into the answer and read for media attributes.
    pub extra_fields: Option<&'a serde_json::Map<String, Value>>,
}

impl TelegramEngine {
    /// Uploads a large local file (> 50 MB up to 2000 MB) directly to Telegram DC via Pure-Rust MTProto 2.0
    /// (`upload.saveBigFilePart` + `messages.sendMedia`), returning a Bot API `Message` JSON payload.
    pub async fn upload_local_file_mtproto(&self, up: LargeUpload<'_>) -> TelegramResult<Bytes> {
        let LargeUpload {
            token,
            method,
            chat_id: chat_id_str,
            path: local_path,
            caption,
            file_name: custom_file_name,
            extra_fields,
        } = up;
        if self.api_id <= 0 || self.api_hash.is_empty() {
            return Err(TelegramError::Internal(
                "MTProto large file upload requires valid --api-id and --api-hash".to_string(),
            ));
        }

        // 1. Fetch bot identity and target chat metadata (including username hint for peer resolution)
        let bot_user = self.get_me_or_stale(token).await;
        let chat_url = format!(
            "{}/bot{token}/getChat?chat_id={}",
            self.upstream_base_url, chat_id_str
        );
        let mut chat_json = serde_json::json!({
            "id": chat_id_str.parse::<i64>().unwrap_or(0),
            "type": "private"
        });
        let mut hint_username: Option<String> = None;

        if let Ok(resp) = self.client.get(&chat_url).send().await
            && let Ok(val) = resp.json::<Value>().await
                && let Some(res_obj) = val.get("result") {
                    chat_json = res_obj.clone();
                    if let Some(u) = res_obj.get("username").and_then(|v| v.as_str()) {
                        hint_username = Some(u.to_string());
                    }
                }

        // 2. Execute Pure-Rust MTProto 2.0 parallel 512 KB chunked upload + messages.sendMedia
        let uploaded = crate::mtproto::upload_and_send_large_file_typed(
            self.api_id,
            &self.api_hash,
            token,
            chat_id_str,
            hint_username.as_deref(),
            local_path,
            caption,
            custom_file_name,
            media_attrs_for(method, extra_fields),
        )
        .await
        .map_err(|e| TelegramError::BadRequest(format!("MTProto upload failed: {e}")))?;

        // 3. Register uploaded file in local SQLite WAL storage for getFile lookups
        let file_id_str = format!(
            "BQACAgIAAxkBAAI{:x}_{}",
            uploaded.doc_id as u64, uploaded.message_id
        );
        let file_unique_id_str = format!("AgAD{:x}", (uploaded.doc_id as u64) & 0xffffffff);
        let local_path_str = local_path.to_string_lossy().to_string();

        self.storage
            .save_cached_file(&File {
                file_id: file_id_str.clone(),
                file_unique_id: file_unique_id_str.clone(),
                file_size: Some(uploaded.file_size),
                file_path: Some(local_path_str),
            })
            .await;

        // 4. Build standard Bot API Message JSON response matching requested method
        let media_field = match media_attrs_for(method, None).kind {
            MediaKind::Video => "video",
            MediaKind::VideoNote => "video_note",
            MediaKind::Animation => "animation",
            MediaKind::Audio => "audio",
            MediaKind::Voice => "voice",
            MediaKind::Document => {
                if method.to_ascii_lowercase().contains("photo") { "photo" } else { "document" }
            }
        };

        let mut msg_obj = serde_json::json!({
            "message_id": uploaded.message_id,
            "chat": chat_json,
            "date": uploaded.date,
            media_field: {
                "file_name": uploaded.file_name,
                "mime_type": uploaded.mime_type,
                "file_id": file_id_str,
                "file_unique_id": file_unique_id_str,
                "file_size": uploaded.file_size
            }
        });

        if let Some(u) = bot_user
            && let Ok(u_val) = serde_json::to_value(u) {
                msg_obj["from"] = u_val;
            }
        if !caption.is_empty() {
            msg_obj["caption"] = Value::String(caption.to_string());
        }
        if let Some(extras) = extra_fields {
            for (k, v) in extras.iter() {
                if k != "chat_id" && k != "caption" && !k.starts_with("file") {
                    msg_obj[k] = v.clone();
                }
            }
        }

        let full_resp = serde_json::json!({
            "ok": true,
            "result": msg_obj
        });

        Ok(Bytes::from(serde_json::to_vec(&full_resp).unwrap_or_default()))
    }

    /// In `--local` mode, if a remote HTTP/HTTPS URL points to a file > 20 MB (which standard
    /// Bot API HTTP URL fetching rejects), stream-downloads it in O(1) memory to a temporary file
    /// and uploads it via the Pure-Rust MTProto 2.0 512 KB chunked pipeline.
    pub async fn maybe_upload_large_remote_url_mtproto(
        &self,
        token: &str,
        method: &str,
        chat_id_str: &str,
        remote_url: &str,
        caption: &str,
    ) -> Option<TelegramResult<Bytes>> {
        if !self.local_mode
            || (!remote_url.starts_with("http://") && !remote_url.starts_with("https://"))
        {
            return None;
        }

        let head_resp = self.client.head(remote_url).send().await.ok()?;
        let content_len = head_resp.content_length().unwrap_or(0);
        if content_len <= 20 * 1024 * 1024 {
            return None;
        }

        let url_file_name = remote_url
            .split('?')
            .next()
            .unwrap_or(remote_url)
            .rsplit('/')
            .next()
            .filter(|s| !s.is_empty())
            .unwrap_or("remote_file.bin")
            .to_string();

        let temp_dir = self.dir.join("temp");
        let _ = fs::create_dir_all(&temp_dir).await;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        // Removed on every exit path, including a failed download and cancellation.
        let temp = TempFile::new(temp_dir.join(format!("url_{}_{}", nanos, url_file_name)));

        let mut get_resp = match self.client.get(remote_url).send().await {
            Ok(r) if r.status().is_success() => r,
            _ => return None,
        };

        let out_file = match fs::File::create(temp.path()).await {
            Ok(f) => f,
            Err(e) => {
                return Some(Err(TelegramError::Internal(format!(
                    "Failed to create temp file for remote URL: {e}"
                ))))
            }
        };
        let mut writer = tokio::io::BufWriter::with_capacity(STREAM_BUFFER_SIZE, out_file);
        loop {
            match get_resp.chunk().await {
                Ok(Some(chunk)) => {
                    if let Err(e) = writer.write_all(&chunk).await {
                        return Some(Err(TelegramError::Internal(format!(
                            "Failed writing remote URL stream: {e}"
                        ))));
                    }
                }
                Ok(None) => break,
                // A broken download must not be uploaded as if it were the whole file.
                Err(e) => {
                    return Some(Err(TelegramError::Internal(format!(
                        "Failed reading remote URL stream: {e}"
                    ))));
                }
            }
        }
        if let Err(e) = writer.flush().await {
            return Some(Err(TelegramError::Internal(format!("Failed writing remote URL stream: {e}"))));
        }
        drop(writer);

        let res = self
            .upload_local_file_mtproto(LargeUpload {
                token,
                method,
                chat_id: chat_id_str,
                path: temp.path(),
                caption,
                file_name: Some(&url_file_name),
                extra_fields: None,
            })
            .await;
        Some(res)
    }

    /// Build a zero-copy 64KB chunked streaming Multipart Part from a local file without loading into RAM
    pub async fn build_streaming_part(
        local_path: &Path,
        default_name: &'static str,
    ) -> TelegramResult<Part> {
        let file = fs::File::open(local_path)
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to open local file: {e}")))?;
        let file_len = file
            .metadata()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to stat local file: {e}")))?
            .len();

        let file_name = local_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or(default_name)
            .to_string();

        let mime = mime_guess::from_path(local_path)
            .first_or_octet_stream()
            .as_ref()
            .to_string();

        let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
        let body = reqwest::Body::wrap_stream(stream);
        let part = Part::stream_with_length(body, file_len)
            .file_name(file_name)
            .mime_str(&mime)
            .map_err(|e| TelegramError::Internal(format!("Invalid MIME type: {e}")))?;

        Ok(part)
    }
}

/// Maps a send* method to its upload media kind and reads duration/width/height from request fields.
pub fn media_attrs_for(
    method: &str,
    fields: Option<&serde_json::Map<String, Value>>,
) -> MediaAttrs {
    let kind = match method.to_ascii_lowercase().as_str() {
        "sendvideo" => MediaKind::Video,
        "sendvideonote" => MediaKind::VideoNote,
        "sendanimation" => MediaKind::Animation,
        "sendaudio" => MediaKind::Audio,
        "sendvoice" => MediaKind::Voice,
        _ => MediaKind::Document,
    };
    let num = |k: &str| -> i32 {
        fields
            .and_then(|m| m.get(k))
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .and_then(|n| i32::try_from(n).ok())
            .unwrap_or(0)
    };
    let (width, height) = if kind == MediaKind::VideoNote {
        let l = num("length");
        (l, l)
    } else {
        (num("width"), num("height"))
    };
    // C++ to_bool: "true" / "yes" / "1" (case-insensitive, trimmed); JSON booleans are accepted too.
    let supports_streaming = fields
        .and_then(|m| m.get("supports_streaming"))
        .map(|v| match v {
            Value::Bool(b) => *b,
            Value::String(s) => matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "yes" | "1"),
            Value::Number(n) => n.as_i64() == Some(1),
            _ => false,
        })
        .unwrap_or(false);
    MediaAttrs { kind, duration: num("duration"), width, height, supports_streaming }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attrs_from_fields() {
        let m: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"duration":"7","width":640,"height":480}"#).unwrap();
        let a = media_attrs_for("sendVideo", Some(&m));
        assert_eq!((a.kind, a.duration, a.width, a.height), (MediaKind::Video, 7, 640, 480));
        assert!(!a.supports_streaming);
        for (raw, want) in [("true", true), ("\"Yes\"", true), ("\"1\"", true), ("\"false\"", false), ("false", false), ("\"0\"", false)] {
            let m: serde_json::Map<String, Value> =
                serde_json::from_str(&format!("{{\"supports_streaming\":{raw}}}")).unwrap();
            assert_eq!(media_attrs_for("sendVideo", Some(&m)).supports_streaming, want, "{raw}");
        }
        assert_eq!(media_attrs_for("sendDocument", Some(&m)).kind, MediaKind::Document);
        assert_eq!(media_attrs_for("sendVoice", None).kind, MediaKind::Voice);
        assert_eq!(media_attrs_for("sendVideoNote", None).kind, MediaKind::VideoNote);
    }
}
