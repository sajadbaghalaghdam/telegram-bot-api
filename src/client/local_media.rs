//! `--local` mode: a send* request that names a file on the server's disk is turned into a
//! streaming multipart upload (or an MTProto upload above 49 MB) instead of being forwarded as is.
//! A remote URL pointing at a file > 20 MB is fetched and uploaded through MTProto as well.

use std::collections::HashMap;
use std::path::PathBuf;

use bytes::Bytes;
use reqwest::multipart::Form;
use serde_json::{Map, Value};
use tokio::fs;

use super::engine::TelegramEngine;
use super::relay::{Method, RelayCall};
use super::upload::LargeUpload;
use super::upstream::send_bytes;
use crate::error::TelegramResult;

/// Media field names in Telegram Bot API that may reference a local file path in `--local` mode
const MEDIA_FIELDS: &[&str] = &[
    "photo",
    "video",
    "document",
    "audio",
    "voice",
    "animation",
    "video_note",
    "sticker",
    "thumbnail",
    "certificate",
];

/// Files above this size cannot go through the HTTP Bot API upload and use MTProto instead.
const MTPROTO_UPLOAD_THRESHOLD: u64 = 49 * 1024 * 1024;

/// A multipart request to build: local files as streaming parts plus the remaining JSON fields as text.
struct Multipart {
    /// `(form field name, local path)`.
    files: Vec<(String, PathBuf)>,
    /// Name used for a part whose path has no file name of its own.
    default_name: &'static str,
    /// Replacement text of the `media` field (sendMediaGroup / editMessageMedia).
    media_json: Option<String>,
    /// JSON field that is not copied as text (the file field itself, or `media`).
    skip: String,
}

impl TelegramEngine {
    /// Rewrites a `--local` request that references files on disk. `None`: nothing to rewrite, the
    /// caller forwards the request unchanged.
    pub(super) async fn relay_local_media(
        &self,
        call: &RelayCall<'_>,
        method: Method,
        url: &str,
    ) -> Option<TelegramResult<Bytes>> {
        if let Some(res) = self.local_media_from_json(call, method, url).await {
            return Some(res);
        }
        self.local_media_from_query(call).await
    }

    async fn local_media_from_json(
        &self,
        call: &RelayCall<'_>,
        method: Method,
        url: &str,
    ) -> Option<TelegramResult<Bytes>> {
        let body = &call.body;
        let ct = call.content_type.unwrap_or("");
        // Cheap pre-checks: a JSON object that mentions a path separator, for a method that takes media.
        if body.is_empty()
            || method == Method::NoLocalMedia
            || !(ct.starts_with("application/json") || body.starts_with(b"{"))
            || !(body.contains(&b'/') || body.contains(&b'\\'))
        {
            return None;
        }
        let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(body) else {
            return None;
        };

        let plan = match method {
            Method::SendMediaGroup => self.plan_media_group(&map).await,
            Method::EditMessageMedia => self.plan_edit_media(&map).await,
            _ => None,
        };
        if let Some(plan) = plan {
            return Some(self.post_multipart(url, plan, map).await);
        }

        let (chat_id, caption) = (chat_id_of(&map, call.query_map), caption_of(&map, call.query_map));
        let mut local: Option<(&str, PathBuf)> = None;
        for &field in MEDIA_FIELDS {
            let Some(Value::String(s)) = map.get(field) else { continue };
            if let Some(path) = self.resolve_local_path(s).await {
                local = Some((field, path));
                break;
            }
            if is_http_url(s)
                && let Some(res) = self
                    .maybe_upload_large_remote_url_mtproto(call.token, call.method, &chat_id, s, caption)
                    .await
            {
                return Some(res);
            }
        }
        let (field, path) = local?;

        if file_len(&path).await > MTPROTO_UPLOAD_THRESHOLD {
            return Some(
                self.upload_local_file_mtproto(LargeUpload {
                    token: call.token,
                    method: call.method,
                    chat_id: &chat_id,
                    path: &path,
                    caption,
                    file_name: None,
                    extra_fields: Some(&map),
                })
                .await,
            );
        }
        let plan = Multipart {
            files: vec![(field.to_string(), path)],
            default_name: "file.bin",
            media_json: None,
            skip: field.to_string(),
        };
        Some(self.post_multipart(url, plan, map).await)
    }

    /// `--local` request with the media given as query parameters: only files above the HTTP upload
    /// limit and large remote URLs need special handling.
    async fn local_media_from_query(&self, call: &RelayCall<'_>) -> Option<TelegramResult<Bytes>> {
        let query = call.query_map;
        if query.is_empty() {
            return None;
        }
        let chat_id = query.get("chat_id").map(String::as_str).unwrap_or("");
        let caption = query.get("caption").map(String::as_str).unwrap_or("");
        for &field in MEDIA_FIELDS {
            let Some(val) = query.get(field) else { continue };
            if let Some(path) = self.resolve_local_path(val).await {
                if file_len(&path).await > MTPROTO_UPLOAD_THRESHOLD {
                    return Some(
                        self.upload_local_file_mtproto(LargeUpload {
                            token: call.token,
                            method: call.method,
                            chat_id,
                            path: &path,
                            caption,
                            file_name: None,
                            extra_fields: None,
                        })
                        .await,
                    );
                }
            } else if is_http_url(val)
                && let Some(res) = self
                    .maybe_upload_large_remote_url_mtproto(call.token, call.method, chat_id, val, caption)
                    .await
            {
                return Some(res);
            }
        }
        None
    }

    /// sendMediaGroup: every `media[].media` that names a local file becomes an `attach://file_<i>` part.
    async fn plan_media_group(&self, map: &Map<String, Value>) -> Option<Multipart> {
        let Some(Value::Array(items)) = map.get("media") else { return None };
        let mut items = items.clone();
        let mut files = Vec::new();
        for (idx, item) in items.iter_mut().enumerate() {
            let Some(media) = item.get("media").and_then(Value::as_str) else { continue };
            let Some(path) = self.resolve_local_path(media).await else { continue };
            let attach = format!("file_{idx}");
            item["media"] = Value::String(format!("attach://{attach}"));
            files.push((attach, path));
        }
        (!files.is_empty()).then(|| Multipart {
            files,
            default_name: "media.bin",
            media_json: Some(serde_json::to_string(&items).unwrap_or_default()),
            skip: "media".to_string(),
        })
    }

    /// editMessageMedia: a local `media.media` becomes the `attach://file_edit` part.
    async fn plan_edit_media(&self, map: &Map<String, Value>) -> Option<Multipart> {
        let Some(Value::Object(item)) = map.get("media") else { return None };
        let path = self.resolve_local_path(item.get("media")?.as_str()?).await?;
        let mut item = item.clone();
        item.insert("media".to_string(), Value::String("attach://file_edit".to_string()));
        Some(Multipart {
            files: vec![("file_edit".to_string(), path)],
            default_name: "media.bin",
            media_json: Some(serde_json::to_string(&item).unwrap_or_default()),
            skip: "media".to_string(),
        })
    }

    /// Streams the files as multipart parts, copies the remaining JSON fields as text and posts to upstream.
    async fn post_multipart(&self, url: &str, plan: Multipart, map: Map<String, Value>) -> TelegramResult<Bytes> {
        let mut form = Form::new();
        for (name, path) in plan.files {
            form = form.part(name, Self::build_streaming_part(&path, plan.default_name).await?);
        }
        if let Some(media) = plan.media_json {
            form = form.text("media", media);
        }
        for (k, v) in map {
            if k == plan.skip || v.is_null() {
                continue;
            }
            let text = match v {
                Value::String(s) => s,
                Value::Bool(b) => b.to_string(),
                Value::Number(n) => n.to_string(),
                other => serde_json::to_string(&other).unwrap_or_default(),
            };
            form = form.text(k, text);
        }
        send_bytes(self.client.post(url).multipart(form)).await
    }
}

fn is_http_url(s: &str) -> bool {
    s.starts_with("http://") || s.starts_with("https://")
}

async fn file_len(path: &std::path::Path) -> u64 {
    fs::metadata(path).await.map(|m| m.len()).unwrap_or(0)
}

/// `chat_id` from the JSON body (string or number), else from the query.
fn chat_id_of(map: &Map<String, Value>, query: &HashMap<String, String>) -> String {
    map.get("chat_id")
        .map(|v| match v {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => String::new(),
        })
        .or_else(|| query.get("chat_id").cloned())
        .unwrap_or_default()
}

/// `caption` from the JSON body, else from the query.
fn caption_of<'a>(map: &'a Map<String, Value>, query: &'a HashMap<String, String>) -> &'a str {
    map.get("caption")
        .and_then(Value::as_str)
        .or_else(|| query.get("caption").map(String::as_str))
        .unwrap_or("")
}
