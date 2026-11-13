use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use bytes::Bytes;
use dashmap::DashMap;
use reqwest::multipart::{Form, Part};
use reqwest::Client;
use serde_json::Value;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::task::AbortHandle;
use tokio_util::io::ReaderStream;

use crate::mtproto::{MediaAttrs, MediaKind};
use crate::error::{TelegramError, TelegramResult};
use crate::models::{
    ApiResponse, DeleteMessageRequest, EditMessageTextRequest, File, GetFileRequest,
    GetUpdatesRequest, Message, SendAnimationRequest, SendAudioRequest, SendDocumentRequest,
    SendMessageRequest, SendPhotoRequest, SendVideoRequest, SendVoiceRequest, SetWebhookRequest,
    Update, User, WebhookInfo,
};
use crate::storage::StorageEngine;

/// 64KB chunk size for O(1) constant-memory file streaming
const STREAM_BUFFER_SIZE: usize = 65536;

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

/// Core engine handling upstream communication, local file streaming, and SQLite WAL caching.
pub struct TelegramEngine {
    pub client: Client,
    pub webhook_client: Client,
    pub upstream_base_url: String,
    pub local_mode: bool,
    pub dir: PathBuf,
    pub temp_dir: PathBuf,
    pub storage: Arc<StorageEngine>,
    pub webhook_workers: Arc<DashMap<String, AbortHandle>>,
    /// Runtime state (pending count, last error) of running local webhook workers.
    pub webhook_states: Arc<DashMap<String, Arc<super::webhook::WebhookState>>>,
    pub set_webhook_times: DashMap<String, std::time::Instant>,
    pub proxy_url: Option<String>,
    pub active_bots: Arc<DashMap<String, ()>>,
    pub token_filter: Option<(u64, u64)>,
    pub default_max_webhook_connections: i32,
    pub api_id: i32,
    pub api_hash: String,
}

impl TelegramEngine {
    pub fn new(
        local_mode: bool,
        dir: PathBuf,
        temp_dir: PathBuf,
        storage: Arc<StorageEngine>,
    ) -> Self {
        Self::with_options(local_mode, dir, temp_dir, storage, None, None, if local_mode { 100 } else { 40 })
    }

    pub fn with_api_credentials(mut self, api_id: i32, api_hash: String) -> Self {
        self.api_id = api_id;
        self.api_hash = api_hash;
        self
    }

    pub fn with_options(
        local_mode: bool,
        dir: PathBuf,
        temp_dir: PathBuf,
        storage: Arc<StorageEngine>,
        proxy_url: Option<&str>,
        token_filter: Option<(u64, u64)>,
        default_max_webhook_connections: i32,
    ) -> Self {
        let abs_dir = std::fs::canonicalize(&dir).unwrap_or_else(|_| {
            std::env::current_dir()
                .map(|cwd| cwd.join(&dir))
                .unwrap_or(dir)
        });

        let mut client_builder = Client::builder()
            .pool_max_idle_per_host(64)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(300));

        let mut webhook_builder = Client::builder()
            .pool_max_idle_per_host(64)
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .timeout(Duration::from_secs(60));

        if let Some(p) = proxy_url {
            if let Ok(proxy) = reqwest::Proxy::all(p) {
                webhook_builder = webhook_builder.proxy(proxy);
            }
            if std::env::var("TELEGRAM_UPSTREAM_PROXY").is_ok() {
                if let Ok(proxy) = reqwest::Proxy::all(p) {
                    client_builder = client_builder.proxy(proxy);
                }
            }
        }

        let client = client_builder
            .build()
            .expect("Failed to initialize HTTP client");

        let webhook_client = webhook_builder
            .build()
            .expect("Failed to initialize webhook HTTP client");

        Self {
            client,
            webhook_client,
            upstream_base_url: "https://api.telegram.org".to_string(),
            local_mode,
            dir: abs_dir,
            temp_dir,
            storage,
            webhook_workers: Arc::new(DashMap::new()),
            webhook_states: Arc::new(DashMap::new()),
            set_webhook_times: DashMap::new(),
            proxy_url: proxy_url.map(|s| s.to_string()),
            active_bots: Arc::new(DashMap::with_capacity(64)),
            token_filter,
            default_max_webhook_connections,
            api_id: 0,
            api_hash: String::new(),
        }
    }

    #[inline]
    pub fn record_active_bot(&self, token: &str) {
        let bot_id = token.split(':').next().unwrap_or(token);
        if !bot_id.is_empty() && !self.active_bots.contains_key(bot_id) {
            if self.active_bots.len() < 4096 {
                self.active_bots.insert(bot_id.to_string(), ());
            }
        }
    }

    /// Drops a bot from the seen-bots map (called when upstream answers 401 for its token).
    pub fn forget_active_bot(&self, token: &str) {
        let bot_id = token.split(':').next().unwrap_or(token);
        self.active_bots.remove(bot_id);
    }

    /// Updates the seen-bots map from an upstream reply: record on ok:true, forget on error_code 401.
    /// Only tokens upstream accepted are counted, so fake tokens cannot inflate `bot_count`.
    fn note_upstream_reply(&self, token: &str, reply: &Result<Bytes, TelegramError>) {
        match reply {
            Ok(bytes) => {
                #[derive(serde::Deserialize)]
                struct Head {
                    ok: Option<bool>,
                    error_code: Option<i64>,
                }
                // Skip the parse for already-known bots unless the reply is small enough to be a 401.
                let bot_id = token.split(':').next().unwrap_or(token);
                if self.active_bots.contains_key(bot_id) && bytes.len() > 4096 {
                    return;
                }
                if let Ok(h) = serde_json::from_slice::<Head>(bytes) {
                    if h.ok == Some(true) {
                        self.record_active_bot(token);
                    } else if h.error_code == Some(401) {
                        self.forget_active_bot(token);
                    }
                }
            }
            Err(TelegramError::Unauthorized(_)) | Err(TelegramError::ApiError { code: 401, .. }) => {
                self.forget_active_bot(token)
            }
            Err(_) => {}
        }
    }

    #[inline]
    pub fn active_bots_count(&self) -> usize {
        self.active_bots
            .len()
            .max(self.webhook_workers.len())
            .max(self.storage.cached_bots_count())
    }

    /// Validates the token like C++ ClientManager::send (401 invalid token, 421 forbidden by `--filter`)
    #[inline]
    pub fn check_token_filter(&self, token: &str) -> Result<(), TelegramError> {
        validate_token(token, self.token_filter)
    }

    #[inline]
    pub fn bot_dir_name(token: &str) -> String {
        #[cfg(unix)]
        {
            token.to_string()
        }
        #[cfg(not(unix))]
        {
            token.replace(':', "_")
        }
    }

    /// Parses TDLib `webhooks_db.binlog` binary records to extract the latest `(token, webhook_url)` per bot
    pub(crate) fn parse_webhooks_binlog(bytes: &[u8]) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        let text = String::from_utf8_lossy(bytes);
        let lines: Vec<&str> = text.lines().collect();

        for i in 0..lines.len() {
            let line = lines[i].trim();
            if let Some(colon_pos) = line.find(':') {
                // Extract numeric bot_id before ':'
                let prefix = &line[..colon_pos];
                let bot_id_str: String = prefix
                    .chars()
                    .rev()
                    .take_while(|c| c.is_ascii_digit())
                    .collect::<String>()
                    .chars()
                    .rev()
                    .collect();

                if bot_id_str.len() >= 6 && bot_id_str.len() <= 12 {
                    let secret: String = line[colon_pos + 1..]
                        .chars()
                        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '-')
                        .collect();

                    if secret.len() >= 30 && secret.len() <= 50 {
                        let token = format!("{}:{}", bot_id_str, secret);
                        // Look at the next line or same line for http:// or https://
                        let search_region = if i + 1 < lines.len() {
                            format!("{} {}", line, lines[i + 1])
                        } else {
                            line.to_string()
                        };

                        if let Some(http_idx) = search_region.find("http://").or_else(|| search_region.find("https://")) {
                            let raw_url: String = search_region[http_idx..]
                                .chars()
                                .take_while(|c| !c.is_control() && !c.is_whitespace())
                                .collect();

                            // If the URL ends with /{bot_id} followed by binlog framing garbage, trim right after /{bot_id}
                            let needle = format!("/{}", bot_id_str);
                            let clean_url = if let Some(pos) = raw_url.rfind(&needle) {
                                raw_url[..pos + needle.len()].to_string()
                            } else {
                                raw_url
                            };

                            map.insert(token, clean_url);
                        }
                    }
                }
            }
        }
        map
    }

    /// Resolve a file input string if local_mode is enabled and the path exists on disk
    /// Matches C++ Client::get_local_file_path in telegram-bot-api/Client.cpp:10763
    #[inline]
    pub fn resolve_local_path(&self, file_input: &str) -> Option<PathBuf> {
        if !self.local_mode || file_input.is_empty() {
            return None;
        }
        if file_input.starts_with("http://") || file_input.starts_with("https://") {
            return None;
        }
        if !file_input.contains('/') && !file_input.contains('\\') {
            return None;
        }

        let mut cleaned = file_input;
        if let Some(after_proto) = cleaned.strip_prefix("file:/") {
            cleaned = after_proto
                .strip_prefix("/localhost")
                .or_else(|| after_proto.strip_prefix('/'))
                .unwrap_or(after_proto);
            #[cfg(windows)]
            {
                cleaned = cleaned.strip_prefix('/').unwrap_or(cleaned);
            }
        }

        let path = Path::new(cleaned);
        if path.exists() && path.is_file() {
            return Some(path.to_path_buf());
        }
        let rel = self.dir.join(cleaned);
        if rel.exists() && rel.is_file() {
            return Some(rel);
        }
        None
    }

    /// Uploads a large local file (> 50 MB up to 2000 MB) directly to Telegram DC via Pure-Rust MTProto 2.0
    /// (`upload.saveBigFilePart` + `messages.sendMedia`), returning a Bot API `Message` JSON payload.
    pub async fn upload_local_file_mtproto(
        &self,
        token: &str,
        method: &str,
        chat_id_str: &str,
        local_path: &Path,
        caption: &str,
        custom_file_name: Option<&str>,
        extra_fields: Option<&serde_json::Map<String, Value>>,
    ) -> TelegramResult<Bytes> {
        if self.api_id <= 0 || self.api_hash.is_empty() {
            return Err(TelegramError::Internal(
                "MTProto large file upload requires valid --api-id and --api-hash".to_string(),
            ));
        }

        // 1. Fetch bot identity and target chat metadata (including username hint for peer resolution)
        let bot_user = self.get_me(token).await.ok();
        let chat_url = format!(
            "{}/bot{token}/getChat?chat_id={}",
            self.upstream_base_url, chat_id_str
        );
        let mut chat_json = serde_json::json!({
            "id": chat_id_str.parse::<i64>().unwrap_or(0),
            "type": "private"
        });
        let mut hint_username: Option<String> = None;

        if let Ok(resp) = self.client.get(&chat_url).send().await {
            if let Ok(val) = resp.json::<Value>().await {
                if let Some(res_obj) = val.get("result") {
                    chat_json = res_obj.clone();
                    if let Some(u) = res_obj.get("username").and_then(|v| v.as_str()) {
                        hint_username = Some(u.to_string());
                    }
                }
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

        if let Some(u) = bot_user {
            if let Ok(u_val) = serde_json::to_value(u) {
                msg_obj["from"] = u_val;
            }
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
        let temp_path = temp_dir.join(format!("url_{}_{}", nanos, url_file_name));

        let mut get_resp = match self.client.get(remote_url).send().await {
            Ok(r) if r.status().is_success() => r,
            _ => return None,
        };

        let out_file = match fs::File::create(&temp_path).await {
            Ok(f) => f,
            Err(e) => {
                return Some(Err(TelegramError::Internal(format!(
                    "Failed to create temp file for remote URL: {e}"
                ))))
            }
        };
        let mut writer = tokio::io::BufWriter::with_capacity(STREAM_BUFFER_SIZE, out_file);
        while let Ok(Some(chunk)) = get_resp.chunk().await {
            if let Err(e) = writer.write_all(&chunk).await {
                let _ = fs::remove_file(&temp_path).await;
                return Some(Err(TelegramError::Internal(format!(
                    "Failed writing remote URL stream: {e}"
                ))));
            }
        }
        let _ = writer.flush().await;
        drop(writer);

        let res = self
            .upload_local_file_mtproto(
                token,
                method,
                chat_id_str,
                &temp_path,
                caption,
                Some(&url_file_name),
                None,
            )
            .await;
        let _ = fs::remove_file(&temp_path).await;
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

    /// Execute getMe (checks SQLite WAL + L1 DashMap cache first)
    pub async fn get_me(&self, token: &str) -> TelegramResult<User> {
        if let Some(cached_user) = self.storage.get_bot(token).await {
            self.record_active_bot(token);
            return Ok((*cached_user).clone());
        }
        self.get_me_fresh(token).await
    }

    /// getMe straight from upstream (validates the token), refreshing the storage cache
    pub async fn get_me_fresh(&self, token: &str) -> TelegramResult<User> {

        let url = format!("{}/bot{token}/getMe", self.upstream_base_url);
        let resp: ApiResponse<User> = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        let user = match (resp.ok, resp.result) {
            (true, Some(user)) => user,
            _ => {
                if resp.error_code.unwrap_or(401) == 401 {
                    self.forget_active_bot(token);
                }
                return Err(TelegramError::ApiError {
                    code: resp.error_code.unwrap_or(401),
                    description: resp
                        .description
                        .unwrap_or_else(|| "Unauthorized".to_string()),
                    parameters: resp.parameters,
                })
            }
        };

        self.record_active_bot(token);
        self.storage.save_bot(token, &user).await;
        Ok(user)
    }

    /// Execute sendMessage
    pub async fn send_message(
        &self,
        token: &str,
        req: &SendMessageRequest,
    ) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/sendMessage", self.upstream_base_url);
        let resp: ApiResponse<Message> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute editMessageText
    pub async fn edit_message_text(
        &self,
        token: &str,
        req: &EditMessageTextRequest,
    ) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/editMessageText", self.upstream_base_url);
        let resp: ApiResponse<Message> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute deleteMessage
    pub async fn delete_message(
        &self,
        token: &str,
        req: &DeleteMessageRequest,
    ) -> TelegramResult<bool> {
        let url = format!("{}/bot{token}/deleteMessage", self.upstream_base_url);
        let resp: ApiResponse<bool> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute sendPhoto with O(1) memory streaming
    pub async fn send_photo(&self, token: &str, req: &SendPhotoRequest) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/sendPhoto", self.upstream_base_url);
        let response = if let Some(local_path) = self.resolve_local_path(&req.photo) {
            let part = Self::build_streaming_part(&local_path, "photo.jpg").await?;
            let mut form = Form::new()
                .text("chat_id", req.chat_id.to_string())
                .part("photo", part);
            if let Some(ref cap) = req.caption {
                form = form.text("caption", cap.clone());
            }
            if let Some(ref pm) = req.parse_mode {
                form = form.text("parse_mode", pm.clone());
            }
            self.client.post(&url).multipart(form).send().await
        } else {
            self.client.post(&url).json(req).send().await
        };

        let resp: ApiResponse<Message> = response
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute sendDocument with O(1) memory streaming
    pub async fn send_document(
        &self,
        token: &str,
        req: &SendDocumentRequest,
    ) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/sendDocument", self.upstream_base_url);
        let response = if let Some(local_path) = self.resolve_local_path(&req.document) {
            let part = Self::build_streaming_part(&local_path, "file.bin").await?;
            let mut form = Form::new()
                .text("chat_id", req.chat_id.to_string())
                .part("document", part);
            if let Some(ref cap) = req.caption {
                form = form.text("caption", cap.clone());
            }
            if let Some(ref pm) = req.parse_mode {
                form = form.text("parse_mode", pm.clone());
            }
            self.client.post(&url).multipart(form).send().await
        } else {
            self.client.post(&url).json(req).send().await
        };

        let resp: ApiResponse<Message> = response
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute sendAudio with O(1) memory streaming
    pub async fn send_audio(&self, token: &str, req: &SendAudioRequest) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/sendAudio", self.upstream_base_url);
        let response = if let Some(local_path) = self.resolve_local_path(&req.audio) {
            let part = Self::build_streaming_part(&local_path, "audio.mp3").await?;
            let form = Form::new()
                .text("chat_id", req.chat_id.to_string())
                .part("audio", part);
            self.client.post(&url).multipart(form).send().await
        } else {
            self.client.post(&url).json(req).send().await
        };

        let resp: ApiResponse<Message> = response
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute sendVideo with O(1) memory streaming
    pub async fn send_video(&self, token: &str, req: &SendVideoRequest) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/sendVideo", self.upstream_base_url);
        let response = if let Some(local_path) = self.resolve_local_path(&req.video) {
            let part = Self::build_streaming_part(&local_path, "video.mp4").await?;
            let form = Form::new()
                .text("chat_id", req.chat_id.to_string())
                .part("video", part);
            self.client.post(&url).multipart(form).send().await
        } else {
            self.client.post(&url).json(req).send().await
        };

        let resp: ApiResponse<Message> = response
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute sendVoice with O(1) memory streaming
    pub async fn send_voice(&self, token: &str, req: &SendVoiceRequest) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/sendVoice", self.upstream_base_url);
        let response = if let Some(local_path) = self.resolve_local_path(&req.voice) {
            let part = Self::build_streaming_part(&local_path, "voice.ogg").await?;
            let form = Form::new()
                .text("chat_id", req.chat_id.to_string())
                .part("voice", part);
            self.client.post(&url).multipart(form).send().await
        } else {
            self.client.post(&url).json(req).send().await
        };

        let resp: ApiResponse<Message> = response
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute sendAnimation with O(1) memory streaming
    pub async fn send_animation(
        &self,
        token: &str,
        req: &SendAnimationRequest,
    ) -> TelegramResult<Message> {
        let url = format!("{}/bot{token}/sendAnimation", self.upstream_base_url);
        let response = if let Some(local_path) = self.resolve_local_path(&req.animation) {
            let part = Self::build_streaming_part(&local_path, "animation.mp4").await?;
            let form = Form::new()
                .text("chat_id", req.chat_id.to_string())
                .part("animation", part);
            self.client.post(&url).multipart(form).send().await
        } else {
            self.client.post(&url).json(req).send().await
        };

        let resp: ApiResponse<Message> = response
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute getFile with SQLite WAL lookup and chunked disk write in local mode
    pub async fn get_file(&self, token: &str, req: &GetFileRequest) -> TelegramResult<File> {
        if req.file_id.is_empty() {
            return Err(TelegramError::BadRequest("file_id not specified".to_string()));
        }

        if self.local_mode {
            if let Some(cached) = self.storage.get_cached_file(&req.file_id).await {
                if let Some(ref p) = cached.file_path {
                    let check_path = if Path::new(p).is_absolute() {
                        PathBuf::from(p)
                    } else {
                        self.dir.join(p)
                    };
                    if check_path.exists() {
                        let mut res = cached.clone();
                        let abs_path = std::fs::canonicalize(&check_path).unwrap_or(check_path);
                        res.file_path = Some(abs_path.to_string_lossy().to_string());
                        return Ok(res);
                    }
                }
            }
        }

        let url = format!("{}/bot{token}/getFile", self.upstream_base_url);
        let resp: ApiResponse<File> = self
            .client
            .get(&url)
            .query(&[("file_id", &req.file_id)])
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        if !resp.ok {
            let desc = resp.description.as_deref().unwrap_or("");
            let desc_lower = desc.to_ascii_lowercase();
            if self.local_mode
                && (desc_lower.contains("too big") || desc_lower.contains("file is too big"))
            {
                let bot_folder = Self::bot_dir_name(token);
                let target_dir = self.dir.join(&bot_folder);
                let (target_file, total_bytes) = crate::mtproto::download_large_file_via_mtproto(
                    self.api_id,
                    &self.api_hash,
                    token,
                    &req.file_id,
                    &target_dir,
                )
                .await
                .map_err(|e| {
                    let msg = e.to_string();
                    if let Some(wait_secs) = crate::mtproto::uploader::MtProtoConnection::parse_flood_wait(&msg) {
                        TelegramError::flood_wait(wait_secs)
                    } else {
                        TelegramError::BadRequest(format!("MTProto download failed: {e}"))
                    }
                })?;

                let path_str = if self.local_mode {
                    let abs_path = std::fs::canonicalize(&target_file).unwrap_or_else(|_| {
                        if target_file.is_absolute() {
                            target_file.clone()
                        } else {
                            self.dir.join(&target_file)
                        }
                    });
                    abs_path.to_string_lossy().to_string()
                } else {
                    target_file
                        .strip_prefix(&self.dir)
                        .map(|p| p.to_string_lossy().replace('\\', "/"))
                        .unwrap_or_else(|_| target_file.to_string_lossy().replace('\\', "/"))
                };

                let unique_id = format!("AgAD{:x}", req.file_id.len());
                let file = File {
                    file_id: req.file_id.clone(),
                    file_unique_id: unique_id,
                    file_size: Some(total_bytes as i64),
                    file_path: Some(path_str),
                };
                self.storage.save_cached_file(&file).await;
                return Ok(file);
            }

            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp.description.unwrap_or_else(|| "File not found".to_string()),
                parameters: resp.parameters,
            });
        }

        let mut file = resp.result.ok_or_else(|| {
            TelegramError::BadRequest("File not found".to_string())
        })?;

        // In non-local mode, reject files > 20MB matching C++ Client.cpp:17042
        if !self.local_mode {
            if let Some(size) = file.file_size {
                if size > 20 * 1024 * 1024 {
                    return Err(TelegramError::BadRequest(
                        "Bad Request: file is too big".to_string(),
                    ));
                }
            }
        }

        if self.local_mode {
            if let Some(ref remote_path) = file.file_path {
                let bot_folder = Self::bot_dir_name(token);
                let target_dir = self.dir.join(bot_folder);
                let target_file = target_dir.join(remote_path);

                if let Some(parent) = target_file.parent() {
                    let _ = fs::create_dir_all(parent).await;
                }

                if !target_file.exists() {
                    let dl_url = format!("{}/file/bot{token}/{remote_path}", self.upstream_base_url);
                    if let Ok(mut dl_resp) = self.client.get(&dl_url).send().await {
                        if dl_resp.status().is_success() {
                            if let Ok(out_file) = fs::File::create(&target_file).await {
                                let mut writer =
                                    tokio::io::BufWriter::with_capacity(STREAM_BUFFER_SIZE, out_file);
                                while let Ok(Some(chunk)) = dl_resp.chunk().await {
                                    if writer.write_all(&chunk).await.is_err() {
                                        break;
                                    }
                                }
                                let _ = writer.flush().await;
                            }
                        }
                    }
                }

                if target_file.exists() {
                    let path_str = if self.local_mode {
                        let abs_path = std::fs::canonicalize(&target_file).unwrap_or_else(|_| {
                            if target_file.is_absolute() {
                                target_file.clone()
                            } else {
                                self.dir.join(&target_file)
                            }
                        });
                        abs_path.to_string_lossy().to_string()
                    } else {
                        target_file
                            .strip_prefix(&self.dir)
                            .map(|p| p.to_string_lossy().replace('\\', "/"))
                            .unwrap_or_else(|_| target_file.to_string_lossy().replace('\\', "/"))
                    };
                    file.file_path = Some(path_str);
                    self.storage.save_cached_file(&file).await;
                }
            }
        }

        Ok(file)
    }

    /// Execute getUpdates
    pub async fn get_updates(
        &self,
        token: &str,
        req: &GetUpdatesRequest,
    ) -> TelegramResult<Vec<Update>> {
        if self.webhook_workers.contains_key(token) {
            return Err(TelegramError::Conflict(
                "can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first".to_string(),
            ));
        }

        let url = format!("{}/bot{token}/getUpdates", self.upstream_base_url);
        let resp: ApiResponse<Vec<Update>> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp.description.unwrap_or_else(|| "Unknown error".to_string()),
                parameters: resp.parameters,
            });
        }

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest("Unknown error".to_string())
        })
    }

    /// Execute setWebhook. Locally delivered webhooks (local mode + private/loopback http URL) are
    /// stored and delivered by a local worker; every other webhook is proxied to the upstream
    /// Bot API untouched (not stored, no local worker, never touched by this process afterwards).
    pub async fn set_webhook(
        self: &Arc<Self>,
        token: &str,
        req: &SetWebhookRequest,
    ) -> TelegramResult<bool> {
        if !req.url.is_empty() {
            self.check_set_webhook_throttle(token)?;
            super::webhook::validate_webhook_url(&req.url)
                .map_err(|m| TelegramError::BadRequest(m.to_string()))?;
        }
        if let Some(ref s) = req.secret_token {
            super::webhook::validate_secret_token(s)
                .map_err(|m| TelegramError::BadRequest(m.to_string()))?;
        }

        let is_local_url = self.local_mode && is_local_webhook_url(&req.url);

        if is_local_url {
            // Ensure cloud webhook is deleted so local getUpdates worker can receive updates
            let del_url = format!("{}/bot{token}/deleteWebhook", self.upstream_base_url);
            let drop_param = if req.drop_pending_updates.unwrap_or(false) {
                "?drop_pending_updates=true"
            } else {
                ""
            };
            let _ = self.client.post(format!("{}{}", del_url, drop_param)).send().await;

            let max_conn = super::webhook::clamp_max_connections(
                req.max_connections,
                self.default_max_webhook_connections,
                true,
            );
            let secret = req.secret_token.clone().filter(|s| !s.is_empty());
            let ip = req.ip_address.clone().filter(|s| !s.is_empty());
            self.storage
                .save_webhook(
                    token,
                    &req.url,
                    secret.as_deref(),
                    max_conn,
                    req.allowed_updates.as_deref(),
                    ip.as_deref(),
                )
                .await;

            self.start_webhook_worker(super::webhook::WorkerConfig {
                token: token.to_string(),
                url: req.url.clone(),
                secret_token: secret,
                allowed_updates: req.allowed_updates.clone(),
                drop_pending_updates: req.drop_pending_updates.unwrap_or(false),
                max_connections: max_conn,
                ip_address: ip,
            });
            return Ok(true);
        }

        // Cloud webhook: replaces any local one.
        if self.webhook_workers.contains_key(token) || self.webhook_states.contains_key(token) {
            self.stop_webhook_worker(token);
        }
        self.storage.delete_webhook(token).await;

        let url = format!("{}/bot{token}/setWebhook", self.upstream_base_url);
        let resp: ApiResponse<bool> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp
                    .description
                    .unwrap_or_else(|| "Failed to set webhook".to_string()),
                parameters: resp.parameters,
            });
        }
        Ok(resp.result.unwrap_or(false))
    }

    /// Execute deleteWebhook and remove from SQLite WAL + stop local webhook worker
    pub async fn delete_webhook(&self, token: &str, drop_pending_updates: bool) -> TelegramResult<bool> {
        self.stop_webhook_worker(token);
        self.storage.delete_webhook(token).await;

        let url = format!("{}/bot{token}/deleteWebhook", self.upstream_base_url);
        let drop_param = if drop_pending_updates {
            "?drop_pending_updates=true"
        } else {
            ""
        };
        let resp: ApiResponse<bool> = self
            .client
            .post(format!("{}{}", url, drop_param))
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp
                    .description
                    .unwrap_or_else(|| "Failed to delete webhook".to_string()),
                parameters: resp.parameters,
            });
        }

        Ok(resp.result.unwrap_or(true))
    }

    /// Execute getWebhookInfo. Locally delivered webhooks are answered from storage + worker
    /// state; cloud webhooks are answered by upstream.
    pub async fn get_webhook_info(&self, token: &str) -> TelegramResult<WebhookInfo> {
        if self.local_mode {
            if let Some(wh) = self.storage.get_webhook(token).await {
                if super::webhook::should_restore(true, &wh.url) {
                    let (pending, err_date, err_msg) = self
                        .webhook_states
                        .get(token)
                        .map(|s| s.snapshot())
                        .unwrap_or((0, None, None));
                    return Ok(WebhookInfo {
                        url: wh.url,
                        has_custom_certificate: false,
                        pending_update_count: pending.min(i32::MAX as i64) as i32,
                        ip_address: wh.ip_address,
                        last_error_date: err_date.map(|d| d as i32),
                        last_error_message: err_msg,
                        last_synchronization_error_date: None,
                        max_connections: Some(wh.max_connections),
                        allowed_updates: wh.allowed_updates,
                    });
                }
            }
        }

        let url = format!("{}/bot{token}/getWebhookInfo", self.upstream_base_url);
        let resp: ApiResponse<WebhookInfo> = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp
                    .description
                    .unwrap_or_else(|| "Unknown error".to_string()),
                parameters: resp.parameters,
            });
        }

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Resolves a served file path inside the bot's own working directory (local mode only).
    /// Absolute paths (as returned by getFile in local mode) are accepted only if they stay
    /// within `<dir>/<token>`; `..` traversal and symlink escapes are rejected.
    pub fn resolve_file_on_disk(&self, token: &str, file_path: &str) -> Option<PathBuf> {
        if !self.local_mode {
            return None;
        }
        let roots = [
            self.dir.join(Self::bot_dir_name(token)),
            self.dir.join(token.replace(':', "_")),
        ];
        let requested = Path::new(file_path);
        for root in &roots {
            let cand = if requested.is_absolute() {
                requested.to_path_buf()
            } else {
                root.join(requested)
            };
            if let Some(p) = confine_path(root, &cand) {
                return Some(p);
            }
        }
        None
    }

    /// Resolves a local file for zero-copy streaming if present on disk, or returns an upstream response stream
    pub async fn open_file_stream(
        &self,
        token: &str,
        file_path: &str,
    ) -> Result<(axum::body::Body, Option<u64>), TelegramError> {
        if let Some(local_path) = self.resolve_file_on_disk(token, file_path) {
            let file = fs::File::open(&local_path)
                .await
                .map_err(|e| TelegramError::Internal(format!("Failed to open local file: {e}")))?;
            let len = file.metadata().await.ok().map(|m| m.len());
            let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
            return Ok((axum::body::Body::from_stream(stream), len));
        }

        let url = format!("{}/file/bot{token}/{file_path}", self.upstream_base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("File download network error: {e}")))?;

        if !resp.status().is_success() {
            let code = resp.status().as_u16();
            return Err(TelegramError::ApiError {
                code,
                description: TelegramError::from_http_status(
                    code,
                    &format!("File download failed: status {}", resp.status()),
                )
                .to_string(),
                parameters: None,
            });
        }

        let content_len = resp.content_length();
        let stream = resp.bytes_stream();
        Ok((axum::body::Body::from_stream(stream), content_len))
    }

    /// Universal call supporting JSON (with local file path auto-streaming), Multipart passthrough, Form-urlencoded, and Query params
    pub async fn universal_call(
        self: &Arc<Self>,
        token: &str,
        method: &str,
        query_map: &std::collections::HashMap<String, String>,
        query_str: Option<&str>,
        content_type: Option<&str>,
        body_bytes: Bytes,
    ) -> TelegramResult<Bytes> {
        self.check_token_filter(token)?;
        let reply = self
            .universal_call_inner(token, method, query_map, query_str, content_type, body_bytes)
            .await;
        self.note_upstream_reply(token, &reply);
        reply
    }

    async fn universal_call_inner(
        self: &Arc<Self>,
        token: &str,
        method: &str,
        query_map: &std::collections::HashMap<String, String>,
        query_str: Option<&str>,
        content_type: Option<&str>,
        body_bytes: Bytes,
    ) -> TelegramResult<Bytes> {
        let method_lower = method.to_ascii_lowercase();

        if method_lower == "getupdates" && self.webhook_workers.contains_key(token) {
            return Err(TelegramError::Conflict(
                "can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first".to_string(),
            ));
        }

        // 1. Handle setWebhook / deleteWebhook / getWebhookInfo / getFile specially
        if method_lower == "setwebhook" {
            let mut url_val = query_map.get("url").cloned().unwrap_or_default();
            let mut ip_address = query_map.get("ip_address").cloned();
            let mut max_conn = query_map
                .get("max_connections")
                .and_then(|v| v.parse::<i32>().ok());
            let mut secret_token = query_map.get("secret_token").cloned();
            let mut drop_pending_updates = query_map
                .get("drop_pending_updates")
                .and_then(|v| v.parse::<bool>().ok());
            let mut allowed_updates: Option<Vec<String>> = query_map
                .get("allowed_updates")
                .and_then(|s| serde_json::from_str(s).ok());

            if !body_bytes.is_empty() {
                if let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) {
                    if let Some(u) = json.get("url").and_then(|v| v.as_str()) {
                        url_val = u.to_string();
                    }
                    if let Some(ip) = json.get("ip_address").and_then(|v| v.as_str()) {
                        ip_address = Some(ip.to_string());
                    }
                    if let Some(mc) = json.get("max_connections").and_then(|v| v.as_i64()) {
                        max_conn = Some(mc as i32);
                    }
                    if let Some(sec) = json.get("secret_token").and_then(|v| v.as_str()) {
                        secret_token = Some(sec.to_string());
                    }
                    if let Some(drop) = json.get("drop_pending_updates").and_then(|v| v.as_bool()) {
                        drop_pending_updates = Some(drop);
                    }
                    if let Some(arr) = json.get("allowed_updates").and_then(|v| v.as_array()) {
                        let parsed: Vec<String> = arr
                            .iter()
                            .filter_map(|x| x.as_str().map(|s| s.to_string()))
                            .collect();
                        allowed_updates = Some(parsed);
                    }
                }
            }

            let req = SetWebhookRequest {
                url: url_val,
                ip_address,
                max_connections: max_conn,
                allowed_updates,
                drop_pending_updates,
                secret_token,
            };
            let ok = self.set_webhook(token, &req).await?;
            if !ok {
                return Err(TelegramError::BadRequest("Failed to set webhook".to_string()));
            }
            return Ok(Bytes::from_static(
                br#"{"ok":true,"result":true,"description":"Webhook was set"}"#,
            ));
        }

        if method_lower == "close" {
            // C++ re-creates the client on the next request; webhook delivery must keep running.
            crate::mtproto::uploader::invalidate_bot_auth(token);
            return Ok(Bytes::from_static(
                br#"{"ok":true,"result":true,"description":"Bot instance closed"}"#,
            ));
        }

        if method_lower == "logout" {
            // Relay the upstream result; only tear down local state once upstream confirmed logOut.
            let url = format!("{}/bot{token}/logOut", self.upstream_base_url);
            let out = self
                .client
                .post(&url)
                .send()
                .await
                .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
                .bytes()
                .await
                .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
            let ok = serde_json::from_slice::<Value>(&out)
                .ok()
                .and_then(|j| j.get("ok").and_then(|v| v.as_bool()))
                .unwrap_or(false);
            if ok {
                self.stop_webhook_worker(token);
                self.storage.delete_webhook(token).await;
                crate::mtproto::uploader::invalidate_bot_auth(token);
            }
            return Ok(out);
        }

        if method_lower == "deletewebhook" {
            let drop_pending = query_map
                .get("drop_pending_updates")
                .and_then(|v| v.parse::<bool>().ok())
                .or_else(|| {
                    if !body_bytes.is_empty() {
                        serde_json::from_slice::<Value>(&body_bytes)
                            .ok()
                            .and_then(|j| j.get("drop_pending_updates").and_then(|v| v.as_bool()))
                    } else {
                        None
                    }
                })
                .unwrap_or(false);

            let _ = self.delete_webhook(token, drop_pending).await?;
            return Ok(Bytes::from_static(
                br#"{"ok":true,"result":true,"description":"Webhook was deleted"}"#,
            ));
        }

        if method_lower == "getwebhookinfo" {
            let info = self.get_webhook_info(token).await?;
            let resp = ApiResponse::success(info);
            let vec = serde_json::to_vec(&resp).unwrap_or_default();
            return Ok(Bytes::from(vec));
        }

        if method_lower == "getfile" {
            let mut file_id = query_map.get("file_id").cloned().unwrap_or_default();
            if file_id.is_empty() && !body_bytes.is_empty() {
                if let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) {
                    if let Some(fid) = json.get("file_id").and_then(|v| v.as_str()) {
                        file_id = fid.to_string();
                    }
                }
                if file_id.is_empty() {
                    if let Ok(body_str) = std::str::from_utf8(&body_bytes) {
                        for pair in body_str.split('&') {
                            if let Some((k, v)) = pair.split_once('=') {
                                if k == "file_id" {
                                    file_id = v.to_string();
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            if !file_id.is_empty() {
                match self.get_file(token, &GetFileRequest { file_id }).await {
                    Ok(file) => {
                        let resp = ApiResponse::success(file);
                        let vec = serde_json::to_vec(&resp).unwrap_or_default();
                        return Ok(Bytes::from(vec));
                    }
                    Err(e) => {
                        let err_json = match e {
                            TelegramError::ApiError { code, description, parameters } => serde_json::json!({
                                "ok": false,
                                "error_code": code,
                                "description": description,
                                "parameters": parameters
                            }),
                            _ => serde_json::json!({
                                "ok": false,
                                "error_code": 400,
                                "description": e.to_string()
                            }),
                        };
                        return Ok(Bytes::from(serde_json::to_vec(&err_json).unwrap_or_default()));
                    }
                }
            }
        }

        // 2. Build upstream URL with optional query string
        let url = match query_str {
            Some(q) if !q.is_empty() => {
                format!("{}/bot{token}/{method}?{q}", self.upstream_base_url)
            }
            _ => format!("{}/bot{token}/{method}", self.upstream_base_url),
        };

        let ct = content_type.unwrap_or("");

        // 3. If JSON body in local_mode, fast-check whether this method and payload can possibly contain a local file path
        let may_have_local_media = self.local_mode
            && !body_bytes.is_empty()
            && (ct.starts_with("application/json") || body_bytes.starts_with(b"{"))
            && (body_bytes.contains(&b'/') || body_bytes.contains(&b'\\'))
            && !matches!(
                method_lower.as_str(),
                "sendmessage"
                    | "editmessagetext"
                    | "editmessagecaption"
                    | "editmessagereplymarkup"
                    | "deletemessage"
                    | "deletemessages"
                    | "forwardmessage"
                    | "forwardmessages"
                    | "copymessage"
                    | "copymessages"
                    | "answercallbackquery"
                    | "answerinlinequery"
                    | "getchat"
                    | "getchatmember"
                    | "getchatmembercount"
                    | "getchatadministrators"
                    | "banchatmember"
                    | "unbanchatmember"
                    | "restrictchatmember"
                    | "sendchataction"
            );

        if may_have_local_media {
            if let Ok(Value::Object(map)) = serde_json::from_slice::<Value>(&body_bytes) {
                if method_lower == "sendmediagroup" {
                    if let Some(Value::Array(items)) = map.get("media") {
                        let mut new_items = items.clone();
                        let mut parts_to_add: Vec<(String, PathBuf)> = Vec::new();
                        for (idx, it) in new_items.iter_mut().enumerate() {
                            if let Some(m_str) = it.get("media").and_then(|v| v.as_str()) {
                                if let Some(local_p) = self.resolve_local_path(m_str) {
                                    let attach_name = format!("file_{idx}");
                                    it["media"] = Value::String(format!("attach://{attach_name}"));
                                    parts_to_add.push((attach_name, local_p));
                                }
                            }
                        }
                        if !parts_to_add.is_empty() {
                            let mut form = Form::new();
                            for (attach_name, local_p) in parts_to_add {
                                let part = Self::build_streaming_part(&local_p, "media.bin").await?;
                                form = form.part(attach_name, part);
                            }
                            form = form.text("media", serde_json::to_string(&new_items).unwrap_or_default());
                            for (k, v) in map {
                                if k == "media" || v.is_null() {
                                    continue;
                                }
                                let val_str = match v {
                                    Value::String(s) => s,
                                    Value::Bool(b) => b.to_string(),
                                    Value::Number(n) => n.to_string(),
                                    other => serde_json::to_string(&other).unwrap_or_default(),
                                };
                                form = form.text(k, val_str);
                            }
                            let resp = self.client.post(&url).multipart(form).send().await
                                .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
                            let out = resp.bytes().await
                                .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
                            return Ok(out);
                        }
                    }
                }

                if method_lower == "editmessagemedia" {
                    if let Some(Value::Object(mut item)) = map.get("media").cloned() {
                        if let Some(m_str) = item.get("media").and_then(|v| v.as_str()) {
                            if let Some(local_p) = self.resolve_local_path(m_str) {
                                item.insert("media".to_string(), Value::String("attach://file_edit".to_string()));
                                let part = Self::build_streaming_part(&local_p, "media.bin").await?;
                                let mut form = Form::new().part("file_edit", part);
                                form = form.text("media", serde_json::to_string(&item).unwrap_or_default());
                                for (k, v) in map {
                                    if k == "media" || v.is_null() {
                                        continue;
                                    }
                                    let val_str = match v {
                                        Value::String(s) => s,
                                        Value::Bool(b) => b.to_string(),
                                        Value::Number(n) => n.to_string(),
                                        other => serde_json::to_string(&other).unwrap_or_default(),
                                    };
                                    form = form.text(k, val_str);
                                }
                                let resp = self.client.post(&url).multipart(form).send().await
                                    .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
                                let out = resp.bytes().await
                                    .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
                                return Ok(out);
                            }
                        }
                    }
                }

                let mut local_media_field: Option<(String, PathBuf)> = None;
                for &field_name in MEDIA_FIELDS {
                    if let Some(Value::String(s)) = map.get(field_name) {
                        if let Some(local_p) = self.resolve_local_path(s) {
                            local_media_field = Some((field_name.to_string(), local_p));
                            break;
                        } else if s.starts_with("http://") || s.starts_with("https://") {
                            let chat_id_str = map
                                .get("chat_id")
                                .map(|v| match v {
                                    Value::String(s) => s.clone(),
                                    Value::Number(n) => n.to_string(),
                                    _ => String::new(),
                                })
                                .or_else(|| query_map.get("chat_id").cloned())
                                .unwrap_or_default();
                            let caption = map
                                .get("caption")
                                .and_then(|v| v.as_str())
                                .or_else(|| query_map.get("caption").map(|s| s.as_str()))
                                .unwrap_or("");
                            if let Some(res) = self
                                .maybe_upload_large_remote_url_mtproto(token, method, &chat_id_str, s, caption)
                                .await
                            {
                                return res;
                            }
                        }
                    }
                }

                if let Some((media_key, local_path)) = local_media_field {
                    let file_len = fs::metadata(&local_path)
                        .await
                        .map(|m| m.len())
                        .unwrap_or(0);

                    if file_len > 49 * 1024 * 1024 {
                        let chat_id_str = map
                            .get("chat_id")
                            .map(|v| match v {
                                Value::String(s) => s.clone(),
                                Value::Number(n) => n.to_string(),
                                _ => String::new(),
                            })
                            .or_else(|| query_map.get("chat_id").cloned())
                            .unwrap_or_default();
                        let caption = map
                            .get("caption")
                            .and_then(|v| v.as_str())
                            .or_else(|| query_map.get("caption").map(|s| s.as_str()))
                            .unwrap_or("");
                        return self
                            .upload_local_file_mtproto(
                                token,
                                method,
                                &chat_id_str,
                                &local_path,
                                caption,
                                None,
                                Some(&map),
                            )
                            .await;
                    }

                    let part = Self::build_streaming_part(&local_path, "file.bin").await?;
                    let mut form = Form::new().part(media_key.clone(), part);
                    for (k, v) in map {
                        if k == media_key || v.is_null() {
                            continue;
                        }
                        let val_str = match v {
                            Value::String(s) => s,
                            Value::Bool(b) => b.to_string(),
                            Value::Number(n) => n.to_string(),
                            other => serde_json::to_string(&other).unwrap_or_default(),
                        };
                        form = form.text(k, val_str);
                    }
                    let resp = self
                        .client
                        .post(&url)
                        .multipart(form)
                        .send()
                        .await
                        .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
                    let out = resp
                        .bytes()
                        .await
                        .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
                    return Ok(out);
                }
            }
        }

        // 3b. Check query_map for local media paths or large remote URLs in --local mode
        if self.local_mode && !query_map.is_empty() {
            for &field_name in MEDIA_FIELDS {
                if let Some(val) = query_map.get(field_name) {
                    let chat_id_str = query_map.get("chat_id").cloned().unwrap_or_default();
                    let caption = query_map.get("caption").map(|s| s.as_str()).unwrap_or("");
                    if let Some(local_path) = self.resolve_local_path(val) {
                        let file_len = fs::metadata(&local_path)
                            .await
                            .map(|m| m.len())
                            .unwrap_or(0);
                        if file_len > 49 * 1024 * 1024 {
                            return self
                                .upload_local_file_mtproto(
                                    token,
                                    method,
                                    &chat_id_str,
                                    &local_path,
                                    caption,
                                    None,
                                    None,
                                )
                                .await;
                        }
                    } else if val.starts_with("http://") || val.starts_with("https://") {
                        if let Some(res) = self
                            .maybe_upload_large_remote_url_mtproto(token, method, &chat_id_str, val, caption)
                            .await
                        {
                            return res;
                        }
                    }
                }
            }
        }

        // 4. Direct passthrough (preserves multipart/form-data, application/json, form-urlencoded, or empty body)
        let req_builder = if body_bytes.is_empty() && ct.is_empty() {
            self.client.post(&url)
        } else if !ct.is_empty() {
            self.client
                .post(&url)
                .header("content-type", ct)
                .body(body_bytes)
        } else {
            self.client
                .post(&url)
                .header("content-type", "application/json")
                .body(body_bytes)
        };

        let resp = req_builder
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?;
        let out = resp
            .bytes()
            .await
            .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))?;
        Ok(out)
    }

    /// Generic caller for all 185+ Telegram Bot API methods
    pub async fn generic_call(
        &self,
        token: &str,
        method: &str,
        payload: Option<&Value>,
    ) -> TelegramResult<ApiResponse<Value>> {
        let url = format!("{}/bot{token}/{method}", self.upstream_base_url);
        let req = if let Some(body) = payload {
            self.client.post(&url).json(body)
        } else {
            self.client.get(&url)
        };

        let resp: ApiResponse<Value> = req
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        Ok(resp)
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
    MediaAttrs { kind, duration: num("duration"), width, height }
}

/// Pure token validation matching C++ ClientManager::send: structural problems and out-of-range ids -> 401,
/// ids rejected by `--filter <remainder>/<modulo>` -> 421.
pub fn validate_token(token: &str, filter: Option<(u64, u64)>) -> Result<(), TelegramError> {
    let invalid = || TelegramError::Unauthorized("invalid token specified".to_string());
    if token.is_empty() || token.starts_with('0') || token.len() > 80 || token.contains('/') {
        return Err(invalid());
    }
    let Some((id_str, _)) = token.split_once(':') else {
        return Err(invalid());
    };
    let forbidden = || TelegramError::MisdirectedRequest("forbidden token specified".to_string());
    let parsed = if id_str.starts_with('+') { None } else { id_str.parse::<i64>().ok() };
    let Some(id) = parsed else {
        return Err(forbidden());
    };
    if let Some((rem, modulo)) = filter {
        if modulo != 0 && (id as u64 % modulo) != rem {
            return Err(forbidden());
        }
    }
    if id <= 0 || id >= (1i64 << 54) {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod token_tests {
    use super::*;

    #[test]
    fn token_validation() {
        assert!(validate_token("123:abc", None).is_ok());
        assert!(matches!(validate_token("+123:abc", None), Err(TelegramError::MisdirectedRequest(_))));
        assert!(matches!(validate_token("abc", None), Err(TelegramError::Unauthorized(_))));
        assert!(matches!(validate_token("0123:abc", None), Err(TelegramError::Unauthorized(_))));
        assert!(matches!(validate_token("a/b:c", None), Err(TelegramError::Unauthorized(_))));
        assert!(matches!(validate_token("12x:abc", None), Err(TelegramError::MisdirectedRequest(_))));
        assert!(matches!(validate_token("123:abc", Some((0, 2))), Err(TelegramError::MisdirectedRequest(_))));
        assert!(validate_token("124:abc", Some((0, 2))).is_ok());
        assert!(matches!(validate_token("18014398509481984:a", None), Err(TelegramError::Unauthorized(_))));
    }
}

/// Returns the canonical path of `cand` if it is an existing regular file inside `root`, else None.
fn confine_path(root: &Path, cand: &Path) -> Option<PathBuf> {
    let root = std::fs::canonicalize(root).ok()?;
    let real = std::fs::canonicalize(cand).ok()?;
    if real.starts_with(&root) && real.is_file() {
        Some(real)
    } else {
        None
    }
}

/// True if the webhook URL is a plain-http URL whose host is loopback / private (served by the local worker).
pub fn is_local_webhook_url(url: &str) -> bool {
    let Ok(u) = reqwest::Url::parse(url) else {
        return false;
    };
    if u.scheme() != "http" {
        return false;
    }
    let Some(host) = u.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private(),
        Ok(std::net::IpAddr::V6(ip)) => ip.is_loopback(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod local_tests {
    use super::*;

    #[test]
    fn local_webhook_urls() {
        assert!(is_local_webhook_url("http://127.0.0.1:8080/x"));
        assert!(is_local_webhook_url("http://localhost/x"));
        assert!(is_local_webhook_url("http://[::1]:1/"));
        assert!(is_local_webhook_url("http://192.168.1.5/"));
        assert!(is_local_webhook_url("http://172.20.0.2/"));
        assert!(!is_local_webhook_url("http://172.32.0.2/"));
        assert!(!is_local_webhook_url("http://localhost.evil.com/"));
        assert!(!is_local_webhook_url("http://10.evil.com/"));
        assert!(!is_local_webhook_url("http://example.com/"));
        assert!(!is_local_webhook_url("https://127.0.0.1/"));
    }

    #[test]
    fn confine() {
        let base = std::env::temp_dir().join(format!("confine_{}", std::process::id()));
        let root = base.join("root");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("a.txt"), b"x").unwrap();
        std::fs::write(base.join("secret"), b"x").unwrap();
        assert!(confine_path(&root, &root.join("a.txt")).is_some());
        assert!(confine_path(&root, &root.join("../secret")).is_none());
        assert!(confine_path(&root, &base.join("secret")).is_none());
        let _ = std::fs::remove_dir_all(&base);
    }
}

#[cfg(test)]
mod media_attr_tests {
    use super::*;

    #[test]
    fn attrs_from_fields() {
        let m: serde_json::Map<String, Value> =
            serde_json::from_str(r#"{"duration":"7","width":640,"height":480}"#).unwrap();
        let a = media_attrs_for("sendVideo", Some(&m));
        assert_eq!((a.kind, a.duration, a.width, a.height), (MediaKind::Video, 7, 640, 480));
        assert_eq!(media_attrs_for("sendDocument", Some(&m)).kind, MediaKind::Document);
        assert_eq!(media_attrs_for("sendVoice", None).kind, MediaKind::Voice);
        assert_eq!(media_attrs_for("sendVideoNote", None).kind, MediaKind::VideoNote);
    }
}

#[cfg(test)]
mod active_bot_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// Mock upstream: ok for token "1:good", otherwise a 401 JSON error.
    async fn mock_upstream() -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (mut s, _) = l.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    let n = s.read(&mut buf).await.unwrap_or(0);
                    let head = String::from_utf8_lossy(&buf[..n]).to_string();
                    let body = if head.contains("/bot1:good/") {
                        r#"{"ok":true,"result":true}"#
                    } else {
                        r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#
                    };
                    let r = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = s.write_all(r.as_bytes()).await;
                    let _ = s.shutdown().await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn only_upstream_accepted_tokens_are_counted() {
        let port = mock_upstream().await;
        let dir = std::env::temp_dir().join(format!("activebot_{}", std::process::id()));
        let storage = crate::storage::StorageEngine::new(&dir).unwrap();
        let mut e = TelegramEngine::new(false, dir.clone(), dir.clone(), storage);
        e.upstream_base_url = format!("http://127.0.0.1:{port}");
        let e = Arc::new(e);
        let none = std::collections::HashMap::new();

        for i in 2..12 {
            let tok = format!("{i}:fake");
            let r = e.universal_call(&tok, "getChat", &none, None, None, Bytes::new()).await.unwrap();
            assert!(String::from_utf8_lossy(&r).contains("401"));
        }
        assert_eq!(e.active_bots.len(), 0, "fake tokens must not be recorded");

        e.universal_call("1:good", "getChat", &none, None, None, Bytes::new()).await.unwrap();
        assert_eq!(e.active_bots.len(), 1);

        // A later 401 for a recorded token removes it again.
        e.record_active_bot("7:revoked");
        assert_eq!(e.active_bots.len(), 2);
        e.universal_call("7:revoked", "getChat", &none, None, None, Bytes::new()).await.unwrap();
        assert_eq!(e.active_bots.len(), 1);

        // get_me with a bad token errors and is not counted.
        assert!(e.get_me("9:bad").await.is_err());
        assert_eq!(e.active_bots.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
