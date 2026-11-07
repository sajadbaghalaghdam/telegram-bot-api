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
use tokio::sync::Semaphore;
use tokio::task::AbortHandle;
use tokio_util::io::ReaderStream;

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

    #[inline]
    pub fn active_bots_count(&self) -> usize {
        self.active_bots
            .len()
            .max(self.webhook_workers.len())
            .max(self.storage.cached_bots_count())
    }

    /// Checks `--filter <remainder>/<modulo>` matching C++ ClientManager::get_client (`bot_user_id % modulo == remainder`)
    #[inline]
    pub fn check_token_filter(&self, token: &str) -> Result<(), TelegramError> {
        if let Some((rem, modulo)) = self.token_filter {
            if let Some((id_str, _)) = token.split_once(':') {
                if let Ok(bot_id) = id_str.parse::<u64>() {
                    if bot_id % modulo != rem {
                        return Err(TelegramError::NotFound(
                            "Bot is filtered out by --filter option".to_string(),
                        ));
                    }
                }
            }
        }
        Ok(())
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

    /// Silently restores active webhooks from TDLib's `webhooks_db.binlog` and SQLite WAL on startup
    pub async fn restore_webhooks(self: &Arc<Self>) {
        // 1. First load from TDLib's legacy webhooks_db.binlog if present
        let binlog_path = self.dir.join("webhooks_db.binlog");
        if binlog_path.exists() {
            if let Ok(bytes) = fs::read(&binlog_path).await {
                let pairs = Self::parse_webhooks_binlog(&bytes);
                for (token, url) in pairs {
                    if !url.is_empty() {
                        self.storage
                            .save_webhook(&token, &url, None, 100)
                            .await;
                    }
                }
            }
        }

        // 2. Load all persisted webhooks from SQLite WAL
        let saved = self.storage.get_all_webhooks().await;
        let count = saved.len();
        for (token, url) in saved {
            if !url.is_empty() {
                self.start_webhook_worker(token, url);
            }
        }
        if count > 0 {
            tracing::info!("Restored {} active webhook configurations from SQLite WAL", count);
        }
    }

    /// Parses TDLib `webhooks_db.binlog` binary records to extract the latest `(token, webhook_url)` per bot
    fn parse_webhooks_binlog(bytes: &[u8]) -> std::collections::HashMap<String, String> {
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

    /// Starts or replaces a background long-polling -> local webhook delivery worker for a bot
    pub fn start_webhook_worker(self: &Arc<Self>, token: String, webhook_url: String) {
        self.stop_webhook_worker(&token);
        if webhook_url.is_empty() {
            return;
        }
        self.record_active_bot(&token);

        let engine = Arc::clone(self);
        let tok_clone = token.clone();
        let handle = tokio::spawn(async move {
            let sem = Arc::new(Semaphore::new(100));
            let mut next_offset: i64 = 0;

            // 1. Initial sync: skip already-processed historical updates using offset=-1&limit=1&timeout=0
            let init_url = format!(
                "{}/bot{}/getUpdates?offset=-1&limit=1&timeout=0",
                engine.upstream_base_url, tok_clone
            );
            if let Ok(resp) = engine.client.get(&init_url).send().await {
                if let Ok(json) = resp.json::<Value>().await {
                    if let Some(arr) = json.get("result").and_then(|v| v.as_array()) {
                        if let Some(last) = arr.last() {
                            if let Some(uid) = last.get("update_id").and_then(|v| v.as_i64()) {
                                next_offset = uid + 1;
                            }
                        }
                    }
                }
            }

            let allowed_val: Value = serde_json::from_str(
                r#"["message","edited_message","callback_query","inline_query","message_reaction","channel_post","edited_channel_post","my_chat_member","chat_member","chat_join_request"]"#,
            )
            .unwrap_or(Value::Null);

            loop {
                let poll_url = format!(
                    "{}/bot{}/getUpdates",
                    engine.upstream_base_url, tok_clone
                );
                let body = serde_json::json!({
                    "offset": next_offset,
                    "limit": 100,
                    "timeout": 20,
                    "allowed_updates": allowed_val,
                });

                match engine.client.post(&poll_url).json(&body).send().await {
                    Ok(resp) => {
                        if let Ok(json) = resp.json::<Value>().await {
                            if json.get("ok").and_then(|v| v.as_bool()) == Some(true) {
                                if let Some(arr) = json.get("result").and_then(|v| v.as_array()) {
                                    for update in arr {
                                        if let Some(uid) = update.get("update_id").and_then(|v| v.as_i64()) {
                                            if uid >= next_offset {
                                                next_offset = uid + 1;
                                            }
                                        }
                                        let update_bytes = serde_json::to_vec(update).unwrap_or_default();
                                        let wh_client = engine.webhook_client.clone();
                                        let wh_url = webhook_url.clone();
                                        let sem_clone = Arc::clone(&sem);

                                        tokio::spawn(async move {
                                            let _permit = sem_clone.acquire().await;
                                            let _ = wh_client
                                                .post(&wh_url)
                                                .header("content-type", "application/json")
                                                .body(update_bytes)
                                                .send()
                                                .await;
                                        });
                                    }
                                }
                            } else {
                                let code = json.get("error_code").and_then(|v| v.as_i64()).unwrap_or(0);
                                if code == 409 {
                                    // Conflict: a remote webhook was still set on Telegram cloud; delete it so local polling works
                                    let del_url = format!("{}/bot{}/deleteWebhook", engine.upstream_base_url, tok_clone);
                                    let _ = engine.client.post(&del_url).send().await;
                                } else if code == 401 || code == 404 {
                                    // Invalid token, stop polling
                                    break;
                                }
                                tokio::time::sleep(Duration::from_secs(2)).await;
                            }
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });

        self.webhook_workers.insert(token, handle.abort_handle());
    }

    pub fn stop_webhook_worker(&self, token: &str) {
        if let Some((_, handle)) = self.webhook_workers.remove(token) {
            handle.abort();
        }
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
        chat_id_str: &str,
        local_path: &Path,
        caption: &str,
        custom_file_name: Option<&str>,
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
        let uploaded = crate::mtproto::upload_and_send_large_file(
            self.api_id,
            &self.api_hash,
            token,
            chat_id_str,
            hint_username.as_deref(),
            local_path,
            caption,
            custom_file_name,
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

        // 4. Build standard Bot API Message JSON response
        let mut msg_obj = serde_json::json!({
            "message_id": uploaded.message_id,
            "chat": chat_json,
            "date": uploaded.date,
            "document": {
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
                chat_id_str,
                &temp_path,
                caption,
                Some(&url_file_name),
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
        self.record_active_bot(token);
        if let Some(cached_user) = self.storage.get_bot(token).await {
            return Ok((*cached_user).clone());
        }

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

        let user = resp.result.ok_or_else(|| {
            TelegramError::Unauthorized(
                resp.description.unwrap_or_else(|| "Unauthorized".to_string()),
            )
        })?;

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
        if self.local_mode {
            if let Some(cached) = self.storage.get_cached_file(&req.file_id).await {
                if let Some(ref p) = cached.file_path {
                    if Path::new(p).exists() {
                        return Ok(cached);
                    }
                }
            }
        }

        let url = format!("{}/bot{token}/getFile", self.upstream_base_url);
        let resp: ApiResponse<File> = self
            .client
            .post(&url)
            .json(req)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        let mut file = resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "File not found".to_string()),
            )
        })?;

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
                    file.file_path = Some(target_file.to_string_lossy().to_string());
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

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Execute setWebhook and persist to SQLite WAL + start local webhook worker if local URL
    pub async fn set_webhook(
        self: &Arc<Self>,
        token: &str,
        req: &SetWebhookRequest,
    ) -> TelegramResult<bool> {
        let is_local_url = req.url.starts_with("http://127.0.0.1")
            || req.url.starts_with("http://localhost")
            || req.url.starts_with("http://10.")
            || req.url.starts_with("http://192.168.")
            || req.url.starts_with("http://");

        if is_local_url {
            // Ensure cloud webhook is deleted so local getUpdates worker can receive updates
            let del_url = format!("{}/bot{token}/deleteWebhook", self.upstream_base_url);
            let _ = self.client.post(&del_url).send().await;
            self.storage
                .save_webhook(
                    token,
                    &req.url,
                    req.secret_token.as_deref(),
                    req.max_connections.unwrap_or(100),
                )
                .await;
            self.start_webhook_worker(token.to_string(), req.url.clone());
            return Ok(true);
        }

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

        let ok = resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })?;

        if ok {
            self.storage
                .save_webhook(
                    token,
                    &req.url,
                    req.secret_token.as_deref(),
                    req.max_connections.unwrap_or(40),
                )
                .await;
        }
        Ok(ok)
    }

    /// Execute deleteWebhook and remove from SQLite WAL + stop local webhook worker
    pub async fn delete_webhook(&self, token: &str) -> TelegramResult<bool> {
        self.stop_webhook_worker(token);
        self.storage.delete_webhook(token).await;

        let url = format!("{}/bot{token}/deleteWebhook", self.upstream_base_url);
        let resp: ApiResponse<bool> = self
            .client
            .post(&url)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        Ok(resp.result.unwrap_or(true))
    }

    /// Execute getWebhookInfo
    pub async fn get_webhook_info(&self, token: &str) -> TelegramResult<WebhookInfo> {
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

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }

    /// Resolves a local file for zero-copy streaming if present on disk, or returns an upstream response stream
    pub async fn open_file_stream(
        &self,
        token: &str,
        file_path: &str,
    ) -> Result<(axum::body::Body, Option<u64>), TelegramError> {
        if self.local_mode {
            let bot_folder = Self::bot_dir_name(token);
            let local_candidate = self.dir.join(&bot_folder).join(file_path);
            if local_candidate.exists() && local_candidate.is_file() {
                let file = fs::File::open(&local_candidate).await.map_err(|e| {
                    TelegramError::Internal(format!("Failed to open local file: {e}"))
                })?;
                let len = file.metadata().await.ok().map(|m| m.len());
                let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
                return Ok((axum::body::Body::from_stream(stream), len));
            }

            let sanitized_candidate = self.dir.join(token.replace(':', "_")).join(file_path);
            if sanitized_candidate.exists() && sanitized_candidate.is_file() {
                let file = fs::File::open(&sanitized_candidate).await.map_err(|e| {
                    TelegramError::Internal(format!("Failed to open local file: {e}"))
                })?;
                let len = file.metadata().await.ok().map(|m| m.len());
                let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
                return Ok((axum::body::Body::from_stream(stream), len));
            }

            let direct_candidate = PathBuf::from(file_path);
            if direct_candidate.exists() && direct_candidate.is_file() {
                let file = fs::File::open(&direct_candidate).await.map_err(|e| {
                    TelegramError::Internal(format!("Failed to open file: {e}"))
                })?;
                let len = file.metadata().await.ok().map(|m| m.len());
                let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
                return Ok((axum::body::Body::from_stream(stream), len));
            }
        }

        let url = format!("{}/file/bot{token}/{file_path}", self.upstream_base_url);
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("File download network error: {e}")))?;

        if !resp.status().is_success() {
            return Err(TelegramError::BadRequest(format!(
                "File download failed: status {}",
                resp.status()
            )));
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
        self.record_active_bot(token);
        let method_lower = method.to_ascii_lowercase();

        // 1. Handle setWebhook / deleteWebhook / getWebhookInfo / getFile specially
        if method_lower == "setwebhook" {
            let mut url_val = query_map.get("url").cloned().unwrap_or_default();
            let max_conn = query_map
                .get("max_connections")
                .and_then(|v| v.parse::<i32>().ok())
                .or(Some(100));

            if url_val.is_empty() && !body_bytes.is_empty() {
                if let Ok(json) = serde_json::from_slice::<Value>(&body_bytes) {
                    if let Some(u) = json.get("url").and_then(|v| v.as_str()) {
                        url_val = u.to_string();
                    }
                }
            }
            let req = SetWebhookRequest {
                url: url_val,
                ip_address: None,
                max_connections: max_conn,
                allowed_updates: None,
                drop_pending_updates: None,
                secret_token: None,
            };
            let _ = self.set_webhook(token, &req).await?;
            return Ok(Bytes::from_static(
                br#"{"ok":true,"result":true,"description":"Webhook was set"}"#,
            ));
        }

        if method_lower == "deletewebhook" {
            let _ = self.delete_webhook(token).await?;
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
            }
            if !file_id.is_empty() {
                match self.get_file(token, &GetFileRequest { file_id }).await {
                    Ok(file) => {
                        let resp = ApiResponse::success(file);
                        let vec = serde_json::to_vec(&resp).unwrap_or_default();
                        return Ok(Bytes::from(vec));
                    }
                    Err(e) => {
                        let err_json = serde_json::json!({
                            "ok": false,
                            "error_code": 400,
                            "description": e.to_string()
                        });
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
                                .maybe_upload_large_remote_url_mtproto(token, &chat_id_str, s, caption)
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
                            .upload_local_file_mtproto(token, &chat_id_str, &local_path, caption, None)
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
                                .upload_local_file_mtproto(token, &chat_id_str, &local_path, caption, None)
                                .await;
                        }
                    } else if val.starts_with("http://") || val.starts_with("https://") {
                        if let Some(res) = self
                            .maybe_upload_large_remote_url_mtproto(token, &chat_id_str, val, caption)
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
