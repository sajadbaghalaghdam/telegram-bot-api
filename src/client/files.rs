//! Local files: path resolution/confinement, getFile (with local download) and file streaming.

use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use super::engine::{TelegramEngine, STREAM_BUFFER_SIZE};
use crate::error::{TelegramError, TelegramResult};
use crate::models::{ApiResponse, File};

impl TelegramEngine {
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

    /// Execute getFile with SQLite WAL lookup and chunked disk write in local mode
    pub async fn get_file(&self, token: &str, file_id: &str) -> TelegramResult<File> {
        if file_id.is_empty() {
            return Err(TelegramError::BadRequest("file_id not specified".to_string()));
        }

        if self.local_mode {
            if let Some(cached) = self.storage.get_cached_file(&file_id).await {
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
            .query(&[("file_id", &file_id)])
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
                    &file_id,
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

                let unique_id = format!("AgAD{:x}", file_id.len());
                let file = File {
                    file_id: file_id.to_owned(),
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

#[cfg(test)]
mod tests {
    use super::*;

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
