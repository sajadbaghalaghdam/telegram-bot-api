//! Local files: path resolution/confinement, getFile (with local download) and file streaming.
//! Filesystem probing runs on the blocking pool, never on a runtime worker.

use std::borrow::Cow;
use std::path::{Path, PathBuf};

use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio_util::io::ReaderStream;

use super::engine::{TelegramEngine, STREAM_BUFFER_SIZE};
use super::upstream::send_json;
use super::util::TempFile;
use crate::error::{TelegramError, TelegramResult};
use crate::models::{ApiResponse, File};

/// Non-local servers refuse files above 20 MB (C++ Client.cpp:17042).
const MAX_NON_LOCAL_SIZE: i64 = 20 * 1024 * 1024;

impl TelegramEngine {
    /// Directory name of a bot below the working directory.
    pub fn bot_dir_name(token: &str) -> Cow<'_, str> {
        if cfg!(unix) {
            Cow::Borrowed(token)
        } else {
            Cow::Owned(token.replace(':', "_"))
        }
    }

    /// Resolve a file input string if local_mode is enabled and the path exists on disk
    /// Matches C++ Client::get_local_file_path in telegram-bot-api/Client.cpp:10763
    pub async fn resolve_local_path(&self, file_input: &str) -> Option<PathBuf> {
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

        let direct = PathBuf::from(cleaned);
        let relative = self.dir.join(cleaned);
        tokio::task::spawn_blocking(move || {
            if direct.is_file() {
                Some(direct)
            } else if relative.is_file() {
                Some(relative)
            } else {
                None
            }
        })
        .await
        .ok()
        .flatten()
    }

    /// The path reported to the client in local mode: absolute, canonical when the file exists.
    async fn absolute_path_string(&self, path: &Path) -> String {
        let abs = match fs::canonicalize(path).await {
            Ok(p) => p,
            Err(_) if path.is_absolute() => path.to_path_buf(),
            Err(_) => self.dir.join(path),
        };
        abs.to_string_lossy().into_owned()
    }

    /// Execute getFile with SQLite WAL lookup and chunked disk write in local mode
    pub async fn get_file(&self, token: &str, file_id: &str) -> TelegramResult<File> {
        if file_id.is_empty() {
            return Err(TelegramError::BadRequest("file_id not specified".to_string()));
        }

        if self.local_mode
            && let Some(mut cached) = self.storage.get_cached_file(file_id).await
            && let Some(p) = cached.file_path.as_deref()
        {
            let check_path = if Path::new(p).is_absolute() {
                PathBuf::from(p)
            } else {
                self.dir.join(p)
            };
            if let Ok(abs) = fs::canonicalize(&check_path).await {
                cached.file_path = Some(abs.to_string_lossy().into_owned());
                return Ok(cached);
            }
        }

        let url = format!("{}/bot{token}/getFile", self.upstream_base_url);
        let resp: ApiResponse<File> = send_json(self.client.get(&url).query(&[("file_id", file_id)])).await?;

        if !resp.ok {
            let too_big = resp
                .description
                .as_deref()
                .is_some_and(|d| d.to_ascii_lowercase().contains("too big"));
            if self.local_mode && too_big {
                return self.download_big_file(token, file_id).await;
            }
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp.description.unwrap_or_else(|| "File not found".to_string()),
                parameters: resp.parameters,
            });
        }

        let mut file = resp
            .result
            .ok_or_else(|| TelegramError::BadRequest("File not found".to_string()))?;

        // In non-local mode, reject files > 20MB matching C++ Client.cpp:17042
        if !self.local_mode && file.file_size.is_some_and(|size| size > MAX_NON_LOCAL_SIZE) {
            return Err(TelegramError::BadRequest("Bad Request: file is too big".to_string()));
        }

        if self.local_mode
            && let Some(remote_path) = file.file_path.as_deref()
        {
            let target = self.dir.join(&*Self::bot_dir_name(token)).join(remote_path);
            if let Some(parent) = target.parent() {
                let _ = fs::create_dir_all(parent).await;
            }
            if !fs::try_exists(&target).await.unwrap_or(false) {
                self.download_file(token, remote_path, &target).await;
            }
            if let Ok(abs) = fs::canonicalize(&target).await {
                file.file_path = Some(abs.to_string_lossy().into_owned());
                self.storage.save_cached_file(&file).await;
            }
        }

        Ok(file)
    }

    /// `--local` getFile of a file too big for the HTTP Bot API: downloaded through MTProto.
    async fn download_big_file(&self, token: &str, file_id: &str) -> TelegramResult<File> {
        let target_dir = self.dir.join(&*Self::bot_dir_name(token));
        let (target_file, total_bytes) = crate::mtproto::download_large_file_via_mtproto(
            self.api_id,
            &self.api_hash,
            token,
            file_id,
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

        let file = File {
            file_id: file_id.to_owned(),
            file_unique_id: format!("AgAD{:x}", file_id.len()),
            file_size: Some(total_bytes as i64),
            file_path: Some(self.absolute_path_string(&target_file).await),
        };
        self.storage.save_cached_file(&file).await;
        Ok(file)
    }

    /// Downloads `/file/bot<token>/<remote_path>` to `target`. The data goes to a temp file next to
    /// the target that is renamed into place only when the whole body arrived, so an interrupted
    /// or concurrent download can never leave (or serve) a truncated file.
    async fn download_file(&self, token: &str, remote_path: &str, target: &Path) {
        let url = format!("{}/file/bot{token}/{remote_path}", self.upstream_base_url);
        let Ok(mut resp) = self.client.get(&url).send().await else { return };
        if !resp.status().is_success() {
            return;
        }
        let part = TempFile::new(target.with_extension(format!("{:016x}.part", rand::random::<u64>())));
        let Ok(out) = fs::File::create(part.path()).await else { return };
        let mut writer = tokio::io::BufWriter::with_capacity(STREAM_BUFFER_SIZE, out);
        loop {
            match resp.chunk().await {
                Ok(Some(chunk)) => {
                    if writer.write_all(&chunk).await.is_err() {
                        return;
                    }
                }
                Ok(None) => break,
                Err(_) => return,
            }
        }
        if writer.flush().await.is_ok() {
            let _ = fs::rename(part.path(), target).await;
        }
    }

    /// Opens a served file inside the bot's own working directory (local mode only) and returns it
    /// with its size. Absolute paths (as returned by getFile in local mode) are accepted only if
    /// they stay within `<dir>/<token>`; `..` traversal and symlink escapes are rejected.
    /// Resolution, open and stat share one blocking-pool hop.
    pub async fn open_local_file(&self, token: &str, file_path: &str) -> Option<(fs::File, u64)> {
        if !self.local_mode {
            return None;
        }
        let roots = [
            self.dir.join(&*Self::bot_dir_name(token)),
            self.dir.join(token.replace(':', "_")),
        ];
        let requested = PathBuf::from(file_path);
        tokio::task::spawn_blocking(move || {
            let path = roots.iter().find_map(|root| {
                let cand = if requested.is_absolute() {
                    requested.clone()
                } else {
                    root.join(&requested)
                };
                confine_path(root, &cand)
            })?;
            let file = std::fs::File::open(path).ok()?;
            let len = file.metadata().ok()?.len();
            Some((fs::File::from_std(file), len))
        })
        .await
        .ok()
        .flatten()
    }

    /// Streams a file for download: from the local disk if present (local mode), else from upstream.
    pub async fn open_file_stream(
        &self,
        token: &str,
        file_path: &str,
    ) -> Result<(axum::body::Body, Option<u64>), TelegramError> {
        if let Some((file, len)) = self.open_local_file(token, file_path).await {
            let stream = ReaderStream::with_capacity(file, STREAM_BUFFER_SIZE);
            return Ok((axum::body::Body::from_stream(stream), Some(len)));
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
