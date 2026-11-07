use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use rusqlite::{params, Connection, OpenFlags};

use crate::error::{TelegramError, TelegramResult};
use crate::models::{File, User};

const MAX_L1_CACHE_ENTRIES: usize = 4096;
const READER_POOL_SIZE: usize = 4;

/// High-performance Async SQLite WAL Storage Engine with bounded L1 DashMap cache.
/// Separates 1 WAL Writer from N concurrent WAL Readers to maximize concurrency and minimize RAM.
pub struct StorageEngine {
    writer: Arc<Mutex<Connection>>,
    readers: Vec<Arc<Mutex<Connection>>>,
    reader_idx: AtomicUsize,
    l1_bots: DashMap<String, Arc<User>>,
    l1_files: DashMap<String, File>,
}

impl StorageEngine {
    /// Initializes the SQLite database in WAL mode with strict memory caps.
    pub fn new(db_dir: &Path) -> TelegramResult<Arc<Self>> {
        std::fs::create_dir_all(db_dir).map_err(|e| {
            TelegramError::Internal(format!("Failed to create storage directory: {e}"))
        })?;

        let db_path: PathBuf = db_dir.join("bot_api_state.sqlite3");

        let writer = Self::open_connection(&db_path, false)?;
        Self::init_schema(&writer)?;

        let mut readers = Vec::with_capacity(READER_POOL_SIZE);
        for _ in 0..READER_POOL_SIZE {
            let reader = Self::open_connection(&db_path, true)?;
            readers.push(Arc::new(Mutex::new(reader)));
        }

        Ok(Arc::new(Self {
            writer: Arc::new(Mutex::new(writer)),
            readers,
            reader_idx: AtomicUsize::new(0),
            l1_bots: DashMap::with_capacity(128),
            l1_files: DashMap::with_capacity(512),
        }))
    }

    fn open_connection(path: &Path, read_only: bool) -> TelegramResult<Connection> {
        let flags = if read_only {
            OpenFlags::SQLITE_OPEN_READ_ONLY
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI
        } else {
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX
                | OpenFlags::SQLITE_OPEN_URI
        };

        let conn = Connection::open_with_flags(path, flags)
            .map_err(|e| TelegramError::Internal(format!("SQLite open error: {e}")))?;

        // Low-RAM, high-speed WAL tuning
        conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             PRAGMA temp_store = MEMORY;
             PRAGMA cache_size = -2000;
             PRAGMA mmap_size = 16777216;
             PRAGMA busy_timeout = 5000;",
        )
        .map_err(|e| TelegramError::Internal(format!("SQLite pragma error: {e}")))?;

        Ok(conn)
    }

    fn init_schema(conn: &Connection) -> TelegramResult<()> {
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS bots (
                token TEXT PRIMARY KEY,
                bot_id INTEGER NOT NULL,
                username TEXT,
                first_name TEXT NOT NULL,
                user_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS file_cache (
                file_id TEXT PRIMARY KEY,
                file_unique_id TEXT NOT NULL,
                file_size INTEGER,
                local_path TEXT NOT NULL,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS webhooks (
                token TEXT PRIMARY KEY,
                url TEXT NOT NULL,
                secret_token TEXT,
                max_connections INTEGER NOT NULL DEFAULT 40,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_file_cache_updated ON file_cache(updated_at);",
        )
        .map_err(|e| TelegramError::Internal(format!("SQLite schema init error: {e}")))?;

        Ok(())
    }

    #[inline]
    fn next_reader(&self) -> Arc<Mutex<Connection>> {
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        Arc::clone(&self.readers[idx])
    }

    #[inline]
    fn now_unix() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64
    }

    /// Retrieves cached Bot User profile from L1 DashMap or SQLite WAL reader pool.
    pub async fn get_bot(&self, token: &str) -> Option<Arc<User>> {
        if let Some(cached) = self.l1_bots.get(token) {
            return Some(Arc::clone(cached.value()));
        }

        let reader = self.next_reader();
        let token_owned = token.to_string();

        let user_opt = tokio::task::spawn_blocking(move || {
            let conn = reader.lock().ok()?;
            let mut stmt = conn
                .prepare_cached("SELECT user_json FROM bots WHERE token = ?1")
                .ok()?;
            let json_str: String = stmt.query_row(params![token_owned], |row| row.get(0)).ok()?;
            serde_json::from_str::<User>(&json_str).ok()
        })
        .await
        .ok()
        .flatten()?;

        let arc_user = Arc::new(user_opt);
        if self.l1_bots.len() >= MAX_L1_CACHE_ENTRIES {
            self.l1_bots.clear();
        }
        self.l1_bots.insert(token.to_string(), Arc::clone(&arc_user));
        Some(arc_user)
    }

    /// Persists Bot User profile into L1 DashMap and SQLite WAL asynchronously.
    pub async fn save_bot(&self, token: &str, user: &User) {
        let arc_user = Arc::new(user.clone());
        if self.l1_bots.len() >= MAX_L1_CACHE_ENTRIES {
            self.l1_bots.clear();
        }
        self.l1_bots.insert(token.to_string(), Arc::clone(&arc_user));

        let writer = Arc::clone(&self.writer);
        let token_owned = token.to_string();
        let bot_id = user.id;
        let username = user.username.clone();
        let first_name = user.first_name.clone();
        let user_json = match serde_json::to_string(user) {
            Ok(j) => j,
            Err(_) => return,
        };
        let now = Self::now_unix();

        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(conn) = writer.lock() {
                let _ = conn.execute(
                    "INSERT INTO bots (token, bot_id, username, first_name, user_json, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(token) DO UPDATE SET
                        user_json = excluded.user_json,
                        updated_at = excluded.updated_at",
                    params![token_owned, bot_id, username, first_name, user_json, now],
                );
            }
        })
        .await;
    }

    /// Retrieves cached File metadata from L1 DashMap or SQLite WAL reader pool if local file still exists.
    pub async fn get_cached_file(&self, file_id: &str) -> Option<File> {
        if let Some(cached) = self.l1_files.get(file_id) {
            let f = cached.value().clone();
            if let Some(ref p) = f.file_path {
                if Path::new(p).exists() {
                    return Some(f);
                }
            }
        }

        let reader = self.next_reader();
        let file_id_owned = file_id.to_string();

        let file_opt = tokio::task::spawn_blocking(move || {
            let conn = reader.lock().ok()?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT file_id, file_unique_id, file_size, local_path FROM file_cache WHERE file_id = ?1",
                )
                .ok()?;
            stmt.query_row(params![file_id_owned], |row| {
                Ok(File {
                    file_id: row.get(0)?,
                    file_unique_id: row.get(1)?,
                    file_size: row.get(2)?,
                    file_path: Some(row.get(3)?),
                })
            })
            .ok()
        })
        .await
        .ok()
        .flatten()?;

        if let Some(ref p) = file_opt.file_path {
            if Path::new(p).exists() {
                if self.l1_files.len() >= MAX_L1_CACHE_ENTRIES {
                    self.l1_files.clear();
                }
                self.l1_files.insert(file_id.to_string(), file_opt.clone());
                return Some(file_opt);
            }
        }

        None
    }

    /// Saves resolved local file metadata to L1 cache and SQLite WAL.
    pub async fn save_cached_file(&self, file: &File) {
        let Some(ref local_path) = file.file_path else {
            return;
        };

        if self.l1_files.len() >= MAX_L1_CACHE_ENTRIES {
            self.l1_files.clear();
        }
        self.l1_files.insert(file.file_id.clone(), file.clone());

        let writer = Arc::clone(&self.writer);
        let file_id = file.file_id.clone();
        let file_unique_id = file.file_unique_id.clone();
        let file_size = file.file_size;
        let path_owned = local_path.clone();
        let now = Self::now_unix();

        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(conn) = writer.lock() {
                let _ = conn.execute(
                    "INSERT INTO file_cache (file_id, file_unique_id, file_size, local_path, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(file_id) DO UPDATE SET
                        local_path = excluded.local_path,
                        file_size = excluded.file_size,
                        updated_at = excluded.updated_at",
                    params![file_id, file_unique_id, file_size, path_owned, now],
                );
            }
        })
        .await;
    }

    /// Saves webhook configuration in SQLite WAL.
    pub async fn save_webhook(&self, token: &str, url: &str, secret_token: Option<&str>, max_conn: i32) {
        let writer = Arc::clone(&self.writer);
        let token_owned = token.to_string();
        let url_owned = url.to_string();
        let secret_owned = secret_token.map(|s| s.to_string());
        let now = Self::now_unix();

        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(conn) = writer.lock() {
                let _ = conn.execute(
                    "INSERT INTO webhooks (token, url, secret_token, max_connections, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(token) DO UPDATE SET
                        url = excluded.url,
                        secret_token = excluded.secret_token,
                        max_connections = excluded.max_connections,
                        updated_at = excluded.updated_at",
                    params![token_owned, url_owned, secret_owned, max_conn, now],
                );
            }
        })
        .await;
    }

    /// Deletes webhook configuration from SQLite WAL.
    pub async fn delete_webhook(&self, token: &str) {
        let writer = Arc::clone(&self.writer);
        let token_owned = token.to_string();

        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(conn) = writer.lock() {
                let _ = conn.execute("DELETE FROM webhooks WHERE token = ?1", params![token_owned]);
            }
        })
        .await;
    }

    /// Loads all saved `(token, url)` webhook pairs from SQLite WAL on startup.
    pub async fn get_all_webhooks(&self) -> Vec<(String, String)> {
        let reader = Arc::clone(&self.readers[0]);
        tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            if let Ok(conn) = reader.lock() {
                if let Ok(mut stmt) = conn.prepare("SELECT token, url FROM webhooks") {
                    if let Ok(rows) = stmt.query_map([], |row| {
                        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                    }) {
                        for r in rows.flatten() {
                            out.push(r);
                        }
                    }
                }
            }
            out
        })
        .await
        .unwrap_or_default()
    }

    /// Returns active cached bots count.
    #[inline]
    pub fn cached_bots_count(&self) -> usize {
        self.l1_bots.len()
    }
}
