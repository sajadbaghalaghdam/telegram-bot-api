use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use rusqlite::{params, Connection, OpenFlags};
use tokio::sync::{mpsc, oneshot};

use crate::error::{TelegramError, TelegramResult};
use crate::models::{File, User};

const MAX_L1_CACHE_ENTRIES: usize = 2048;
const READER_POOL_SIZE: usize = 4;
/// Cached getMe profiles older than this are re-fetched from upstream.
const BOT_PROFILE_TTL_SECS: i64 = 3600;
/// An unchanged profile saved again within this window is not written at all (DB row is already current).
const SAVE_SKIP_SECS: i64 = 60;
/// Writes queued for the dedicated SQLite writer thread; senders wait (back-pressure) when it is full.
const WRITE_QUEUE: usize = 256;
/// Most queued writes applied in one transaction.
const MAX_BATCH: usize = 64;
/// `bots` / `file_cache` rows not refreshed for this long are unused and purged (startup, then daily).
/// Active bots are refreshed at least every `BOT_PROFILE_TTL_SECS`, so this never touches live rows.
const ROW_RETENTION_SECS: i64 = 30 * 24 * 3600;
const PURGE_INTERVAL: Duration = Duration::from_secs(24 * 3600);

/// A write executed by the single writer thread; the sender awaits the acknowledgement.
enum WriteOp {
    UpsertBot { token: String, bot_id: i64, username: Option<String>, first_name: String, user_json: String, now: i64 },
    /// Profile unchanged: only refresh the freshness timestamp.
    TouchBot { token: String, now: i64 },
    UpsertFile { file_id: String, file_unique_id: String, file_size: Option<i64>, local_path: String, now: i64 },
    UpsertWebhook { token: String, record: WebhookRecord, now: i64 },
    DeleteWebhook { token: String },
}

type Job = (WriteOp, oneshot::Sender<()>);

/// High-performance Async SQLite WAL Storage Engine with bounded L1 DashMap cache.
/// All writes go through ONE dedicated writer thread (bounded queue, batched into transactions), so
/// writes never occupy Tokio's blocking pool; reads use a small pool of read-only WAL connections.
pub struct StorageEngine {
    writes: mpsc::Sender<Job>,
    readers: Vec<Arc<Mutex<Connection>>>,
    reader_idx: AtomicUsize,
    l1_bots: DashMap<String, (Arc<User>, i64)>,
    l1_files: DashMap<String, File>,
}

#[derive(Debug, Clone)]
pub struct WebhookRecord {
    pub url: String,
    pub secret_token: Option<String>,
    pub max_connections: i32,
    pub allowed_updates: Option<Vec<String>>,
    pub ip_address: Option<String>,
}

impl StorageEngine {
    /// Initializes the SQLite database in WAL mode with strict memory caps and starts the writer thread
    /// (it exits when the engine is dropped, after applying everything still queued).
    pub fn new(db_dir: &Path) -> TelegramResult<Arc<Self>> {
        std::fs::create_dir_all(db_dir).map_err(|e| {
            TelegramError::Internal(format!("Failed to create storage directory: {e}"))
        })?;

        let db_path: PathBuf = db_dir.join("bot_api_state.sqlite3");

        let writer = Self::open_connection(&db_path, false)?;
        Self::init_schema(&writer)?;
        Self::purge_expired(&writer);

        let mut readers = Vec::with_capacity(READER_POOL_SIZE);
        for _ in 0..READER_POOL_SIZE {
            let reader = Self::open_connection(&db_path, true)?;
            readers.push(Arc::new(Mutex::new(reader)));
        }

        let (tx, rx) = mpsc::channel(WRITE_QUEUE);
        std::thread::Builder::new()
            .name("sqlite-writer".into())
            .spawn(move || Self::writer_loop(writer, rx))
            .map_err(|e| TelegramError::Internal(format!("Failed to spawn SQLite writer thread: {e}")))?;

        Ok(Arc::new(Self {
            writes: tx,
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
             PRAGMA cache_size = -500;
             PRAGMA mmap_size = 0;
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
                allowed_updates TEXT,
                ip_address TEXT,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_file_cache_updated ON file_cache(updated_at);
            CREATE INDEX IF NOT EXISTS idx_bots_updated ON bots(updated_at);",
        )
        .map_err(|e| TelegramError::Internal(format!("SQLite schema init error: {e}")))?;

        // Ensure columns exist for upgraded databases (only ignore "already exists", surface real errors)
        for (col, ddl) in [
            ("allowed_updates", "ALTER TABLE webhooks ADD COLUMN allowed_updates TEXT"),
            ("ip_address", "ALTER TABLE webhooks ADD COLUMN ip_address TEXT"),
        ] {
            if !Self::column_exists(conn, "webhooks", col)? {
                conn.execute(ddl, [])
                    .map_err(|e| TelegramError::Internal(format!("SQLite migration error: {e}")))?;
            }
        }

        Ok(())
    }

    fn column_exists(conn: &Connection, table: &str, column: &str) -> TelegramResult<bool> {
        let mut stmt = conn
            .prepare(&format!("PRAGMA table_info({table})"))
            .map_err(|e| TelegramError::Internal(format!("SQLite schema error: {e}")))?;
        let names = stmt
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(|e| TelegramError::Internal(format!("SQLite schema error: {e}")))?;
        Ok(names.flatten().any(|n| n == column))
    }

    /// Deletes bot / file-cache rows nobody has refreshed for `ROW_RETENTION_SECS`.
    /// Webhook rows are configuration and are never purged.
    fn purge_expired(conn: &Connection) {
        let cutoff = Self::now_unix() - ROW_RETENTION_SECS;
        for sql in ["DELETE FROM bots WHERE updated_at < ?1", "DELETE FROM file_cache WHERE updated_at < ?1"] {
            match conn.execute(sql, params![cutoff]) {
                Ok(n) if n > 0 => tracing::info!("SQLite purge: removed {n} unused rows ({sql})"),
                Ok(_) => {}
                Err(e) => tracing::error!("SQLite purge failed: {e}"),
            }
        }
    }

    /// Body of the writer thread: applies queued writes in batches (one transaction per batch) and
    /// acknowledges them; purges expired rows once a day.
    fn writer_loop(conn: Connection, mut rx: mpsc::Receiver<Job>) {
        let mut last_purge = Instant::now();
        let mut batch: Vec<Job> = Vec::with_capacity(MAX_BATCH);
        while let Some(first) = rx.blocking_recv() {
            batch.push(first);
            while batch.len() < MAX_BATCH {
                match rx.try_recv() {
                    Ok(job) => batch.push(job),
                    Err(_) => break,
                }
            }
            Self::apply_batch(&conn, &batch);
            for (_, ack) in batch.drain(..) {
                let _ = ack.send(());
            }
            if last_purge.elapsed() >= PURGE_INTERVAL {
                Self::purge_expired(&conn);
                last_purge = Instant::now();
            }
        }
    }

    fn apply_batch(conn: &Connection, batch: &[Job]) {
        // One transaction per batch (a lone write is already atomic).
        let in_tx = batch.len() > 1
            && conn.execute_batch("BEGIN IMMEDIATE").map_err(|e| tracing::error!("SQLite begin failed: {e}")).is_ok();
        for (op, _) in batch {
            Self::apply(conn, op);
        }
        if in_tx && let Err(e) = conn.execute_batch("COMMIT") {
            tracing::error!("SQLite batch commit failed: {e}");
            let _ = conn.execute_batch("ROLLBACK");
        }
    }

    fn apply(conn: &Connection, op: &WriteOp) {
        let (what, res) = match op {
            WriteOp::UpsertBot { token, bot_id, username, first_name, user_json, now } => (
                "save bot",
                conn.prepare_cached(
                    "INSERT INTO bots (token, bot_id, username, first_name, user_json, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                     ON CONFLICT(token) DO UPDATE SET
                        bot_id = excluded.bot_id,
                        username = excluded.username,
                        first_name = excluded.first_name,
                        user_json = excluded.user_json,
                        updated_at = excluded.updated_at",
                )
                .and_then(|mut st| st.execute(params![token, bot_id, username, first_name, user_json, now])),
            ),
            WriteOp::TouchBot { token, now } => (
                "touch bot",
                conn.prepare_cached("UPDATE bots SET updated_at = ?2 WHERE token = ?1")
                    .and_then(|mut st| st.execute(params![token, now])),
            ),
            WriteOp::UpsertFile { file_id, file_unique_id, file_size, local_path, now } => (
                "save cached file",
                conn.prepare_cached(
                    "INSERT INTO file_cache (file_id, file_unique_id, file_size, local_path, updated_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     ON CONFLICT(file_id) DO UPDATE SET
                        local_path = excluded.local_path,
                        file_size = excluded.file_size,
                        updated_at = excluded.updated_at",
                )
                .and_then(|mut st| st.execute(params![file_id, file_unique_id, file_size, local_path, now])),
            ),
            WriteOp::UpsertWebhook { token, record, now } => {
                let allowed_json = record.allowed_updates.as_deref().and_then(|a| serde_json::to_string(a).ok());
                (
                    "save webhook",
                    conn.prepare_cached(
                        "INSERT INTO webhooks (token, url, secret_token, max_connections, allowed_updates, ip_address, updated_at)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                         ON CONFLICT(token) DO UPDATE SET
                            url = excluded.url,
                            secret_token = excluded.secret_token,
                            max_connections = excluded.max_connections,
                            allowed_updates = excluded.allowed_updates,
                            ip_address = excluded.ip_address,
                            updated_at = excluded.updated_at",
                    )
                    .and_then(|mut st| {
                        st.execute(params![
                            token,
                            record.url,
                            record.secret_token,
                            record.max_connections,
                            allowed_json,
                            record.ip_address,
                            now
                        ])
                    }),
                )
            }
            WriteOp::DeleteWebhook { token } => (
                "delete webhook",
                conn.prepare_cached("DELETE FROM webhooks WHERE token = ?1").and_then(|mut st| st.execute(params![token])),
            ),
        };
        if let Err(e) = res {
            tracing::error!("SQLite {what} failed: {e}");
        }
    }

    /// Queues a write and waits until the writer thread has applied it (so a following read sees it).
    async fn write(&self, op: WriteOp) {
        let (ack_tx, ack_rx) = oneshot::channel();
        if self.writes.send((op, ack_tx)).await.is_err() {
            tracing::error!("SQLite writer thread is gone; write dropped");
            return;
        }
        let _ = ack_rx.await;
    }

    /// Runs `f` on one of the read-only connections on the blocking pool.
    async fn read<T: Send + 'static>(&self, f: impl FnOnce(&Connection) -> Option<T> + Send + 'static) -> Option<T> {
        let idx = self.reader_idx.fetch_add(1, Ordering::Relaxed) % self.readers.len();
        let reader = Arc::clone(&self.readers[idx]);
        tokio::task::spawn_blocking(move || {
            let conn = reader.lock().ok()?;
            f(&conn)
        })
        .await
        .ok()
        .flatten()
    }

    #[inline]
    fn bot_fresh(updated_at: i64, now: i64) -> bool {
        now - updated_at < BOT_PROFILE_TTL_SECS
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
        if let Some(cached) = self.l1_bots.get(token)
            && Self::bot_fresh(cached.value().1, Self::now_unix())
        {
            return Some(Arc::clone(&cached.value().0));
        }
        // Remove only if still stale: a concurrent save_bot may have just inserted a fresh entry.
        let now = Self::now_unix();
        self.l1_bots.remove_if(token, |_, (_, ts)| !Self::bot_fresh(*ts, now));

        let token_owned = token.to_string();
        let (user, updated_at) = self
            .read(move |conn| {
                let mut stmt = conn.prepare_cached("SELECT user_json, updated_at FROM bots WHERE token = ?1").ok()?;
                let (json_str, updated_at): (String, i64) = stmt
                    .query_row(params![token_owned], |row| Ok((row.get(0)?, row.get(1)?)))
                    .ok()?;
                if !Self::bot_fresh(updated_at, Self::now_unix()) {
                    return None;
                }
                serde_json::from_str::<User>(&json_str).ok().map(|u| (u, updated_at))
            })
            .await?;

        let arc_user = Arc::new(user);
        if self.l1_bots.len() >= MAX_L1_CACHE_ENTRIES {
            self.l1_bots.clear();
        }
        // Carry the DB timestamp (not "now") so the TTL counts from the real fetch time, and never
        // overwrite a newer entry stored concurrently by save_bot.
        match self.l1_bots.entry(token.to_string()) {
            dashmap::mapref::entry::Entry::Occupied(e) if e.get().1 >= updated_at => {
                Some(Arc::clone(&e.get().0))
            }
            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                e.insert((Arc::clone(&arc_user), updated_at));
                Some(arc_user)
            }
            dashmap::mapref::entry::Entry::Vacant(e) => {
                e.insert((Arc::clone(&arc_user), updated_at));
                Some(arc_user)
            }
        }
    }

    /// Cached Bot User profile ignoring the TTL (stale-on-error fallback when a refresh fails).
    /// Does not touch L1 freshness bookkeeping.
    pub async fn get_bot_stale(&self, token: &str) -> Option<Arc<User>> {
        if let Some(cached) = self.l1_bots.get(token) {
            return Some(Arc::clone(&cached.value().0));
        }
        let token_owned = token.to_string();
        self.read(move |conn| {
            let mut stmt = conn.prepare_cached("SELECT user_json FROM bots WHERE token = ?1").ok()?;
            let json_str: String = stmt.query_row(params![token_owned], |row| row.get(0)).ok()?;
            serde_json::from_str::<User>(&json_str).ok().map(Arc::new)
        })
        .await
    }

    /// Persists Bot User profile into L1 DashMap and SQLite WAL. An unchanged profile is not rewritten:
    /// it is skipped entirely when saved again within `SAVE_SKIP_SECS`, otherwise only its timestamp is bumped.
    pub async fn save_bot(&self, token: &str, user: &User) {
        let now = Self::now_unix();
        let unchanged = self.l1_bots.get(token).and_then(|e| (*e.0 == *user).then(|| (Arc::clone(&e.0), e.1)));
        let (arc_user, op) = match unchanged {
            Some((_, saved_at)) if now - saved_at < SAVE_SKIP_SECS => return,
            Some((arc, _)) => (arc, WriteOp::TouchBot { token: token.to_string(), now }),
            None => {
                let Ok(user_json) = serde_json::to_string(user) else { return };
                let op = WriteOp::UpsertBot {
                    token: token.to_string(),
                    bot_id: user.id,
                    username: user.username.clone(),
                    first_name: user.first_name.clone(),
                    user_json,
                    now,
                };
                (Arc::new(user.clone()), op)
            }
        };
        if self.l1_bots.len() >= MAX_L1_CACHE_ENTRIES {
            self.l1_bots.clear();
        }
        self.l1_bots.insert(token.to_string(), (arc_user, now));
        self.write(op).await;
    }

    /// Retrieves cached File metadata from L1 DashMap or SQLite WAL reader pool if local file still exists.
    pub async fn get_cached_file(&self, file_id: &str) -> Option<File> {
        if let Some(cached) = self.l1_files.get(file_id) {
            let f = cached.value().clone();
            if f.file_path.as_ref().is_some_and(|p| Path::new(p).exists()) {
                return Some(f);
            }
        }

        let file_id_owned = file_id.to_string();
        let file_opt = self
            .read(move |conn| {
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
            .await?;

        if file_opt.file_path.as_ref().is_some_and(|p| Path::new(p).exists()) {
            if self.l1_files.len() >= MAX_L1_CACHE_ENTRIES {
                self.l1_files.clear();
            }
            self.l1_files.insert(file_id.to_string(), file_opt.clone());
            return Some(file_opt);
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

        self.write(WriteOp::UpsertFile {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            file_size: file.file_size,
            local_path: local_path.clone(),
            now: Self::now_unix(),
        })
        .await;
    }

    /// Saves webhook configuration in SQLite WAL.
    pub async fn save_webhook(
        &self,
        token: &str,
        url: &str,
        secret_token: Option<&str>,
        max_conn: i32,
        allowed_updates: Option<&[String]>,
        ip_address: Option<&str>,
    ) {
        self.write(WriteOp::UpsertWebhook {
            token: token.to_string(),
            record: WebhookRecord {
                url: url.to_string(),
                secret_token: secret_token.map(str::to_string),
                max_connections: max_conn,
                allowed_updates: allowed_updates.map(<[String]>::to_vec),
                ip_address: ip_address.map(str::to_string),
            },
            now: Self::now_unix(),
        })
        .await;
    }

    /// Maps columns `url, secret_token, max_connections, allowed_updates, ip_address` starting at `first`.
    fn webhook_from_row(row: &rusqlite::Row<'_>, first: usize) -> rusqlite::Result<WebhookRecord> {
        let allowed_str: Option<String> = row.get(first + 3)?;
        Ok(WebhookRecord {
            url: row.get(first)?,
            secret_token: row.get(first + 1)?,
            max_connections: row.get(first + 2)?,
            allowed_updates: allowed_str.and_then(|s| serde_json::from_str(&s).ok()),
            ip_address: row.get(first + 4)?,
        })
    }

    /// Retrieves webhook configuration for a specific bot from SQLite WAL.
    pub async fn get_webhook(&self, token: &str) -> Option<WebhookRecord> {
        let token_owned = token.to_string();
        self.read(move |conn| {
            let mut stmt = conn
                .prepare_cached("SELECT url, secret_token, max_connections, allowed_updates, ip_address FROM webhooks WHERE token = ?1")
                .ok()?;
            stmt.query_row(params![token_owned], |row| Self::webhook_from_row(row, 0)).ok()
        })
        .await
    }

    /// Deletes webhook configuration from SQLite WAL.
    pub async fn delete_webhook(&self, token: &str) {
        self.write(WriteOp::DeleteWebhook { token: token.to_string() }).await;
    }

    /// Loads all saved webhook records from SQLite WAL on startup.
    pub async fn get_all_webhooks(&self) -> Vec<(String, WebhookRecord)> {
        self.read(|conn| {
            let mut stmt = conn
                .prepare("SELECT url, secret_token, max_connections, allowed_updates, ip_address, token FROM webhooks")
                .ok()?;
            let rows = stmt.query_map([], |row| Ok((row.get::<_, String>(5)?, Self::webhook_from_row(row, 0)?))).ok()?;
            Some(rows.flatten().collect())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tba_store_{tag}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Direct connection to the test database (bypasses the writer thread).
    fn raw(dir: &Path) -> Connection {
        Connection::open(dir.join("bot_api_state.sqlite3")).unwrap()
    }

    fn user(id: i64, name: &str) -> User {
        serde_json::from_str(&format!(r#"{{"id":{id},"is_bot":true,"first_name":"{name}"}}"#)).unwrap()
    }

    #[test]
    fn bot_ttl() {
        assert!(StorageEngine::bot_fresh(1000, 1000 + BOT_PROFILE_TTL_SECS - 1));
        assert!(!StorageEngine::bot_fresh(1000, 1000 + BOT_PROFILE_TTL_SECS));
    }

    #[test]
    fn migration_adds_missing_columns() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("CREATE TABLE webhooks (token TEXT PRIMARY KEY, url TEXT NOT NULL, secret_token TEXT, max_connections INTEGER NOT NULL DEFAULT 40, updated_at INTEGER NOT NULL);").unwrap();
        StorageEngine::init_schema(&conn).unwrap();
        assert!(StorageEngine::column_exists(&conn, "webhooks", "ip_address").unwrap());
        assert!(StorageEngine::column_exists(&conn, "webhooks", "allowed_updates").unwrap());
        StorageEngine::init_schema(&conn).unwrap(); // idempotent
    }

    #[tokio::test]
    async fn l1_entry_carries_db_timestamp_and_stale_is_dropped() {
        let dir = test_dir("l1");
        let st = StorageEngine::new(&dir).unwrap();
        let user = user(1, "b");
        let old = StorageEngine::now_unix() - 100;
        let json = serde_json::to_string(&user).unwrap();
        let db = raw(&dir);
        db.execute(
            "INSERT INTO bots (token, bot_id, username, first_name, user_json, updated_at) VALUES ('t', 1, NULL, 'b', ?1, ?2)",
            params![json, old],
        ).unwrap();
        assert!(st.get_bot("t").await.is_some());
        assert_eq!(st.l1_bots.get("t").unwrap().1, old, "L1 must carry the DB updated_at");
        // Expired rows are not served.
        db.execute("UPDATE bots SET updated_at = ?1", params![old - BOT_PROFILE_TTL_SECS]).unwrap();
        st.l1_bots.clear();
        assert!(st.get_bot("t").await.is_none());
        // ...but the stale-on-error fallback still serves them.
        assert_eq!(st.get_bot_stale("t").await.unwrap().id, 1);
        assert!(st.get_bot_stale("missing").await.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn writes_are_visible_after_await_and_roundtrip() {
        let dir = test_dir("rw");
        let st = StorageEngine::new(&dir).unwrap();
        st.save_bot("t", &user(7, "seven")).await;
        st.l1_bots.clear();
        assert_eq!(st.get_bot("t").await.unwrap().first_name, "seven", "served from SQLite after L1 clear");

        let allowed = vec!["message".to_string()];
        st.save_webhook("w", "http://h/x", Some("s"), 12, Some(&allowed), Some("1.2.3.4")).await;
        let rec = st.get_webhook("w").await.unwrap();
        assert_eq!((rec.url.as_str(), rec.max_connections), ("http://h/x", 12));
        assert_eq!(rec.secret_token.as_deref(), Some("s"));
        assert_eq!(rec.allowed_updates, Some(allowed));
        assert_eq!(st.get_all_webhooks().await.len(), 1);
        st.delete_webhook("w").await;
        assert!(st.get_webhook("w").await.is_none());

        let f = File { file_id: "f".into(), file_unique_id: "u".into(), file_size: Some(3), file_path: Some(dir.to_string_lossy().into()) };
        st.save_cached_file(&f).await;
        st.l1_files.clear();
        assert_eq!(st.get_cached_file("f").await.unwrap().file_unique_id, "u");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn unchanged_profile_is_not_rewritten() {
        let dir = test_dir("skip");
        let st = StorageEngine::new(&dir).unwrap();
        let u = user(5, "five");
        st.save_bot("t", &u).await;
        let db = raw(&dir);
        let stamp = |db: &Connection| db.query_row("SELECT updated_at FROM bots WHERE token='t'", [], |r| r.get::<_, i64>(0)).unwrap();
        // Make the stored row and the L1 entry look 10 minutes old.
        let old = StorageEngine::now_unix() - 600;
        db.execute("UPDATE bots SET updated_at = ?1", params![old]).unwrap();
        st.l1_bots.insert("t".into(), (Arc::new(u.clone()), old));
        // Unchanged and not recent: only the timestamp is bumped.
        st.save_bot("t", &u).await;
        assert!(stamp(&db) > old);
        // Unchanged and recent (just saved): no write at all.
        db.execute("UPDATE bots SET updated_at = ?1", params![old]).unwrap();
        st.save_bot("t", &u).await;
        assert_eq!(stamp(&db), old, "second save within SAVE_SKIP_SECS must not touch the DB");
        // A changed profile is always written.
        st.save_bot("t", &user(5, "renamed")).await;
        let name: String = db.query_row("SELECT first_name FROM bots WHERE token='t'", [], |r| r.get(0)).unwrap();
        assert_eq!(name, "renamed");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_removes_only_unused_rows() {
        let dir = test_dir("purge");
        {
            let st = StorageEngine::new(&dir).unwrap();
            st.save_bot("old", &user(1, "a")).await;
            st.save_bot("new", &user(2, "b")).await;
            st.save_webhook("w", "http://h", None, 40, None, None).await;
                let db = raw(&dir);
            let ancient = StorageEngine::now_unix() - ROW_RETENTION_SECS - 10;
            db.execute("UPDATE bots SET updated_at = ?1 WHERE token='old'", params![ancient]).unwrap();
            db.execute("UPDATE webhooks SET updated_at = ?1", params![ancient]).unwrap();
        }
        // Startup purge (next boot).
        let st = StorageEngine::new(&dir).unwrap();
        let db = raw(&dir);
        let count = |t: &str| db.query_row(&format!("SELECT COUNT(*) FROM {t}"), [], |r| r.get::<_, i64>(0)).unwrap();
        assert_eq!(count("bots"), 1, "expired bot row purged, fresh one kept");
        assert_eq!(count("webhooks"), 1, "webhook config is never purged");
        assert!(st.get_webhook("w").await.is_some());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn concurrent_writers_are_bounded_and_all_applied() {
        let dir = test_dir("burst");
        let st = StorageEngine::new(&dir).unwrap();
        let mut tasks = Vec::new();
        for i in 0..1000 {
            let st = Arc::clone(&st);
            tasks.push(tokio::spawn(async move { st.save_bot(&format!("tok{i}"), &user(i, "n")).await }));
        }
        for t in tasks {
            t.await.unwrap();
        }
        let n: i64 = raw(&dir).query_row("SELECT COUNT(*) FROM bots", [], |r| r.get(0)).unwrap();
        assert_eq!(n, 1000);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
