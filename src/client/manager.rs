use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use bytes::Bytes;
use dashmap::DashMap;
use crate::client::TelegramEngine;
use crate::error::{TelegramError, TelegramResult};
use crate::models::ApiResponse;

const MAX_ACTIVE_SESSIONS: usize = 2048;

/// Represents an active Bot session matching official Client.h
/// Stores pre-serialized JSON bytes for zero-allocation, zero-serialization fast path responses.
pub struct BotSession {
    pub me_json_bytes: Bytes,
    created: Instant,
    /// Set while one request re-validates this (expired) session; the others keep using it meanwhile.
    refreshing: AtomicBool,
}

/// Clears `refreshing` when the refresh ends, also when the refreshing request is cancelled.
struct RefreshGuard<'a>(&'a AtomicBool);

impl Drop for RefreshGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Cached getMe sessions are re-validated against upstream after this long.
const SESSION_TTL: Duration = Duration::from_secs(60);

/// Lock-free sharded manager for multiple Bot sessions matching official ClientManager.h/ClientManager.cpp
pub struct ClientManager {
    engine: Arc<TelegramEngine>,
    sessions: DashMap<String, Arc<BotSession>>,
    ttl: Duration,
}

impl ClientManager {
    pub fn new(engine: Arc<TelegramEngine>) -> Self {
        Self {
            engine,
            sessions: DashMap::with_capacity(64),
            ttl: SESSION_TTL,
        }
    }

    /// Retrieves an existing bot session or initializes a new one with zero global lock contention.
    /// When a session has expired exactly one request re-validates it with upstream; requests that
    /// arrive meanwhile are served the previous profile instead of each hitting upstream.
    pub async fn get_or_create(&self, token: &str) -> TelegramResult<Arc<BotSession>> {
        let cached = self.sessions.get(token).map(|e| Arc::clone(e.value()));
        if let Some(session) = &cached
            && (session.created.elapsed() < self.ttl || session.refreshing.swap(true, Ordering::AcqRel))
        {
            return Ok(Arc::clone(session));
        }
        let _refreshing = cached.as_ref().map(|s| RefreshGuard(&s.refreshing));

        match self.fetch(token).await {
            Ok(session) => Ok(session),
            Err(e) => {
                if cached.is_some() {
                    self.sessions.remove(token);
                }
                Err(e)
            }
        }
    }

    /// Fetches the bot profile from upstream and stores the new session.
    async fn fetch(&self, token: &str) -> TelegramResult<Arc<BotSession>> {
        // Fetch bot metadata (backed by SQLite WAL + L1 cache in engine)
        let me = self.engine.get_me_fresh(token).await?;
        let envelope = ApiResponse::success(&me);
        let serialized = serde_json::to_vec(&envelope)
            .map_err(|e| TelegramError::Internal(format!("Serialization error: {e}")))?;

        let session = Arc::new(BotSession {
            me_json_bytes: Bytes::from(serialized),
            created: Instant::now(),
            refreshing: AtomicBool::new(false),
        });

        if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
            let stale: Vec<String> = self
                .sessions
                .iter()
                .filter(|e| e.value().created.elapsed() >= self.ttl)
                .map(|e| e.key().clone())
                .collect();
            for k in stale {
                self.sessions.remove(&k);
            }
            if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
                evict_one(&self.sessions);
            }
        }
        self.sessions.insert(token.to_string(), Arc::clone(&session));

        Ok(session)
    }

    /// Closes a bot session
    pub fn close(&self, token: &str) {
        self.sessions.remove(token);
    }

    #[inline]
    pub fn active_count(&self) -> usize {
        self.sessions.len()
    }
}

/// Removes an arbitrary entry. The key is bound first so the iterator's shard guard is
/// dropped before `remove()` (holding both deadlocks).
fn evict_one<V>(map: &DashMap<String, V>) {
    let victim = map.iter().next().map(|e| e.key().clone());
    if let Some(k) = victim {
        map.remove(&k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::client::test_support::{MockUpstream, Reply, TOKEN};

    fn manager(upstream: &MockUpstream, name: &str) -> ClientManager {
        let dir = std::env::temp_dir().join(format!("mgr_{name}_{}", std::process::id()));
        let storage = crate::storage::StorageEngine::new(&dir).unwrap();
        let mut engine = TelegramEngine::new(false, dir.clone(), dir, storage);
        engine.upstream_base_url = upstream.url.clone();
        ClientManager::new(Arc::new(engine))
    }

    const ME: &str = r#"{"ok":true,"result":{"id":123456,"is_bot":true,"first_name":"B"}}"#;

    #[tokio::test]
    async fn expired_session_is_refreshed_by_one_request_only() {
        let up = MockUpstream::start(|_| Reply::json(200, ME)).await;
        let mut mgr = manager(&up, "refresh");
        mgr.ttl = Duration::from_millis(40);
        let mgr = Arc::new(mgr);
        let first = mgr.get_or_create(TOKEN).await.unwrap();
        assert_eq!(up.requests().len(), 1);
        // still fresh: no upstream call
        mgr.get_or_create(TOKEN).await.unwrap();
        assert_eq!(up.requests().len(), 1);

        tokio::time::sleep(Duration::from_millis(80)).await;
        let tasks: Vec<_> = (0..20)
            .map(|_| {
                let m = Arc::clone(&mgr);
                tokio::spawn(async move { m.get_or_create(TOKEN).await.map(|s| s.me_json_bytes.len()) })
            })
            .collect();
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap(), first.me_json_bytes.len());
        }
        assert_eq!(up.requests().len(), 2, "20 concurrent requests share one refresh");
        // the refreshed session is fresh again
        mgr.get_or_create(TOKEN).await.unwrap();
        assert_eq!(up.requests().len(), 2);
    }

    #[tokio::test]
    async fn failed_refresh_drops_the_session() {
        let ok = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let flag = Arc::clone(&ok);
        let up = MockUpstream::start(move |_| {
            if flag.load(Ordering::Relaxed) {
                Reply::json(200, ME)
            } else {
                Reply::json(401, r#"{"ok":false,"error_code":401,"description":"Unauthorized"}"#)
            }
        })
        .await;
        let mut mgr = manager(&up, "failrefresh");
        mgr.ttl = Duration::from_millis(20);
        mgr.get_or_create(TOKEN).await.unwrap();
        ok.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(mgr.get_or_create(TOKEN).await.is_err());
        assert_eq!(mgr.active_count(), 0);
        // the next request tries again (nothing is stuck in "refreshing")
        assert!(mgr.get_or_create(TOKEN).await.is_err());
        assert_eq!(up.requests().len(), 3);
    }

    #[test]
    fn evict_one_does_not_deadlock() {
        let (tx, rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let map: DashMap<String, u32> = DashMap::new();
            for i in 0..(MAX_ACTIVE_SESSIONS + 10) {
                map.insert(i.to_string(), 0);
                if map.len() >= MAX_ACTIVE_SESSIONS {
                    evict_one(&map);
                }
            }
            let _ = tx.send(map.len());
        });
        let len = rx.recv_timeout(std::time::Duration::from_secs(10)).expect("evict hung");
        assert_eq!(len, MAX_ACTIVE_SESSIONS - 1);
    }
}
