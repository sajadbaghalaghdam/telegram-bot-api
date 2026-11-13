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
}

/// Cached getMe sessions are re-validated against upstream after this long.
const SESSION_TTL: Duration = Duration::from_secs(60);

/// Lock-free sharded manager for multiple Bot sessions matching official ClientManager.h/ClientManager.cpp
pub struct ClientManager {
    engine: Arc<TelegramEngine>,
    sessions: DashMap<String, Arc<BotSession>>,
}

impl ClientManager {
    pub fn new(engine: Arc<TelegramEngine>) -> Self {
        Self {
            engine,
            sessions: DashMap::with_capacity(64),
        }
    }

    /// Retrieves an existing bot session or initializes a new one with zero global lock contention
    pub async fn get_or_create(&self, token: &str) -> TelegramResult<Arc<BotSession>> {
        if let Some(session) = self.sessions.get(token)
            && session.created.elapsed() < SESSION_TTL {
                return Ok(Arc::clone(session.value()));
            }
        self.sessions.remove(token);

        // Fetch bot metadata (backed by SQLite WAL + L1 cache in engine)
        let me = self.engine.get_me_fresh(token).await?;
        let envelope = ApiResponse::success(&me);
        let serialized = serde_json::to_vec(&envelope)
            .map_err(|e| TelegramError::Internal(format!("Serialization error: {e}")))?;

        let session = Arc::new(BotSession {
            me_json_bytes: Bytes::from(serialized),
            created: Instant::now(),
        });

        if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
            let stale: Vec<String> = self
                .sessions
                .iter()
                .filter(|e| e.value().created.elapsed() >= SESSION_TTL)
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
