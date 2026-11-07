use std::sync::Arc;
use bytes::Bytes;
use dashmap::DashMap;
use crate::client::TelegramEngine;
use crate::error::{TelegramError, TelegramResult};
use crate::models::{ApiResponse, User};

const MAX_ACTIVE_SESSIONS: usize = 2048;

/// Represents an active Bot session matching official Client.h
/// Stores pre-serialized JSON bytes for zero-allocation, zero-serialization fast path responses.
pub struct BotSession {
    pub token: String,
    pub me: User,
    pub me_json_bytes: Bytes,
}

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
        if let Some(session) = self.sessions.get(token) {
            return Ok(Arc::clone(session.value()));
        }

        // Fetch bot metadata (backed by SQLite WAL + L1 cache in engine)
        let me = self.engine.get_me(token).await?;
        let envelope = ApiResponse::success(&me);
        let serialized = serde_json::to_vec(&envelope)
            .map_err(|e| TelegramError::Internal(format!("Serialization error: {e}")))?;

        let session = Arc::new(BotSession {
            token: token.to_string(),
            me,
            me_json_bytes: Bytes::from(serialized),
        });

        if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
            self.sessions.clear();
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
