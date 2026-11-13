//! getMe / bot profile: storage-backed cache in front of upstream `getMe`.

use super::engine::TelegramEngine;
use crate::error::{TelegramError, TelegramResult};
use crate::models::{ApiResponse, User};

impl TelegramEngine {
    /// Execute getMe (checks SQLite WAL + L1 DashMap cache first)
    pub async fn get_me(&self, token: &str) -> TelegramResult<User> {
        if let Some(cached_user) = self.storage.get_bot(token).await {
            self.record_active_bot(token);
            return Ok((*cached_user).clone());
        }
        self.get_me_fresh(token).await
    }

    /// getMe for the MTProto upload path: if the refresh fails for a transient reason (network / 5xx),
    /// fall back to the expired cached profile instead of losing the bot identity. Auth errors
    /// (401 etc.) are not masked.
    pub(super) async fn get_me_or_stale(&self, token: &str) -> Option<User> {
        match self.get_me(token).await {
            Ok(u) => Some(u),
            Err(e) if is_transient_upstream_error(&e) => {
                let stale = self.storage.get_bot_stale(token).await;
                if stale.is_some() {
                    tracing::warn!("getMe refresh failed ({e}); using stale cached bot profile");
                }
                stale.map(|u| (*u).clone())
            }
            Err(_) => None,
        }
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
}

/// True for failures that say nothing about the token itself: network/parse errors and upstream 5xx.
fn is_transient_upstream_error(e: &TelegramError) -> bool {
    match e {
        TelegramError::Internal(_) => true,
        TelegramError::ApiError { code, .. } => *code >= 500,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transient_errors_allow_stale_profile() {
        assert!(is_transient_upstream_error(&TelegramError::Internal("Network error: x".into())));
        assert!(is_transient_upstream_error(&TelegramError::ApiError {
            code: 502,
            description: "Bad Gateway".into(),
            parameters: None
        }));
        assert!(!is_transient_upstream_error(&TelegramError::ApiError {
            code: 401,
            description: "Unauthorized".into(),
            parameters: None
        }));
        assert!(!is_transient_upstream_error(&TelegramError::Unauthorized("x".into())));
    }
}
