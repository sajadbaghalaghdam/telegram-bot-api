//! Token validation (C++ `ClientManager::send` rules) and the bounded set of bots seen alive.

use bytes::Bytes;

use super::engine::TelegramEngine;
use crate::error::TelegramError;

impl TelegramEngine {
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
    pub(super) fn note_upstream_reply(&self, token: &str, reply: &Result<Bytes, TelegramError>) {
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
    // C++ to_integer_safe: the parsed value must print back as the same string ("+1", "-0", "-05" are rejected).
    let parsed = id_str.parse::<i64>().ok().filter(|id| id.to_string() == id_str);
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
mod tests {
    use super::*;

    #[test]
    fn token_validation() {
        assert!(validate_token("123:abc", None).is_ok());
        assert!(matches!(validate_token("+123:abc", None), Err(TelegramError::MisdirectedRequest(_))));
        assert!(matches!(validate_token("abc", None), Err(TelegramError::Unauthorized(_))));
        assert!(matches!(validate_token("0123:abc", None), Err(TelegramError::Unauthorized(_))));
        assert!(matches!(validate_token("a/b:c", None), Err(TelegramError::Unauthorized(_))));
        assert!(matches!(validate_token("12x:abc", None), Err(TelegramError::MisdirectedRequest(_))));
        for bad in ["-0:abc", "-05:abc", "-00:abc", "+0:a", ":abc", "99999999999999999999:abc"] {
            assert!(matches!(validate_token(bad, None), Err(TelegramError::MisdirectedRequest(_))), "{bad}");
        }
        // Canonical negative ids parse fine and are then rejected as out of range (401).
        assert!(matches!(validate_token("-5:abc", None), Err(TelegramError::Unauthorized(_))));
        assert!(matches!(validate_token("123:abc", Some((0, 2))), Err(TelegramError::MisdirectedRequest(_))));
        assert!(validate_token("124:abc", Some((0, 2))).is_ok());
        assert!(matches!(validate_token("18014398509481984:a", None), Err(TelegramError::Unauthorized(_))));
    }
}
