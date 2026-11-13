//! Bot API webhook methods (setWebhook / deleteWebhook / getWebhookInfo) on top of the engine:
//! local delivery decisions, upstream proxying and the legacy binlog import. The delivery
//! workers themselves live in `webhook.rs`.

use std::sync::Arc;

use super::engine::TelegramEngine;
use crate::error::{TelegramError, TelegramResult};
use crate::models::{ApiResponse, SetWebhookRequest, WebhookInfo};

impl TelegramEngine {
    /// Parses TDLib `webhooks_db.binlog` binary records to extract the latest `(token, webhook_url)` per bot
    pub(crate) fn parse_webhooks_binlog(bytes: &[u8]) -> std::collections::HashMap<String, String> {
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

    /// Asks the upstream Bot API to delete the cloud webhook (so getUpdates works); upstream
    /// errors are returned as-is.
    pub async fn delete_upstream_webhook(&self, token: &str, drop_pending_updates: bool) -> TelegramResult<()> {
        let url = format!("{}/bot{token}/deleteWebhook", self.upstream_base_url);
        let drop_param = if drop_pending_updates { "?drop_pending_updates=true" } else { "" };
        let resp: ApiResponse<bool> = self
            .client
            .post(format!("{url}{drop_param}"))
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;
        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp
                    .description
                    .unwrap_or_else(|| "Failed to delete the upstream webhook".to_string()),
                parameters: resp.parameters,
            });
        }
        Ok(())
    }

    /// Executes setWebhook. Locally delivered webhooks (local mode + private/loopback http URL)
    /// are stored and delivered by a local worker; every other webhook is proxied to the upstream
    /// Bot API (not stored, no local worker). When `raw` is given and the webhook goes to the cloud
    /// server the original request (body, content type, query) is forwarded unchanged, so multipart
    /// `certificate` uploads and every other parameter reach upstream as the client sent them.
    pub async fn set_webhook_raw(
        self: &Arc<Self>,
        token: &str,
        req: &SetWebhookRequest,
        raw: Option<super::webhook::RawForward<'_>>,
    ) -> TelegramResult<bool> {
        let mut url = req.url.clone();
        if !url.is_empty() {
            self.check_set_webhook_throttle(token)?;
            url = super::webhook::normalize_webhook_url(&url)
                .map_err(|m| TelegramError::BadRequest(m.to_string()))?;
            // Like C++, secret token and IP address only matter when a URL is being set.
            if let Some(ref s) = req.secret_token {
                super::webhook::validate_secret_token(s)
                    .map_err(|m| TelegramError::BadRequest(m.to_string()))?;
            }
            if let Some(ip) = req.ip_address.as_deref().filter(|s| !s.is_empty()) {
                super::webhook::validate_ip_address(ip)
                    .map_err(|m| TelegramError::BadRequest(m.to_string()))?;
            }
        }

        let is_local_url = self.local_mode && is_local_webhook_url(&url);

        if is_local_url {
            // Ensure cloud webhook is deleted so local getUpdates worker can receive updates
            let drop_pending = req.drop_pending_updates.unwrap_or(false);
            self.delete_upstream_webhook(token, drop_pending).await?;

            let max_conn = super::webhook::clamp_max_connections(
                req.max_connections,
                self.default_max_webhook_connections,
                true,
            );
            let secret = req.secret_token.clone().filter(|s| !s.is_empty());
            let ip = req.ip_address.clone().filter(|s| !s.is_empty());
            self.storage
                .save_webhook(
                    token,
                    &url,
                    secret.as_deref(),
                    max_conn,
                    req.allowed_updates.as_deref(),
                    ip.as_deref(),
                )
                .await;

            self.start_webhook_worker(super::webhook::WorkerConfig {
                token: token.to_string(),
                url,
                secret_token: secret,
                allowed_updates: req.allowed_updates.clone(),
                drop_pending_updates: drop_pending,
                max_connections: max_conn,
                ip_address: ip,
            });
            return Ok(true);
        }

        // Cloud webhook (or deletion via an empty URL): ask upstream first, and only replace the
        // local webhook once upstream accepted the request.
        let target = format!("{}/bot{token}/setWebhook", self.upstream_base_url);
        let builder = match raw {
            Some(r) => {
                let target = match r.query {
                    Some(q) if !q.is_empty() => format!("{target}?{q}"),
                    _ => target,
                };
                let mut b = self.client.post(target);
                if let Some(ct) = r.content_type {
                    b = b.header(reqwest::header::CONTENT_TYPE, ct);
                }
                b.body(r.body.clone())
            }
            None => {
                let mut fwd = req.clone();
                fwd.url = url;
                self.client.post(target).json(&fwd)
            }
        };
        let resp: ApiResponse<bool> = builder
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp
                    .description
                    .unwrap_or_else(|| "Failed to set webhook".to_string()),
                parameters: resp.parameters,
            });
        }
        self.stop_webhook_worker(token);
        self.storage.delete_webhook(token).await;
        Ok(resp.result.unwrap_or(false))
    }

    /// Execute deleteWebhook and remove from SQLite WAL + stop local webhook worker
    pub async fn delete_webhook(&self, token: &str, drop_pending_updates: bool) -> TelegramResult<bool> {
        self.stop_webhook_worker(token);
        self.storage.delete_webhook(token).await;

        let url = format!("{}/bot{token}/deleteWebhook", self.upstream_base_url);
        let drop_param = if drop_pending_updates {
            "?drop_pending_updates=true"
        } else {
            ""
        };
        let resp: ApiResponse<bool> = self
            .client
            .post(format!("{}{}", url, drop_param))
            .send()
            .await
            .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
            .json()
            .await
            .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))?;

        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp
                    .description
                    .unwrap_or_else(|| "Failed to delete webhook".to_string()),
                parameters: resp.parameters,
            });
        }

        Ok(resp.result.unwrap_or(true))
    }

    /// Execute getWebhookInfo. Locally delivered webhooks are answered from storage + worker
    /// state; cloud webhooks are answered by upstream.
    pub async fn get_webhook_info(&self, token: &str) -> TelegramResult<WebhookInfo> {
        if self.local_mode
            && let Some(wh) = self.storage.get_webhook(token).await
                && super::webhook::should_restore(true, &wh.url) {
                    let (pending, err_date, err_msg) = self
                        .webhook_states
                        .get(token)
                        .map(|s| s.snapshot())
                        .unwrap_or((0, None, None));
                    return Ok(WebhookInfo {
                        url: wh.url,
                        has_custom_certificate: false,
                        pending_update_count: pending.min(i32::MAX as i64) as i32,
                        ip_address: wh.ip_address,
                        last_error_date: err_date.map(|d| d as i32),
                        last_error_message: err_msg,
                        last_synchronization_error_date: None,
                        max_connections: Some(wh.max_connections),
                        allowed_updates: wh.allowed_updates,
                    });
                }

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

        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp
                    .description
                    .unwrap_or_else(|| "Unknown error".to_string()),
                parameters: resp.parameters,
            });
        }

        resp.result.ok_or_else(|| {
            TelegramError::BadRequest(
                resp.description.unwrap_or_else(|| "Unknown error".to_string()),
            )
        })
    }
}

/// True if the webhook URL is a plain-http URL whose host is loopback / private (served by the local worker).
pub fn is_local_webhook_url(url: &str) -> bool {
    let Ok(u) = reqwest::Url::parse(url) else {
        return false;
    };
    if u.scheme() != "http" {
        return false;
    }
    let Some(host) = u.host_str() else {
        return false;
    };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    let host = host.trim_start_matches('[').trim_end_matches(']');
    match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V4(ip)) => ip.is_loopback() || ip.is_private(),
        Ok(std::net::IpAddr::V6(ip)) => ip.is_loopback(),
        Err(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_webhook_urls() {
        assert!(is_local_webhook_url("http://127.0.0.1:8080/x"));
        assert!(is_local_webhook_url("http://localhost/x"));
        assert!(is_local_webhook_url("http://[::1]:1/"));
        assert!(is_local_webhook_url("http://192.168.1.5/"));
        assert!(is_local_webhook_url("http://172.20.0.2/"));
        assert!(!is_local_webhook_url("http://172.32.0.2/"));
        assert!(!is_local_webhook_url("http://localhost.evil.com/"));
        assert!(!is_local_webhook_url("http://10.evil.com/"));
        assert!(!is_local_webhook_url("http://example.com/"));
        assert!(!is_local_webhook_url("https://127.0.0.1/"));
    }

}
