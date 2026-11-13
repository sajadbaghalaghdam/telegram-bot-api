//! Helpers for talking to the upstream Bot API: transport errors become 500s with a stable
//! description, `ok: false` envelopes become `ApiError`s.

use bytes::Bytes;
use reqwest::RequestBuilder;
use serde::de::DeserializeOwned;

use super::engine::TelegramEngine;
use crate::error::{TelegramError, TelegramResult};
use crate::models::ApiResponse;

/// Sends the request and returns the raw response body (any HTTP status).
pub(super) async fn send_bytes(req: RequestBuilder) -> TelegramResult<Bytes> {
    req.send()
        .await
        .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
        .bytes()
        .await
        .map_err(|e| TelegramError::Internal(format!("Read error: {e}")))
}

/// Sends the request and parses the Bot API envelope.
pub(super) async fn send_json<T: DeserializeOwned>(req: RequestBuilder) -> TelegramResult<ApiResponse<T>> {
    req.send()
        .await
        .map_err(|e| TelegramError::Internal(format!("Network error: {e}")))?
        .json()
        .await
        .map_err(|e| TelegramError::Internal(format!("Failed to parse response: {e}")))
}

impl TelegramEngine {
    /// Sends the request and returns the envelope's `result`; `ok: false` becomes an `ApiError`
    /// carrying upstream's code (default 400), description (default `fallback_description`) and
    /// parameters.
    pub(super) async fn call_upstream<T: DeserializeOwned>(
        &self,
        req: RequestBuilder,
        fallback_description: &str,
    ) -> TelegramResult<Option<T>> {
        let resp = send_json::<T>(req).await?;
        if !resp.ok {
            return Err(TelegramError::ApiError {
                code: resp.error_code.unwrap_or(400),
                description: resp.description.unwrap_or_else(|| fallback_description.to_string()),
                parameters: resp.parameters,
            });
        }
        Ok(resp.result)
    }

    /// `POST <base>/bot<token>/deleteWebhook[?drop_pending_updates=true]`.
    pub(super) fn delete_webhook_request(&self, token: &str, drop_pending_updates: bool) -> RequestBuilder {
        let query = if drop_pending_updates { "?drop_pending_updates=true" } else { "" };
        self.client
            .post(format!("{}/bot{token}/deleteWebhook{query}", self.upstream_base_url))
    }
}
