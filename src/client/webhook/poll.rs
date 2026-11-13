//! Parsing and classification of upstream getUpdates replies. The updates themselves are kept as
//! raw byte ranges of the reply (never turned into `serde_json::Value`).
use std::borrow::Cow;
use std::ops::Range;

use bytes::Bytes;
use reqwest::header::{HeaderValue, CONTENT_TYPE};
use reqwest::Client;
use serde::Deserialize;
use serde_json::value::RawValue;

use super::retry::MAX_RETRY_AFTER;

/// Issues the getUpdates calls of one worker. The URL and the `allowed_updates` fragment are
/// built once; a poll only formats three numbers.
pub(super) struct Poller {
    client: Client,
    url: String,
    /// `,"allowed_updates":[...]` or empty.
    allowed: String,
}

impl Poller {
    pub(super) fn new(client: Client, upstream_base_url: &str, token: &str, allowed_updates: Option<&[String]>) -> Self {
        let allowed = allowed_updates
            .and_then(|a| serde_json::to_string(a).ok())
            .map(|a| format!(",\"allowed_updates\":{a}"))
            .unwrap_or_default();
        Self { client, url: format!("{upstream_base_url}/bot{token}/getUpdates"), allowed }
    }

    fn body(&self, offset: i64, limit: usize, timeout: u32) -> String {
        format!("{{\"offset\":{offset},\"limit\":{limit},\"timeout\":{timeout}{}}}", self.allowed)
    }

    /// The raw reply body (whatever the HTTP status), or the transport error text.
    pub(super) async fn fetch(&self, offset: i64, limit: usize, timeout: u32) -> Result<Bytes, String> {
        let req = self
            .client
            .post(&self.url)
            .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
            .body(self.body(offset, limit, timeout));
        match req.send().await {
            // A body that cannot be read is treated like an unreadable reply.
            Ok(resp) => Ok(resp.bytes().await.unwrap_or_default()),
            Err(e) => Err(e.without_url().to_string()),
        }
    }
}

/// The parts of a getUpdates reply the worker looks at.
#[derive(Deserialize)]
pub(super) struct PollReply<'a> {
    ok: Option<bool>,
    #[serde(borrow)]
    result: Option<Vec<&'a RawValue>>,
    error_code: Option<i64>,
    #[serde(borrow)]
    description: Option<Cow<'a, str>>,
    parameters: Option<Parameters>,
}

#[derive(Deserialize)]
struct Parameters {
    retry_after: Option<u64>,
}

impl<'a> PollReply<'a> {
    /// None when the body is not a JSON object (unreadable / truncated reply).
    pub(super) fn parse(body: &'a [u8]) -> Option<Self> {
        serde_json::from_slice(body).ok()
    }
}

/// Result of one upstream poll.
#[derive(Debug, PartialEq)]
pub(super) enum PollOutcome {
    /// Byte ranges (within the reply body) of the returned updates.
    Updates(Vec<Range<usize>>),
    /// Token invalid / bot gone: stop the worker.
    Fatal,
    /// 409 because a cloud webhook is still set upstream: it has to be deleted again.
    WebhookActive,
    /// Wait this long, then poll again (None = use exponential backoff).
    Retry(Option<u32>),
}

/// `body` must be the buffer `reply` was parsed from (the ranges are offsets into it).
pub(super) fn classify_poll(reply: Option<&PollReply<'_>>, body: &[u8]) -> PollOutcome {
    let Some(r) = reply else {
        return PollOutcome::Retry(None);
    };
    if r.ok == Some(true) {
        return match &r.result {
            Some(a) => {
                let base = body.as_ptr() as usize;
                PollOutcome::Updates(
                    a.iter()
                        .map(|u| {
                            let start = u.get().as_ptr() as usize - base;
                            start..start + u.get().len()
                        })
                        .collect(),
                )
            }
            None => PollOutcome::Retry(None),
        };
    }
    match r.error_code.unwrap_or(0) {
        401 | 404 => PollOutcome::Fatal,
        409 if r.description.as_deref().is_some_and(|d| d.to_ascii_lowercase().contains("webhook is active")) => {
            PollOutcome::WebhookActive
        }
        429 => {
            let ra = r.parameters.as_ref().and_then(|p| p.retry_after).unwrap_or(1).clamp(1, MAX_RETRY_AFTER as u64) as u32;
            PollOutcome::Retry(Some(ra))
        }
        _ => PollOutcome::Retry(None),
    }
}

/// Human readable reason of a failed poll, for logs and `getWebhookInfo.last_error_message`.
pub(super) fn poll_error_text(reply: Option<&PollReply<'_>>, transport_error: Option<&str>) -> String {
    if let Some(e) = transport_error {
        return format!("getUpdates failed: {e}");
    }
    match reply {
        Some(r) => {
            let code = r.error_code.unwrap_or(0);
            let desc = r.description.as_deref().unwrap_or("unexpected response");
            format!("getUpdates failed: {code} {desc}")
        }
        None => "getUpdates failed: unreadable response".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn classify(body: &str) -> PollOutcome {
        classify_poll(PollReply::parse(body.as_bytes()).as_ref(), body.as_bytes())
    }

    fn error_text(body: &str) -> String {
        poll_error_text(PollReply::parse(body.as_bytes()).as_ref(), None)
    }

    #[test]
    fn poll_classification() {
        assert_eq!(classify_poll(None, b""), PollOutcome::Retry(None));
        assert_eq!(classify("not json"), PollOutcome::Retry(None));
        assert_eq!(classify(r#"{"ok":true,"result":[]}"#), PollOutcome::Updates(vec![]));
        assert_eq!(classify(r#"{"ok":false,"error_code":401}"#), PollOutcome::Fatal);
        assert_eq!(classify(r#"{"ok":false,"error_code":404}"#), PollOutcome::Fatal);
        assert_eq!(classify(r#"{"ok":false,"error_code":429,"parameters":{"retry_after":7}}"#), PollOutcome::Retry(Some(7)));
        assert_eq!(classify(r#"{"ok":false,"error_code":429,"parameters":{"retry_after":99999}}"#), PollOutcome::Retry(Some(3600)));
        assert_eq!(classify(r#"{"ok":false,"error_code":429}"#), PollOutcome::Retry(Some(1)));
        assert_eq!(classify(r#"{"ok":false,"error_code":409}"#), PollOutcome::Retry(None));
        assert_eq!(
            classify(r#"{"ok":false,"error_code":409,"description":"Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first"}"#),
            PollOutcome::WebhookActive
        );
        assert_eq!(
            classify(r#"{"ok":false,"error_code":409,"description":"Conflict: terminated by other getUpdates request"}"#),
            PollOutcome::Retry(None)
        );
        assert!(error_text(r#"{"error_code":502,"description":"Bad Gateway"}"#).contains("502 Bad Gateway"));
        assert!(poll_error_text(None, Some("boom")).contains("boom"));
        assert_eq!(classify(r#"{"ok":true}"#), PollOutcome::Retry(None));
    }

    #[test]
    fn poll_body_is_valid_json_with_optional_allowed_updates() {
        let client = Client::new();
        let p = Poller::new(client.clone(), "http://x", "1:a", None);
        let v: serde_json::Value = serde_json::from_str(&p.body(-3, 100, 20)).unwrap();
        assert_eq!(v, serde_json::json!({"offset": -3, "limit": 100, "timeout": 20}));
        assert_eq!(p.url, "http://x/bot1:a/getUpdates");
        let allowed = vec!["message".to_string(), "we\"ird".to_string()];
        let p = Poller::new(client, "http://x", "1:a", Some(&allowed));
        let v: serde_json::Value = serde_json::from_str(&p.body(0, 1, 0)).unwrap();
        assert_eq!(v, serde_json::json!({"offset": 0, "limit": 1, "timeout": 0, "allowed_updates": ["message", "we\"ird"]}));
    }

    #[test]
    fn updates_are_byte_ranges_of_the_reply() {
        let body = r#"{"ok":true,"result":[{"update_id":1,"message":{"text":"a \"b\""}}, {"update_id":2},7]}"#;
        let PollOutcome::Updates(r) = classify(body) else { panic!("not updates") };
        let slices: Vec<&str> = r.iter().map(|r| &body[r.clone()]).collect();
        assert_eq!(slices, [r#"{"update_id":1,"message":{"text":"a \"b\""}}"#, r#"{"update_id":2}"#, "7"]);
    }
}
