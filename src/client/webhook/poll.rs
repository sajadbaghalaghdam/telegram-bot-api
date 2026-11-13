//! Classification of upstream getUpdates replies.
use serde_json::Value;

use super::retry::MAX_RETRY_AFTER;

/// Result of one upstream poll.
#[derive(Debug, PartialEq)]
pub enum PollOutcome {
    Updates(Vec<Value>),
    /// Token invalid / bot gone: stop the worker.
    Fatal,
    /// 409 because a cloud webhook is still set upstream: it has to be deleted again.
    WebhookActive,
    /// Wait this long, then poll again (None = use exponential backoff).
    Retry(Option<u32>),
}

pub fn classify_poll(body: Option<&Value>) -> PollOutcome {
    let Some(json) = body else {
        return PollOutcome::Retry(None);
    };
    if json.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        return match json.get("result").and_then(|v| v.as_array()) {
            Some(a) => PollOutcome::Updates(a.clone()),
            None => PollOutcome::Retry(None),
        };
    }
    match json.get("error_code").and_then(|v| v.as_i64()).unwrap_or(0) {
        401 | 404 => PollOutcome::Fatal,
        409 if json
            .get("description")
            .and_then(|v| v.as_str())
            .is_some_and(|d| d.to_ascii_lowercase().contains("webhook is active")) =>
        {
            PollOutcome::WebhookActive
        }
        429 => {
            let ra = json
                .get("parameters")
                .and_then(|p| p.get("retry_after"))
                .and_then(|v| v.as_u64())
                .unwrap_or(1)
                .clamp(1, MAX_RETRY_AFTER as u64) as u32;
            PollOutcome::Retry(Some(ra))
        }
        _ => PollOutcome::Retry(None),
    }
}

/// Human readable reason of a failed poll, for logs and `getWebhookInfo.last_error_message`.
pub fn poll_error_text(body: Option<&Value>, transport_error: Option<&str>) -> String {
    if let Some(e) = transport_error {
        return format!("getUpdates failed: {e}");
    }
    match body {
        Some(j) => {
            let code = j.get("error_code").and_then(|v| v.as_i64()).unwrap_or(0);
            let desc = j.get("description").and_then(|v| v.as_str()).unwrap_or("unexpected response");
            format!("getUpdates failed: {code} {desc}")
        }
        None => "getUpdates failed: unreadable response".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn poll_classification() {
        assert_eq!(classify_poll(None), PollOutcome::Retry(None));
        assert_eq!(classify_poll(Some(&json!({"ok":true,"result":[]}))), PollOutcome::Updates(vec![]));
        assert_eq!(classify_poll(Some(&json!({"ok":false,"error_code":401}))), PollOutcome::Fatal);
        assert_eq!(classify_poll(Some(&json!({"ok":false,"error_code":404}))), PollOutcome::Fatal);
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":429,"parameters":{"retry_after":7}}))),
            PollOutcome::Retry(Some(7))
        );
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":429,"parameters":{"retry_after":99999}}))),
            PollOutcome::Retry(Some(3600))
        );
        assert_eq!(classify_poll(Some(&json!({"ok":false,"error_code":409}))), PollOutcome::Retry(None));
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":409,"description":"Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first"}))),
            PollOutcome::WebhookActive
        );
        assert_eq!(
            classify_poll(Some(&json!({"ok":false,"error_code":409,"description":"Conflict: terminated by other getUpdates request"}))),
            PollOutcome::Retry(None)
        );
        assert!(poll_error_text(Some(&json!({"error_code":502,"description":"Bad Gateway"})), None).contains("502 Bad Gateway"));
        assert!(poll_error_text(None, Some("boom")).contains("boom"));
        assert_eq!(classify_poll(Some(&json!({"ok":true}))), PollOutcome::Retry(None));
    }
}
