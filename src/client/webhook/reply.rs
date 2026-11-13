//! Webhook replies that carry a Bot API method call.
use serde_json::Value;

/// C++ WebhookActor.cpp:620: a reply with a `method` other than delete/setWebhook/close/logOut/get*.
pub(super) fn reply_method(body: &[u8]) -> Option<String> {
    let v: Value = serde_json::from_slice(body).ok()?;
    let m = v.get("method")?.as_str()?.to_ascii_lowercase();
    if m.is_empty() || matches!(m.as_str(), "deletewebhook" | "setwebhook" | "close" | "logout") || m.starts_with("get") {
        return None;
    }
    Some(m)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reply_methods() {
        assert_eq!(reply_method(br#"{"method":"sendMessage","chat_id":1,"text":"x"}"#).as_deref(), Some("sendmessage"));
        assert_eq!(reply_method(br#"{"method":"getMe"}"#), None);
        assert_eq!(reply_method(br#"{"method":"deleteWebhook"}"#), None);
        assert_eq!(reply_method(b"OK"), None);
        assert_eq!(reply_method(b""), None);
        assert_eq!(reply_method(br#"{"ok":true}"#), None);
    }
}
