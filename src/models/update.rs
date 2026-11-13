use serde::{Deserialize, Serialize};

/// Request parameters for `setWebhook`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetWebhookRequest {
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_updates: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drop_pending_updates: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secret_token: Option<String>,
}

/// Describes the current status of a webhook.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookInfo {
    pub url: String,
    pub has_custom_certificate: bool,
    pub pending_update_count: i32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_date: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_synchronization_error_date: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allowed_updates: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_webhook_request_serialization() {
        let req = SetWebhookRequest {
            url: "https://example.com/hook".to_string(),
            ip_address: Some("1.2.3.4".to_string()),
            max_connections: Some(50),
            allowed_updates: Some(vec!["message".to_string(), "callback_query".to_string()]),
            drop_pending_updates: Some(true),
            secret_token: Some("secret123".to_string()),
        };
        let json_str = serde_json::to_string(&req).unwrap();
        assert!(json_str.contains("\"allowed_updates\":[\"message\",\"callback_query\"]"));
        assert!(json_str.contains("\"secret_token\":\"secret123\""));

        let deserialized: SetWebhookRequest = serde_json::from_str(&json_str).unwrap();
        assert_eq!(deserialized.url, "https://example.com/hook");
        assert_eq!(
            deserialized.allowed_updates,
            Some(vec!["message".to_string(), "callback_query".to_string()])
        );
        assert_eq!(deserialized.secret_token, Some("secret123".to_string()));
    }
}
