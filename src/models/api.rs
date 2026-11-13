use serde::{Deserialize, Serialize};

/// Contains information about why a request was unsuccessful.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct ResponseParameters {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub migrate_to_chat_id: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<i32>,
}

/// Generic response envelope for the Telegram Bot API.
/// 
/// All Telegram API responses follow this structure:
/// - `ok`: Boolean indicating if the request was successful.
/// - `result`: The actual response data (present if ok is true).
/// - `error_code`: The Telegram error code (present if ok is false).
/// - `description`: A human-readable error description (present if ok is false).
/// - `parameters`: Extra parameters like retry_after or migrate_to_chat_id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters: Option<ResponseParameters>,
}

impl<T> ApiResponse<T> {
    /// Creates a successful API response.
    #[inline]
    pub fn success(result: T) -> Self {
        Self {
            ok: true,
            result: Some(result),
            error_code: None,
            description: None,
            parameters: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_success_serialization() {
        let resp = ApiResponse::success(42);
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":true,"result":42}"#);
    }
}
