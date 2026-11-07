use axum::{response::{IntoResponse, Response}, Json};
use serde::{Deserialize, Serialize};

/// Generic response envelope for the Telegram Bot API.
/// 
/// All Telegram API responses follow this structure:
/// - `ok`: Boolean indicating if the request was successful.
/// - `result`: The actual response data (present if ok is true).
/// - `error_code`: The Telegram error code (present if ok is false).
/// - `description`: A human-readable error description (present if ok is false).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
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
        }
    }

    /// Creates an error API response.
    #[inline]
    pub fn error(error_code: u16, description: impl Into<String>) -> Self {
        Self {
            ok: false,
            result: None,
            error_code: Some(error_code),
            description: Some(description.into()),
        }
    }
}

impl<T> IntoResponse for ApiResponse<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        Json(self).into_response()
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

    #[test]
    fn test_error_serialization() {
        let resp = ApiResponse::<()>::error(400, "Bad Request");
        let json = serde_json::to_string(&resp).unwrap();
        assert_eq!(json, r#"{"ok":false,"error_code":400,"description":"Bad Request"}"#);
    }
}