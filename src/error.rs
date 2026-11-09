use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;
use crate::models::api::ResponseParameters;

/// Standard Telegram Bot API error response envelope matching official C++ Query.cpp
#[derive(Serialize)]
struct TelegramErrorResponse {
    ok: bool,
    error_code: u16,
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    parameters: Option<ResponseParameters>,
}

/// Strongly typed errors for the Telegram Bot API server.
#[derive(Error, Debug)]
pub enum TelegramError {
    #[error("Not Found: {0}")]
    NotFound(String),

    #[error("Bad Request: {0}")]
    BadRequest(String),

    #[error("Unauthorized: {0}")]
    Unauthorized(String),

    #[error("Conflict: {0}")]
    Conflict(String),

    #[error("Misdirected Request: {0}")]
    MisdirectedRequest(String),

    #[error("Internal Server Error: {0}")]
    Internal(String),

    #[error("MTProto Error: {0}")]
    MtProto(String),

    #[error("{description}")]
    ApiError {
        code: u16,
        description: String,
        parameters: Option<ResponseParameters>,
    },
}

fn format_error_prefix(prefix: &str, msg: &str) -> String {
    let needle = format!("{prefix}: ");
    if msg.starts_with(&needle) {
        msg.to_string()
    } else {
        format!("{prefix}: {msg}")
    }
}

impl TelegramError {
    /// Maps the internal error variant to the corresponding HTTP status code.
    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::MisdirectedRequest(_) => StatusCode::MISDIRECTED_REQUEST,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            Self::MtProto(_) => StatusCode::BAD_GATEWAY,
            Self::ApiError { code, .. } => {
                StatusCode::from_u16(*code).unwrap_or(StatusCode::BAD_REQUEST)
            }
        }
    }
}

impl IntoResponse for TelegramError {
    fn into_response(self) -> Response {
        let status = self.status_code();

        let (desc, params) = match self {
            Self::ApiError {
                description,
                parameters,
                ..
            } => (description, parameters),
            Self::NotFound(msg) => (format_error_prefix("Not Found", &msg), None),
            Self::BadRequest(msg) => (format_error_prefix("Bad Request", &msg), None),
            Self::Unauthorized(msg) => (format_error_prefix("Unauthorized", &msg), None),
            Self::Conflict(msg) => (format_error_prefix("Conflict", &msg), None),
            Self::MisdirectedRequest(msg) => (format_error_prefix("Misdirected Request", &msg), None),
            Self::Internal(msg) => (format_error_prefix("Internal Server Error", &msg), None),
            Self::MtProto(msg) => (format_error_prefix("Bad Gateway", &msg), None),
        };

        let body = TelegramErrorResponse {
            ok: false,
            error_code: status.as_u16(),
            description: desc,
            parameters: params.clone(),
        };

        let mut response = (status, Json(body)).into_response();
        if let Some(ref p) = params {
            if let Some(retry_after) = p.retry_after {
                if let Ok(val) = retry_after.to_string().parse() {
                    response.headers_mut().insert(axum::http::header::RETRY_AFTER, val);
                }
            }
        }
        response
    }
}

/// Type alias for results within the Telegram Bot API context.
pub type TelegramResult<T> = Result<T, TelegramError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_telegram_error_response_serialization() {
        let err = TelegramErrorResponse {
            ok: false,
            error_code: 429,
            description: "Too Many Requests: retry after 10".to_string(),
            parameters: Some(ResponseParameters {
                migrate_to_chat_id: None,
                retry_after: Some(10),
            }),
        };
        let json = serde_json::to_string(&err).unwrap();
        assert_eq!(
            json,
            r#"{"ok":false,"error_code":429,"description":"Too Many Requests: retry after 10","parameters":{"retry_after":10}}"#
        );
    }

    #[test]
    fn test_error_prefix_no_duplicate() {
        let err1 = TelegramError::BadRequest("Bad Request: file is too big".to_string());
        let resp1 = err1.into_response();
        assert_eq!(resp1.status(), StatusCode::BAD_REQUEST);

        let err2 = TelegramError::Conflict("webhook already active".to_string());
        let resp2 = err2.into_response();
        assert_eq!(resp2.status(), StatusCode::CONFLICT);

        let err3 = TelegramError::MisdirectedRequest("forbidden token specified".to_string());
        let resp3 = err3.into_response();
        assert_eq!(resp3.status(), StatusCode::MISDIRECTED_REQUEST);
    }
}
