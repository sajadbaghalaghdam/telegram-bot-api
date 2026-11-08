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

impl TelegramError {
    /// Maps the internal error variant to the corresponding HTTP status code.
    fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unauthorized(_) => StatusCode::UNAUTHORIZED,
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
            other => (other.to_string(), None),
        };

        let body = TelegramErrorResponse {
            ok: false,
            error_code: status.as_u16(),
            description: desc,
            parameters: params,
        };

        (status, Json(body)).into_response()
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
}
