use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use thiserror::Error;

/// Standard Telegram Bot API error response envelope.
#[derive(Serialize)]
struct TelegramErrorResponse {
    ok: bool,
    error_code: u16,
    description: String,
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
        }
    }
}

impl IntoResponse for TelegramError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        
        let body = TelegramErrorResponse {
            ok: false,
            error_code: status.as_u16(),
            description: self.to_string(),
        };

        (status, Json(body)).into_response()
    }
}

/// Type alias for results within the Telegram Bot API context.
pub type TelegramResult<T> = Result<T, TelegramError>;