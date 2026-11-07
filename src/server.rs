use std::sync::Arc;
use axum::{
    body::HttpBody,
    extract::{DefaultBodyLimit, Request, State},
    http::header,
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Router,
};
use crate::client::{ClientManager, TelegramEngine};
use crate::config::ServerConfig;
use crate::handlers;
use crate::stats::ServerStats;
use crate::watchdog::Watchdog;

/// AppState holds the shared application state across all request handlers matching official ClientManager.h
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<ServerConfig>,
    pub engine: Arc<TelegramEngine>,
    pub client_manager: Arc<ClientManager>,
    pub stats: Arc<ServerStats>,
    pub watchdog: Arc<Watchdog>,
}

/// Zero-allocation telemetry & watchdog middleware
async fn telemetry_middleware(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let bytes_in = req
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    state.stats.record_request(bytes_in);

    let resp = next.run(req).await;

    let bytes_out = resp
        .body()
        .size_hint()
        .exact()
        .or_else(|| {
            resp.headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
        })
        .unwrap_or(0);

    state.stats.record_response(resp.status().is_success(), bytes_out);
    resp
}

/// Creates the Axum router with defined routes for the Telegram Bot API.
pub fn create_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/stats", get(handlers::get_stats))

        // Fast-path cached session lifecycle routes
        .route("/{token}/getMe", get(handlers::get_me).post(handlers::get_me))
        .route("/{token}/close", post(handlers::close).get(handlers::close))
        .route("/{token}/logOut", post(handlers::log_out).get(handlers::log_out))
        .route("/bot/{token}/getMe", get(handlers::get_me).post(handlers::get_me))
        .route("/bot/{token}/close", post(handlers::close).get(handlers::close))
        .route("/bot/{token}/logOut", post(handlers::log_out).get(handlers::log_out))

        // Universal Routes for 100% of all 185+ Telegram Bot API methods (JSON, Multipart, Form, Query)
        .route("/{token}/{method}", post(handlers::generic_post_handler).get(handlers::generic_get_handler))
        .route("/bot/{token}/{method}", post(handlers::generic_post_handler).get(handlers::generic_get_handler))

        // File download routes (/file/bot{token}/{file_path} and /file/{token}/{file_path})
        .route("/file/{token}/{*file_path}", get(handlers::serve_file))
        .route("/file/bot/{token}/{*file_path}", get(handlers::serve_file))

        .layer(DefaultBodyLimit::disable())
        .layer(middleware::from_fn_with_state(state.clone(), telemetry_middleware))
        .with_state(state)
}

/// Simple health check handler to verify server availability.
async fn health_check() -> &'static str {
    "OK"
}
