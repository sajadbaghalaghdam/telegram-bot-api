use axum::{
    extract::State,
    response::{IntoResponse, Response},
};
use crate::server::AppState;

/// Handler for /stats endpoint matching official HttpStatConnection.h/HttpStatConnection.cpp
pub async fn get_stats(State(state): State<AppState>) -> Response {
    state.stats.set_active_bots(state.client_manager.active_count());
    let report = state.stats.to_report();
    (
        [("content-type", "text/plain; charset=utf-8")],
        report,
    ).into_response()
}
