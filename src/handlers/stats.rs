use axum::{
    extract::State,
    response::{IntoResponse, Response},
};
use crate::server::AppState;

/// Handler for /stats endpoint matching official HttpStatConnection.h/HttpStatConnection.cpp
pub async fn get_stats(State(state): State<AppState>) -> Response {
    let active = state
        .engine
        .active_bots_count()
        .max(state.client_manager.active_count());
    state.stats.set_active_bots(active);
    let report = state.stats.to_report();
    (
        [("content-type", "text/plain")],
        report,
    )
        .into_response()
}
