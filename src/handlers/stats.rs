use axum::{
    extract::State,
    response::{IntoResponse, Response},
};
use crate::client::{ClientManager, TelegramEngine};
use crate::server::AppState;
use crate::stats::ServerStats;

/// Builds the statistics text from live sources. `active_bot_count` is the number of live bot sessions;
/// `bot_count` is the larger of that and the bots the engine has seen/cached/serves webhooks for.
pub fn render_report(engine: &TelegramEngine, cm: &ClientManager, stats: &ServerStats) -> String {
    let active = cm.active_count();
    let known = engine.active_bots_count().max(active);
    stats.to_report(known, active)
}

/// Handler for /stats endpoint matching official HttpStatConnection.h/HttpStatConnection.cpp
pub async fn get_stats(State(state): State<AppState>) -> Response {
    let report = render_report(&state.engine, &state.client_manager, &state.stats);
    ([("content-type", "text/plain")], report).into_response()
}
