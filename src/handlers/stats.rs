use crate::client::{ClientManager, TelegramEngine};
use crate::stats::ServerStats;

/// Builds the statistics text from live sources. `active_bot_count` is the number of live bot sessions;
/// `bot_count` is the larger of that and the bots the engine has seen/cached/serves webhooks for.
pub fn render_report(engine: &TelegramEngine, cm: &ClientManager, stats: &ServerStats) -> String {
    let active = cm.active_count();
    let known = engine.active_bots_count().max(active);
    stats.to_report(known, active)
}
