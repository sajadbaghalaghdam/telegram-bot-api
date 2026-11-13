//! Local webhook delivery: a long-polling worker per bot that pushes updates to a locally
//! reachable webhook URL (see `is_local_webhook_url`). Cloud (non-local) webhooks are never
//! handled here; they are proxied to the upstream Bot API untouched.
//!
//! Semantics follow the C++ `WebhookActor`:
//! * the worker polls upstream continuously; every update goes into the FIFO queue of its chat
//!   (queue key), each queue has its own delivery task and retry timer, so one failing chat never
//!   blocks the others; at most `max_connections` requests are in flight and at most
//!   `2 * max_connections` updates are loaded at once;
//! * the getUpdates offset is a watermark: it only confirms updates below the lowest id that is
//!   not yet delivered/dropped (`Window`), so delivery stays at-least-once. Upstream re-sends
//!   everything >= offset, so already loaded ids are skipped. Known limitation: one poll page is
//!   100 updates, so once a stuck update has 99 finished successors behind it polling pauses until
//!   it is delivered or dropped;
//! * failures are retried with exponential backoff (`next_retry`), honoring `Retry-After`; an
//!   update is dropped 23h after it was first seen (the timer restarts on process restart);
//! * a 2xx reply that is a JSON object with a `method` field is executed as a Bot API call, in a
//!   detached task (result ignored, never delays delivery);
//! * `pending_update_count` of getWebhookInfo counts updates loaded locally and not yet finished;
//!   the backlog still held by upstream is not included.

mod args;
mod ctx;
mod delivery;
mod poll;
mod queue;
mod registry;
mod reply;
mod restore;
mod retry;
mod state;
mod throttle;
mod update;
mod validation;
mod window;
mod worker;

#[cfg(test)]
mod tests;

pub use args::{parse_request_args, set_webhook_request_from_args, RawForward};
pub use state::WebhookState;
pub use validation::{clamp_max_connections, normalize_webhook_url, should_restore, validate_ip_address, validate_secret_token};
pub use worker::WorkerConfig;
