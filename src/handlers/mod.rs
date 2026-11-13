pub mod bot;
pub mod files;
pub mod generic;
mod large_upload;
pub mod stats;
pub mod test_dc;

pub use bot::{close, get_me, get_upload_progress, log_out};
pub use files::serve_file;
pub use generic::{fallback_404_handler, relay_handler};
pub use test_dc::{relay_test_handler, serve_test_file};
