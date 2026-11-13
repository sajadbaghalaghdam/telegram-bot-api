pub mod bot;
pub mod files;
pub mod generic;
pub mod stats;

pub use bot::{close, get_me, log_out};
pub use files::{serve_file, serve_test_file};
pub use generic::{
    fallback_404_handler, generic_get_handler, generic_get_test_handler, generic_post_handler,
    generic_post_test_handler,
};
