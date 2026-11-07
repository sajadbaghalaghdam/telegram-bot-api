pub mod bot;
pub mod files;
pub mod generic;
pub mod messages;
pub mod stats;
pub mod updates;

pub use bot::{close, get_me, log_out};
pub use files::{
    get_file, send_animation, send_audio, send_document, send_photo, send_video, send_voice,
    serve_file,
};
pub use generic::{generic_get_handler, generic_post_handler};
pub use messages::{delete_message, edit_message_text, send_message};
pub use stats::get_stats;
pub use updates::{delete_webhook, get_updates, get_webhook_info, set_webhook};
