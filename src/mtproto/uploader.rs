//! Public upload entry points: upload a local file over MTProto and send it as a document.
//!
//! The heavy lifting lives in sibling modules: [`super::auth`] (bot authorization),
//! [`super::peer`] (InputPeer), [`super::upload_parts`] (part workers), [`super::media`]
//! (sendMedia TL) and [`super::updates`] (response parsing).
use std::io::{Error, ErrorKind};
use std::path::Path;

use super::auth::cache_auth;
use super::media::{build_send_media, UploadedFileRef};
use super::peer::resolve_input_peer;
use super::updates::parse_send_media_updates;
use super::upload_parts::{part_count, upload_parts, PartUpload};

// Items that other modules (src/client) address through `mtproto::uploader::*`.
pub use super::auth::{invalidate_bot_auth, CachedBotAuth};
pub use super::connection::MtProtoConnection;
pub use super::media::{MediaAttrs, MediaKind};

/// Maximum upload size: 4000 parts of 512 KiB (2000 MiB), as in TDLib.
const MAX_UPLOAD_PARTS: usize = 4000;

/// Result of uploading and sending a media file over MTProto
#[derive(Debug, Clone)]
pub struct UploadedMediaResult {
    pub message_id: i32,
    pub date: i64,
    pub doc_id: i64,
    pub access_hash: i64,
    pub file_size: i64,
    pub file_name: String,
    pub mime_type: String,
    pub dc_id: i32,
}

/// Uploads a local file (up to 2000 MB) and sends it to the target peer over MTProto 2.0
/// using parallel worker connections sharing the bot's authorized `auth_key` (matching TDLib's SessionMultiProxy)
/// and `upload.saveBigFilePart#de7b673d` + `messages.sendMedia#330e77f`.
#[allow(clippy::too_many_arguments)]
pub async fn upload_and_send_large_file(
    api_id: i32,
    api_hash: &str,
    bot_token: &str,
    chat_id_str: &str,
    hint_username: Option<&str>,
    file_path: &Path,
    caption: &str,
    custom_file_name: Option<&str>,
) -> Result<UploadedMediaResult, Error> {
    upload_and_send_large_file_typed(
        api_id,
        api_hash,
        bot_token,
        chat_id_str,
        hint_username,
        file_path,
        caption,
        custom_file_name,
        MediaAttrs::default(),
    )
    .await
}

/// Same as [`upload_and_send_large_file`] but preserves the media type (video/audio/voice/...).
#[allow(clippy::too_many_arguments)]
pub async fn upload_and_send_large_file_typed(
    api_id: i32,
    api_hash: &str,
    bot_token: &str,
    chat_id_str: &str,
    hint_username: Option<&str>,
    file_path: &Path,
    caption: &str,
    custom_file_name: Option<&str>,
    media_attrs: MediaAttrs,
) -> Result<UploadedMediaResult, Error> {
    let metadata = tokio::fs::metadata(file_path).await?;
    let file_size = metadata.len() as usize;
    if file_size == 0 {
        return Err(Error::new(ErrorKind::InvalidInput, "Cannot upload an empty file"));
    }
    if part_count(file_size) > MAX_UPLOAD_PARTS {
        return Err(Error::new(ErrorKind::InvalidInput, "File is too big (maximum 2000 MiB)"));
    }

    let file_name = custom_file_name
        .map(|s| s.to_string())
        .or_else(|| {
            file_path
                .file_name()
                .and_then(|n| n.to_str())
                .map(|s| s.to_string())
        })
        .unwrap_or_else(|| "document.bin".to_string());

    let mime_type = mime_guess::from_path(&file_name)
        .first_or_octet_stream()
        .to_string();

    // 1. Connect primary MTProto control session and authenticate bot on its Home DC
    let mut main_conn = MtProtoConnection::connect_home_dc(api_id, api_hash, bot_token).await?;
    let home_dc = main_conn.dc_id;

    // 2. Resolve target InputPeer before starting the heavy upload
    let input_peer = resolve_input_peer(&mut main_conn, chat_id_str, hint_username).await?;

    let shared_auth = main_conn.cached_auth();
    cache_auth(bot_token, shared_auth);

    // 3. Upload the 512 KB parts (translated from FileUploader.cpp)
    let plan = PartUpload {
        path: file_path,
        file_size,
        file_id: rand::random::<i64>(),
        auth: shared_auth,
        api_id,
    };
    // The control connection doubles as the first part worker; another worker's connection
    // (still open) comes back for the final sendMedia.
    let mut send_conn = upload_parts(&plan, main_conn).await?;

    // 4. Send the uploaded document
    let random_id: i64 = rand::random::<i64>();
    let total_parts = part_count(file_size);
    let send_req = build_send_media(
        &input_peer,
        &UploadedFileRef {
            file_id: plan.file_id,
            total_parts,
            is_big: plan.is_big(),
            file_name: &file_name,
            mime_type: &mime_type,
        },
        &media_attrs,
        caption,
        random_id,
    );

    let send_resp = match send_conn.rpc_call(&send_req).await {
        Ok(resp) => resp,
        Err(e) => {
            let mut retry_conn = MtProtoConnection::connect_with_auth(shared_auth, api_id).await?;
            retry_conn.rpc_call(&send_req).await.map_err(|e2| {
                Error::other(format!("MTProto sendMedia failed: {} (retry failed: {})", e, e2),
                )
            })?
        }
    };

    // 5. Parse message_id and document metadata from Updates response
    let (message_id, date, doc_id, access_hash) = parse_send_media_updates(&send_resp, random_id);

    Ok(UploadedMediaResult {
        message_id,
        date,
        doc_id,
        access_hash,
        file_size: file_size as i64,
        file_name,
        mime_type,
        dc_id: home_dc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mtproto::upload_parts::PART_SIZE;

    #[test]
    fn part_layout_matches_512k_parts() {
        assert_eq!(part_count(1), 1);
        assert_eq!(part_count(PART_SIZE), 1);
        assert_eq!(part_count(PART_SIZE + 1), 2);
        assert_eq!(part_count(MAX_UPLOAD_PARTS * PART_SIZE), MAX_UPLOAD_PARTS);
        assert!(part_count(MAX_UPLOAD_PARTS * PART_SIZE + 1) > MAX_UPLOAD_PARTS);
    }
}
