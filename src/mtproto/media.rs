//! `inputMediaUploadedDocument` building blocks: media kind/attributes, flags and attribute TL.
use bytes::{BufMut, BytesMut};

use super::tl::{write_int, write_long, write_string};

/// Media type used for `inputMediaUploadedDocument` attributes so large uploads keep their
/// original type (video/audio/voice/animation) instead of always becoming a plain document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MediaKind {
    #[default]
    Document,
    Video,
    VideoNote,
    Animation,
    Audio,
    Voice,
}

/// Optional media attributes (0 when unknown).
#[derive(Debug, Clone, Copy, Default)]
pub struct MediaAttrs {
    pub kind: MediaKind,
    pub duration: i32,
    pub width: i32,
    pub height: i32,
    /// `supports_streaming` request flag (sendVideo only, default false as in C++).
    pub supports_streaming: bool,
}

/// `inputMediaUploadedDocument` flags for a media kind. `force_file` (bit 4) makes Telegram
/// treat the upload as a plain file, so it is set only for `MediaKind::Document`; typed media
/// (video/audio/voice/animation) must not carry it or they would be shown as documents.
/// `supports_streaming` is an attribute-level flag written by `write_document_attributes`.
/// `nosound_video` (bit 3) is deliberately not set: the audio presence of the file is unknown.
pub(super) fn document_media_flags(kind: MediaKind) -> i32 {
    if kind == MediaKind::Document { 1 << 4 } else { 0 }
}

/// Writes the `attributes` vector (filename plus kind-specific attribute). The media flags
/// are computed separately by [`document_media_flags`] because they precede the file in the TL.
pub(super) fn write_document_attributes(buf: &mut BytesMut, file_name: &str, attrs: &MediaAttrs) {
    let mut count = 1;
    let extra = attrs.kind != MediaKind::Document;
    if extra {
        count += 1;
    }
    if attrs.kind == MediaKind::Animation {
        count += 1;
    }
    write_int(buf, 0x1cb5c415u32 as i32);
    write_int(buf, count);
    match attrs.kind {
        MediaKind::Video | MediaKind::VideoNote | MediaKind::Animation => {
            // documentAttributeVideo#43c57c48 flags:# round_message:flags.0?true supports_streaming:flags.1?true
            //   duration:double w:int h:int
            if attrs.kind == MediaKind::Animation {
                write_int(buf, 0x11b58939u32 as i32); // documentAttributeAnimated#11b58939
            }
            let flags = match attrs.kind {
                MediaKind::VideoNote => 1,
                MediaKind::Video if attrs.supports_streaming => 2,
                _ => 0,
            };
            write_int(buf, 0x43c57c48u32 as i32);
            write_int(buf, flags);
            buf.put_f64_le(attrs.duration as f64);
            write_int(buf, attrs.width);
            write_int(buf, attrs.height);
        }
        MediaKind::Audio | MediaKind::Voice => {
            // documentAttributeAudio#9852f9c6 flags:# voice:flags.10?true duration:int
            write_int(buf, 0x9852f9c6u32 as i32);
            write_int(buf, if attrs.kind == MediaKind::Voice { 1 << 10 } else { 0 });
            write_int(buf, attrs.duration);
        }
        MediaKind::Document => {}
    }
    write_int(buf, 0x15590068u32 as i32); // documentAttributeFilename#15590068
    write_string(buf, file_name);
}

/// Describes the uploaded file for `messages.sendMedia`.
pub(super) struct UploadedFileRef<'a> {
    pub file_id: i64,
    pub total_parts: usize,
    pub is_big: bool,
    pub file_name: &'a str,
    pub mime_type: &'a str,
}

/// Builds `messages.sendMedia#330e77f` with `inputMediaUploadedDocument#37c9330`.
/// Translated from td/telegram/DocumentsManager.cpp:718 & MessagesManager.cpp:1592.
pub(super) fn build_send_media(
    input_peer: &[u8],
    file: &UploadedFileRef<'_>,
    attrs: &MediaAttrs,
    caption: &str,
    random_id: i64,
) -> BytesMut {
    let mut req = BytesMut::with_capacity(512);

    // messages.sendMedia#330e77f
    write_int(&mut req, 0x0330e77fu32 as i32);
    write_int(&mut req, 0); // flags = 0
    req.put_slice(input_peer);

    // inputMediaUploadedDocument#37c9330
    write_int(&mut req, 0x037c9330u32 as i32);
    write_int(&mut req, document_media_flags(attrs.kind)); // force_file only for plain documents
    if file.is_big {
        // inputFileBig#fa4f0bb5 id:long parts:int name:string
        write_int(&mut req, 0xfa4f0bb5u32 as i32);
        write_long(&mut req, file.file_id);
        write_int(&mut req, file.total_parts as i32);
        write_string(&mut req, file.file_name);
    } else {
        // inputFile#f52ff27f id:long parts:int name:string md5_checksum:string
        write_int(&mut req, 0xf52ff27fu32 as i32);
        write_long(&mut req, file.file_id);
        write_int(&mut req, file.total_parts as i32);
        write_string(&mut req, file.file_name);
        write_string(&mut req, "");
    }
    write_string(&mut req, file.mime_type);

    // attributes: Vector<DocumentAttribute>
    write_document_attributes(&mut req, file.file_name, attrs);

    // message (caption) & random_id
    write_string(&mut req, caption);
    write_long(&mut req, random_id);
    req
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_flags_force_file_only_for_documents() {
        assert_eq!(document_media_flags(MediaKind::Document), 1 << 4);
        for k in [MediaKind::Video, MediaKind::VideoNote, MediaKind::Animation, MediaKind::Audio, MediaKind::Voice] {
            assert_eq!(document_media_flags(k), 0);
        }
    }

    #[test]
    fn document_attributes_by_kind() {
        let mut b = BytesMut::new();
        write_document_attributes(&mut b, "a.mp4", &MediaAttrs::default());
        assert_eq!(&b[4..8], &1i32.to_le_bytes());

        let mut b = BytesMut::new();
        let attrs = MediaAttrs { kind: MediaKind::Voice, duration: 5, ..Default::default() };
        write_document_attributes(&mut b, "a.ogg", &attrs);
        assert_eq!(&b[4..8], &2i32.to_le_bytes());
        assert_eq!(&b[8..12], &0x9852f9c6u32.to_le_bytes());
        assert_eq!(&b[12..16], &(1i32 << 10).to_le_bytes());

        let mut b = BytesMut::new();
        let attrs = MediaAttrs { kind: MediaKind::Animation, ..Default::default() };
        write_document_attributes(&mut b, "a.gif", &attrs);
        assert_eq!(&b[4..8], &3i32.to_le_bytes());
        // Animated attr, then video attr whose flags (never supports_streaming for animations) are 0.
        assert_eq!(&b[8..12], &0x11b58939u32.to_le_bytes());
        assert_eq!(&b[12..16], &0x43c57c48u32.to_le_bytes());
        assert_eq!(&b[16..20], &0i32.to_le_bytes());
    }

    #[test]
    fn video_supports_streaming_flag_only_when_requested() {
        let flags_of = |attrs: MediaAttrs| {
            let mut b = BytesMut::new();
            write_document_attributes(&mut b, "a.mp4", &attrs);
            // vector header (8) + attr constructor (4) -> flags
            i32::from_le_bytes(b[12..16].try_into().unwrap())
        };
        assert_eq!(flags_of(MediaAttrs { kind: MediaKind::Video, ..Default::default() }), 0);
        assert_eq!(flags_of(MediaAttrs { kind: MediaKind::Video, supports_streaming: true, ..Default::default() }), 2);
        assert_eq!(flags_of(MediaAttrs { kind: MediaKind::VideoNote, ..Default::default() }), 1);
    }
}
