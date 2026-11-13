use axum::{
    body::Body,
    extract::{RawPathParams, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::ReaderStream;

use crate::error::{TelegramError, TelegramResult};
use crate::server::SharedState;

/// Streaming chunk size of file bodies.
const CHUNK: usize = 64 * 1024;
/// Non-local servers refuse to serve files above 20 MB (C++ Client.cpp:17042).
const MAX_NON_LOCAL_SIZE: u64 = 20 * 1024 * 1024;

/// What a `Range` request header asks for, relative to a file of known size.
#[derive(Debug, PartialEq, Eq)]
enum ByteRange {
    /// No (usable) range: serve the whole file.
    Full,
    /// `start..=end`, both inside the file.
    Partial { start: u64, end: u64 },
    /// RFC 7233: 416 Range Not Satisfiable.
    Unsatisfiable,
}

fn parse_range(header: Option<&HeaderValue>, total: u64) -> ByteRange {
    let Some(spec) = header
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("bytes="))
    else {
        return ByteRange::Full;
    };
    // Exactly one '-' (a multi-range spec is not supported and ignored).
    let Some((first, second)) = spec.split_once('-').filter(|(_, b)| !b.contains('-')) else {
        return ByteRange::Full;
    };
    let (first, second) = (first.trim(), second.trim());
    let last = total.saturating_sub(1);

    let (start, end) = if first.is_empty() {
        // Suffix range: bytes=-500 (last 500 bytes)
        (total.saturating_sub(second.parse().unwrap_or(0)), last)
    } else if second.is_empty() {
        // Open range: bytes=500- (from 500 to the end)
        (first.parse().unwrap_or(0), last)
    } else {
        // Explicit range: bytes=start-end
        (first.parse().unwrap_or(0), second.parse().unwrap_or(last).min(last))
    };

    if start > end || start >= total {
        ByteRange::Unsatisfiable
    } else {
        ByteRange::Partial { start, end }
    }
}

fn too_big() -> TelegramError {
    TelegramError::BadRequest("Bad Request: file is too big".to_string())
}

/// `(token without the bot prefix, file path)` from the route captures.
fn token_and_file(params: &RawPathParams) -> (&str, &str) {
    let (mut token, mut file_path) = ("", "");
    for (name, value) in params {
        match name {
            "token" => token = value,
            "file_path" => file_path = value,
            _ => {}
        }
    }
    (token.strip_prefix("bot").unwrap_or(token), file_path)
}

/// Serve file content with O(1) memory streaming and HTTP Range support (matching C++ HttpFileConnection)
pub async fn serve_file(
    req_headers: HeaderMap,
    State(state): State<SharedState>,
    params: RawPathParams,
) -> TelegramResult<Response> {
    let (token, file_path) = token_and_file(&params);
    state.engine.check_token_filter(token)?;

    let mut resp_headers = HeaderMap::new();
    if let Ok(ct) = HeaderValue::from_str(mime_guess::from_path(file_path).first_or_octet_stream().as_ref()) {
        resp_headers.insert(header::CONTENT_TYPE, ct);
    }
    resp_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));

    // 1. A file on local disk (local mode only): served directly, with Range support.
    if let Some(local_path) = state.engine.resolve_file_on_disk(token, file_path)
        && let Ok(mut f) = tokio::fs::File::open(&local_path).await
        && let Ok(meta) = f.metadata().await
    {
        let total = meta.len();
        match parse_range(req_headers.get(header::RANGE), total) {
            ByteRange::Unsatisfiable => {
                let mut err_headers = HeaderMap::new();
                err_headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
                if let Ok(cr) = HeaderValue::from_str(&format!("bytes */{total}")) {
                    err_headers.insert(header::CONTENT_RANGE, cr);
                }
                return Ok((StatusCode::RANGE_NOT_SATISFIABLE, err_headers, "").into_response());
            }
            ByteRange::Partial { start, end } if f.seek(std::io::SeekFrom::Start(start)).await.is_ok() => {
                let length = end - start + 1;
                if let Ok(cr) = HeaderValue::from_str(&format!("bytes {start}-{end}/{total}")) {
                    resp_headers.insert(header::CONTENT_RANGE, cr);
                }
                resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(length));
                let body = Body::from_stream(ReaderStream::with_capacity(f.take(length), CHUNK));
                return Ok((StatusCode::PARTIAL_CONTENT, resp_headers, body).into_response());
            }
            _ => {}
        }
        resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(total));
        let body = Body::from_stream(ReaderStream::with_capacity(f, CHUNK));
        return Ok((resp_headers, body).into_response());
    }

    // 2. Fall back to the upstream file stream
    let (body, content_len) = state.engine.open_file_stream(token, file_path).await?;

    if let Some(len) = content_len {
        if !state.engine.local_mode && len > MAX_NON_LOCAL_SIZE {
            return Err(too_big());
        }
        resp_headers.insert(header::CONTENT_LENGTH, HeaderValue::from(len));
    }

    Ok((resp_headers, body).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(h: &str, total: u64) -> ByteRange {
        parse_range(Some(&HeaderValue::from_str(h).unwrap()), total)
    }

    #[test]
    fn range_parsing() {
        use ByteRange::*;
        assert_eq!(parse_range(None, 10), Full);
        assert_eq!(r("bytes=2-5", 10), Partial { start: 2, end: 5 });
        assert_eq!(r("bytes=7-", 10), Partial { start: 7, end: 9 });
        assert_eq!(r("bytes=-3", 10), Partial { start: 7, end: 9 });
        assert_eq!(r("bytes=5-500", 10), Partial { start: 5, end: 9 });
        assert_eq!(r("bytes= 1 - 2 ", 10), Partial { start: 1, end: 2 });
        assert_eq!(r("bytes=10-", 10), Unsatisfiable);
        assert_eq!(r("bytes=5-2", 10), Unsatisfiable);
        assert_eq!(r("bytes=-abc", 10), Unsatisfiable);
        assert_eq!(r("bytes=0-0", 0), Unsatisfiable);
        // not a single byte range: ignored
        assert_eq!(r("bytes=0-1-2", 10), Full);
        assert_eq!(r("bytes=5", 10), Full);
        assert_eq!(r("items=0-1", 10), Full);
    }
}
