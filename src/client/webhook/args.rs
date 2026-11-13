//! setWebhook argument parsing (C++ reads `query->arg(..)` regardless of the content type).
use std::collections::HashMap;

use bytes::Bytes;
use serde_json::Value;

/// The original HTTP request, forwarded unchanged when the webhook is set on the cloud server.
pub struct RawForward<'a> {
    pub query: Option<&'a str>,
    pub content_type: Option<&'a str>,
    pub body: &'a Bytes,
}

fn percent_decode_form(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let h = std::str::from_utf8(&b[i + 1..i + 3]).ok().and_then(|h| u8::from_str_radix(h, 16).ok());
                match h {
                    Some(v) => {
                        out.push(v);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn parse_urlencoded(body: &[u8], out: &mut HashMap<String, String>) {
    for pair in String::from_utf8_lossy(body).split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.insert(percent_decode_form(k), percent_decode_form(v));
    }
}

fn json_scalar_to_string(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

fn find_sub(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    if needle.is_empty() || from > hay.len() {
        return None;
    }
    hay[from..].windows(needle.len()).position(|w| w == needle).map(|p| p + from)
}

fn header_param(disposition: &str, key: &str) -> Option<String> {
    for part in disposition.split(';').skip(1) {
        if let Some((k, v)) = part.trim().split_once('=')
            && k.trim().eq_ignore_ascii_case(key) {
                return Some(v.trim().trim_matches('"').to_string());
            }
    }
    None
}

/// Parses text fields of a multipart/form-data body (file parts such as `certificate` are skipped).
fn parse_multipart(body: &[u8], boundary: &str, out: &mut HashMap<String, String>) -> Result<(), String> {
    const BAD: &str = "can't parse multipart/form-data request body";
    let delim = format!("--{boundary}").into_bytes();
    let mut next_delim = b"\r\n".to_vec();
    next_delim.extend_from_slice(&delim);
    let mut pos = find_sub(body, &delim, 0).ok_or(BAD)? + delim.len();
    loop {
        if body[pos..].starts_with(b"--") {
            return Ok(());
        }
        if !body[pos..].starts_with(b"\r\n") {
            return Err(BAD.into());
        }
        pos += 2;
        let (head_end, body_start) = if body[pos..].starts_with(b"\r\n") {
            (pos, pos + 2)
        } else {
            let e = find_sub(body, b"\r\n\r\n", pos).ok_or(BAD)?;
            (e, e + 4)
        };
        let head = String::from_utf8_lossy(&body[pos..head_end]).into_owned();
        let end = find_sub(body, &next_delim, body_start).ok_or(BAD)?;
        let content = &body[body_start..end];
        let disposition = head
            .lines()
            .find(|l| l.to_ascii_lowercase().starts_with("content-disposition:"))
            .map(|l| l.to_string());
        if let Some(d) = disposition
            && let Some(name) = header_param(&d, "name")
                && header_param(&d, "filename").is_none() {
                    out.insert(name, String::from_utf8_lossy(content).into_owned());
                }
        pos = end + next_delim.len();
        if pos > body.len() {
            return Err(BAD.into());
        }
    }
}

/// Merges the URL query and the request body (JSON, form-urlencoded or multipart) into one
/// argument map, the way C++ `Query::arg` sees them. Body values win over query values.
/// A body that cannot be parsed is an error (never silently treated as "no arguments").
pub fn parse_request_args(
    query: &HashMap<String, String>,
    content_type: Option<&str>,
    body: &[u8],
) -> Result<HashMap<String, String>, String> {
    let mut args = query.clone();
    if body.iter().all(|b| b.is_ascii_whitespace()) {
        return Ok(args);
    }
    let ct = content_type.unwrap_or("").trim();
    let ct_lower = ct.to_ascii_lowercase();
    let starts_obj = body.iter().find(|b| !b.is_ascii_whitespace()) == Some(&b'{');
    if ct_lower.starts_with("multipart/form-data") {
        let boundary = header_param(&format!("x;{ct}"), "boundary").filter(|b| !b.is_empty());
        let boundary = boundary.ok_or_else(|| "multipart/form-data boundary is missing".to_string())?;
        parse_multipart(body, &boundary, &mut args)?;
    } else if ct_lower.starts_with("application/x-www-form-urlencoded") {
        parse_urlencoded(body, &mut args);
    } else if ct_lower.contains("json") || (ct_lower.is_empty() && starts_obj) {
        let v: Value = serde_json::from_slice(body).map_err(|_| "can't parse JSON object request body".to_string())?;
        let obj = v.as_object().ok_or_else(|| "JSON request body must be an object".to_string())?;
        for (k, v) in obj {
            if let Some(s) = json_scalar_to_string(v) {
                args.insert(k.clone(), s);
            }
        }
    } else if ct_lower.is_empty() {
        parse_urlencoded(body, &mut args);
    } else {
        return Err("unsupported request Content-Type".to_string());
    }
    Ok(args)
}

/// C++ `to_bool`: trimmed, case-insensitive "true" / "yes" / "1".
pub fn arg_to_bool(s: &str) -> bool {
    matches!(s.trim().to_ascii_lowercase().as_str(), "true" | "yes" | "1")
}

/// Builds the setWebhook parameters from the merged argument map.
pub fn set_webhook_request_from_args(args: &HashMap<String, String>) -> Result<crate::models::SetWebhookRequest, String> {
    let allowed_updates = match args.get("allowed_updates").map(|s| s.trim()).filter(|s| !s.is_empty()) {
        None => None,
        Some(s) => Some(
            serde_json::from_str::<Vec<String>>(s).map_err(|_| "can't parse allowed_updates as a JSON array of strings".to_string())?,
        ),
    };
    Ok(crate::models::SetWebhookRequest {
        url: args.get("url").cloned().unwrap_or_default(),
        ip_address: args.get("ip_address").cloned(),
        max_connections: args.get("max_connections").and_then(|v| v.trim().parse::<i32>().ok()),
        allowed_updates,
        drop_pending_updates: args.get("drop_pending_updates").map(|v| arg_to_bool(v)),
        secret_token: args.get("secret_token").cloned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn args_from_query_only() {
        let a = parse_request_args(&q(&[("url", "http://127.0.0.1:1/h")]), None, b"").unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "http://127.0.0.1:1/h");
        assert_eq!(r.max_connections, None);
    }

    #[test]
    fn args_from_json_body() {
        let body = br#"{"url":"http://127.0.0.1:1/h","max_connections":7,"drop_pending_updates":true,"secret_token":"s_1","allowed_updates":["message","edited_message"],"ip_address":"127.0.0.1"}"#;
        let a = parse_request_args(&q(&[]), Some("application/json; charset=utf-8"), body).unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "http://127.0.0.1:1/h");
        assert_eq!(r.max_connections, Some(7));
        assert_eq!(r.drop_pending_updates, Some(true));
        assert_eq!(r.secret_token.as_deref(), Some("s_1"));
        assert_eq!(r.allowed_updates, Some(vec!["message".into(), "edited_message".into()]));
        assert_eq!(r.ip_address.as_deref(), Some("127.0.0.1"));
    }

    #[test]
    fn args_from_form_urlencoded_body() {
        let body = b"url=http%3A%2F%2F127.0.0.1%3A1%2Fh%3Fa%3Db&max_connections=9&drop_pending_updates=yes&secret_token=abc&allowed_updates=%5B%22message%22%5D";
        let a = parse_request_args(&q(&[]), Some("application/x-www-form-urlencoded"), body).unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "http://127.0.0.1:1/h?a=b");
        assert_eq!(r.max_connections, Some(9));
        assert_eq!(r.drop_pending_updates, Some(true));
        assert_eq!(r.secret_token.as_deref(), Some("abc"));
        assert_eq!(r.allowed_updates, Some(vec!["message".into()]));
        // no content type at all: curl -d style bodies are form data
        let a = parse_request_args(&q(&[]), None, b"url=http%3A%2F%2Fx.y%2F&a+b=c+d").unwrap();
        assert_eq!(a["url"], "http://x.y/");
        assert_eq!(a["a b"], "c d");
    }

    #[test]
    fn args_from_multipart_body_skips_certificate() {
        let b = "XyZ";
        let body = format!(
            "--{b}\r\nContent-Disposition: form-data; name=\"url\"\r\n\r\nhttps://example.com/hook\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"certificate\"; filename=\"cert.pem\"\r\nContent-Type: application/x-pem-file\r\n\r\n-----BEGIN CERTIFICATE-----\r\nAAA\r\n-----END CERTIFICATE-----\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"max_connections\"\r\n\r\n12\r\n\
             --{b}\r\nContent-Disposition: form-data; name=\"allowed_updates\"\r\n\r\n[\"message\"]\r\n\
             --{b}--\r\n"
        );
        let ct = format!("multipart/form-data; boundary=\"{b}\"");
        let a = parse_request_args(&q(&[("secret_token", "fromquery")]), Some(&ct), body.as_bytes()).unwrap();
        let r = set_webhook_request_from_args(&a).unwrap();
        assert_eq!(r.url, "https://example.com/hook");
        assert_eq!(r.max_connections, Some(12));
        assert_eq!(r.secret_token.as_deref(), Some("fromquery"));
        assert_eq!(r.allowed_updates, Some(vec!["message".into()]));
        assert!(!a.contains_key("certificate"));
    }

    #[test]
    fn unparsable_bodies_are_errors_not_empty_args() {
        assert!(parse_request_args(&q(&[]), Some("application/json"), b"{not json").is_err());
        assert!(parse_request_args(&q(&[]), Some("application/json"), b"[1,2]").is_err());
        assert!(parse_request_args(&q(&[]), Some("multipart/form-data"), b"--x\r\n").is_err());
        assert!(parse_request_args(&q(&[]), Some("multipart/form-data; boundary=x"), b"garbage").is_err());
        assert!(parse_request_args(&q(&[]), Some("text/plain"), b"url=x").is_err());
        let bad = q(&[("allowed_updates", "not json")]);
        assert!(set_webhook_request_from_args(&bad).is_err());
    }

    #[test]
    fn body_overrides_query_and_bool_parsing() {
        let a = parse_request_args(&q(&[("url", "http://q/")]), Some("application/json"), br#"{"url":"http://b/"}"#).unwrap();
        assert_eq!(a["url"], "http://b/");
        assert!(arg_to_bool(" TRUE ") && arg_to_bool("1") && arg_to_bool("yes"));
        assert!(!arg_to_bool("false") && !arg_to_bool(""));
    }
}
