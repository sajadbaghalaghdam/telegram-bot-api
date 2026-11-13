//! Validation of setWebhook parameters.
use std::net::IpAddr;

use crate::client::engine::is_local_webhook_url;

/// Local mode allows up to 100000 connections in C++ (`get_webhook_max_connections`).
pub const MAX_CONNECTIONS_LOCAL: i32 = 100_000;

/// Secret token: at most 256 characters of `[A-Za-z0-9_-]` (empty means "no secret").
pub fn validate_secret_token(s: &str) -> Result<(), &'static str> {
    if s.len() > 256 {
        return Err("secret token is too long");
    }
    if !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-') {
        return Err("secret token contains illegal characters");
    }
    Ok(())
}

/// Clamps like C++ `get_integer_arg(.., default, 1, max)`.
pub fn clamp_max_connections(requested: Option<i32>, default: i32, local_mode: bool) -> i32 {
    let max = if local_mode { MAX_CONNECTIONS_LOCAL } else { 100 };
    requested.unwrap_or(default).clamp(1, max)
}

/// Only locally delivered webhooks are restored/run by this process.
pub fn should_restore(local_mode: bool, url: &str) -> bool {
    local_mode && !url.is_empty() && is_local_webhook_url(url)
}

/// URL must be parseable http/https with a host. Like C++ `parse_url(url, Protocol::Https)` a
/// scheme-less URL is retried with `https://` prepended. Returns the URL string to use.
pub fn normalize_webhook_url(url: &str) -> Result<String, &'static str> {
    let candidate = if url.contains("://") { url.to_string() } else { format!("https://{url}") };
    let u = reqwest::Url::parse(&candidate).map_err(|_| "invalid webhook URL specified")?;
    if (u.scheme() != "http" && u.scheme() != "https") || u.host_str().is_none() {
        return Err("unsupported URL protocol");
    }
    Ok(candidate)
}

/// C++ WebhookActor: "Invalid IP address specified".
pub fn validate_ip_address(ip: &str) -> Result<(), &'static str> {
    ip.parse::<IpAddr>().map(|_| ()).map_err(|_| "invalid IP address specified")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_token() {
        assert!(validate_secret_token("abc_DEF-123").is_ok());
        assert!(validate_secret_token("").is_ok());
        assert!(validate_secret_token(&"a".repeat(256)).is_ok());
        assert!(validate_secret_token(&"a".repeat(257)).is_err());
        assert!(validate_secret_token("bad token").is_err());
        assert!(validate_secret_token("bad!").is_err());
    }

    #[test]
    fn max_connections_clamp() {
        assert_eq!(clamp_max_connections(None, 40, false), 40);
        assert_eq!(clamp_max_connections(Some(0), 40, false), 1);
        assert_eq!(clamp_max_connections(Some(-5), 40, true), 1);
        assert_eq!(clamp_max_connections(Some(5000), 40, false), 100);
        assert_eq!(clamp_max_connections(Some(5000), 40, true), 5000);
    }

    #[test]
    fn restore_filter() {
        assert!(should_restore(true, "http://127.0.0.1:8080/hook"));
        assert!(!should_restore(true, "https://example.com/hook"));
        assert!(!should_restore(true, "http://example.com/hook"));
        assert!(!should_restore(true, ""));
        assert!(!should_restore(false, "http://127.0.0.1:8080/hook"));
    }

    #[test]
    fn url_validation() {
        assert!(normalize_webhook_url("https://example.com/x").is_ok());
        assert!(normalize_webhook_url("http://127.0.0.1:1/").is_ok());
        assert!(normalize_webhook_url("ftp://example.com/").is_err());
        assert!(normalize_webhook_url("not a url").is_err());
    }

    #[test]
    fn scheme_less_url_gets_https() {
        assert_eq!(normalize_webhook_url("example.com/hook").unwrap(), "https://example.com/hook");
        assert_eq!(normalize_webhook_url("localhost:8080/x").unwrap(), "https://localhost:8080/x");
        assert_eq!(normalize_webhook_url("http://127.0.0.1:1/").unwrap(), "http://127.0.0.1:1/");
        assert!(normalize_webhook_url("ftp://example.com/").is_err());
        assert!(normalize_webhook_url("").is_err());
        assert!(normalize_webhook_url("a b").is_err());
    }

    #[test]
    fn ip_address_validation() {
        assert!(validate_ip_address("1.2.3.4").is_ok());
        assert!(validate_ip_address("::1").is_ok());
        assert!(validate_ip_address("example.com").is_err());
        assert!(validate_ip_address("999.1.1.1").is_err());
    }
}
