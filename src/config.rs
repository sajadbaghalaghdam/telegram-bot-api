use clap::Parser;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

/// Server configuration matching 100% of official tdlib/telegram-bot-api CLI options (`telegram-bot-api.cpp`).
#[derive(Parser, Debug, Clone)]
#[command(
    author,
    version = "10.3",
    about = "Telegram Bot API server (Pure Rust 2024 Edition)",
    long_about = None
)]
pub struct ServerConfig {
    /// Allow the Bot API server to serve local requests
    #[arg(long = "local", default_value_t = false)]
    pub local_mode: bool,

    /// Application identifier for Telegram API access (defaults to TELEGRAM_API_ID)
    /// Validated in `validate()` with the C++ message; 0 means "not provided".
    #[arg(long = "api-id", env = "TELEGRAM_API_ID", default_value_t = 0)]
    pub api_id: i32,

    /// Application identifier hash for Telegram API access (defaults to TELEGRAM_API_HASH)
    /// Only non-emptiness is checked (as in C++); Telegram validates the value itself.
    #[arg(long = "api-hash", env = "TELEGRAM_API_HASH", default_value = "")]
    pub api_hash: String,

    /// HTTP listening port (default is 8081)
    #[arg(long = "http-port", short = 'p', env = "TELEGRAM_HTTP_PORT", default_value_t = 8081, value_parser = clap::value_parser!(u16).range(1..))]
    pub http_port: u16,

    /// HTTP statistics port (0 or unset disables the statistics server)
    #[arg(long = "http-stat-port", short = 's', env = "TELEGRAM_HTTP_STAT_PORT")]
    pub http_stat_port: Option<u16>,

    /// Server working directory
    #[arg(long = "dir", short = 'd', env = "TELEGRAM_DIR", default_value = ".")]
    pub dir: String,

    /// Directory for storing auxiliary files (defaults to the working directory)
    #[arg(long = "files-dir", short = 'f')]
    pub files_dir: Option<String>,

    /// Directory for storing HTTP server temporary files (relative paths are resolved against --files-dir)
    #[arg(long = "temp-dir", short = 't', env = "TELEGRAM_TEMP_DIR", default_value = "temp")]
    pub temp_dir: String,

    /// "<remainder>/<modulo>". Allow only bots with 'bot_user_id % modulo == remainder'
    #[arg(long = "filter")]
    pub filter: Option<String>,

    /// Default value of the maximum webhook connections per bot (defaults to 100 in --local mode, 40 otherwise)
    #[arg(long = "max-webhook-connections", default_value_t = 0)]
    pub max_webhook_connections: i32,

    /// Local IP address, HTTP connections to which will be accepted (default 0.0.0.0)
    #[arg(long = "http-ip-address", env = "TELEGRAM_HTTP_IP", default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    pub http_ip_address: IpAddr,

    /// Local IP address, HTTP statistics connections to which will be accepted (default 0.0.0.0)
    #[arg(long = "http-stat-ip-address", default_value_t = IpAddr::V4(Ipv4Addr::UNSPECIFIED))]
    pub http_stat_ip_address: IpAddr,

    /// Path to the file where the log will be written
    #[arg(long = "log", short = 'l')]
    pub log_file: Option<String>,

    /// Log verbosity level (TDLib scale, default 0: 0=fatal/off, 1=error, 2=warn, 3=info, 4=debug, 5+=trace).
    /// A non-empty RUST_LOG environment variable overrides it.
    #[arg(long = "verbosity", short = 'v', default_value_t = 0, allow_negative_numbers = true)]
    pub verbosity: i32,

    /// Memory log verbosity level; accepted for compatibility, has no effect (no in-memory log)
    #[arg(long = "memory-verbosity", default_value_t = 3, allow_negative_numbers = true)]
    pub memory_verbosity: i32,

    /// Maximum size of the log file in bytes before it is rotated to "<log>.old" (default is 2000000000)
    #[arg(long = "log-max-file-size", default_value_t = 2_000_000_000)]
    pub log_max_file_size: i64,

    /// Effective user name to switch to
    #[arg(long = "username", short = 'u')]
    pub username: Option<String>,

    /// Effective group name to switch to
    #[arg(long = "groupname", short = 'g')]
    pub groupname: Option<String>,

    /// Maximum number of open file descriptors
    #[arg(long = "max-connections", short = 'c', default_value_t = 0)]
    pub max_connections: u64,

    /// CPU affinity as 64-bit mask (defaults to all available CPUs)
    #[arg(long = "cpu-affinity", default_value_t = 0)]
    pub cpu_affinity: u64,

    /// CPU affinity of the main thread as 64-bit mask (defaults to --cpu-affinity)
    #[arg(long = "main-thread-affinity", default_value_t = 0)]
    pub main_thread_affinity: u64,

    /// HTTP proxy server for outgoing webhook requests in the format http://host:port
    #[arg(long = "proxy", env = "TELEGRAM_PROXY")]
    pub proxy: Option<String>,

    /// Base URL of the upstream Bot API the server relays to (Rust-port extension, no C++ equivalent;
    /// meant for tests, benchmarks and private mirrors). Must be an http(s) URL without query or fragment.
    #[arg(
        long = "upstream-url",
        env = "TELEGRAM_UPSTREAM_URL",
        default_value = DEFAULT_UPSTREAM_URL,
        value_parser = parse_upstream_url
    )]
    pub upstream_url: String,
}

/// Production upstream used unless `--upstream-url` / `TELEGRAM_UPSTREAM_URL` says otherwise.
pub const DEFAULT_UPSTREAM_URL: &str = "https://api.telegram.org";

/// Validates `--upstream-url` and returns it without trailing slashes (the engine appends `/bot<token>/...`).
fn parse_upstream_url(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let url = reqwest::Url::parse(trimmed).map_err(|e| format!("invalid URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("scheme must be http or https".into());
    }
    if url.host_str().is_none_or(str::is_empty) {
        return Err("URL must contain a host".into());
    }
    if url.query().is_some() || url.fragment().is_some() {
        return Err("URL must not contain a query or fragment".into());
    }
    Ok(trimmed.to_string())
}

impl ServerConfig {
    /// `SocketAddr` for the main HTTP listener (IP already validated at parse time).
    pub fn address(&self) -> SocketAddr {
        SocketAddr::new(self.http_ip_address, self.http_port)
    }

    /// `SocketAddr` for the statistics listener; `None` when `-s` is unset or 0 (C++ `http_stat_port != 0`).
    pub fn stat_address(&self) -> Option<SocketAddr> {
        match self.http_stat_port {
            Some(port) if port != 0 => Some(SocketAddr::new(self.http_stat_ip_address, port)),
            _ => None,
        }
    }

    /// Option checks matching C++ `options.add_check` (telegram-bot-api.cpp:304-321).
    pub fn validate(&self) -> Result<(), String> {
        if self.api_id <= 0 || self.api_hash.trim().is_empty() {
            return Err("You must provide valid api-id and api-hash obtained at https://my.telegram.org".into());
        }
        if self.verbosity < 0 {
            return Err("Wrong verbosity level specified".into());
        }
        if self.memory_verbosity < 0 {
            return Err("Wrong memory verbosity level specified".into());
        }
        Ok(())
    }

    /// Maps the TDLib-style `--verbosity` level to a tracing filter directive.
    pub fn log_level(&self) -> &'static str {
        match self.verbosity {
            // tracing has no fatal level; C++ level 0 logs only fatal errors, so silence everything.
            ..=0 => "off",
            1 => "error",
            2 => "warn",
            3 => "info",
            4 => "debug",
            _ => "trace",
        }
    }

    /// Resolves a possibly relative path against `base` (C++ prefixes relative --temp-dir / --log with files-dir).
    pub fn resolve_in(base: &Path, p: &str) -> PathBuf {
        let pb = PathBuf::from(p);
        if pb.is_absolute() { pb } else { base.join(pb) }
    }

    /// Returns effective default max webhook connections matching C++ line 483 (`local_mode_ ? 100 : 40`)
    pub fn effective_max_webhook_connections(&self) -> i32 {
        if self.max_webhook_connections > 0 {
            self.max_webhook_connections
        } else if self.local_mode {
            100
        } else {
            40
        }
    }

    /// Parses `--filter "<remainder>/<modulo>"` matching C++ lines 232-245
    pub fn parsed_filter(&self) -> Result<Option<(u64, u64)>, String> {
        let Some(ref raw) = self.filter else {
            return Ok(None);
        };
        let Some((rem_s, mod_s)) = raw.split_once('/') else {
            return Err("Wrong --filter format: expected <remainder>/<modulo>".to_string());
        };
        let rem: u64 = rem_s.parse().map_err(|_| "Invalid remainder in --filter".to_string())?;
        let modulo: u64 = mod_s.parse().map_err(|_| "Invalid modulo in --filter".to_string())?;
        if modulo == 0 || rem >= modulo {
            return Err("Wrong argument specified: ensure that remainder < modulo".to_string());
        }
        Ok(Some((rem, modulo)))
    }

    /// Normalizes `--proxy` URL (prepends `http://` if scheme is omitted, matching C++ lines 296-302)
    pub fn normalized_proxy(&self) -> Option<String> {
        self.proxy.as_ref().map(|p| {
            let trimmed = p.trim();
            if trimmed.starts_with("http://") || trimmed.starts_with("https://") || trimmed.starts_with("socks5://") {
                trimmed.to_string()
            } else {
                format!("http://{trimmed}")
            }
        })
    }
}

/// Creates `dir` if needed and verifies that files and subdirectories can be created in it
/// (C++ telegram-bot-api.cpp:344-370: mkpath + mkstemp + mkdtemp). `label` is e.g. "working"/"files"/"temporary".
pub fn ensure_writable_dir(dir: &Path, option: &str, label: &str) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| format!("Failed to create {label} directory: {e}"))?;
    let tag = std::process::id();
    let probe = dir.join(format!(".write_probe_{tag}"));
    std::fs::File::create(&probe).map_err(|_| {
        format!(
            "Can't create files in the directory \"{}\". Use {option} option to specify a writable {label} directory",
            dir.display()
        )
    })?;
    let _ = std::fs::remove_file(&probe);
    let probe_dir = dir.join(format!(".write_probe_dir_{tag}"));
    std::fs::create_dir(&probe_dir).map_err(|_| {
        format!(
            "Can't create directories in the directory \"{}\". Use {option} option to specify a writable {label} directory",
            dir.display()
        )
    })?;
    let _ = std::fs::remove_dir(&probe_dir);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> ServerConfig {
        ServerConfig::parse_from(["x", "--api-id", "1", "--api-hash", "h"])
    }

    #[test]
    fn resolve_relative_and_absolute() {
        assert_eq!(ServerConfig::resolve_in(Path::new("/a"), "temp"), PathBuf::from("/a/temp"));
        assert_eq!(ServerConfig::resolve_in(Path::new("/a"), "/b/t"), PathBuf::from("/b/t"));
    }

    #[test]
    fn writable_dir_check() {
        let d = std::env::temp_dir().join(format!("tba_cfg_test_{}", std::process::id()));
        assert!(ensure_writable_dir(&d.join("x"), "--dir", "working").is_ok());
        assert_eq!(std::fs::read_dir(d.join("x")).unwrap().count(), 0, "probes cleaned up");
        let f = d.join("file");
        std::fs::write(&f, b"x").unwrap();
        let e = ensure_writable_dir(&f.join("sub"), "--dir", "working").unwrap_err();
        assert!(e.starts_with("Failed to create working directory"), "{e}");
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn log_level_scale() {
        let mut c = cfg();
        assert_eq!(c.log_level(), "off");
        c.verbosity = 1;
        assert_eq!(c.log_level(), "error");
        c.verbosity = 3;
        assert_eq!(c.log_level(), "info");
        c.verbosity = 9;
        assert_eq!(c.log_level(), "trace");
    }

    #[test]
    fn ipv6_address_parses() {
        let c = ServerConfig::parse_from(["x", "--api-id", "1", "--api-hash", "h", "--http-ip-address", "::1"]);
        assert_eq!(c.address().to_string(), "[::1]:8081");
    }

    fn try_cfg(args: &[&str]) -> Result<ServerConfig, clap::Error> {
        let mut v = vec!["x", "--api-id", "1", "--api-hash", "h"];
        v.extend_from_slice(args);
        ServerConfig::try_parse_from(v)
    }

    #[test]
    fn ports_and_ips_validated_at_parse_time() {
        assert!(try_cfg(&["-p", "0"]).is_err());
        assert!(try_cfg(&["-p", "65536"]).is_err());
        assert!(try_cfg(&["--http-ip-address", "nope"]).is_err());
        assert!(try_cfg(&["--http-stat-ip-address", "1.2.3"]).is_err());
        assert!(try_cfg(&["-s", "0"]).unwrap().stat_address().is_none());
        assert!(try_cfg(&[]).unwrap().stat_address().is_none());
        assert_eq!(try_cfg(&["-s", "9"]).unwrap().stat_address().unwrap().port(), 9);
    }

    #[test]
    fn upstream_url_default_and_validation() {
        assert_eq!(cfg().upstream_url, "https://api.telegram.org");
        assert_eq!(try_cfg(&["--upstream-url", "http://127.0.0.1:9/"]).unwrap().upstream_url, "http://127.0.0.1:9");
        assert_eq!(
            try_cfg(&["--upstream-url", "https://mirror.example/api//"]).unwrap().upstream_url,
            "https://mirror.example/api"
        );
        for bad in ["", "api.telegram.org", "ftp://x", "http://", "http://h/?a=1", "http://h/#f", "not a url"] {
            assert!(try_cfg(&["--upstream-url", bad]).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn validate_matches_cpp_messages() {
        assert!(cfg().validate().is_ok());
        let c = ServerConfig::parse_from(["x", "--api-id", "0", "--api-hash", "h"]);
        assert!(c.validate().unwrap_err().starts_with("You must provide valid api-id"));
        let c = ServerConfig::parse_from(["x", "--api-id", "1", "--api-hash", " "]);
        assert!(c.validate().is_err());
        assert_eq!(try_cfg(&["-v", "-1"]).unwrap().validate().unwrap_err(), "Wrong verbosity level specified");
        assert_eq!(
            try_cfg(&["--memory-verbosity", "-1"]).unwrap().validate().unwrap_err(),
            "Wrong memory verbosity level specified"
        );
    }
}
