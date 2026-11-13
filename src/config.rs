use clap::Parser;
use std::net::{AddrParseError, SocketAddr};
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
    #[arg(long = "api-id", env = "TELEGRAM_API_ID")]
    pub api_id: i32,

    /// Application identifier hash for Telegram API access (defaults to TELEGRAM_API_HASH)
    #[arg(long = "api-hash", env = "TELEGRAM_API_HASH")]
    pub api_hash: String,

    /// HTTP listening port (default is 8081)
    #[arg(long = "http-port", short = 'p', env = "TELEGRAM_HTTP_PORT", default_value_t = 8081)]
    pub http_port: u16,

    /// HTTP statistics port
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
    #[arg(long = "http-ip-address", env = "TELEGRAM_HTTP_IP", default_value = "0.0.0.0")]
    pub http_ip_address: String,

    /// Local IP address, HTTP statistics connections to which will be accepted (default 0.0.0.0)
    #[arg(long = "http-stat-ip-address", default_value = "0.0.0.0")]
    pub http_stat_ip_address: String,

    /// Path to the file where the log will be written
    #[arg(long = "log", short = 'l')]
    pub log_file: Option<String>,

    /// Log verbosity level (TDLib scale, default 0: 0-1=error, 2=warn, 3=info, 4=debug, 5+=trace)
    #[arg(long = "verbosity", short = 'v', default_value_t = 0)]
    pub verbosity: i32,

    /// Memory log verbosity level; accepted for compatibility, has no effect (no in-memory log)
    #[arg(long = "memory-verbosity", default_value_t = 3)]
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
}

impl ServerConfig {
    /// Returns the parsed `SocketAddr` for the HTTP server to bind to.
    pub fn address(&self) -> Result<SocketAddr, AddrParseError> {
        Ok(SocketAddr::new(self.http_ip_address.trim().parse()?, self.http_port))
    }

    /// Returns the parsed `SocketAddr` for the dedicated HTTP statistics server.
    pub fn stat_address(&self) -> Option<Result<SocketAddr, AddrParseError>> {
        self.http_stat_port
            .map(|port| Ok(SocketAddr::new(self.http_stat_ip_address.trim().parse()?, port)))
    }

    /// Maps the TDLib-style `--verbosity` level to a tracing filter directive.
    pub fn log_level(&self) -> &'static str {
        match self.verbosity {
            ..=1 => "error",
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

/// Creates `dir` if needed and verifies files can be created in it (C++ startup sanity check).
pub fn ensure_writable_dir(dir: &Path, option: &str) -> Result<(), String> {
    std::fs::create_dir_all(dir).map_err(|e| {
        format!("Can't create directory \"{}\": {e}. Use {option} option to specify a writable directory", dir.display())
    })?;
    let probe = dir.join(format!(".write_probe_{}", std::process::id()));
    std::fs::File::create(&probe).map_err(|e| {
        format!("Can't create files in the directory \"{}\": {e}. Use {option} option to specify a writable directory", dir.display())
    })?;
    let _ = std::fs::remove_file(&probe);
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
        assert!(ensure_writable_dir(&d.join("x"), "--dir").is_ok());
        let f = d.join("file");
        std::fs::write(&f, b"x").unwrap();
        assert!(ensure_writable_dir(&f.join("sub"), "--dir").is_err());
        let _ = std::fs::remove_dir_all(&d);
    }

    #[test]
    fn log_level_scale() {
        let mut c = cfg();
        assert_eq!(c.log_level(), "error");
        c.verbosity = 3;
        assert_eq!(c.log_level(), "info");
        c.verbosity = 9;
        assert_eq!(c.log_level(), "trace");
    }

    #[test]
    fn ipv6_address_parses() {
        let mut c = cfg();
        c.http_ip_address = "::1".into();
        assert_eq!(c.address().unwrap().to_string(), "[::1]:8081");
    }
}
