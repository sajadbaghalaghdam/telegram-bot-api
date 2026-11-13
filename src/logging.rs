//! Simple size-rotating log file writer used for `--log` / `--log-max-file-size`.
//! When the file would exceed the limit it is renamed to `<path>.old` (like TDLib's FileLog) and reopened.
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tracing_subscriber::filter::Targets;

/// Builds the log filter: a non-empty `RUST_LOG` wins, otherwise `default_level` (from `-v`).
/// Supported syntax is the `RUST_LOG` subset `level` and `target=level`, comma separated
/// (e.g. `info`, `off`, `telegram_bot_api_rs=debug,hyper=warn`); span/field directives of
/// `tracing_subscriber::EnvFilter` are not supported (this avoids the regex dependency stack).
pub fn build_filter(rust_log: Option<&str>, default_level: &str) -> Result<Targets, String> {
    let spec = rust_log.map(str::trim).filter(|v| !v.is_empty()).unwrap_or(default_level);
    // Whitespace around directives is tolerated (as in EnvFilter).
    let normalized = spec.split(',').map(str::trim).filter(|d| !d.is_empty()).collect::<Vec<_>>().join(",");
    normalized.parse::<Targets>().map_err(|e| e.to_string())
}

struct Inner {
    path: PathBuf,
    file: File,
    size: u64,
    max_size: u64,
    /// After a failed rotation, do not retry until the file has grown past this size (avoids a retry per line).
    retry_after: u64,
}

#[derive(Clone)]
pub struct RotatingFile(Arc<Mutex<Inner>>);

impl RotatingFile {
    pub fn open(path: &Path, max_size: i64) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        let max_size = if max_size > 0 { max_size as u64 } else { u64::MAX };
        Ok(Self(Arc::new(Mutex::new(Inner { path: path.to_path_buf(), file, size, max_size, retry_after: 0 }))))
    }
}

impl RotatingFile {
    /// Reopens the log file at its path (for external rotation, triggered by SIGUSR1 like C++ `after_rotation`).
    pub fn reopen(&self) -> io::Result<()> {
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        g.file = OpenOptions::new().create(true).append(true).open(&g.path)?;
        g.size = g.file.metadata()?.len();
        Ok(())
    }
}

impl Inner {
    /// Renames the file to `<path>.old` and starts a new one. If the rename fails the current file keeps
    /// being used and its size is NOT reset, so the limit stays meaningful; the failure goes to stderr.
    fn rotate(&mut self) {
        let mut old = self.path.clone().into_os_string();
        old.push(".old");
        if let Err(e) = std::fs::rename(&self.path, PathBuf::from(old)) {
            eprintln!("Failed to rotate log file \"{}\": {e}", self.path.display());
            self.retry_after = self.size.saturating_add(64 * 1024);
            return;
        }
        match OpenOptions::new().create(true).append(true).open(&self.path) {
            Ok(f) => {
                self.file = f;
                self.size = 0;
                self.retry_after = 0;
            }
            Err(e) => {
                // The old handle still points at the renamed file; keep writing there rather than lose logs.
                eprintln!("Failed to reopen log file \"{}\" after rotation: {e}", self.path.display());
                self.retry_after = self.size.saturating_add(64 * 1024);
            }
        }
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        // A tracing event arrives as one buffer (one line), so rotating only between writes never splits a line.
        if g.size > 0 && g.size.saturating_add(buf.len() as u64) > g.max_size && g.size >= g.retry_after {
            g.rotate();
        }
        g.file.write_all(buf)?;
        g.size += buf.len() as u64;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).file.flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RotatingFile {
    type Writer = RotatingFile;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Level;

    #[test]
    fn filter_levels_and_targets() {
        let off = build_filter(None, "off").unwrap();
        assert!(!off.would_enable("x", &Level::ERROR));
        let info = build_filter(Some("  "), "info").unwrap(); // blank RUST_LOG is ignored
        assert!(info.would_enable("x", &Level::INFO) && !info.would_enable("x", &Level::DEBUG));
        let f = build_filter(Some("warn,telegram_bot_api_rs::client=debug, hyper=off"), "off").unwrap();
        assert!(f.would_enable("other", &Level::WARN) && !f.would_enable("other", &Level::INFO));
        assert!(f.would_enable("telegram_bot_api_rs::client::engine", &Level::DEBUG));
        assert!(!f.would_enable("hyper::proto", &Level::ERROR));
        let only_target = build_filter(Some("telegram_bot_api_rs=trace"), "info").unwrap();
        assert!(only_target.would_enable("telegram_bot_api_rs::server", &Level::TRACE));
        assert!(!only_target.would_enable("hyper", &Level::ERROR), "unlisted targets are off, as with EnvFilter");
        assert!(build_filter(Some("info,foo=loud"), "off").is_err());
        assert!(build_filter(Some("foo=bar=baz"), "off").is_err());
    }

    #[test]
    fn rotates_when_limit_exceeded() {
        let dir = std::env::temp_dir().join(format!("tba_log_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.log");
        let mut w = RotatingFile::open(&path, 10).unwrap();
        w.write_all(b"123456").unwrap();
        w.write_all(b"789012").unwrap(); // would exceed 10 -> rotate first
        w.flush().unwrap();
        assert_eq!(std::fs::read(dir.join("a.log.old")).unwrap(), b"123456");
        assert_eq!(std::fs::read(&path).unwrap(), b"789012");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rotation_failure_keeps_size_and_file() {
        let dir = std::env::temp_dir().join(format!("tba_log_fail_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.log");
        let mut w = RotatingFile::open(&path, 10).unwrap();
        // Make "<path>.old" a non-empty directory so rename(file -> dir) fails.
        std::fs::create_dir_all(dir.join("a.log.old/x")).unwrap();
        w.write_all(b"123456").unwrap();
        w.write_all(b"789012").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"123456789012");
        assert_eq!(w.0.lock().unwrap().size, 12, "size must not be reset on failed rotation");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopen_after_external_rename() {
        let dir = std::env::temp_dir().join(format!("tba_log_reopen_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("a.log");
        let mut w = RotatingFile::open(&path, 1000).unwrap();
        w.write_all(b"one").unwrap();
        std::fs::rename(&path, dir.join("moved.log")).unwrap();
        w.reopen().unwrap();
        w.write_all(b"two").unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"two");
        assert_eq!(std::fs::read(dir.join("moved.log")).unwrap(), b"one");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
