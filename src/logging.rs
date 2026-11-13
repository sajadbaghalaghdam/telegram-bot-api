//! Simple size-rotating log file writer used for `--log` / `--log-max-file-size`.
//! When the file would exceed the limit it is renamed to `<path>.old` (like TDLib's FileLog) and reopened.
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

struct Inner {
    path: PathBuf,
    file: File,
    size: u64,
    max_size: u64,
}

#[derive(Clone)]
pub struct RotatingFile(Arc<Mutex<Inner>>);

impl RotatingFile {
    pub fn open(path: &Path, max_size: i64) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        let max_size = if max_size > 0 { max_size as u64 } else { u64::MAX };
        Ok(Self(Arc::new(Mutex::new(Inner { path: path.to_path_buf(), file, size, max_size }))))
    }
}

impl Inner {
    fn rotate(&mut self) -> io::Result<()> {
        let mut old = self.path.clone().into_os_string();
        old.push(".old");
        let _ = std::fs::rename(&self.path, PathBuf::from(old));
        self.file = OpenOptions::new().create(true).append(true).open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut g = self.0.lock().unwrap_or_else(|e| e.into_inner());
        if g.size > 0 && g.size.saturating_add(buf.len() as u64) > g.max_size {
            g.rotate()?;
        }
        let n = g.file.write(buf)?;
        g.size += n as u64;
        Ok(n)
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
}
