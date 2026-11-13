//! Progress of the large-file uploads in flight, by the local path of the file, so a client
//! (the bot that asked for the upload) can poll `getUploadProgress` while its request is open.
//!
//! The part workers add the bytes of each stored batch; the entry goes when the transfer ends
//! (done, failed or cancelled). Nothing here is persisted.
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex};

/// One upload's counters.
#[derive(Debug)]
pub struct Entry {
    total: u64,
    done: AtomicU64,
}

impl Entry {
    /// `bytes` more are stored on Telegram's side.
    pub fn add(&self, bytes: u64) {
        self.done.fetch_add(bytes, Ordering::Relaxed);
    }
}

static ACTIVE: LazyLock<Mutex<HashMap<String, Arc<Entry>>>> = LazyLock::new(Default::default);

/// Keeps an upload listed; dropping it takes it off the list.
pub struct Tracking {
    key: String,
    entry: Arc<Entry>,
}

impl Tracking {
    pub fn entry(&self) -> Arc<Entry> {
        Arc::clone(&self.entry)
    }
}

impl Drop for Tracking {
    fn drop(&mut self) {
        let mut active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
        // Only its own entry: another upload of the same path may have started since.
        if active.get(&self.key).is_some_and(|e| Arc::ptr_eq(e, &self.entry)) {
            active.remove(&self.key);
        }
    }
}

/// Lists an upload of `path` (`total` bytes). Two at once for one path: the later is shown.
pub fn start(path: &Path, total: u64) -> Tracking {
    let key = path.to_string_lossy().into_owned();
    let entry = Arc::new(Entry { total, done: AtomicU64::new(0) });
    ACTIVE.lock().unwrap_or_else(|e| e.into_inner()).insert(key.clone(), Arc::clone(&entry));
    Tracking { key, entry }
}

/// `(done bytes, total bytes)` of the upload of `path` now running; `None` when there is none.
pub fn snapshot(path: &str) -> Option<(u64, u64)> {
    let active = ACTIVE.lock().unwrap_or_else(|e| e.into_inner());
    let entry = active.get(path)?;
    Some((entry.done.load(Ordering::Relaxed).min(entry.total), entry.total))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listed_while_tracked_and_gone_after() {
        let path = Path::new("/tmp/upload-progress-test/a.mp4");
        assert_eq!(snapshot("/tmp/upload-progress-test/a.mp4"), None);
        let tracking = start(path, 1000);
        assert_eq!(snapshot("/tmp/upload-progress-test/a.mp4"), Some((0, 1000)));
        tracking.entry().add(400);
        tracking.entry().add(700);
        // Never past the total.
        assert_eq!(snapshot("/tmp/upload-progress-test/a.mp4"), Some((1000, 1000)));
        drop(tracking);
        assert_eq!(snapshot("/tmp/upload-progress-test/a.mp4"), None);
    }

    #[test]
    fn an_older_upload_ending_leaves_the_newer_listed() {
        let path = Path::new("/tmp/upload-progress-test/b.mp4");
        let older = start(path, 10);
        let newer = start(path, 20);
        drop(older);
        assert_eq!(snapshot("/tmp/upload-progress-test/b.mp4"), Some((0, 20)));
        drop(newer);
        assert_eq!(snapshot("/tmp/upload-progress-test/b.mp4"), None);
    }
}
