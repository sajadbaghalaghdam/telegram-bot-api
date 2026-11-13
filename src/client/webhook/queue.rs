//! Per-chat delivery queues and the scheduler state that owns them.
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use bytes::Bytes;
use serde_json::Value;

use super::window::{Window, POLL_LIMIT};

fn id_of(v: &Value) -> Option<i64> {
    v.get("id").and_then(|x| x.as_i64())
}

/// Ordering key of an update: its chat (or sender when there is no chat).
pub fn queue_key(update: &Value) -> Option<i64> {
    let obj = update.as_object()?;
    for (k, v) in obj {
        if k == "update_id" || !v.is_object() {
            continue;
        }
        if let Some(id) = v.get("chat").and_then(id_of) {
            return Some(id);
        }
        if let Some(id) = v.get("message").and_then(|m| m.get("chat")).and_then(id_of) {
            return Some(id);
        }
        if let Some(id) = v.get("from").and_then(id_of) {
            return Some(id);
        }
    }
    None
}

/// Identity of a delivery queue. Updates of one chat are delivered strictly in order; updates
/// without any chat/sender get a queue of their own.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueueId {
    Chat(i64),
    Solo(i64),
}

pub fn queue_id(update: &Value, update_id: i64) -> QueueId {
    queue_key(update).map(QueueId::Chat).unwrap_or(QueueId::Solo(update_id))
}

pub(super) struct Item {
    pub(super) id: i64,
    pub(super) body: Bytes,
    /// When this update was first seen; the drop timeout is measured from here, per update.
    pub(super) first_seen: Instant,
}

/// Scheduler state: the offset window plus the per-queue FIFOs. A queue key exists exactly while
/// its task is running.
pub(super) struct Inner {
    pub(super) window: Window,
    pub(super) queues: HashMap<QueueId, VecDeque<Item>>,
}

impl Inner {
    pub(super) fn new(base: i64) -> Self {
        Self { window: Window::new(base), queues: HashMap::new() }
    }

    /// None: duplicate. Some(true): a new queue was created and its task must be spawned.
    pub(super) fn admit(&mut self, qid: QueueId, item: Item) -> Option<bool> {
        if !self.window.add(item.id) {
            return None;
        }
        let spawn = !self.queues.contains_key(&qid);
        self.queues.entry(qid).or_default().push_back(item);
        Some(spawn)
    }

    /// Next item of a queue; removes the (empty) queue, which tells `admit` to spawn a new task.
    pub(super) fn next(&mut self, qid: QueueId) -> Option<Item> {
        let item = self.queues.get_mut(&qid)?.pop_front();
        if item.is_none() {
            self.queues.remove(&qid);
        }
        item
    }

    /// `(offset, limit)` for the next poll, or None while the window is full: at most `cap`
    /// updates may be loaded at once, and since upstream re-sends everything >= offset, the
    /// window (unfinished + finished-above-watermark) must stay below one poll page.
    pub(super) fn poll_window(&self, cap: usize) -> Option<(i64, usize)> {
        let room = cap.saturating_sub(self.window.unfinished());
        let occ = self.window.occupancy();
        if room == 0 || occ >= POLL_LIMIT {
            return None;
        }
        Some((self.window.offset(), POLL_LIMIT.min(occ + room)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn item(id: i64) -> Item {
        Item { id, body: Bytes::from(id.to_string()), first_seen: Instant::now() }
    }

    #[test]
    fn queue_ids() {
        let u = |v: Value| {
            let id = v["update_id"].as_i64().unwrap();
            queue_id(&v, id)
        };
        assert_eq!(u(json!({"update_id":1,"message":{"chat":{"id":10}}})), QueueId::Chat(10));
        assert_eq!(u(json!({"update_id":3,"edited_message":{"chat":{"id":10}}})), QueueId::Chat(10));
        assert_eq!(u(json!({"update_id":4,"callback_query":{"from":{"id":5},"message":{"chat":{"id":20}}}})), QueueId::Chat(20));
        assert_eq!(u(json!({"update_id":5,"inline_query":{"from":{"id":7}}})), QueueId::Chat(7));
        assert_eq!(u(json!({"update_id":6,"poll":{"id":"x"}})), QueueId::Solo(6));
    }

    #[test]
    fn queue_is_fifo_and_independent_per_chat() {
        let mut inner = Inner::new(0);
        let (a, b) = (QueueId::Chat(10), QueueId::Chat(20));
        assert_eq!(inner.admit(a, item(1)), Some(true));
        assert_eq!(inner.admit(b, item(2)), Some(true));
        assert_eq!(inner.admit(a, item(3)), Some(false));
        assert_eq!(inner.admit(a, item(4)), Some(false));
        // duplicates (upstream re-sends unconfirmed updates) are ignored
        assert_eq!(inner.admit(a, item(3)), None);
        // chat a is strictly ordered, chat b is not held up behind it
        assert_eq!(inner.next(a).unwrap().id, 1);
        assert_eq!(inner.next(b).unwrap().id, 2);
        assert_eq!(inner.next(a).unwrap().id, 3);
        assert_eq!(inner.next(a).unwrap().id, 4);
        assert!(inner.next(a).is_none());
        // an emptied queue is gone, so the next admit spawns a fresh task
        assert_eq!(inner.admit(a, item(5)), Some(true));
    }

    #[test]
    fn window_respects_initial_offset_and_bounds() {
        let mut w = Window::new(100);
        assert_eq!(w.offset(), 100);
        assert!(!w.add(99));
        assert!(w.add(100));
        assert_eq!(w.unfinished(), 1);
        let mut inner = Inner::new(0);
        // cap = 2 * max_connections: room shrinks as updates are loaded
        assert_eq!(inner.poll_window(4), Some((0, 4)));
        for id in 1..=4 {
            inner.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(inner.poll_window(4), None, "loaded updates reached 2*max_connections");
        inner.window.finish(1);
        assert_eq!(inner.poll_window(4), Some((2, 4)));
        // limit counts ids upstream re-sends (everything >= offset)
        let mut big = Inner::new(0);
        for id in 1..=99 {
            big.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(big.poll_window(1000), Some((1, 100)));
        big.admit(QueueId::Solo(100), item(100));
        assert_eq!(big.poll_window(1000), None, "one poll page is full");
    }
}
