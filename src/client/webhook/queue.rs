//! Per-chat delivery queues and the scheduler state that owns them.
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use bytes::Bytes;

use super::update::QueueId;
use super::window::{Window, POLL_LIMIT};

/// One loaded update: the exact bytes upstream sent (a slice of the poll reply, not a copy).
pub(super) struct Item {
    pub(super) id: i64,
    pub(super) body: Bytes,
    /// When this update was first seen; the drop timeout is measured from here, per update.
    pub(super) first_seen: Instant,
}

/// Scheduler state: the offset window plus the per-queue FIFOs. A queue key exists exactly while
/// its task is running, so an idle bot holds no per-chat state.
pub(super) struct Scheduler {
    pub(super) window: Window,
    queues: HashMap<QueueId, VecDeque<Item>>,
}

impl Scheduler {
    pub(super) fn new(base: i64) -> Self {
        Self { window: Window::new(base), queues: HashMap::new() }
    }

    /// True when upstream re-sent an update that is already loaded or finished.
    pub(super) fn is_known(&self, id: i64) -> bool {
        self.window.is_known(id)
    }

    /// None: duplicate. Some(true): a new queue was created and its task must be spawned.
    pub(super) fn admit(&mut self, qid: QueueId, item: Item) -> Option<bool> {
        if !self.window.add(item.id) {
            return None;
        }
        match self.queues.entry(qid) {
            Entry::Occupied(mut q) => {
                q.get_mut().push_back(item);
                Some(false)
            }
            Entry::Vacant(v) => {
                v.insert(VecDeque::from([item]));
                Some(true)
            }
        }
    }

    /// First item of a freshly spawned queue task.
    pub(super) fn first(&mut self, qid: QueueId) -> Option<Item> {
        self.queues.get_mut(&qid)?.pop_front()
    }

    /// Marks `done` finished (delivered or dropped) and returns the next item of the queue. An
    /// empty queue is removed, which tells `admit` to spawn a new task for the next update.
    pub(super) fn finish_and_next(&mut self, qid: QueueId, done: i64) -> Option<Item> {
        self.window.finish(done);
        let q = self.queues.get_mut(&qid)?;
        let next = q.pop_front();
        if next.is_none() {
            self.queues.remove(&qid);
        }
        next
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

    #[cfg(test)]
    fn queue_count(&self) -> usize {
        self.queues.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(id: i64) -> Item {
        Item { id, body: Bytes::from(id.to_string()), first_seen: Instant::now() }
    }

    #[test]
    fn queue_is_fifo_and_independent_per_chat() {
        let mut s = Scheduler::new(0);
        let (a, b) = (QueueId::Chat(10), QueueId::Chat(20));
        assert_eq!(s.admit(a, item(1)), Some(true));
        assert_eq!(s.admit(b, item(2)), Some(true));
        assert_eq!(s.admit(a, item(3)), Some(false));
        assert_eq!(s.admit(a, item(4)), Some(false));
        // duplicates (upstream re-sends unconfirmed updates) are ignored
        assert_eq!(s.admit(a, item(3)), None);
        // chat a is strictly ordered, chat b is not held up behind it
        assert_eq!(s.first(a).unwrap().id, 1);
        assert_eq!(s.first(b).unwrap().id, 2);
        assert_eq!(s.finish_and_next(a, 1).unwrap().id, 3);
        assert_eq!(s.finish_and_next(a, 3).unwrap().id, 4);
        assert!(s.finish_and_next(a, 4).is_none());
        assert!(s.finish_and_next(b, 2).is_none());
        // emptied queues are gone (nothing is retained per idle chat) ...
        assert_eq!(s.queue_count(), 0);
        // ... so the next admit spawns a fresh task
        assert_eq!(s.admit(a, item(5)), Some(true));
    }

    #[test]
    fn window_respects_initial_offset_and_bounds() {
        let mut s = Scheduler::new(0);
        // cap = 2 * max_connections: room shrinks as updates are loaded
        assert_eq!(s.poll_window(4), Some((0, 4)));
        for id in 1..=4 {
            s.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(s.poll_window(4), None, "loaded updates reached 2*max_connections");
        s.window.finish(1);
        assert_eq!(s.poll_window(4), Some((2, 4)));
        // limit counts ids upstream re-sends (everything >= offset)
        let mut big = Scheduler::new(0);
        for id in 1..=99 {
            big.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(big.poll_window(1000), Some((1, 100)));
        big.admit(QueueId::Solo(100), item(100));
        assert_eq!(big.poll_window(1000), None, "one poll page is full");
    }
}
