//! Per-chat delivery queues and the scheduler state that owns them.
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::time::Instant;

use bytes::Bytes;

use super::update::QueueId;
use super::window::{Window, POLL_LIMIT};

/// One loaded update: the exact bytes upstream sent (a slice of the poll reply, not a copy).
/// The slice keeps its poll reply alive until the update is delivered; fewer than [`POLL_LIMIT`]
/// updates are loaded at any time and a reply holds at most that many, so a bot pins at most
/// `POLL_LIMIT` replies of `POLL_LIMIT` updates each (and in practice one or two).
pub(super) struct Item {
    pub(super) id: i64,
    pub(super) body: Bytes,
    /// When this update was first seen; the drop timeout is measured from here, per update.
    pub(super) first_seen: Instant,
}

/// Parameters of the next getUpdates call.
#[derive(Debug, PartialEq)]
pub(super) struct PollPlan {
    pub(super) offset: i64,
    pub(super) limit: usize,
    /// Updates are loaded but unfinished: upstream still holds them, so a poll answers at once
    /// and long polling would not help.
    pub(super) busy: bool,
    /// Enough new updates fit to justify polling right now; otherwise poll only once
    /// [`MIN_POLL_GAP`] has passed since the previous poll.
    pub(super) worthwhile: bool,
}

/// Minimum spacing of polls that would fetch only a few new updates.
pub(super) const MIN_POLL_GAP: std::time::Duration = std::time::Duration::from_millis(50);

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

    /// The next poll, or None while the window is full: at most `cap` updates may be loaded at
    /// once, and since upstream re-sends everything >= offset, the window (unfinished +
    /// finished-above-watermark) must stay below one poll page.
    pub(super) fn poll_window(&self, cap: usize) -> Option<PollPlan> {
        let room = cap.saturating_sub(self.window.unfinished());
        let occ = self.window.occupancy();
        let fits = room.min(POLL_LIMIT.saturating_sub(occ));
        if fits == 0 {
            return None;
        }
        Some(PollPlan {
            offset: self.window.offset(),
            limit: POLL_LIMIT.min(occ + room),
            busy: occ > 0,
            // A quarter of the largest possible load: every poll re-reads all unconfirmed
            // updates, so polling after each single finished update under load would mostly
            // fetch (and skip) duplicates.
            worthwhile: fits >= (cap.min(POLL_LIMIT) / 4).max(1),
        })
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

    fn plan(s: &Scheduler, cap: usize) -> Option<(i64, usize, bool, bool)> {
        s.poll_window(cap).map(|p| (p.offset, p.limit, p.busy, p.worthwhile))
    }

    #[test]
    fn window_respects_initial_offset_and_bounds() {
        let mut s = Scheduler::new(0);
        // cap = 2 * max_connections: room shrinks as updates are loaded
        assert_eq!(plan(&s, 4), Some((0, 4, false, true)));
        for id in 1..=4 {
            s.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(plan(&s, 4), None, "loaded updates reached 2*max_connections");
        s.window.finish(1);
        assert_eq!(plan(&s, 4), Some((2, 4, true, true)));
        // limit counts ids upstream re-sends (everything >= offset)
        let mut big = Scheduler::new(0);
        for id in 1..=99 {
            big.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(plan(&big, 1000).map(|p| (p.0, p.1)), Some((1, 100)));
        big.admit(QueueId::Solo(100), item(100));
        assert_eq!(plan(&big, 1000), None, "one poll page is full");
    }

    #[test]
    fn polls_that_fetch_few_new_updates_are_not_worthwhile() {
        // cap 200 but one page holds 100: a poll is worthwhile once 25 new updates fit
        let mut s = Scheduler::new(0);
        for id in 1..=80 {
            s.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(plan(&s, 200), Some((1, 100, true, false)), "20 fit: only a rate-limited poll");
        for id in 1..=5 {
            s.window.finish(id);
        }
        assert_eq!(plan(&s, 200), Some((6, 100, true, true)), "25 fit now");
        // small caps: a quarter of the cap, at least one
        let mut small = Scheduler::new(0);
        for id in 1..=7 {
            small.admit(QueueId::Solo(id), item(id));
        }
        assert_eq!(plan(&small, 8), Some((1, 8, true, false)), "1 of 8 fits, 2 needed");
        small.window.finish(1);
        assert_eq!(plan(&small, 8), Some((2, 8, true, true)));
    }
}
