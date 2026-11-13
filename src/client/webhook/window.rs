//! The getUpdates offset watermark.
use std::collections::BTreeSet;

/// Upper bound of `limit` of one getUpdates call (Bot API).
pub(super) const POLL_LIMIT: usize = 100;

/// Tracks which update ids are loaded and which are finished (delivered or dropped), so that the
/// getUpdates offset only ever confirms updates below the lowest id that is still unfinished
/// (a watermark): a slow or failing chat can never cause an update of another chat to be lost,
/// and nothing is confirmed upstream before it was delivered (at-least-once).
#[derive(Debug)]
pub(super) struct Window {
    /// Everything below this id is finished and confirmed.
    base: i64,
    max_seen: i64,
    unfinished: BTreeSet<i64>,
    /// Finished ids above the watermark (they are re-sent by upstream until confirmed).
    finished: BTreeSet<i64>,
}

impl Window {
    pub(super) fn new(base: i64) -> Self {
        Self { base, max_seen: base - 1, unfinished: BTreeSet::new(), finished: BTreeSet::new() }
    }

    /// The getUpdates `offset` that is safe to send.
    pub(super) fn offset(&self) -> i64 {
        self.unfinished.first().copied().unwrap_or(self.max_seen + 1).max(self.base)
    }

    pub(super) fn is_known(&self, id: i64) -> bool {
        id < self.offset() || self.unfinished.contains(&id) || self.finished.contains(&id)
    }

    /// Registers a freshly loaded update; false if it was already seen (upstream re-sends
    /// everything that is not confirmed yet).
    pub(super) fn add(&mut self, id: i64) -> bool {
        if self.is_known(id) {
            return false;
        }
        self.unfinished.insert(id);
        self.max_seen = self.max_seen.max(id);
        true
    }

    pub(super) fn finish(&mut self, id: i64) {
        if self.unfinished.remove(&id) {
            self.finished.insert(id);
        }
        self.base = self.offset();
        while self.finished.first().is_some_and(|&f| f < self.base) {
            self.finished.pop_first();
        }
    }

    /// Loaded and not yet delivered/dropped.
    pub(super) fn unfinished(&self) -> usize {
        self.unfinished.len()
    }

    /// Ids upstream will return again on the next poll (everything at or above the offset).
    pub(super) fn occupancy(&self) -> usize {
        self.unfinished.len() + self.finished.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_is_a_watermark() {
        let mut w = Window::new(0);
        assert_eq!(w.offset(), 0);
        for id in [5, 6, 7, 8] {
            assert!(w.add(id));
        }
        assert!(!w.add(6));
        // nothing finished: confirm nothing beyond the first unfinished id
        assert_eq!(w.offset(), 5);
        // later updates finishing does not advance past the stuck one (no loss on crash)
        w.finish(6);
        w.finish(8);
        assert_eq!(w.offset(), 5);
        assert!(!w.add(6) && !w.add(8), "finished-but-unconfirmed ids are not redelivered");
        w.finish(5);
        assert_eq!(w.offset(), 7, "5 and 6 are done, 7 is still pending");
        w.finish(7);
        assert_eq!(w.offset(), 9);
        assert_eq!(w.occupancy(), 0);
        // ids below the offset are never admitted again
        assert!(!w.add(8) && !w.add(1));
        assert!(w.add(9));
    }
}
