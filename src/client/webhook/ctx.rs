//! State shared by the worker loop and its queue tasks.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use tokio::sync::{Notify, Semaphore};

use super::delivery::Target;
use super::queue::{Item, Scheduler};
use super::reply::ReplySink;
use super::retry::GoneTracker;
use super::state::WebhookState;
use super::update::QueueId;

pub(super) struct Ctx {
    pub(super) target: Target,
    pub(super) replies: ReplySink,
    /// At most `max_connections` requests in flight.
    pub(super) permits: Semaphore,
    pub(super) state: Arc<WebhookState>,
    gone: GoneTracker,
    /// Set when 410 replies lasted longer than the drop timeout: the webhook is closed.
    pub(super) closed: AtomicBool,
    sched: Mutex<Scheduler>,
    /// Signalled whenever an update finished (the watermark may have advanced).
    pub(super) progress: Notify,
    pub(super) closed_notify: Notify,
}

impl Ctx {
    pub(super) fn new(target: Target, replies: ReplySink, state: Arc<WebhookState>, max_connections: usize, base_offset: i64) -> Self {
        Self {
            target,
            replies,
            permits: Semaphore::new(max_connections),
            state,
            gone: GoneTracker::new(),
            closed: AtomicBool::new(false),
            sched: Mutex::new(Scheduler::new(base_offset)),
            progress: Notify::new(),
            closed_notify: Notify::new(),
        }
    }

    /// The scheduler lock is only ever held for short, non-blocking sections (never across an
    /// `.await`).
    pub(super) fn lock(&self) -> MutexGuard<'_, Scheduler> {
        self.sched.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn sync_pending(&self, sched: &Scheduler) {
        self.state.pending.store(sched.window.unfinished() as i64, Ordering::Relaxed);
    }

    /// The webhook answered 2xx.
    pub(super) fn note_delivered(&self) {
        self.gone.delivered();
    }

    /// A delivery attempt failed (`is_410`: the webhook answered 410 Gone). Returns true, after
    /// marking the webhook closed, when 410 replies have lasted longer than the drop timeout.
    pub(super) fn note_failure(&self, is_410: bool) -> bool {
        let close = self.gone.failed(is_410);
        if close {
            self.closed.store(true, Ordering::Relaxed);
        }
        close
    }

    /// First item of a queue whose task was just spawned.
    pub(super) fn first(&self, qid: QueueId) -> Option<Item> {
        self.lock().first(qid)
    }

    /// Marks `id` finished, wakes the worker (the watermark may have advanced) and returns the
    /// next item of the queue, if any.
    pub(super) fn finish_and_next(&self, qid: QueueId, id: i64) -> Option<Item> {
        let next = {
            let mut g = self.lock();
            let next = g.finish_and_next(qid, id);
            self.sync_pending(&g);
            next
        };
        self.progress.notify_one();
        next
    }
}
