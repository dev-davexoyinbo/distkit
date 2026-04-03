use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::sync::watch;

/// Tracks whether a counter has recent activity, driving the flush task's
/// sleep/wake cycle.
///
/// Holds an epoch counter that increments on a background timer, a watch
/// channel to broadcast epoch changes, and a "last committed epoch" so
/// repeated calls are no-ops until the epoch actually advances.
#[derive(Debug)]
pub(crate) struct ActivityTracker {
    epoch: AtomicU64,
    is_active: AtomicBool,
    last_commited_epoch: AtomicU64,
    is_active_watch: watch::Sender<u64>,
    epoch_interval: Duration,
}

impl ActivityTracker {
    pub(crate) fn new(epoch_interval: Duration) -> Arc<Self> {
        let tracker = Arc::new(Self {
            epoch: AtomicU64::new(0), // start at 1 so the first signal() fires immediately
            is_active: AtomicBool::new(false),
            last_commited_epoch: AtomicU64::new(0),
            is_active_watch: watch::Sender::new(0u64),
            epoch_interval,
        });

        tracker.run_is_active_task();
        tracker.run_epoch_task();

        tracker
    }

    fn run_epoch_task(self: &Arc<Self>) {
        self.epoch.fetch_add(1, Ordering::Relaxed);

        let epoch_interval = self.epoch_interval;
        let weak = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(epoch_interval).await;
                let Some(tracker) = weak.upgrade() else {
                    break;
                };

                tracker.epoch.fetch_add(1, Ordering::Relaxed);
            }
        });
    } // end method run_epoch_task

    fn run_is_active_task(self: &Arc<Self>) {
        let mut is_active_watch = self.subscribe();
        let sleep_interval = self.epoch_interval / 2;
        let weak = Arc::downgrade(self);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    val = is_active_watch.changed() => {
                        if val.is_err() {
                            break;
                        }

                        let Some(tracker) = weak.upgrade() else {
                            break;
                        };

                        tracker.is_active.store(true, Ordering::Release);
                    }
                    _ = tokio::time::sleep(sleep_interval) => {
                        let Some(tracker) = weak.upgrade() else {
                            break;
                        };

                        tracker.is_active.store(false, Ordering::Release);
                    }
                }
            }
        });
    } // end method run_is_active_task

    /// Sends an epoch-change signal if the epoch has advanced since the last
    /// signal, waking any subscribers (e.g. the flush task).
    #[inline(always)]
    pub(crate) fn signal(&self) {
        let epoch = self.epoch.load(Ordering::Relaxed);
        if self.last_commited_epoch.load(Ordering::Relaxed) < epoch {
            let _ = self.is_active_watch.send(epoch);
            self.last_commited_epoch.store(epoch, Ordering::Relaxed);
        }
    }

    /// Returns a receiver that wakes whenever [`signal`](Self::signal) sends a
    /// new epoch value.
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.is_active_watch.subscribe()
    }

    pub(crate) fn get_is_active(&self) -> bool {
        self.is_active.load(Ordering::Acquire)
    }
}
