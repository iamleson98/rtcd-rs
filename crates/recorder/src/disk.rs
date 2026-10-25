//! The disk a recording may use.
//!
//! Every byte written is counted against the job's volume. Within a margin of it the recording
//! stops capturing and uploads what it has: filling the volume would get the pod evicted and lose
//! the whole recording.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::Notify;

#[derive(Debug)]
struct Inner {
    limit: u64,
    written: AtomicU64,
    full: AtomicBool,
    notify: Notify,
}

/// A shared count of the bytes a recording has written.
#[derive(Debug, Clone)]
pub struct Disk(Arc<Inner>);

impl Disk {
    pub fn new(limit: u64) -> Self {
        Self(Arc::new(Inner {
            limit,
            written: AtomicU64::new(0),
            full: AtomicBool::new(false),
            notify: Notify::new(),
        }))
    }

    /// Counts bytes written.
    pub fn wrote(&self, n: u64) {
        let total = self.0.written.fetch_add(n, Ordering::Relaxed) + n;
        if total >= self.0.limit && !self.0.full.swap(true, Ordering::Relaxed) {
            self.0.notify.notify_one();
        }
    }

    pub fn written(&self) -> u64 {
        self.0.written.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.0.full.load(Ordering::Relaxed)
    }

    /// Resolves once the limit is reached.
    pub async fn full(&self) {
        if self.is_full() {
            return;
        }
        self.0.notify.notified().await;
    }
}
