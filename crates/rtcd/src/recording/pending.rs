//! How much of this node's disk the files waiting for upload hold.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::metrics::Metrics;

/// The bytes of closed files that are not uploaded yet, all recordings of the node together.
///
/// Files still being written are not counted: they close only when packets reach them, so a writer
/// that stopped feeding streams to get below the cap would never get there. Each recording's own
/// `max_bytes` bounds what it can have open.
#[derive(Debug)]
pub struct Pending {
    bytes: AtomicU64,
    files: AtomicU64,
    cap: u64,
    metrics: Arc<Metrics>,
}

impl Pending {
    pub fn new(cap: u64, metrics: Arc<Metrics>) -> Self {
        Self {
            bytes: AtomicU64::new(0),
            files: AtomicU64::new(0),
            cap,
            metrics,
        }
    }

    /// A file of `n` bytes closed and waits for upload.
    pub fn add(&self, n: u64) {
        self.files.fetch_add(1, Ordering::Relaxed);
        let now = self.bytes.fetch_add(n, Ordering::Relaxed) + n;
        self.metrics.record_pending(now);
    }

    /// A file of `n` bytes was uploaded or given up on, and removed.
    pub fn release(&self, n: u64) {
        let _ = self
            .files
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |f| {
                Some(f.saturating_sub(1))
            });
        let before = self
            .bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |b| {
                Some(b.saturating_sub(n))
            })
            .unwrap_or(0);
        self.metrics.record_pending(before.saturating_sub(n));
    }

    /// Whether uploads are so far behind that recordings should leave packets out.
    pub fn over(&self) -> bool {
        self.bytes.load(Ordering::Relaxed) >= self.cap
    }

    /// Whether the files waiting are half of what the node will hold: past that, uploads must go
    /// at their full rate, whatever else wants the network.
    pub fn half_full(&self) -> bool {
        self.bytes.load(Ordering::Relaxed).saturating_mul(2) >= self.cap
    }

    pub fn bytes(&self) -> u64 {
        self.bytes.load(Ordering::Relaxed)
    }

    /// How many files wait.
    pub fn files(&self) -> u64 {
        self.files.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_is_over_from_the_cap_until_uploads_catch_up() {
        let m = Arc::new(Metrics::new());
        let p = Pending::new(1000, m.clone());
        p.add(600);
        assert!(!p.over());
        p.add(400);
        assert!(p.over());
        assert!(m.render().contains("rtcd_record_pending_bytes 1000"));
        p.release(100);
        assert!(!p.over());
        p.release(u64::MAX);
        assert_eq!(p.bytes(), 0, "a release never goes below nothing");
        assert!(m.render().contains("rtcd_record_pending_bytes 0"));
    }
}
