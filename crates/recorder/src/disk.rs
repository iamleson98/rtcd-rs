//! The disk a recording may use.
//!
//! Every byte written is counted against the volume the recording may fill, so that it stops
//! capturing and uploads what it has before the volume is full.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

#[derive(Debug)]
struct Inner {
    limit: u64,
    written: AtomicU64,
    full: AtomicBool,
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
        }))
    }

    /// Counts bytes written.
    pub fn wrote(&self, n: u64) {
        let total = self.0.written.fetch_add(n, Ordering::Relaxed) + n;
        if total >= self.0.limit {
            self.0.full.store(true, Ordering::Relaxed);
        }
    }

    pub fn written(&self) -> u64 {
        self.0.written.load(Ordering::Relaxed)
    }

    pub fn is_full(&self) -> bool {
        self.0.full.load(Ordering::Relaxed)
    }
}
