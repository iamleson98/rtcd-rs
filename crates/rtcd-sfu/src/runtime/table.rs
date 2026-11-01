//! The shards that are running, by slot, for whoever has to reach one: the other shards (to pass on
//! datagrams and connections that are not theirs), the TCP acceptor and the service.
//!
//! The shards come and go while the SFU runs, so the table is read for every batch rather than
//! copied once at the start.

use std::sync::RwLock;

use std::sync::atomic::Ordering;

use super::shard::ShardHandle;

/// What the shards' queues hold, summed over the running shards (the peaks are the largest of
/// any, over the last second).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Queues {
    /// Datagrams one shard passed on to another that the other has not yet taken up.
    pub forward_queued: usize,
    /// Of those, thrown away since the node started because the other shard had too many waiting.
    pub forward_dropped: u64,
    /// The most datagrams one turn of a shard's loop took in, the most the engine had queued to
    /// send when it flushed, and the longest turn (microseconds).
    pub peak_reads: u32,
    pub peak_transmits: u32,
    pub longest_turn_us: u32,
}

#[derive(Debug)]
pub(crate) struct ShardTable {
    slots: RwLock<Vec<Option<ShardHandle>>>,
}

impl ShardTable {
    pub fn new(slots: usize) -> Self {
        Self {
            slots: RwLock::new(vec![None; slots]),
        }
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<Option<ShardHandle>>> {
        self.slots.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set(&self, slot: usize, handle: Option<ShardHandle>) {
        let mut slots = self.slots.write().unwrap_or_else(|e| e.into_inner());
        if let Some(s) = slots.get_mut(slot) {
            *s = handle;
        }
    }

    pub fn get(&self, slot: usize) -> Option<ShardHandle> {
        self.read().get(slot).cloned().flatten()
    }

    /// How many running shards have no room left and are shedding load.
    pub fn pressured(&self) -> usize {
        self.read()
            .iter()
            .flatten()
            .filter(|h| h.pressured())
            .count()
    }

    /// Whether the shard of `slot` is running and at its limit.
    pub fn saturated(&self, slot: usize) -> bool {
        self.read()
            .get(slot)
            .and_then(Option::as_ref)
            .is_some_and(ShardHandle::saturated)
    }

    /// How many running shards are at their limit and refuse new sessions.
    pub fn saturated_count(&self) -> usize {
        self.read()
            .iter()
            .flatten()
            .filter(|h| h.saturated())
            .count()
    }

    /// The queues of the running shards.
    pub fn queues(&self) -> Queues {
        let mut q = Queues::default();
        for h in self.read().iter().flatten() {
            let s = h.stats();
            q.forward_queued += s.forward_queued.load(Ordering::Relaxed);
            q.forward_dropped += s.forward_dropped.load(Ordering::Relaxed);
            q.peak_reads = q.peak_reads.max(s.peak_reads.load(Ordering::Relaxed));
            q.peak_transmits = q.peak_transmits.max(s.peak_transmits.load(Ordering::Relaxed));
            q.longest_turn_us = q
                .longest_turn_us
                .max(s.longest_turn_us.load(Ordering::Relaxed));
        }
        q
    }

    /// How many shards are running.
    pub fn len(&self) -> usize {
        self.read().iter().flatten().count()
    }

    /// The first slot with no shard in it.
    pub fn free_slot(&self) -> Option<usize> {
        self.read().iter().position(Option::is_none)
    }

    /// The `n`th running shard, counting round and round: for spreading what has no owner yet.
    pub fn nth(&self, n: usize) -> Option<ShardHandle> {
        let slots = self.read();
        let live = slots.iter().flatten().count();
        if live == 0 {
            return None;
        }
        slots.iter().flatten().nth(n % live).cloned()
    }

    pub fn handles(&self) -> Vec<(usize, ShardHandle)> {
        self.read()
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.clone().map(|s| (i, s)))
            .collect()
    }
}
