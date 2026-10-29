//! The shards that are running, by slot, for whoever has to reach one: the other shards (to pass on
//! datagrams and connections that are not theirs), the TCP acceptor and the service.
//!
//! The shards come and go while the SFU runs, so the table is read for every batch rather than
//! copied once at the start.

use std::sync::RwLock;

use super::shard::ShardHandle;

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
