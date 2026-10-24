//! Which sessions exist and which shard serves each, for lookups from outside the shards (the
//! service's HTTP API, signaling routed to the right thread).

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use rtcd_proto::SessionConfig;

/// Why a session could not be admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum AdmitError {
    #[error("a session with this id already exists")]
    Duplicate,
    #[error("the SFU is shutting down")]
    ShuttingDown,
}

#[derive(Debug)]
struct Entry {
    cfg: SessionConfig,
    shard: usize,
}

#[derive(Debug, Default)]
struct State {
    sessions: HashMap<String, Entry>,
    /// Sessions of each call, and the shard the call lives on.
    calls: HashMap<(String, String), (usize, HashSet<String>)>,
    load: Vec<usize>,
    closing: bool,
}

/// The sessions of an SFU.
#[derive(Debug)]
pub(crate) struct Registry {
    state: Mutex<State>,
}

impl Registry {
    pub fn new(shards: usize) -> Self {
        Self {
            state: Mutex::new(State {
                load: vec![0; shards],
                ..Default::default()
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        // The state is a set of maps that are consistent between statements, so a panic elsewhere
        // that poisoned the lock left nothing half done.
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Records a new session and returns the shard to serve it: the one its call lives on, else
    /// the one with the fewest sessions.
    pub fn admit(&self, cfg: &SessionConfig) -> Result<usize, AdmitError> {
        let mut st = self.lock();
        if st.closing {
            return Err(AdmitError::ShuttingDown);
        }
        if st.sessions.contains_key(&cfg.session_id) {
            return Err(AdmitError::Duplicate);
        }
        let call = (cfg.group_id.clone(), cfg.call_id.clone());
        let shard = match st.calls.get(&call) {
            Some((shard, _)) => *shard,
            None => st
                .load
                .iter()
                .enumerate()
                .min_by_key(|&(_, n)| *n)
                .map_or(0, |(i, _)| i),
        };
        st.load[shard] += 1;
        st.calls
            .entry(call)
            .or_insert_with(|| (shard, HashSet::new()))
            .1
            .insert(cfg.session_id.clone());
        st.sessions.insert(
            cfg.session_id.clone(),
            Entry {
                cfg: cfg.clone(),
                shard,
            },
        );
        Ok(shard)
    }

    /// Forgets a session; returns whether it was known.
    pub fn release(&self, session_id: &str) -> bool {
        let mut st = self.lock();
        let Some(entry) = st.sessions.remove(session_id) else {
            return false;
        };
        st.load[entry.shard] -= 1;
        let call = (entry.cfg.group_id, entry.cfg.call_id);
        if let Some((_, members)) = st.calls.get_mut(&call) {
            members.remove(session_id);
            if members.is_empty() {
                st.calls.remove(&call);
            }
        }
        true
    }

    pub fn shard_of(&self, session_id: &str) -> Option<usize> {
        self.lock().sessions.get(session_id).map(|e| e.shard)
    }

    pub fn lookup(&self, session_id: &str) -> Option<SessionConfig> {
        self.lock().sessions.get(session_id).map(|e| e.cfg.clone())
    }

    pub fn get(&self, group: &str, call: &str, session_id: &str) -> Option<SessionConfig> {
        let st = self.lock();
        let e = st.sessions.get(session_id)?;
        (e.cfg.group_id == group && e.cfg.call_id == call).then(|| e.cfg.clone())
    }

    /// The sessions of a call, in no particular order.
    pub fn call(&self, group: &str, call: &str) -> Vec<SessionConfig> {
        let st = self.lock();
        let Some((_, members)) = st.calls.get(&(group.to_owned(), call.to_owned())) else {
            return Vec::new();
        };
        members
            .iter()
            .filter_map(|id| st.sessions.get(id))
            .map(|e| e.cfg.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.lock().sessions.len()
    }

    /// Removes and returns every session of a shard that has stopped, and keeps new ones away
    /// from it.
    pub fn take_shard(&self, shard: usize) -> Vec<SessionConfig> {
        let mut st = self.lock();
        let ids: Vec<String> = st
            .sessions
            .iter()
            .filter(|(_, e)| e.shard == shard)
            .map(|(id, _)| id.clone())
            .collect();
        let mut lost = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(e) = st.sessions.remove(&id) {
                lost.push(e.cfg);
            }
        }
        st.calls.retain(|_, (s, _)| *s != shard);
        st.load[shard] = usize::MAX / 2;
        lost
    }

    /// Refuses new sessions from now on.
    pub fn close(&self) {
        self.lock().closing = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(call: &str, session: &str) -> SessionConfig {
        SessionConfig {
            group_id: "g".into(),
            call_id: call.into(),
            user_id: "u".into(),
            session_id: session.into(),
            ..Default::default()
        }
    }

    #[test]
    fn a_call_stays_on_one_shard_and_new_calls_go_to_the_least_loaded() {
        let r = Registry::new(3);
        let a = r.admit(&cfg("c1", "s1")).unwrap();
        assert_eq!(r.admit(&cfg("c1", "s2")).unwrap(), a);
        assert_eq!(r.admit(&cfg("c1", "s3")).unwrap(), a);
        let b = r.admit(&cfg("c2", "s4")).unwrap();
        assert_ne!(a, b, "the next call goes where there is room");
        let c = r.admit(&cfg("c3", "s5")).unwrap();
        assert!(c != a && c != b);
    }

    #[test]
    fn duplicates_are_refused_and_released_ids_can_return() {
        let r = Registry::new(1);
        r.admit(&cfg("c", "s")).unwrap();
        assert_eq!(r.admit(&cfg("c", "s")), Err(AdmitError::Duplicate));
        assert!(r.release("s"));
        assert!(!r.release("s"));
        assert!(r.admit(&cfg("c", "s")).is_ok());
    }

    #[test]
    fn lookups_need_the_right_group_and_call() {
        let r = Registry::new(1);
        r.admit(&cfg("c", "s")).unwrap();
        assert!(r.get("g", "c", "s").is_some());
        assert!(r.get("other", "c", "s").is_none());
        assert!(r.get("g", "other", "s").is_none());
        assert_eq!(r.call("g", "c").len(), 1);
        assert!(r.call("g", "none").is_empty());
    }

    #[test]
    fn an_empty_call_is_forgotten_so_its_shard_can_change() {
        let r = Registry::new(2);
        let first = r.admit(&cfg("c1", "s1")).unwrap();
        r.admit(&cfg("c2", "s2")).unwrap();
        r.release("s1");
        r.release("s2");
        r.admit(&cfg("c3", "s3")).unwrap();
        r.admit(&cfg("c4", "s4")).unwrap();
        r.release("s3");
        // c1 returns to the emptier shard, whichever it is, rather than to where it once was.
        let again = r.admit(&cfg("c1", "s5")).unwrap();
        let other = r.shard_of("s4").unwrap();
        assert_ne!(again, other);
        let _ = first;
    }

    #[test]
    fn closing_refuses_new_sessions() {
        let r = Registry::new(1);
        r.close();
        assert_eq!(r.admit(&cfg("c", "s")), Err(AdmitError::ShuttingDown));
    }
}
