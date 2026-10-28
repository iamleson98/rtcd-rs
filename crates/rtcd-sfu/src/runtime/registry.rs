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
    #[error("no shard is running to serve the session")]
    NoShard,
    #[error("every shard that could serve the session is at its limit")]
    Saturated,
}

#[derive(Debug)]
struct Entry {
    cfg: SessionConfig,
    shard: usize,
}

#[derive(Debug, Default)]
struct State {
    /// Sessions a call counts for at least when shards are compared (0: just its sessions).
    expected_call_size: usize,
    sessions: HashMap<String, Entry>,
    /// Sessions of each call, and the shard the call lives on.
    calls: HashMap<(String, String), (usize, HashSet<String>)>,
    /// Sessions on each shard slot.
    load: Vec<usize>,
    /// Which slots have a shard that takes new sessions.
    active: Vec<bool>,
    closing: bool,
}

/// The sessions of an SFU.
#[derive(Debug)]
pub(crate) struct Registry {
    state: Mutex<State>,
}

impl Registry {
    #[cfg(test)]
    pub fn new(shards: usize) -> Self {
        Self::with_slots(shards, shards)
    }

    /// Counts each call as at least `size` sessions when choosing a shard for a new one.
    pub fn expecting_calls_of(self, size: usize) -> Self {
        self.lock().expected_call_size = size;
        self
    }

    /// Room for `slots` shards, the first `active` of which are running.
    pub fn with_slots(slots: usize, active: usize) -> Self {
        Self {
            state: Mutex::new(State {
                load: vec![0; slots],
                active: (0..slots).map(|i| i < active).collect(),
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

    /// What each shard slot is carrying: its calls, each counted for at least the size a call is
    /// expected to reach.
    fn weights(st: &State) -> Vec<usize> {
        let mut weights = vec![0; st.load.len()];
        for (shard, members) in st.calls.values() {
            weights[*shard] += members.len().max(st.expected_call_size);
        }
        weights
    }

    /// Records a new session and returns the shard to serve it: the one its call lives on, else
    /// the running one with the fewest sessions.
    pub fn admit(&self, cfg: &SessionConfig) -> Result<usize, AdmitError> {
        self.admit_unless(cfg, &|_| false)
    }

    /// Like [`admit`](Self::admit), but refuses a session whose shard is `saturated` (the one its
    /// call lives on, or for a new call every shard that takes new sessions): a shard at its
    /// limit loses packets for every call on it, and the sessions it has are all it can serve. A
    /// new call goes to the emptiest shard that is not saturated.
    pub fn admit_unless(
        &self,
        cfg: &SessionConfig,
        saturated: &dyn Fn(usize) -> bool,
    ) -> Result<usize, AdmitError> {
        let mut st = self.lock();
        if st.closing {
            return Err(AdmitError::ShuttingDown);
        }
        if st.sessions.contains_key(&cfg.session_id) {
            return Err(AdmitError::Duplicate);
        }
        let call = (cfg.group_id.clone(), cfg.call_id.clone());
        // The call's host is never refused: without the teacher a class cannot start, or goes on
        // without one (a teacher whose connection dropped comes back as a new session).
        let host = cfg.props.host();
        let shard = match st.calls.get(&call) {
            Some((shard, _)) if saturated(*shard) && !host => return Err(AdmitError::Saturated),
            Some((shard, _)) => *shard,
            None => {
                let open: Vec<(usize, usize)> = Self::weights(&st)
                    .into_iter()
                    .enumerate()
                    .filter(|&(i, _)| st.active[i])
                    .collect();
                if open.is_empty() {
                    return Err(AdmitError::NoShard);
                }
                let with_room = open
                    .iter()
                    .copied()
                    .filter(|&(i, _)| !saturated(i))
                    .min_by_key(|&(_, n)| n);
                match with_room {
                    Some((i, _)) => i,
                    // Every shard is saturated: only the host starts a call, on the emptiest.
                    None if host => open
                        .into_iter()
                        .min_by_key(|&(_, n)| n)
                        .map(|(i, _)| i)
                        .ok_or(AdmitError::NoShard)?,
                    None => return Err(AdmitError::Saturated),
                }
            }
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
        st.load[shard] = 0;
        st.active[shard] = false;
        lost
    }

    /// Whether `cfg` starts a call and every running shard already has `per_shard` sessions or
    /// more: a call that is already placed stays where it is, so only a new one can use a new
    /// shard.
    pub fn needs_new_shard(&self, cfg: &SessionConfig, per_shard: usize) -> bool {
        let st = self.lock();
        if st.closing
            || st
                .calls
                .contains_key(&(cfg.group_id.clone(), cfg.call_id.clone()))
        {
            return false;
        }
        Self::weights(&st)
            .into_iter()
            .enumerate()
            .filter(|&(i, _)| st.active[i])
            .all(|(_, n)| n >= per_shard)
    }

    /// Lets new sessions go to `shard`, whose thread is running.
    pub fn activate(&self, shard: usize) {
        let mut st = self.lock();
        if let Some(active) = st.active.get_mut(shard) {
            *active = true;
        }
    }

    /// Stops sending new sessions to `shard`, if it has none and is not the last shard running.
    /// Done under the same lock as admitting, so a session cannot be placed on it after the
    /// answer is yes.
    pub fn retire(&self, shard: usize) -> bool {
        let mut st = self.lock();
        let running = st.active.iter().filter(|&&a| a).count();
        if running <= 1 || !st.active.get(shard).copied().unwrap_or(false) || st.load[shard] != 0 {
            return false;
        }
        st.active[shard] = false;
        true
    }

    /// The sessions on each running shard, with its slot.
    pub fn running(&self) -> Vec<(usize, usize)> {
        let st = self.lock();
        st.active
            .iter()
            .enumerate()
            .filter(|&(_, &a)| a)
            .map(|(i, _)| (i, st.load[i]))
            .collect()
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
    fn a_saturated_shard_refuses_new_sessions_and_new_calls_go_to_a_shard_with_room() {
        let r = Registry::new(2);
        let a = r.admit(&cfg("c1", "s1")).unwrap();
        let saturated = |slot: usize| slot == a;
        assert_eq!(
            r.admit_unless(&cfg("c1", "s2"), &saturated),
            Err(AdmitError::Saturated),
            "a call cannot move: its next participant is refused"
        );
        let b = r.admit_unless(&cfg("c2", "s3"), &saturated).unwrap();
        assert_ne!(a, b, "a new call goes where there is room");
        assert_eq!(r.len(), 2, "nothing is recorded for the refused one");
        assert_eq!(
            r.admit_unless(&cfg("c3", "s4"), &|_| true),
            Err(AdmitError::Saturated),
            "every shard is at its limit"
        );
        assert!(
            r.admit_unless(&cfg("c1", "s5"), &|_| false).is_ok(),
            "and room again"
        );
    }

    #[test]
    fn the_host_is_never_refused_by_a_saturated_shard() {
        let host = |call: &str, session: &str| {
            SessionConfig::from_join(
                "g",
                &rtcd_proto::JoinRequest {
                    call_id: call.into(),
                    user_id: "teacher".into(),
                    session_id: session.into(),
                    host: true,
                    ..Default::default()
                },
            )
        };
        let r = Registry::new(2);
        let a = r.admit(&cfg("c1", "s1")).unwrap();
        let saturated = |slot: usize| slot == a;
        assert_eq!(
            r.admit_unless(&cfg("c1", "s2"), &saturated),
            Err(AdmitError::Saturated),
            "a student is refused"
        );
        assert_eq!(
            r.admit_unless(&host("c1", "teacher"), &saturated),
            Ok(a),
            "the teacher of that class is not"
        );
        assert_eq!(
            r.admit_unless(&cfg("c2", "s3"), &|_| true),
            Err(AdmitError::Saturated),
            "nobody else starts a call on a node whose shards are all saturated"
        );
        assert!(
            r.admit_unless(&host("c2", "teacher2"), &|_| true).is_ok(),
            "a teacher does, on the emptiest shard"
        );
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
    fn a_shard_added_later_takes_the_next_calls() {
        let r = Registry::with_slots(3, 1);
        assert_eq!(r.admit(&cfg("c1", "s1")).unwrap(), 0);
        assert_eq!(r.admit(&cfg("c2", "s2")).unwrap(), 0, "only one is running");
        r.activate(1);
        assert_eq!(
            r.admit(&cfg("c3", "s3")).unwrap(),
            1,
            "the empty one takes it"
        );
        assert_eq!(r.running(), vec![(0, 2), (1, 1)]);
    }

    #[test]
    fn a_shard_with_sessions_or_the_last_one_is_not_retired() {
        let r = Registry::with_slots(2, 2);
        r.admit(&cfg("c1", "s1")).unwrap();
        let used = r.shard_of("s1").unwrap();
        assert!(!r.retire(used), "it has a session");
        let empty = 1 - used;
        assert!(r.retire(empty));
        assert!(!r.retire(empty), "already retired");
        assert_eq!(
            r.admit(&cfg("c2", "s2")).unwrap(),
            used,
            "new calls avoid it"
        );
        r.release("s1");
        r.release("s2");
        assert!(!r.retire(used), "the last one stays");
    }

    #[test]
    fn with_no_shard_running_nothing_is_admitted() {
        let r = Registry::with_slots(2, 1);
        r.admit(&cfg("c1", "s1")).unwrap();
        assert_eq!(r.take_shard(0).len(), 1);
        assert_eq!(r.admit(&cfg("c2", "s2")), Err(AdmitError::NoShard));
    }

    #[test]
    fn only_a_new_call_on_full_shards_needs_another_shard() {
        let r = Registry::with_slots(3, 1);
        assert!(!r.needs_new_shard(&cfg("c1", "s1"), 2), "there is room");
        r.admit(&cfg("c1", "s1")).unwrap();
        r.admit(&cfg("c1", "s2")).unwrap();
        assert!(
            !r.needs_new_shard(&cfg("c1", "s3"), 2),
            "the call is placed already"
        );
        assert!(
            r.needs_new_shard(&cfg("c2", "s4"), 2),
            "a new call, and the only shard is full"
        );
        r.activate(1);
        assert!(
            !r.needs_new_shard(&cfg("c2", "s4"), 2),
            "the new shard is empty"
        );
        r.close();
        assert!(
            !r.needs_new_shard(&cfg("c2", "s4"), 2),
            "nothing is admitted any more"
        );
    }

    #[test]
    fn calls_are_counted_at_the_size_they_are_expected_to_reach() {
        // Four calls start at once, one session each; two shards, a call counts for ten.
        let r = Registry::with_slots(2, 2).expecting_calls_of(10);
        let shards: Vec<usize> = (0..4)
            .map(|i| r.admit(&cfg(&format!("c{i}"), &format!("s{i}"))).unwrap())
            .collect();
        let on_first = shards.iter().filter(|&&s| s == shards[0]).count();
        assert_eq!(
            on_first, 2,
            "{shards:?}: two and two, not decided by one session each"
        );

        // A call that has grown past it counts for what it has.
        let r = Registry::with_slots(2, 2).expecting_calls_of(3);
        let big = r.admit(&cfg("big", "b0")).unwrap();
        for i in 1..6 {
            assert_eq!(r.admit(&cfg("big", &format!("b{i}"))).unwrap(), big);
        }
        assert_ne!(r.admit(&cfg("next", "n0")).unwrap(), big);
        assert_ne!(r.admit(&cfg("then", "t0")).unwrap(), big, "6 against 3");
    }

    #[test]
    fn closing_refuses_new_sessions() {
        let r = Registry::new(1);
        r.close();
        assert_eq!(r.admit(&cfg("c", "s")), Err(AdmitError::ShuttingDown));
    }
}
