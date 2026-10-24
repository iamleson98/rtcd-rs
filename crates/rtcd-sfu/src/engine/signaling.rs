//! Who may offer, and when. Both sides renegotiate a connection (the participant to add a
//! microphone or a screen, the SFU to add everyone else's tracks), and two offers in flight at once
//! collide. The data channel carries a lock for it: a side takes the lock before it offers and
//! releases it once the answer is in. The participant asks the SFU for the lock over the data
//! channel; the SFU just records that it holds it while its own offer is out.

use std::time::{Duration, Instant};

/// Which transport carries a session description.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Channel {
    /// Through the service's websocket.
    Ws,
    /// Over the session's data channel.
    Dc,
}

/// Who holds the signaling lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Holder {
    Free,
    /// The SFU, while an offer of its own is unanswered.
    Server {
        since: Instant,
    },
    /// The participant, between its lock request and its unlock.
    Client {
        since: Instant,
    },
}

/// The signaling lock of one session.
#[derive(Debug)]
pub struct SignalingLock {
    holder: Holder,
    max_hold: Duration,
}

impl SignalingLock {
    pub fn new(max_hold: Duration) -> Self {
        Self {
            holder: Holder::Free,
            max_hold,
        }
    }

    #[cfg(test)]
    pub fn holder(&self) -> Holder {
        self.holder
    }

    /// Clears a hold that has lasted too long (a participant that took the lock and vanished would
    /// otherwise stop the SFU from ever offering again). Returns who lost it.
    pub fn expire(&mut self, now: Instant) -> Option<Holder> {
        let since = match self.holder {
            Holder::Client { since } | Holder::Server { since } => since,
            Holder::Free => return None,
        };
        if now.saturating_duration_since(since) >= self.max_hold {
            Some(std::mem::replace(&mut self.holder, Holder::Free))
        } else {
            None
        }
    }

    /// The participant asks for the lock. Granted when nobody holds it.
    pub fn client_lock(&mut self, now: Instant) -> bool {
        self.expire(now);
        if self.holder == Holder::Free {
            self.holder = Holder::Client { since: now };
            true
        } else {
            false
        }
    }

    /// The participant releases the lock; returns how long it held it, if it did.
    pub fn client_unlock(&mut self, now: Instant) -> Option<Duration> {
        match self.holder {
            Holder::Client { since } => {
                self.holder = Holder::Free;
                Some(now.saturating_duration_since(since))
            }
            _ => None,
        }
    }

    /// The SFU takes the lock for an offer of its own. It waits (returns false) while the
    /// participant holds it.
    pub fn server_lock(&mut self, now: Instant) -> bool {
        self.expire(now);
        if self.holder == Holder::Free {
            self.holder = Holder::Server { since: now };
            true
        } else {
            false
        }
    }

    /// The SFU releases the lock; returns how long it held it, if it did.
    pub fn server_unlock(&mut self, now: Instant) -> Option<Duration> {
        match self.holder {
            Holder::Server { since } => {
                self.holder = Holder::Free;
                Some(now.saturating_duration_since(since))
            }
            _ => None,
        }
    }

    /// When the current hold runs out, so the owner of the clock can wake to expire it.
    pub fn deadline(&self) -> Option<Instant> {
        match self.holder {
            Holder::Free => None,
            Holder::Client { since } | Holder::Server { since } => Some(since + self.max_hold),
        }
    }
}

/// Where a session is in its conversation of offers and answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The participant's first offer has not arrived.
    AwaitingFirstOffer {
        deadline: Instant,
    },
    Idle,
    /// An offer of the SFU's is out, waiting for the participant's answer.
    Offering {
        deadline: Instant,
        via: Channel,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOLD: Duration = Duration::from_secs(10);

    #[test]
    fn the_lock_is_taken_by_one_side_at_a_time() {
        let t = Instant::now();
        let mut lock = SignalingLock::new(HOLD);
        assert!(lock.client_lock(t));
        assert!(!lock.client_lock(t), "already held");
        assert!(!lock.server_lock(t), "the SFU waits for the participant");
        assert_eq!(
            lock.client_unlock(t + Duration::from_millis(40)),
            Some(Duration::from_millis(40))
        );
        assert!(lock.server_lock(t));
        assert!(
            !lock.client_lock(t),
            "the participant waits for the SFU's answer"
        );
        assert!(lock.server_unlock(t).is_some());
    }

    #[test]
    fn unlocking_a_lock_one_does_not_hold_does_nothing() {
        let t = Instant::now();
        let mut lock = SignalingLock::new(HOLD);
        assert_eq!(lock.client_unlock(t), None);
        assert!(lock.server_lock(t));
        assert_eq!(
            lock.client_unlock(t),
            None,
            "the participant cannot release the SFU's hold"
        );
        assert!(matches!(lock.holder(), Holder::Server { .. }));
    }

    #[test]
    fn a_vanished_holder_loses_the_lock() {
        let t = Instant::now();
        let mut lock = SignalingLock::new(HOLD);
        assert!(lock.client_lock(t));
        assert_eq!(lock.deadline(), Some(t + HOLD));
        assert_eq!(lock.expire(t + HOLD - Duration::from_millis(1)), None);
        assert!(matches!(lock.expire(t + HOLD), Some(Holder::Client { .. })));
        assert!(lock.server_lock(t + HOLD));
    }

    #[test]
    fn asking_after_the_hold_ran_out_succeeds_without_an_explicit_expire() {
        let t = Instant::now();
        let mut lock = SignalingLock::new(HOLD);
        assert!(lock.server_lock(t));
        assert!(lock.client_lock(t + HOLD));
    }
}
