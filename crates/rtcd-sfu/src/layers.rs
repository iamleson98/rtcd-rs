//! Choosing between the simulcast layers of a video stream.
//!
//! A participant sends a camera or a screen share at two sizes, a small layer (`l`) and a large one
//! (`h`). The SFU sends each receiver one of them. What decides which is policy, kept here as plain
//! functions of measurements so it can be tested without a network: for a screen share the
//! receiver's bandwidth estimate against the rate of the large layer, for a camera what the
//! receiver asked for and whether the sender still sends the large layer.

use std::time::{Duration, Instant};

/// One of the two sizes a video is sent at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Layer {
    Low,
    High,
}

impl Layer {
    /// The layer a restriction identifier (rid) names. A stream sent without simulcast has none and
    /// counts as the small one, which is where a receiver falls back to.
    pub fn from_rid(rid: Option<&str>) -> Self {
        match rid {
            Some("h") | Some("f") => Self::High,
            _ => Self::Low,
        }
    }

    pub fn other(self) -> Self {
        match self {
            Self::Low => Self::High,
            Self::High => Self::Low,
        }
    }
}

/// Nominal rates of the layers of a screen share, used until a layer has been measured.
pub const NOMINAL_HIGH_RATE: u64 = 2_500_000;
pub const NOMINAL_LOW_RATE: u64 = 500_000;

/// The share of a layer's rate the estimate has to reach for the layer to be sent.
const TOLERANCE: f64 = 0.9;
const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// What the estimator and the sender's streams say, for one decision.
#[derive(Debug, Clone, Copy)]
pub struct ScreenInputs {
    /// The receiver's bandwidth estimate, bits per second.
    pub target: f64,
    /// The loss-based half of the estimate.
    pub loss_based: f64,
    /// The measured rate of the large layer, if it has been measured.
    pub high_rate: Option<u64>,
}

/// Decides which layer of a screen share one receiver gets.
///
/// The large layer is sent while the estimate can carry it. Moving up waits out a backoff that
/// grows with every change, so a link hovering around the rate does not flip back and forth;
/// moving down is immediate, because a receiver short of bandwidth loses frames at once.
#[derive(Debug)]
pub struct ScreenLevel {
    level: Layer,
    last_change: Option<Instant>,
    backoff: Duration,
}

impl ScreenLevel {
    /// Starts on the small layer, which every receiver can take.
    pub fn new() -> Self {
        Self {
            level: Layer::Low,
            last_change: None,
            backoff: INITIAL_BACKOFF,
        }
    }

    pub fn level(&self) -> Layer {
        self.level
    }

    /// Returns the layer to send now, which is the current one unless the inputs call for a change.
    pub fn decide(&mut self, now: Instant, inputs: ScreenInputs) -> Layer {
        let high = inputs.high_rate.unwrap_or(NOMINAL_HIGH_RATE) as f64 * TOLERANCE;
        let wanted = match self.level {
            // Dropping needs both halves of the estimate to agree; the delay-based half alone
            // swings with every burst, and the loss-based one only falls when packets are lost.
            Layer::High if inputs.target <= high && inputs.loss_based <= high => Layer::Low,
            Layer::High => Layer::High,
            Layer::Low if inputs.target > high => Layer::High,
            Layer::Low => Layer::Low,
        };
        if wanted == self.level {
            // Calm for long enough: forget past flapping.
            if self
                .last_change
                .is_some_and(|t| now.saturating_duration_since(t) > self.backoff * 3)
            {
                self.backoff = INITIAL_BACKOFF;
            }
            return self.level;
        }
        let waited = self
            .last_change
            .is_none_or(|t| now.saturating_duration_since(t) >= self.backoff);
        if wanted == Layer::High && !waited {
            return self.level;
        }
        self.level = wanted;
        self.last_change = Some(now);
        self.backoff = (self.backoff + self.backoff / 2).min(MAX_BACKOFF);
        wanted
    }
}

impl Default for ScreenLevel {
    fn default() -> Self {
        Self::new()
    }
}

/// How long the large layer of a camera may be silent, while the small one flows, before receivers
/// are moved to the small one: a browser drops its top layer when its upload is short.
pub const STALL_AFTER: Duration = Duration::from_secs(2);
/// How often a camera's layers are checked.
pub const WATCH_EVERY: Duration = Duration::from_secs(1);

/// Tracks whether the large layer of one camera is paused.
#[derive(Debug, Default)]
pub struct StallWatch {
    stalled: bool,
}

impl StallWatch {
    pub fn stalled(&self) -> bool {
        self.stalled
    }

    /// Updates the state from when each layer last brought a packet. Returns whether it changed.
    /// When neither layer flows the camera is off, and nothing changes.
    pub fn update(&mut self, now: Instant, high: Option<Instant>, low: Option<Instant>) -> bool {
        let idle =
            |t: Option<Instant>| t.map_or(Duration::MAX, |t| now.saturating_duration_since(t));
        let (high_idle, low_idle) = (idle(high), idle(low));
        let stalled = if high_idle < WATCH_EVERY {
            false
        } else if high_idle >= STALL_AFTER && low_idle < WATCH_EVERY {
            true
        } else {
            self.stalled
        };
        std::mem::replace(&mut self.stalled, stalled) != stalled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(target: f64) -> ScreenInputs {
        ScreenInputs {
            target,
            loss_based: target,
            high_rate: Some(1_000_000),
        }
    }

    #[test]
    fn rids_map_to_layers() {
        assert_eq!(Layer::from_rid(Some("h")), Layer::High);
        assert_eq!(Layer::from_rid(Some("l")), Layer::Low);
        assert_eq!(Layer::from_rid(None), Layer::Low);
        assert_eq!(Layer::High.other(), Layer::Low);
    }

    #[test]
    fn the_large_layer_is_sent_once_the_estimate_carries_it() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        assert_eq!(s.decide(t, inputs(500_000.0)), Layer::Low);
        assert_eq!(s.decide(t, inputs(1_500_000.0)), Layer::High);
    }

    #[test]
    fn an_unmeasured_layer_is_assumed_to_need_its_nominal_rate() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        let unmeasured = |target| ScreenInputs {
            target,
            loss_based: target,
            high_rate: None,
        };
        assert_eq!(s.decide(t, unmeasured(1_500_000.0)), Layer::Low);
        assert_eq!(s.decide(t, unmeasured(2_400_000.0)), Layer::High);
    }

    #[test]
    fn moving_down_is_immediate_and_moving_up_waits_out_the_backoff() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        assert_eq!(s.decide(t, inputs(2_000_000.0)), Layer::High);
        assert_eq!(
            s.decide(t + Duration::from_secs(1), inputs(200_000.0)),
            Layer::Low,
            "down at once"
        );
        // Bandwidth is back, but the last change was a second ago.
        assert_eq!(
            s.decide(t + Duration::from_secs(2), inputs(2_000_000.0)),
            Layer::Low
        );
        // Each change lengthens the backoff by half: 10 s, 15 s after the first, 22.5 s after the drop.
        assert_eq!(
            s.decide(t + Duration::from_secs(20), inputs(2_000_000.0)),
            Layer::Low
        );
        assert_eq!(
            s.decide(t + Duration::from_secs(24), inputs(2_000_000.0)),
            Layer::High
        );
    }

    #[test]
    fn repeated_flapping_lengthens_the_backoff() {
        let mut t = Instant::now();
        let mut s = ScreenLevel::new();
        let mut gaps = vec![];
        let mut last_up = None;
        for _ in 0..5 {
            // Wait for the upgrade, then drop again right away.
            while s.decide(t, inputs(2_000_000.0)) != Layer::High {
                t += Duration::from_secs(1);
            }
            if let Some(prev) = last_up {
                gaps.push(t.duration_since(prev));
            }
            last_up = Some(t);
            t += Duration::from_secs(1);
            assert_eq!(s.decide(t, inputs(100_000.0)), Layer::Low);
        }
        assert!(gaps.windows(2).all(|w| w[1] >= w[0]), "{gaps:?}");
        assert!(gaps.last().unwrap() > &Duration::from_secs(20), "{gaps:?}");
    }

    #[test]
    fn a_delay_based_dip_alone_does_not_drop_the_large_layer() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        s.decide(t, inputs(2_000_000.0));
        let dip = ScreenInputs {
            target: 300_000.0,
            loss_based: 2_000_000.0,
            high_rate: Some(1_000_000),
        };
        assert_eq!(s.decide(t + Duration::from_secs(1), dip), Layer::High);
    }

    #[test]
    fn a_calm_period_forgets_the_backoff() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        s.decide(t, inputs(2_000_000.0));
        s.decide(t, inputs(100_000.0));
        assert!(s.backoff > INITIAL_BACKOFF);
        s.decide(t + Duration::from_secs(600), inputs(100_000.0));
        assert_eq!(s.backoff, INITIAL_BACKOFF);
    }

    #[test]
    fn the_large_camera_layer_is_stalled_after_two_silent_seconds_while_the_small_flows() {
        let t = Instant::now();
        let mut w = StallWatch::default();
        let at = |ms| Some(t + Duration::from_millis(ms));
        assert!(
            !w.update(t + Duration::from_millis(3000), at(2900), at(2950)),
            "flowing"
        );
        assert!(
            !w.update(t + Duration::from_millis(3000), at(1500), at(2950)),
            "silent for 1.5 s: not yet"
        );
        assert!(
            w.update(t + Duration::from_millis(5000), at(2900), at(4950)),
            "silent for 2.1 s"
        );
        assert!(w.stalled());
        assert!(
            !w.update(t + Duration::from_millis(6000), at(2900), at(5950)),
            "still stalled, no change"
        );
        assert!(
            w.update(t + Duration::from_millis(6000), at(5950), at(5950)),
            "back"
        );
        assert!(!w.stalled());
    }

    #[test]
    fn a_camera_with_both_layers_silent_is_off_not_stalled() {
        let t = Instant::now();
        let mut w = StallWatch::default();
        assert!(!w.update(t + Duration::from_secs(60), Some(t), Some(t)));
        assert!(!w.stalled());
        assert!(!w.update(t, None, None));
    }
}
