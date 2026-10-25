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
/// A receiver that asked for the large layer keeps it unless its link is starved: the loss-based
/// estimate (which only falls when packets are lost) below this share of the layer's rate for
/// [`PIN_PATIENCE`], once the estimate has had [`PIN_GRACE`] to find its level. It returns to the
/// large layer when the estimate passes [`PIN_RESUME`].
const PIN_FLOOR: f64 = 0.4;
const PIN_RESUME: f64 = 0.6;
const PIN_GRACE: Duration = Duration::from_secs(30);
const PIN_PATIENCE: Duration = Duration::from_secs(10);
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
    /// The receiver wants the large layer unless its link is starved.
    pin: Option<Pin>,
    level: Layer,
    last_change: Option<Instant>,
    backoff: Duration,
}

#[derive(Debug)]
struct Pin {
    since: Instant,
    starved_since: Option<Instant>,
}

impl ScreenLevel {
    /// Starts on the small layer, which every receiver can take.
    pub fn new() -> Self {
        Self {
            pin: None,
            level: Layer::Low,
            last_change: None,
            backoff: INITIAL_BACKOFF,
        }
    }

    /// Makes the large layer the receiver's from now on (see [`PIN_FLOOR`] for when it is let go),
    /// or hands the choice back to the estimate.
    pub fn pin_high(&mut self, pinned: bool, now: Instant) {
        if !pinned {
            self.pin = None;
        } else if self.pin.is_none() {
            self.pin = Some(Pin {
                since: now,
                starved_since: None,
            });
            self.level = Layer::High;
        }
    }

    pub fn level(&self) -> Layer {
        self.level
    }

    /// Returns the layer to send now, which is the current one unless the inputs call for a change.
    pub fn decide(&mut self, now: Instant, inputs: ScreenInputs) -> Layer {
        let high = inputs.high_rate.unwrap_or(NOMINAL_HIGH_RATE) as f64 * TOLERANCE;
        if let Some(pin) = &mut self.pin {
            match self.level {
                Layer::High => {
                    let starved = now.saturating_duration_since(pin.since) >= PIN_GRACE
                        && inputs.loss_based < high * PIN_FLOOR;
                    if starved {
                        let since = *pin.starved_since.get_or_insert(now);
                        if now.saturating_duration_since(since) >= PIN_PATIENCE {
                            pin.starved_since = None;
                            self.level = Layer::Low;
                        }
                    } else {
                        pin.starved_since = None;
                    }
                }
                Layer::Low if inputs.loss_based > high * PIN_RESUME => self.level = Layer::High,
                Layer::Low => {}
            }
            return self.level;
        }
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
    fn a_pinned_receiver_stays_on_the_large_layer_through_the_estimates_ramp_up() {
        let mut level = ScreenLevel::new();
        let t0 = Instant::now();
        level.pin_high(true, t0);
        assert_eq!(level.level(), Layer::High);
        // An estimate that has not found its level yet does not take the layer away.
        for s in 0..29 {
            assert_eq!(
                level.decide(t0 + Duration::from_secs(s), inputs(10_000.0)),
                Layer::High
            );
        }
    }

    #[test]
    fn a_pinned_receiver_whose_link_is_starved_falls_back_and_returns() {
        let mut level = ScreenLevel::new();
        let t0 = Instant::now();
        level.pin_high(true, t0);
        let at = |s: u64| t0 + Duration::from_secs(s);
        // Starved from 30 s on: still High for the patience, then Low.
        assert_eq!(level.decide(at(30), inputs(10_000.0)), Layer::High);
        assert_eq!(level.decide(at(39), inputs(10_000.0)), Layer::High);
        assert_eq!(level.decide(at(41), inputs(10_000.0)), Layer::Low);
        // The link recovers (the large layer is 1 Mbit/s here): back to it.
        assert_eq!(level.decide(at(50), inputs(2_000_000.0)), Layer::High);
        // A dip shorter than the patience changes nothing.
        assert_eq!(level.decide(at(60), inputs(10_000.0)), Layer::High);
        assert_eq!(level.decide(at(65), inputs(2_000_000.0)), Layer::High);
        assert_eq!(level.decide(at(80), inputs(10_000.0)), Layer::High);
        assert_eq!(level.decide(at(85), inputs(10_000.0)), Layer::High);
    }

    #[test]
    fn handing_the_choice_back_restores_the_estimate() {
        let mut level = ScreenLevel::new();
        let t0 = Instant::now();
        level.pin_high(true, t0);
        level.pin_high(false, t0);
        assert_eq!(
            level.decide(t0, inputs(10_000.0)),
            Layer::Low,
            "the estimate decides again, and a starved link drops at once"
        );
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
