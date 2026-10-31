//! Choosing between the simulcast layers of a video stream.
//!
//! A participant sends a camera or a screen share at two sizes, a small layer (`l`) and a large one
//! (`h`). The SFU sends each receiver one of them. What decides which is policy, kept here as plain
//! functions of measurements so it can be tested without a network: for a screen share what the
//! receiver's path can carry and how much of what is sent to it arrives, against the rate of the
//! large layer; for a camera what the receiver asked for and whether the sender still sends the
//! large layer.

use std::time::{Duration, Instant};

/// One of the sizes a video is sent at.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Layer {
    Low,
    High,
    /// A layer made for the recording alone (rid `r`): sharper and dearer than any viewer needs,
    /// sent only by a sender with bandwidth to spare, recorded when it flows and never forwarded.
    Record,
}

impl Layer {
    /// The layer a restriction identifier (rid) names. A stream sent without simulcast has none and
    /// counts as the small one, which is where a receiver falls back to.
    pub fn from_rid(rid: Option<&str>) -> Self {
        match rid {
            Some("h") | Some("f") => Self::High,
            Some("r") => Self::Record,
            _ => Self::Low,
        }
    }

    /// The other of the two layers a viewer can be sent.
    pub fn other(self) -> Self {
        match self {
            Self::Low => Self::High,
            Self::High | Self::Record => Self::Low,
        }
    }

    /// How much picture the layer carries: what the recording prefers, and what it waits to
    /// move up to.
    pub fn rank(self) -> u8 {
        match self {
            Self::Low => 0,
            Self::High => 1,
            Self::Record => 2,
        }
    }
}

/// Nominal rates of the layers of a screen share, used until a layer has been measured.
pub const NOMINAL_HIGH_RATE: u64 = 2_500_000;
pub const NOMINAL_LOW_RATE: u64 = 500_000;

/// The share of a layer's rate the estimate has to reach for the layer to be sent.
const TOLERANCE: f64 = 0.9;
/// A receiver that asked for the large layer keeps it unless its link is starved: what its path
/// can carry below this share of the layer's rate, or less than [`DROP_DELIVERY`] of what is sent
/// arriving, for [`PIN_PATIENCE`], once the estimate has had [`PIN_GRACE`] to find its level. It
/// returns to the large layer when the path can carry [`PIN_RESUME`] of it.
const PIN_FLOOR: f64 = 0.4;
const PIN_RESUME: f64 = 0.6;
const PIN_GRACE: Duration = Duration::from_secs(30);
const PIN_PATIENCE: Duration = Duration::from_secs(10);
/// The large layer is sent only while this much of what is sent to the receiver arrives...
const RAISE_DELIVERY: f64 = 0.9;
/// ...and is taken away when less than this arrives, or when the path is also losing packets
/// ([`LOSING`]) while its delay says it cannot carry the layer, for [`DROP_PATIENCE`].
const DROP_DELIVERY: f64 = 0.75;
const LOSING: f64 = 0.95;
const DROP_PATIENCE: Duration = Duration::from_secs(2);
const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(300);

/// What the estimator and the sender's streams say, for one decision.
#[derive(Debug, Clone, Copy)]
pub struct ScreenInputs {
    /// What the receiver's path can carry, bits per second: the delay-based half of its
    /// estimate. The other half, from loss, only grows while under 2% of the packets are lost and
    /// so stays at its floor on a path that loses a few per cent at random, which kept every such
    /// receiver on the small layer for good; what arrives is measured instead (`delivery`).
    pub capacity: f64,
    /// The share (0..1) of the video sent to the receiver over the last seconds that arrived,
    /// `None` when too little was sent to say.
    pub delivery: Option<f64>,
    /// The measured rate of the large layer, if it has been measured.
    pub high_rate: Option<u64>,
}

/// Decides which layer of a screen share one receiver gets.
///
/// The large layer is sent while the path can carry it and what is sent arrives. Moving up waits
/// out a backoff that grows with every change, so a link hovering around the rate does not flip
/// back and forth, and is a trial: if the large layer does not arrive, the receiver is moved
/// down again within [`DROP_PATIENCE`].
#[derive(Debug)]
pub struct ScreenLevel {
    /// The receiver wants the large layer unless its link is starved.
    pin: Option<Pin>,
    level: Layer,
    last_change: Option<Instant>,
    backoff: Duration,
    /// Since when the large layer has not been carried or has not arrived.
    starved_since: Option<Instant>,
}

#[derive(Debug)]
struct Pin {
    since: Instant,
}

impl ScreenLevel {
    /// Starts on the small layer, which every receiver can take.
    pub fn new() -> Self {
        Self {
            pin: None,
            level: Layer::Low,
            last_change: None,
            backoff: INITIAL_BACKOFF,
            starved_since: None,
        }
    }

    /// Makes the large layer the receiver's from now on (see [`PIN_FLOOR`] for when it is let go),
    /// or hands the choice back to the estimate.
    pub fn pin_high(&mut self, pinned: bool, now: Instant) {
        if !pinned {
            self.pin = None;
        } else if self.pin.is_none() {
            self.pin = Some(Pin { since: now });
            self.starved_since = None;
            self.level = Layer::High;
        }
    }

    pub fn level(&self) -> Layer {
        self.level
    }

    /// Whether the receiver has been starved of the large layer for `patience`: records when it
    /// began, and forgets it when it ends.
    fn starved_for(&mut self, now: Instant, starved: bool, patience: Duration) -> bool {
        if !starved {
            self.starved_since = None;
            return false;
        }
        let since = *self.starved_since.get_or_insert(now);
        now.saturating_duration_since(since) >= patience
    }

    /// Returns the layer to send now, which is the current one unless the inputs call for a change.
    pub fn decide(&mut self, now: Instant, inputs: ScreenInputs) -> Layer {
        let high = inputs.high_rate.unwrap_or(NOMINAL_HIGH_RATE) as f64 * TOLERANCE;
        let arrives = |share: f64| inputs.delivery.is_none_or(|d| d >= share);
        if let Some(since) = self.pin.as_ref().map(|p| p.since) {
            // (A viewer is never on the record layer: it is not forwarded.)
            match self.level {
                Layer::High | Layer::Record => {
                    let starved = now.saturating_duration_since(since) >= PIN_GRACE
                        && (inputs.capacity < high * PIN_FLOOR || !arrives(DROP_DELIVERY));
                    if self.starved_for(now, starved, PIN_PATIENCE) {
                        self.starved_since = None;
                        self.level = Layer::Low;
                    }
                }
                Layer::Low if inputs.capacity > high * PIN_RESUME && arrives(RAISE_DELIVERY) => {
                    self.level = Layer::High
                }
                Layer::Low => {}
            }
            return self.level;
        }
        // A path whose delay says it cannot carry the layer is not enough to take it away: that
        // half of the estimate swings with every burst. It also has to be losing packets.
        let starved = !arrives(DROP_DELIVERY) || (inputs.capacity <= high && !arrives(LOSING));
        let wanted = match self.level {
            Layer::High | Layer::Record => {
                if self.starved_for(now, starved, DROP_PATIENCE) {
                    Layer::Low
                } else {
                    Layer::High
                }
            }
            Layer::Low if inputs.capacity > high && arrives(RAISE_DELIVERY) => Layer::High,
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
        self.starved_since = None;
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

    /// A path that can carry `capacity` and delivers everything.
    fn inputs(capacity: f64) -> ScreenInputs {
        ScreenInputs {
            capacity,
            delivery: Some(1.0),
            high_rate: Some(1_000_000),
        }
    }

    /// A path that can carry the large layer (1 Mbit/s here) and delivers `delivery` of it.
    fn delivering(delivery: f64) -> ScreenInputs {
        ScreenInputs {
            delivery: Some(delivery),
            ..inputs(2_000_000.0)
        }
    }

    /// A path that cannot carry the layer and delivers half of what is sent.
    fn starved() -> ScreenInputs {
        ScreenInputs {
            delivery: Some(0.5),
            ..inputs(100_000.0)
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
        assert_eq!(level.decide(t0, starved()), Layer::High, "not at once");
        assert_eq!(
            level.decide(t0 + DROP_PATIENCE, starved()),
            Layer::Low,
            "the estimate decides again, and a starved link drops"
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
        let unmeasured = |capacity| ScreenInputs {
            high_rate: None,
            ..inputs(capacity)
        };
        assert_eq!(s.decide(t, unmeasured(1_500_000.0)), Layer::Low);
        assert_eq!(s.decide(t, unmeasured(2_400_000.0)), Layer::High);
    }

    #[test]
    fn moving_down_waits_out_the_patience_and_moving_up_the_backoff() {
        let t = Instant::now();
        let at = |s: u64| t + Duration::from_secs(s);
        let mut s = ScreenLevel::new();
        assert_eq!(s.decide(t, inputs(2_000_000.0)), Layer::High);
        assert_eq!(
            s.decide(at(1), starved()),
            Layer::High,
            "a moment of it is not enough"
        );
        assert_eq!(s.decide(at(3), starved()), Layer::Low, "down once it lasts");
        // Bandwidth is back, but the last change was a second ago.
        assert_eq!(s.decide(at(4), inputs(2_000_000.0)), Layer::Low);
        // Each change lengthens the backoff by half: 10 s, 15 s after the first, 22.5 s after the drop.
        assert_eq!(s.decide(at(22), inputs(2_000_000.0)), Layer::Low);
        assert_eq!(s.decide(at(26), inputs(2_000_000.0)), Layer::High);
    }

    #[test]
    fn repeated_flapping_lengthens_the_backoff() {
        let mut t = Instant::now();
        let mut s = ScreenLevel::new();
        let mut gaps = vec![];
        let mut last_up = None;
        for _ in 0..5 {
            // Wait for the upgrade, then be starved until it is taken away again.
            while s.decide(t, inputs(2_000_000.0)) != Layer::High {
                t += Duration::from_secs(1);
            }
            if let Some(prev) = last_up {
                gaps.push(t.duration_since(prev));
            }
            last_up = Some(t);
            while s.decide(t, starved()) != Layer::Low {
                t += Duration::from_secs(1);
            }
        }
        assert!(gaps.windows(2).all(|w| w[1] >= w[0]), "{gaps:?}");
        assert!(gaps.last().unwrap() > &Duration::from_secs(20), "{gaps:?}");
    }

    #[test]
    fn a_delay_based_dip_alone_does_not_drop_the_large_layer() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        s.decide(t, inputs(2_000_000.0));
        // The path's delay says it cannot carry the layer, and everything arrives.
        let dip = inputs(300_000.0);
        for secs in 1..30 {
            assert_eq!(s.decide(t + Duration::from_secs(secs), dip), Layer::High);
        }
    }

    #[test]
    fn a_dip_while_packets_go_missing_drops_the_large_layer_once_it_lasts() {
        let t = Instant::now();
        let at = |s: u64| t + Duration::from_secs(s);
        let mut s = ScreenLevel::new();
        s.decide(t, inputs(2_000_000.0));
        let dip = ScreenInputs {
            delivery: Some(0.9),
            ..inputs(300_000.0)
        };
        assert_eq!(s.decide(at(1), dip), Layer::High);
        // It ends before the patience: nothing is remembered.
        assert_eq!(s.decide(at(2), inputs(2_000_000.0)), Layer::High);
        assert_eq!(s.decide(at(3), dip), Layer::High);
        assert_eq!(s.decide(at(5), dip), Layer::Low);
    }

    #[test]
    fn a_path_that_loses_a_few_per_cent_at_random_gets_the_large_layer_and_keeps_it() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        let lossy = delivering(0.95);
        assert_eq!(s.decide(t, lossy), Layer::High);
        for secs in 1..60 {
            assert_eq!(s.decide(t + Duration::from_secs(secs), lossy), Layer::High);
        }
    }

    #[test]
    fn a_path_that_loses_more_than_a_tenth_is_not_given_the_large_layer() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        for secs in 0..60 {
            assert_eq!(
                s.decide(t + Duration::from_secs(secs), delivering(0.85)),
                Layer::Low
            );
        }
    }

    #[test]
    fn a_large_layer_that_does_not_arrive_is_taken_away_whatever_the_estimate_says() {
        let t = Instant::now();
        let at = |s: u64| t + Duration::from_secs(s);
        let mut s = ScreenLevel::new();
        s.decide(t, inputs(2_000_000.0));
        // The estimate still climbs, but a third of what is sent is lost.
        assert_eq!(s.decide(at(1), delivering(0.6)), Layer::High);
        assert_eq!(s.decide(at(3), delivering(0.6)), Layer::Low);
    }

    #[test]
    fn without_feedback_to_say_what_arrives_the_estimate_decides() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        let unknown = ScreenInputs {
            delivery: None,
            ..inputs(2_000_000.0)
        };
        assert_eq!(s.decide(t, unknown), Layer::High);
        let dip = ScreenInputs {
            delivery: None,
            ..inputs(100_000.0)
        };
        for secs in 1..30 {
            assert_eq!(
                s.decide(t + Duration::from_secs(secs), dip),
                Layer::High,
                "nothing says the layer is not arriving"
            );
        }
    }

    #[test]
    fn a_calm_period_forgets_the_backoff() {
        let t = Instant::now();
        let mut s = ScreenLevel::new();
        s.decide(t, inputs(2_000_000.0));
        s.decide(t + Duration::from_secs(1), starved());
        s.decide(t + Duration::from_secs(3), starved());
        assert!(s.backoff > INITIAL_BACKOFF);
        s.decide(t + Duration::from_secs(600), starved());
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
