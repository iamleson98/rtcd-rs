//! Choosing between the simulcast layers of a video stream.
//!
//! A participant sends a camera or a screen share at two sizes, a small layer (`l`) and a large one
//! (`h`), each in three temporal layers. The SFU sends each receiver one of the sizes, and of a
//! screen share as many of its temporal layers as the receiver's path carries. What decides is
//! policy, kept here as plain functions of measurements so it can be tested without a network: for
//! a screen share what the receiver's path is measured to do with what it is sent (the queue that
//! builds on it, what it delivers, what it loses); for a camera what the receiver asked for and
//! whether the sender still sends the large layer.

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
/// The share of a layer's rate its lower temporal layers take until they have been measured: a
/// browser's encoder gives the base layer two fifths of the bits and the second layer one fifth.
const NOMINAL_TEMPORAL_SHARE: [f64; 2] = [0.4, 0.6];
/// A keyframe not measured yet is taken to be this many seconds of its layer (six frames).
const NOMINAL_KEYFRAME_SECONDS: f64 = 0.4;
/// Moving onto the other layer is tried only where its keyframe would drain within this long,
/// with what the path carries beyond the new rung.
const KEYFRAME_DRAIN: f64 = 2.5;

/// A path holding this many seconds of what is sent to it for [`QUEUE_PERSISTS`], while the queue
/// is not shrinking, carries less than it is given (the queue a keyframe leaves shrinks once the
/// keyframe has gone in), as does one holding [`QUEUE_CRITICAL`] whatever the queue does. The
/// feedback that says so comes through the queue too, so it is late by as much as it reports...
const QUEUE_HIGH: f64 = 0.15;
const QUEUE_PERSISTS: Duration = Duration::from_secs(1);
const QUEUE_CRITICAL: f64 = 1.5;
/// A queue shrinking by less than this over the half second the trend covers is not shrinking.
const QUEUE_SHRINKING: f64 = -0.02;
/// One growing by this much over the half second is fed at least a third more than the path
/// carries: that is acted on without waiting for it to last, except in the seconds after a move to
/// the large layer, whose keyframe sets a queue all at once.
const QUEUE_SURGING: f64 = 0.15;
const KEYFRAME_GRACE: Duration = Duration::from_secs(2);
/// For a few seconds after a temporal layer is added (which costs no keyframe), a queue that grows
/// past this means the layer did not fit.
const TRIAL_QUEUE: f64 = 0.08;
const TRIAL: Duration = Duration::from_secs(3);
/// ...and one holding less than this carries all of it.
const QUEUE_LOW: f64 = 0.05;
/// Less than this share of what is sent arriving is a path that drops what it cannot carry (one
/// that queues little, or not at all)...
const CONGESTED_DELIVERY: f64 = 0.85;
/// ...unless nothing stands queued and the narrowest link of the path is known to carry this many
/// times what is sent: then what was lost is a burst (a wireless hiccup, a router's blip) that
/// the receiver's requests for packets sent again repair, not a path that carries less than it is
/// given. A picture that is still sends little, so what the path delivered says nothing of what it
/// can carry, and cutting the rate to it left the receiver on the lowest rung until the long climb
/// back.
const LOSS_ROOM: f64 = 1.5;
/// The next rung is tried only while this much of what is sent arrives: a path that loses more
/// than a tenth at random is not given more to lose.
const RAISE_DELIVERY: f64 = 0.9;
/// After congestion the screen is given this share of what the path delivered, or less while it
/// has a queue to drain, so the queue is gone within [`DRAIN_SECONDS`] instead of holding the
/// voice back...
const HEADROOM: f64 = 0.85;
const DRAIN_SECONDS: f64 = 3.0;
const LEAST_SHARE: f64 = 0.25;
/// ...and nothing more is tried for a while.
const HOLD: Duration = Duration::from_secs(3);
/// A path that has stayed clear this long on a rung is trusted with a little more each second, up
/// to twice what it was seen to deliver or the rate of the rung above, whichever is more (a layer
/// without temporal layers is one step of five times its rate)...
const CLEAR_FOR: Duration = Duration::from_secs(2);
const GROWTH_PER_SECOND: f64 = 0.08;
const GROWTH_CEILING: f64 = 2.0;
/// ...and the next rung is tried once that adds up to its rate, and fits in this share of what the
/// narrowest link of the path carries (when that is known). A rung that brought congestion is not
/// tried again for a wait that doubles each time, and is forgotten once held without trouble.
const CAPACITY_SHARE: f64 = 0.85;
const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(120);
const FORGET_AFTER: Duration = Duration::from_secs(30);

/// One step of what a receiver can be sent of a screen share: a simulcast layer, and the highest
/// of its temporal layers sent (`None`: every frame of the layer).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rung {
    pub layer: Layer,
    pub tid: Option<u8>,
}

/// What a sharer sends of one simulcast layer, as measured.
#[derive(Debug, Clone, Copy, Default)]
pub struct LayerRates {
    /// Bits per second of the whole layer, once measured.
    pub total: Option<u64>,
    /// Bits per second of the base temporal layer and of the second, for a layer whose frames
    /// carry temporal layers.
    pub temporal: Option<[Option<u64>; 2]>,
    /// The size of the layer's last keyframe, bits.
    pub keyframe_bits: Option<f64>,
}

/// The rungs a screen share offers now, lowest first, with the bits per second each costs, and the
/// size of a keyframe of each layer (what moving onto the layer costs at once).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Ladder {
    rungs: Vec<(Rung, f64)>,
    keyframe_bits: [f64; 2],
}

impl Ladder {
    /// The rungs of the layers a sharer sends (`low`, `high`; `None` for a layer it does not):
    /// each temporal layer of a layer that has them is a rung, cumulative, the whole layer the
    /// top one; a layer not measured yet costs its nominal rate.
    pub fn new(low: Option<LayerRates>, high: Option<LayerRates>) -> Self {
        let mut rungs = Vec::new();
        let mut keyframe_bits = [0.0; 2];
        for (layer, rates, nominal) in [
            (Layer::Low, low, NOMINAL_LOW_RATE),
            (Layer::High, high, NOMINAL_HIGH_RATE),
        ] {
            let Some(rates) = rates else { continue };
            let total = rates.total.unwrap_or(nominal) as f64;
            keyframe_bits[layer as usize] = rates
                .keyframe_bits
                .unwrap_or(nominal as f64 * NOMINAL_KEYFRAME_SECONDS);
            if let Some(temporal) = rates.temporal {
                let mut below = 0.0;
                for (tid, measured) in temporal.iter().enumerate() {
                    let rate = match measured {
                        Some(r) => below + *r as f64,
                        None => total * NOMINAL_TEMPORAL_SHARE[tid],
                    };
                    below = rate;
                    rungs.push((
                        Rung {
                            layer,
                            tid: Some(tid as u8),
                        },
                        rate.min(total),
                    ));
                }
            }
            rungs.push((Rung { layer, tid: None }, total));
        }
        Self {
            rungs,
            keyframe_bits,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.rungs.is_empty()
    }

    fn position(&self, rung: Rung) -> Option<usize> {
        self.rungs.iter().position(|(r, _)| *r == rung)
    }

    fn rate(&self, i: usize) -> f64 {
        self.rungs[i].1
    }

    fn top(&self) -> usize {
        self.rungs.len() - 1
    }

    /// Whether rung `next` fits the path's narrowest link, when that is known: its rate in a share
    /// of it, and, for a move from `current` onto the other layer, the keyframe that costs drained
    /// within [`KEYFRAME_DRAIN`].
    fn fits(&self, current: usize, next: usize, inputs: &ScreenInputs) -> bool {
        let Some(capacity) = inputs.capacity else {
            return true;
        };
        let load = self.rate(next) + inputs.other;
        let (from, to) = (self.rungs[current].0, self.rungs[next].0);
        let keyframe = if from.layer == to.layer {
            0.0
        } else {
            self.keyframe_bits[to.layer as usize]
        };
        load <= capacity * CAPACITY_SHARE
            && (keyframe == 0.0 || keyframe <= (capacity - load) * KEYFRAME_DRAIN)
    }

    /// The whole of the small layer: where a receiver starts.
    fn start(&self) -> usize {
        self.rungs
            .iter()
            .rposition(|(r, _)| r.layer == Layer::Low)
            .unwrap_or(0)
    }
}

/// What the receiver's feedback says of its path (see `bwe`), for one decision.
#[derive(Debug, Clone, Copy, Default)]
pub struct ScreenInputs {
    /// Seconds of what is sent that stand queued on the path, and how they moved over the last
    /// half second.
    pub queue: Option<f64>,
    pub queue_trend: Option<f64>,
    /// Bits per second of video the path delivered over the last second.
    pub acked: Option<f64>,
    /// Bits per second the narrowest link of the path carries.
    pub capacity: Option<f64>,
    /// The share of what was sent over the last seconds that arrived.
    pub delivery: Option<f64>,
    /// Bits per second of the other video the receiver is sent (cameras): it shares the path.
    pub other: f64,
}

impl ScreenInputs {
    /// Whether the path may be carrying less than it is sent: it queues it, or drops it. Whether
    /// it is, `ScreenLevel` judges from how long it lasts.
    pub fn congested(&self) -> bool {
        self.queue.is_some_and(|q| q >= QUEUE_HIGH) || self.dropping()
    }

    fn dropping(&self) -> bool {
        self.delivery.is_some_and(|d| d < CONGESTED_DELIVERY)
    }

    fn clear(&self) -> bool {
        self.queue.is_some_and(|q| q < QUEUE_LOW)
            && self.delivery.is_none_or(|d| d >= RAISE_DELIVERY)
    }
}

/// Decides which rung of a screen share one receiver is sent, so that what it is sent never
/// outgrows what its path carries.
///
/// A path that is given more than it carries says so first in its queue: the excess waits there,
/// and everything on the link waits behind it, the voice that shares it too; a path that queues
/// little drops it instead. Either is congestion: the receiver is moved down at once to what the
/// path was measured to deliver, with room for the queue to drain. While the path stays clear the
/// rung above is tried, one at a time, once the trust a clear path earns adds up to its rate; a
/// rung that brought congestion waits longer each time before it is tried again. A temporal layer
/// costs nothing to drop or take up again, so most moves are between those; moving between the
/// small and the large layer costs a keyframe.
#[derive(Debug)]
pub struct ScreenLevel {
    /// The receiver asked for the sharp picture: it starts on the top rung.
    pin: bool,
    rung: Option<Rung>,
    /// Bits per second the receiver's path is trusted with for the screen.
    budget: f64,
    last: Option<Instant>,
    clear_since: Option<Instant>,
    hold_until: Option<Instant>,
    /// Since when the path has held [`QUEUE_HIGH`].
    queued_since: Option<Instant>,
    /// Until when the queue the large layer's keyframe sets is expected, or a temporal layer just
    /// added is on trial.
    keyframe_grace: Option<Instant>,
    trial_until: Option<Instant>,
    /// Since when the current rung has been held without congestion.
    calm_since: Option<Instant>,
    /// Rungs that brought congestion: when each may be tried again, and the wait after that.
    backoff: Vec<(Rung, Instant, Duration)>,
}

impl ScreenLevel {
    /// Starts on the small layer, which every receiver can take.
    pub fn new() -> Self {
        Self {
            pin: false,
            rung: None,
            budget: 0.0,
            last: None,
            clear_since: None,
            hold_until: None,
            queued_since: None,
            keyframe_grace: None,
            trial_until: None,
            calm_since: None,
            backoff: Vec::new(),
        }
    }

    /// Whether the path carries less than the receiver is sent (see [`QUEUE_HIGH`]). `room`: the
    /// narrowest link is known to carry far more than is sent (see [`LOSS_ROOM`]), so loss alone,
    /// with nothing queued, is not congestion.
    fn congested(&mut self, now: Instant, inputs: &ScreenInputs, room: bool) -> bool {
        let trend = inputs.queue_trend.unwrap_or(0.0);
        if self.trial_until.is_some_and(|t| now < t)
            && inputs.queue.is_some_and(|q| q >= TRIAL_QUEUE)
            && trend > 0.0
        {
            return true;
        }
        let Some(q) = inputs.queue.filter(|&q| q >= QUEUE_HIGH) else {
            self.queued_since = None;
            return inputs.dropping() && !room;
        };
        let since = *self.queued_since.get_or_insert(now);
        let surging = trend >= QUEUE_SURGING && self.keyframe_grace.is_none_or(|t| now >= t);
        inputs.dropping()
            || q >= QUEUE_CRITICAL
            || surging
            || (now.saturating_duration_since(since) >= QUEUE_PERSISTS && trend >= QUEUE_SHRINKING)
    }

    /// Starts the receiver on the top rung from the next decision (the sharp picture it asked
    /// for), or lets it start as any receiver does.
    pub fn pin_high(&mut self, pinned: bool) {
        if pinned && !self.pin {
            self.rung = None;
            self.hold_until = None;
        }
        self.pin = pinned;
    }

    /// The simulcast layer the receiver is sent.
    pub fn level(&self) -> Layer {
        self.rung
            .map_or(if self.pin { Layer::High } else { Layer::Low }, |r| r.layer)
    }

    /// The highest temporal layer the receiver is sent, `None` for all.
    pub fn temporal_limit(&self) -> Option<u8> {
        self.rung.and_then(|r| r.tid)
    }

    pub fn rung(&self) -> Option<Rung> {
        self.rung
    }

    fn may_try(&self, rung: Rung, now: Instant) -> bool {
        self.backoff
            .iter()
            .find(|(r, ..)| *r == rung)
            .is_none_or(|&(_, at, _)| now >= at)
    }

    fn failed(&mut self, rung: Rung, now: Instant) {
        match self.backoff.iter_mut().find(|(r, ..)| *r == rung) {
            Some(entry) => {
                entry.2 = (entry.2 * 2).min(MAX_BACKOFF);
                entry.1 = now + entry.2;
            }
            None => self
                .backoff
                .push((rung, now + INITIAL_BACKOFF, INITIAL_BACKOFF)),
        }
    }

    /// Returns the rung to send now, from what the path says and what the sharer offers.
    pub fn decide(&mut self, now: Instant, inputs: ScreenInputs, ladder: &Ladder) -> Rung {
        if ladder.is_empty() {
            return self.rung.unwrap_or(Rung {
                layer: self.level(),
                tid: None,
            });
        }
        let elapsed = self
            .last
            .map_or(Duration::ZERO, |t| now.saturating_duration_since(t))
            .min(Duration::from_secs(2));
        self.last = Some(now);
        let current = match self.rung.and_then(|r| ladder.position(r)) {
            Some(i) => i,
            None => {
                // The first decision, or the sharer changed what it offers: start afresh.
                let i = if self.rung.is_none() && self.pin {
                    ladder.top()
                } else {
                    ladder.start()
                };
                self.budget = self.budget.max(ladder.rate(i));
                i
            }
        };
        let mut target = current;
        let room = inputs.capacity.is_some_and(|c| {
            c * CAPACITY_SHARE >= (ladder.rate(current) + inputs.other) * LOSS_ROOM
        });
        if self.congested(now, &inputs, room) {
            let carried = inputs.acked.map(|a| a - inputs.other);
            let share = HEADROOM
                .min(1.0 - inputs.queue.unwrap_or(0.0) / DRAIN_SECONDS)
                .max(LEAST_SHARE);
            let cut = carried
                .map_or(self.budget * HEADROOM, |c| c * share)
                .max(0.0);
            self.budget = self.budget.min(cut);
            self.hold_until = Some(now + HOLD);
            self.clear_since = None;
            self.calm_since = None;
            while target > 0 && ladder.rate(target) > self.budget {
                target -= 1;
            }
            if target < current {
                self.failed(ladder.rungs[current].0, now);
            }
        } else if inputs.clear() {
            let since = *self.clear_since.get_or_insert(now);
            let held = self.hold_until.is_none_or(|t| now >= t);
            if held {
                let seen = inputs
                    .acked
                    .map_or(0.0, |a| a - inputs.other)
                    .max(ladder.rate(current));
                let grown = self.budget * (1.0 + GROWTH_PER_SECOND * elapsed.as_secs_f64());
                let above = ladder.rate((current + 1).min(ladder.top()));
                // A narrowest link known to carry more is trusted at once.
                let link = inputs
                    .capacity
                    .map_or(0.0, |c| c * CAPACITY_SHARE - inputs.other);
                self.budget = grown.min((GROWTH_CEILING * seen).max(above)).max(link);
            }
            let next = current + 1;
            if next < ladder.rungs.len()
                && held
                && now.saturating_duration_since(since) >= CLEAR_FOR
                && self.budget >= ladder.rate(next)
                && ladder.fits(current, next, &inputs)
                && self.may_try(ladder.rungs[next].0, now)
            {
                target = next;
            }
        } else {
            self.clear_since = None;
        }
        if target == current {
            let calm = *self.calm_since.get_or_insert(now);
            if now.saturating_duration_since(calm) >= FORGET_AFTER {
                let rung = ladder.rungs[current].0;
                self.backoff.retain(|(r, ..)| *r != rung);
            }
        } else {
            // Each rung earns the next on its own.
            self.calm_since = None;
            self.clear_since = None;
            let (from, to) = (ladder.rungs[current].0, ladder.rungs[target].0);
            if target > current {
                if from.layer == to.layer {
                    self.trial_until = Some(now + TRIAL);
                } else {
                    self.keyframe_grace = Some(now + KEYFRAME_GRACE);
                }
            } else {
                self.trial_until = None;
            }
        }
        let rung = ladder.rungs[target].0;
        self.rung = Some(rung);
        rung
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

    /// What a browser sends of a screen: 500 kbit/s small, 2.5 Mbit/s large, each in three
    /// temporal layers taking two fifths, one fifth and two fifths of it.
    fn ladder() -> Ladder {
        let layer = |total: u64| LayerRates {
            total: Some(total),
            temporal: Some([Some(total * 2 / 5), Some(total / 5)]),
            keyframe_bits: Some(total as f64 * 0.4),
        };
        Ladder::new(Some(layer(500_000)), Some(layer(2_500_000)))
    }

    fn rung(layer: Layer, tid: Option<u8>) -> Rung {
        Rung { layer, tid }
    }

    /// A path with nothing queued that delivers all of `acked`.
    fn clear(acked: f64) -> ScreenInputs {
        ScreenInputs {
            queue: Some(0.01),
            queue_trend: Some(0.0),
            acked: Some(acked),
            capacity: None,
            delivery: Some(1.0),
            other: 0.0,
        }
    }

    /// A path holding `queue` seconds, and more each moment, and delivering `acked`.
    fn queued(queue: f64, acked: f64) -> ScreenInputs {
        ScreenInputs {
            queue: Some(queue),
            queue_trend: Some(0.05),
            ..clear(acked)
        }
    }

    /// Decides at `t` and a second later with the same inputs: a queue has to last.
    fn twice(s: &mut ScreenLevel, t: &mut Instant, inputs: ScreenInputs) -> (Rung, Rung) {
        *t += Duration::from_secs(1);
        let first = s.decide(*t, inputs, &ladder());
        *t += Duration::from_secs(1);
        (first, s.decide(*t, inputs, &ladder()))
    }

    /// Runs the decision once a second for `secs` seconds from `t`, with the same inputs.
    fn hold(s: &mut ScreenLevel, t: &mut Instant, secs: u64, inputs: ScreenInputs) -> Rung {
        let mut last = s.decide(*t, inputs, &ladder());
        for _ in 0..secs {
            *t += Duration::from_secs(1);
            last = s.decide(*t, inputs, &ladder());
        }
        last
    }

    #[test]
    fn the_ladder_has_a_rung_per_temporal_layer_cumulative() {
        let rates: Vec<_> = ladder()
            .rungs
            .iter()
            .map(|&(r, bps)| (r, bps as u64))
            .collect();
        assert_eq!(
            rates,
            vec![
                (rung(Layer::Low, Some(0)), 200_000),
                (rung(Layer::Low, Some(1)), 300_000),
                (rung(Layer::Low, None), 500_000),
                (rung(Layer::High, Some(0)), 1_000_000),
                (rung(Layer::High, Some(1)), 1_500_000),
                (rung(Layer::High, None), 2_500_000),
            ]
        );
    }

    #[test]
    fn a_layer_without_temporal_layers_is_one_rung_and_an_unmeasured_one_costs_its_nominal_rate() {
        let plain = LayerRates {
            total: Some(400_000),
            temporal: None,
            keyframe_bits: None,
        };
        let unmeasured = LayerRates {
            total: None,
            temporal: Some([None, None]),
            keyframe_bits: None,
        };
        let l = Ladder::new(Some(plain), Some(unmeasured));
        let rates: Vec<_> = l.rungs.iter().map(|&(r, bps)| (r, bps as u64)).collect();
        assert_eq!(
            rates,
            vec![
                (rung(Layer::Low, None), 400_000),
                (rung(Layer::High, Some(0)), 1_000_000),
                (rung(Layer::High, Some(1)), 1_500_000),
                (rung(Layer::High, None), 2_500_000),
            ]
        );
    }

    #[test]
    fn the_large_layer_is_tried_only_where_its_keyframe_would_drain_within_seconds() {
        let l = ladder();
        let small = l.start();
        let link = |capacity| ScreenInputs {
            capacity: Some(capacity),
            ..clear(500_000.0)
        };
        // On 1.2 Mbit/s the base of the large layer (1 Mbit/s) fits, but the keyframe moving onto
        // it costs (1 Mbit here) would take 5 s to drain beside it.
        assert!(!l.fits(small, small + 1, &link(1_200_000.0)));
        assert!(l.fits(small, small + 1, &link(1_600_000.0)));
        // A temporal layer costs no keyframe: only its rate counts.
        let base = small + 1;
        assert!(l.fits(base, base + 1, &link(1_800_000.0)));
        assert!(!l.fits(base, base + 1, &link(1_700_000.0)));
        assert!(l.fits(base, base + 1, &clear(500_000.0)), "an unknown link");
    }

    #[test]
    fn a_receiver_starts_on_the_whole_small_layer_and_climbs_one_rung_at_a_time() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        assert_eq!(
            s.decide(t, clear(500_000.0), &ladder()),
            rung(Layer::Low, None)
        );
        let mut climbed = vec![];
        for _ in 0..120 {
            t += Duration::from_secs(1);
            let r = s.decide(t, clear(4_000_000.0), &ladder());
            if climbed.last() != Some(&r) {
                climbed.push(r);
            }
        }
        assert_eq!(
            climbed,
            vec![
                rung(Layer::Low, None),
                rung(Layer::High, Some(0)),
                rung(Layer::High, Some(1)),
                rung(Layer::High, None)
            ]
        );
    }

    #[test]
    fn a_queue_that_lasts_moves_the_receiver_to_what_the_path_delivered() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        assert_eq!(s.rung(), Some(rung(Layer::High, None)));
        // 300 ms queued, and growing: the path carries 1.7 Mbit/s of the 2.5 it is sent.
        let (first, then) = twice(&mut s, &mut t, queued(0.3, 1_700_000.0));
        assert_eq!(
            first,
            rung(Layer::High, None),
            "a moment of it is not enough"
        );
        assert_eq!(
            then,
            rung(Layer::High, Some(0)),
            "1.7 Mbit/s, less the headroom and the queue to drain: the base of the large layer"
        );
    }

    #[test]
    fn a_queue_that_shrinks_is_a_burst_going_through_not_congestion() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        // A keyframe left 600 ms queued, draining.
        let draining = ScreenInputs {
            queue_trend: Some(-0.15),
            ..queued(0.6, 1_700_000.0)
        };
        let (a, b) = twice(&mut s, &mut t, draining);
        assert_eq!((a, b), (rung(Layer::High, None), rung(Layer::High, None)));
        // It stops draining.
        t += Duration::from_secs(1);
        assert_eq!(
            s.decide(t, queued(0.6, 1_700_000.0), &ladder()),
            rung(Layer::High, Some(0))
        );
    }

    #[test]
    fn a_long_queue_is_drained_by_going_lower() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        t += Duration::from_secs(1);
        assert_eq!(
            s.decide(t, queued(2.0, 1_700_000.0), &ladder()),
            rung(Layer::Low, None),
            "a queue that long is acted on at once, and drained within seconds"
        );
    }

    #[test]
    fn a_path_that_drops_what_it_cannot_carry_is_congested_too() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        let policed = ScreenInputs {
            delivery: Some(0.7),
            ..clear(1_750_000.0)
        };
        t += Duration::from_secs(1);
        assert_eq!(s.decide(t, policed, &ladder()), rung(Layer::High, Some(0)));
    }

    #[test]
    fn a_rung_that_brought_congestion_waits_longer_each_time() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        let mut tries = vec![];
        let start = t;
        let top = rung(Layer::High, None);
        let mut was = None;
        for _ in 0..600 {
            t += Duration::from_secs(1);
            // The path carries 2 Mbit/s: the top rung queues, the one below does not.
            let on_top = s.rung() == Some(top);
            if on_top && was != Some(top) {
                tries.push(t.duration_since(start).as_secs());
            }
            was = s.rung();
            let inputs = if on_top {
                queued(0.25, 2_000_000.0)
            } else {
                clear(s.rung().map_or(500_000.0, |_| 1_500_000.0))
            };
            s.decide(t, inputs, &ladder());
        }
        let gaps: Vec<u64> = tries.windows(2).map(|w| w[1] - w[0]).collect();
        assert!(gaps.len() >= 3, "{tries:?}");
        // Doubling, until the longest wait.
        let cap = MAX_BACKOFF.as_secs() + 1;
        assert!(
            gaps.windows(2).all(|g| g[1] > g[0] || g[1] == cap),
            "{gaps:?}"
        );
        assert_eq!(*gaps.last().unwrap(), cap, "{gaps:?}");
    }

    #[test]
    fn a_burst_of_loss_on_a_path_with_room_is_left_to_the_receivers_repair_requests() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        let room = |capacity| ScreenInputs {
            delivery: Some(0.7),
            capacity: Some(capacity),
            ..clear(400_000.0)
        };
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        assert_eq!(s.rung(), Some(rung(Layer::High, None)));
        // A still picture sends little, so what arrived (400 kbit/s) says nothing of what the
        // path carries; its narrowest link carries 20 Mbit/s, 8 times the large layer.
        t += Duration::from_secs(1);
        assert_eq!(
            s.decide(t, room(20_000_000.0), &ladder()),
            rung(Layer::High, None),
            "loss alone, with nothing queued and room to spare, is not congestion"
        );
        // The same loss on a link that carries little more than is sent is.
        t += Duration::from_secs(1);
        assert_eq!(
            s.decide(t, room(3_000_000.0), &ladder()),
            rung(Layer::Low, Some(1)),
            "what arrived, less the headroom, is all the path is known to carry"
        );
    }

    #[test]
    fn a_path_that_loses_a_few_per_cent_at_random_climbs_all_the_same() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        let lossy = ScreenInputs {
            delivery: Some(0.93),
            ..clear(4_000_000.0)
        };
        assert_eq!(hold(&mut s, &mut t, 120, lossy), rung(Layer::High, None));
    }

    #[test]
    fn a_path_that_loses_more_than_a_tenth_is_given_nothing_more() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        let lossy = ScreenInputs {
            delivery: Some(0.87),
            ..clear(4_000_000.0)
        };
        assert_eq!(hold(&mut s, &mut t, 120, lossy), rung(Layer::Low, None));
    }

    #[test]
    fn without_a_reading_of_the_path_nothing_more_is_tried() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        let unknown = ScreenInputs::default();
        assert_eq!(hold(&mut s, &mut t, 60, unknown), rung(Layer::Low, None));
    }

    #[test]
    fn the_other_video_on_the_path_is_left_its_share() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        // 2 Mbit/s delivered, 600 kbit/s of it a camera.
        let with_camera = ScreenInputs {
            other: 600_000.0,
            ..queued(0.25, 2_000_000.0)
        };
        assert_eq!(
            twice(&mut s, &mut t, with_camera).1,
            rung(Layer::High, Some(0))
        );
    }

    #[test]
    fn a_pinned_receiver_starts_on_the_top_rung_and_still_comes_down_when_the_path_is_congested() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        s.pin_high(true);
        assert_eq!(s.level(), Layer::High);
        assert_eq!(
            s.decide(t, clear(500_000.0), &ladder()),
            rung(Layer::High, None)
        );
        assert_eq!(
            twice(&mut s, &mut t, queued(0.4, 900_000.0)).1,
            rung(Layer::Low, None)
        );
    }

    #[test]
    fn a_rung_held_without_trouble_for_a_minute_is_forgiven() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        twice(&mut s, &mut t, queued(0.25, 2_000_000.0));
        assert!(!s.backoff.is_empty());
        hold(&mut s, &mut t, 60, clear(1_500_000.0));
        hold(&mut s, &mut t, 61, clear(2_600_000.0));
        assert!(s.backoff.is_empty(), "{:?}", s.backoff);
    }

    #[test]
    fn rids_map_to_layers() {
        assert_eq!(Layer::from_rid(Some("h")), Layer::High);
        assert_eq!(Layer::from_rid(Some("l")), Layer::Low);
        assert_eq!(Layer::from_rid(None), Layer::Low);
        assert_eq!(Layer::High.other(), Layer::Low);
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
