//! Choosing between the simulcast layers of a video stream.
//!
//! A participant sends a camera or a screen share at two sizes, a small layer (`l`) and a large one
//! (`h`), each in three temporal layers. The SFU sends each receiver one of the sizes, and of a
//! screen share as many of its temporal layers as the receiver's path carries. What decides is
//! policy, kept here as plain functions of measurements so it can be tested without a network: for
//! a screen share what the receiver's path is measured to do with what it is sent (the queue that
//! builds on it, what it delivers, what it loses); for a camera what the receiver asked for and
//! whether the sender still sends the large layer.

use std::collections::VecDeque;
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
pub const NOMINAL_HIGH_RATE: u64 = 4_000_000;
pub const NOMINAL_LOW_RATE: u64 = 500_000;
/// The share of a layer's rate its lower temporal layers take until they have been measured: a
/// browser's encoder gives the base layer two fifths of the bits and the second layer one fifth.
const NOMINAL_TEMPORAL_SHARE: [f64; 2] = [0.4, 0.6];
/// A keyframe not measured yet is taken to be this many seconds of its layer (six frames).
pub(crate) const NOMINAL_KEYFRAME_SECONDS: f64 = 0.4;
/// Moving onto the other layer is tried only where its keyframe would drain within this long,
/// with what the path carries beyond the new rung.
pub(crate) const KEYFRAME_DRAIN: f64 = 2.5;

/// A path holding this many seconds of what is sent to it for [`QUEUE_PERSISTS`], while the queue
/// is not shrinking, carries less than it is given (the queue a keyframe leaves shrinks once the
/// keyframe has gone in), as does one holding [`QUEUE_CRITICAL`] whatever the queue does. The
/// feedback that says so comes through the queue too, so it is late by as much as it reports...
pub(crate) const QUEUE_HIGH: f64 = 0.15;
pub(crate) const QUEUE_PERSISTS: Duration = Duration::from_secs(1);
pub(crate) const QUEUE_CRITICAL: f64 = 1.5;
/// A queue shrinking by less than this over the half second the trend covers is not shrinking.
pub(crate) const QUEUE_SHRINKING: f64 = -0.02;
/// One growing by this much over the half second is fed at least a third more than the path
/// carries: that is acted on without waiting for it to last, once two readings in a row say so,
/// except in the seconds after a keyframe was asked for or a move to the large layer, whose
/// keyframe sets a queue all at once.
pub(crate) const QUEUE_SURGING: f64 = 0.15;
pub(crate) const KEYFRAME_GRACE: Duration = Duration::from_secs(2);
/// How long after a keyframe was sent its queue is not yet expected to show: the feedback that
/// reports it is a round trip late, and the pacer spreads the keyframe over a third of a second.
pub(crate) const KEYFRAME_LAG: Duration = Duration::from_millis(500);
/// For a few seconds after a temporal layer is added (which costs no keyframe), a queue that grows
/// past this means the layer did not fit.
const TRIAL_QUEUE: f64 = 0.08;
pub(crate) const TRIAL: Duration = Duration::from_secs(3);
/// ...and one holding less than this carries all of it.
const QUEUE_LOW: f64 = 0.05;
/// Less than this share of what is sent arriving is a path that drops what it cannot carry (one
/// that queues little, or not at all)...
pub(crate) const CONGESTED_DELIVERY: f64 = 0.85;
/// ...unless nothing stands queued and the narrowest link of the path is known to carry this many
/// times what is sent: then what was lost is a burst (a wireless hiccup, a router's blip) that
/// the receiver's requests for packets sent again repair, not a path that carries less than it is
/// given. A picture that is still sends little, so what the path delivered says nothing of what it
/// can carry, and cutting the rate to it left the receiver on the lowest rung until the long climb
/// back.
pub(crate) const LOSS_ROOM: f64 = 1.5;
/// The next rung is tried only while this much of what is sent arrives: a path that loses more
/// than a tenth at random is not given more to lose.
pub(crate) const RAISE_DELIVERY: f64 = 0.9;
/// After congestion the screen is given this share of what the path delivered, or less while it
/// has a queue to drain, so the queue is gone within [`DRAIN_SECONDS`] instead of holding the
/// voice back...
pub(crate) const HEADROOM: f64 = 0.85;
pub(crate) const DRAIN_SECONDS: f64 = 3.0;
pub(crate) const LEAST_SHARE: f64 = 0.25;
/// ...and nothing more is tried for a while.
pub(crate) const HOLD: Duration = Duration::from_secs(3);
/// A path that has stayed clear this long on a rung is trusted with a little more each second, up
/// to twice what it was seen to deliver or the rate of the rung above, whichever is more (a layer
/// without temporal layers is one step of five times its rate)...
pub(crate) const CLEAR_FOR: Duration = Duration::from_secs(2);
pub(crate) const GROWTH_PER_SECOND: f64 = 0.08;
pub(crate) const GROWTH_CEILING: f64 = 2.0;
/// ...and the next rung is tried once that adds up to its rate, and fits in this share of what the
/// narrowest link of the path carries (when that is known). A rung that brought congestion is not
/// tried again for a wait that doubles each time, and is forgotten once held without trouble.
pub(crate) const CAPACITY_SHARE: f64 = 0.85;
const INITIAL_BACKOFF: Duration = Duration::from_secs(10);
const MAX_BACKOFF: Duration = Duration::from_secs(120);
const FORGET_AFTER: Duration = Duration::from_secs(30);
/// A rung's wait also shrinks by this share of every calm second, on whatever rung: a blip every
/// half minute or so is not a rung that fails again at once, and a viewer parked below the rate
/// its path carries for want of a longer calm stretch on one rung is not parked for ever.
const BACKOFF_DECAY: f64 = 0.25;
/// A path that delivered this much of what it was sent lost nothing: what it carried was all that
/// was sent, which says nothing of what it could carry...
pub(crate) const DELIVERED_ALL: f64 = 0.97;
/// ...when what it was sent was this share of the rung's rate or more (a picture that is still
/// sends little), and a verdict of congestion then costs the one rung, not what that was acked.
pub(crate) const SENT_RUNG: f64 = 0.8;

/// What a receiver's plan lets it be sent in all, the voices included, is held to in bits per
/// second, and what the voices take of it is kept back for them: video gets the budget less this.
pub const VOICE_RESERVE: f64 = 100_000.0;
/// A step up under a receiver's budget is taken only where everything sent after it stays within
/// this share of what is allowed: a rate that sits at the edge of its allowance is not given
/// more, and a measurement that swings a tenth either way moves nothing twice. (The paths'
/// `RAISE_MARGIN` is not used for this: it would keep a speaker's large layer off a budget that
/// carries it.)
pub const BUDGET_RAISE_SHARE: f64 = 0.9;
/// After the budget cut anything, nothing is taken up for this long.
pub const BUDGET_HOLD: Duration = Duration::from_secs(10);

/// What the video part of a receiver's budget leaves for the cameras and for the screen share.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Split {
    /// Most the screen share may cost: what is left after the cameras at their small layer.
    pub screen: f64,
    /// Most the cameras may cost: what is left after the screen share as it is sent.
    pub cameras: f64,
}

/// Splits a receiver's `budget` (bits per second, voices included) between its cameras and its
/// screen share, given what the cameras cost whole on their small layers (`cameras_small`) and
/// what the screen share costs as it is sent (`screen_sent`).
///
/// The cameras are held to what the screen share leaves and the screen share to what the cameras
/// at their small layers leave, which orders the cuts: the cameras give up their large layers
/// first, then the screen share gives up temporal layers and its large layer, and only then do
/// the cameras thin and pause, by rank. The voices are never touched.
pub fn split_budget(budget: f64, cameras_small: f64, screen_sent: f64) -> Split {
    let video = (budget - VOICE_RESERVE).max(0.0);
    Split {
        screen: (video - cameras_small).max(0.0),
        cameras: (video - screen_sent).max(0.0),
    }
}

/// The highest of what was recorded over the last [`BUDGET_HOLD`]: a screen share whose rate swings
/// from one second to the next would otherwise move the cameras' room with it, and they would
/// follow it up and down.
#[derive(Debug, Default)]
pub struct RecentPeak {
    samples: VecDeque<(Instant, f64)>,
}

impl RecentPeak {
    /// Records `value` at `now` and returns the highest recorded within [`BUDGET_HOLD`].
    pub fn record(&mut self, now: Instant, value: f64) -> f64 {
        while self
            .samples
            .front()
            .is_some_and(|&(t, _)| now.saturating_duration_since(t) > BUDGET_HOLD)
        {
            self.samples.pop_front();
        }
        self.samples.push_back((now, value));
        self.samples.iter().map(|&(_, v)| v).fold(0.0, f64::max)
    }
}

/// When a receiver's budget last cut something: nothing is taken up for [`BUDGET_HOLD`] after.
#[derive(Debug, Default, Clone, Copy)]
pub struct BudgetHold {
    cut_at: Option<Instant>,
}

impl BudgetHold {
    pub fn may_raise(&self, now: Instant) -> bool {
        self.cut_at
            .is_none_or(|t| now.saturating_duration_since(t) >= BUDGET_HOLD)
    }

    pub fn cut(&mut self, now: Instant) {
        self.cut_at = Some(now);
    }
}

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

/// What one layer of a stream costs a receiver at each of its temporal steps, cheapest first: the
/// base layer, the base and the second, and the whole layer (the only step of a layer that has no
/// temporal layers).
#[derive(Debug, Clone, Copy)]
pub struct Steps {
    /// The highest temporal layer sent (`None`: every frame), and the bits per second it costs.
    steps: [(Option<u8>, f64); 3],
    len: usize,
}

impl Steps {
    /// The steps of a layer from what was measured of it, `nominal` bits per second while it has
    /// not been measured.
    pub fn of(rates: LayerRates, nominal: u64) -> Self {
        let total = rates.total.unwrap_or(nominal) as f64;
        let mut steps = [(None, total); 3];
        let mut len = 0;
        if let Some(temporal) = rates.temporal {
            let mut below = 0.0;
            for (tid, measured) in temporal.iter().enumerate() {
                let rate = match measured {
                    Some(r) => below + *r as f64,
                    None => total * NOMINAL_TEMPORAL_SHARE[tid],
                };
                below = rate;
                steps[len] = (Some(tid as u8), rate.min(total));
                len += 1;
            }
        }
        steps[len] = (None, total);
        Self {
            steps,
            len: len + 1,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = (Option<u8>, f64)> + '_ {
        self.steps[..self.len].iter().copied()
    }

    /// How many steps there are (at least one).
    pub fn count(&self) -> usize {
        self.len
    }

    /// Bits per second of the whole layer, the last step.
    pub fn whole(&self) -> f64 {
        self.steps[self.len - 1].1
    }
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
            keyframe_bits[layer as usize] = rates
                .keyframe_bits
                .unwrap_or(nominal as f64 * NOMINAL_KEYFRAME_SECONDS);
            rungs.extend(
                Steps::of(rates, nominal)
                    .iter()
                    .map(|(tid, rate)| (Rung { layer, tid }, rate)),
            );
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

    /// Bits per second `rung` costs, if the sharer offers it.
    pub fn rate_of(&self, rung: Rung) -> Option<f64> {
        self.position(rung).map(|i| self.rate(i))
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

    /// Whether rung `i` may be sent under `ceiling`: the lowest rung always may.
    fn allowed(&self, i: usize, ceiling: Option<f64>) -> bool {
        i == 0 || ceiling.is_none_or(|c| self.rate(i) <= c)
    }

    /// The highest rung at or below `i` that may be sent under `ceiling`.
    fn clamp(&self, mut i: usize, ceiling: Option<f64>) -> usize {
        while !self.allowed(i, ceiling) {
            i -= 1;
        }
        i
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
    /// Bits per second the narrowest link of the path carries: the ceiling a rung must fit.
    pub capacity: Option<f64>,
    /// Bits per second the path was seen to carry at least (see `bwe::EstimateSnapshot::link`):
    /// trusted at once, and room for a burst of loss to be a blip.
    pub link: Option<f64>,
    /// The share of what was sent over the last seconds that arrived.
    pub delivery: Option<f64>,
    /// Bits per second of the other video the receiver is sent (the cameras, for a screen; the
    /// screen, for the cameras): it shares the path.
    pub other: f64,
    /// Bits per second the receiver's budget lets the screen share cost, if it has one: no rung
    /// above it is sent, tried, or blamed on the path (the lowest rung is sent whatever it
    /// costs), whatever the receiver pinned, and a rung is taken up only where it costs at most
    /// [`BUDGET_RAISE_SHARE`] of it.
    pub ceiling: Option<f64>,
}

impl ScreenInputs {
    /// Whether the path may be carrying less than it is sent: it queues it, or drops it. Whether
    /// it is, `ScreenLevel` judges from how long it lasts.
    pub fn congested(&self) -> bool {
        self.queue.is_some_and(|q| q >= QUEUE_HIGH) || self.dropping()
    }

    pub(crate) fn dropping(&self) -> bool {
        self.delivery.is_some_and(|d| d < CONGESTED_DELIVERY)
    }

    pub(crate) fn clear(&self) -> bool {
        self.empty() && self.delivery.is_none_or(|d| d >= RAISE_DELIVERY)
    }

    /// Whether nothing stands queued on the path.
    pub(crate) fn empty(&self) -> bool {
        self.queue.is_some_and(|q| q < QUEUE_LOW)
    }

    /// Whether the path is emptying its queue while it delivers what it is sent: a link that
    /// flaps between wide and narrow never holds [`QUEUE_LOW`] for [`CLEAR_FOR`], but each wide
    /// half is time the path carries more than it is given.
    pub(crate) fn draining(&self) -> bool {
        self.queue_trend.is_some_and(|t| t <= QUEUE_SHRINKING)
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
    /// Until when the queue the large layer's keyframe sets is expected (a keyframe is on its way
    /// to the receiver), or a temporal layer just added is on trial.
    keyframe_grace: Option<Instant>,
    /// When the keyframe whose queue is explained (see [`ScreenLevel::keyframe_sent`]) was sent.
    keyframe: Option<Instant>,
    /// Whether the last reading was a queue growing as fast as [`QUEUE_SURGING`] says: one
    /// reading is a burst (a keyframe's first packets), two in a row are a path fed too much.
    surging: bool,
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
            keyframe: None,
            surging: false,
            trial_until: None,
            calm_since: None,
            backoff: Vec::new(),
        }
    }

    /// Whether the path carries less than the receiver is sent (see [`QUEUE_HIGH`]). `room`: the
    /// narrowest link is known to carry far more than is sent (see [`LOSS_ROOM`]), so loss alone,
    /// with nothing queued, is not congestion. `explained`: seconds of the queue that a keyframe
    /// the path carries accounts for (see [`ScreenLevel::explained`]); the rest is judged.
    fn congested(
        &mut self,
        now: Instant,
        inputs: &ScreenInputs,
        room: bool,
        explained: f64,
    ) -> bool {
        let trend = inputs.queue_trend.unwrap_or(0.0);
        if self.trial_until.is_some_and(|t| now < t)
            && inputs.queue.is_some_and(|q| q >= TRIAL_QUEUE)
            && trend > 0.0
        {
            return true;
        }
        let Some(q) = inputs.queue.filter(|&q| q - explained >= QUEUE_HIGH) else {
            self.queued_since = None;
            self.surging = false;
            return inputs.dropping() && !room;
        };
        let since = *self.queued_since.get_or_insert(now);
        let surge = trend >= QUEUE_SURGING && self.keyframe_grace.is_none_or(|t| now >= t);
        let surging = surge && self.surging;
        self.surging = surge;
        inputs.dropping()
            || q >= QUEUE_CRITICAL
            || surging
            || (now.saturating_duration_since(since) >= QUEUE_PERSISTS && trend >= QUEUE_SHRINKING)
    }

    /// A keyframe is on its way to the receiver (it, or another receiver of the stream, asked for
    /// one; or a stream of it was moved or started): it comes in a burst, and the queue it sets is
    /// not congestion.
    pub fn expect_keyframe(&mut self, now: Instant) {
        let until = now + KEYFRAME_GRACE;
        self.keyframe_grace = Some(self.keyframe_grace.map_or(until, |t| t.max(until)));
    }

    /// A keyframe of the screen was sent to the receiver. Where the path carries the rung it is
    /// sent on, the queue it leaves is not congestion (see [`ScreenLevel::explained`]): it earns
    /// that only once, a second one inside the time the first is allowed for earns nothing.
    pub fn keyframe_sent(&mut self, now: Instant) {
        let window = Duration::from_secs_f64(KEYFRAME_DRAIN) + KEYFRAME_LAG;
        if self
            .keyframe
            .is_none_or(|t| now.saturating_duration_since(t) >= window)
        {
            self.keyframe = Some(now);
        }
    }

    /// Seconds of the queue the last keyframe accounts for on rung `current`: nothing unless the
    /// path's narrowest link is known to carry the rung (and what else is sent). The keyframe
    /// takes `K / C` seconds to cross, and is not looked for in the first [`KEYFRAME_LAG`]; what
    /// it leaves drains at what the link carries beyond the load, so the allowance shrinks to
    /// nothing over that time, at most [`KEYFRAME_DRAIN`]. A queue standing past it is not the
    /// keyframe's.
    fn explained(
        &self,
        now: Instant,
        inputs: &ScreenInputs,
        ladder: &Ladder,
        current: usize,
    ) -> f64 {
        let (Some(capacity), Some(sent)) = (inputs.capacity, self.keyframe) else {
            return 0.0;
        };
        let spare = capacity - (ladder.rate(current) + inputs.other);
        let keyframe = ladder.keyframe_bits[ladder.rungs[current].0.layer as usize];
        if spare <= 0.0 || keyframe <= 0.0 {
            return 0.0;
        }
        let drain = (keyframe / spare).min(KEYFRAME_DRAIN);
        let late = now
            .saturating_duration_since(sent)
            .saturating_sub(KEYFRAME_LAG)
            .as_secs_f64();
        (keyframe / capacity * (1.0 - late / drain)).max(0.0)
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

    /// Lets the waits of the rungs that brought congestion shrink by `calm`, and forgets those
    /// that have run out and may be tried.
    fn decay_backoff(&mut self, now: Instant, calm: Duration) {
        let shrink = calm.mul_f64(BACKOFF_DECAY);
        for entry in &mut self.backoff {
            entry.2 = entry.2.saturating_sub(shrink);
        }
        self.backoff
            .retain(|&(_, at, wait)| !wait.is_zero() || now < at);
    }

    fn failed(&mut self, rung: Rung, now: Instant) {
        match self.backoff.iter_mut().find(|(r, ..)| *r == rung) {
            Some(entry) => {
                entry.2 = (entry.2 * 2).clamp(INITIAL_BACKOFF, MAX_BACKOFF);
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
                let i = ladder.clamp(i, inputs.ceiling);
                self.budget = self.budget.max(ladder.rate(i));
                i
            }
        };
        let mut target = current;
        let room = inputs.link.is_some_and(|c| {
            c * CAPACITY_SHARE >= (ladder.rate(current) + inputs.other) * LOSS_ROOM
        });
        let explained = self.explained(now, &inputs, ladder, current);
        if self.congested(now, &inputs, room, explained) {
            let carried = inputs.acked.map(|a| a - inputs.other);
            let share = HEADROOM
                .min(1.0 - inputs.queue.unwrap_or(0.0) / DRAIN_SECONDS)
                .max(LEAST_SHARE);
            let cut = carried
                .map_or(self.budget * HEADROOM, |c| c * share)
                .max(0.0);
            self.budget = self.budget.min(cut);
            // A path that delivered all of what the rung sends has shown only that it carries that
            // much: cutting to what it delivered costs a rung however little was wrong, and the
            // next verdict another. One rung down is room for what queued to drain, if it is a
            // step down at all (the rungs are not in order of rate across the layers: the base of
            // the large layer can cost more than the whole small one) and the path carried it.
            let delivered = inputs.delivery.is_some_and(|d| d >= DELIVERED_ALL);
            if delivered
                && current > 0
                && let Some(c) = carried.filter(|&c| c >= ladder.rate(current) * SENT_RUNG)
            {
                let below = ladder.rate(current - 1);
                if below < ladder.rate(current) && below <= c * HEADROOM {
                    self.budget = self.budget.max(below);
                }
            }
            self.hold_until = Some(now + HOLD);
            self.clear_since = None;
            self.calm_since = None;
            while target > 0 && ladder.rate(target) > self.budget {
                target -= 1;
            }
            // A rung the budget holds the receiver off is not one the path failed at.
            if target < current && ladder.allowed(current, inputs.ceiling) {
                self.failed(ladder.rungs[current].0, now);
            }
        } else if inputs.clear() {
            self.decay_backoff(now, elapsed);
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
                    .link
                    .map_or(0.0, |c| c * CAPACITY_SHARE - inputs.other);
                self.budget = grown.min((GROWTH_CEILING * seen).max(above)).max(link);
            }
            let next = current + 1;
            if next < ladder.rungs.len()
                && held
                && now.saturating_duration_since(since) >= CLEAR_FOR
                && self.budget >= ladder.rate(next)
                && inputs
                    .ceiling
                    .is_none_or(|c| ladder.rate(next) <= c * BUDGET_RAISE_SHARE)
                && ladder.fits(current, next, &inputs)
                && self.may_try(ladder.rungs[next].0, now)
            {
                target = next;
            }
        } else {
            self.decay_backoff(now, elapsed);
            self.clear_since = None;
        }
        let target = ladder.clamp(target, inputs.ceiling);
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

/// A source whose base temporal layer brings a frame no more often than this is a still picture
/// with a few changes (an idle share sends about a frame a second, a quarter of them in the base
/// layer): a receiver sent only those waits past what a browser's decoder tolerates before it asks
/// for a keyframe, so at least the second layer is sent, which costs almost nothing at that pace.
pub const SLOW_BASE_FRAMES: Duration = Duration::from_secs(1);

/// The highest temporal layer to send for a rung's `limit` while the source's base layer brings a
/// frame every `base_gap` (see [`SLOW_BASE_FRAMES`]).
pub fn limit_for_pace(limit: Option<u8>, base_gap: Option<Duration>) -> Option<u8> {
    match limit {
        Some(0) if base_gap.is_some_and(|g| g > SLOW_BASE_FRAMES) => Some(1),
        _ => limit,
    }
}

/// How long the large layer of a camera may be silent, while the small one flows, before receivers
/// are moved to the small one: a browser drops its top layer when its upload is short, and until
/// the move whoever watched the large layer sees a frozen picture. A camera sends a frame every 40
/// to 70 ms (an encoder short of CPU or upload skips a few), so this long without one is a layer
/// that stopped, not one that slowed.
pub const STALL_AFTER: Duration = Duration::from_millis(600);
/// The same for a screen share, whose large layer is sent the picture's changes and nothing else
/// (a browser told to favour detail drops frames before it blurs them): a few frames a second
/// while something moves and one every second or two while it hardly does is a layer that flows.
pub const SCREEN_STALL_AFTER: Duration = Duration::from_secs(3);
/// How long a large layer that stalled must flow again before receivers go back to it: a browser
/// at the edge of its upload turns the layer on and off, and every move costs a keyframe.
pub const RESUME_AFTER: Duration = Duration::from_secs(2);
/// How often a session's video is looked after (see `housekeeping`).
pub const WATCH_EVERY: Duration = Duration::from_secs(1);

/// Tracks whether the large layer of one camera is paused.
#[derive(Debug, Default)]
pub struct StallWatch {
    stalled: bool,
    /// Since when the large layer of a stalled camera flows again.
    flowing_since: Option<Instant>,
    /// The small layer's last packet as last seen, and since when it has flowed without a pause:
    /// a camera that starts again (both layers paused) brings a packet of one layer a moment
    /// before one of the other, which is not the large layer stopping.
    low_last: Option<Instant>,
    low_since: Option<Instant>,
}

impl StallWatch {
    pub fn stalled(&self) -> bool {
        self.stalled
    }

    /// Updates the state from when each layer last brought a packet; a layer silent for `after`
    /// (see [`STALL_AFTER`]) has stopped. Returns whether it changed. When neither layer flows the
    /// camera is off, and nothing changes; a camera that never sent a large layer has none to
    /// stall.
    pub fn update(
        &mut self,
        now: Instant,
        high: Option<Instant>,
        low: Option<Instant>,
        after: Duration,
    ) -> bool {
        let idle =
            |t: Option<Instant>| t.map_or(Duration::MAX, |t| now.saturating_duration_since(t));
        let high_flows = idle(high) < after;
        if low != self.low_last {
            let resumed = match (self.low_last, low) {
                (Some(before), Some(at)) => at.saturating_duration_since(before) >= after,
                _ => true,
            };
            if resumed {
                self.low_since = low;
            }
            self.low_last = low;
        }
        let low_flowed = idle(low) < after && idle(self.low_since) >= after;
        let stalled = if !self.stalled {
            high.is_some() && !high_flows && low_flowed
        } else if high_flows {
            let since = *self.flowing_since.get_or_insert(now);
            now.saturating_duration_since(since) < RESUME_AFTER
        } else {
            true
        };
        if !stalled || !high_flows {
            self.flowing_since = None;
        }
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
            link: None,
            delivery: Some(1.0),
            other: 0.0,
            ceiling: None,
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
                (rung(Layer::High, Some(0)), 1_600_000),
                (rung(Layer::High, Some(1)), 2_400_000),
                (rung(Layer::High, None), 4_000_000),
            ]
        );
    }

    #[test]
    fn the_large_layer_is_tried_only_where_its_keyframe_would_drain_within_seconds() {
        let l = ladder();
        let small = l.start();
        let link = |capacity| ScreenInputs {
            capacity: Some(capacity),
            link: Some(capacity),
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
            link: Some(capacity),
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
    fn a_verdict_on_a_path_that_delivered_all_it_was_sent_costs_one_rung() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        assert_eq!(s.rung(), Some(rung(Layer::High, None)));
        // A second of queue, growing, with the whole 2.5 Mbit/s acked (the acked second covers
        // less than the rung's rate does): cut to what was acked less the queue's share, that is
        // two rungs; the path lost nothing, so it is one.
        let (_, then) = twice(&mut s, &mut t, queued(1.0, 2_100_000.0));
        assert_eq!(then, rung(Layer::High, Some(1)));
        // A path that lost some of it is cut to what it delivered.
        let mut lossy = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut lossy, &mut t, 120, clear(4_000_000.0));
        let policed = ScreenInputs {
            delivery: Some(0.9),
            ..queued(1.0, 2_100_000.0)
        };
        assert_eq!(
            twice(&mut lossy, &mut t, policed).1,
            rung(Layer::High, Some(0))
        );
    }

    #[test]
    fn a_rung_whose_neighbour_below_costs_as_much_is_cut_to_what_the_path_delivered() {
        // The base of the large layer costs less than the whole small one here, so the rung
        // below the base is no step down at all.
        let small = LayerRates {
            total: Some(500_000),
            temporal: Some([Some(200_000), Some(100_000)]),
            keyframe_bits: Some(200_000.0),
        };
        let large = LayerRates {
            total: Some(1_000_000),
            temporal: Some([Some(400_000), Some(200_000)]),
            keyframe_bits: Some(400_000.0),
        };
        let odd = Ladder::new(Some(small), Some(large));
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        s.decide(t, clear(500_000.0), &odd);
        while s.rung() != Some(rung(Layer::High, Some(0))) {
            t += Duration::from_secs(1);
            s.decide(t, clear(4_000_000.0), &odd);
        }
        t += Duration::from_secs(1);
        s.decide(t, queued(0.3, 400_000.0), &odd);
        t += Duration::from_secs(1);
        assert_eq!(
            s.decide(t, queued(0.3, 400_000.0), &odd),
            rung(Layer::Low, Some(1)),
            "400 kbit/s acked, less the headroom: below the small layer's 500"
        );
    }

    #[test]
    fn a_receiver_whose_large_layer_stopped_restarts_on_the_small_one_and_earns_the_large_again() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        assert_eq!(s.rung(), Some(rung(Layer::High, None)));
        // The sharer's large layer stopped: it is no longer offered.
        let layer = LayerRates {
            total: Some(500_000),
            temporal: Some([Some(200_000), Some(100_000)]),
            keyframe_bits: Some(200_000.0),
        };
        let small_only = Ladder::new(Some(layer), None);
        t += Duration::from_secs(1);
        assert_eq!(
            s.decide(t, clear(500_000.0), &small_only),
            rung(Layer::Low, None)
        );
        for _ in 0..10 {
            t += Duration::from_secs(1);
            assert_eq!(
                s.decide(t, clear(4_000_000.0), &small_only),
                rung(Layer::Low, None)
            );
        }
        // It flows again: the receiver climbs back one rung at a time, not onto the top at once.
        t += Duration::from_secs(1);
        let first = s.decide(t, clear(4_000_000.0), &ladder());
        assert!(
            first == rung(Layer::Low, None) || first == rung(Layer::High, Some(0)),
            "{first:?}"
        );
        hold(&mut s, &mut t, 60, clear(4_000_000.0));
        assert_eq!(s.rung(), Some(rung(Layer::High, None)));
    }

    #[test]
    fn a_blip_every_half_minute_does_not_park_the_receiver_below_what_its_path_carries() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        let top = rung(Layer::High, None);
        for _ in 0..6 {
            assert_eq!(s.rung(), Some(top), "back on the top rung before the next");
            twice(&mut s, &mut t, queued(0.3, 2_500_000.0));
            assert_ne!(s.rung(), Some(top));
            hold(&mut s, &mut t, 25, clear(2_500_000.0));
        }
        assert!(
            s.backoff.iter().all(|b| b.2 <= INITIAL_BACKOFF),
            "{:?}",
            s.backoff
        );
    }

    #[test]
    fn calm_time_on_any_rung_shortens_the_wait_of_a_rung_that_failed() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        twice(&mut s, &mut t, queued(0.25, 2_000_000.0));
        assert!(!s.backoff.is_empty());
        // 20 calm seconds at a quarter a second take 5 s off the 10 s wait, whichever rung they
        // were spent on (the one that failed was left a minute short of being forgiven).
        hold(&mut s, &mut t, 20, clear(1_000_000.0));
        assert!(
            s.backoff.iter().all(|b| b.2 <= INITIAL_BACKOFF / 2),
            "{:?}",
            s.backoff
        );
    }

    #[test]
    fn one_reading_of_a_growing_queue_is_a_burst_two_are_a_path_fed_too_much() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        let surging = ScreenInputs {
            queue_trend: Some(0.25),
            ..queued(0.3, 1_700_000.0)
        };
        t += Duration::from_secs(1);
        assert_eq!(s.decide(t, surging, &ladder()), rung(Layer::High, None));
        t += Duration::from_millis(250);
        assert_eq!(
            s.decide(t, surging, &ladder()),
            rung(Layer::High, Some(0)),
            "the second reading in a row"
        );
        // A reading that is not surging between them starts the count again.
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        t += Duration::from_secs(1);
        s.decide(t, surging, &ladder());
        t += Duration::from_millis(250);
        s.decide(t, queued(0.3, 1_700_000.0), &ladder());
        t += Duration::from_millis(250);
        assert_eq!(s.decide(t, surging, &ladder()), rung(Layer::High, None));
    }

    #[test]
    fn a_keyframe_on_its_way_to_the_receiver_explains_the_queue_that_surges() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        let surging = ScreenInputs {
            queue_trend: Some(0.25),
            ..queued(0.3, 1_700_000.0)
        };
        // Another viewer's request, or a join, brings the large layer's keyframe to this one too.
        t += Duration::from_secs(1);
        s.expect_keyframe(t);
        for _ in 0..4 {
            t += Duration::from_millis(250);
            assert_eq!(s.decide(t, surging, &ladder()), rung(Layer::High, None));
        }
        // A second request does not shorten what the first started.
        t += Duration::from_millis(250);
        s.expect_keyframe(t - Duration::from_secs(1));
        t += Duration::from_secs(2);
        s.decide(t, surging, &ladder());
        t += Duration::from_millis(250);
        assert_eq!(
            s.decide(t, surging, &ladder()),
            rung(Layer::High, Some(0)),
            "once the keyframe has gone in, a queue that surges is congestion"
        );
    }

    #[test]
    fn a_base_layer_that_is_slow_is_sent_with_the_second_layer() {
        let gap = |ms| Some(Duration::from_millis(ms));
        assert_eq!(limit_for_pace(Some(0), gap(4_000)), Some(1));
        assert_eq!(limit_for_pace(Some(0), gap(1_001)), Some(1));
        assert_eq!(limit_for_pace(Some(0), gap(133)), Some(0), "a busy screen");
        assert_eq!(limit_for_pace(Some(0), None), Some(0), "not measured yet");
        assert_eq!(limit_for_pace(Some(1), gap(4_000)), Some(1));
        assert_eq!(limit_for_pace(None, gap(4_000)), None);
    }

    #[test]
    fn what_the_path_was_seen_to_carry_trusts_at_once_and_the_ceiling_is_the_links_rate() {
        // Bursts that left spread out arrived at the pace they left at: the link carries at
        // least that, which is room for a burst of loss to be a blip...
        let paced = ScreenInputs {
            capacity: None,
            link: Some(20_000_000.0),
            delivery: Some(0.7),
            ..clear(2_500_000.0)
        };
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        t += Duration::from_secs(1);
        assert_eq!(s.decide(t, paced, &ladder()), rung(Layer::High, None));
        // ...and trust for the next rung at once, while the ceiling a rung must fit is the
        // narrowest link's, not what was seen.
        let narrow = ScreenInputs {
            capacity: Some(1_000_000.0),
            link: Some(20_000_000.0),
            ..clear(1_000_000.0)
        };
        let l = ladder();
        assert!(!l.fits(2, 3, &narrow), "1 Mbit/s of rung does not fit 0.85");
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        s.decide(t, clear(500_000.0), &l);
        let wide = ScreenInputs {
            capacity: None,
            ..narrow
        };
        for _ in 0..3 {
            t += Duration::from_secs(1);
            s.decide(t, wide, &l);
        }
        assert_eq!(s.rung(), Some(rung(Layer::High, Some(0))));
    }

    #[test]
    fn rids_map_to_layers() {
        assert_eq!(Layer::from_rid(Some("h")), Layer::High);
        assert_eq!(Layer::from_rid(Some("l")), Layer::Low);
        assert_eq!(Layer::from_rid(None), Layer::Low);
        assert_eq!(Layer::High.other(), Layer::Low);
    }

    #[test]
    fn the_large_camera_layer_is_stalled_after_600_silent_ms_while_the_small_flows() {
        let t = Instant::now();
        let mut w = StallWatch::default();
        let at = |ms| Some(t + Duration::from_millis(ms));
        let now = |ms| t + Duration::from_millis(ms);
        for ms in (0..3000).step_by(50) {
            assert!(!w.update(now(ms), at(ms), at(ms), STALL_AFTER), "flowing");
        }
        assert!(
            !w.update(now(3000), at(2960), at(2950), STALL_AFTER),
            "flowing"
        );
        assert!(
            !w.update(now(3000), at(2500), at(2950), STALL_AFTER),
            "silent for 500 ms: a slow frame, not yet"
        );
        assert!(
            w.update(now(3100), at(2500), at(3080), STALL_AFTER),
            "silent for 600 ms"
        );
        assert!(w.stalled());
        assert!(
            !w.update(now(4000), at(2500), at(3990), STALL_AFTER),
            "still stalled, no change"
        );
    }

    #[test]
    fn a_large_screen_layer_that_sends_a_frame_a_second_is_not_stalled() {
        let t = Instant::now();
        let (now, at) = (
            |ms| t + Duration::from_millis(ms),
            |ms| t + Duration::from_millis(ms),
        );
        let mut w = StallWatch::default();
        // The small layer at 30 frames a second, the large one a frame every 1.2 s: a camera's
        // threshold would call that stopped at every gap, and cost a keyframe at every return.
        for ms in (0..10_000).step_by(33) {
            let large = at(ms / 1200 * 1200);
            assert!(
                !w.update(now(ms), Some(large), Some(at(ms)), SCREEN_STALL_AFTER),
                "{ms}"
            );
        }
        let mut c = StallWatch::default();
        let stalls = (0..10_000)
            .step_by(33)
            .filter(|&ms| {
                c.update(
                    now(ms),
                    Some(at(ms / 1200 * 1200)),
                    Some(at(ms)),
                    STALL_AFTER,
                )
            })
            .count();
        assert!(stalls > 0, "a camera's threshold stalls it");
        // Silent for 3 s while the small layer flows, it has stopped.
        assert!(!w.update(
            now(12_000),
            Some(at(9_600)),
            Some(at(12_000)),
            SCREEN_STALL_AFTER
        ));
        assert!(w.update(
            now(13_000),
            Some(at(9_600)),
            Some(at(13_000)),
            SCREEN_STALL_AFTER
        ));
    }

    #[test]
    fn a_stalled_large_layer_is_taken_back_once_it_flowed_two_seconds() {
        let t = Instant::now();
        let mut w = StallWatch::default();
        let at = |ms| Some(t + Duration::from_millis(ms));
        let now = |ms| t + Duration::from_millis(ms);
        for ms in (0..1000).step_by(50) {
            w.update(now(ms), at(ms), at(ms), STALL_AFTER);
        }
        for ms in (1000..1550).step_by(50) {
            assert!(!w.update(now(ms), at(950), at(ms), STALL_AFTER), "not yet");
        }
        assert!(
            w.update(now(1600), at(950), at(1590), STALL_AFTER),
            "stalled"
        );
        assert!(
            !w.update(now(5000), at(4990), at(4990), STALL_AFTER),
            "back, not for long"
        );
        assert!(
            !w.update(now(6500), at(6490), at(6490), STALL_AFTER),
            "1.5 s of it"
        );
        assert!(
            w.update(now(7000), at(6990), at(6990), STALL_AFTER),
            "2 s of it: resumed"
        );
        assert!(!w.stalled());
    }

    #[test]
    fn a_large_layer_that_flickers_stays_stalled() {
        let t = Instant::now();
        let mut w = StallWatch::default();
        let at = |ms| Some(t + Duration::from_millis(ms));
        let now = |ms| t + Duration::from_millis(ms);
        for ms in (0..1000).step_by(50) {
            w.update(now(ms), at(ms), at(ms), STALL_AFTER);
        }
        for ms in (1000..1550).step_by(50) {
            assert!(!w.update(now(ms), at(950), at(ms), STALL_AFTER), "not yet");
        }
        assert!(
            w.update(now(1600), at(950), at(1590), STALL_AFTER),
            "stalled"
        );
        assert!(
            !w.update(now(2000), at(1990), at(1990), STALL_AFTER),
            "back"
        );
        assert!(
            !w.update(now(3000), at(2200), at(2990), STALL_AFTER),
            "gone again"
        );
        assert!(
            !w.update(now(3500), at(3490), at(3490), STALL_AFTER),
            "back again"
        );
        assert!(
            !w.update(now(4800), at(4790), at(4790), STALL_AFTER),
            "1.3 s since it came back the second time"
        );
        assert!(
            w.update(now(5500), at(5490), at(5490), STALL_AFTER),
            "2 s of it"
        );
    }

    #[test]
    fn a_camera_starting_again_is_not_stalled_by_its_small_layer_arriving_first() {
        let t = Instant::now();
        let mut w = StallWatch::default();
        let at = |ms| Some(t + Duration::from_millis(ms));
        let now = |ms| t + Duration::from_millis(ms);
        for ms in (0..1000).step_by(50) {
            w.update(now(ms), at(ms), at(ms), STALL_AFTER);
        }
        // Both layers paused for five seconds; the small one's packet comes first.
        assert!(!w.update(now(6000), at(950), at(6000), STALL_AFTER));
        assert!(!w.update(now(6010), at(6010), at(6000), STALL_AFTER));
        assert!(!w.stalled());
    }

    #[test]
    fn a_camera_with_both_layers_silent_is_off_not_stalled() {
        let t = Instant::now();
        let mut w = StallWatch::default();
        assert!(!w.update(t + Duration::from_secs(60), Some(t), Some(t), STALL_AFTER));
        assert!(!w.stalled());
        assert!(!w.update(t, None, None, STALL_AFTER));
        assert!(
            !w.update(t, None, Some(t), STALL_AFTER),
            "a camera of one layer has no large one to stall"
        );
    }

    fn under(ceiling: f64, inputs: ScreenInputs) -> ScreenInputs {
        ScreenInputs {
            ceiling: Some(ceiling),
            ..inputs
        }
    }

    #[test]
    fn the_budget_is_split_so_that_the_cameras_give_up_their_large_layers_first() {
        // 2 Mbit/s: 1.9 of video. Cameras whole on their small layers cost 0.6; a screen share
        // sent at 1.0.
        let split = split_budget(2_000_000.0, 600_000.0, 1_000_000.0);
        assert_eq!(split.screen, 1_300_000.0, "what the small cameras leave");
        assert_eq!(split.cameras, 900_000.0, "what the screen share leaves");
        // The screen share is cut before the cameras are thinned: its ceiling never goes below
        // what is left of the cameras on their small layers, so the cameras' ceiling never goes
        // below the small layers while the screen share still has rungs to give up.
        let tight = split_budget(1_500_000.0, 600_000.0, 800_000.0);
        assert_eq!(tight.screen, 800_000.0);
        assert!(tight.cameras >= 600_000.0);
        // Nothing is left for the screen share beyond the cameras and the voices: it gets 0 and
        // stays on its lowest rung.
        let none = split_budget(1_000_000.0, 1_200_000.0, 200_000.0);
        assert_eq!(none.screen, 0.0);
        assert_eq!(none.cameras, 700_000.0);
        assert_eq!(
            split_budget(50_000.0, 0.0, 0.0).cameras,
            0.0,
            "under the reserve"
        );
    }

    #[test]
    fn the_hold_lets_nothing_up_for_ten_seconds_after_a_cut() {
        let t = Instant::now();
        let mut h = BudgetHold::default();
        assert!(h.may_raise(t), "nothing was ever cut");
        h.cut(t);
        assert!(!h.may_raise(t + Duration::from_secs(9)));
        assert!(h.may_raise(t + BUDGET_HOLD));
        h.cut(t + Duration::from_secs(9));
        assert!(
            !h.may_raise(t + Duration::from_secs(12)),
            "a cut starts it again"
        );
    }

    #[test]
    fn the_ceiling_clamps_the_rung_and_is_not_blamed_on_the_path() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        assert_eq!(s.rung(), Some(rung(Layer::High, None)));
        // 1.2 Mbit/s: the base of the large layer (1 Mbit/s) is the highest that fits.
        let rung_under = |s: &mut ScreenLevel, t: Instant, ceiling| {
            s.decide(t, under(ceiling, clear(4_000_000.0)), &ladder())
        };
        t += Duration::from_secs(1);
        assert_eq!(
            rung_under(&mut s, t, 1_200_000.0),
            rung(Layer::High, Some(0))
        );
        t += Duration::from_secs(1);
        assert_eq!(rung_under(&mut s, t, 450_000.0), rung(Layer::Low, Some(1)));
        assert!(
            s.backoff.is_empty(),
            "the path did not fail: {:?}",
            s.backoff
        );
    }

    #[test]
    fn the_lowest_rung_is_sent_whatever_the_ceiling_says() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, clear(4_000_000.0));
        t += Duration::from_secs(1);
        assert_eq!(
            s.decide(t, under(0.0, clear(4_000_000.0)), &ladder()),
            rung(Layer::Low, Some(0)),
            "the screen share is never paused"
        );
    }

    #[test]
    fn the_ceiling_beats_the_pin() {
        let mut s = ScreenLevel::new();
        let t = Instant::now();
        s.pin_high(true);
        assert_eq!(
            s.decide(t, under(600_000.0, clear(500_000.0)), &ladder()),
            rung(Layer::Low, None)
        );
    }

    #[test]
    fn a_rung_is_taken_up_only_within_nine_tenths_of_the_ceiling() {
        // The base of the large layer costs 1 Mbit/s: it needs a ceiling of 1.12 Mbit/s or more.
        let run = |ceiling| {
            let mut s = ScreenLevel::new();
            let mut t = Instant::now();
            hold(&mut s, &mut t, 120, under(ceiling, clear(4_000_000.0)));
            s.rung()
        };
        assert_eq!(
            run(1_050_000.0),
            Some(rung(Layer::Low, None)),
            "inside the band"
        );
        assert_eq!(run(1_200_000.0), Some(rung(Layer::High, Some(0))));
    }

    #[test]
    fn a_rung_inside_the_band_is_kept_not_cut() {
        let mut s = ScreenLevel::new();
        let mut t = Instant::now();
        hold(&mut s, &mut t, 120, under(1_200_000.0, clear(4_000_000.0)));
        assert_eq!(s.rung(), Some(rung(Layer::High, Some(0))));
        // The ceiling falls to 1.05 Mbit/s: the rung (1 Mbit/s) still fits it, though it would
        // not be taken up under it.
        assert_eq!(
            hold(&mut s, &mut t, 30, under(1_050_000.0, clear(4_000_000.0))),
            rung(Layer::High, Some(0))
        );
    }

    #[test]
    fn the_peak_is_the_highest_of_the_last_ten_seconds() {
        let t = Instant::now();
        let mut p = RecentPeak::default();
        let at = |s| t + Duration::from_secs(s);
        assert_eq!(p.record(at(0), 900.0), 900.0);
        assert_eq!(p.record(at(1), 100.0), 900.0);
        assert_eq!(
            p.record(at(10), 100.0),
            900.0,
            "ten seconds on, still in it"
        );
        assert_eq!(p.record(at(11), 200.0), 200.0, "and then it is gone");
    }

    /// A path of 1.458 Mbit/s, which carries the large layer of [`keyframe_ladder`] (1 Mbit/s).
    const SLOW_LINK: f64 = 1_458_000.0;

    /// A sharer whose large layer (1 Mbit/s, keyframes of 0.4 s of it) a 1.458 Mbit/s path carries.
    fn keyframe_ladder() -> Ladder {
        let layer = |total: u64| LayerRates {
            total: Some(total),
            temporal: Some([Some(total * 2 / 5), Some(total / 5)]),
            keyframe_bits: Some(total as f64 * 0.4),
        };
        Ladder::new(Some(layer(500_000)), Some(layer(1_000_000)))
    }

    /// `queue` seconds on the slow link, which carries the rung it is sent, with the trend given.
    fn slow(queue: f64, trend: f64) -> ScreenInputs {
        ScreenInputs {
            queue: Some(queue),
            queue_trend: Some(trend),
            capacity: Some(SLOW_LINK),
            ..clear(1_000_000.0)
        }
    }

    /// A receiver pinned to the large layer, a keyframe of which is sent at the returned time.
    fn keyframed(capacity: Option<f64>) -> (ScreenLevel, Instant) {
        let mut s = ScreenLevel::new();
        s.pin_high(true);
        let t = Instant::now();
        let inputs = ScreenInputs {
            capacity,
            ..slow(0.01, 0.0)
        };
        assert_eq!(
            s.decide(t, inputs, &keyframe_ladder()),
            rung(Layer::High, None)
        );
        s.keyframe_sent(t);
        (s, t)
    }

    fn after(t: Instant, ms: u64) -> Instant {
        t + Duration::from_millis(ms)
    }

    #[test]
    fn the_queue_of_a_keyframe_on_a_rung_the_link_carries_is_kept() {
        let (mut s, t) = keyframed(Some(SLOW_LINK));
        let l = keyframe_ladder();
        let top = rung(Layer::High, None);
        assert_eq!(s.decide(after(t, 1_000), slow(0.23, 0.05), &l), top);
        assert_eq!(s.decide(after(t, 2_000), slow(0.23, 0.0), &l), top);
        assert_eq!(s.decide(after(t, 3_000), slow(0.02, -0.1), &l), top);
    }

    #[test]
    fn a_queue_above_what_the_keyframe_explains_is_cut() {
        let (mut s, t) = keyframed(Some(SLOW_LINK));
        let l = keyframe_ladder();
        s.decide(after(t, 1_000), slow(0.9, 0.05), &l);
        assert_ne!(
            s.decide(after(t, 2_000), slow(0.9, 0.05), &l),
            rung(Layer::High, None)
        );
    }

    #[test]
    fn a_queue_standing_past_the_drain_is_cut() {
        let (mut s, t) = keyframed(Some(SLOW_LINK));
        let l = keyframe_ladder();
        s.decide(after(t, 3_500), slow(0.3, 0.0), &l);
        assert_ne!(
            s.decide(after(t, 4_500), slow(0.3, 0.0), &l),
            rung(Layer::High, None)
        );
    }

    #[test]
    fn a_second_keyframe_earns_no_allowance() {
        let (mut s, t) = keyframed(Some(SLOW_LINK));
        let l = keyframe_ladder();
        s.keyframe_sent(after(t, 1_000));
        s.decide(after(t, 2_000), slow(0.23, 0.0), &l);
        assert_ne!(
            s.decide(after(t, 3_000), slow(0.23, 0.0), &l),
            rung(Layer::High, None),
            "the queue the first explained has drained by now"
        );
    }

    #[test]
    fn a_keyframe_explains_nothing_where_the_link_is_unknown() {
        let (mut s, t) = keyframed(None);
        let l = keyframe_ladder();
        let unknown = |queue, trend| ScreenInputs {
            capacity: None,
            ..slow(queue, trend)
        };
        s.decide(after(t, 1_000), unknown(0.23, 0.05), &l);
        assert_ne!(
            s.decide(after(t, 2_000), unknown(0.23, 0.0), &l),
            rung(Layer::High, None)
        );
    }
}
