//! Holding the cameras a receiver is sent to what its path carries.
//!
//! A receiver's path that carries less than the cameras it is sent loses the excess at random, a
//! packet here and a packet there of every camera, and a frame that lost one is lost whole with
//! the frames after it up to the next keyframe: every picture breaks, where the same bits spent on
//! fewer pictures would have played. So when the path says it carries less (it queues what it is
//! sent, or drops it: see `ScreenInputs`), the cameras are given what it delivered less the screen
//! share's part, in the order of what matters: the host's camera first, then the others. A camera
//! that does not fit gives up what it can: it goes onto its small simulcast layer (from the
//! layer's next keyframe) when it sends one, or gives up temporal layers (the frames between, from
//! the next one: a camera that has none has no such step), and then is paused for this receiver,
//! until its keyframe at the next time it fits.
//!
//! What the path carries is read from what it delivered, not from the estimator's target: under a
//! hard limit that drops packets, the target falls to its floor (the estimator reads every loss
//! as a sign to send less than anything) and says nothing of what the path delivers.
//!
//! Taking a camera up again is where flapping would come from, so it is made hard to do by
//! accident: only while the path has stayed clear for a while, only to what fits with a margin
//! (an estimate that swings by a tenth does not bring a camera back at its top, nor take it away
//! again at its bottom), one camera step at a time, and a step that brought congestion is not
//! tried again for a wait that doubles each time.
//!
//! Two things keep that caution from holding a camera back for good. A camera paused for
//! [`MAX_PAUSE`] is tried at its lowest step, against what the narrowest link carries and not
//! against what the budget allows: a picture at the lowest rate is better than none. And what the
//! budget believes is forgotten once the path has stayed calm for a while, so that a limit a flapping
//! link set is not kept long after the link has changed.
//!
//! An outage is not a measurement. Both directions drop everything, and the first feedback after it
//! reports every packet sent meanwhile as lost: while the feedback has stopped (see
//! [`feedback_stopped`]), and for a few seconds after it comes back (see [`path_settling`]) the
//! cameras are left as they are, and the first feedback to follow says nothing of the path (see
//! `bwe`).

use std::time::{Duration, Instant};

use crate::bwe::OUTAGE_GAP;
use crate::layers::{
    CAPACITY_SHARE, CLEAR_FOR, DRAIN_SECONDS, FORGET_AFTER, GROWTH_CEILING, GROWTH_PER_SECOND,
    HEADROOM, HOLD, INITIAL_BACKOFF, KEYFRAME_DRAIN, KEYFRAME_GRACE, LEAST_SHARE, LOSS_ROOM, Layer,
    LayerRates, MAX_BACKOFF, NOMINAL_KEYFRAME_SECONDS, QUEUE_CRITICAL, QUEUE_HIGH, QUEUE_PERSISTS,
    QUEUE_SHRINKING, QUEUE_SURGING, Rung, ScreenInputs, Steps, TRIAL,
};

/// What a camera costs until it has been measured.
pub const NOMINAL_CAMERA_RATE: u64 = 500_000;
/// A camera is taken up a step only where it fits this many times over: more than the swing of an
/// estimate that moves a tenth either way.
const RAISE_MARGIN: f64 = 1.25;
/// After the cameras were cut to what the path delivered, the feedback still reports the packets
/// sent before the cut for about this long (the estimator looks back three seconds): it is not
/// read as congestion that calls for another cut, which would take the same share of an already
/// smaller rate again.
const CUT_SETTLES: Duration = Duration::from_millis(3500);
/// How long after the cameras were cut the narrowest link's rate (see `bwe`) is believed to be
/// what the cameras may reach. It reads low on a fast link (bursts of a few packets do not spread
/// over it) and is stale once the path has changed, so it stops holding a camera back after this,
/// and the next step is tried instead. A step beyond what the narrowest link carries fails, and
/// costs the receiver a burst of what the link drops: on a link that is narrow for good, this is
/// how often that happens.
const CEILING_TRUST: Duration = Duration::from_secs(60);
/// A step taken up that the path has carried this long without congestion has held: the wait that
/// grew with the steps that failed before it starts over.
const STEP_HELD: Duration = FORGET_AFTER;
/// A camera paused this long is tried at its lowest step wherever the narrowest link carries it
/// (see [`REACH_SHARE`]), whatever the budget says.
pub const MAX_PAUSE: Duration = Duration::from_secs(10);
/// A cut takes the cameras down to what the path delivered, but never below this share of what
/// was sent.
const MOST_CUT: f64 = 0.5;
/// ...with no margin, against this share of what the narrowest link carries: a tenth is left, so
/// that a link whose rate swings a tenth either way carries the camera in its narrow half, and a
/// camera that does not is not tried again and again.
const REACH_SHARE: f64 = 0.9;
/// A camera paused for [`MAX_PAUSE`] is tried with room left for the screen share's keyframes:
/// the screen is counted this many times over against what the narrowest link carries.
const SCREEN_KEYFRAME_ROOM: f64 = 1.2;

/// For this long after the feedback comes back from an outage, what the path says is not read: the
/// receiver asks for everything lost again (the node's whole retransmission history, for every
/// camera, at once) and for keyframes, which the path carries as a flood that has nothing to do
/// with what the cameras cost.
const OUTAGE_SETTLES: Duration = Duration::from_secs(4);

/// Whether the receiver's feedback has stopped (an outage: what the path said last is not what it
/// says now). A receiver that is sent nothing has no feedback to give either, which says nothing
/// of its path: only where something is sent does it mean an outage.
pub fn feedback_stopped(now: Instant, fed_at: Option<Instant>) -> bool {
    fed_at.is_some_and(|t| now.saturating_duration_since(t) > OUTAGE_GAP)
}

/// Whether the feedback has just come back from an outage (see [`OUTAGE_SETTLES`]): what the path
/// says then is not read.
pub fn path_settling(now: Instant, resumed_at: Option<Instant>) -> bool {
    resumed_at.is_some_and(|t| now.saturating_duration_since(t) < OUTAGE_SETTLES)
}

/// Who is given room first: the host's camera, then those of the voices heard now, then the
/// others the receiver watches, then those it asked for small.
pub const RANK_HOST: u8 = 0;
pub const RANK_LOUD: u8 = 1;
pub const RANK_WATCHED: u8 = 2;
pub const RANK_SMALL: u8 = 3;

/// The most steps a camera has: the three of its small layer and the whole of its large one.
const MAX_STEPS: usize = 4;

/// What a camera costs a receiver at each of its steps, cheapest first, and which layer and which
/// of its temporal layers each is.
///
/// A camera that sends both simulcast layers goes down to the small one before it gives up frames
/// of the large one: the steps are those of the small layer (its temporal layers, and the whole
/// of it) and then the whole of the large layer, which has no steps of its own, so that nothing
/// can thin the large layer before the small one is reached. When the small layer is not cheaper
/// than the large (it is no step down) the camera has the steps of the large layer alone.
#[derive(Debug, Clone, Copy)]
pub struct CameraSteps {
    steps: [(Rung, f64); MAX_STEPS],
    len: usize,
    /// Bits of the keyframe the receiver is sent when it moves from the small layer onto the large
    /// one: it arrives at once, on top of what flows.
    switch_bits: f64,
}

impl CameraSteps {
    /// The steps of a camera from what was measured of each layer it sends (`None` for a layer it
    /// does not send, or one that is not flowing now: a rate that is not measured is a guess that
    /// must not take a camera off its large layer). A camera with no layer at all costs what a
    /// camera not measured yet does.
    pub fn of(low: Option<LayerRates>, high: Option<LayerRates>) -> Self {
        let steps = |rates| Steps::of(rates, NOMINAL_CAMERA_RATE);
        let mut out = Self {
            steps: [(
                Rung {
                    layer: Layer::Low,
                    tid: None,
                },
                0.0,
            ); MAX_STEPS],
            len: 0,
            switch_bits: 0.0,
        };
        match (low.map(steps), high.map(steps)) {
            (Some(small), Some(large)) if small.whole() < large.whole() => {
                out.extend(Layer::Low, &small);
                out.push(Layer::High, None, large.whole());
                out.switch_bits = high
                    .and_then(|r| r.keyframe_bits)
                    .unwrap_or(large.whole() * NOMINAL_KEYFRAME_SECONDS);
            }
            (_, Some(large)) => out.extend(Layer::High, &large),
            (Some(small), None) => out.extend(Layer::Low, &small),
            (None, None) => out.extend(Layer::Low, &steps(LayerRates::default())),
        }
        out
    }

    fn push(&mut self, layer: Layer, tid: Option<u8>, rate: f64) {
        self.steps[self.len] = (Rung { layer, tid }, rate);
        self.len += 1;
    }

    fn extend(&mut self, layer: Layer, steps: &Steps) {
        for (tid, rate) in steps.iter() {
            self.push(layer, tid, rate);
        }
    }

    fn rungs(&self) -> &[(Rung, f64)] {
        &self.steps[..self.len]
    }

    /// How many steps there are (at least one).
    pub fn count(&self) -> usize {
        self.len
    }

    /// Bits per second the cheapest step costs: what the camera is never held below, if it is
    /// the host's.
    pub fn lowest(&self) -> f64 {
        self.steps[0].1
    }

    /// Whether the large layer is among the steps.
    fn reaches_high(&self) -> bool {
        self.rungs().iter().any(|(r, _)| r.layer == Layer::High)
    }

    /// The steps of the small layer alone, for a receiver that is held to it anyway (a camera
    /// that has no small layer keeps what it has).
    pub fn small_only(mut self) -> Self {
        let small = self
            .rungs()
            .iter()
            .take_while(|(r, _)| r.layer == Layer::Low)
            .count();
        if small > 0 {
            self.len = small;
        }
        self
    }
}

impl Default for CameraSteps {
    fn default() -> Self {
        Self::of(None, None)
    }
}

/// One camera a receiver is sent, and what it can be sent at.
#[derive(Debug, Clone, Copy)]
pub struct Camera {
    /// What comes first when there is not room for all (`RANK_*`, the lowest first).
    pub rank: u8,
    pub steps: CameraSteps,
    /// What the receiver is sent now: 0 is nothing (paused), `n` is the `n`th of `steps`.
    pub level: usize,
    /// The layer and temporal layer the receiver is sent now.
    at: Rung,
    /// For how long the camera has been paused (nothing when it is not).
    pub paused_for: Duration,
}

impl Camera {
    /// A camera as the receiver is sent it: paused (`Some`, for how long), or at the step that is
    /// `current` (the top one when `steps` has none like it: the cameras are only ever taken down
    /// from it).
    pub fn new(rank: u8, steps: CameraSteps, paused: Option<Duration>, current: Rung) -> Self {
        let level = if paused.is_some() {
            0
        } else {
            steps
                .rungs()
                .iter()
                .position(|(rung, _)| *rung == current)
                .map_or(steps.count(), |i| i + 1)
        };
        Self {
            rank,
            steps,
            level,
            at: current,
            paused_for: paused.unwrap_or_default(),
        }
    }

    /// Whether the camera is sent at a rung that is not the one of its level: it was at none of
    /// the steps (the large layer thinned while only that flowed, and the small one flows now) and
    /// was counted at the top, so it has to be moved to it for the receiver to be sent what the
    /// budget counts.
    pub fn off_step(&self) -> bool {
        self.level > 0 && self.rung(self.level) != self.at
    }

    /// Whether the camera has been paused for [`MAX_PAUSE`].
    fn long_paused(&self) -> bool {
        self.level == 0 && self.paused_for >= MAX_PAUSE
    }

    /// The layer and the highest temporal layer sent at `level` (not 0).
    pub fn rung(&self, level: usize) -> Rung {
        self.steps.rungs()[level.saturating_sub(1)].0
    }

    /// Whether the camera is held on its small layer at `level` while it sends a large one, for
    /// want of room: the receiver is kept on the small layer until the budget takes it up.
    pub fn held_small(&self, level: usize) -> bool {
        level > 0 && self.rung(level).layer == Layer::Low && self.steps.reaches_high()
    }

    /// Bits per second `level` costs.
    fn cost(&self, level: usize) -> f64 {
        level
            .checked_sub(1)
            .map_or(0.0, |i| self.steps.rungs()[i].1)
    }

    /// Bits of the keyframe that taking the camera from `before` up to `level` sends at once: the
    /// one of the large layer, when it is a move onto it.
    fn switch_cost(&self, before: usize, level: usize) -> f64 {
        if before > 0 && self.rung(before).layer != self.rung(level).layer {
            self.steps.switch_bits
        } else {
            0.0
        }
    }

    fn top(&self) -> usize {
        self.steps.count()
    }
}

/// What a set of cameras costs.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Load {
    /// Bits per second sent as they are.
    pub sent: f64,
    /// ...and if every one were sent whole.
    pub wanted: f64,
    /// The least the next step up of any of them costs more than it does now (0 when none can go
    /// up).
    pub next: f64,
    /// The least the host's camera costs: what the cameras are never held below.
    pub floor: f64,
}

impl Load {
    pub fn of(cameras: &[Camera]) -> Self {
        let mut load = Self::default();
        for c in cameras {
            load.sent += c.cost(c.level);
            load.wanted += c.cost(c.top());
            if c.rank == RANK_HOST {
                load.floor += c.cost(1);
            }
            if c.level < c.top() {
                let step = c.cost(c.level + 1) - c.cost(c.level);
                load.next = if load.next == 0.0 {
                    step
                } else {
                    load.next.min(step)
                };
            }
        }
        load
    }
}

/// Gives each camera the most that fits in `limit` bits per second (`None`: all of it), the host's
/// first and then the others by rank and in the order they come in, but no camera a step beyond
/// its lowest at the price of pausing one that flows: the cameras that fit at their lowest steps
/// are given those first, and what is left goes to the steps above them in the same order. Of the
/// cameras that are not at the top,
/// one at most (the first in that order) is taken up, and only when `raise` allows it (`Some`:
/// the most the cameras may cost then) and everything sent now and the step fit [`RAISE_MARGIN`]
/// times over in `limit`: rank says who goes up first and who is cut first, never who takes the
/// place of a camera that flows. A step onto the large layer also needs its keyframe to drain
/// within [`KEYFRAME_DRAIN`] in what is left of the most the cameras may cost (everything, while
/// that is not known). One that no longer fits goes down at once.
///
/// The cameras that have been paused for [`MAX_PAUSE`] take that one step before the others, up to
/// their lowest, whatever `limit` says and the path having stayed clear or not, when `trial` allows
/// it (`Some`: the most the cameras may cost, what the narrowest link carries, or infinite while
/// that is not known, when it must fit [`RAISE_MARGIN`] times over in `limit` too), as many as fit
/// in it with everything sent: they flow at the lowest rate they have instead of waiting for a path
/// that a link which flaps never leaves clear, and a step up after it is taken by none of the
/// others. Where that is not known, one at a time (the first, by rank, of those that fit): what
/// they send on starting would be the flood of all of them at once.
pub fn allocate(
    limit: Option<f64>,
    mut raise: Option<f64>,
    trial: Option<f64>,
    cameras: &mut [Camera],
) {
    let unlimited = limit.is_none();
    let limit = limit.unwrap_or(f64::INFINITY);
    let mut order: Vec<usize> = (0..cameras.len()).collect();
    order.sort_by_key(|&i| cameras[i].rank);
    let sent: f64 = cameras.iter().map(|c| c.cost(c.level)).sum();
    let mut tried = vec![false; cameras.len()];
    if let Some(reach) = trial {
        // Where the link is not read (`reach` is not a number), one camera at a time, and only where
        // the limit has room for it too: nothing says how many the link carries.
        let known = reach.is_finite();
        let mut total = sent;
        for &i in &order {
            let after = total + cameras[i].cost(1);
            if cameras[i].long_paused()
                && after <= reach
                && (known || after * RAISE_MARGIN <= limit)
            {
                total = after;
                tried[i] = true;
                raise = None;
                if !known {
                    break;
                }
            }
        }
    }
    // What the cameras after each in `order` that flow now need at the least: a step beyond its
    // lowest is not given to a camera at the price of pausing one of them.
    let mut keep = vec![0.0; cameras.len() + 1];
    for (n, &i) in order.iter().enumerate().rev() {
        let c = &cameras[i];
        keep[n] = keep[n + 1]
            + if c.level > 0 && !tried[i] {
                c.cost(1)
            } else {
                0.0
            };
    }
    let mut given = 0.0;
    for (n, i) in order.into_iter().enumerate() {
        let c = &mut cameras[i];
        let before = c.level;
        if tried[i] {
            // On top of what the limit holds, so as to take nothing from the cameras that flow.
            c.level = 1;
            continue;
        }
        let reserve = keep[n + 1];
        let mut level = c.top();
        while level > 0 {
            // The lowest step is given whatever the cameras after it need.
            let needed = given + c.cost(level) + if level > 1 { reserve } else { 0.0 };
            let fits = if level > before {
                // Taking a camera up adds to everything sent now, whatever its rank: it never
                // displaces a camera that flows.
                let total = sent + c.cost(level) - c.cost(before);
                unlimited
                    || raise.is_some_and(|most| {
                        total <= most
                            && c.switch_cost(before, level) <= (most - total) * KEYFRAME_DRAIN
                    }) && total * RAISE_MARGIN <= limit
                        && needed <= limit
            } else {
                needed <= limit
            };
            if fits {
                break;
            }
            level -= 1;
        }
        given += c.cost(level);
        c.level = level;
        if level > before {
            raise = None;
        }
    }
}

/// How much of the receiver's path the cameras may have, and when to look for more.
#[derive(Debug)]
pub struct CameraBudget {
    /// Bits per second the cameras are held to; `None` while the path has given no reason to.
    limit: Option<f64>,
    last: Option<Instant>,
    clear_since: Option<Instant>,
    hold_until: Option<Instant>,
    /// When the cameras were last cut.
    cut_at: Option<Instant>,
    /// Since when the path has held [`QUEUE_HIGH`].
    queued_since: Option<Instant>,
    /// Until when the queue the keyframe of a camera just taken up sets is expected, and until
    /// when that step is on trial: congestion within it means the step did not fit.
    grace_until: Option<Instant>,
    trial_until: Option<Instant>,
    /// The most the cameras may cost after one is taken up: what the narrowest link of the path
    /// carries (less the screen), when that is known.
    ceiling: Option<f64>,
    /// The most the cameras may cost for a camera paused too long to be tried (see
    /// [`MAX_PAUSE`]): what the narrowest link carries, less the screen and room for its
    /// keyframes; unbounded while unknown.
    reach: f64,
    /// Whether the last review found the path congested, or something standing queued on it.
    unsteady: bool,
    /// When a step may be tried again, and the wait after the next failure.
    raise_after: Option<Instant>,
    backoff: Duration,
    /// Since when the cameras have been held without trouble.
    calm_since: Option<Instant>,
    /// Since when the last step has held without trouble (nothing while none has been taken since
    /// the path was last congested).
    held_since: Option<Instant>,
    /// What the last review found the path congested by, for the log.
    cause: Option<Cause>,
}

/// What made a review find the path congested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    /// A queue stands on the path and it drops what it is sent.
    Queue,
    /// The queue is past [`QUEUE_CRITICAL`].
    Critical,
    /// The queue is growing.
    Surging,
    /// The queue has stood for [`QUEUE_PERSISTS`] and is not shrinking.
    Persisting,
    /// No queue, but the path drops what it is sent and its narrowest link is too narrow for it.
    Dropping,
}

impl Default for CameraBudget {
    fn default() -> Self {
        Self {
            limit: None,
            last: None,
            clear_since: None,
            hold_until: None,
            cut_at: None,
            queued_since: None,
            grace_until: None,
            trial_until: None,
            ceiling: None,
            reach: f64::INFINITY,
            unsteady: false,
            raise_after: None,
            backoff: INITIAL_BACKOFF,
            calm_since: None,
            held_since: None,
            cause: None,
        }
    }
}

impl CameraBudget {
    pub fn limit(&self) -> Option<f64> {
        self.limit
    }

    /// What the last review found the path congested by.
    pub fn cause(&self) -> Option<Cause> {
        self.cause
    }

    /// What a camera paused for [`MAX_PAUSE`] may be tried against (see [`allocate`]), unless the
    /// path is congested or holds a queue, a cut is being waited out, or the last step that failed
    /// has not been waited out. The screen share is counted in what is left, with room for its
    /// keyframes (see [`SCREEN_KEYFRAME_ROOM`]).
    pub fn pause_trial(&self, now: Instant) -> Option<f64> {
        (self.limit.is_some()
            && !self.unsteady
            && self.hold_until.is_none_or(|t| now >= t)
            && self.raise_after.is_none_or(|t| now >= t))
        .then_some(self.reach)
    }

    /// Why the path carries less than the receiver is sent, if it does (see `ScreenLevel`, whose
    /// reading of the queue this is, less the screen share's own keyframes).
    ///
    /// Loss with nothing standing queued counts only where the narrowest link is known and too
    /// narrow for what is sent: a path that drops at random (a lossy mobile link, a reordering
    /// one) drops as much at any rate, so cutting for it only pauses the cameras.
    fn congested(&mut self, now: Instant, path: &ScreenInputs, room: bool) -> Option<Cause> {
        let Some(q) = path.queue.filter(|&q| q >= QUEUE_HIGH) else {
            self.queued_since = None;
            return (path.dropping() && path.capacity.is_some() && !room)
                .then_some(Cause::Dropping);
        };
        let since = *self.queued_since.get_or_insert(now);
        let trend = path.queue_trend.unwrap_or(0.0);
        let surging = trend >= QUEUE_SURGING && self.grace_until.is_none_or(|t| now >= t);
        if path.dropping() {
            Some(Cause::Queue)
        } else if q >= QUEUE_CRITICAL {
            Some(Cause::Critical)
        } else if surging {
            Some(Cause::Surging)
        } else if now.saturating_duration_since(since) >= QUEUE_PERSISTS && trend >= QUEUE_SHRINKING
        {
            Some(Cause::Persisting)
        } else {
            None
        }
    }

    /// Reads the path (`path.other` is the screen share's rate) for the cameras `load` describes:
    /// held to what the path delivered when it is congested, trusted with a little more each
    /// second while it is clear, and let go once every camera has been taken up again and the path
    /// has stayed clear a long time.
    pub fn review(&mut self, now: Instant, path: &ScreenInputs, load: Load) {
        let elapsed = self
            .last
            .map_or(Duration::ZERO, |t| now.saturating_duration_since(t))
            .min(Duration::from_secs(2));
        self.last = Some(now);
        self.ceiling = path.capacity.map(|c| c * CAPACITY_SHARE - path.other);
        self.reach = path.capacity.map_or(f64::INFINITY, |c| {
            c * REACH_SHARE - path.other * SCREEN_KEYFRAME_ROOM
        });
        let room = path
            .capacity
            .is_some_and(|c| c * CAPACITY_SHARE >= (load.sent + path.other) * LOSS_ROOM);
        self.cause = self.congested(now, path, room);
        let congested = self.cause.is_some();
        self.unsteady = congested || !path.empty();
        if congested {
            self.clear_since = None;
            self.calm_since = None;
            self.held_since = None;
            if self.trial_until.take().is_some_and(|t| now < t) {
                self.raise_after = Some(now + self.backoff);
                self.backoff = (self.backoff * 2).min(MAX_BACKOFF);
            }
            if self
                .cut_at
                .is_none_or(|t| now.saturating_duration_since(t) >= CUT_SETTLES)
            {
                let carried = path.acked.map(|a| a - path.other);
                let share = HEADROOM
                    .min(1.0 - path.queue.unwrap_or(0.0) / DRAIN_SECONDS)
                    .max(LEAST_SHARE);
                // Never below half of what is sent: a path that carried little for a moment (a
                // link that flaps) is not taken at its worst, and a fresh reading replaces an old
                // one, whichever is lower.
                let cut = carried
                    .map_or(load.sent * HEADROOM, |c| c * share)
                    .max(load.sent * MOST_CUT)
                    .max(0.0);
                self.limit = Some(cut);
                self.hold_until = Some(now + HOLD);
                self.cut_at = Some(now);
            }
        } else {
            // A step that held (the path stayed calm for [`STEP_HELD`] after it): the wait that
            // grew with the steps before it starts over. Not on the time the path has been calm:
            // the wait that has grown is itself spent calm, and would be started over by it.
            if self
                .held_since
                .is_some_and(|t| now.saturating_duration_since(t) >= STEP_HELD)
            {
                self.held_since = None;
                self.backoff = INITIAL_BACKOFF;
            }
            // Cameras that move more than they did (a still student starts to gesture) cost more
            // without the path having said anything: the limit follows what is sent while it is
            // not congested, so nothing is lowered then.
            if let Some(limit) = self.limit.as_mut() {
                *limit = limit.max(load.sent);
            }
            let clear = path.clear();
            if clear || path.draining() {
                self.grow(now, path, load, elapsed, clear);
            } else {
                self.clear_since = None;
            }
        }
        if let Some(limit) = self.limit.as_mut() {
            // Whatever the path does, the host's camera keeps its lowest step.
            *limit = limit.max(load.floor);
        }
    }

    /// A clear path is trusted with a little more each second, and with all the cameras once they
    /// have been taken up again and it has stayed clear a long time.
    ///
    /// A path that is not `clear` but empties its queue while it delivers (see
    /// [`ScreenInputs::draining`]) earns the trust and the forgetting, not a camera taken up: that
    /// still needs the path to have been clear.
    fn grow(
        &mut self,
        now: Instant,
        path: &ScreenInputs,
        load: Load,
        elapsed: Duration,
        clear: bool,
    ) {
        let Some(limit) = self.limit else { return };
        if clear {
            self.clear_since.get_or_insert(now);
        } else {
            self.clear_since = None;
        }
        if self.hold_until.is_none_or(|t| now >= t) {
            let seen = path.acked.map_or(0.0, |a| a - path.other).max(load.sent);
            // From at least what the cheapest step costs: a limit of nothing (every camera paused
            // and no host's camera to hold it up) grows at nothing.
            let grown = limit.max(load.next) * (1.0 + GROWTH_PER_SECOND * elapsed.as_secs_f64());
            // Enough to take the cheapest camera step up, with the margin that takes.
            let above = (load.sent + load.next) * RAISE_MARGIN;
            // A narrowest link known to carry more is trusted at once.
            let link = path
                .capacity
                .map_or(0.0, |c| c * CAPACITY_SHARE - path.other);
            self.limit = Some(grown.min((GROWTH_CEILING * seen).max(above)).max(link));
        }
        let calm = now.saturating_duration_since(*self.calm_since.get_or_insert(now));
        let clear_for = self
            .clear_since
            .map_or(Duration::ZERO, |t| now.saturating_duration_since(t));
        if calm >= FORGET_AFTER && clear_for >= FORGET_AFTER && load.sent >= load.wanted {
            self.limit = None;
            self.backoff = INITIAL_BACKOFF;
            self.raise_after = None;
        }
    }

    /// Whether a camera may be taken up a step, and if so what the cameras may cost then (see
    /// [`allocate`]): the path has stayed clear since the last hold, and the last step that failed
    /// has been waited out.
    pub fn raise_ceiling(&self, now: Instant) -> Option<f64> {
        (self.limit.is_some()
            && self.hold_until.is_none_or(|t| now >= t)
            && self
                .clear_since
                .is_some_and(|t| now.saturating_duration_since(t) >= CLEAR_FOR)
            && self.raise_after.is_none_or(|t| now >= t))
        .then(|| {
            self.ceiling
                .filter(|_| {
                    self.cut_at
                        .is_some_and(|t| now.saturating_duration_since(t) < CEILING_TRUST)
                })
                .unwrap_or(f64::INFINITY)
        })
    }

    /// A camera was taken up a step: each step earns the next on its own.
    pub fn raised(&mut self, now: Instant) {
        self.trial_until = Some(now + TRIAL);
        self.held_since = Some(now);
        self.expect_keyframes(now);
        self.clear_since = None;
        self.calm_since = None;
    }

    /// Keyframes are on their way to the receiver (it asked for them, or a stream of it is being
    /// moved or started): they arrive in a burst, and the queue they set is not congestion.
    pub fn expect_keyframes(&mut self, now: Instant) {
        let until = now + KEYFRAME_GRACE;
        self.grace_until = Some(self.grace_until.map_or(until, |t| t.max(until)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layers::LayerRates;

    const CAMERA: u64 = 400_000;

    fn plain() -> CameraSteps {
        CameraSteps::of(
            Some(LayerRates {
                total: Some(CAMERA),
                ..Default::default()
            }),
            None,
        )
    }

    /// A camera sent as a browser's encoder does: the base layer two fifths of the bits and the
    /// second one fifth.
    fn layered() -> CameraSteps {
        CameraSteps::of(Some(layered_rates()), None)
    }

    fn layered_rates() -> LayerRates {
        LayerRates {
            total: Some(CAMERA),
            temporal: Some([Some(160_000), Some(80_000)]),
            keyframe_bits: None,
        }
    }

    /// The rung of a camera sent whole.
    const WHOLE: Rung = Rung {
        layer: Layer::Low,
        tid: None,
    };

    fn class(students: usize, steps: CameraSteps) -> Vec<Camera> {
        // The host is last on purpose: the order cameras come in does not matter to it.
        (0..students)
            .map(|_| Camera::new(RANK_WATCHED, steps, None, WHOLE))
            .chain([Camera::new(RANK_HOST, steps, None, WHOLE)])
            .collect()
    }

    fn levels(cameras: &[Camera]) -> Vec<usize> {
        cameras.iter().map(|c| c.level).collect()
    }

    #[test]
    fn without_a_limit_every_camera_is_sent_whole() {
        let mut cameras = class(7, plain());
        allocate(None, None, None, &mut cameras);
        assert!(cameras.iter().all(|c| c.level == c.top()));
    }

    #[test]
    fn the_host_keeps_its_camera_and_the_last_students_wait() {
        let mut cameras = class(7, plain());
        // Room for three of eight cameras.
        allocate(Some(3.0 * CAMERA as f64), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 1, 0, 0, 0, 0, 0, 1]);
        assert!(Load::of(&cameras).sent <= 3.0 * CAMERA as f64);
    }

    #[test]
    fn a_camera_with_temporal_layers_gives_up_frames_before_it_is_paused() {
        let mut cameras = class(3, layered());
        // Two cameras and a bit more: the lowest step of every camera (a base layer, 160 kb/s)
        // fits, so none is paused; the host has what is above its lowest step first (all of it),
        // then the first student (its base and second layers, three fifths of its bits), and
        // what is left goes to the next.
        allocate(Some(1_040_000.0), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![2, 2, 1, 3]);
        assert_eq!(cameras[0].rung(2).tid, Some(1));
        // A little less, and the first student gives up its second layer for the others.
        allocate(Some(960_000.0), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![2, 1, 1, 3]);
        assert_eq!(cameras[1].rung(1).tid, Some(0));
        // A camera is paused only where not even its lowest step fits.
        allocate(Some(560_000.0), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 1, 0, 1]);
    }

    #[test]
    fn a_camera_is_not_given_a_step_above_its_lowest_at_the_price_of_pausing_another() {
        // The host's camera could be sent whole, which leaves the students too little for their
        // lowest steps: it is held to its lowest too, and none is paused.
        let mut cameras = class(7, simulcast());
        allocate(Some(1_700_000.0), None, None, &mut cameras);
        assert!(
            cameras.iter().all(|c| c.level == 1),
            "{:?}",
            levels(&cameras)
        );
        // With room for all the lowest steps and more, the host has the more first.
        allocate(Some(3_200_000.0), Some(f64::INFINITY), None, &mut cameras);
        assert_eq!(levels(&cameras).iter().filter(|&&l| l == 0).count(), 0);
        assert_eq!(
            cameras.iter().find(|c| c.rank == RANK_HOST).unwrap().level,
            2
        );
    }

    #[test]
    fn what_goes_down_does_not_come_back_without_room_and_a_clear_path() {
        let mut cameras = class(3, plain());
        allocate(Some(2.0 * CAMERA as f64), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 0, 0, 1]);
        // Room for all, but the path has not been clear long enough: nothing is taken up.
        allocate(Some(8.0 * CAMERA as f64), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 0, 0, 1]);
        // Clear: one is taken up, where it and the two sent fit a quarter over.
        allocate(
            Some(4.0 * CAMERA as f64),
            Some(f64::INFINITY),
            None,
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 1, 0, 1]);
    }

    #[test]
    fn a_camera_is_not_taken_up_past_what_the_narrowest_link_carries() {
        let mut cameras = class(3, plain());
        allocate(Some(2.0 * CAMERA as f64), None, None, &mut cameras);
        // Room in the limit for all four, but the link carries three of them.
        allocate(
            Some(8.0 * CAMERA as f64),
            Some(3.0 * CAMERA as f64),
            None,
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 1, 0, 1]);
        allocate(
            Some(8.0 * CAMERA as f64),
            Some(3.0 * CAMERA as f64),
            None,
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 1, 0, 1], "and no fourth");
    }

    #[test]
    fn one_camera_at_most_is_taken_up_at_a_time() {
        let mut cameras = class(7, plain());
        allocate(Some(1.0 * CAMERA as f64), None, None, &mut cameras);
        allocate(
            Some(80.0 * CAMERA as f64),
            Some(f64::INFINITY),
            None,
            &mut cameras,
        );
        assert_eq!(cameras.iter().filter(|c| c.level > 0).count(), 2);
    }

    #[test]
    fn an_estimate_swinging_a_tenth_either_way_moves_no_camera_twice() {
        for middle in (10..20).map(|n| n as f64 * 100_000.0) {
            for first in [0.9, 1.1] {
                let mut cameras = class(7, plain());
                let mut changes = 0;
                let mut before = levels(&cameras);
                for second in 0..120 {
                    let swing = if (second % 2 == 0) == (first == 0.9) {
                        0.9
                    } else {
                        1.1
                    };
                    allocate(
                        Some(middle * swing),
                        Some(f64::INFINITY),
                        None,
                        &mut cameras,
                    );
                    let now = levels(&cameras);
                    changes += usize::from(now != before);
                    before = now;
                }
                assert!(changes <= 2, "{middle}: {changes} changes, {before:?}");
            }
        }
    }

    /// A path that drops a quarter of what it is sent with nothing queued, its narrowest link
    /// known and carrying what the path delivers.
    fn congested() -> ScreenInputs {
        ScreenInputs {
            queue: Some(0.0),
            delivery: Some(0.75),
            acked: Some(2_300_000.0),
            capacity: Some(2_300_000.0),
            ..Default::default()
        }
    }

    fn clear(acked: f64) -> ScreenInputs {
        ScreenInputs {
            queue: Some(0.0),
            delivery: Some(1.0),
            acked: Some(acked),
            ..Default::default()
        }
    }

    fn load(sent: f64, wanted: f64) -> Load {
        Load {
            sent,
            wanted,
            next: CAMERA as f64,
            floor: 0.0,
        }
    }

    #[test]
    fn a_path_that_drops_what_it_is_sent_holds_the_cameras_to_what_it_delivered() {
        let mut b = CameraBudget::default();
        let t = Instant::now();
        b.review(t, &clear(3_000_000.0), load(3_000_000.0, 3_000_000.0));
        assert_eq!(b.limit(), None, "a clear path is not held");
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        let limit = b.limit().expect("held");
        assert!((limit - 2_300_000.0 * HEADROOM).abs() < 1.0, "{limit}");
    }

    #[test]
    fn loss_alone_on_a_path_whose_narrowest_link_is_unknown_holds_nothing() {
        let mut b = CameraBudget::default();
        let t = Instant::now();
        let path = ScreenInputs {
            capacity: None,
            ..congested()
        };
        b.review(t, &path, load(3_300_000.0, 3_300_000.0));
        assert_eq!(b.limit(), None);
        assert_eq!(b.cause(), None);
    }

    #[test]
    fn loss_alone_on_a_path_whose_narrowest_link_is_wide_holds_nothing() {
        let mut b = CameraBudget::default();
        let path = ScreenInputs {
            capacity: Some(50_000_000.0),
            ..congested()
        };
        b.review(Instant::now(), &path, load(3_300_000.0, 3_300_000.0));
        assert_eq!(b.limit(), None);
    }

    #[test]
    fn loss_on_a_path_whose_narrowest_link_is_narrow_cuts_and_says_why() {
        let mut b = CameraBudget::default();
        b.review(Instant::now(), &congested(), load(3_300_000.0, 3_300_000.0));
        assert!(b.limit().is_some());
        assert_eq!(b.cause(), Some(Cause::Dropping));
    }

    #[test]
    fn the_screen_share_is_given_its_part_first() {
        let mut b = CameraBudget::default();
        let path = ScreenInputs {
            other: 1_000_000.0,
            ..congested()
        };
        b.review(Instant::now(), &path, load(2_000_000.0, 3_300_000.0));
        let limit = b.limit().expect("held");
        assert!((limit - 1_300_000.0 * HEADROOM).abs() < 1.0, "{limit}");
    }

    #[test]
    fn a_clear_path_is_trusted_with_more_only_after_it_has_stayed_clear() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        let held = b.limit().unwrap();
        let mut raised_at = None;
        for second in 1..10 {
            t += Duration::from_secs(1);
            b.review(t, &clear(held), load(held, 3_300_000.0));
            if raised_at.is_none() && b.raise_ceiling(t).is_some() {
                raised_at = Some(second);
            }
        }
        // Held for HOLD, and clear for CLEAR_FOR from the first clear second.
        let expected = HOLD.max(Duration::from_secs(1) + CLEAR_FOR);
        assert_eq!(raised_at, Some(expected.as_secs()));
        assert!(b.limit().unwrap() > held, "trusted with more");
    }

    #[test]
    fn a_step_that_brought_congestion_waits_longer_each_time() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        let mut waits = Vec::new();
        for _ in 0..3 {
            // Clear until a step may be taken, take it, and have the path refuse it.
            let from = t;
            loop {
                t += Duration::from_secs(1);
                b.review(t, &clear(2_000_000.0), load(2_000_000.0, 3_300_000.0));
                if b.raise_ceiling(t).is_some() {
                    break;
                }
            }
            waits.push(t.duration_since(from).as_secs());
            b.raised(t);
            t += Duration::from_millis(1500);
            b.review(t, &congested(), load(2_400_000.0, 3_300_000.0));
        }
        assert!(waits[1] > waits[0] && waits[2] > waits[1], "{waits:?}");
    }

    #[test]
    fn a_path_flickering_between_clear_and_congested_takes_few_steps() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        let mut steps = 0;
        for second in 0..180 {
            t += Duration::from_secs(1);
            let path = if second % 4 == 0 {
                congested()
            } else {
                clear(2_000_000.0)
            };
            b.review(t, &path, load(2_000_000.0, 3_300_000.0));
            if b.raise_ceiling(t).is_some() {
                b.raised(t);
                steps += 1;
            }
        }
        // The wait after a step that failed doubles up to MAX_BACKOFF: one step in twenty seconds
        // (the first waits are shorter, and a step takes a cut's hold and a clear path to come to).
        // Before the wait was capped at two minutes the bound was five.
        let most = 180 / MAX_BACKOFF.as_secs();
        assert!(steps <= most, "{steps} steps in three minutes, over {most}");
    }

    #[test]
    fn a_narrowest_link_known_to_carry_more_is_trusted_at_once() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        t += Duration::from_secs(5);
        let path = ScreenInputs {
            capacity: Some(4_000_000.0),
            ..clear(2_000_000.0)
        };
        b.review(t, &path, load(2_000_000.0, 3_300_000.0));
        assert!((b.limit().unwrap() - 4_000_000.0 * CAPACITY_SHARE).abs() < 1.0);
    }

    #[test]
    fn the_narrowest_links_rate_holds_a_camera_back_for_a_while_after_a_cut() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        let path = ScreenInputs {
            capacity: Some(2_400_000.0),
            ..clear(2_000_000.0)
        };
        t += Duration::from_secs(10);
        b.review(t, &path, load(2_000_000.0, 3_300_000.0));
        t += CLEAR_FOR;
        b.review(t, &path, load(2_000_000.0, 3_300_000.0));
        let held = b.raise_ceiling(t).expect("may raise");
        assert!((held - 2_400_000.0 * CAPACITY_SHARE).abs() < 1.0, "{held}");
        // Steps taken one after the other, none refused, until the rate has been trusted as long as
        // it is: it no longer holds the next back.
        for second in 0..CEILING_TRUST.as_secs() + 3 {
            t += Duration::from_secs(1);
            b.review(t, &path, load(2_000_000.0, 3_300_000.0));
            if second % 5 == 4 {
                b.raised(t);
            }
        }
        assert_eq!(b.raise_ceiling(t), Some(f64::INFINITY));
    }

    #[test]
    fn a_path_clear_for_a_long_time_with_room_for_all_lets_the_cameras_go() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        let path = ScreenInputs {
            capacity: Some(8_000_000.0),
            ..clear(3_300_000.0)
        };
        for _ in 0..40 {
            t += Duration::from_secs(1);
            b.review(t, &path, load(3_300_000.0, 3_300_000.0));
        }
        assert_eq!(b.limit(), None);
    }

    /// A path that carries `link` bits a second, as the cameras sent now find it (its narrowest
    /// link is read while it drops what is sent, and not while it is clear), and the budget with
    /// the cameras it has been holding.
    struct Sim {
        budget: CameraBudget,
        cameras: Vec<Camera>,
        t: Instant,
    }

    impl Sim {
        fn new(cameras: Vec<Camera>) -> Self {
            Sim {
                budget: CameraBudget::default(),
                cameras,
                t: Instant::now(),
            }
        }

        /// One second: the path says what it makes of what is sent, and the cameras follow.
        fn second(&mut self, link: f64) {
            let load = Load::of(&self.cameras);
            let path = if load.sent <= link * 0.97 {
                clear(load.sent)
            } else {
                ScreenInputs {
                    queue: Some(0.0),
                    delivery: Some(link / load.sent),
                    acked: Some(link),
                    capacity: Some(link),
                    ..Default::default()
                }
            };
            self.budget.review(self.t, &path, load);
            let before = levels(&self.cameras);
            allocate(
                self.budget.limit(),
                self.budget.raise_ceiling(self.t),
                None,
                &mut self.cameras,
            );
            if levels(&self.cameras)
                .iter()
                .zip(&before)
                .any(|(n, b)| n > b)
            {
                self.budget.raised(self.t);
            }
            self.t += Duration::from_secs(1);
        }

        fn whole(&self) -> bool {
            self.cameras.iter().all(|c| c.level == c.top())
        }
    }

    #[test]
    fn cameras_come_back_when_a_link_opens_moderately_with_no_reading_of_its_capacity() {
        let mut sim = Sim::new(class(7, plain()));
        for _ in 0..30 {
            sim.second(2_500_000.0);
        }
        assert!(!sim.whole() && sim.cameras.iter().any(|c| c.level == 0));
        // Room for all eight, but not by much: nothing says so but what is delivered.
        for _ in 0..180 {
            sim.second(6_000_000.0);
        }
        assert!(sim.whole(), "{:?}", levels(&sim.cameras));
        // And, with all of them sent and the path clear, the hold is let go.
        for _ in 0..60 {
            sim.second(6_000_000.0);
        }
        assert_eq!(sim.budget.limit(), None);
    }

    #[test]
    fn the_host_alone_flowing_is_not_a_dead_end() {
        let mut cameras = class(3, plain());
        for c in cameras.iter_mut().filter(|c| c.rank != RANK_HOST) {
            c.level = 0;
        }
        let mut sim = Sim::new(cameras);
        sim.budget
            .review(sim.t, &congested(), Load::of(&sim.cameras));
        for _ in 0..120 {
            sim.second(6_000_000.0);
        }
        assert!(sim.whole(), "{:?}", levels(&sim.cameras));
    }

    #[test]
    fn the_host_keeps_its_lowest_step_whatever_the_path_says() {
        let mut sim = Sim::new(class(7, layered()));
        // Congestion with another cause: even the cameras held to nothing do not clear it.
        for _ in 0..90 {
            let load = Load::of(&sim.cameras);
            let path = ScreenInputs {
                queue: Some(0.5),
                delivery: Some(0.4),
                acked: Some(20_000.0),
                ..Default::default()
            };
            sim.budget.review(sim.t, &path, load);
            allocate(sim.budget.limit(), None, None, &mut sim.cameras);
            sim.t += Duration::from_secs(1);
            let host = sim.cameras.iter().find(|c| c.rank == RANK_HOST).unwrap();
            assert!(host.level >= 1, "the host's camera was paused");
        }
    }

    #[test]
    fn cameras_that_move_more_are_not_cut_on_a_clear_path() {
        let mut sim = Sim::new(class(7, plain()));
        for _ in 0..30 {
            sim.second(2_500_000.0);
        }
        let held = levels(&sim.cameras);
        // The cameras that flow now cost three times what they did, and the path is clear.
        let busy = CameraSteps::of(
            Some(LayerRates {
                total: Some(3 * CAMERA),
                ..Default::default()
            }),
            None,
        );
        for c in sim.cameras.iter_mut().filter(|c| c.level > 0) {
            c.steps = busy;
        }
        let load = Load::of(&sim.cameras);
        sim.budget.review(sim.t, &clear(load.sent), load);
        allocate(sim.budget.limit(), None, None, &mut sim.cameras);
        assert_eq!(levels(&sim.cameras), held);
    }

    #[test]
    fn the_queue_a_keyframe_sets_is_not_congestion() {
        let burst = ScreenInputs {
            queue: Some(0.3),
            queue_trend: Some(0.25),
            delivery: Some(1.0),
            acked: Some(4_000_000.0),
            ..Default::default()
        };
        let t = Instant::now();
        let mut b = CameraBudget::default();
        b.review(t, &burst, load(3_300_000.0, 3_300_000.0));
        assert!(b.limit().is_some(), "a surge nobody expected is acted on");
        let mut b = CameraBudget::default();
        b.expect_keyframes(t);
        b.review(t, &burst, load(3_300_000.0, 3_300_000.0));
        assert_eq!(b.limit(), None);
    }

    #[test]
    fn speakers_taking_turns_do_not_swap_the_cameras_that_flow() {
        // Room for four of the eight cameras; the host and three students flow.
        let mut cameras = class(6, plain());
        allocate(Some(1_700_000.0), None, None, &mut cameras);
        let flowing = levels(&cameras);
        assert_eq!(flowing.iter().filter(|&&l| l > 0).count(), 4);
        // Two of the students that are paused speak in turn, each for a second and a half.
        let paused: Vec<usize> = (0..cameras.len()).filter(|&i| flowing[i] == 0).collect();
        for turn in 0..20 {
            let speaker = paused[turn % 2];
            for (i, c) in cameras.iter_mut().enumerate() {
                if c.rank != RANK_HOST {
                    c.rank = if i == speaker {
                        RANK_LOUD
                    } else {
                        RANK_WATCHED
                    };
                }
            }
            allocate(Some(1_700_000.0), Some(f64::INFINITY), None, &mut cameras);
            assert_eq!(levels(&cameras), flowing, "turn {turn}");
        }
    }

    #[test]
    fn who_is_given_room_first_goes_by_rank() {
        let mut cameras = vec![
            Camera::new(RANK_SMALL, plain(), None, WHOLE),
            Camera::new(RANK_WATCHED, plain(), None, WHOLE),
            Camera::new(RANK_LOUD, plain(), None, WHOLE),
            Camera::new(RANK_HOST, plain(), None, WHOLE),
        ];
        allocate(Some(2.0 * CAMERA as f64), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![0, 0, 1, 1]);
        allocate(
            Some(4.0 * CAMERA as f64),
            Some(f64::INFINITY),
            None,
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![0, 1, 1, 1]);
    }

    const SMALL: u64 = 200_000;
    const LARGE: u64 = 1_000_000;
    const KEYFRAME: f64 = 100_000.0;

    fn small_layer() -> LayerRates {
        LayerRates {
            total: Some(SMALL),
            ..Default::default()
        }
    }

    fn large_layer(total: u64) -> LayerRates {
        LayerRates {
            total: Some(total),
            keyframe_bits: Some(KEYFRAME),
            ..Default::default()
        }
    }

    /// A camera that sends a small and a large layer, as the host's does.
    fn simulcast() -> CameraSteps {
        CameraSteps::of(Some(small_layer()), Some(large_layer(LARGE)))
    }

    fn rungs(steps: &CameraSteps) -> Vec<(Layer, Option<u8>)> {
        steps
            .rungs()
            .iter()
            .map(|(r, _)| (r.layer, r.tid))
            .collect()
    }

    #[test]
    fn a_camera_with_two_layers_goes_down_to_the_small_one_before_it_loses_frames() {
        let whole = (Layer::Low, None);
        let large = (Layer::High, None);
        assert_eq!(rungs(&simulcast()), vec![whole, large]);
        // The temporal layers of the small one are steps; the large one's are not, even when it
        // has them.
        let layered_large = LayerRates {
            temporal: Some([Some(400_000), Some(200_000)]),
            ..large_layer(LARGE)
        };
        let steps = CameraSteps::of(Some(layered_rates()), Some(layered_large));
        assert_eq!(
            rungs(&steps),
            vec![(Layer::Low, Some(0)), (Layer::Low, Some(1)), whole, large]
        );
        assert_eq!(steps.lowest(), 160_000.0);
    }

    #[test]
    fn a_small_layer_that_is_no_cheaper_than_the_large_is_no_step_down() {
        let steps = CameraSteps::of(Some(large_layer(LARGE)), Some(large_layer(LARGE)));
        assert_eq!(rungs(&steps), vec![(Layer::High, None)]);
        let layered_large = LayerRates {
            temporal: Some([Some(400_000), Some(200_000)]),
            ..large_layer(LARGE)
        };
        let steps = CameraSteps::of(Some(large_layer(2 * LARGE)), Some(layered_large));
        assert_eq!(
            rungs(&steps),
            vec![
                (Layer::High, Some(0)),
                (Layer::High, Some(1)),
                (Layer::High, None)
            ]
        );
    }

    #[test]
    fn a_camera_with_one_layer_has_the_steps_of_it() {
        assert_eq!(rungs(&layered()).len(), 3);
        assert!(rungs(&layered()).iter().all(|&(l, _)| l == Layer::Low));
        let large = CameraSteps::of(None, Some(large_layer(LARGE)));
        assert_eq!(rungs(&large), vec![(Layer::High, None)]);
        assert_eq!(rungs(&CameraSteps::default()), vec![(Layer::Low, None)]);
    }

    #[test]
    fn a_receiver_held_to_the_small_layer_has_its_steps_alone() {
        assert_eq!(rungs(&simulcast().small_only()), vec![(Layer::Low, None)]);
        let large = CameraSteps::of(None, Some(large_layer(LARGE)));
        assert_eq!(rungs(&large.small_only()), vec![(Layer::High, None)]);
    }

    fn host(steps: CameraSteps, rung: Rung) -> Camera {
        Camera::new(RANK_HOST, steps, None, rung)
    }

    const HIGH_WHOLE: Rung = Rung {
        layer: Layer::High,
        tid: None,
    };

    #[test]
    fn a_link_that_carries_the_small_layer_alone_holds_the_host_on_it_whole() {
        // The large layer of the host's camera with temporal layers would have been thinned to
        // fit; the small one is sent whole instead.
        let large = LayerRates {
            temporal: Some([Some(400_000), Some(200_000)]),
            ..large_layer(LARGE)
        };
        let steps = CameraSteps::of(Some(small_layer()), Some(large));
        let mut cameras = [host(steps, HIGH_WHOLE)];
        allocate(Some(600_000.0), None, None, &mut cameras);
        let level = cameras[0].level;
        assert_eq!(cameras[0].rung(level), WHOLE);
        assert!(cameras[0].held_small(level));
        // Room for the large one whole: it is no longer held.
        allocate(Some(5_000_000.0), Some(f64::INFINITY), None, &mut cameras);
        let level = cameras[0].level;
        assert_eq!(cameras[0].rung(level), HIGH_WHOLE);
        assert!(!cameras[0].held_small(level));
    }

    #[test]
    fn a_camera_with_no_larger_layer_is_never_held_small() {
        let cameras = class(1, plain());
        assert!(!cameras[0].held_small(cameras[0].level));
        let cut = Camera::new(RANK_HOST, simulcast().small_only(), None, WHOLE);
        assert!(
            !cut.held_small(cut.level),
            "held small by the receiver, not the budget"
        );
    }

    #[test]
    fn the_host_keeps_the_lowest_step_of_the_ladder_it_has() {
        let layered_small = CameraSteps::of(Some(layered_rates()), Some(large_layer(LARGE)));
        let cameras = [host(layered_small, HIGH_WHOLE)];
        assert_eq!(Load::of(&cameras).floor, 160_000.0);
        let cameras = [host(simulcast(), HIGH_WHOLE)];
        assert_eq!(Load::of(&cameras).floor, SMALL as f64);
        let cameras = [host(
            CameraSteps::of(None, Some(large_layer(LARGE))),
            HIGH_WHOLE,
        )];
        assert_eq!(Load::of(&cameras).floor, LARGE as f64);
    }

    #[test]
    fn a_camera_goes_onto_the_large_layer_only_where_its_keyframe_drains() {
        let mut cameras = [host(simulcast(), WHOLE)];
        let level = cameras[0].level;
        // Room in the limit, and in the narrowest link for the large layer whole (1 Mb/s) but not
        // for its keyframe (100 kbit would take longer than the drain allows to leave in 20 kb/s).
        allocate(Some(5_000_000.0), Some(1_020_000.0), None, &mut cameras);
        assert_eq!(cameras[0].level, level, "the keyframe would queue");
        allocate(Some(5_000_000.0), Some(1_100_000.0), None, &mut cameras);
        assert_eq!(cameras[0].level, 2);
        // And where the link is not known, nothing is held back.
        let mut cameras = [host(simulcast(), WHOLE)];
        allocate(Some(5_000_000.0), Some(f64::INFINITY), None, &mut cameras);
        assert_eq!(cameras[0].level, 2);
    }

    #[test]
    fn a_move_down_from_the_large_layer_needs_no_room_for_a_keyframe() {
        let mut cameras = [host(simulcast(), HIGH_WHOLE)];
        allocate(Some(400_000.0), None, None, &mut cameras);
        assert_eq!(cameras[0].level, 1);
    }

    #[test]
    fn a_cut_never_goes_below_half_of_what_is_sent() {
        let mut b = CameraBudget::default();
        let t = Instant::now();
        // The path delivered next to nothing for a moment.
        let path = ScreenInputs {
            acked: Some(100_000.0),
            ..congested()
        };
        b.review(t, &path, load(2_000_000.0, 3_300_000.0));
        assert_eq!(b.limit(), Some(1_000_000.0));
    }

    #[test]
    fn a_fresh_cut_replaces_an_older_lower_one() {
        let mut b = CameraBudget::default();
        let t = Instant::now();
        let low = ScreenInputs {
            acked: Some(500_000.0),
            capacity: Some(500_000.0),
            ..congested()
        };
        b.review(t, &low, load(1_000_000.0, 3_300_000.0));
        let first = b.limit().unwrap();
        let better = ScreenInputs {
            acked: Some(2_000_000.0),
            capacity: Some(2_000_000.0),
            ..congested()
        };
        b.review(t + CUT_SETTLES, &better, load(3_300_000.0, 3_300_000.0));
        assert!(b.limit().unwrap() > first, "{:?} after {first}", b.limit());
    }

    #[test]
    fn nothing_is_read_while_the_feedback_has_stopped_or_has_just_come_back() {
        let t = Instant::now();
        assert!(!feedback_stopped(t, None), "nothing is known yet");
        assert!(!feedback_stopped(t + OUTAGE_GAP, Some(t)));
        assert!(feedback_stopped(
            t + OUTAGE_GAP + Duration::from_millis(1),
            Some(t)
        ));
        let back = t + Duration::from_secs(10);
        assert!(!path_settling(back, None));
        assert!(path_settling(back + OUTAGE_SETTLES / 2, Some(back)));
        assert!(!path_settling(back + OUTAGE_SETTLES, Some(back)));
    }

    #[test]
    fn the_wait_after_a_failed_step_stops_at_its_maximum() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        let mut waits = Vec::new();
        for _ in 0..8 {
            let from = t;
            loop {
                t += Duration::from_secs(1);
                b.review(t, &clear(2_000_000.0), load(2_000_000.0, 3_300_000.0));
                if b.raise_ceiling(t).is_some() {
                    break;
                }
            }
            waits.push(t.duration_since(from));
            b.raised(t);
            t += Duration::from_millis(1500);
            b.review(t, &congested(), load(2_400_000.0, 3_300_000.0));
        }
        let last = waits.last().unwrap();
        // 5, 10 and then 20 seconds (and what a cut and a clear path take), no more and no less:
        // the wait is not started over by the calm it is waited out in.
        assert!(*last >= MAX_BACKOFF, "{waits:?}");
        assert!(*last <= MAX_BACKOFF + HOLD + CLEAR_FOR, "{waits:?}");
        assert!(waits.windows(2).all(|w| w[0] <= w[1]), "{waits:?}");
    }

    #[test]
    fn the_wait_starts_over_only_once_a_step_has_held() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        b.raised(t);
        t += Duration::from_secs(1);
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        assert!(b.backoff > INITIAL_BACKOFF);
        // Calm for as long as the longest wait: the wait does not start over for that, or a link
        // that flickers would never wait the longest.
        for _ in 0..=MAX_BACKOFF.as_secs() {
            t += Duration::from_secs(1);
            b.review(t, &clear(2_000_000.0), load(2_000_000.0, 3_300_000.0));
        }
        assert!(b.backoff > INITIAL_BACKOFF);
        // A step taken then that holds does.
        b.raised(t);
        for _ in 1..STEP_HELD.as_secs() {
            t += Duration::from_secs(1);
            b.review(t, &clear(2_000_000.0), load(2_000_000.0, 3_300_000.0));
            assert!(b.backoff > INITIAL_BACKOFF, "too soon");
        }
        t += Duration::from_secs(1);
        b.review(t, &clear(2_000_000.0), load(2_000_000.0, 3_300_000.0));
        assert_eq!(b.backoff, INITIAL_BACKOFF);
    }

    #[test]
    fn a_limit_the_path_has_stopped_asking_for_is_forgotten_whether_or_not_a_camera_was_paused() {
        // Congested for ten seconds with every camera still sent whole, then clear for fifteen.
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        for _ in 0..10 {
            t += Duration::from_secs(1);
            b.review(t, &congested(), load(2_000_000.0, 2_000_000.0));
        }
        assert!(b.limit().is_some());
        for _ in 0..15 {
            t += Duration::from_secs(1);
            b.review(t, &clear(2_000_000.0), load(2_000_000.0, 2_000_000.0));
        }
        assert_eq!(b.limit(), None);
        // With a camera still held back it is not: it would be sent again with all the others at
        // once.
        let mut b = CameraBudget::default();
        for _ in 0..10 {
            t += Duration::from_secs(1);
            b.review(t, &congested(), load(2_000_000.0, 3_300_000.0));
        }
        for _ in 0..15 {
            t += Duration::from_secs(1);
            b.review(t, &clear(2_000_000.0), load(2_000_000.0, 3_300_000.0));
        }
        assert!(b.limit().is_some());
    }

    #[test]
    fn a_link_that_flaps_earns_the_limit_more_trust_but_no_camera_a_step_up() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        b.review(t, &congested(), load(3_300_000.0, 3_300_000.0));
        let held = b.limit().unwrap();
        // The queue empties while the path delivers, but is never down at nothing.
        let draining = ScreenInputs {
            queue: Some(0.1),
            queue_trend: Some(-0.1),
            delivery: Some(0.95),
            acked: Some(held),
            ..Default::default()
        };
        for _ in 0..10 {
            t += Duration::from_secs(1);
            b.review(t, &draining, load(held, 3_300_000.0));
            assert_eq!(b.raise_ceiling(t), None, "a step was allowed");
        }
        assert!(b.limit().unwrap() > held, "trusted with more");
    }

    fn paused_for(rank: u8, seconds: u64) -> Camera {
        Camera::new(rank, plain(), Some(Duration::from_secs(seconds)), WHOLE)
    }

    #[test]
    fn a_camera_paused_for_ten_seconds_flows_at_its_lowest_step_whatever_the_limit_says() {
        let mut cameras = vec![
            Camera::new(RANK_HOST, plain(), None, WHOLE),
            paused_for(RANK_WATCHED, 9),
            paused_for(RANK_WATCHED, 10),
        ];
        // The limit holds the host alone, no step is allowed, but the link carries three.
        allocate(
            Some(CAMERA as f64),
            None,
            Some(3.0 * CAMERA as f64),
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 0, 1]);
        // Not where it would not fit the link...
        let mut cameras = vec![
            Camera::new(RANK_HOST, plain(), None, WHOLE),
            paused_for(RANK_WATCHED, 10),
        ];
        allocate(
            Some(CAMERA as f64),
            None,
            Some(1.5 * CAMERA as f64),
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 0]);
        // ...nor where the trial is not allowed.
        allocate(Some(CAMERA as f64), None, None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 0]);
    }

    #[test]
    fn with_no_reading_of_the_link_one_paused_camera_comes_back_per_review() {
        let paused = || {
            let mut cameras = vec![Camera::new(RANK_HOST, plain(), None, WHOLE)];
            cameras.extend((0..4).map(|_| paused_for(RANK_WATCHED, 10)));
            cameras
        };
        let room = 10.0 * CAMERA as f64;
        let mut cameras = paused();
        for returned in 1..=4 {
            allocate(Some(room), None, Some(f64::INFINITY), &mut cameras);
            assert_eq!(
                cameras.iter().filter(|c| c.level > 0).count(),
                1 + returned,
                "{:?}",
                levels(&cameras)
            );
            // The ones that came back are not paused any more, the others still are.
            for c in cameras.iter_mut().filter(|c| c.level > 0) {
                c.paused_for = Duration::ZERO;
            }
        }
        // Where the link is read, as many as it carries come back together.
        let mut cameras = paused();
        allocate(Some(room), None, Some(3.0 * CAMERA as f64), &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 1, 1, 0, 0]);
        // Where the link is not read, the camera must fit in what the limit allows too.
        let mut cameras = paused();
        allocate(
            Some(1.5 * CAMERA as f64),
            None,
            Some(f64::INFINITY),
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 0, 0, 0, 0]);
    }

    #[test]
    fn a_paused_camera_is_tried_again_whatever_share_of_the_link_a_screen_share_takes() {
        let capacity = Some(5_000_000.0);
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        let screen = ScreenInputs {
            capacity,
            other: 2_000_000.0,
            ..congested()
        };
        b.review(t, &screen, load(1_000_000.0, 2_000_000.0));
        t += HOLD + Duration::from_secs(1);
        let calm = ScreenInputs {
            capacity,
            other: 2_000_000.0,
            ..clear(1_400_000.0)
        };
        b.review(t, &calm, load(400_000.0, 800_000.0));
        // The screen takes two fifths of the link: the cameras are left what is not the screen's,
        // with room for its keyframes (a fifth more).
        let reach = b.pause_trial(t).expect("a screen share is no reason");
        assert!(
            (reach - (5_000_000.0 * REACH_SHARE - 2_000_000.0 * SCREEN_KEYFRAME_ROOM)).abs() < 1.0,
            "{reach}"
        );
        let mut cameras = vec![
            Camera::new(RANK_HOST, plain(), None, WHOLE),
            paused_for(RANK_WATCHED, 10),
        ];
        allocate(b.limit(), b.raise_ceiling(t), Some(reach), &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 1]);
        // A screen that takes the whole link leaves nothing to try with.
        let mut b = CameraBudget::default();
        let hog = ScreenInputs {
            other: 4_900_000.0,
            ..screen
        };
        b.review(t, &hog, load(1_000_000.0, 2_000_000.0));
        t += HOLD + Duration::from_secs(1);
        b.review(
            t,
            &ScreenInputs {
                other: 4_900_000.0,
                ..calm
            },
            load(400_000.0, 800_000.0),
        );
        let reach = b
            .pause_trial(t)
            .expect("allowed, but with nothing to spend");
        let mut cameras = vec![
            Camera::new(RANK_HOST, plain(), None, WHOLE),
            paused_for(RANK_WATCHED, 10),
        ];
        allocate(b.limit(), None, Some(reach), &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 0]);
    }

    #[test]
    fn a_limit_of_nothing_with_every_camera_paused_grows_and_lets_one_back() {
        // Every camera is paused and none is the host's, so nothing holds the limit up: a cut
        // that found nothing sent leaves it at nothing.
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        let dropping = ScreenInputs {
            queue: Some(0.5),
            delivery: Some(0.5),
            ..Default::default()
        };
        b.review(t, &dropping, load(0.0, 3.0 * CAMERA as f64));
        assert_eq!(b.limit(), Some(0.0));
        let idle = ScreenInputs {
            queue: Some(0.0),
            ..Default::default()
        };
        let mut cameras: Vec<Camera> = (0..3).map(|_| paused_for(RANK_WATCHED, 0)).collect();
        let mut back = None;
        for second in 1..=30 {
            t += Duration::from_secs(1);
            b.review(t, &idle, load(0.0, 3.0 * CAMERA as f64));
            allocate(
                b.limit(),
                b.raise_ceiling(t),
                b.pause_trial(t),
                &mut cameras,
            );
            if back.is_none() && cameras.iter().any(|c| c.level > 0) {
                back = Some(second);
            }
        }
        assert!(back.is_some_and(|s| s <= 20), "{back:?}");
    }

    #[test]
    fn a_camera_sent_at_none_of_its_steps_is_counted_at_the_top_and_moved_to_it() {
        // The host's large layer thinned to its base layer while only that flowed, and the small
        // layer flows now: its steps are the small layer's and then the large one whole, and the
        // rung the host is sent at is none of them.
        let thinned = Rung {
            layer: Layer::High,
            tid: Some(0),
        };
        let off = host(simulcast(), thinned);
        assert_eq!(off.level, off.top());
        assert!(off.off_step());
        assert!(!host(simulcast(), HIGH_WHOLE).off_step());
        assert!(!host(simulcast(), WHOLE).off_step());
        let paused = Camera::new(RANK_HOST, simulcast(), Some(Duration::ZERO), thinned);
        assert!(!paused.off_step(), "nothing is sent");
    }

    #[test]
    fn a_pause_is_tried_again_only_when_nothing_stands_queued_and_the_last_trial_was_waited_out() {
        let mut b = CameraBudget::default();
        let mut t = Instant::now();
        let path = ScreenInputs {
            capacity: Some(2_000_000.0),
            ..congested()
        };
        b.review(t, &path, load(1_500_000.0, 2_000_000.0));
        assert_eq!(b.pause_trial(t), None, "congested");
        t += HOLD;
        let queued = ScreenInputs {
            queue: Some(0.1),
            delivery: Some(1.0),
            capacity: Some(2_000_000.0),
            ..Default::default()
        };
        b.review(t, &queued, load(1_500_000.0, 2_000_000.0));
        assert_eq!(b.pause_trial(t), None, "a queue stands");
        t += Duration::from_secs(1);
        let calm = ScreenInputs {
            capacity: Some(2_000_000.0),
            ..clear(1_500_000.0)
        };
        b.review(t, &calm, load(1_500_000.0, 2_000_000.0));
        let reach = b.pause_trial(t).expect("allowed");
        assert!((reach - 2_000_000.0 * REACH_SHARE).abs() < 1.0, "{reach}");
    }
}
