//! Holding the cameras a receiver is sent to what its path carries.
//!
//! A receiver's path that carries less than the cameras it is sent loses the excess at random, a
//! packet here and a packet there of every camera, and a frame that lost one is lost whole with
//! the frames after it up to the next keyframe: every picture breaks, where the same bits spent on
//! fewer pictures would have played. So when the path says it carries less (it queues what it is
//! sent, or drops it: see `ScreenInputs`), the cameras are given what it delivered less the screen
//! share's part, in the order of what matters: the host's camera first, then the others. A camera
//! that does not fit gives up the temporal layers it can (the frames between, from the next one:
//! a camera that has none has no such step) and then is paused for this receiver, until its
//! keyframe at the next time it fits.
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

use std::time::{Duration, Instant};

use crate::layers::{
    CAPACITY_SHARE, CLEAR_FOR, DRAIN_SECONDS, FORGET_AFTER, GROWTH_CEILING, GROWTH_PER_SECOND,
    HEADROOM, HOLD, INITIAL_BACKOFF, KEYFRAME_GRACE, LEAST_SHARE, LOSS_ROOM, MAX_BACKOFF,
    QUEUE_CRITICAL, QUEUE_HIGH, QUEUE_PERSISTS, QUEUE_SHRINKING, QUEUE_SURGING, ScreenInputs,
    Steps, TRIAL,
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
/// and the next step is tried instead.
const CEILING_TRUST: Duration = Duration::from_secs(60);

/// Who is given room first: the host's camera, then those of the voices heard now, then the
/// others the receiver watches, then those it asked for small.
pub const RANK_HOST: u8 = 0;
pub const RANK_LOUD: u8 = 1;
pub const RANK_WATCHED: u8 = 2;
pub const RANK_SMALL: u8 = 3;

/// One camera a receiver is sent, and what it can be sent at.
#[derive(Debug, Clone, Copy)]
pub struct Camera {
    /// What comes first when there is not room for all (`RANK_*`, the lowest first).
    pub rank: u8,
    pub steps: Steps,
    /// What the receiver is sent now: 0 is nothing (paused), `n` is the `n`th of `steps`.
    pub level: usize,
}

impl Camera {
    /// A camera as the receiver is sent it: `paused`, or up to temporal layer `limit` (`None`: all).
    pub fn new(rank: u8, steps: Steps, paused: bool, limit: Option<u8>) -> Self {
        let level = if paused {
            0
        } else {
            steps
                .iter()
                .position(|(tid, _)| tid == limit)
                .map_or(steps.count(), |i| i + 1)
        };
        Self { rank, steps, level }
    }

    /// The highest temporal layer sent at `level` (`None`: every frame).
    pub fn limit(&self, level: usize) -> Option<u8> {
        self.steps
            .iter()
            .nth(level.saturating_sub(1))
            .and_then(|(tid, _)| tid)
    }

    /// Bits per second `level` costs.
    fn cost(&self, level: usize) -> f64 {
        level
            .checked_sub(1)
            .and_then(|i| self.steps.iter().nth(i))
            .map_or(0.0, |(_, rate)| rate)
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
/// first and then the others by rank and in the order they come in. Of the cameras that are not at the top,
/// one at most (the first in that order) is taken up, and only when `raise` allows it (`Some`:
/// the most the cameras may cost then) and everything sent now and the step fit [`RAISE_MARGIN`]
/// times over in `limit`: rank says who goes up first and who is cut first, never who takes the
/// place of a camera that flows. One that no longer fits goes down at once.
pub fn allocate(limit: Option<f64>, mut raise: Option<f64>, cameras: &mut [Camera]) {
    let limit = limit.unwrap_or(f64::INFINITY);
    let mut order: Vec<usize> = (0..cameras.len()).collect();
    order.sort_by_key(|&i| cameras[i].rank);
    let sent: f64 = cameras.iter().map(|c| c.cost(c.level)).sum();
    let mut given = 0.0;
    for i in order {
        let c = &mut cameras[i];
        let before = c.level;
        let mut level = c.top();
        while level > 0 {
            let fits = if level > before {
                // Taking a camera up adds to everything sent now, whatever its rank: it never
                // displaces a camera that flows.
                let total = sent + c.cost(level) - c.cost(before);
                raise.is_some_and(|most| total <= most)
                    && total * RAISE_MARGIN <= limit
                    && given + c.cost(level) <= limit
            } else {
                given + c.cost(level) <= limit
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
    /// When a step may be tried again, and the wait after the next failure.
    raise_after: Option<Instant>,
    backoff: Duration,
    /// Since when the cameras have been held without trouble.
    calm_since: Option<Instant>,
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
            raise_after: None,
            backoff: INITIAL_BACKOFF,
            calm_since: None,
        }
    }
}

impl CameraBudget {
    pub fn limit(&self) -> Option<f64> {
        self.limit
    }

    /// Whether the path carries less than the receiver is sent (see `ScreenLevel`, whose reading
    /// of the queue this is, less the screen share's own keyframes).
    fn congested(&mut self, now: Instant, path: &ScreenInputs, room: bool) -> bool {
        let Some(q) = path.queue.filter(|&q| q >= QUEUE_HIGH) else {
            self.queued_since = None;
            return path.dropping() && !room;
        };
        let since = *self.queued_since.get_or_insert(now);
        let trend = path.queue_trend.unwrap_or(0.0);
        let surging = trend >= QUEUE_SURGING && self.grace_until.is_none_or(|t| now >= t);
        path.dropping()
            || q >= QUEUE_CRITICAL
            || surging
            || (now.saturating_duration_since(since) >= QUEUE_PERSISTS && trend >= QUEUE_SHRINKING)
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
        let room = path
            .capacity
            .is_some_and(|c| c * CAPACITY_SHARE >= (load.sent + path.other) * LOSS_ROOM);
        if self.congested(now, path, room) {
            self.clear_since = None;
            self.calm_since = None;
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
                let cut = carried.map_or(load.sent * HEADROOM, |c| c * share).max(0.0);
                self.limit = Some(self.limit.map_or(cut, |l| l.min(cut)));
                self.hold_until = Some(now + HOLD);
                self.cut_at = Some(now);
            }
        } else {
            // Cameras that move more than they did (a still student starts to gesture) cost more
            // without the path having said anything: the limit follows what is sent while it is
            // not congested, so nothing is lowered then.
            if let Some(limit) = self.limit.as_mut() {
                *limit = limit.max(load.sent);
            }
            if path.clear() {
                self.grow(now, path, load, elapsed);
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
    fn grow(&mut self, now: Instant, path: &ScreenInputs, load: Load, elapsed: Duration) {
        let Some(limit) = self.limit else { return };
        let since = *self.clear_since.get_or_insert(now);
        if self.hold_until.is_none_or(|t| now >= t) {
            let seen = path.acked.map_or(0.0, |a| a - path.other).max(load.sent);
            let grown = limit * (1.0 + GROWTH_PER_SECOND * elapsed.as_secs_f64());
            // Enough to take the cheapest camera step up, with the margin that takes.
            let above = (load.sent + load.next) * RAISE_MARGIN;
            // A narrowest link known to carry more is trusted at once.
            let link = path
                .capacity
                .map_or(0.0, |c| c * CAPACITY_SHARE - path.other);
            self.limit = Some(grown.min((GROWTH_CEILING * seen).max(above)).max(link));
        }
        let calm = *self.calm_since.get_or_insert(now);
        if now.saturating_duration_since(calm) >= FORGET_AFTER
            && now.saturating_duration_since(since) >= FORGET_AFTER
            && load.sent >= load.wanted
        {
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

    fn plain() -> Steps {
        Steps::of(
            LayerRates {
                total: Some(CAMERA),
                ..Default::default()
            },
            CAMERA,
        )
    }

    /// A camera sent as a browser's encoder does: the base layer two fifths of the bits and the
    /// second one fifth.
    fn layered() -> Steps {
        Steps::of(
            LayerRates {
                total: Some(CAMERA),
                temporal: Some([Some(160_000), Some(80_000)]),
                keyframe_bits: None,
            },
            CAMERA,
        )
    }

    fn class(students: usize, steps: Steps) -> Vec<Camera> {
        // The host is last on purpose: the order cameras come in does not matter to it.
        (0..students)
            .map(|_| Camera::new(RANK_WATCHED, steps, false, None))
            .chain([Camera::new(RANK_HOST, steps, false, None)])
            .collect()
    }

    fn levels(cameras: &[Camera]) -> Vec<usize> {
        cameras.iter().map(|c| c.level).collect()
    }

    #[test]
    fn without_a_limit_every_camera_is_sent_whole() {
        let mut cameras = class(7, plain());
        allocate(None, None, &mut cameras);
        assert!(cameras.iter().all(|c| c.level == c.top()));
    }

    #[test]
    fn the_host_keeps_its_camera_and_the_last_students_wait() {
        let mut cameras = class(7, plain());
        // Room for three of eight cameras.
        allocate(Some(3.0 * CAMERA as f64), None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 1, 0, 0, 0, 0, 0, 1]);
        assert!(Load::of(&cameras).sent <= 3.0 * CAMERA as f64);
    }

    #[test]
    fn a_camera_with_temporal_layers_gives_up_frames_before_it_is_paused() {
        let mut cameras = class(3, layered());
        // Two cameras and a bit more: the host whole, the first student whole, the second down to
        // its base and second layers (three fifths of its bits), the third has nothing left.
        allocate(Some(1_040_000.0), None, &mut cameras);
        assert_eq!(levels(&cameras), vec![3, 2, 0, 3]);
        assert_eq!(cameras[1].limit(2), Some(1));
        // A little less, and the second one is down to its base layer.
        allocate(Some(960_000.0), None, &mut cameras);
        assert_eq!(levels(&cameras), vec![3, 1, 0, 3]);
        assert_eq!(cameras[1].limit(1), Some(0));
    }

    #[test]
    fn what_goes_down_does_not_come_back_without_room_and_a_clear_path() {
        let mut cameras = class(3, plain());
        allocate(Some(2.0 * CAMERA as f64), None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 0, 0, 1]);
        // Room for all, but the path has not been clear long enough: nothing is taken up.
        allocate(Some(8.0 * CAMERA as f64), None, &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 0, 0, 1]);
        // Clear: one is taken up, where it and the two sent fit a quarter over.
        allocate(Some(4.0 * CAMERA as f64), Some(f64::INFINITY), &mut cameras);
        assert_eq!(levels(&cameras), vec![1, 1, 0, 1]);
    }

    #[test]
    fn a_camera_is_not_taken_up_past_what_the_narrowest_link_carries() {
        let mut cameras = class(3, plain());
        allocate(Some(2.0 * CAMERA as f64), None, &mut cameras);
        // Room in the limit for all four, but the link carries three of them.
        allocate(
            Some(8.0 * CAMERA as f64),
            Some(3.0 * CAMERA as f64),
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 1, 0, 1]);
        allocate(
            Some(8.0 * CAMERA as f64),
            Some(3.0 * CAMERA as f64),
            &mut cameras,
        );
        assert_eq!(levels(&cameras), vec![1, 1, 0, 1], "and no fourth");
    }

    #[test]
    fn one_camera_at_most_is_taken_up_at_a_time() {
        let mut cameras = class(7, plain());
        allocate(Some(1.0 * CAMERA as f64), None, &mut cameras);
        allocate(
            Some(80.0 * CAMERA as f64),
            Some(f64::INFINITY),
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
                    allocate(Some(middle * swing), Some(f64::INFINITY), &mut cameras);
                    let now = levels(&cameras);
                    changes += usize::from(now != before);
                    before = now;
                }
                assert!(changes <= 2, "{middle}: {changes} changes, {before:?}");
            }
        }
    }

    fn congested() -> ScreenInputs {
        ScreenInputs {
            queue: Some(0.0),
            delivery: Some(0.75),
            acked: Some(2_300_000.0),
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
    fn the_screen_share_is_given_its_part_first() {
        let mut b = CameraBudget::default();
        let path = ScreenInputs {
            other: 1_000_000.0,
            ..congested()
        };
        b.review(Instant::now(), &path, load(3_300_000.0, 3_300_000.0));
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
        for second in 1..30 {
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
        assert!(steps <= 5, "{steps} steps in three minutes");
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
    fn the_narrowest_links_rate_holds_a_camera_back_for_a_minute_after_a_cut() {
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
        t += Duration::from_secs(60);
        b.review(t, &path, load(2_000_000.0, 3_300_000.0));
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

    /// A path that carries `link` bits a second, as the cameras sent now find it, and the budget
    /// with the cameras it has been holding.
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
                    ..Default::default()
                }
            };
            self.budget.review(self.t, &path, load);
            let before = levels(&self.cameras);
            allocate(
                self.budget.limit(),
                self.budget.raise_ceiling(self.t),
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
            allocate(sim.budget.limit(), None, &mut sim.cameras);
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
        let busy = Steps::of(
            LayerRates {
                total: Some(3 * CAMERA),
                ..Default::default()
            },
            CAMERA,
        );
        for c in sim.cameras.iter_mut().filter(|c| c.level > 0) {
            c.steps = busy;
        }
        let load = Load::of(&sim.cameras);
        sim.budget.review(sim.t, &clear(load.sent), load);
        allocate(sim.budget.limit(), None, &mut sim.cameras);
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
        allocate(Some(1_700_000.0), None, &mut cameras);
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
            allocate(Some(1_700_000.0), Some(f64::INFINITY), &mut cameras);
            assert_eq!(levels(&cameras), flowing, "turn {turn}");
        }
    }

    #[test]
    fn who_is_given_room_first_goes_by_rank() {
        let mut cameras = vec![
            Camera::new(RANK_SMALL, plain(), false, None),
            Camera::new(RANK_WATCHED, plain(), false, None),
            Camera::new(RANK_LOUD, plain(), false, None),
            Camera::new(RANK_HOST, plain(), false, None),
        ];
        allocate(Some(2.0 * CAMERA as f64), None, &mut cameras);
        assert_eq!(levels(&cameras), vec![0, 0, 1, 1]);
        allocate(Some(4.0 * CAMERA as f64), Some(f64::INFINITY), &mut cameras);
        assert_eq!(levels(&cameras), vec![0, 1, 1, 1]);
    }
}
