//! What the node learns about its own card: how many bits and how many packets a second it can
//! send before the kernel starts refusing them.
//!
//! Nobody can say that number in advance. A virtual network card reports a speed that is wrong or
//! missing, the hypervisor behind it limits bits and packets on its own terms (the OVH 4-vCPU video
//! node took 800 to 850 Mbit/s of a 1 Gbit/s card and 117 thousand packets a second, and not both
//! at once with small packets), and a number written in a setting is a guess that leaves a third of
//! the card unused or runs the node into the wall. So the node finds it by sending, the way a
//! congestion controller does: it starts under what a card of this class takes, raises its ceiling
//! in steps while it is pressed against it and the kernel accepts everything, and takes the rate at
//! which the kernel began to refuse as the wall, just under which it stays.
//!
//! Each of the two limits (bits, packets) has a ceiling of its own, and they are learned apart: a
//! node of voice calls meets the packet wall at a bit rate far under the card's. The estimator is
//! given one window of measurements a second by the gauge ([`Window`]) and decides nothing about
//! calls: what to do near the ceiling (refuse newcomers, shed) is the admission's, in `runtime`.
//!
//! Every rate here is the *sustained* one, the median of the last seven windows. A node sends in bursts
//! (a keyframe fanned out to a class is a second of several times the usual rate), the kernel
//! refuses during the bursts, and the windows that refuse are the high ones: a wall read from them
//! is the peak of the bursts, not the rate the node can keep up, and a ceiling closed on a burst
//! turns people away that the card would carry. Nor is a wall read from the share the kernel
//! refused: it counts calls, not datagrams (a segmented send of 64 is one), so it reads low and a
//! wall taken from it would be too high for as long as the refusals went on.
//!
//! # The rules
//!
//! - **Start.** The ceiling is [`Config::start`] until something better is known. It is also the
//!   least the node ever concludes: refusals while it sends under it are taken for the machine's
//!   (other processes share the kernel's counter), not for the card's limit.
//! - **Rise.** A limit is *pressed* when the rate is within [`PRESSED`] of its ceiling. After
//!   `wait` windows in a row pressed and without two refusing windows in a row, the ceiling rises by `step`, and never to
//!   more than [`REACH`] times the rate: it runs ahead of the node, not far ahead (not while the
//!   node holds its people down: the rate it shows is its own choice). The first
//!   steps are [`FIRST_STEP`], and the step doubles with every step that held, up to [`MAX_STEP`].
//!   A ceiling that nothing presses against does not move: there is nothing to learn from a node
//!   that is not asking.
//! - **Wall.** [`WALL_AFTER`] refusing windows in a row are a wall, not a stall (a hypervisor pause
//!   is over in a second or two): the limit being met takes the highest rate the kernel accepted
//!   whole just before, the sustained rate of the last window that refused nothing, and the ceiling
//!   is that, not less than the start. A node asked for more in one step (everyone turns a camera
//!   on) takes the rate from before the step, which it could carry, and finds the rest by probing.
//!   A wall already known is lowered by a tenth at most by one run: the rate before it may be a node
//!   coming out of a shed, which carried less than the card does.
//!   The limit being met is the one whose rate is near its ceiling; the kernel does not say which
//!   of the two it was.
//! - **Probing the wall.** A rise made soon before a wall failed: the next one waits twice as long
//!   (at most [`MAX_WAIT`]) and is [`MIN_STEP`]. One that held (the next rise came without a
//!   refusal in between) puts the wait and the step back. So a node sitting at its wall tests it
//!   less and less often, and one whose card got faster, or whose wall was a stall's, finds out.
//!
//! The link speed (`/sys/class/net/<interface>/speed`) is used for what it can say with some
//! certainty: a card of 100 Mbit/s starts lower, and no ceiling in bits exceeds what the link
//! carries. It is never used to start higher than [`Config::start`], because a virtual card says
//! 10000 for a hypervisor that gives it 1000.

/// Index of the limit in bits per second, and of the one in packets per second.
pub const BITS: usize = 0;
pub const PACKETS: usize = 1;

/// A limit is pressed when the rate is at least this share of its ceiling.
pub const PRESSED: f64 = 0.85;
/// A rise puts the ceiling at most this many times above the rate.
const REACH: f64 = 1.25;
/// The first rise, the least one after a failed rise, and the most one after many that held.
const FIRST_STEP: f64 = 0.10;
const MIN_STEP: f64 = 0.05;
const MAX_STEP: f64 = 0.25;
/// Windows pressed and clean before a rise, normally and the most after failed ones.
const BASE_WAIT: u32 = 10;
const MAX_WAIT: u32 = 600;
/// Refusing windows in a row that are a wall.
pub const WALL_AFTER: u32 = 3;
/// A rise is blamed for a wall that comes within this many windows of it.
const BLAME_FOR: u32 = 120;
/// A wall is lowered to no less than this share of the one known by a run of refusals.
const LOWEST: f64 = 0.9;
/// A limit whose rate is at least this share of its ceiling is taken to be the one that was met.
const MET: f64 = 0.8;
/// The link's own speed leaves this share for the bytes the counters do not see (preamble, frame
/// check, gap: 24 bytes a frame).
const LINK_SHARE: f64 = 0.97;
/// The node starts at this share of the link's speed when that is under the configured start.
const START_OF_LINK: f64 = 0.4;

/// What the estimator is told once.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Config {
    /// Where the node starts, and the least it concludes, in bits and packets per second.
    pub start: [f64; 2],
    /// A ceiling given by the operator, per limit (0: learn it, or not known without `learn`).
    pub fixed: [f64; 2],
    /// Whether the limits the operator did not give are learned.
    pub learn: bool,
    /// What the link carries in bits per second (0: not known).
    pub link_bps: f64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            start: [400e6, 80_000.0],
            fixed: [0.0; 2],
            learn: true,
            link_bps: 0.0,
        }
    }
}

impl Config {
    /// The start a link of `link_bps` leaves: the configured one, or 40 % of the link when that is
    /// less (the packets scale with it).
    fn started(mut self) -> Self {
        if self.link_bps > 0.0 && START_OF_LINK * self.link_bps < self.start[BITS] {
            let scale = START_OF_LINK * self.link_bps / self.start[BITS];
            self.start[BITS] *= scale;
            self.start[PACKETS] *= scale;
        }
        self
    }
}

/// One window of the gauge's measurements.
#[derive(Debug, Clone, Copy, Default)]
pub struct Window {
    /// What was handed to the sockets per second, sustained (the median of the last seven windows).
    pub rate: [f64; 2],
    /// The kernel refused enough in this window to matter.
    pub refusing: bool,
    /// Refusing windows in a row up to and including this one.
    pub run: u32,
    /// The node holds its people to less than they ask for (video on the small layers, voices
    /// cut): the rate says nothing about the card, and no ceiling is raised on it.
    pub squeezed: bool,
}

#[derive(Debug, Clone, Copy)]
struct Dim {
    ceiling: f64,
    /// What the card took when it last refused (0: never).
    wall: f64,
    /// Windows pressed and clean in a row, and how many a rise needs.
    calm: u32,
    wait: u32,
    step: f64,
    /// Whether a rise was made and not followed by another yet, and how many windows ago.
    stepped: bool,
    since_step: u32,
    /// The sustained rate of the last window that refused nothing and was not held down.
    clean: f64,
}

impl Dim {
    fn new(start: f64) -> Self {
        Self {
            ceiling: start,
            wall: 0.0,
            calm: 0,
            wait: BASE_WAIT,
            step: FIRST_STEP,
            stepped: false,
            since_step: u32::MAX,
            clean: 0.0,
        }
    }
}

/// The learned ceilings of one node.
#[derive(Debug, Clone)]
pub struct Estimator {
    cfg: Config,
    dims: [Dim; 2],
}

impl Estimator {
    pub fn new(cfg: Config) -> Self {
        let cfg = cfg.started();
        Self {
            dims: [Dim::new(cfg.start[BITS]), Dim::new(cfg.start[PACKETS])],
            cfg,
        }
    }

    /// The ceiling of a limit: the operator's, or the one learned (the start before anything is).
    pub fn ceiling(&self, limit: usize) -> f64 {
        if self.cfg.fixed[limit] > 0.0 {
            return self.cfg.fixed[limit];
        }
        if !self.cfg.learn {
            return 0.0;
        }
        let c = self.dims[limit].ceiling;
        if limit == BITS && self.cfg.link_bps > 0.0 {
            c.min(LINK_SHARE * self.cfg.link_bps)
        } else {
            c
        }
    }

    /// What the card took when it last refused (0: it has not), or the operator's ceiling.
    pub fn wall(&self, limit: usize) -> f64 {
        if self.cfg.fixed[limit] > 0.0 {
            self.cfg.fixed[limit]
        } else {
            self.dims[limit].wall
        }
    }

    /// Whether the ceiling of a limit is a fact (the operator's, or a wall the card showed) and
    /// not a probe still on its way up.
    pub fn proven(&self, limit: usize) -> bool {
        self.cfg.fixed[limit] > 0.0 || (self.cfg.learn && self.dims[limit].wall > 0.0)
    }

    /// Where the ceiling of a limit comes from: `fixed` (the operator's), `learned` (the card
    /// refused at it) or `probing` (still the start, or only raised from it).
    pub fn source(&self, limit: usize) -> &'static str {
        if self.cfg.fixed[limit] > 0.0 {
            "fixed"
        } else if !self.cfg.learn {
            ""
        } else if self.dims[limit].wall > 0.0 {
            "learned"
        } else {
            "probing"
        }
    }

    pub fn observe(&mut self, w: &Window) {
        for limit in [BITS, PACKETS] {
            let d = &mut self.dims[limit];
            if !w.refusing && !w.squeezed {
                d.clean = w.rate[limit];
            }
        }
        if w.run == WALL_AFTER {
            self.learn_wall(w);
        }
        for limit in [BITS, PACKETS] {
            if self.cfg.fixed[limit] > 0.0 || !self.cfg.learn {
                continue;
            }
            let ceiling = self.ceiling(limit);
            let start = self.cfg.start[limit];
            let d = &mut self.dims[limit];
            d.since_step = d.since_step.saturating_add(1);
            // One refusing window is a burst (a keyframe fanned out to a class overflows a send
            // queue at a rate far under the card's): it does not start the count again, two in a
            // row do.
            if w.run >= 2 || w.squeezed || w.rate[limit] < PRESSED * ceiling {
                d.calm = 0;
                continue;
            }
            d.calm += 1;
            if d.calm < d.wait {
                continue;
            }
            if d.stepped {
                d.wait = BASE_WAIT;
                d.step = (d.step * 2.0).min(MAX_STEP);
            }
            let to = (d.ceiling * (1.0 + d.step)).min(REACH * w.rate[limit]);
            d.ceiling = to.max(d.ceiling).max(start);
            d.stepped = true;
            d.since_step = 0;
            d.calm = 0;
        }
    }

    fn learn_wall(&mut self, w: &Window) {
        let share = [BITS, PACKETS].map(|l| w.rate[l] / self.ceiling(l).max(1.0));
        let most = if share[PACKETS] > share[BITS] {
            PACKETS
        } else {
            BITS
        };
        for limit in [BITS, PACKETS] {
            let met = share[limit] >= MET || (limit == most && share[1 - limit] < MET);
            if self.cfg.fixed[limit] > 0.0 || !self.cfg.learn || !met {
                continue;
            }
            let start = self.cfg.start[limit];
            let d = &mut self.dims[limit];
            // A wall already known is lowered by a tenth at most by one run of refusals: the rate
            // before it may be a node coming out of a shed, which carried less than the card does.
            let taken = if d.wall > 0.0 {
                d.clean.max(LOWEST * d.wall)
            } else {
                d.clean
            };
            if taken < start {
                continue;
            }
            d.wall = taken;
            d.ceiling = taken;
            if d.stepped && d.since_step <= BLAME_FOR {
                d.wait = (d.wait * 2).clamp(2 * BASE_WAIT, MAX_WAIT);
                d.step = MIN_STEP;
                d.stepped = false;
            }
            d.calm = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MBIT: f64 = 1e6;

    fn quiet() -> Estimator {
        Estimator::new(Config::default())
    }

    /// `secs` clean windows at `bps` and `pps`.
    fn clean(e: &mut Estimator, secs: u32, bps: f64, pps: f64) {
        for _ in 0..secs {
            e.observe(&Window {
                rate: [bps, pps],
                refusing: false,
                run: 0,
                squeezed: false,
            });
        }
    }

    /// `secs` refusing windows at `bps` and `pps`.
    fn refused(e: &mut Estimator, secs: u32, bps: f64, pps: f64) {
        for run in 1..=secs {
            e.observe(&Window {
                rate: [bps, pps],
                refusing: true,
                run,
                squeezed: false,
            });
        }
    }

    /// A wall: the node carried `bps` and `pps` cleanly, then the kernel refused for as many
    /// windows as make a wall at 5 % more.
    fn wall(e: &mut Estimator, bps: f64, pps: f64) {
        clean(e, 12, bps, pps);
        refused(e, WALL_AFTER, bps * 1.05, pps * 1.05);
    }

    #[test]
    fn a_node_starts_at_the_configured_start_and_a_quiet_one_never_leaves_it() {
        let mut e = quiet();
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT);
        assert_eq!(e.ceiling(PACKETS), 80_000.0);
        clean(&mut e, 3_600, 100.0 * MBIT, 10_000.0);
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT, "nothing presses on it");
        assert_eq!(e.source(BITS), "probing");
    }

    #[test]
    fn a_node_pressed_against_its_ceiling_raises_it_in_growing_steps() {
        let mut e = quiet();
        clean(&mut e, 9, 380.0 * MBIT, 10_000.0);
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT, "ten windows are needed");
        clean(&mut e, 1, 380.0 * MBIT, 10_000.0);
        let first = e.ceiling(BITS);
        assert!(
            (first - 440.0 * MBIT).abs() < 1.0,
            "10 %: {} Mbit/s",
            first / MBIT
        );
        // The next one needs the node to press again: 380 is 86 % of 440.
        clean(&mut e, 10, 380.0 * MBIT, 10_000.0);
        assert!(
            (e.ceiling(BITS) - 475.0 * MBIT).abs() < 1.0,
            "20 %, held to a quarter over the rate of 380: {} Mbit/s",
            e.ceiling(BITS) / MBIT
        );
        // From a rate of 470 the ceiling is held to a quarter above it.
        let mut e = quiet();
        clean(&mut e, 10, 380.0 * MBIT, 10_000.0);
        clean(&mut e, 10, 380.0 * MBIT, 10_000.0);
        clean(&mut e, 10, 470.0 * MBIT, 10_000.0);
        assert!(e.ceiling(BITS) <= 1.25 * 470.0 * MBIT + 1.0);
    }

    #[test]
    fn the_ceiling_runs_at_most_a_quarter_ahead_of_the_rate() {
        let mut e = quiet();
        // Pressed at 340 (85 % of 400): a rise of 10 % is 440, a quarter over the rate is 425.
        clean(&mut e, 10, 340.0 * MBIT, 10_000.0);
        let c = e.ceiling(BITS);
        assert!((c - 425.0 * MBIT).abs() < 1.0, "{} Mbit/s", c / MBIT);
    }

    #[test]
    fn a_wall_is_the_rate_the_kernel_last_took_whole_not_the_burst_that_failed() {
        // The windows that refuse are the ones with a burst in them: the wall is what the node
        // carried before.
        let mut e = quiet();
        clean(&mut e, 30, 500.0 * MBIT, 12_000.0);
        refused(&mut e, WALL_AFTER, 560.0 * MBIT, 13_000.0);
        let wall = e.wall(BITS) / MBIT;
        assert!((wall - 500.0).abs() < 0.01, "{wall}");
        assert!((e.ceiling(BITS) - 500.0 * MBIT).abs() < 1.0);
        assert_eq!(e.source(BITS), "learned");
    }

    #[test]
    fn a_wall_needs_three_refusing_windows_in_a_row() {
        let mut e = quiet();
        clean(&mut e, 30, 600.0 * MBIT, 20_000.0);
        let before = e.ceiling(BITS);
        refused(&mut e, 2, 900.0 * MBIT, 20_000.0);
        assert_eq!(e.wall(BITS), 0.0);
        assert_eq!(e.ceiling(BITS), before);
        clean(&mut e, 1, 600.0 * MBIT, 20_000.0);
        refused(&mut e, 2, 900.0 * MBIT, 20_000.0);
        assert_eq!(e.wall(BITS), 0.0, "a clean window starts the count again");
        clean(&mut e, 1, 600.0 * MBIT, 20_000.0);
        refused(&mut e, 3, 900.0 * MBIT, 20_000.0);
        assert!(e.wall(BITS) > 0.0);
    }

    #[test]
    fn a_wall_below_the_start_is_not_one() {
        let mut e = quiet();
        wall(&mut e, 200.0 * MBIT, 5_000.0);
        assert_eq!(e.wall(BITS), 0.0);
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT);
    }

    #[test]
    fn the_wall_is_put_to_the_limit_that_was_met() {
        let mut e = quiet();
        // 85 thousand packets of 100 bytes: 68 Mbit/s of 400, 106 % of the packet ceiling.
        wall(&mut e, 68.0 * MBIT, 85_000.0);
        assert!((e.wall(PACKETS) - 85_000.0).abs() < 1.0);
        assert_eq!(e.wall(BITS), 0.0, "the bits were far from theirs");
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT);
    }

    #[test]
    fn a_burst_that_refuses_for_a_window_does_not_stop_the_climb_but_two_do() {
        let mut e = quiet();
        for _ in 0..3 {
            clean(&mut e, 9, 390.0 * MBIT, 10_000.0);
            refused(&mut e, 1, 400.0 * MBIT, 10_000.0);
        }
        assert!(e.ceiling(BITS) > 400.0 * MBIT, "isolated bursts");
        let mut e = quiet();
        for _ in 0..3 {
            clean(&mut e, 8, 390.0 * MBIT, 10_000.0);
            refused(&mut e, 2, 400.0 * MBIT, 10_000.0);
        }
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT, "refusing twice in a row");
    }

    #[test]
    fn a_node_that_holds_its_people_down_raises_nothing() {
        let mut e = quiet();
        for _ in 0..100 {
            e.observe(&Window {
                rate: [390.0 * MBIT, 10_000.0],
                refusing: false,
                run: 0,
                squeezed: true,
            });
        }
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT);
    }

    #[test]
    fn a_step_in_demand_takes_the_rate_from_before_it_and_the_rest_is_found_by_probing() {
        let mut e = quiet();
        clean(&mut e, 30, 500.0 * MBIT, 10_000.0);
        // Everyone turns a camera on: the kernel refuses from the first window of the step.
        refused(&mut e, WALL_AFTER, 900.0 * MBIT, 18_000.0);
        assert!((e.ceiling(BITS) - 500.0 * MBIT).abs() < 1.0);
        // The card takes 850: still refusing, but the ceiling is probed up while it is kept
        // pressed and the kernel accepts what is sent.
        for _ in 0..40 {
            let rate = e.ceiling(BITS) * 0.97;
            clean(&mut e, 10, rate, 10_000.0);
        }
        assert!(e.ceiling(BITS) > 800.0 * MBIT, "{}", e.ceiling(BITS) / MBIT);
    }

    #[test]
    fn a_rise_that_ends_in_a_wall_is_tried_again_later_and_smaller() {
        let mut e = quiet();
        clean(&mut e, 10, 380.0 * MBIT, 10_000.0);
        assert!(e.ceiling(BITS) > 400.0 * MBIT, "raised");
        let ceiling = e.ceiling(BITS);
        // The kernel took 420 whole, and refused 440.
        wall(&mut e, 420.0 * MBIT, 10_000.0);
        let after = e.ceiling(BITS);
        assert!(
            (after - 420.0 * MBIT).abs() < 1.0,
            "{after} for a ceiling of {ceiling}"
        );
        // 19 pressed windows are not enough, 20 are.
        clean(&mut e, 19, 0.95 * after, 10_000.0);
        assert_eq!(e.ceiling(BITS), after);
        clean(&mut e, 1, 0.95 * after, 10_000.0);
        let up = e.ceiling(BITS) / after;
        assert!((1.04..1.06).contains(&up), "5 %: {up}");
        // It fails too: the wait doubles.
        let ceiling = e.ceiling(BITS);
        wall(&mut e, 0.97 * ceiling, 10_000.0);
        let ceiling = e.ceiling(BITS);
        clean(&mut e, 39, 0.95 * ceiling, 10_000.0);
        assert_eq!(e.ceiling(BITS), ceiling);
        clean(&mut e, 1, 0.95 * ceiling, 10_000.0);
        assert!(e.ceiling(BITS) > ceiling);
    }

    #[test]
    fn a_rise_that_held_resets_the_wait_and_grows_the_step() {
        let mut e = quiet();
        clean(&mut e, 10, 380.0 * MBIT, 10_000.0);
        wall(&mut e, 440.0 * MBIT, 10_000.0);
        let mut steps = Vec::new();
        let (mut since, mut waits) = (0, Vec::new());
        for _ in 0..200 {
            let before = e.ceiling(BITS);
            clean(&mut e, 1, before, 10_000.0);
            since += 1;
            if e.ceiling(BITS) > before {
                steps.push(e.ceiling(BITS) / before);
                waits.push(since);
                since = 0;
            }
        }
        // The first comes after the doubled wait of a failed rise and is 5 %; the second still
        // waits for it, and it is the second that shows the first held: from then on a rise comes
        // after ten windows, each bigger, up to the most (25 %).
        assert!((1.04..1.06).contains(&steps[0]), "{steps:?}");
        assert_eq!(waits[..2], [20, 20], "{waits:?}");
        assert!(waits[2..].iter().all(|&w| w == 10), "{waits:?}");
        assert!(steps[1] > steps[0] && steps[2] > steps[1], "{steps:?}");
        assert!(steps.iter().all(|&s| s <= 1.26), "{steps:?}");
    }

    #[test]
    fn an_operators_ceiling_is_never_moved() {
        let mut e = Estimator::new(Config {
            fixed: [850.0 * MBIT, 0.0],
            ..Config::default()
        });
        clean(&mut e, 600, 840.0 * MBIT, 10_000.0);
        refused(&mut e, 10, 900.0 * MBIT, 20_000.0);
        assert_eq!(e.ceiling(BITS), 850.0 * MBIT);
        assert_eq!(e.wall(BITS), 850.0 * MBIT);
        assert_eq!(e.source(BITS), "fixed");
        assert_eq!(
            e.source(PACKETS),
            "probing",
            "the packets are still learned"
        );
    }

    #[test]
    fn a_slow_link_starts_lower_and_no_ceiling_passes_the_link() {
        let mut e = Estimator::new(Config {
            link_bps: 100.0 * MBIT,
            ..Config::default()
        });
        assert_eq!(e.ceiling(BITS), 40.0 * MBIT);
        assert_eq!(
            e.ceiling(PACKETS),
            8_000.0,
            "the packets scale with the bits"
        );
        for _ in 0..50 {
            let rate = 0.95 * e.ceiling(BITS);
            clean(&mut e, 10, rate, 1_000.0);
        }
        assert!(e.ceiling(BITS) <= 97.0 * MBIT + 1.0, "{}", e.ceiling(BITS));
        // A fast link starts where it is configured to, however fast it says it is.
        let e = Estimator::new(Config {
            link_bps: 10_000.0 * MBIT,
            ..Config::default()
        });
        assert_eq!(e.ceiling(BITS), 400.0 * MBIT);
    }
}
