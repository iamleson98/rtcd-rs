//! How busy the machine's CPUs are, from `/proc/stat` (Linux; elsewhere it is not known).
//!
//! The average over all CPUs says how much capacity is left; the busiest CPU says whether one of
//! them is the limit. They differ whenever the work does not spread evenly: a media thread per
//! core is spread, but the network card's interrupts and the kernel's send work stay on one core,
//! which can sit at 100 % while the average reads 70.

use std::collections::VecDeque;

/// Idle and total jiffies of one CPU, or of all of them.
pub type Jiffies = (u64, u64);

/// What `/proc/stat` says about the CPUs at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stat {
    /// All CPUs together (the `cpu` line).
    pub all: Cpu,
    /// Each CPU (the `cpu0`, `cpu1`, ... lines).
    pub cores: Vec<Cpu>,
}

/// The jiffies one `cpu` line holds that matter here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Cpu {
    /// Time spent idle, waiting for I/O included: a CPU waiting for a disk can run another thread.
    pub idle: u64,
    /// Of that, the time spent waiting for I/O. Tools that show a CPU as busy when it is not idle
    /// count it as busy, so it is reported on its own.
    pub iowait: u64,
    pub softirq: u64,
    pub total: u64,
}

/// How busy the machine was over an interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reading {
    /// The CPUs' busy share, from 0 (idle) to 1 (all busy).
    pub busy: f64,
    /// The same for the busiest single CPU.
    pub busiest: f64,
    /// The share of all CPU time spent waiting for I/O (counted as idle in `busy`).
    pub iowait: f64,
    /// The share of all CPU time spent in the kernel's deferred work, which includes sending and
    /// receiving packets (counted as busy in `busy`).
    pub softirq: f64,
    /// How many CPUs the machine has.
    pub cpus: usize,
}

impl Reading {
    /// The idle CPU there is, in cores: what a new thread could run on.
    pub fn spare_cores(&self) -> f64 {
        (1.0 - self.busy) * self.cpus as f64
    }
}

/// Reads the machine's CPU use over the last few calls.
#[derive(Debug)]
pub struct Sampler {
    /// The readings of `/proc/stat` at the last calls, oldest first, at most `window + 1`.
    history: VecDeque<Stat>,
    window: usize,
}

impl Default for Sampler {
    fn default() -> Self {
        Self::new(1)
    }
}

impl Sampler {
    /// A sampler whose readings cover the last `window` intervals between calls (at least one):
    /// with a call a second, the last `window` seconds. A longer window is steadier, a shorter one
    /// quicker.
    pub fn new(window: usize) -> Self {
        Self {
            history: VecDeque::new(),
            window: window.max(1),
        }
    }

    /// How busy the CPUs were over the last `window` intervals; `None` until there are two calls
    /// and wherever the system does not say.
    pub fn sample(&mut self) -> Option<Reading> {
        self.observe(read()?)
    }

    fn observe(&mut self, now: Stat) -> Option<Reading> {
        self.history.push_back(now);
        while self.history.len() > self.window + 1 {
            self.history.pop_front();
        }
        reading(self.history.front()?, self.history.back()?)
    }
}

#[cfg(target_os = "linux")]
fn read() -> Option<Stat> {
    parse_stat(&std::fs::read_to_string("/proc/stat").ok()?)
}

#[cfg(not(target_os = "linux"))]
fn read() -> Option<Stat> {
    None
}

/// What happened between two readings; `None` if no time passed on the CPUs (two readings in the
/// same jiffy say nothing, and reading them as idle would be wrong).
pub fn reading(before: &Stat, after: &Stat) -> Option<Reading> {
    let all = after.all.total.checked_sub(before.all.total)?;
    if all == 0 {
        return None;
    }
    let share = |a: u64, b: u64| a.saturating_sub(b) as f64 / all as f64;
    let busiest = before
        .cores
        .iter()
        .zip(&after.cores)
        .map(|(b, a)| busy_fraction(b.jiffies(), a.jiffies()))
        .fold(0.0, f64::max);
    Some(Reading {
        busy: busy_fraction(before.all.jiffies(), after.all.jiffies()),
        busiest,
        iowait: share(after.all.iowait, before.all.iowait),
        softirq: share(after.all.softirq, before.all.softirq),
        cpus: after.cores.len(),
    })
}

impl Cpu {
    fn jiffies(&self) -> Jiffies {
        (self.idle, self.total)
    }
}

/// Reads `/proc/stat`: the `cpu` line and a line for each CPU.
pub fn parse_stat(stat: &str) -> Option<Stat> {
    let mut lines = stat.lines();
    let all = parse_cpu_line(lines.next()?)?;
    let cores = lines
        .take_while(|l| l.starts_with("cpu"))
        .filter_map(parse_cpu_line)
        .collect();
    Some(Stat { all, cores })
}

/// Reads one `cpuN` line of `/proc/stat`.
fn parse_cpu_line(line: &str) -> Option<Cpu> {
    let fields: Vec<u64> = line
        .strip_prefix("cpu")?
        .split_whitespace()
        // The first word of a per-CPU line is its number.
        .skip(usize::from(
            line.as_bytes().get(3).is_some_and(u8::is_ascii_digit),
        ))
        .filter_map(|f| f.parse().ok())
        .collect();
    if fields.len() < 5 {
        return None;
    }
    // user nice system idle iowait irq softirq steal: idle time is idle plus iowait. Guest time is
    // inside user time already.
    let at = |i: usize| fields.get(i).copied().unwrap_or(0);
    Some(Cpu {
        idle: at(3) + at(4),
        iowait: at(4),
        softirq: at(6),
        total: fields.iter().take(8).sum(),
    })
}

/// The busy share of the time between two readings.
pub fn busy_fraction(before: Jiffies, after: Jiffies) -> f64 {
    let total = after.1.saturating_sub(before.1);
    if total == 0 {
        return 0.0;
    }
    let idle = after.0.saturating_sub(before.0).min(total);
    1.0 - idle as f64 / total as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Four CPUs; the numbers are jiffies since boot: user nice system idle iowait irq softirq steal.
    fn stat(c: [[u64; 8]; 4]) -> String {
        let sum: [u64; 8] = std::array::from_fn(|i| c.iter().map(|r| r[i]).sum());
        let line = |name: &str, v: &[u64; 8]| {
            let n: Vec<String> = v.iter().map(u64::to_string).collect();
            format!("{name} {} 0 0\n", n.join(" "))
        };
        let mut out = line("cpu ", &sum);
        for (i, r) in c.iter().enumerate() {
            out += &line(&format!("cpu{i}"), r);
        }
        out + "intr 5 6 7\nctxt 8\n"
    }

    #[test]
    fn cpu_load_is_the_busy_share_of_the_interval() {
        let a = parse_stat("cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 1 2 3 4 5").unwrap();
        assert_eq!((a.all.idle, a.all.total), (800, 1000));
        let b = parse_stat("cpu  200 0 200 750 150 0 0 0 0 0").unwrap();
        let busy = busy_fraction(a.all.jiffies(), b.all.jiffies());
        assert!((busy - 2.0 / 3.0).abs() < 1e-9, "{busy}");
        assert_eq!(busy_fraction(a.all.jiffies(), a.all.jiffies()), 0.0);
        assert_eq!(parse_stat("garbage"), None);
    }

    #[test]
    fn the_busiest_core_is_told_apart_from_the_average() {
        // One core spends the whole interval working, the others are 20 % busy: the average hides
        // the core that is the limit.
        let before = parse_stat(&stat([[0; 8]; 4])).unwrap();
        let after = parse_stat(&stat([
            [100, 0, 0, 0, 0, 0, 0, 0],
            [10, 0, 10, 80, 0, 0, 0, 0],
            [10, 0, 10, 80, 0, 0, 0, 0],
            [10, 0, 10, 80, 0, 0, 0, 0],
        ]))
        .unwrap();
        let r = reading(&before, &after).unwrap();
        assert_eq!(r.cpus, 4);
        assert!((r.busy - 0.4).abs() < 1e-9, "{r:?}");
        assert!((r.busiest - 1.0).abs() < 1e-9, "{r:?}");
        assert!((r.spare_cores() - 2.4).abs() < 1e-9, "{r:?}");
    }

    #[test]
    fn io_wait_is_idle_for_spare_cores_and_reported_on_its_own() {
        let before = parse_stat(&stat([[0; 8]; 4])).unwrap();
        // Every core: 50 working, 20 idle, 30 waiting for the disk; 10 of the work is softirq.
        let core = [30, 0, 10, 20, 30, 0, 10, 0];
        let after = parse_stat(&stat([core; 4])).unwrap();
        let r = reading(&before, &after).unwrap();
        assert!((r.busy - 0.5).abs() < 1e-9, "{r:?}");
        assert!((r.iowait - 0.3).abs() < 1e-9, "{r:?}");
        assert!((r.softirq - 0.1).abs() < 1e-9, "{r:?}");
        assert!((r.spare_cores() - 2.0).abs() < 1e-9, "{r:?}");
    }

    #[test]
    fn no_time_passing_is_not_an_idle_machine() {
        let a = parse_stat(&stat([[5, 0, 5, 90, 0, 0, 0, 0]; 4])).unwrap();
        assert_eq!(reading(&a, &a), None);
        let mut sampler = Sampler::default();
        assert_eq!(
            sampler.observe(a.clone()),
            None,
            "one reading is no interval"
        );
        assert_eq!(sampler.observe(a), None);
    }

    #[test]
    fn a_longer_window_smooths_a_burst() {
        // Five seconds at 50 % busy, then one at 100 %, read once a second (100 jiffies a second
        // on each CPU).
        let seconds = [0u64, 50, 50, 50, 50, 50, 100];
        let last_reading = |window: usize| {
            let mut sampler = Sampler::new(window);
            let (mut user, mut idle) = (0, 0);
            let mut last = None;
            for busy in seconds {
                user += busy;
                idle += 100 - busy;
                let core = [user, 0, 0, idle, 0, 0, 0, 0];
                last = sampler.observe(parse_stat(&stat([core; 4])).unwrap());
            }
            last.unwrap()
        };
        assert!((last_reading(1).busy - 1.0).abs() < 1e-9);
        assert!((last_reading(5).busy - 0.6).abs() < 1e-9);
    }

    #[test]
    fn a_real_linux_stat_is_read() {
        // The head of /proc/stat of a six-CPU Linux 6 machine (ten columns, guest times last).
        let s = parse_stat(
            "cpu  3106856 10039 1252472 41942962 62754 0 88280 0 0 0\n\
cpu0 509306 0 211509 6972055 8061 0 40776 0 0 0\n\
cpu1 510417 8523 207477 6988661 11063 0 16623 0 0 0\n\
cpu2 519627 0 208260 6996534 11353 0 10548 0 0 0\n\
cpu3 522141 0 208508 6995443 10677 0 7840 0 0 0\n\
cpu4 519467 252 208464 6998809 10747 0 6584 0 0 0\n\
cpu5 525896 1263 208252 6991458 10851 0 5906 0 0 0\n\
intr 835191919 0 9566571\n",
        )
        .unwrap();
        assert_eq!(s.cores.len(), 6);
        assert_eq!(s.all.idle, 41_942_962 + 62_754);
        assert_eq!(s.all.iowait, 62_754);
        assert_eq!(s.all.softirq, 88_280);
        assert_eq!(
            s.cores[0].total,
            509_306 + 211_509 + 6_972_055 + 8_061 + 40_776
        );
    }

    #[test]
    fn the_cpus_are_the_per_cpu_lines() {
        let s =
            parse_stat("cpu  1 2 3 4 5\ncpu0 1 2 3 4 5\ncpu1 1 2 3 4 5\nintr 5\ncpufoo 1 2 3 4 5")
                .unwrap();
        assert_eq!(s.cores.len(), 2);
    }

    #[test]
    fn spare_cores_are_the_idle_share_of_the_cpus() {
        let r = Reading {
            busy: 0.6,
            busiest: 1.0,
            iowait: 0.0,
            softirq: 0.0,
            cpus: 4,
        };
        assert!((r.spare_cores() - 1.6).abs() < 1e-9);
    }
}
