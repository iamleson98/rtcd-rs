//! How busy the machine's CPUs are, from `/proc/stat` (Linux; elsewhere it is not known).

/// Idle and total jiffies of all CPUs.
pub type Jiffies = (u64, u64);

/// How busy the machine was over an interval.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Reading {
    /// The CPUs' busy share, from 0 (idle) to 1 (all busy).
    pub busy: f64,
    /// How many CPUs the machine has.
    pub cpus: usize,
}

impl Reading {
    /// The idle CPU there is, in cores: what a new thread could run on.
    pub fn spare_cores(&self) -> f64 {
        (1.0 - self.busy) * self.cpus as f64
    }
}

/// Reads the machine's CPU use between one call and the next.
#[derive(Debug, Default)]
pub struct Sampler {
    before: Option<Jiffies>,
}

impl Sampler {
    /// How busy the CPUs were since the last call; `None` on the first call and wherever the
    /// system does not say.
    pub fn sample(&mut self) -> Option<Reading> {
        let (now, cpus) = match read() {
            Some((j, cpus)) => (Some(j), cpus),
            None => (None, 0),
        };
        let before = std::mem::replace(&mut self.before, now);
        Some(Reading {
            busy: busy_fraction(before?, now?),
            cpus,
        })
    }
}

#[cfg(target_os = "linux")]
fn read() -> Option<(Jiffies, usize)> {
    let stat = std::fs::read_to_string("/proc/stat").ok()?;
    Some((parse_cpu_line(&stat)?, count_cpus(&stat)))
}

#[cfg(not(target_os = "linux"))]
fn read() -> Option<(Jiffies, usize)> {
    None
}

/// The per-CPU lines (`cpu0`, `cpu1`, ...) of `/proc/stat`.
pub fn count_cpus(stat: &str) -> usize {
    stat.lines()
        .filter(|l| {
            l.strip_prefix("cpu")
                .is_some_and(|rest| rest.starts_with(|c: char| c.is_ascii_digit()))
        })
        .count()
}

/// Reads the aggregate `cpu` line of `/proc/stat`.
pub fn parse_cpu_line(stat: &str) -> Option<Jiffies> {
    let line = stat.lines().next()?;
    let fields: Vec<u64> = line
        .strip_prefix("cpu")?
        .split_whitespace()
        .filter_map(|f| f.parse().ok())
        .collect();
    if fields.len() < 5 {
        return None;
    }
    // user nice system idle iowait irq softirq steal: idle time is idle plus iowait.
    let idle = fields[3] + fields.get(4).copied().unwrap_or(0);
    Some((idle, fields.iter().take(8).sum()))
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

    #[test]
    fn cpu_load_is_the_busy_share_of_the_interval() {
        let a = parse_cpu_line("cpu  100 0 100 700 100 0 0 0 0 0\ncpu0 1 2 3 4").unwrap();
        assert_eq!(a, (800, 1000));
        let b = parse_cpu_line("cpu  200 0 200 750 150 0 0 0 0 0").unwrap();
        assert!(
            (busy_fraction(a, b) - 2.0 / 3.0).abs() < 1e-9,
            "{}",
            busy_fraction(a, b)
        );
        assert_eq!(busy_fraction(a, a), 0.0);
        assert_eq!(parse_cpu_line("garbage"), None);
    }

    #[test]
    fn spare_cores_are_the_idle_share_of_the_cpus() {
        let stat = "cpu  1 2 3 4\ncpu0 1 2 3 4\ncpu1 1 2 3 4\ncpu2 1 2 3 4\ncpu3 1 2 3 4\nintr 5";
        assert_eq!(count_cpus(stat), 4);
        let r = Reading { busy: 0.6, cpus: 4 };
        assert!((r.spare_cores() - 1.6).abs() < 1e-9);
    }
}
