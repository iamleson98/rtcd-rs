//! How much memory the machine and this process use, from `/proc` and the control group
//! (Linux; elsewhere it is not known).
//!
//! A machine runs out of memory differently from CPU: nothing slows down first, a process is
//! killed. The figure to admit more work by is what the kernel could still hand out without
//! swapping or killing (`MemAvailable`: free memory plus the cache it can drop), not what is free.

/// Memory, in bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Memory {
    /// All the machine has.
    pub total: u64,
    /// What a new allocation could get without the kernel killing anything.
    pub available: u64,
    /// What this process holds in RAM.
    pub rss: u64,
    /// What this process's control group may hold (0: no limit).
    pub limit: u64,
}

impl Memory {
    /// The share of the machine's memory that is in use, from 0 to 1.
    pub fn used(&self) -> f64 {
        if self.total == 0 {
            return 0.0;
        }
        1.0 - self.available.min(self.total) as f64 / self.total as f64
    }
}

/// Reads the machine's and the process's memory; `None` where the system does not say.
pub fn read() -> Option<Memory> {
    #[cfg(target_os = "linux")]
    {
        let (total, available) = parse_meminfo(&std::fs::read_to_string("/proc/meminfo").ok()?)?;
        let rss = std::fs::read_to_string("/proc/self/status")
            .ok()
            .and_then(|s| parse_rss(&s))
            .unwrap_or(0);
        let limit = [
            "/sys/fs/cgroup/memory.max",
            "/sys/fs/cgroup/memory/memory.limit_in_bytes",
        ]
        .iter()
        .find_map(|p| std::fs::read_to_string(p).ok())
        .map_or(0, |s| parse_limit(&s, total));
        Some(Memory {
            total,
            available,
            rss,
            limit,
        })
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

/// `MemTotal` and `MemAvailable` of `/proc/meminfo`, in bytes.
pub fn parse_meminfo(meminfo: &str) -> Option<(u64, u64)> {
    let kib = |key: &str| -> Option<u64> {
        let line = meminfo.lines().find_map(|l| l.strip_prefix(key))?;
        Some(
            line.strip_prefix(':')?
                .split_whitespace()
                .next()?
                .parse::<u64>()
                .ok()?
                * 1024,
        )
    };
    Some((kib("MemTotal")?, kib("MemAvailable")?))
}

/// `VmRSS` of `/proc/<pid>/status`, in bytes.
pub fn parse_rss(status: &str) -> Option<u64> {
    let line = status.lines().find_map(|l| l.strip_prefix("VmRSS:"))?;
    Some(line.split_whitespace().next()?.parse::<u64>().ok()? * 1024)
}

/// A control group's memory limit as its file says it: a number, or `max` (version 2) or a number
/// near the largest there is (version 1) for none. 0 when there is none, or it is more than the
/// machine has.
pub fn parse_limit(text: &str, machine: u64) -> u64 {
    match text.trim().parse::<u64>() {
        Ok(limit) if limit < machine => limit,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MEMINFO: &str = "MemTotal:        8000000 kB\nMemFree:          500000 kB\nMemAvailable:    2000000 kB\nBuffers:          100 kB\n";

    #[test]
    fn the_machines_memory_in_use_is_what_is_not_available() {
        let (total, available) = parse_meminfo(MEMINFO).unwrap();
        assert_eq!((total, available), (8_000_000 * 1024, 2_000_000 * 1024));
        let m = Memory {
            total,
            available,
            ..Memory::default()
        };
        assert!((m.used() - 0.75).abs() < 1e-9, "{}", m.used());
        assert_eq!(Memory::default().used(), 0.0);
    }

    #[test]
    fn a_meminfo_without_the_figures_is_not_read() {
        assert_eq!(parse_meminfo("MemTotal: 1 kB\n"), None);
        assert_eq!(parse_meminfo("MemAvailable: 1 kB\n"), None);
        assert_eq!(parse_meminfo("garbage"), None);
    }

    #[test]
    fn the_process_resident_set_is_vmrss() {
        let status = "Name:\trtcd\nVmPeak:\t  900 kB\nVmRSS:\t  614400 kB\nThreads:\t9\n";
        assert_eq!(parse_rss(status), Some(614_400 * 1024));
        assert_eq!(parse_rss("Name: x\n"), None);
    }

    #[test]
    fn a_limit_is_a_number_below_the_machines_memory() {
        let machine = 8 << 30;
        assert_eq!(parse_limit("3221225472\n", machine), 3 << 30);
        assert_eq!(
            parse_limit("max\n", machine),
            0,
            "cgroup v2 without a limit"
        );
        assert_eq!(
            parse_limit("9223372036854771712\n", machine),
            0,
            "cgroup v1 without a limit"
        );
        assert_eq!(parse_limit("", machine), 0);
    }
}
