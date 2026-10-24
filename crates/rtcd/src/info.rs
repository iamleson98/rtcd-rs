//! What the service reports about itself: its version and the load of the machine.

use std::sync::Arc;

use parking_lot::Mutex;
use serde::Serialize;

/// The body of `GET /version`. The key names are the Go service's, which clients decode.
#[derive(Debug, Clone, Serialize)]
pub struct VersionInfo {
    #[serde(rename = "buildDate")]
    pub build_date: String,
    #[serde(rename = "buildVersion")]
    pub build_version: String,
    #[serde(rename = "buildHash")]
    pub build_hash: String,
    /// The compiler, under the Go service's key for its runtime version.
    #[serde(rename = "goVersion")]
    pub go_version: String,
    #[serde(rename = "goOS")]
    pub go_os: String,
    #[serde(rename = "goArch")]
    pub go_arch: String,
}

impl VersionInfo {
    pub fn current() -> Self {
        Self {
            build_date: option_env!("RTCD_BUILD_DATE")
                .unwrap_or_default()
                .to_owned(),
            build_version: option_env!("RTCD_BUILD_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_owned(),
            build_hash: option_env!("RTCD_BUILD_HASH")
                .unwrap_or_default()
                .to_owned(),
            go_version: format!(
                "rust-{}",
                option_env!("RTCD_RUSTC_VERSION").unwrap_or("unknown")
            ),
            go_os: std::env::consts::OS.to_owned(),
            go_arch: std::env::consts::ARCH.to_owned(),
        }
    }
}

/// The body of `GET /system`.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct SystemInfo {
    /// How busy the machine's CPUs were over the last second, from 0 (idle) to 1 (all busy).
    pub cpu_load: f64,
}

/// Samples the CPU load once a second for as long as the returned handle lives.
#[derive(Debug, Clone, Default)]
pub struct SystemMonitor {
    latest: Arc<Mutex<SystemInfo>>,
}

impl SystemMonitor {
    pub fn info(&self) -> SystemInfo {
        *self.latest.lock()
    }

    /// Starts sampling (Linux only: it reads `/proc/stat`). The task ends with the runtime.
    pub fn spawn(&self) {
        #[cfg(target_os = "linux")]
        {
            let latest = self.latest.clone();
            tokio::spawn(async move {
                let mut previous = read_cpu();
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let now = read_cpu();
                    if let (Some(a), Some(b)) = (previous, now) {
                        latest.lock().cpu_load = busy_fraction(a, b);
                    }
                    previous = now;
                }
            });
        }
    }
}

/// Idle and total jiffies of all CPUs.
#[cfg(target_os = "linux")]
fn read_cpu() -> Option<(u64, u64)> {
    parse_cpu_line(&std::fs::read_to_string("/proc/stat").ok()?)
}

#[cfg(any(target_os = "linux", test))]
fn parse_cpu_line(stat: &str) -> Option<(u64, u64)> {
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

#[cfg(any(target_os = "linux", test))]
fn busy_fraction(before: (u64, u64), after: (u64, u64)) -> f64 {
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
    fn the_version_has_the_keys_clients_decode() {
        let json = serde_json::to_value(VersionInfo::current()).unwrap();
        for key in [
            "buildDate",
            "buildVersion",
            "buildHash",
            "goVersion",
            "goOS",
            "goArch",
        ] {
            assert!(json.get(key).is_some(), "{key}");
        }
        assert!(!json["buildVersion"].as_str().unwrap().is_empty());
    }

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
}
