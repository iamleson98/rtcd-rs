//! What the service reports about itself: its version and the load of the machine.

use std::sync::Arc;

use parking_lot::Mutex;
use rtcd_sfu::{hostcpu, hostmem, hostnet};
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

/// How many seconds the machine's CPU and network are averaged over: a second is too short to
/// decide on (a reading of 0.6 and one of 0.8 come from the same steady load), a minute too slow
/// to follow a class starting.
const WINDOW: usize = 5;

/// What `GET /system` says about the machine and the process, apart from the media plane: the key
/// names are the ones in the response.
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct SystemInfo {
    /// How busy the machine's CPUs were over the last few seconds, from 0 (idle) to 1 (all busy);
    /// time waiting for I/O counts as idle. The Go service's key.
    pub cpu_load: f64,
    /// The same for the busiest single CPU: a core can be the limit while the average has room.
    pub cpu_busiest: f64,
    /// The share of all CPU time spent waiting for I/O (counted as idle in `cpu_load`).
    pub cpu_iowait: f64,
    /// The share of all CPU time the kernel spent on deferred work, which sending and receiving
    /// packets is (counted as busy in `cpu_load`).
    pub cpu_softirq: f64,
    /// The share of the machine's memory in use, from 0 to 1: what the kernel could not hand out
    /// without swapping or killing something (`MemAvailable`) is not in use.
    pub mem_used: f64,
    pub mem_total_bytes: u64,
    pub mem_available_bytes: u64,
    /// What this process holds in RAM, and what its control group allows it (0: no limit).
    pub rss_bytes: u64,
    pub rss_limit_bytes: u64,
    /// What the machine's network interface sends, as the interface counts it (link-layer headers
    /// included, everything on the machine counted: media, recording uploads, the rest).
    pub host_tx_bps: u64,
    pub host_tx_pps: u64,
    /// Packets per second the interface's driver dropped for want of room.
    pub host_tx_dropped_pps: u64,
}

/// The body of `GET /system`, and the node-level gauges of `/metrics`: the machine, and what the
/// media plane is doing about it. The key names are the names of the metrics without their
/// `rtcd_` prefix (and `_total` for the counter).
#[derive(Debug, Clone, Copy, Default, PartialEq, Serialize)]
pub struct SystemStatus {
    #[serde(flatten)]
    pub host: SystemInfo,
    /// Sessions running on this instance.
    pub sessions: usize,
    /// Threads serving them.
    pub shards: usize,
    /// Of those, the ones with no room left, shedding load (small video layers, fewer voices).
    pub shards_pressured: usize,
    /// Of those, the ones at their limit: they refuse new sessions.
    pub shards_saturated: usize,
    /// What the instance sends now, in bits per second, and in packets per second: every datagram
    /// it hands to its sockets, headers up to the link layer included.
    pub egress_bps: u64,
    pub egress_pps: u64,
    /// Datagrams the kernel refused to queue since the instance started reading the count.
    pub kernel_send_drops: u64,
    /// Receivers are held to the small video layers: a send budget or the node's own learned
    /// ceiling is reached.
    pub egress_squeezed: bool,
    /// The kernel is refusing datagrams now (or was a few seconds ago): recording uploads slow
    /// down.
    pub egress_refusing: bool,
    /// It went on refusing after the squeeze: calls forward their loudest voice only.
    pub egress_choked: bool,
    /// The send rate at which the kernel started refusing datagrams, as the node learned it, in
    /// bits and in packets per second (0 until it has).
    pub egress_ceiling_bps: u64,
    pub egress_ceiling_pps: u64,
}

/// Samples the machine once a second for as long as the returned handle lives.
#[derive(Debug, Clone, Default)]
pub struct SystemMonitor {
    latest: Arc<Mutex<SystemInfo>>,
}

impl SystemMonitor {
    pub fn info(&self) -> SystemInfo {
        *self.latest.lock()
    }

    /// Starts sampling (Linux only: it reads `/proc`; elsewhere the figures stay 0). The task ends
    /// with the runtime.
    pub fn spawn(&self) {
        let latest = self.latest.clone();
        tokio::spawn(async move {
            let mut cpu = hostcpu::Sampler::new(WINDOW);
            let mut net = hostnet::Sampler::new(WINDOW);
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let (cpu, mem, net) = (
                    cpu.sample(),
                    hostmem::read(),
                    net.sample(std::time::Instant::now()),
                );
                let mut info = latest.lock();
                if let Some(r) = cpu {
                    info.cpu_load = r.busy;
                    info.cpu_busiest = r.busiest;
                    info.cpu_iowait = r.iowait;
                    info.cpu_softirq = r.softirq;
                }
                if let Some(m) = mem {
                    info.mem_used = m.used();
                    info.mem_total_bytes = m.total;
                    info.mem_available_bytes = m.available;
                    info.rss_bytes = m.rss;
                    info.rss_limit_bytes = m.limit;
                }
                if let Some(n) = net {
                    info.host_tx_bps = n.tx_bps as u64;
                    info.host_tx_pps = n.tx_pps as u64;
                    info.host_tx_dropped_pps = n.tx_dropped_per_sec as u64;
                }
            }
        });
    }
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
}
