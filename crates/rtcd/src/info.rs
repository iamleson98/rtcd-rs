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
                let mut sampler = rtcd_sfu::hostcpu::Sampler::default();
                let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    if let Some(reading) = sampler.sample() {
                        latest.lock().cpu_load = reading.busy;
                    }
                }
            });
        }
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
