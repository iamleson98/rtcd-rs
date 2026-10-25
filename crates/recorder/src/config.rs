//! What the job runner passes in the environment.

use std::time::Duration;

/// The disk kept free when the recording stops on its disk limit: what is still in flight, and
/// finishing the files.
pub const DATA_HEADROOM: u64 = 512 << 20;
/// How often one person's pointer is logged while it only moves: smooth enough to watch, half the
/// rate the room relays. Updates that draw are all logged.
pub const PRESENCE_EVERY: Duration = Duration::from_millis(100);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub site_url: String,
    pub auth_token: String,
    pub recording_id: String,
    pub max_duration: Duration,
    /// The disk the recording may use (the job's volume).
    pub max_data: u64,
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

impl Config {
    pub fn from_env() -> Result<Self, ConfigError> {
        Self::from_vars(|k| std::env::var(k).ok())
    }

    /// Reads the configuration from `var`, which looks a variable up.
    pub fn from_vars(var: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let get = |k: &str| var(k).unwrap_or_default();
        let secs: u64 = get("MAX_DURATION_SEC")
            .trim()
            .parse::<i64>()
            .map_err(|_| {
                ConfigError(format!(
                    "MAX_DURATION_SEC: invalid number {:?}",
                    get("MAX_DURATION_SEC")
                ))
            })?
            .max(0) as u64;
        let max_data: i64 = get("MAX_DATA_BYTES").trim().parse().map_err(|_| {
            ConfigError(format!(
                "MAX_DATA_BYTES: invalid number {:?}",
                get("MAX_DATA_BYTES")
            ))
        })?;
        let cfg = Self {
            site_url: get("SITE_URL").trim_end_matches('/').to_owned(),
            auth_token: get("AUTH_TOKEN"),
            recording_id: get("RECORDING_ID"),
            max_duration: Duration::from_secs(secs),
            max_data: max_data.max(0) as u64,
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        let err = |m: &str| Err(ConfigError(m.to_owned()));
        let host_ok = self
            .site_url
            .split_once("://")
            .is_some_and(|(scheme, rest)| {
                (scheme == "http" || scheme == "https")
                    && !rest.split('/').next().unwrap_or("").is_empty()
            });
        if !host_ok {
            return err("SITE_URL must be an http(s) URL");
        }
        if self.auth_token.is_empty() {
            return err("AUTH_TOKEN is required");
        }
        let id = self.recording_id.as_bytes();
        if id.len() != 26
            || !id
                .iter()
                .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase())
        {
            return err("RECORDING_ID must be a 26-character id");
        }
        if self.max_duration.is_zero() {
            return err("MAX_DURATION_SEC must be positive");
        }
        if self.max_data <= 2 * DATA_HEADROOM {
            return err("MAX_DATA_BYTES must leave room to record");
        }
        Ok(())
    }

    /// How much the recording writes before it stops capturing, keeping the headroom free.
    pub fn capture_limit(&self) -> u64 {
        self.max_data - DATA_HEADROOM
    }

    /// The app's realtime endpoint (ws for http, wss for https).
    pub fn socket_url(&self) -> String {
        format!(
            "ws{}/api/v4/websocket",
            self.site_url.strip_prefix("http").unwrap_or(&self.site_url)
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "rrrrrrrrrrrrrrrrrrrrrrrrrr";

    fn vars(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let m: Vec<(String, String)> = pairs
            .iter()
            .map(|(a, b)| ((*a).into(), (*b).into()))
            .collect();
        move |k| m.iter().find(|(a, _)| a == k).map(|(_, b)| b.clone())
    }

    fn good() -> Config {
        Config {
            site_url: "http://lms".into(),
            auth_token: "t".into(),
            recording_id: ID.into(),
            max_duration: Duration::from_secs(3600),
            max_data: 2 << 30,
        }
    }

    #[test]
    fn reads_the_environment() {
        let cfg = Config::from_vars(vars(&[
            ("SITE_URL", "http://lms.internal:8065/"),
            ("AUTH_TOKEN", "token"),
            ("RECORDING_ID", ID),
            ("MAX_DURATION_SEC", "3600"),
            ("MAX_DATA_BYTES", "17179869184"),
        ]))
        .unwrap();
        assert_eq!(cfg.site_url, "http://lms.internal:8065");
        assert_eq!(cfg.max_duration, Duration::from_secs(3600));
        assert_eq!(cfg.max_data, 16 << 30);
        assert_eq!(cfg.capture_limit(), (16 << 30) - DATA_HEADROOM);
        assert_eq!(cfg.socket_url(), "ws://lms.internal:8065/api/v4/websocket");
        let tls = Config {
            site_url: "https://lms.example".into(),
            ..good()
        };
        assert_eq!(tls.socket_url(), "wss://lms.example/api/v4/websocket");
    }

    #[test]
    fn refuses_what_would_not_work() {
        let bad = |c: Config| c.validate().unwrap_err().to_string();
        assert_eq!(
            bad(Config {
                site_url: "lms".into(),
                ..good()
            }),
            "SITE_URL must be an http(s) URL"
        );
        assert_eq!(
            bad(Config {
                site_url: "ftp://lms".into(),
                ..good()
            }),
            "SITE_URL must be an http(s) URL"
        );
        assert_eq!(
            bad(Config {
                site_url: "http://".into(),
                ..good()
            }),
            "SITE_URL must be an http(s) URL"
        );
        assert_eq!(
            bad(Config {
                auth_token: String::new(),
                ..good()
            }),
            "AUTH_TOKEN is required"
        );
        assert_eq!(
            bad(Config {
                recording_id: "x".into(),
                ..good()
            }),
            "RECORDING_ID must be a 26-character id"
        );
        assert_eq!(
            bad(Config {
                recording_id: "R".repeat(26),
                ..good()
            }),
            "RECORDING_ID must be a 26-character id",
            "upper case is not an id"
        );
        assert_eq!(
            bad(Config {
                max_duration: Duration::ZERO,
                ..good()
            }),
            "MAX_DURATION_SEC must be positive"
        );
        assert_eq!(
            bad(Config {
                max_data: DATA_HEADROOM,
                ..good()
            }),
            "MAX_DATA_BYTES must leave room to record"
        );
        assert!(good().validate().is_ok());
    }

    #[test]
    fn refuses_numbers_that_are_not() {
        let base = [
            ("SITE_URL", "http://lms"),
            ("AUTH_TOKEN", "t"),
            ("RECORDING_ID", ID),
        ];
        for (dur, data) in [
            ("", "17179869184"),
            ("abc", "17179869184"),
            ("60", ""),
            ("60", "1.5"),
        ] {
            let mut v = base.to_vec();
            v.push(("MAX_DURATION_SEC", dur));
            v.push(("MAX_DATA_BYTES", data));
            assert!(Config::from_vars(vars(&v)).is_err(), "{dur:?} {data:?}");
        }
    }
}
