//! Settings of the media plane.

use std::net::{IpAddr, SocketAddr};
use std::time::Duration;

/// An address the SFU listens on and the candidates it offers for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalAddress {
    /// The interface address the sockets are bound to.
    pub bind: IpAddr,
    /// The address browsers are told to use, when it differs from `bind` (a NAT'd or load
    /// balanced deployment). `None` advertises `bind` itself.
    pub public: Option<IpAddr>,
    /// The UDP port bound.
    pub udp_port: u16,
    /// The port advertised for UDP, when it differs from `udp_port`.
    pub advertised_udp_port: Option<u16>,
    /// The TCP port bound, if TCP is offered.
    pub tcp_port: Option<u16>,
    /// The port advertised for TCP, when it differs from `tcp_port`.
    pub advertised_tcp_port: Option<u16>,
    /// Whether browsers are offered this address as a candidate. Several local addresses can map to
    /// one public address; all are listened on, but it is offered once.
    pub advertise: bool,
}

impl LocalAddress {
    pub fn advertised_ip(&self) -> IpAddr {
        self.public.unwrap_or(self.bind)
    }

    pub fn advertised_udp(&self) -> SocketAddr {
        SocketAddr::new(
            self.advertised_ip(),
            self.advertised_udp_port.unwrap_or(self.udp_port),
        )
    }

    pub fn advertised_tcp(&self) -> Option<SocketAddr> {
        let port = self.advertised_tcp_port.or(self.tcp_port)?;
        Some(SocketAddr::new(self.advertised_ip(), port))
    }
}

/// How the media plane behaves. The defaults are those of the Go rtcd.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Where to listen and what to advertise.
    pub addresses: Vec<LocalAddress>,
    /// Packets each video sender remembers for retransmission (a power of two).
    pub nack_buffer_size: u16,
    /// How long a new session may take to send its first offer.
    pub signaling_timeout: Duration,
    /// How long an offer to a participant may wait for its answer.
    pub answer_timeout: Duration,
    /// How long a participant may hold the signaling lock before the SFU takes it back.
    pub lock_max_hold: Duration,
    /// How long a new session may take to connect before it is dropped.
    pub connect_timeout: Duration,
    /// How long a connected session may go without any packet from its participant before it is
    /// dropped. A browser sends something every few seconds, even a muted one with its camera off
    /// (the ICE consent check), so silence means the participant is gone.
    pub idle_timeout: Duration,
    /// The lowest and highest send rate the estimator reports per participant, in bits per second.
    pub min_bitrate: u64,
    pub max_bitrate: u64,
    /// Whether the SFU is an ICE-lite agent (it answers checks, never sends them).
    pub ice_lite: bool,
    /// Whether to pace video sends to each participant's estimated rate. Off matches the Go rtcd.
    pub pacing: bool,
    /// How many sessions one shard serves at most; joins beyond are refused.
    pub max_sessions: usize,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            addresses: Vec::new(),
            nack_buffer_size: 256,
            signaling_timeout: Duration::from_secs(10),
            answer_timeout: Duration::from_secs(10),
            lock_max_hold: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(30),
            // Half the small screen layer to one and a half times the large one: the range in which
            // choosing between the layers means something.
            min_bitrate: 250_000,
            max_bitrate: 3_750_000,
            ice_lite: true,
            pacing: false,
            max_sessions: 10_000,
        }
    }
}

/// Why a configuration is unusable.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ConfigError {
    #[error("nack_buffer_size must be a power of two between 32 and 8192, got {0}")]
    NackBufferSize(u16),
    #[error("min_bitrate must be positive and below max_bitrate")]
    BitrateRange,
}

impl EngineConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let n = self.nack_buffer_size;
        if !(32..=8192).contains(&n) || !n.is_power_of_two() {
            return Err(ConfigError::NackBufferSize(n));
        }
        if self.min_bitrate == 0 || self.min_bitrate >= self.max_bitrate {
            return Err(ConfigError::BitrateRange);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        assert_eq!(EngineConfig::default().validate(), Ok(()));
    }

    #[test]
    fn nack_buffers_must_be_powers_of_two_in_range() {
        for (n, ok) in [
            (16, false),
            (32, true),
            (100, false),
            (256, true),
            (8192, true),
            (16384, false),
        ] {
            let cfg = EngineConfig {
                nack_buffer_size: n,
                ..Default::default()
            };
            assert_eq!(cfg.validate().is_ok(), ok, "{n}");
        }
    }

    #[test]
    fn advertised_addresses_prefer_the_public_one() {
        let a = LocalAddress {
            bind: "10.0.0.5".parse().unwrap(),
            public: Some("203.0.113.9".parse().unwrap()),
            udp_port: 8443,
            advertised_udp_port: Some(30443),
            tcp_port: Some(8443),
            advertised_tcp_port: None,
            advertise: true,
        };
        let addr = |s: &str| s.parse::<SocketAddr>().unwrap();
        assert_eq!(a.advertised_udp(), addr("203.0.113.9:30443"));
        assert_eq!(a.advertised_tcp(), Some(addr("203.0.113.9:8443")));
        let plain = LocalAddress {
            public: None,
            advertised_udp_port: None,
            tcp_port: None,
            ..a
        };
        assert_eq!(plain.advertised_udp(), addr("10.0.0.5:8443"));
        assert_eq!(plain.advertised_tcp(), None);
    }
}
