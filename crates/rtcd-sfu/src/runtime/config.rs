//! Settings of the runtime that puts the engine on real sockets.

use std::time::Duration;

use crate::config::{ConfigError, EngineConfig};

/// How the SFU runs: what it listens on and how many threads serve it.
#[derive(Debug, Clone)]
pub struct SfuConfig {
    /// The engine's settings, including the addresses to listen on.
    pub engine: EngineConfig,
    /// Threads serving sessions. A call lives on one of them; more threads carry more calls.
    pub shards: usize,
    /// Kernel receive buffer asked for each UDP socket; a burst the loop has not read yet waits
    /// in it.
    pub udp_recv_buffer: usize,
    /// Kernel send buffer asked for each UDP socket.
    pub udp_send_buffer: usize,
    /// Datagrams read per system call.
    pub recv_batch: usize,
    /// How long a new TCP connection may take to prove it belongs to a session (its first packet
    /// must be an ICE check naming one).
    pub tcp_handshake_timeout: Duration,
    /// How long a TCP connection may stay silent before it is closed.
    pub tcp_idle_timeout: Duration,
    /// Bytes queued for a TCP connection that does not read before it is closed.
    pub tcp_write_limit: usize,
}

impl Default for SfuConfig {
    fn default() -> Self {
        Self {
            engine: EngineConfig::default(),
            shards: 1,
            udp_recv_buffer: 16 * 1024 * 1024,
            udp_send_buffer: 16 * 1024 * 1024,
            recv_batch: 64,
            tcp_handshake_timeout: Duration::from_secs(5),
            tcp_idle_timeout: Duration::from_secs(60),
            tcp_write_limit: 4 * 1024 * 1024,
        }
    }
}

/// Why a runtime configuration is unusable.
#[derive(Debug, thiserror::Error)]
pub enum SfuConfigError {
    #[error(transparent)]
    Engine(#[from] ConfigError),
    #[error("at least one address to listen on is required")]
    NoAddresses,
    #[error("shards must be between 1 and 256, got {0}")]
    Shards(usize),
    #[error("recv_batch must be between 1 and 1024, got {0}")]
    RecvBatch(usize),
}

impl SfuConfig {
    pub fn validate(&self) -> Result<(), SfuConfigError> {
        self.engine.validate()?;
        if self.engine.addresses.is_empty() {
            return Err(SfuConfigError::NoAddresses);
        }
        if !(1..=256).contains(&self.shards) {
            return Err(SfuConfigError::Shards(self.shards));
        }
        if !(1..=1024).contains(&self.recv_batch) {
            return Err(SfuConfigError::RecvBatch(self.recv_batch));
        }
        Ok(())
    }
}
