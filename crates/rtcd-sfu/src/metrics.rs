//! What the media plane reports. The service implements this over Prometheus; the plane itself only
//! calls it, so it carries no metrics dependency and tests can count calls.

use std::time::Duration;

use crate::ids::TrackType;

/// Direction of a track as seen by the SFU.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// From a participant to the SFU.
    In,
    /// From the SFU to a participant.
    Out,
}

impl Direction {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::In => "in",
            Self::Out => "out",
        }
    }
}

/// Sink for the media plane's measurements. Every method must be cheap and must not block: they are
/// called from the forwarding loop.
pub trait Metrics: Send + Sync + 'static {
    fn session_started(&self, group: &str) {
        let _ = group;
    }
    fn session_ended(&self, group: &str) {
        let _ = group;
    }
    /// A connection state change: `connected`, `disconnected`, `failed` or `closed`.
    fn connection_state(&self, state: &str) {
        let _ = state;
    }
    fn error(&self, group: &str, kind: &str) {
        let _ = (group, kind);
    }
    fn track_added(&self, group: &str, direction: Direction, mime: &str) {
        let _ = (group, direction, mime);
    }
    fn track_removed(&self, group: &str, direction: Direction, mime: &str) {
        let _ = (group, direction, mime);
    }
    fn connection_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn data_channel_open_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn signaling_lock_grab_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn signaling_lock_held_time(&self, group: &str, took: Duration) {
        let _ = (group, took);
    }
    fn client_loss_rate(&self, group: &str, v: f64) {
        let _ = (group, v);
    }
    fn client_rtt(&self, group: &str, v: f64) {
        let _ = (group, v);
    }
    fn client_jitter(&self, group: &str, v: f64) {
        let _ = (group, v);
    }
    /// RTP bytes (header and payload) sent to receivers.
    fn rtp_bytes_sent(&self, kind: TrackType, bytes: u64) {
        let _ = (kind, bytes);
    }
    /// RTP bytes (header and payload) received from participants, by track type: the uplink.
    fn rtp_bytes_received(&self, kind: TrackType, bytes: u64) {
        let _ = (kind, bytes);
    }
    /// Everything that crossed the sockets since the last report, RTCP, STUN and DTLS included:
    /// what the network bill is made of. Payload bytes only; IP and UDP headers add 28 bytes per
    /// IPv4 datagram.
    fn net_traffic(&self, traffic: NetTraffic) {
        let _ = traffic;
    }
    /// How long writing one packet to one receiver took. Reported for a sample of the packets: the
    /// clock reads would cost more than they tell if every write were timed.
    fn rtp_write_time(&self, group: &str, kind: TrackType, took: Duration) {
        let _ = (group, kind, took);
    }
}

/// Traffic on the sockets over some span, per transport.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NetTraffic {
    pub udp_in_bytes: u64,
    pub udp_in_packets: u64,
    pub udp_out_bytes: u64,
    pub udp_out_packets: u64,
    pub tcp_in_bytes: u64,
    pub tcp_in_packets: u64,
    pub tcp_out_bytes: u64,
    pub tcp_out_packets: u64,
    /// Datagrams the kernel refused to send (a full send buffer, an unreachable peer).
    pub udp_dropped_packets: u64,
    /// Datagrams a shard read that belonged to another shard's session and passed on: the traffic
    /// that steering to per-participant sockets did not (yet) keep off that detour.
    pub udp_forwarded_packets: u64,
}

impl NetTraffic {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Discards everything.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoMetrics;

impl Metrics for NoMetrics {}
