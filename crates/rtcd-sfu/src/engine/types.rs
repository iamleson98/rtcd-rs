//! The engine's inputs and outputs. The engine does no I/O: the caller feeds it datagrams, commands
//! and the passage of time, and collects what it wants sent and reported.

use std::net::SocketAddr;

use bytes::BytesMut;
use rtcd_proto::Message;

/// The transport a datagram travels on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Proto {
    Udp,
    /// A TCP stream carrying RFC 4571 framed packets; the framing is the I/O layer's business, so
    /// the engine sees and produces whole packets.
    Tcp,
}

impl Proto {
    /// What a packet of `payload` bytes to `peer` costs on the wire: the payload plus the IP header
    /// (20 bytes for IPv4, 40 for IPv6) and the UDP header (8) or the TCP header (20) with its
    /// RFC 4571 length (2). Egress is billed by the wire bytes, so the count a session reports
    /// carries them too.
    pub fn wire_bytes(self, payload: usize, peer: &std::net::SocketAddr) -> u64 {
        let ip = if peer.is_ipv4() { 20 } else { 40 };
        let transport = match self {
            Proto::Udp => 8,
            Proto::Tcp => 22,
        };
        (payload + ip + transport) as u64
    }
}

#[cfg(test)]
mod wire_bytes_tests {
    use super::Proto;

    #[test]
    fn a_packet_costs_its_headers_too() {
        let v4: std::net::SocketAddr = "192.0.2.1:5000".parse().unwrap();
        let v6: std::net::SocketAddr = "[2001:db8::1]:5000".parse().unwrap();
        assert_eq!(Proto::Udp.wire_bytes(1000, &v4), 1028);
        assert_eq!(Proto::Udp.wire_bytes(1000, &v6), 1048);
        assert_eq!(Proto::Tcp.wire_bytes(1000, &v4), 1042);
    }
}

impl Proto {
    pub(crate) fn from_transport(p: rtc::shared::TransportProtocol) -> Self {
        match p {
            rtc::shared::TransportProtocol::TCP => Self::Tcp,
            _ => Self::Udp,
        }
    }

    pub(crate) fn to_transport(self) -> rtc::shared::TransportProtocol {
        match self {
            Self::Udp => rtc::shared::TransportProtocol::UDP,
            Self::Tcp => rtc::shared::TransportProtocol::TCP,
        }
    }
}

/// A packet to send.
#[derive(Debug)]
pub struct Transmit {
    pub proto: Proto,
    /// The local address to send from (the socket the peer's traffic arrived on).
    pub local: SocketAddr,
    pub peer: SocketAddr,
    pub data: BytesMut,
}

/// Something asked of the engine by the service.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// Start a session.
    Join(rtcd_proto::SessionConfig),
    /// End a session.
    Leave { session_id: String },
    /// A message from a session's client: SDP, ICE, mute, screen and camera announcements.
    Signal(Message),
}

/// Why the SFU ended a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloseReason {
    /// The service asked for it.
    Left,
    /// The media connection failed or was closed by the participant.
    ConnectionLost,
    /// The participant never connected.
    ConnectTimeout,
    /// The first offer never came, or negotiation broke down.
    SignalingFailed,
    /// The SFU is shutting down.
    Shutdown,
    /// The join was refused.
    Rejected,
}

/// Something the engine tells the service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// A message for a session's client: an answer or offer, an ICE candidate, voice activity.
    Message(Message),
    /// A session ended, for whatever reason, and no longer exists.
    SessionClosed {
        group_id: String,
        session_id: String,
        reason: CloseReason,
        /// What the engine sent to the participant over the session's life, in bytes of
        /// datagrams (see `Session::bytes_sent`). `None` when it was lost with the shard that
        /// held it.
        bytes_sent: Option<u64>,
    },
}
