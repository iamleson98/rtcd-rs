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
    },
}
