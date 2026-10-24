//! Socket primitives for the rtcd media plane.
//!
//! Plain functions and small types over the operating system's sockets, with no knowledge of
//! WebRTC: batched UDP receive and send (`recvmmsg`, `sendmmsg` and UDP segmentation offload on
//! Linux, plain datagram calls elsewhere), RFC 4571 framing for media over TCP, enumeration of the
//! machine's addresses, and a STUN binding client to learn the public one.

mod ifaddrs;
mod stun;
mod tcp;
mod udp;

pub use ifaddrs::local_addresses;
pub use stun::{StunError, discover_public_ip, parse_stun_url};
pub use tcp::{FrameReader, MAX_FRAME, bind_tcp, encode_frame};
pub use udp::{MAX_DATAGRAM, Outgoing, RecvBatch, SendBatch, SendReport, bind_udp};
