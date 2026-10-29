//! The packet the recording code reads.
//!
//! Only what writing a file needs, so that any source of RTP can feed a recording: the bot gets
//! packets from its peer connection, and rtcd hands them over from its forward path, where
//! cloning an `rtc` packet would allocate its header extensions for nothing. Building one from a
//! packet already in hand is a refcount bump on the payload and a few plain copies.

use std::time::Instant;

use bytes::Bytes;

#[derive(Debug, Clone)]
pub struct TapPacket {
    pub seq: u16,
    /// The RTP timestamp.
    pub ts: u32,
    /// Ends a video frame.
    pub marker: bool,
    pub payload: Bytes,
    /// When the packet reached us; media is placed on the recording's timeline by this.
    pub arrived: Instant,
}
