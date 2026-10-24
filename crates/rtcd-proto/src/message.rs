use rmpv::Value;

use crate::ProtoError;
use crate::pack::{as_bytes, as_map, as_string, lookup};

/// What a [`Message`] asks for or reports. The numbers are the wire values of the Go service and
/// must not change.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MessageType {
    /// A trickled ICE candidate.
    Ice = 1,
    /// An SDP offer or answer.
    Sdp = 2,
    /// The participant muted their microphone.
    Mute = 3,
    /// The participant unmuted their microphone.
    Unmute = 4,
    /// The participant started sharing their screen; the data names the stream.
    ScreenOn = 5,
    /// The participant stopped sharing their screen.
    ScreenOff = 6,
    /// Voice activity started (sent by the SFU).
    VoiceOn = 7,
    /// Voice activity stopped (sent by the SFU).
    VoiceOff = 8,
    /// The participant turned their camera on; the data names the stream.
    VideoOn = 9,
    /// The participant turned their camera off.
    VideoOff = 10,
    /// Which cameras the participant wants to receive, and at which size.
    VideoSubscribe = 11,
}

impl MessageType {
    /// The type for a wire value.
    pub fn from_wire(n: i64) -> Result<Self, ProtoError> {
        Ok(match n {
            1 => Self::Ice,
            2 => Self::Sdp,
            3 => Self::Mute,
            4 => Self::Unmute,
            5 => Self::ScreenOn,
            6 => Self::ScreenOff,
            7 => Self::VoiceOn,
            8 => Self::VoiceOff,
            9 => Self::VideoOn,
            10 => Self::VideoOff,
            11 => Self::VideoSubscribe,
            other => return Err(ProtoError::UnknownMessageType(other)),
        })
    }
}

/// A message about one session, in either direction between the service and the SFU (`rtc.Message`
/// in the Go service). Encoded as a msgpack map in the field order below, `data` left out when empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub group_id: String,
    pub user_id: String,
    pub session_id: String,
    pub call_id: String,
    pub kind: MessageType,
    /// The payload: JSON for SDP, ICE and the stream announcements, empty for the rest.
    pub data: Vec<u8>,
}

impl Message {
    /// Appends the msgpack encoding of the message to `out`.
    pub(crate) fn encode(&self, out: &mut Vec<u8>) {
        // Writing to a Vec cannot fail.
        rmp::encode::write_map_len(out, if self.data.is_empty() { 5 } else { 6 })
            .expect("vec write");
        for (key, value) in [
            ("group_id", &self.group_id),
            ("user_id", &self.user_id),
            ("session_id", &self.session_id),
            ("call_id", &self.call_id),
        ] {
            rmp::encode::write_str(out, key).expect("vec write");
            rmp::encode::write_str(out, value).expect("vec write");
        }
        rmp::encode::write_str(out, "type").expect("vec write");
        rmp::encode::write_uint(out, self.kind as u64).expect("vec write");
        if !self.data.is_empty() {
            rmp::encode::write_str(out, "data").expect("vec write");
            rmp::encode::write_bin(out, &self.data).expect("vec write");
        }
    }

    /// Reads a message from its msgpack value. Missing fields are empty, as with the Go decoder.
    pub(crate) fn decode(value: &Value) -> Result<Self, ProtoError> {
        let map = as_map(value, "rtc message")?;
        let text = |key: &'static str| -> Result<String, ProtoError> {
            lookup(map, key).map_or(Ok(String::new()), |v| as_string(v, key))
        };
        let kind = lookup(map, "type")
            .and_then(|v| v.as_i64())
            .ok_or(ProtoError::Type {
                field: "type",
                expected: "integer",
            })?;
        Ok(Self {
            group_id: text("group_id")?,
            user_id: text("user_id")?,
            session_id: text("session_id")?,
            call_id: text("call_id")?,
            kind: MessageType::from_wire(kind)?,
            data: lookup(map, "data").map_or(Ok(Vec::new()), |v| as_bytes(v, "data"))?,
        })
    }
}
