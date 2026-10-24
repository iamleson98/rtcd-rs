//! The framing of signaling on the data channel.
//!
//! A frame is a msgpack unsigned integer naming the type, then, for the types that have one, a
//! msgpack payload. SDP travels zlib-compressed because offers listing every media line of a big
//! call outgrow what the websocket path carried.

use std::collections::BTreeMap;
use std::io::{Read, Write};

use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use rmpv::Value;

use crate::ProtoError;
use crate::pack::{as_map, as_string, lookup, read_value};

/// Largest SDP accepted after inflating; a few hundred media lines come to well under a megabyte,
/// and the cap keeps a tiny frame from expanding without bound.
pub const MAX_SDP_BYTES: u64 = 4 * 1024 * 1024;

/// Who sends a track and what it is, by the mid of the transceiver carrying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackInfo {
    /// `voice`, `screen`, `screen-audio` or `video`.
    pub kind: String,
    /// The session id of the sender.
    pub sender_id: String,
}

/// A frame on the data channel.
#[derive(Debug, Clone, PartialEq)]
pub enum DcMessage {
    Ping,
    Pong,
    /// A JSON session description, inflated.
    Sdp(Vec<u8>),
    /// Client report: fraction of packets lost.
    LossRate(f64),
    /// Client report: round trip time, in seconds.
    RoundTripTime(f64),
    /// Client report: jitter, in seconds.
    Jitter(f64),
    /// `None` asks for the signaling lock; `Some(granted)` answers a request.
    Lock(Option<bool>),
    Unlock,
    /// Which mid carries which track, sent with every offer.
    MediaMap(BTreeMap<String, TrackInfo>),
}

impl DcMessage {
    fn wire_type(&self) -> u8 {
        match self {
            Self::Ping => 1,
            Self::Pong => 2,
            Self::Sdp(_) => 3,
            Self::LossRate(_) => 4,
            Self::RoundTripTime(_) => 5,
            Self::Jitter(_) => 6,
            Self::Lock(_) => 7,
            Self::Unlock => 8,
            Self::MediaMap(_) => 9,
        }
    }

    /// Encodes the frame.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        // The Go encoder writes the type as a fixed-width uint8 (0xcc NN), and old clients decode that form.
        out.push(0xcc);
        out.push(self.wire_type());
        match self {
            Self::Ping | Self::Pong | Self::Unlock | Self::Lock(None) => {}
            Self::Lock(Some(granted)) => {
                rmp::encode::write_bool(&mut out, *granted).expect("vec write")
            }
            Self::LossRate(v) | Self::RoundTripTime(v) | Self::Jitter(v) => {
                rmp::encode::write_f64(&mut out, *v).expect("vec write");
            }
            Self::Sdp(json) => {
                let mut z =
                    ZlibEncoder::new(Vec::with_capacity(json.len() / 2), Compression::default());
                z.write_all(json).expect("vec write");
                rmp::encode::write_bin(&mut out, &z.finish().expect("vec write"))
                    .expect("vec write");
            }
            Self::MediaMap(map) => {
                rmp::encode::write_map_len(&mut out, map.len() as u32).expect("vec write");
                for (mid, info) in map {
                    rmp::encode::write_str(&mut out, mid).expect("vec write");
                    rmp::encode::write_map_len(&mut out, 2).expect("vec write");
                    rmp::encode::write_str(&mut out, "type").expect("vec write");
                    rmp::encode::write_str(&mut out, &info.kind).expect("vec write");
                    rmp::encode::write_str(&mut out, "sender_id").expect("vec write");
                    rmp::encode::write_str(&mut out, &info.sender_id).expect("vec write");
                }
            }
        }
        out
    }

    /// Decodes a frame.
    pub fn decode(frame: &[u8]) -> Result<Self, ProtoError> {
        let (kind, rest) = read_value(frame)?;
        let kind = kind.as_u64().ok_or(ProtoError::Type {
            field: "dc type",
            expected: "unsigned integer",
        })?;
        let payload = || read_value(rest).map(|(v, _)| v);
        let float = || {
            payload()?.as_f64().ok_or(ProtoError::Type {
                field: "dc payload",
                expected: "number",
            })
        };
        Ok(match kind {
            1 => Self::Ping,
            2 => Self::Pong,
            3 => {
                let compressed = match payload()? {
                    Value::Binary(b) => b,
                    Value::String(s) => s.into_bytes(),
                    _ => {
                        return Err(ProtoError::Type {
                            field: "sdp",
                            expected: "bytes",
                        });
                    }
                };
                let mut json = Vec::new();
                ZlibDecoder::new(compressed.as_slice())
                    .take(MAX_SDP_BYTES + 1)
                    .read_to_end(&mut json)
                    .map_err(|e| ProtoError::Sdp(e.to_string()))?;
                if json.len() as u64 > MAX_SDP_BYTES {
                    return Err(ProtoError::Sdp(format!(
                        "larger than {MAX_SDP_BYTES} bytes"
                    )));
                }
                Self::Sdp(json)
            }
            4 => Self::LossRate(float()?),
            5 => Self::RoundTripTime(float()?),
            6 => Self::Jitter(float()?),
            // A request carries no payload; an answer carries the verdict.
            7 => Self::Lock(read_value(rest).ok().and_then(|(v, _)| v.as_bool())),
            8 => Self::Unlock,
            9 => {
                let value = payload()?;
                let mut map = BTreeMap::new();
                for (mid, info) in as_map(&value, "media map")? {
                    let info = as_map(info, "track info")?;
                    let text = |key: &'static str| {
                        lookup(info, key).map_or(Ok(String::new()), |v| as_string(v, key))
                    };
                    map.insert(
                        as_string(mid, "mid")?,
                        TrackInfo {
                            kind: text("type")?,
                            sender_id: text("sender_id")?,
                        },
                    );
                }
                Self::MediaMap(map)
            }
            other => return Err(ProtoError::UnknownDcMessage(other)),
        })
    }
}
