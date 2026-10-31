//! Names the browser sees. A forwarded track is called `<type>_<sender session>_<random>`; the
//! client reads who sends it and what it is from that id (it appears as `a=msid` in the SDP), so
//! the format is part of the protocol.

use std::fmt;

/// What a forwarded track carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TrackType {
    Voice,
    /// A screen share's video.
    Screen,
    /// A screen share's audio.
    ScreenAudio,
    /// A camera.
    Video,
}

impl TrackType {
    pub const ALL: [TrackType; 4] = [Self::Voice, Self::Screen, Self::ScreenAudio, Self::Video];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Voice => "voice",
            Self::Screen => "screen",
            Self::ScreenAudio => "screen-audio",
            Self::Video => "video",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|t| t.as_str() == s)
    }

    pub fn is_video(self) -> bool {
        matches!(self, Self::Screen | Self::Video)
    }
}

impl fmt::Display for TrackType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// z-base-32, the alphabet of the Go service's ids.
const ALPHABET: &[u8; 32] = b"ybndrfg8ejkmcpqxot1uwisza345h769";

/// A new id for a track of `kind` sent by `session_id`. `entropy` should be random: 40 bits of it
/// become the suffix, which only has to tell one share or camera of a session from the next.
pub fn track_id(kind: TrackType, session_id: &str, entropy: u64) -> String {
    let mut suffix = [0u8; 8];
    for (i, c) in suffix.iter_mut().enumerate() {
        *c = ALPHABET[((entropy >> (5 * i)) & 31) as usize];
    }
    format!(
        "{kind}_{session_id}_{}",
        std::str::from_utf8(&suffix).expect("ascii")
    )
}

/// The stream a forwarded track of `kind` sent by `session_id` is in (`a=msid`): the sender's
/// session for its voice and camera, and a stream of its own for a screen share and its sound. A
/// browser keeps one video of a stream in step with the stream's audio: in the sender's stream that
/// could be the share instead of the camera, and the share is played a second late (lms-web asks
/// for it, so that what is lost on the way is sent again in time), which the voice is not to be.
pub fn stream_id(kind: TrackType, session_id: &str) -> String {
    match kind {
        TrackType::Screen | TrackType::ScreenAudio => format!("{session_id}_screen"),
        TrackType::Voice | TrackType::Video => session_id.to_owned(),
    }
}

/// Who sends a track and what it is, from its id; `None` when the id is not one of ours.
pub fn parse_track_id(id: &str) -> Option<(TrackType, &str)> {
    let mut fields = id.split('_');
    let (kind, session, suffix) = (fields.next()?, fields.next()?, fields.next()?);
    if fields.next().is_some() || suffix.is_empty() {
        return None;
    }
    Some((TrackType::parse(kind)?, session))
}

/// A small fast generator for ids and SSRCs. Nothing here is secret (SSRCs are on the wire), so a
/// seedable splitmix64 beats asking the OS for every packet stream; tests seed it for repeatability.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    pub fn seeded(seed: u64) -> Self {
        Self(seed)
    }

    pub fn from_os() -> Self {
        Self(rand::random())
    }

    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A non-zero SSRC.
    pub fn ssrc(&mut self) -> u32 {
        loop {
            let v = self.next_u64() as u32;
            if v != 0 {
                return v;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn track_ids_round_trip_through_the_parser() {
        for kind in TrackType::ALL {
            let id = track_id(kind, "sessionabc", 0xdead_beef);
            assert_eq!(parse_track_id(&id), Some((kind, "sessionabc")), "{id}");
        }
    }

    #[test]
    fn foreign_ids_are_not_ours() {
        for id in [
            "",
            "voice",
            "voice_s",
            "voice_s_x_y",
            "audio_s_x",
            "voice_s_",
            "{3f6c}",
        ] {
            assert_eq!(parse_track_id(id), None, "{id:?}");
        }
    }

    #[test]
    fn the_suffix_varies_with_the_entropy() {
        assert_ne!(
            track_id(TrackType::Voice, "s", 1),
            track_id(TrackType::Voice, "s", 2)
        );
        assert_eq!(
            track_id(TrackType::Voice, "s", 7),
            track_id(TrackType::Voice, "s", 7)
        );
    }

    #[test]
    fn ssrcs_are_never_zero() {
        let mut rng = Rng::seeded(0);
        for _ in 0..10_000 {
            assert_ne!(rng.ssrc(), 0);
        }
    }
}
