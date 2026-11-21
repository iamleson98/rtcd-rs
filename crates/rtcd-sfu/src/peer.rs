//! The peer connection of one session, configured as the Go rtcd configures pion's: the codecs a
//! class call needs, the header extensions simulcast and audio levels need, and the interceptors
//! for loss recovery, reports and send-side congestion control.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rtc::interceptor::{
    CongestionControlBuilder, KeyframeRequestForwarderBuilder, NackGeneratorBuilder,
    NackResponderBuilder, PacerBuilder, ReceiverReportBuilder, Registry, SenderReportBuilder, Slot,
    TwccReceiverBuilder, TwccSenderBuilder,
};
use rtc::peer_connection::configuration::RTCConfigurationBuilder;
use rtc::peer_connection::configuration::media_engine::{
    MIME_TYPE_AV1, MIME_TYPE_OPUS, MIME_TYPE_VP8, MediaEngine,
};
use rtc::peer_connection::configuration::setting_engine::SettingEngineBuilder;
use rtc::peer_connection::transport::RTCDtlsRole;
use rtc::peer_connection::{RTCPeerConnection, RTCPeerConnectionBuilder};
use rtc::rtp_transceiver::rtp_sender::{
    RTCPFeedback, RTCRtpCodec, RTCRtpCodecParameters, RTCRtpHeaderExtensionCapability, RtpCodecKind,
};
use rtc::shared::error::Result;

use crate::bwe::{Estimate, SharedGcc};
use crate::config::EngineConfig;
use crate::nack_limit::LimitedNackResponder;
use crate::red::{MIME_TYPE_RED, RED_FMTP, RED_PAYLOAD_TYPE};

pub const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
pub use crate::red::OPUS_PAYLOAD_TYPE;
pub const VP8_PAYLOAD_TYPE: u8 = 96;
pub const AV1_PAYLOAD_TYPE: u8 = 45;

/// A silent microphone sends one packet every 400 ms instead of every 20 ms with DTX, and the SFU
/// forwards each packet to every participant, so it is asked for in what the SFU offers.
const OPUS_FMTP: &str = "minptime=10;useinbandfec=1;usedtx=1";

const VIDEO_EXTENSIONS: [&str; 3] = [
    "urn:ietf:params:rtp-hdrext:sdes:mid",
    "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id",
    "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id",
];

fn feedback(typ: &str, parameter: &str) -> RTCPFeedback {
    RTCPFeedback {
        typ: typ.to_owned(),
        parameter: parameter.to_owned(),
    }
}

fn video_codec(mime: &str, payload_type: u8) -> RTCRtpCodecParameters {
    RTCRtpCodecParameters {
        rtp_codec: RTCRtpCodec {
            mime_type: mime.to_owned(),
            clock_rate: 90_000,
            channels: 0,
            sdp_fmtp_line: String::new(),
            rtcp_feedback: vec![
                feedback("goog-remb", ""),
                feedback("ccm", "fir"),
                feedback("nack", ""),
                feedback("nack", "pli"),
            ],
        },
        payload_type,
    }
}

pub(crate) fn opus_codec() -> RTCRtpCodecParameters {
    RTCRtpCodecParameters {
        rtp_codec: RTCRtpCodec {
            mime_type: MIME_TYPE_OPUS.to_owned(),
            clock_rate: 48_000,
            channels: 2,
            sdp_fmtp_line: OPUS_FMTP.to_owned(),
            rtcp_feedback: vec![],
        },
        payload_type: OPUS_PAYLOAD_TYPE,
    }
}

/// Redundant audio: a payload of Opus packets (see [`crate::red`]).
pub(crate) fn red_codec() -> RTCRtpCodecParameters {
    RTCRtpCodecParameters {
        rtp_codec: RTCRtpCodec {
            mime_type: MIME_TYPE_RED.to_owned(),
            clock_rate: 48_000,
            channels: 2,
            sdp_fmtp_line: RED_FMTP.to_owned(),
            rtcp_feedback: vec![],
        },
        payload_type: RED_PAYLOAD_TYPE,
    }
}

fn media_engine(red: bool) -> Result<MediaEngine> {
    let mut m = MediaEngine::default();
    if red {
        m.register_codec(red_codec(), RtpCodecKind::Audio)?;
    }
    m.register_codec(opus_codec(), RtpCodecKind::Audio)?;
    m.register_codec(
        video_codec(MIME_TYPE_VP8, VP8_PAYLOAD_TYPE),
        RtpCodecKind::Video,
    )?;
    m.register_codec(
        video_codec(MIME_TYPE_AV1, AV1_PAYLOAD_TYPE),
        RtpCodecKind::Video,
    )?;
    m.register_header_extension(
        RTCRtpHeaderExtensionCapability {
            uri: AUDIO_LEVEL_URI.to_owned(),
        },
        RtpCodecKind::Audio,
        None,
    )?;
    for uri in VIDEO_EXTENSIONS {
        m.register_header_extension(
            RTCRtpHeaderExtensionCapability {
                uri: uri.to_owned(),
            },
            RtpCodecKind::Video,
            None,
        )?;
    }
    Ok(m)
}

/// Packets an SRTP receiver remembers, to refuse a replay and to accept a late arrival.
const SRTP_REPLAY_WINDOW: usize = 2048;
/// The pacer sends video at this many times what it sent over the last second: room for a
/// keyframe and a rising layer, and a burst that leaves in about half the time it would take the
/// link, so [`crate::bwe`] still reads the link's rate from it.
const PACE_RATE_MULTIPLIER: f64 = 2.0;
/// The pace before anything was sent and its floor, in bits per second. A burst measures what the
/// link carries only if it leaves faster than the link takes it (see `TRAIN_MAX_DEPARTURE_SHARE` in
/// [`crate::bwe`]); a pacer slower than that never lets the estimate see the link, and the layer
/// choice, finding nothing known of it, tries the large layer again every few seconds. At 3 Mb/s, a
/// link of up to about 2.4 Mb/s is read from its bursts; a faster one is not held by the floor.
/// A floor below about 2.4 Mb/s would put each 1500 byte packet more than [`crate::bwe`]'s
/// 5 ms burst gap after the one before, so a frame would never form a burst and the link would not
/// be measured.
const PACE_MIN_BITRATE: f64 = 3_000_000.0;
/// What one burst covers at the pace rate: a few packets, so a voice packet waits for at most one.
const PACE_BURST_WINDOW: Duration = Duration::from_millis(5);

/// A new peer connection for a session, and the handle to its bandwidth estimate.
pub(crate) fn new_peer(
    cfg: &EngineConfig,
    now: Instant,
) -> Result<(RTCPeerConnection, Arc<Estimate>)> {
    let mut media = media_engine(cfg.red)?;
    let (estimator, estimate) = SharedGcc::new(
        cfg.min_bitrate as f64,
        cfg.min_bitrate as f64,
        cfg.max_bitrate as f64,
    );

    // Order in the chain is decided by the slots, not by the order they are added here.
    let mut registry = Registry::new()
        .with(
            Slot::NackResponder,
            LimitedNackResponder::new(
                NackResponderBuilder::new()
                    .with_size(cfg.nack_buffer_size)
                    // A receiver that lost packets in an outage asks for all of them when the path
                    // is back; after a second a frame they belonged to has been given up on.
                    .with_max_packet_age(Duration::from_secs(1))
                    .build(),
                cfg.egress.clone(),
            ),
        )
        .with(Slot::NackGenerator, NackGeneratorBuilder::new().build())
        .with(Slot::ReceiverReport, ReceiverReportBuilder::new().build())
        .with(Slot::SenderReport, SenderReportBuilder::new().build())
        // A subscriber's request for a keyframe is the SFU's cue to ask the publisher.
        .with(
            Slot::KeyframeRequests,
            KeyframeRequestForwarderBuilder::new().build(),
        )
        // Feedback about what the participant sends us, for their own estimator.
        .with(Slot::TwccReceiver, TwccReceiverBuilder::new().build())
        // Numbering of what we send, and the estimator that reads the feedback on it.
        .with(Slot::TwccSender, TwccSenderBuilder::new().build())
        .with(
            Slot::CongestionControl,
            CongestionControlBuilder::new(estimator)
                // Browsers send transport-wide feedback only, and only video is numbered.
                .with_twcc_only(true)
                .build(),
        );
    if cfg.pacing {
        registry = registry.with(
            Slot::Pacer,
            PacerBuilder::new()
                .with_send_rate_multiplier(PACE_RATE_MULTIPLIER)
                .with_min_bitrate(PACE_MIN_BITRATE)
                .with_burst_window(PACE_BURST_WINDOW)
                .build(),
        );
    }
    // Only video takes part in the bandwidth estimate (it picks the screen share layer). Numbering
    // audio too would add 8 bytes to every voice packet, which is about 65 bytes, in both
    // directions, and make every participant report on it.
    media.register_feedback(feedback("transport-cc", ""), RtpCodecKind::Video);
    media.register_header_extension(
        RTCRtpHeaderExtensionCapability {
            uri: rtc::sdp::extmap::TRANSPORT_CC_URI.to_owned(),
        },
        RtpCodecKind::Video,
        None,
    )?;

    let settings = SettingEngineBuilder::new()
        // The browser offers; answering as the DTLS server saves the SFU a round trip of handshake.
        .with_answering_dtls_role(RTCDtlsRole::Server)
        .with_lite(cfg.ice_lite)
        .with_sctp_enable_zero_checksum(true)
        // A retransmission arrives a round trip and a NACK interval after the packet it replaces,
        // and a screen share sends a few hundred packets a second. The library's window of 64
        // packets rejects it as a duplicate, so a hole never closes and the stream stalls. Browsers
        // use 1024; this covers a second of a large layer at a round trip of several hundred ms.
        .with_srtp_replay_protection_window(SRTP_REPLAY_WINDOW)
        .build();

    let pc = RTCPeerConnectionBuilder::new()
        .with_configuration(RTCConfigurationBuilder::new().build())
        .with_setting_engine(settings)
        .with_media_engine(media)
        .with_interceptor_registry(registry)
        .build(now)?;
    Ok((pc, estimate))
}
