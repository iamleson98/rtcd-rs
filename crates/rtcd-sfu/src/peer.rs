//! The peer connection of one session, configured as the Go rtcd configures pion's: the codecs a
//! class call needs, the header extensions simulcast and audio levels need, and the interceptors
//! for loss recovery, reports and send-side congestion control.

use std::sync::Arc;
use std::time::Instant;

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

pub const AUDIO_LEVEL_URI: &str = "urn:ietf:params:rtp-hdrext:ssrc-audio-level";
pub const OPUS_PAYLOAD_TYPE: u8 = 111;
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

fn media_engine() -> Result<MediaEngine> {
    let mut m = MediaEngine::default();
    m.register_codec(
        RTCRtpCodecParameters {
            rtp_codec: RTCRtpCodec {
                mime_type: MIME_TYPE_OPUS.to_owned(),
                clock_rate: 48_000,
                channels: 2,
                sdp_fmtp_line: OPUS_FMTP.to_owned(),
                rtcp_feedback: vec![],
            },
            payload_type: OPUS_PAYLOAD_TYPE,
        },
        RtpCodecKind::Audio,
    )?;
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

/// A new peer connection for a session, and the handle to its bandwidth estimate.
pub(crate) fn new_peer(
    cfg: &EngineConfig,
    now: Instant,
) -> Result<(RTCPeerConnection, Arc<Estimate>)> {
    let mut media = media_engine()?;
    let (estimator, estimate) = SharedGcc::new(
        cfg.min_bitrate as f64,
        cfg.min_bitrate as f64,
        cfg.max_bitrate as f64,
    );

    // Order in the chain is decided by the slots, not by the order they are added here.
    let mut registry = Registry::new()
        .with(
            Slot::NackResponder,
            NackResponderBuilder::new()
                .with_size(cfg.nack_buffer_size)
                .build(),
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
        registry = registry.with(Slot::Pacer, PacerBuilder::new().build());
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
        .build();

    let pc = RTCPeerConnectionBuilder::new()
        .with_configuration(RTCConfigurationBuilder::new().build())
        .with_setting_engine(settings)
        .with_media_engine(media)
        .with_interceptor_registry(registry)
        .build(now)?;
    Ok((pc, estimate))
}
