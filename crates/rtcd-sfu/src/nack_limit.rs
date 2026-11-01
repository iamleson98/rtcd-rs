//! The NACK responder, answering only what the node can afford (see
//! [`Gauge::allow_retransmits`]).

use std::sync::Arc;
use std::time::Instant;

use rtc::interceptor::NackResponderInterceptor;
use rtc::interceptor::{Interceptor, Packet, StreamInfo, TaggedPacket};
use rtc::rtcp::transport_feedbacks::transport_layer_nack::TransportLayerNack;
use rtc::sansio::Protocol;
use rtc::shared::error::Error;

use crate::egress::Gauge;

/// A responder that leaves a request unanswered when the node's retransmissions are used up.
pub(crate) struct LimitedNackResponder {
    inner: NackResponderInterceptor,
    node: Arc<Gauge>,
}

impl LimitedNackResponder {
    pub fn new(inner: NackResponderInterceptor, node: Arc<Gauge>) -> Self {
        Self { inner, node }
    }
}

/// How many packets a request asks for.
fn requested(nack: &TransportLayerNack) -> u64 {
    nack.nacks
        .iter()
        .map(|pair| 1 + u64::from(pair.lost_packets.count_ones()))
        .sum()
}

impl Protocol<TaggedPacket, TaggedPacket, ()> for LimitedNackResponder {
    type Rout = TaggedPacket;
    type Wout = TaggedPacket;
    type Eout = ();
    type Error = Error;
    type Time = Instant;

    fn handle_read(&mut self, mut msg: TaggedPacket) -> Result<(), Self::Error> {
        if let Packet::Rtcp(packets) = &mut msg.message.packet {
            let (node, now) = (&self.node, msg.now);
            // What is left of the compound packet is still handed on: the report in it is read
            // by the interceptors beyond.
            packets.retain(|p| {
                p.as_any()
                    .downcast_ref::<TransportLayerNack>()
                    .is_none_or(|nack| node.allow_retransmits(requested(nack), now))
            });
        }
        self.inner.handle_read(msg)
    }

    fn poll_read(&mut self) -> Option<Self::Rout> {
        self.inner.poll_read()
    }

    fn handle_write(&mut self, msg: TaggedPacket) -> Result<(), Self::Error> {
        self.inner.handle_write(msg)
    }

    fn poll_write(&mut self) -> Option<Self::Wout> {
        self.inner.poll_write()
    }

    fn handle_timeout(&mut self, now: Self::Time) -> Result<(), Self::Error> {
        self.inner.handle_timeout(now)
    }

    fn poll_timeout(&mut self) -> Option<Self::Time> {
        self.inner.poll_timeout()
    }
}

impl Interceptor for LimitedNackResponder {
    fn bind_local_stream(&mut self, info: &StreamInfo) {
        self.inner.bind_local_stream(info);
    }

    fn unbind_local_stream(&mut self, info: &StreamInfo) {
        self.inner.unbind_local_stream(info);
    }

    fn bind_remote_stream(&mut self, info: &StreamInfo) {
        self.inner.bind_remote_stream(info);
    }

    fn unbind_remote_stream(&mut self, info: &StreamInfo) {
        self.inner.unbind_remote_stream(info);
    }
}

#[cfg(test)]
mod tests {
    use rtc::interceptor::{AttributedPacket, NackResponderBuilder};
    use rtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication;
    use rtc::rtcp::transport_feedbacks::transport_layer_nack::NackPair;
    use rtc::shared::{TransportContext, TransportMessage};

    use super::*;

    fn feedback(now: Instant) -> TaggedPacket {
        let nack = TransportLayerNack {
            sender_ssrc: 1,
            media_ssrc: 2,
            // 1 + 16 packets.
            nacks: vec![NackPair {
                packet_id: 100,
                lost_packets: 0xffff,
            }],
        };
        let rtcp: Vec<Box<dyn rtc::rtcp::Packet>> = vec![
            Box::new(nack),
            Box::new(PictureLossIndication {
                sender_ssrc: 1,
                media_ssrc: 2,
            }),
        ];
        TransportMessage {
            now,
            transport: TransportContext::default(),
            message: AttributedPacket::new(Packet::Rtcp(rtcp)),
        }
    }

    fn left(mut r: LimitedNackResponder) -> (usize, usize) {
        let out = r.poll_read().expect("the feedback is handed on");
        let Packet::Rtcp(packets) = out.message.packet else {
            panic!("not RTCP");
        };
        let nacks = packets
            .iter()
            .filter(|p| p.as_any().downcast_ref::<TransportLayerNack>().is_some())
            .count();
        (nacks, packets.len() - nacks)
    }

    #[test]
    fn a_request_is_counted_in_packets() {
        let Packet::Rtcp(p) = feedback(Instant::now()).message.packet else {
            unreachable!()
        };
        let nack = p[0].as_any().downcast_ref::<TransportLayerNack>().unwrap();
        assert_eq!(requested(nack), 17);
    }

    #[test]
    fn a_request_the_node_can_afford_is_handed_on_with_the_rest_of_the_feedback() {
        let gauge = Arc::new(Gauge::default());
        let now = Instant::now();
        let mut r = LimitedNackResponder::new(NackResponderBuilder::new().build(), gauge.clone());
        r.handle_read(feedback(now)).unwrap();
        assert_eq!(left(r), (1, 1));
        assert_eq!(gauge.retransmits_limited(), 0);
    }

    #[test]
    fn a_request_the_node_cannot_afford_is_dropped_and_the_rest_of_the_feedback_kept() {
        let gauge = Arc::new(Gauge::default());
        let now = Instant::now();
        // Use the second's allowance up.
        while gauge.allow_retransmits(100, now) {}
        let mut r = LimitedNackResponder::new(NackResponderBuilder::new().build(), gauge.clone());
        let before = gauge.retransmits_limited();
        r.handle_read(feedback(now)).unwrap();
        assert_eq!(left(r), (0, 1), "the picture loss indication stays");
        assert_eq!(gauge.retransmits_limited(), before + 17);
    }
}
