//! The send-side bandwidth estimator given any feedback: packet trains with any timing, size,
//! loss and arrival, sequence numbers that repeat and wrap, and silences. What it publishes is
//! finite and within what it documents.
#![no_main]

use std::time::{Duration, Instant};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use rtc::interceptor::{BandwidthEstimator, PacketReport};
use rtc::rtcp::transport_feedbacks::cc_feedback_report::Ecn;
use rtcd_sfu::bwe::SharedGcc;

#[derive(Arbitrary, Debug)]
struct Report {
    ssrc: u8,
    seq: u16,
    size: u16,
    arrived: bool,
    /// Milliseconds after the start of the run that it left.
    departure: u16,
    /// Milliseconds (on the receiver's clock) that it arrived, if the feedback says so.
    arrival: Option<u32>,
}

#[derive(Arbitrary, Debug)]
struct Feedback {
    /// Milliseconds after the one before; 0 is the same instant.
    after: u16,
    reports: Vec<Report>,
}

#[derive(Arbitrary, Debug)]
struct Input {
    initial: u32,
    min: u16,
    max: u32,
    feedback: Vec<Feedback>,
}

fn finite_from_zero(name: &str, v: f64) {
    assert!(v.is_finite() && v >= 0.0, "{name} is {v}");
}

fuzz_target!(|input: Input| {
    // Bits per second, from a thousandth of a kilobit up to what a link can carry.
    let min = f64::from(input.min.max(1)) * 1000.0;
    let max = min + f64::from(input.max % 1_000_000_000) + 1000.0;
    let initial = (f64::from(input.initial) % max).max(min);
    let (mut gcc, estimate) = SharedGcc::new(initial, min, max);
    let t0 = Instant::now();
    let mut now = t0;
    for fb in input.feedback.iter().take(200) {
        now += Duration::from_millis(u64::from(fb.after));
        let reports: Vec<PacketReport> = fb
            .reports
            .iter()
            .take(200)
            .map(|r| PacketReport {
                ssrc: u32::from(r.ssrc),
                id: 0,
                rtp_sequence_number: r.seq,
                is_twcc: true,
                twcc_sequence_number: r.seq,
                size: usize::from(r.size),
                arrived: r.arrived,
                departure: t0 + Duration::from_millis(u64::from(r.departure)),
                arrival: r.arrival.map(|a| Duration::from_millis(u64::from(a))),
                ecn: Ecn::NotEct,
            })
            .collect();
        gcc.on_reports(now, &reports);

        let s = estimate.snapshot();
        let target = gcc.target_bitrate();
        assert!(
            target.is_finite() && (min..=max).contains(&target),
            "target {target} outside [{min}, {max}]"
        );
        assert_eq!(s.target, target, "the handle says what the estimator says");
        finite_from_zero("delay_based", s.delay_based);
        finite_from_zero("loss_based", s.loss_based);
        assert!(
            s.average_loss.is_finite() && (0.0..=1.0).contains(&s.average_loss),
            "average loss {}",
            s.average_loss
        );
        if let Some(d) = s.delivery {
            assert!(d.is_finite() && (0.0..=1.0).contains(&d), "delivery {d}");
        }
        for (name, v) in [
            ("queue", s.queue),
            ("acked", s.acked),
            ("capacity", s.capacity),
            ("link", s.link),
            ("rtt", s.rtt),
        ] {
            if let Some(v) = v {
                finite_from_zero(name, v);
            }
        }
        if let Some(t) = s.queue_trend {
            assert!(t.is_finite(), "queue trend {t}");
        }
        assert_eq!(s.fed_at, Some(now), "the last feedback is when it came");
    }
});
