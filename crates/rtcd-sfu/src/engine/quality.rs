//! What the media plane learns about how well receivers are served: the feedback they send, the
//! packets sent again, the loss and delay they report, and the bandwidth estimate. Read from the
//! connection's statistics every few seconds, per session with video, as metrics.

use std::time::{Duration, Instant};

use rtc::statistics::StatsSelector;

use super::Engine;
use super::session::SessionKey;
use crate::metrics::ReceiverQuality;

/// How often a session's statistics are read: the snapshot allocates, and the figures are rates
/// that need no finer resolution.
const EVERY: Duration = Duration::from_secs(5);

impl Engine {
    /// Reports what changed in a session's statistics since the last time, if it is time.
    pub(super) fn report_quality(&mut self, key: SessionKey, now: Instant) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if s.quality
            .at
            .is_some_and(|t| now.saturating_duration_since(t) < EVERY)
        {
            return;
        }
        s.quality.at = Some(now);
        let report = s.pc.get_stats(now, StatsSelector::None);
        let (mut nacks, mut plis, mut firs, mut rtx_packets, mut rtx_bytes) = (0u64, 0, 0, 0, 0);
        for o in report.outbound_rtp_streams() {
            nacks += u64::from(o.nack_count);
            plis += u64::from(o.pli_count);
            firs += u64::from(o.fir_count);
            rtx_packets += o.retransmitted_packets_sent;
            rtx_bytes += o.retransmitted_bytes_sent;
        }
        let (mut loss, mut rtt, mut n) = (0.0, 0.0, 0u32);
        for r in report.iter().filter_map(|e| match e {
            rtc::statistics::report::RTCStatsReportEntry::RemoteInboundRtp(r) => Some(r),
            _ => None,
        }) {
            if r.round_trip_time_measurements > 0 {
                loss += r.fraction_lost;
                rtt += r.round_trip_time;
                n += 1;
            }
        }
        let m = &mut s.quality;
        let quality = ReceiverQuality {
            nacks: nacks.saturating_sub(m.nacks),
            plis: plis.saturating_sub(m.plis),
            firs: firs.saturating_sub(m.firs),
            retransmitted_packets: rtx_packets.saturating_sub(m.retransmitted_packets),
            retransmitted_bytes: rtx_bytes.saturating_sub(m.retransmitted_bytes),
            loss: (n > 0).then(|| loss / f64::from(n)),
            rtt: (n > 0).then(|| Duration::from_secs_f64((rtt / f64::from(n)).max(0.0))),
        };
        (m.nacks, m.plis, m.firs) = (nacks, plis, firs);
        (m.retransmitted_packets, m.retransmitted_bytes) = (rtx_packets, rtx_bytes);
        let estimate = s.estimate.snapshot().target;
        self.metrics.receiver_quality(quality);
        if estimate > 0.0 {
            self.metrics.bandwidth_estimate(estimate);
        }
    }
}
