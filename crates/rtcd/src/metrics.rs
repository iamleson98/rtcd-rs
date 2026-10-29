//! Prometheus metrics, under the names the Go service exports so existing dashboards and alerts
//! keep working.

use std::time::Duration;

use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder,
};
use rtcd_sfu::ids::TrackType;
use rtcd_sfu::metrics::{Direction, Metrics as SfuMetrics, NetTraffic, ReceiverQuality};

const NAMESPACE: &str = "rtcd";
const LATENCY_BUCKETS: [f64; 14] = [
    0.001, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.3, 0.4, 0.5, 0.75, 1.0,
];
const LOSS_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0,
];
const RTT_BUCKETS: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.15, 0.25, 0.4, 0.6, 1.0, 2.0,
];
/// A receiver's bandwidth estimate, bits per second.
const ESTIMATE_BUCKETS: [f64; 12] = [
    100e3, 250e3, 500e3, 750e3, 1e6, 1.5e6, 2e6, 3e6, 4e6, 6e6, 10e6, 20e6,
];
/// Items queued for a recording's writer; the queue holds 8,192.
const RECORD_QUEUE_BUCKETS: [f64; 10] = [
    0.0, 1.0, 4.0, 16.0, 64.0, 256.0, 1024.0, 2048.0, 4096.0, 8192.0,
];
/// Handshakes and signaling take longer than packet latencies: seconds, not milliseconds.
const SETUP_BUCKETS: [f64; 14] = [
    0.001, 0.005, 0.0075, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.3, 0.4, 0.5, 0.75, 1.0,
];

/// The service's metrics.
pub struct Metrics {
    registry: Registry,
    rtp_tracks: GaugeVec,
    /// One counter per track type, resolved once: a label lookup on every packet would show.
    rtp_bytes_sent: [Counter; 4],
    rtp_write_time: HistogramVec,
    rtp_bytes_received: [Counter; 4],
    /// Bytes and packets on the sockets: [udp in, udp out, tcp in, tcp out].
    net_bytes: [Counter; 4],
    net_packets: [Counter; 4],
    net_dropped: Counter,
    net_forwarded: Counter,
    rtc_sessions: GaugeVec,
    conn_states: CounterVec,
    rtc_errors: CounterVec,
    connection_time: HistogramVec,
    dc_open_time: HistogramVec,
    lock_grab_time: HistogramVec,
    lock_held_time: HistogramVec,
    client_loss: HistogramVec,
    client_rtt: HistogramVec,
    client_jitter: HistogramVec,
    ws_connections: GaugeVec,
    ws_messages: CounterVec,
    /// Feedback receivers sent: [nack, pli, fir].
    feedback: [Counter; 3],
    retransmitted_packets: Counter,
    retransmitted_bytes: Counter,
    receiver_loss: prometheus::Histogram,
    receiver_rtt: prometheus::Histogram,
    estimate: prometheus::Histogram,
    /// Seconds receivers spent on a shared screen's [small, large] layer.
    screen_layer_seconds: [Counter; 2],
    /// Keyframe requests: [sent, held back].
    keyframe_requests: [Counter; 2],
    record_dropped: Counter,
    record_queue_depth: prometheus::Histogram,
    /// Recordings being captured on this node.
    record_recordings: Gauge,
    record_files_written: Counter,
    /// Packets the writer left out because uploads were behind (see `max_pending_bytes`).
    record_writer_dropped: Counter,
    /// Bytes of closed files waiting to be uploaded.
    record_pending_bytes: Gauge,
    record_uploaded_bytes: Counter,
    record_upload_failures: Counter,
    /// Seconds the writer threads spent writing (not sleeping).
    record_writer_busy: Counter,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Metrics")
    }
}

fn gauge(r: &Registry, subsystem: &str, name: &str, help: &str, labels: &[&str]) -> GaugeVec {
    let g = GaugeVec::new(
        Opts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(subsystem),
        labels,
    )
    .expect("valid metric");
    r.register(Box::new(g.clone())).expect("unique metric");
    g
}

fn counter(r: &Registry, subsystem: &str, name: &str, help: &str, labels: &[&str]) -> CounterVec {
    let c = CounterVec::new(
        Opts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(subsystem),
        labels,
    )
    .expect("valid metric");
    r.register(Box::new(c.clone())).expect("unique metric");
    c
}

fn histogram(
    r: &Registry,
    subsystem: &str,
    name: &str,
    help: &str,
    buckets: &[f64],
    labels: &[&str],
) -> HistogramVec {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem(subsystem)
            .buckets(buckets.to_vec()),
        labels,
    )
    .expect("valid metric");
    r.register(Box::new(h.clone())).expect("unique metric");
    h
}

/// A counter of the `record` subsystem.
fn record_counter(r: &Registry, name: &str, help: &str) -> Counter {
    let c = Counter::with_opts(
        Opts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem("record"),
    )
    .expect("valid metric");
    r.register(Box::new(c.clone())).expect("unique metric");
    c
}

/// A gauge of the `record` subsystem.
fn record_gauge(r: &Registry, name: &str, help: &str) -> Gauge {
    let g = Gauge::with_opts(
        Opts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem("record"),
    )
    .expect("valid metric");
    r.register(Box::new(g.clone())).expect("unique metric");
    g
}

fn plain_counter(r: &Registry, name: &str, help: &str) -> Counter {
    let c = Counter::with_opts(Opts::new(name, help).namespace(NAMESPACE).subsystem("rtc"))
        .expect("valid metric");
    r.register(Box::new(c.clone())).expect("unique metric");
    c
}

fn plain_histogram(r: &Registry, name: &str, help: &str, buckets: &[f64]) -> prometheus::Histogram {
    let h = prometheus::Histogram::with_opts(
        HistogramOpts::new(name, help)
            .namespace(NAMESPACE)
            .subsystem("rtc")
            .buckets(buckets.to_vec()),
    )
    .expect("valid metric");
    r.register(Box::new(h.clone())).expect("unique metric");
    h
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        #[cfg(target_os = "linux")]
        {
            // Process CPU, memory and file descriptors, prefixed like the Go process collector.
            let collector = prometheus::process_collector::ProcessCollector::new(
                std::process::id() as i32,
                NAMESPACE,
            );
            let _ = registry.register(Box::new(collector));
        }
        let r = &registry;
        Self {
            rtp_tracks: gauge(
                r,
                "rtc",
                "rtp_tracks_total",
                "Total number of active RTP tracks",
                &["groupID", "direction", "type"],
            ),
            rtp_bytes_sent: {
                let family = counter(
                    r,
                    "rtc",
                    "rtp_bytes_sent_total",
                    "RTP bytes (header and payload) sent to receivers, by track type",
                    &["type"],
                );
                // Every type is reported from the start, at 0, as the Go service does.
                TrackType::ALL.map(|t| family.with_label_values(&[t.as_str()]))
            },
            rtp_bytes_received: {
                let family = counter(
                    r,
                    "rtc",
                    "rtp_bytes_received_total",
                    "RTP bytes (header and payload) received from participants, by track type",
                    &["type"],
                );
                TrackType::ALL.map(|t| family.with_label_values(&[t.as_str()]))
            },
            net_bytes: {
                let family = counter(
                    r,
                    "net",
                    "bytes_total",
                    "Bytes that crossed the media sockets (RTP, RTCP, STUN and DTLS), without IP and UDP headers",
                    &["direction", "transport"],
                );
                [("in", "udp"), ("out", "udp"), ("in", "tcp"), ("out", "tcp")]
                    .map(|(d, t)| family.with_label_values(&[d, t]))
            },
            net_packets: {
                let family = counter(
                    r,
                    "net",
                    "packets_total",
                    "Packets that crossed the media sockets",
                    &["direction", "transport"],
                );
                [("in", "udp"), ("out", "udp"), ("in", "tcp"), ("out", "tcp")]
                    .map(|(d, t)| family.with_label_values(&[d, t]))
            },
            net_dropped: {
                let c = Counter::with_opts(
                    Opts::new(
                        "dropped_packets_total",
                        "Datagrams the kernel refused to send",
                    )
                    .namespace(NAMESPACE)
                    .subsystem("net"),
                )
                .expect("valid metric");
                r.register(Box::new(c.clone())).expect("unique metric");
                c
            },
            net_forwarded: {
                let c = Counter::with_opts(
                    Opts::new(
                        "forwarded_packets_total",
                        "Datagrams one shard read and passed to the shard that owns the session",
                    )
                    .namespace(NAMESPACE)
                    .subsystem("net"),
                )
                .expect("valid metric");
                r.register(Box::new(c.clone())).expect("unique metric");
                c
            },
            rtp_write_time: histogram(
                r,
                "rtc",
                "rtp_tracks_writes_time",
                "Time taken to write to RTP tracks",
                &LATENCY_BUCKETS,
                &["groupID", "type"],
            ),
            rtc_sessions: gauge(
                r,
                "rtc",
                "sessions_total",
                "Total number of active RTC sessions",
                &["groupID"],
            ),
            conn_states: counter(
                r,
                "rtc",
                "conn_states_total",
                "Total number of RTC connection state changes",
                &["type"],
            ),
            rtc_errors: counter(
                r,
                "rtc",
                "errors_total",
                "Total number of RTC related errors",
                &["groupID", "type"],
            ),
            connection_time: histogram(
                r,
                "rtc",
                "connection_time",
                "Time taken to establish the connection",
                &SETUP_BUCKETS,
                &["groupID"],
            ),
            dc_open_time: histogram(
                r,
                "rtc",
                "dc_open_time",
                "Time taken to open the data channel",
                &SETUP_BUCKETS,
                &["groupID"],
            ),
            lock_grab_time: histogram(
                r,
                "rtc",
                "signaling_lock_grab_time",
                "Time taken to acquire the signaling lock",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            lock_held_time: histogram(
                r,
                "rtc",
                "signaling_lock_locked_time",
                "Time the signaling lock was held",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            client_loss: histogram(
                r,
                "rtc_client",
                "loss_rate",
                "Client loss rate",
                &LOSS_BUCKETS,
                &["groupID"],
            ),
            client_rtt: histogram(
                r,
                "rtc_client",
                "rtt",
                "Client round trip time",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            client_jitter: histogram(
                r,
                "rtc_client",
                "jitter",
                "Client latency jitter",
                &LATENCY_BUCKETS,
                &["groupID"],
            ),
            ws_connections: gauge(
                r,
                "ws",
                "connections_total",
                "Total number of active WebSocket sessions",
                &["clientID"],
            ),
            ws_messages: counter(
                r,
                "ws",
                "messages_total",
                "Total number of sent/received WebSocket messages",
                &["clientID", "type", "direction"],
            ),
            feedback: {
                let family = counter(
                    r,
                    "rtc",
                    "receiver_feedback_total",
                    "Feedback receivers sent about the streams they get: retransmission requests (nack), keyframe requests (pli) and full intra requests (fir)",
                    &["type"],
                );
                ["nack", "pli", "fir"].map(|t| family.with_label_values(&[t]))
            },
            retransmitted_packets: plain_counter(
                r,
                "retransmitted_packets_total",
                "Packets sent to receivers again because they asked",
            ),
            retransmitted_bytes: plain_counter(
                r,
                "retransmitted_bytes_total",
                "Bytes sent to receivers again because they asked",
            ),
            receiver_loss: plain_histogram(
                r,
                "receiver_loss_ratio",
                "Fraction of packets lost on the way to a receiver, as it reports (a sample every few seconds per receiver)",
                &LOSS_BUCKETS,
            ),
            receiver_rtt: plain_histogram(
                r,
                "receiver_rtt",
                "Round trip time to a receiver, seconds (a sample every few seconds per receiver)",
                &RTT_BUCKETS,
            ),
            estimate: plain_histogram(
                r,
                "bandwidth_estimate_bps",
                "A receiver's bandwidth estimate, bits per second (a sample every few seconds per receiver)",
                &ESTIMATE_BUCKETS,
            ),
            screen_layer_seconds: {
                let family = counter(
                    r,
                    "rtc",
                    "screen_layer_seconds_total",
                    "Seconds receivers spent on each layer of a shared screen",
                    &["layer"],
                );
                ["small", "large"].map(|l| family.with_label_values(&[l]))
            },
            keyframe_requests: {
                let family = counter(
                    r,
                    "rtc",
                    "keyframe_requests_total",
                    "Keyframes asked of senders, sent or held back because one was asked a moment ago",
                    &["result"],
                );
                ["sent", "held_back"].map(|l| family.with_label_values(&[l]))
            },
            record_dropped: {
                let c = Counter::with_opts(
                    Opts::new(
                        "dropped_packets_total",
                        "Packets of a recording left out because its queue to the writer was full",
                    )
                    .namespace(NAMESPACE)
                    .subsystem("record"),
                )
                .expect("valid metric");
                r.register(Box::new(c.clone())).expect("unique metric");
                c
            },
            record_queue_depth: {
                let h = prometheus::Histogram::with_opts(
                    HistogramOpts::new(
                        "queue_depth",
                        "Items waiting for a recording's writer (a sample a second per recording)",
                    )
                    .namespace(NAMESPACE)
                    .subsystem("record")
                    .buckets(RECORD_QUEUE_BUCKETS.to_vec()),
                )
                .expect("valid metric");
                r.register(Box::new(h.clone())).expect("unique metric");
                h
            },
            record_recordings: record_gauge(
                r,
                "recordings",
                "Recordings being captured on this node",
            ),
            record_files_written: record_counter(
                r,
                "files_written_total",
                "Files of recordings finished on this node",
            ),
            record_writer_dropped: record_counter(
                r,
                "writer_dropped_packets_total",
                "Packets of a recording left out because uploads were behind",
            ),
            record_pending_bytes: record_gauge(
                r,
                "pending_bytes",
                "Bytes of finished recording files waiting to be uploaded",
            ),
            record_uploaded_bytes: record_counter(
                r,
                "uploaded_bytes_total",
                "Bytes of recording files uploaded",
            ),
            record_upload_failures: record_counter(
                r,
                "upload_failures_total",
                "Recording files given up on after their uploads kept failing",
            ),
            record_writer_busy: record_counter(
                r,
                "writer_busy_seconds_total",
                "Seconds the recording writer threads spent writing rather than waiting",
            ),
            registry,
        }
    }

    pub fn record_writer_busy(&self, spent: Duration) {
        self.record_writer_busy.inc_by(spent.as_secs_f64());
    }

    /// A recording started or stopped being captured here.
    pub fn record_capture(&self, delta: i64) {
        self.record_recordings.add(delta as f64);
    }

    pub fn record_file_written(&self) {
        self.record_files_written.inc();
    }

    pub fn record_writer_dropped(&self, packets: u64) {
        self.record_writer_dropped.inc_by(packets as f64);
    }

    pub fn record_pending(&self, bytes: u64) {
        self.record_pending_bytes.set(bytes as f64);
    }

    pub fn record_uploaded(&self, bytes: u64) {
        self.record_uploaded_bytes.inc_by(bytes as f64);
    }

    pub fn record_upload_failed(&self) {
        self.record_upload_failures.inc();
    }

    pub fn ws_connected(&self, client: &str) {
        self.ws_connections.with_label_values(&[client]).inc();
    }

    pub fn ws_disconnected(&self, client: &str) {
        self.ws_connections.with_label_values(&[client]).dec();
    }

    pub fn ws_message(&self, client: &str, kind: &str, direction: &str) {
        self.ws_messages
            .with_label_values(&[client, kind, direction])
            .inc();
    }

    /// The metrics in the Prometheus text exposition format.
    pub fn render(&self) -> String {
        let mut out = Vec::new();
        if TextEncoder::new()
            .encode(&self.registry.gather(), &mut out)
            .is_err()
        {
            return String::new();
        }
        String::from_utf8(out).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl SfuMetrics for Metrics {
    fn session_started(&self, group: &str) {
        self.rtc_sessions.with_label_values(&[group]).inc();
    }

    fn session_ended(&self, group: &str) {
        self.rtc_sessions.with_label_values(&[group]).dec();
    }

    fn connection_state(&self, state: &str) {
        self.conn_states.with_label_values(&[state]).inc();
    }

    fn error(&self, group: &str, kind: &str) {
        self.rtc_errors.with_label_values(&[group, kind]).inc();
    }

    fn track_added(&self, group: &str, direction: Direction, mime: &str) {
        self.rtp_tracks
            .with_label_values(&[group, direction.as_str(), mime])
            .inc();
    }

    fn track_removed(&self, group: &str, direction: Direction, mime: &str) {
        self.rtp_tracks
            .with_label_values(&[group, direction.as_str(), mime])
            .dec();
    }

    fn connection_time(&self, group: &str, took: Duration) {
        self.connection_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn data_channel_open_time(&self, group: &str, took: Duration) {
        self.dc_open_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn signaling_lock_grab_time(&self, group: &str, took: Duration) {
        self.lock_grab_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn signaling_lock_held_time(&self, group: &str, took: Duration) {
        self.lock_held_time
            .with_label_values(&[group])
            .observe(took.as_secs_f64());
    }

    fn client_loss_rate(&self, group: &str, v: f64) {
        self.client_loss.with_label_values(&[group]).observe(v);
    }

    fn client_rtt(&self, group: &str, v: f64) {
        self.client_rtt.with_label_values(&[group]).observe(v);
    }

    fn client_jitter(&self, group: &str, v: f64) {
        self.client_jitter.with_label_values(&[group]).observe(v);
    }

    fn rtp_bytes_sent(&self, kind: TrackType, bytes: u64) {
        if let Some(i) = TrackType::ALL.iter().position(|t| *t == kind) {
            self.rtp_bytes_sent[i].inc_by(bytes as f64);
        }
    }

    fn rtp_bytes_received(&self, kind: TrackType, bytes: u64) {
        if let Some(i) = TrackType::ALL.iter().position(|t| *t == kind) {
            self.rtp_bytes_received[i].inc_by(bytes as f64);
        }
    }

    fn net_traffic(&self, t: NetTraffic) {
        let bytes = [
            t.udp_in_bytes,
            t.udp_out_bytes,
            t.tcp_in_bytes,
            t.tcp_out_bytes,
        ];
        let packets = [
            t.udp_in_packets,
            t.udp_out_packets,
            t.tcp_in_packets,
            t.tcp_out_packets,
        ];
        for i in 0..4 {
            if bytes[i] > 0 {
                self.net_bytes[i].inc_by(bytes[i] as f64);
                self.net_packets[i].inc_by(packets[i] as f64);
            }
        }
        if t.udp_dropped_packets > 0 {
            self.net_dropped.inc_by(t.udp_dropped_packets as f64);
        }
        if t.udp_forwarded_packets > 0 {
            self.net_forwarded.inc_by(t.udp_forwarded_packets as f64);
        }
    }

    fn receiver_quality(&self, q: ReceiverQuality) {
        for (c, n) in self.feedback.iter().zip([q.nacks, q.plis, q.firs]) {
            if n > 0 {
                c.inc_by(n as f64);
            }
        }
        if q.retransmitted_packets > 0 {
            self.retransmitted_packets
                .inc_by(q.retransmitted_packets as f64);
            self.retransmitted_bytes
                .inc_by(q.retransmitted_bytes as f64);
        }
        if let Some(loss) = q.loss {
            self.receiver_loss.observe(loss.clamp(0.0, 1.0));
        }
        if let Some(rtt) = q.rtt {
            self.receiver_rtt.observe(rtt.as_secs_f64());
        }
    }

    fn bandwidth_estimate(&self, bits_per_second: f64) {
        self.estimate.observe(bits_per_second);
    }

    fn screen_layer_time(&self, large: bool, took: Duration) {
        self.screen_layer_seconds[usize::from(large)].inc_by(took.as_secs_f64());
    }

    fn keyframe_request(&self, sent: bool) {
        self.keyframe_requests[usize::from(!sent)].inc();
    }

    fn record_dropped(&self, packets: u64) {
        self.record_dropped.inc_by(packets as f64);
    }

    fn record_queue_depth(&self, depth: u64) {
        self.record_queue_depth.observe(depth as f64);
    }

    fn rtp_write_time(&self, group: &str, kind: TrackType, took: Duration) {
        self.rtp_write_time
            .with_label_values(&[group, kind.as_str()])
            .observe(took.as_secs_f64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_names_are_the_ones_the_go_service_exports() {
        let m = Metrics::new();
        m.session_started("g");
        m.connection_state("connected");
        m.error("g", "rtp");
        m.track_added("g", Direction::In, "audio/opus");
        m.connection_time("g", Duration::from_millis(80));
        m.data_channel_open_time("g", Duration::from_millis(120));
        m.signaling_lock_grab_time("g", Duration::from_millis(2));
        m.signaling_lock_held_time("g", Duration::from_millis(40));
        m.client_loss_rate("g", 0.01);
        m.client_rtt("g", 0.05);
        m.client_jitter("g", 0.004);
        m.rtp_bytes_sent(TrackType::Voice, 1200);
        m.receiver_quality(ReceiverQuality {
            nacks: 3,
            plis: 1,
            retransmitted_packets: 3,
            retransmitted_bytes: 3300,
            loss: Some(0.02),
            rtt: Some(Duration::from_millis(40)),
            ..Default::default()
        });
        m.bandwidth_estimate(2.5e6);
        m.screen_layer_time(true, Duration::from_secs(1));
        m.keyframe_request(false);
        m.record_dropped(5);
        m.record_queue_depth(12);
        m.record_capture(1);
        m.record_file_written();
        m.record_writer_dropped(2);
        m.record_pending(4096);
        m.record_uploaded(1000);
        m.record_upload_failed();
        m.record_writer_busy(Duration::from_millis(1500));
        m.ws_connected("client");
        m.ws_message("client", "join", "in");
        let text = m.render();
        for name in [
            "rtcd_rtc_sessions_total{groupID=\"g\"} 1",
            "rtcd_rtc_conn_states_total{type=\"connected\"} 1",
            "rtcd_rtc_errors_total{groupID=\"g\",type=\"rtp\"} 1",
            "rtcd_rtc_rtp_tracks_total{direction=\"in\",groupID=\"g\",type=\"audio/opus\"} 1",
            "rtcd_rtc_connection_time_count{groupID=\"g\"} 1",
            "rtcd_rtc_dc_open_time_count",
            "rtcd_rtc_signaling_lock_grab_time_count",
            "rtcd_rtc_signaling_lock_locked_time_count",
            "rtcd_rtc_client_loss_rate_count",
            "rtcd_rtc_client_rtt_count",
            "rtcd_rtc_client_jitter_count",
            "rtcd_rtc_rtp_bytes_sent_total{type=\"voice\"} 1200",
            "rtcd_rtc_receiver_feedback_total{type=\"nack\"} 3",
            "rtcd_rtc_receiver_feedback_total{type=\"pli\"} 1",
            "rtcd_rtc_retransmitted_packets_total 3",
            "rtcd_rtc_retransmitted_bytes_total 3300",
            "rtcd_rtc_receiver_loss_ratio_count 1",
            "rtcd_rtc_receiver_rtt_count 1",
            "rtcd_rtc_bandwidth_estimate_bps_count 1",
            "rtcd_rtc_screen_layer_seconds_total{layer=\"large\"} 1",
            "rtcd_rtc_keyframe_requests_total{result=\"held_back\"} 1",
            "rtcd_record_dropped_packets_total 5",
            "rtcd_record_queue_depth_count 1",
            "rtcd_record_recordings 1",
            "rtcd_record_files_written_total 1",
            "rtcd_record_writer_dropped_packets_total 2",
            "rtcd_record_pending_bytes 4096",
            "rtcd_record_uploaded_bytes_total 1000",
            "rtcd_record_upload_failures_total 1",
            "rtcd_record_writer_busy_seconds_total 1.5",
            "rtcd_ws_connections_total{clientID=\"client\"} 1",
            "rtcd_ws_messages_total{clientID=\"client\",direction=\"in\",type=\"join\"} 1",
        ] {
            assert!(text.contains(name), "missing {name} in\n{text}");
        }
    }

    #[test]
    fn gauges_come_back_down() {
        let m = Metrics::new();
        m.session_started("g");
        m.session_ended("g");
        m.ws_connected("c");
        m.ws_disconnected("c");
        let text = m.render();
        assert!(text.contains("rtcd_rtc_sessions_total{groupID=\"g\"} 0"));
        assert!(text.contains("rtcd_ws_connections_total{clientID=\"c\"} 0"));
    }
}
