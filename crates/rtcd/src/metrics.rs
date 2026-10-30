//! Prometheus metrics, under the names the Go service exports so existing dashboards and alerts
//! keep working.

use std::time::Duration;

use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, HistogramOpts, HistogramVec, Opts, Registry,
    TextEncoder,
};
use std::sync::atomic::{AtomicU64, Ordering};

use rtcd_sfu::ids::TrackType;
use rtcd_sfu::metrics::{Direction, Metrics as SfuMetrics, NetTraffic, ReceiverQuality};

use crate::info::SystemStatus;

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

/// The node-level gauges, set from `GET /system`'s figures whenever the metrics are read: the
/// machine, and what the media plane does about it.
struct Node {
    cpu_load: Gauge,
    cpu_busiest: Gauge,
    cpu_iowait: Gauge,
    cpu_softirq: Gauge,
    mem_used: Gauge,
    mem_total: Gauge,
    mem_available: Gauge,
    rss_limit: Gauge,
    tx_bps: Gauge,
    tx_pps: Gauge,
    tx_dropped_pps: Gauge,
    sessions: Gauge,
    shards: Gauge,
    shards_pressured: Gauge,
    shards_saturated: Gauge,
    egress_bps: Gauge,
    egress_pps: Gauge,
    egress_ceiling_bps: Gauge,
    egress_ceiling_pps: Gauge,
    egress_adapt: Gauge,
    egress_squeezed: Gauge,
    egress_refusing: Gauge,
    egress_choked: Gauge,
    kernel_send_drops: Counter,
    /// What `kernel_send_drops` has counted so far (a counter is only ever added to).
    kernel_send_drops_seen: AtomicU64,
}

/// A gauge with no subsystem: `rtcd_<name>`.
fn node_gauge(r: &Registry, name: &str, help: &str) -> Gauge {
    let g = Gauge::with_opts(Opts::new(name, help).namespace(NAMESPACE)).expect("valid metric");
    r.register(Box::new(g.clone())).expect("unique metric");
    g
}

impl Node {
    fn new(r: &Registry) -> Self {
        let g = |name, help| node_gauge(r, name, help);
        Self {
            cpu_load: g(
                "host_cpu_load",
                "Share of the machine's CPU time that was busy over the last few seconds (0 to 1; waiting for I/O counts as idle)",
            ),
            cpu_busiest: g(
                "host_cpu_busiest",
                "The same for the busiest single CPU: a core can be the limit while the average has room",
            ),
            cpu_iowait: g(
                "host_cpu_iowait",
                "Share of all CPU time spent waiting for I/O",
            ),
            cpu_softirq: g(
                "host_cpu_softirq",
                "Share of all CPU time the kernel spent on deferred work, which includes sending and receiving packets",
            ),
            mem_used: g(
                "host_mem_used",
                "Share of the machine's memory in use (0 to 1; what the kernel could hand out without swapping is not in use)",
            ),
            mem_total: g("host_mem_total_bytes", "Memory the machine has"),
            mem_available: g(
                "host_mem_available_bytes",
                "Memory the kernel could hand out without swapping or killing anything",
            ),
            rss_limit: g(
                "rss_limit_bytes",
                "What this process's control group may hold in memory (0: no limit); its use is rtcd_process_resident_memory_bytes",
            ),
            tx_bps: g(
                "host_tx_bps",
                "What the default network interface sends, in bits per second, link headers included, everything on the machine counted",
            ),
            tx_pps: g(
                "host_tx_pps",
                "What the default network interface sends, in packets per second",
            ),
            tx_dropped_pps: g(
                "host_tx_dropped_pps",
                "Packets per second the default interface's driver dropped for want of room",
            ),
            sessions: g("sessions", "Sessions running on this instance"),
            shards: g("shards", "Threads serving sessions"),
            shards_pressured: g(
                "shards_pressured",
                "Shards with no room left, shedding load (small video layers, fewer voices)",
            ),
            shards_saturated: g(
                "shards_saturated",
                "Shards at their limit: they refuse new sessions",
            ),
            egress_bps: g(
                "egress_bps",
                "What the instance hands to its sockets, in bits per second, headers up to the link layer included",
            ),
            egress_pps: g(
                "egress_pps",
                "What the instance hands to its sockets, in datagrams per second",
            ),
            egress_ceiling_bps: g(
                "egress_ceiling_bps",
                "The send rate in bits per second at which the kernel started refusing datagrams, as the node learned it (0 until it has)",
            ),
            egress_ceiling_pps: g("egress_ceiling_pps", "The same in datagrams per second"),
            egress_adapt: g(
                "egress_adapt",
                "1 when the node acts on the kernel's refusals (RTCD_RTC_EGRESSADAPT), 0 when it only counts and reports them",
            ),
            egress_squeezed: g(
                "egress_squeezed",
                "1 while receivers are held to the small video layers and calls to two voices: a send budget is reached, or, with egress_adapt, the kernel is refusing datagrams (until ten seconds after) or the learned ceiling is reached",
            ),
            egress_refusing: g(
                "egress_refusing",
                "1 while the kernel refuses datagrams (two refusing seconds in a row, and ten seconds after); with egress_adapt recording uploads slow down meanwhile",
            ),
            egress_choked: g(
                "egress_choked",
                "With egress_adapt, 1 while the kernel keeps refusing through a squeeze the node decided on: calls forward their loudest voice only (always 0 without it)",
            ),
            kernel_send_drops: {
                let c = Counter::with_opts(
                    Opts::new(
                        "kernel_send_drops_total",
                        "Datagrams the kernel refused to queue (UDP SndbufErrors), counted from when the instance first read it",
                    )
                    .namespace(NAMESPACE),
                )
                .expect("valid metric");
                r.register(Box::new(c.clone())).expect("unique metric");
                c
            },
            kernel_send_drops_seen: AtomicU64::new(0),
        }
    }

    fn observe(&self, s: &SystemStatus) {
        let h = &s.host;
        self.cpu_load.set(h.cpu_load);
        self.cpu_busiest.set(h.cpu_busiest);
        self.cpu_iowait.set(h.cpu_iowait);
        self.cpu_softirq.set(h.cpu_softirq);
        self.mem_used.set(h.mem_used);
        self.mem_total.set(h.mem_total_bytes as f64);
        self.mem_available.set(h.mem_available_bytes as f64);
        self.rss_limit.set(h.rss_limit_bytes as f64);
        self.tx_bps.set(h.host_tx_bps as f64);
        self.tx_pps.set(h.host_tx_pps as f64);
        self.tx_dropped_pps.set(h.host_tx_dropped_pps as f64);
        self.sessions.set(s.sessions as f64);
        self.shards.set(s.shards as f64);
        self.shards_pressured.set(s.shards_pressured as f64);
        self.shards_saturated.set(s.shards_saturated as f64);
        self.egress_bps.set(s.egress_bps as f64);
        self.egress_pps.set(s.egress_pps as f64);
        self.egress_ceiling_bps.set(s.egress_ceiling_bps as f64);
        self.egress_ceiling_pps.set(s.egress_ceiling_pps as f64);
        self.egress_adapt.set(f64::from(s.egress_adapt));
        self.egress_squeezed.set(f64::from(s.egress_squeezed));
        self.egress_refusing.set(f64::from(s.egress_refusing));
        self.egress_choked.set(f64::from(s.egress_choked));
        let seen = self
            .kernel_send_drops_seen
            .fetch_max(s.kernel_send_drops, Ordering::Relaxed);
        self.kernel_send_drops
            .inc_by(s.kernel_send_drops.saturating_sub(seen) as f64);
    }
}

/// The service's metrics.
pub struct Metrics {
    registry: Registry,
    node: Node,
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
            node: Node::new(r),
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

    /// Sets the node-level gauges (see [`SystemStatus`]); call it just before `render`.
    pub fn observe_system(&self, status: &SystemStatus) {
        self.node.observe(status);
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
    fn the_node_figures_are_exported_under_the_names_of_system() {
        let m = Metrics::new();
        let mut status = SystemStatus {
            sessions: 1500,
            shards: 4,
            shards_pressured: 3,
            shards_saturated: 1,
            egress_bps: 540_000_000,
            egress_pps: 105_000,
            kernel_send_drops: 7_000,
            egress_adapt: true,
            egress_squeezed: true,
            egress_choked: true,
            egress_ceiling_bps: 700_000_000,
            egress_ceiling_pps: 117_000,
            ..SystemStatus::default()
        };
        status.host.cpu_load = 0.75;
        status.host.cpu_busiest = 1.0;
        status.host.mem_used = 0.5;
        status.host.mem_total_bytes = 8 << 30;
        status.host.rss_limit_bytes = 3 << 30;
        status.host.host_tx_bps = 1_200_000_000;
        m.observe_system(&status);
        let text = m.render();
        for line in [
            "rtcd_sessions 1500",
            "rtcd_shards 4",
            "rtcd_shards_pressured 3",
            "rtcd_shards_saturated 1",
            "rtcd_egress_bps 540000000",
            "rtcd_egress_pps 105000",
            "rtcd_egress_ceiling_bps 700000000",
            "rtcd_egress_ceiling_pps 117000",
            "rtcd_egress_adapt 1",
            "rtcd_egress_squeezed 1",
            "rtcd_egress_refusing 0",
            "rtcd_egress_choked 1",
            "rtcd_kernel_send_drops_total 7000",
            "rtcd_host_cpu_load 0.75",
            "rtcd_host_cpu_busiest 1",
            "rtcd_host_mem_used 0.5",
            "rtcd_host_mem_total_bytes 8589934592",
            "rtcd_rss_limit_bytes 3221225472",
            "rtcd_host_tx_bps 1200000000",
        ] {
            assert!(text.lines().any(|l| l == line), "missing {line} in\n{text}");
        }
        // The drop counter only adds what is new, and never goes back.
        status.kernel_send_drops = 7_500;
        m.observe_system(&status);
        status.kernel_send_drops = 100;
        m.observe_system(&status);
        assert!(m.render().contains("rtcd_kernel_send_drops_total 7500\n"));
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
