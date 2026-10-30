//! The service: ties the clients' websocket connections to the SFU.
//!
//! A client (a Mattermost installation) holds one websocket. Over it come requests to start and
//! end sessions and the SDP, ICE and notifications of those sessions' participants; back go the
//! SFU's answers, candidates and voice activity. Each session is bound to the connection it should
//! reply on, which a reconnecting client moves to its new connection.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use rtcd_proto::{ClientMessage, Hello, Message, MessageType, SessionConfig, SessionRef};
use rtcd_sfu::engine::Event;
use rtcd_sfu::runtime::{AdmitError, JoinError, Sfu};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::auth::AuthService;
use crate::config::Config;
use crate::info::{SystemMonitor, SystemStatus};
use crate::metrics::Metrics;
use crate::recording::{Recordings, Reports};

/// Messages queued for one connection before it is considered too slow to serve.
pub const CONN_QUEUE: usize = 5000;

/// The way to send to one websocket connection.
#[derive(Debug, Clone)]
pub struct ConnHandle {
    pub client_id: String,
    tx: mpsc::Sender<Outbound>,
}

/// What a connection's writer is asked to send.
#[derive(Debug)]
pub enum Outbound {
    /// A packed [`ClientMessage`].
    Binary(Vec<u8>),
    /// End the connection.
    Close,
}

impl ConnHandle {
    pub fn new(client_id: String, tx: mpsc::Sender<Outbound>) -> Self {
        Self { client_id, tx }
    }

    /// Queues a message; false if the connection is gone or not keeping up.
    pub fn send(&self, msg: &ClientMessage) -> bool {
        self.tx.try_send(Outbound::Binary(msg.pack())).is_ok()
    }

    pub fn close(&self) {
        let _ = self.tx.try_send(Outbound::Close);
    }
}

/// The running service.
pub struct Service {
    pub cfg: Arc<Config>,
    pub auth: Arc<AuthService>,
    pub sfu: Arc<Sfu>,
    pub metrics: Arc<Metrics>,
    pub system: SystemMonitor,
    /// Recording calls on this node.
    pub recordings: Recordings,
    /// What is told to the server about recordings, on whichever of its connections is up.
    reports: Arc<Reports>,
    conns: Mutex<HashMap<String, ConnHandle>>,
    /// The connection each session's messages go to.
    session_conn: Mutex<HashMap<String, String>>,
    /// Sessions that ended a moment ago. A browser that hangs up closes its media connection, which
    /// ends the session, before the server's own `leave` for it arrives: that one is expected.
    closed: Mutex<HashMap<String, Instant>>,
}

/// Recording uploads slow down for as long as the kernel refuses the datagrams of the node's media
/// (both go out of the same network card, and a file can wait where a call cannot), and speed up
/// again when it stops.
#[derive(Debug, Default)]
struct UploadPace {
    yielding: bool,
}

impl UploadPace {
    /// One tick: whether the kernel is refusing (and the node acts on it: otherwise `false`, and
    /// the uploads go at their full rate), and so how fast they go. Asked every tick
    /// and not only when `refusing` changes, because the answer also depends on how many files
    /// wait (see `Recordings::yield_to_media`), which changes while the kernel goes on refusing.
    fn step(&mut self, refusing: bool, adapts: bool, recordings: &Recordings) {
        let refusing = refusing && adapts;
        if refusing != self.yielding {
            if refusing {
                warn!("the kernel is refusing datagrams: recording uploads slow down");
            } else {
                info!("the kernel accepts datagrams again: recording uploads at full rate");
            }
            self.yielding = refusing;
        }
        recordings.yield_to_media(refusing);
    }
}

/// How long a closed session is remembered.
const CLOSED_MEMORY: Duration = Duration::from_secs(60);

impl std::fmt::Debug for Service {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Service")
            .field("connections", &self.conns.lock().len())
            .finish_non_exhaustive()
    }
}

impl Service {
    pub fn new(
        cfg: Arc<Config>,
        auth: Arc<AuthService>,
        sfu: Arc<Sfu>,
        metrics: Arc<Metrics>,
    ) -> Arc<Self> {
        let reports = Arc::new(Reports::new(metrics.clone()));
        let recordings = Recordings::new(
            &cfg.record,
            cfg.rtc.egress_adapt,
            reports.clone(),
            metrics.clone(),
        );
        Arc::new(Self {
            cfg,
            auth,
            sfu,
            metrics,
            system: SystemMonitor::default(),
            recordings,
            reports,
            conns: Mutex::new(HashMap::new()),
            session_conn: Mutex::new(HashMap::new()),
            closed: Mutex::new(HashMap::new()),
        })
    }

    /// Relays what the SFU reports until its channel closes.
    pub async fn run_events(self: Arc<Self>, mut events: mpsc::UnboundedReceiver<Event>) {
        while let Some(event) = events.recv().await {
            self.on_event(event);
        }
    }

    /// What the service does about the network once a second, until the task is aborted: brings
    /// the node's send figures up to date (sessions do it too, but a node with none left would
    /// report its last rate and squeeze for ever), and sets how fast recording uploads go.
    pub async fn upkeep(self: Arc<Self>) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut uploads = UploadPace::default();
        loop {
            tick.tick().await;
            self.tick(&mut uploads, Instant::now());
        }
    }

    fn tick(&self, uploads: &mut UploadPace, now: Instant) {
        self.sfu.tick_egress(now);
        uploads.step(
            self.sfu.egress_refusing(),
            self.sfu.egress_adapts(),
            &self.recordings,
        );
    }

    /// What `GET /system` reports and `/metrics` exports about the node.
    pub fn status(&self) -> SystemStatus {
        let sfu = &self.sfu;
        SystemStatus {
            host: self.system.info(),
            sessions: sfu.session_count(),
            shards: sfu.shard_count(),
            shards_pressured: sfu.shards_pressured(),
            shards_saturated: sfu.shards_saturated(),
            egress_bps: sfu.egress_bps(),
            egress_pps: sfu.egress_pps(),
            kernel_send_drops: sfu.kernel_send_drops(),
            egress_adapt: sfu.egress_adapts(),
            egress_squeezed: sfu.egress_squeezed(),
            egress_refusing: sfu.egress_refusing(),
            egress_choked: sfu.egress_choked(),
            egress_ceiling_bps: sfu.egress_ceiling_bps(),
            egress_ceiling_pps: sfu.egress_ceiling_pps(),
        }
    }

    pub fn connection_count(&self) -> usize {
        self.conns.lock().len()
    }

    /// Registers a new connection and greets it with its identity.
    pub fn connected(&self, conn_id: &str, handle: ConnHandle) {
        self.metrics.ws_connected(&handle.client_id);
        info!(conn = conn_id, client = %handle.client_id, "client connected");
        let hello = ClientMessage::Hello(Hello {
            client_id: handle.client_id.clone(),
            conn_id: conn_id.to_owned(),
        });
        if handle.send(&hello) {
            self.metrics.ws_message(&handle.client_id, "hello", "out");
        }
        self.conns.lock().insert(conn_id.to_owned(), handle.clone());
        // Reports that waited for a connection of this client go out now.
        self.reports.attach(conn_id, handle);
    }

    /// Forgets a connection. Its sessions stay: the client is expected to reconnect and say so.
    pub fn disconnected(&self, conn_id: &str) {
        self.reports.detach(conn_id);
        if let Some(h) = self.conns.lock().remove(conn_id) {
            self.metrics.ws_disconnected(&h.client_id);
            warn!(conn = conn_id, client = %h.client_id, "client disconnected");
        }
    }

    /// Asks every connection to close, at shutdown.
    pub fn close_connections(&self) {
        for h in self.conns.lock().values() {
            h.close();
        }
    }

    /// Whether `session_id` is a session of `client_id`; a client may act only on its own.
    fn owns(&self, client_id: &str, session_id: &str) -> bool {
        self.sfu
            .session_by_id(session_id)
            .is_some_and(|c| c.group_id == client_id)
    }

    /// Handles one binary frame from a client.
    pub fn on_frame(&self, conn_id: &str, client_id: &str, frame: &[u8]) {
        let msg = match ClientMessage::unpack(frame) {
            Ok(m) => m,
            Err(err) => {
                error!(conn = conn_id, client = client_id, %err, "unreadable message");
                return;
            }
        };
        let kind = msg.type_name();
        self.metrics.ws_message(client_id, kind, "in");
        match msg {
            ClientMessage::Join(join) => self.join(conn_id, client_id, &join),
            ClientMessage::Leave(r) => {
                if self.owns(client_id, &r.session_id) {
                    debug!(session = %r.session_id, "leave");
                    self.sfu.leave(&r.session_id);
                } else if self.closed_lately(&r.session_id) {
                    debug!(session = %r.session_id, "leave for a session that already ended");
                } else {
                    warn!(session = %r.session_id, client = client_id, "leave for a session the client does not have");
                }
            }
            ClientMessage::Reconnect(r) => {
                if r.session_id.is_empty() {
                    error!(conn = conn_id, "reconnect without a session id");
                } else if self.owns(client_id, &r.session_id) {
                    debug!(session = %r.session_id, conn = conn_id, "reconnect: messages now go to the new connection");
                    self.session_conn
                        .lock()
                        .insert(r.session_id, conn_id.to_owned());
                } else {
                    warn!(session = %r.session_id, client = client_id, "reconnect for a session the client does not have");
                }
            }
            ClientMessage::Rtc(m) => self.signal(client_id, m),
            // The start carries a token: only its type and the recording are ever logged.
            ClientMessage::RecordStart(r) => self.recordings.start(&self.sfu, client_id, r),
            ClientMessage::RecordStop(r) => {
                self.recordings.stop(&self.sfu, client_id, &r.recording_id);
            }
            ClientMessage::Hello(_)
            | ClientMessage::Close(_)
            | ClientMessage::Vad(_)
            | ClientMessage::RecordFile(_)
            | ClientMessage::RecordEnded(_) => {
                warn!(conn = conn_id, "unexpected message from a client");
            }
        }
    }

    fn join(&self, conn_id: &str, client_id: &str, join: &rtcd_proto::JoinRequest) {
        let cfg = SessionConfig::from_join(client_id, join);
        let session_id = cfg.session_id.clone();
        debug!(session = %session_id, call = %cfg.call_id, "join");
        // Bind first: the SFU may have something to say the moment it starts the session.
        self.session_conn
            .lock()
            .insert(session_id.clone(), conn_id.to_owned());
        if let Err(err) = self.sfu.join(cfg) {
            error!(session = %session_id, %err, "failed to initialize the rtc session");
            // A duplicate leaves the first session in place, still bound to its connection.
            if !matches!(err, JoinError::Admit(AdmitError::Duplicate)) {
                self.session_conn.lock().remove(&session_id);
                self.send_close(conn_id, &session_id);
            }
        }
    }

    fn signal(&self, client_id: &str, msg: Message) {
        if msg.session_id.is_empty() {
            error!("rtc message without a session id");
            return;
        }
        if !self.owns(client_id, &msg.session_id) {
            if self.closed_lately(&msg.session_id) {
                debug!(session = %msg.session_id, "message for a session that already ended");
            } else {
                warn!(session = %msg.session_id, client = client_id, "message for a session the client does not have");
            }
            return;
        }
        if let Err(err) = self.sfu.signal(msg) {
            error!(%err, "failed to pass a message to the SFU");
        }
    }

    fn closed_lately(&self, session_id: &str) -> bool {
        self.closed
            .lock()
            .get(session_id)
            .is_some_and(|t| t.elapsed() < CLOSED_MEMORY)
    }

    fn send_close(&self, conn_id: &str, session_id: &str) {
        let Some(h) = self.conns.lock().get(conn_id).cloned() else {
            return;
        };
        let ok = h.send(&ClientMessage::Close(SessionRef {
            session_id: session_id.to_owned(),
        }));
        if ok {
            self.metrics.ws_message(&h.client_id, "close", "out");
        }
    }

    /// Relays one report of the SFU to the client the session belongs to.
    fn on_event(&self, event: Event) {
        match event {
            Event::Message(m) => {
                let conn = {
                    let map = self.session_conn.lock();
                    map.get(&m.session_id).cloned()
                };
                let Some(handle) = conn.and_then(|c| self.conns.lock().get(&c).cloned()) else {
                    debug!(session = %m.session_id, "no connection for the session, dropping its message");
                    return;
                };
                let group = m.group_id.clone();
                let msg = match m.kind {
                    MessageType::VoiceOn | MessageType::VoiceOff => ClientMessage::Vad(m),
                    _ => ClientMessage::Rtc(m),
                };
                let kind = msg.type_name();
                if handle.send(&msg) {
                    self.metrics.ws_message(&group, kind, "out");
                } else {
                    error!(client = %group, "failed to queue a message: the connection is full or gone");
                }
            }
            Event::SessionClosed {
                session_id, reason, ..
            } => {
                let conn = self.session_conn.lock().remove(&session_id);
                debug!(session = %session_id, ?reason, "session closed");
                {
                    let mut closed = self.closed.lock();
                    closed.retain(|_, t| t.elapsed() < CLOSED_MEMORY);
                    closed.insert(session_id.clone(), Instant::now());
                }
                if let Some(conn) = conn {
                    self.send_close(&conn, &session_id);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RecordConfig;

    fn recordings(name: &str) -> (Recordings, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("rtcd-pace-{name}-{}", std::process::id()));
        let metrics = Arc::new(Metrics::new());
        let cfg = RecordConfig {
            enabled: true,
            data_dir: dir.to_string_lossy().into_owned(),
            max_pending_bytes: 4 << 30,
            ..RecordConfig::default()
        };
        let r = Recordings::new(&cfg, true, Arc::new(Reports::new(metrics.clone())), metrics);
        (r, dir)
    }

    #[tokio::test]
    async fn uploads_speed_up_when_files_pile_up_while_the_kernel_goes_on_refusing() {
        let (r, dir) = recordings("pile");
        let mut pace = UploadPace::default();
        // The kernel starts refusing with little waiting: uploads give way.
        pace.step(true, true, &r);
        assert_eq!(r.upload_share(), Some(250));
        // It goes on refusing for minutes, and the files of the ended classes pile up; `refusing`
        // never changes, so only a check on every tick sees half the cap go by.
        for gib in [1u64, 1] {
            r.hold(gib << 30);
            pace.step(true, true, &r);
        }
        assert_eq!(
            r.upload_share(),
            Some(1000),
            "uploads at full speed once half of the cap waits, refusals or not"
        );
        pace.step(false, true, &r);
        assert_eq!(r.upload_share(), Some(1000));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn uploads_never_yield_on_a_node_that_does_not_adapt() {
        let (r, dir) = recordings("off");
        let mut pace = UploadPace::default();
        for _ in 0..30 {
            pace.step(true, false, &r);
            assert_eq!(r.upload_share(), Some(1000));
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
