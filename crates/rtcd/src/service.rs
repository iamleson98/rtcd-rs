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
use crate::info::SystemMonitor;
use crate::metrics::Metrics;

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
    conns: Mutex<HashMap<String, ConnHandle>>,
    /// The connection each session's messages go to.
    session_conn: Mutex<HashMap<String, String>>,
    /// Sessions that ended a moment ago. A browser that hangs up closes its media connection, which
    /// ends the session, before the server's own `leave` for it arrives: that one is expected.
    closed: Mutex<HashMap<String, Instant>>,
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
        Arc::new(Self {
            cfg,
            auth,
            sfu,
            metrics,
            system: SystemMonitor::default(),
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
        self.conns.lock().insert(conn_id.to_owned(), handle);
    }

    /// Forgets a connection. Its sessions stay: the client is expected to reconnect and say so.
    pub fn disconnected(&self, conn_id: &str) {
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
        self.metrics.ws_message(client_id, msg.type_name(), "in");
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
            ClientMessage::Hello(_) | ClientMessage::Close(_) | ClientMessage::Vad(_) => {
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
