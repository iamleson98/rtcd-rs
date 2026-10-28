//! A stand-in for the LMS server, for one recording: the recorder endpoints, the whiteboard, the
//! calls API and the app's websocket, which relays the recorder's call signaling to a real rtcd
//! the way the server's calls plugin does.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::{Path as AxPath, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{any, get, post, put};
use futures_util::StreamExt;
use rtcd_proto::{ClientMessage, JoinRequest, Message, MessageType, SessionRef};
use serde_json::{Value, json};
use tokio::io::AsyncWriteExt;
use tokio::sync::{broadcast, mpsc};

use crate::host::{Rtcd, RtcdConn};

pub const REC_ID: &str = "rrrrrrrrrrrrrrrrrrrrrrrrrr";
pub const SESS_ID: &str = "ssssssssssssssssssssssssss";
pub const CHAN_ID: &str = "cccccccccccccccccccccccccc";
pub const REC_PATH: &str = "/api/v4/lms/recorder/recordings/rrrrrrrrrrrrrrrrrrrrrrrrrr";

#[derive(Debug)]
pub struct State_ {
    pub status: String,
    pub stop_at_ms: i64,
    pub call_up: bool,
    pub room_code: u16,
    pub start_err: u16,
    pub paths: Vec<String>,
    pub files: HashMap<String, PathBuf>,
    pub complete: Option<Value>,
    pub failed: Option<String>,
    /// The actions the recorder sent over the websocket.
    pub actions: Vec<Value>,
    /// Who is in the call: session id to user id (not counting the recorder).
    pub people: Vec<(String, String)>,
    pub sockets: usize,
}

/// The recorder's session on the SFU: what it asks of the bridge, and where the bridge's events go (the
/// recorder's current websocket, which changes when it reconnects).
struct BotLink {
    tx: mpsc::UnboundedSender<Bot>,
    out: Arc<Mutex<mpsc::UnboundedSender<Value>>>,
}

struct Inner {
    bot: Mutex<Option<BotLink>>,
    state: Mutex<State_>,
    events: broadcast::Sender<Value>,
    rtcd: Option<Arc<Rtcd>>,
    dir: PathBuf,
    bots: AtomicU64,
}

/// The stand-in server.
#[derive(Clone)]
pub struct Lms {
    pub url: String,
    inner: Arc<Inner>,
}

impl std::fmt::Debug for Lms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lms")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

impl Lms {
    /// Starts serving. With an rtcd, the recorder's calls go through it; without one, joining is
    /// refused.
    pub async fn start(rtcd: Option<Arc<Rtcd>>, dir: PathBuf) -> Self {
        std::fs::create_dir_all(&dir).unwrap();
        let (events, _) = broadcast::channel(4096);
        let inner = Arc::new(Inner {
            bot: Mutex::new(None),
            state: Mutex::new(State_ {
                status: "RECORDING".into(),
                stop_at_ms: now_ms() + 3_600_000,
                call_up: false,
                room_code: 200,
                start_err: 0,
                paths: Vec::new(),
                files: HashMap::new(),
                complete: None,
                failed: None,
                actions: Vec::new(),
                people: Vec::new(),
                sockets: 0,
            }),
            events,
            rtcd,
            dir,
            bots: AtomicU64::new(0),
        });
        let app = Router::new()
            .route("/api/v4/websocket", any(ws))
            .route(REC_PATH, get(room))
            .route(&format!("{REC_PATH}/started"), post(started))
            .route(&format!("{REC_PATH}/uploads"), post(uploads))
            .route(&format!("{REC_PATH}/files/{{n}}"), put(file))
            .route(&format!("{REC_PATH}/complete"), post(complete))
            .route(&format!("{REC_PATH}/fail"), post(fail))
            .route(
                &format!("/api/v4/lms/live/sessions/{SESS_ID}/whiteboard"),
                get(board),
            )
            .route(&format!("/api/v4/calls/channels/{CHAN_ID}"), get(call))
            .with_state(inner.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, inner }
    }

    pub fn set(&self, f: impl FnOnce(&mut State_)) {
        f(&mut self.inner.state.lock().unwrap());
    }

    pub fn read<T>(&self, f: impl FnOnce(&State_) -> T) -> T {
        f(&self.inner.state.lock().unwrap())
    }

    pub fn has(&self, path: &str) -> bool {
        self.read(|s| s.paths.iter().any(|p| p == path))
    }

    pub fn actions(&self) -> Vec<String> {
        self.read(|s| {
            s.actions
                .iter()
                .map(|a| a["action"].as_str().unwrap_or("").to_owned())
                .collect()
        })
    }

    /// Sends an event to the recorder's websocket (without its sequence number, which each
    /// connection adds).
    pub fn emit(&self, event: &str, channel: &str, data: Value) {
        let _ = self
            .inner
            .events
            .send(json!({"event": event, "data": data, "broadcast": {"channel_id": channel}}));
    }

    /// A person joined the class call.
    pub fn person_joined(&self, session: &str, user: &str) {
        let first = self.read(|s| s.people.is_empty());
        self.set(|s| {
            s.people.push((session.into(), user.into()));
            s.call_up = true;
        });
        if first {
            self.emit("custom_calls_call_start", CHAN_ID, json!({}));
        }
        self.emit(
            "custom_calls_user_joined",
            CHAN_ID,
            json!({"user_id": user, "session_id": session}),
        );
    }

    pub fn person_left(&self, session: &str) {
        let user = self
            .read(|s| {
                s.people
                    .iter()
                    .find(|p| p.0 == session)
                    .map(|p| p.1.clone())
            })
            .unwrap_or_default();
        self.set(|s| s.people.retain(|p| p.0 != session));
        self.emit(
            "custom_calls_user_left",
            CHAN_ID,
            json!({"user_id": user, "session_id": session}),
        );
        if self.read(|s| s.people.is_empty()) {
            self.set(|s| s.call_up = false);
            self.emit("custom_calls_call_end", CHAN_ID, json!({}));
        }
    }

    /// Closes every websocket the recorder has open; it is expected to reconnect.
    pub fn drop_sockets(&self) {
        let _ = self.inner.events.send(json!({"__drop": true}));
    }

    pub fn config_env(&self) -> Vec<(String, String)> {
        vec![
            ("SITE_URL".into(), self.url.clone()),
            ("AUTH_TOKEN".into(), "bot-token".into()),
            ("RECORDING_ID".into(), REC_ID.into()),
            ("MAX_DURATION_SEC".into(), "3600".into()),
            ("MAX_DATA_BYTES".into(), (4u64 << 30).to_string()),
        ]
    }
}

type S = State<Arc<Inner>>;

fn note(s: &Inner, method: &str, path: &str) {
    s.state
        .lock()
        .unwrap()
        .paths
        .push(format!("{method} {path}"));
}

async fn room(State(s): S) -> impl IntoResponse {
    note(&s, "GET", REC_PATH);
    let st = s.state.lock().unwrap();
    if st.room_code != 200 {
        return StatusCode::from_u16(st.room_code).unwrap().into_response();
    }
    axum::Json(json!({"session_id": SESS_ID, "chat_channel_id": CHAN_ID, "stop_at": st.stop_at_ms, "status": st.status})).into_response()
}

async fn started(State(s): S) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/started"));
    let e = s.state.lock().unwrap().start_err;
    StatusCode::from_u16(if e == 0 { 200 } else { e }).unwrap()
}

async fn uploads(State(s): S) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/uploads"));
    axum::Json(json!({"direct": false, "uploads": []}))
}

async fn file(State(s): S, AxPath(n): AxPath<String>, body: Body) -> impl IntoResponse {
    note(&s, "PUT", &format!("{REC_PATH}/files/{n}"));
    let path = s.dir.join(format!("upload-{n}"));
    let Ok(mut f) = tokio::fs::File::create(&path).await else {
        return StatusCode::INTERNAL_SERVER_ERROR;
    };
    let mut stream = body.into_data_stream();
    while let Some(chunk) = stream.next().await {
        let Ok(chunk) = chunk else {
            return StatusCode::BAD_REQUEST;
        };
        if f.write_all(&chunk).await.is_err() {
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    }
    s.state.lock().unwrap().files.insert(n, path);
    StatusCode::OK
}

async fn complete(State(s): S, body: axum::body::Bytes) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/complete"));
    s.state.lock().unwrap().complete = serde_json::from_slice(&body).ok();
    StatusCode::OK
}

async fn fail(State(s): S, body: axum::body::Bytes) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/fail"));
    s.state.lock().unwrap().failed = Some(String::from_utf8_lossy(&body).into_owned());
    StatusCode::OK
}

async fn board(State(s): S) -> impl IntoResponse {
    note(
        &s,
        "GET",
        &format!("/api/v4/lms/live/sessions/{SESS_ID}/whiteboard"),
    );
    axum::Json(json!({"locked": false, "elements": [{"id": "e0"}]}))
}

async fn call(State(s): S) -> impl IntoResponse {
    note(&s, "GET", &format!("/api/v4/calls/channels/{CHAN_ID}"));
    if s.state.lock().unwrap().call_up {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn ws(
    State(s): S,
    Query(q): Query<HashMap<String, String>>,
    _h: HeaderMap,
    up: WebSocketUpgrade,
) -> impl IntoResponse {
    // A client that names its earlier connection is resumed on it.
    let id = q
        .get("connection_id")
        .cloned()
        .unwrap_or_else(|| "conn-1".into());
    up.on_upgrade(move |socket| socket_loop(s, socket, id))
}

/// What the recorder asks of the bridge.
enum Bot {
    Sdp(String),
    Ice(String),
    Mute,
    Leave,
}

async fn socket_loop(s: Arc<Inner>, mut socket: WebSocket, connection_id: String) {
    let _ = socket
        .send(WsMessage::text(
            json!({"event": "hello", "seq": 0, "data": {"connection_id": connection_id}})
                .to_string(),
        ))
        .await;
    s.state.lock().unwrap().sockets += 1;
    let mut events = s.events.subscribe();
    let mut seq = 0i64;
    // Out of the bridge: events for this socket. A session the recorder already has follows it here.
    let (out_tx, mut out_rx) = mpsc::unbounded_channel::<Value>();
    if let Some(link) = s.bot.lock().unwrap().as_ref() {
        *link.out.lock().unwrap() = out_tx.clone();
    }
    loop {
        tokio::select! {
            ev = events.recv() => {
                let Ok(mut ev) = ev else { continue };
                if ev.get("__drop").is_some() {
                    break;
                }
                seq += 1;
                ev["seq"] = seq.into();
                if socket.send(WsMessage::text(ev.to_string())).await.is_err() { break }
            }
            ev = out_rx.recv() => {
                let Some(mut ev) = ev else { break };
                seq += 1;
                ev["seq"] = seq.into();
                if socket.send(WsMessage::text(ev.to_string())).await.is_err() { break }
            }
            msg = socket.recv() => match msg {
                Some(Ok(WsMessage::Text(t))) => {
                    let Ok(v) = serde_json::from_str::<Value>(&t) else { continue };
                    s.state.lock().unwrap().actions.push(v.clone());
                    let data = &v["data"];
                    let to_bot = |a: Bot| {
                        if let Some(link) = s.bot.lock().unwrap().as_ref() {
                            let _ = link.tx.send(a);
                        }
                    };
                    match v["action"].as_str().unwrap_or("") {
                        "custom_calls_join" => {
                            if let Some(rtcd) = s.rtcd.clone() {
                                let id = format!("bot-{}", s.bots.fetch_add(1, Ordering::Relaxed));
                                let (tx, rx) = mpsc::unbounded_channel();
                                let out = Arc::new(Mutex::new(out_tx.clone()));
                                *s.bot.lock().unwrap() = Some(BotLink { tx, out: out.clone() });
                                tokio::spawn(bridge(s.clone(), rtcd, id, rx, out));
                            } else {
                                let _ = out_tx.send(json!({"event": "custom_calls_error", "data": {}, "broadcast": {"channel_id": CHAN_ID}}));
                            }
                        }
                        "custom_calls_sdp" => if let Some(d) = data["data"].as_str() { to_bot(Bot::Sdp(d.into())) },
                        "custom_calls_ice" => if let Some(d) = data["data"].as_str() { to_bot(Bot::Ice(d.into())) },
                        "custom_calls_mute" => to_bot(Bot::Mute),
                        "custom_calls_leave" => {
                            to_bot(Bot::Leave);
                            *s.bot.lock().unwrap() = None;
                        }
                        "custom_calls_call_state" => {
                            let people = s.state.lock().unwrap().people.clone();
                            let sessions: Vec<Value> = people.iter().map(|(sid, uid)| json!({"id": sid, "user_id": uid})).collect();
                            let _ = out_tx.send(json!({"event": "custom_calls_call_state", "data": {"call": json!({"sessions": sessions}).to_string()}, "broadcast": {"channel_id": CHAN_ID}}));
                        }
                        _ => {}
                    }
                }
                Some(Ok(_)) => {}
                _ => break,
            }
        }
    }
}

/// One recorder session on the SFU: the join acknowledgement, then signaling both ways.
async fn bridge(
    s: Arc<Inner>,
    rtcd: Arc<Rtcd>,
    id: String,
    mut from_bot: mpsc::UnboundedReceiver<Bot>,
    out: Arc<Mutex<mpsc::UnboundedSender<Value>>>,
) {
    let to_bot = move |v: Value| {
        let _ = out.lock().unwrap().send(v);
    };
    let mut conn: RtcdConn = rtcd.connect().await;
    let _ = conn.rx.recv().await; // hello
    conn.send(ClientMessage::Join(JoinRequest {
        call_id: CHAN_ID.into(),
        user_id: "recorder-bot".into(),
        session_id: id.clone(),
        channel_id: CHAN_ID.into(),
        av1_support: false,
        dc_signaling: true,
        host: false,
        observer: false,
    }));
    to_bot(
        json!({"event": "custom_calls_join", "data": {"connID": id, "iceServers": []}, "broadcast": {"channel_id": CHAN_ID}}),
    );
    // The recorder is told who is there once it is in.
    let people = s.state.lock().unwrap().people.clone();
    let mut sessions: Vec<Value> = people
        .iter()
        .map(|(sid, uid)| json!({"id": sid, "user_id": uid}))
        .collect();
    sessions.push(json!({"id": id, "user_id": "recorder-bot"}));
    to_bot(
        json!({"event": "custom_calls_call_state", "data": {"call": json!({"sessions": sessions}).to_string()}, "broadcast": {"channel_id": CHAN_ID}}),
    );
    let msg = |kind, data: Vec<u8>| {
        ClientMessage::Rtc(Message {
            group_id: "default".into(),
            user_id: "recorder-bot".into(),
            session_id: id.clone(),
            call_id: CHAN_ID.into(),
            kind,
            data,
        })
    };
    loop {
        tokio::select! {
            a = from_bot.recv() => match a {
                Some(Bot::Sdp(d)) => conn.send(msg(MessageType::Sdp, d.into_bytes())),
                Some(Bot::Ice(d)) => conn.send(msg(MessageType::Ice, d.into_bytes())),
                Some(Bot::Mute) => conn.send(msg(MessageType::Mute, Vec::new())),
                Some(Bot::Leave) | None => {
                    conn.send(ClientMessage::Leave(SessionRef { session_id: id.clone() }));
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    return;
                }
            },
            m = conn.rx.recv() => match m {
                Some(ClientMessage::Rtc(m)) if m.session_id == id => {
                    if matches!(m.kind, MessageType::Sdp | MessageType::Ice) {
                        let data = String::from_utf8_lossy(&m.data).into_owned();
                        to_bot(json!({"event": "custom_calls_signal", "data": {"data": data, "connID": id}, "broadcast": {"channel_id": CHAN_ID}}));
                    }
                }
                Some(_) => {}
                None => return,
            }
        }
    }
}
