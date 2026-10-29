//! A server that plays the LMS and storage for the tests, recording every call.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use serde_json::Value;

use crate::api::Api;
use crate::config::Config;

pub const REC_ID: &str = "rrrrrrrrrrrrrrrrrrrrrrrrrr";
pub const REC_PATH: &str = "/api/v4/lms/recorder/recordings/rrrrrrrrrrrrrrrrrrrrrrrrrr";

#[derive(Debug, Clone)]
pub struct Request {
    pub method: String,
    pub path: String,
    pub auth: String,
    pub content_type: String,
    pub body: Vec<u8>,
}

pub type Reply = dyn Fn(&Request, usize, &str) -> (u16, Option<Value>) + Send + Sync;

struct Shared {
    seen: Mutex<Vec<Request>>,
    attempts: Mutex<HashMap<String, usize>>,
    reply: Box<Reply>,
    site: String,
}

pub struct Fake {
    pub url: String,
    shared: Arc<Shared>,
}

impl Fake {
    pub async fn start(
        reply: impl Fn(&Request, usize, &str) -> (u16, Option<Value>) + Send + Sync + 'static,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let shared = Arc::new(Shared {
            seen: Mutex::new(Vec::new()),
            attempts: Mutex::new(HashMap::new()),
            reply: Box::new(reply),
            site: url.clone(),
        });
        let app = Router::new().fallback(handle).with_state(shared.clone());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, shared }
    }

    pub fn seen(&self) -> Vec<Request> {
        self.shared.seen.lock().unwrap().clone()
    }

    pub fn api(&self) -> Api {
        let cfg = Config {
            site_url: self.url.clone(),
            auth_token: "bot-token".into(),
            recording_id: REC_ID.into(),
            max_duration: Duration::from_secs(3600),
            max_data: 2 << 30,
        };
        let mut api = Api::new(&cfg);
        api.backoff = Duration::from_millis(1);
        api
    }
}

async fn handle(
    State(s): State<Arc<Shared>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let get = |h: &str| {
        headers
            .get(h)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_owned()
    };
    let req = Request {
        method: method.to_string(),
        path: uri.path().to_owned(),
        auth: get("authorization"),
        content_type: get("content-type"),
        body: body.to_vec(),
    };
    s.seen.lock().unwrap().push(req.clone());
    let n = {
        let mut a = s.attempts.lock().unwrap();
        let n = a.entry(req.path.clone()).or_insert(0);
        *n += 1;
        *n
    };
    let (status, json) = (s.reply)(&req, n, &s.site);
    let status = StatusCode::from_u16(status).unwrap();
    match json {
        Some(v) => (
            status,
            [("content-type", "application/json")],
            v.to_string(),
        )
            .into_response(),
        None => status.into_response(),
    }
}

// ─── A whole fake app: the recorder endpoints, the calls API and the websocket ───────

use std::future::Future;
use std::pin::Pin;
use std::time::Instant;

use axum::extract::Path as AxPath;
use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::routing::{any, get, post, put};
use tokio::sync::mpsc;

pub const SESS_ID: &str = "ssssssssssssssssssssssssss";
pub const CHAN_ID: &str = "cccccccccccccccccccccccccc";

type OnSocket = Box<
    dyn Fn(mpsc::UnboundedSender<Value>) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync,
>;

pub struct LmsState {
    pub status: String,
    pub stop_at_ms: i64,
    pub call_up: bool,
    pub room_code: u16,
    pub start_err: u16,
    pub paths: Vec<String>,
    pub uploads: HashMap<String, Vec<u8>>,
    pub complete: Option<Value>,
    /// The actions the recorder sent over the websocket.
    pub actions: Vec<Value>,
    pub on_socket: Option<Arc<OnSocket>>,
    pub sockets: usize,
}

/// Plays the whole app for one recording.
pub struct Lms {
    pub url: String,
    pub state: Arc<Mutex<LmsState>>,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

impl Lms {
    pub async fn start() -> Self {
        let state = Arc::new(Mutex::new(LmsState {
            status: "RECORDING".into(),
            stop_at_ms: now_ms() + 3_600_000,
            call_up: false,
            room_code: 200,
            start_err: 0,
            paths: Vec::new(),
            uploads: HashMap::new(),
            complete: None,
            actions: Vec::new(),
            on_socket: None,
            sockets: 0,
        }));
        let app = Router::new()
            .route("/api/v4/websocket", any(lms_ws))
            .route(REC_PATH, get(lms_room))
            .route(&format!("{REC_PATH}/started"), post(lms_started))
            .route(&format!("{REC_PATH}/uploads"), post(lms_uploads))
            .route(&format!("{REC_PATH}/files/{{n}}"), put(lms_file))
            .route(&format!("{REC_PATH}/complete"), post(lms_complete))
            .route(&format!("{REC_PATH}/fail"), post(lms_fail))
            .route(&format!("/api/v4/calls/channels/{CHAN_ID}"), get(lms_call))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, state }
    }

    pub fn set(&self, f: impl FnOnce(&mut LmsState)) {
        f(&mut self.state.lock().unwrap());
    }

    pub fn has(&self, path: &str) -> bool {
        self.state.lock().unwrap().paths.iter().any(|p| p == path)
    }

    pub fn paths(&self) -> Vec<String> {
        self.state.lock().unwrap().paths.clone()
    }

    pub fn actions(&self) -> Vec<String> {
        self.state
            .lock()
            .unwrap()
            .actions
            .iter()
            .map(|a| a["action"].as_str().unwrap_or("").to_owned())
            .collect()
    }

    /// Runs for each websocket connection after its hello: send events through the channel.
    pub fn on_socket<F, Fut>(&self, f: F)
    where
        F: Fn(mpsc::UnboundedSender<Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let f: OnSocket = Box::new(move |tx| Box::pin(f(tx)));
        self.set(|s| s.on_socket = Some(Arc::new(f)));
    }

    pub fn config(&self) -> Config {
        Config {
            site_url: self.url.clone(),
            auth_token: "bot-token".into(),
            recording_id: REC_ID.into(),
            max_duration: Duration::from_secs(3600),
            max_data: 4 << 30,
        }
    }

    pub fn api(&self) -> Api {
        let mut api = Api::new(&self.config());
        api.backoff = Duration::from_millis(1);
        api
    }
}

type S = State<Arc<Mutex<LmsState>>>;

fn note(s: &Arc<Mutex<LmsState>>, method: &str, path: &str) {
    s.lock().unwrap().paths.push(format!("{method} {path}"));
}

async fn lms_ws(State(s): S, ws: WebSocketUpgrade) -> impl IntoResponse {
    ws.on_upgrade(move |socket| lms_socket(s, socket))
}

async fn lms_socket(s: Arc<Mutex<LmsState>>, mut socket: WebSocket) {
    let _ = socket
        .send(WsMessage::text(
            serde_json::json!({"event": "hello", "seq": 0, "data": {"connection_id": "conn-1"}})
                .to_string(),
        ))
        .await;
    let (tx, mut rx) = mpsc::unbounded_channel::<Value>();
    let hook = {
        let mut st = s.lock().unwrap();
        st.sockets += 1;
        st.on_socket.clone()
    };
    if let Some(hook) = hook {
        tokio::spawn(hook(tx.clone()));
    }
    loop {
        tokio::select! {
            ev = rx.recv() => {
                let Some(ev) = ev else { break };
                if socket.send(WsMessage::text(ev.to_string())).await.is_err() { break }
            }
            msg = socket.recv() => match msg {
                Some(Ok(WsMessage::Text(t))) => {
                    if let Ok(v) = serde_json::from_str::<Value>(&t) {
                        s.lock().unwrap().actions.push(v);
                    }
                }
                Some(Ok(_)) => {}
                _ => break,
            }
        }
    }
}

async fn lms_room(State(s): S) -> impl IntoResponse {
    note(&s, "GET", REC_PATH);
    let st = s.lock().unwrap();
    if st.room_code != 200 {
        return StatusCode::from_u16(st.room_code).unwrap().into_response();
    }
    axum::Json(serde_json::json!({"session_id": SESS_ID, "chat_channel_id": CHAN_ID, "stop_at": st.stop_at_ms, "status": st.status})).into_response()
}

async fn lms_started(State(s): S) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/started"));
    let e = s.lock().unwrap().start_err;
    StatusCode::from_u16(if e == 0 { 200 } else { e }).unwrap()
}

async fn lms_uploads(State(s): S) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/uploads"));
    axum::Json(serde_json::json!({"direct": false, "uploads": []}))
}

async fn lms_file(State(s): S, AxPath(n): AxPath<String>, body: Bytes) -> impl IntoResponse {
    note(&s, "PUT", &format!("{REC_PATH}/files/{n}"));
    s.lock().unwrap().uploads.insert(n, body.to_vec());
    StatusCode::OK
}

async fn lms_complete(State(s): S, body: Bytes) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/complete"));
    s.lock().unwrap().complete = serde_json::from_slice(&body).ok();
    StatusCode::OK
}

async fn lms_fail(State(s): S, body: Bytes) -> impl IntoResponse {
    note(&s, "POST", &format!("{REC_PATH}/fail"));
    s.lock().unwrap().complete =
        Some(serde_json::json!({"failed": String::from_utf8_lossy(&body)}));
    StatusCode::OK
}

async fn lms_call(State(s): S) -> impl IntoResponse {
    note(&s, "GET", &format!("/api/v4/calls/channels/{CHAN_ID}"));
    if s.lock().unwrap().call_up {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

#[allow(dead_code)]
pub fn since(t: Instant) -> Duration {
    t.elapsed()
}
