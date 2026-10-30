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

use crate::api::{Api, HttpClients};

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

impl std::fmt::Debug for Fake {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fake")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
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
        test_api(&self.url)
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

// ─── A fake app: the recorder endpoints uploads go to ───────────────────────────────

use axum::extract::Path as AxPath;
use axum::routing::{post, put};

/// The API of the recording [`REC_ID`] against the fake at `site`, retrying without waiting.
fn test_api(site: &str) -> Api {
    let mut api = Api::with_clients(&HttpClients::new(), site, REC_ID, "bot-token");
    api.backoff = Duration::from_millis(1);
    api
}

#[derive(Debug, Default)]
pub struct LmsState {
    /// The bytes of each file the recording sent to the server, by its number.
    pub uploads: HashMap<String, Vec<u8>>,
}

/// Plays the app's side of the uploads of one recording.
pub struct Lms {
    pub url: String,
    pub state: Arc<Mutex<LmsState>>,
}

impl std::fmt::Debug for Lms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Lms")
            .field("url", &self.url)
            .finish_non_exhaustive()
    }
}

impl Lms {
    pub async fn start() -> Self {
        let state = Arc::new(Mutex::new(LmsState::default()));
        let app = Router::new()
            .route(&format!("{REC_PATH}/uploads"), post(lms_uploads))
            .route(&format!("{REC_PATH}/files/{{n}}"), put(lms_file))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, state }
    }

    pub fn api(&self) -> Api {
        test_api(&self.url)
    }
}

type S = State<Arc<Mutex<LmsState>>>;

async fn lms_uploads() -> impl IntoResponse {
    axum::Json(serde_json::json!({"direct": false, "uploads": []}))
}

async fn lms_file(State(s): S, AxPath(n): AxPath<String>, body: Bytes) -> impl IntoResponse {
    s.lock().unwrap().uploads.insert(n, body.to_vec());
    StatusCode::OK
}
