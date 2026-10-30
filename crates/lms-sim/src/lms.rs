//! A stand-in for the LMS server, for one recording: the endpoints rtcd uploads the recording's
//! files to, which keeps what it is sent in a directory.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::Body;
use axum::extract::{Path as AxPath, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{post, put};
use futures_util::StreamExt;
use serde_json::json;
use tokio::io::AsyncWriteExt;

pub const REC_ID: &str = "rrrrrrrrrrrrrrrrrrrrrrrrrr";
pub const CHAN_ID: &str = "cccccccccccccccccccccccccc";
pub const REC_PATH: &str = "/api/v4/lms/recorder/recordings/rrrrrrrrrrrrrrrrrrrrrrrrrr";

struct Inner {
    /// Where an uploaded file is kept.
    dir: PathBuf,
    /// The files received, by their number.
    files: Mutex<HashMap<String, PathBuf>>,
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

impl Lms {
    /// Starts serving; uploaded files are kept in `dir`.
    pub async fn start(dir: PathBuf) -> Self {
        std::fs::create_dir_all(&dir).unwrap();
        let inner = Arc::new(Inner {
            dir,
            files: Mutex::new(HashMap::new()),
        });
        let app = Router::new()
            .route(&format!("{REC_PATH}/uploads"), post(uploads))
            .route(&format!("{REC_PATH}/files/{{n}}"), put(file))
            .with_state(inner.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, inner }
    }

    /// Where the file numbered `n` was kept, once it has been received.
    pub fn file(&self, n: &str) -> Option<PathBuf> {
        self.inner.files.lock().unwrap().get(n).cloned()
    }
}

type S = State<Arc<Inner>>;

async fn uploads() -> impl IntoResponse {
    axum::Json(json!({"direct": false, "uploads": []}))
}

async fn file(State(s): S, AxPath(n): AxPath<String>, body: Body) -> impl IntoResponse {
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
    s.files.lock().unwrap().insert(n, path);
    StatusCode::OK
}
