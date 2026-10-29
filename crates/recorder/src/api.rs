//! The LMS recorder endpoints, called with the bot session made for this recording
//! (`/api/v4/lms/recorder/recordings/{id}/...`), and storage, called with the URLs the server
//! presigns.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, Method, RequestBuilder, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::error;

use crate::config::Config;
use crate::media::Entry;
use crate::throttle::Throttle;
use crate::upload::MultipartDone;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    /// An answer outside 2xx; 4xx ones are not worth retrying.
    #[error("status {status}: {body}")]
    Status { status: u16, body: String },
    #[error("{0}")]
    Network(String),
}

impl ApiError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::Status { status, .. } => *status >= 500 || *status == 429,
            Self::Network(_) => true,
        }
    }

    pub fn status(&self) -> Option<u16> {
        match self {
            Self::Status { status, .. } => Some(*status),
            Self::Network(_) => None,
        }
    }
}

impl From<reqwest::Error> for ApiError {
    fn from(e: reqwest::Error) -> Self {
        // The chain names the cause (a refused connection, a timeout), which `Display` hides.
        let mut text = e.to_string();
        let mut source = std::error::Error::source(&e);
        while let Some(s) = source {
            text.push_str(": ");
            text.push_str(&s.to_string());
            source = s.source();
        }
        Self::Network(text)
    }
}

/// TLS (the server may be reached over https) runs on ring; the process chooses it once.
pub fn install_tls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// The HTTP clients uploads go through. A process that serves several recordings builds them once
/// and gives each recording's [`Api`] a handle: they hold the connection pools.
#[derive(Debug, Clone)]
pub struct HttpClients {
    client: Client,
    uploads: Client,
}

impl HttpClients {
    pub fn new() -> Self {
        install_tls_provider();
        let client = Client::builder()
            .timeout(Duration::from_secs(300))
            .user_agent("rtcd-recorder")
            .build()
            .expect("an http client");
        // Carries the files, to storage or the server: no overall timeout, each request is
        // bounded by its size.
        let uploads = Client::builder()
            .user_agent("rtcd-recorder")
            .build()
            .expect("an http client");
        Self { client, uploads }
    }
}

impl Default for HttpClients {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct Api {
    pub(crate) site: String,
    pub(crate) base: String,
    pub(crate) token: String,
    pub(crate) client: Client,
    pub(crate) uploads: Client,
    /// Retries and backoff shape the uploads' retry loop.
    pub(crate) retries: u32,
    pub(crate) backoff: Duration,
    /// Holds the bytes of every upload to a rate, if set.
    pub(crate) throttle: Option<Arc<Throttle>>,
}

impl std::fmt::Debug for Api {
    /// Never shows the token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Api")
            .field("base", &self.base)
            .field("token", &"<redacted>")
            .finish_non_exhaustive()
    }
}

/// What the recorder needs to know about its recording.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct Room {
    #[serde(default)]
    pub session_id: String,
    #[serde(default)]
    pub chat_channel_id: String,
    #[serde(default)]
    pub stop_at: i64,
    #[serde(default)]
    pub status: String,
}

impl Api {
    pub fn new(cfg: &Config) -> Self {
        Self::with_clients(
            &HttpClients::new(),
            &cfg.site_url,
            &cfg.recording_id,
            &cfg.auth_token,
        )
    }

    /// The API of one recording, called with its bot session's `token`, through `clients`.
    pub fn with_clients(
        clients: &HttpClients,
        site_url: &str,
        recording_id: &str,
        token: &str,
    ) -> Self {
        let site = site_url.trim_end_matches('/').to_owned();
        Self {
            base: format!("{site}/api/v4/lms/recorder/recordings/{recording_id}"),
            site,
            token: token.to_owned(),
            client: clients.client.clone(),
            uploads: clients.uploads.clone(),
            retries: 8,
            backoff: Duration::from_secs(2),
            throttle: None,
        }
    }

    /// Holds every upload made through this to the bucket's rate.
    pub fn throttled(mut self, throttle: Arc<Throttle>) -> Self {
        self.throttle = Some(throttle);
        self
    }

    /// How often a request is tried again after a network or server error, and how long the first
    /// wait is (the n-th is n times as long).
    pub fn retrying(mut self, retries: u32, backoff: Duration) -> Self {
        self.retries = retries;
        self.backoff = backoff;
        self
    }

    pub(crate) fn authed(&self, method: Method, url: &str) -> RequestBuilder {
        self.client.request(method, url).bearer_auth(&self.token)
    }

    /// Sends the request; an answer outside 2xx is a status error.
    pub(crate) async fn send(req: RequestBuilder) -> Result<reqwest::Response, ApiError> {
        let resp = req.send().await?;
        if resp.status().is_success() {
            return Ok(resp);
        }
        let status = resp.status().as_u16();
        let mut body = resp.bytes().await.map(|b| b.to_vec()).unwrap_or_default();
        body.truncate(512);
        Err(ApiError::Status {
            status,
            body: String::from_utf8_lossy(&body).into_owned(),
        })
    }

    pub(crate) async fn json<T: DeserializeOwned>(req: RequestBuilder) -> Result<T, ApiError> {
        Ok(Self::send(req).await?.json().await?)
    }

    pub async fn room(&self) -> Result<Room, ApiError> {
        Self::json(self.authed(Method::GET, &self.base)).await
    }

    /// Whether the channel has a call in progress (the server answers 404 when it has none).
    pub async fn call_active(&self, channel_id: &str) -> Result<bool, ApiError> {
        let url = format!("{}/api/v4/calls/channels/{channel_id}", self.site);
        match Self::send(self.authed(Method::GET, &url)).await {
            Ok(_) => Ok(true),
            Err(e) if e.status() == Some(StatusCode::NOT_FOUND.as_u16()) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn post_json(&self, path: &str, body: &impl Serialize) -> Result<(), ApiError> {
        Self::send(
            self.authed(Method::POST, &format!("{}{path}", self.base))
                .json(body),
        )
        .await?;
        Ok(())
    }

    /// Reports that capture began.
    pub async fn started(&self) -> Result<(), ApiError> {
        self.post_json("/started", &serde_json::json!({})).await
    }

    /// Reports the recording's length, its files (in manifest order) and the uploads in parts to
    /// finish once every file is uploaded.
    pub async fn complete(
        &self,
        duration: Duration,
        files: &[Entry],
        multipart: &[MultipartDone],
    ) -> Result<(), ApiError> {
        self.post_json(
            "/complete",
            &serde_json::json!({
                "duration_ms": duration.as_millis().max(1) as u64,
                "files": files,
                "multipart": multipart,
            }),
        )
        .await
    }

    /// Gives the recording up; the server tells the teacher.
    pub async fn fail(&self, reason: &str) {
        if let Err(e) = self
            .post_json("/fail", &serde_json::json!({ "error": reason }))
            .await
        {
            error!(error = %e, "could not report the failure");
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::testing::{Fake, REC_PATH};

    #[tokio::test]
    async fn fail_reports_the_reason() {
        let fake = Fake::start(|_, _, _| (200, None)).await;
        fake.api().fail("no room").await;
        let seen = fake.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].path, format!("{REC_PATH}/fail"));
        assert!(String::from_utf8_lossy(&seen[0].body).contains(r#""error":"no room""#));
        assert_eq!(seen[0].auth, "Bearer bot-token");

        // A refusal is logged, not raised.
        Fake::start(|_, _, _| (409, None))
            .await
            .api()
            .fail("x")
            .await;
    }

    #[tokio::test]
    async fn call_active_reads_the_status() {
        for (status, want) in [(200, true), (404, false)] {
            let fake = Fake::start(move |_, _, _| (status, None)).await;
            assert_eq!(
                fake.api().call_active("chan").await.unwrap(),
                want,
                "status {status}"
            );
            assert_eq!(fake.seen()[0].path, "/api/v4/calls/channels/chan");
        }
        let fake = Fake::start(|_, _, _| (500, None)).await;
        assert!(
            fake.api().call_active("chan").await.is_err(),
            "a server error is an error, not 'no call'"
        );
    }

    #[tokio::test]
    async fn the_room_is_read_with_the_bot_token() {
        let fake = Fake::start(|_, _, _| {
            (200, Some(serde_json::json!({"session_id": "s1", "chat_channel_id": "c1", "stop_at": 5, "status": "RECORDING"})))
        })
        .await;
        let api = fake.api();
        let room = api.room().await.unwrap();
        assert_eq!(
            (
                room.session_id.as_str(),
                room.chat_channel_id.as_str(),
                room.stop_at,
                room.status.as_str()
            ),
            ("s1", "c1", 5, "RECORDING")
        );
        let seen = fake.seen();
        assert_eq!(seen[0].path, REC_PATH);
        assert!(seen.iter().all(|r| r.auth == "Bearer bot-token"));
    }
}
