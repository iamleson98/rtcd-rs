//! The LMS recorder endpoints that take a recording's files, called with the bot session made for this recording
//! (`/api/v4/lms/recorder/recordings/{id}/...`), and storage, called with the URLs the server
//! presigns.

use std::sync::Arc;
use std::time::Duration;

use reqwest::{Client, Method, RequestBuilder};
use serde::de::DeserializeOwned;

use crate::throttle::Throttle;

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
fn install_tls_provider() {
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

impl Api {
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
}
