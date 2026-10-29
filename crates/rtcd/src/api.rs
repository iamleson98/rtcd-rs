//! The HTTP API: the endpoints of the Go service, with the same paths, authentication, status
//! codes and bodies.

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use subtle::ConstantTimeEq;
use tracing::{debug, error};

use crate::info::VersionInfo;
use crate::service::Service;

/// Request bodies are a few small fields; this is generous.
const MAX_BODY: usize = 64 * 1024;

/// Who is calling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Caller {
    /// A registered client, by id.
    Client(String),
    /// The administrator, who belongs to no client.
    Admin,
}

/// Why a caller is not accepted: the status to answer with and the message.
pub type Denied = (StatusCode, String);

/// Identifies the caller from its `Authorization` header: basic with a client id and key (or the
/// admin key, when enabled), or bearer with a token from `/login`.
pub async fn authenticate(svc: &Service, headers: &HeaderMap) -> Result<Caller, Denied> {
    let unauthorized = |m: &str| (StatusCode::UNAUTHORIZED, m.to_owned());
    let header = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default();
    if header.len() > 7 && header[..7].eq_ignore_ascii_case("bearer ") {
        let token = &header[7..];
        let client = svc
            .auth
            .sessions()
            .get(token)
            .map_err(|e| unauthorized(&format!("authentication failed: {e}")))?;
        if client.is_empty() {
            return Err(unauthorized("authentication failed: unauthorized"));
        }
        return Ok(Caller::Client(client));
    }
    let basic = header
        .split_once(' ')
        .filter(|(scheme, _)| scheme.eq_ignore_ascii_case("basic"))
        .and_then(|(_, v)| STANDARD.decode(v.trim()).ok())
        .and_then(|v| String::from_utf8(v).ok())
        .and_then(|v| v.split_once(':').map(|(a, b)| (a.to_owned(), b.to_owned())));
    let Some((id, key)) = basic else {
        return Err(unauthorized("authentication failed: invalid auth header"));
    };
    let sec = &svc.cfg.api.security;
    if sec.enable_admin && bool::from(key.as_bytes().ct_eq(sec.admin_secret_key.as_bytes())) {
        return Ok(Caller::Admin);
    }
    if id.is_empty() {
        return Err(unauthorized("authentication failed: unauthorized"));
    }
    svc.auth.authenticate(&id, &key).await.map_err(|e| {
        error!(client = %id, %e, "authentication failed");
        unauthorized("authentication failed")
    })?;
    Ok(Caller::Client(id))
}

pub fn router(svc: Arc<Service>) -> Router {
    Router::new()
        .route("/version", any(version))
        .route("/login", any(login))
        .route("/register", any(register))
        .route("/unregister", any(unregister))
        .route("/calls/{call_id}/sessions", any(sessions))
        .route("/calls/{call_id}/sessions/{session_id}", any(session))
        .route("/ws", get(crate::ws::upgrade))
        .route("/metrics", any(metrics))
        .route("/system", any(system))
        .layer(DefaultBodyLimit::max(MAX_BODY))
        .with_state(svc)
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "404 page not found\n").into_response()
}

/// The JSON answer of the credential endpoints: a flat string map with the status as `code`, and
/// `error` when there is one.
fn answer(
    code: StatusCode,
    mut fields: serde_json::Map<String, serde_json::Value>,
    error: Option<String>,
) -> Response {
    if let Some(e) = error {
        fields.insert("error".into(), e.into());
    }
    fields.insert("code".into(), code.as_u16().to_string().into());
    (
        code,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::Value::Object(fields).to_string() + "\n",
    )
        .into_response()
}

fn failure(code: StatusCode, error: impl Into<String>) -> Response {
    answer(code, serde_json::Map::new(), Some(error.into()))
}

fn client_request(
    body: &[u8],
) -> Result<std::collections::BTreeMap<String, String>, Box<Response>> {
    serde_json::from_slice(body)
        .map_err(|e| Box::new(failure(StatusCode::BAD_REQUEST, e.to_string())))
}

async fn version(method: axum::http::Method) -> Response {
    if method != axum::http::Method::GET {
        return not_found();
    }
    json_response(&VersionInfo::current())
}

fn json_response(v: &impl serde::Serialize) -> Response {
    match serde_json::to_string(v) {
        Ok(s) => ([(header::CONTENT_TYPE, "application/json")], s + "\n").into_response(),
        Err(e) => {
            error!(%e, "failed to encode a response");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn login(
    State(svc): State<Arc<Service>>,
    method: axum::http::Method,
    body: Bytes,
) -> Response {
    if method != axum::http::Method::POST {
        return not_found();
    }
    let req = match client_request(&body) {
        Ok(r) => r,
        Err(r) => return *r,
    };
    let (id, key) = (
        req.get("clientID").map_or("", String::as_str),
        req.get("authKey").map_or("", String::as_str),
    );
    match svc.auth.login(id, key).await {
        Ok(token) => {
            debug!(client = id, "logged in");
            let mut f = serde_json::Map::new();
            f.insert("bearerToken".into(), token.into());
            answer(StatusCode::OK, f, None)
        }
        Err(e) => failure(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

async fn register(
    State(svc): State<Arc<Service>>,
    method: axum::http::Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method != axum::http::Method::POST {
        return not_found();
    }
    if !svc.cfg.api.security.allow_self_registration
        && let Err((code, msg)) = authenticate(&svc, &headers).await
    {
        return failure(code, msg);
    }
    let req = match client_request(&body) {
        Ok(r) => r,
        Err(r) => return *r,
    };
    let (id, key) = (
        req.get("clientID").map_or("", String::as_str),
        req.get("authKey").map_or("", String::as_str),
    );
    match svc.auth.register(id, key).await {
        Ok(()) => {
            tracing::info!(client = id, "registered a new client");
            let mut f = serde_json::Map::new();
            f.insert("clientID".into(), id.into());
            answer(StatusCode::CREATED, f, None)
        }
        Err(e) => failure(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

async fn unregister(
    State(svc): State<Arc<Service>>,
    method: axum::http::Method,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if method != axum::http::Method::POST {
        return not_found();
    }
    let sec = &svc.cfg.api.security;
    if !sec.enable_admin && !sec.allow_self_registration {
        tracing::warn!(
            "/unregister was called, but enable_admin and allow_self_registration are both false"
        );
        return failure(StatusCode::FORBIDDEN, "unregister not enabled");
    }
    let caller = match authenticate(&svc, &headers).await {
        Ok(c) => c,
        Err((code, msg)) => return failure(code, msg),
    };
    let req = match client_request(&body) {
        Ok(r) => r,
        Err(r) => return *r,
    };
    let id = req.get("clientID").map_or("", String::as_str);
    if id.is_empty() {
        return failure(StatusCode::BAD_REQUEST, "client id should not be empty");
    }
    // A client may remove itself; only the administrator may remove another.
    if let Caller::Client(me) = &caller
        && me != id
    {
        return failure(StatusCode::FORBIDDEN, "client id not valid");
    }
    match svc.auth.unregister(id) {
        Ok(()) => {
            tracing::info!(client = id, "unregistered a client");
            answer(StatusCode::OK, serde_json::Map::new(), None)
        }
        Err(e) => failure(StatusCode::BAD_REQUEST, e.to_string()),
    }
}

/// Authenticates a lookup, which needs a client (the administrator has no calls).
async fn lookup_client(svc: &Service, headers: &HeaderMap) -> Result<String, Box<Response>> {
    match authenticate(svc, headers).await {
        Ok(Caller::Client(id)) => Ok(id),
        Ok(Caller::Admin) => Err(Box::new(
            (StatusCode::UNAUTHORIZED, "unauthorized\n").into_response(),
        )),
        Err((_, msg)) => {
            error!(%msg, "failed to authenticate");
            Err(Box::new(
                (StatusCode::UNAUTHORIZED, "unauthorized\n").into_response(),
            ))
        }
    }
}

async fn sessions(
    State(svc): State<Arc<Service>>,
    method: axum::http::Method,
    headers: HeaderMap,
    Path(call_id): Path<String>,
) -> Response {
    if method != axum::http::Method::GET {
        return not_found();
    }
    let client = match lookup_client(&svc, &headers).await {
        Ok(c) => c,
        Err(r) => return *r,
    };
    let sessions = svc.sfu.call_sessions(&client, &call_id);
    if sessions.is_empty() {
        return (
            StatusCode::NOT_FOUND,
            "failed to get session configs: call not found\n",
        )
            .into_response();
    }
    json_response(&sessions)
}

async fn session(
    State(svc): State<Arc<Service>>,
    method: axum::http::Method,
    headers: HeaderMap,
    Path((call_id, session_id)): Path<(String, String)>,
) -> Response {
    if method != axum::http::Method::GET {
        return not_found();
    }
    let client = match lookup_client(&svc, &headers).await {
        Ok(c) => c,
        Err(r) => return *r,
    };
    match svc.sfu.session(&client, &call_id, &session_id) {
        Some(cfg) => json_response(&cfg),
        None => {
            let why = if svc.sfu.call_sessions(&client, &call_id).is_empty() {
                "call not found"
            } else {
                "session not found"
            };
            (
                StatusCode::NOT_FOUND,
                format!("failed to get session config: {why}\n"),
            )
                .into_response()
        }
    }
}

async fn metrics(State(svc): State<Arc<Service>>) -> Response {
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        svc.metrics.render(),
    )
        .into_response()
}

async fn system(State(svc): State<Arc<Service>>, method: axum::http::Method) -> Response {
    if method != axum::http::Method::GET {
        return not_found();
    }
    // `cpu_load` is the Go service's key; the rest is for choosing the instance a new call goes to.
    #[derive(serde::Serialize)]
    struct Body {
        cpu_load: f64,
        /// Sessions running on this instance.
        sessions: usize,
        /// Threads serving them.
        shards: usize,
        /// Of those, the ones with no room left, shedding load (small video layers, fewer voices).
        shards_pressured: usize,
        /// Of those, the ones at their limit: they refuse new sessions.
        shards_saturated: usize,
        /// What the instance sends now, in bits per second.
        egress_bps: u64,
        /// What the instance sends now, in packets per second.
        egress_pps: u64,
        /// Datagrams the kernel refused to queue since the instance started.
        kernel_send_drops: u64,
        /// Receivers are held to the small video layers: a send budget or the node's own learned
        /// ceiling is reached.
        egress_squeezed: bool,
        /// The send rate at which the kernel started refusing datagrams, as the node learned it,
        /// in bits and in packets per second (0 until it has).
        egress_ceiling_bps: u64,
        egress_ceiling_pps: u64,
    }
    json_response(&Body {
        cpu_load: svc.system.info().cpu_load,
        sessions: svc.sfu.session_count(),
        shards: svc.sfu.shard_count(),
        shards_pressured: svc.sfu.shards_pressured(),
        shards_saturated: svc.sfu.shards_saturated(),
        egress_bps: svc.sfu.egress_bps(),
        egress_pps: svc.sfu.egress_pps(),
        kernel_send_drops: svc.sfu.kernel_send_drops(),
        egress_squeezed: svc.sfu.egress_squeezed(),
        egress_ceiling_bps: svc.sfu.egress_ceiling_bps(),
        egress_ceiling_pps: svc.sfu.egress_ceiling_pps(),
    })
}
