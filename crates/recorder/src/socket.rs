//! The app's realtime connection (Mattermost's `/api/v4/websocket`), as the recorder bot.
//!
//! It reconnects like the web app's client does: with the connection id and the next expected
//! sequence number, so the server resumes the same connection and replays what was missed. When it
//! cannot (the connection expired, or events were lost), the socket says so and the recorder
//! re-reads its state.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::sync::mpsc;
use tokio::time::{Instant, timeout};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::config::Config;

const DIAL_TIMEOUT: Duration = Duration::from_secs(15);
const BACKOFF_MAX: Duration = Duration::from_secs(10);
/// Keeps idle connections open through proxies.
const PING_EVERY: Duration = Duration::from_secs(30);
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Made by the socket itself: the connection was replaced or events were missed.
pub const EVENT_RESYNC: &str = "recorder_resync";

#[derive(Debug, Default, Deserialize)]
pub struct Broadcast {
    #[serde(default)]
    pub channel_id: String,
}

/// One server event.
#[derive(Debug, Default, Deserialize)]
pub struct Event {
    #[serde(default)]
    pub event: String,
    #[serde(default)]
    pub data: Option<Box<RawValue>>,
    #[serde(default)]
    pub broadcast: Broadcast,
    #[serde(default)]
    pub seq: i64,
}

impl Event {
    pub fn named(name: &str) -> Self {
        Self {
            event: name.to_owned(),
            ..Self::default()
        }
    }

    /// The event's data, or `null`.
    pub fn data(&self) -> &str {
        self.data.as_ref().map_or("null", |d| d.get())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("realtime connection is offline")]
pub struct Offline;

/// Sends actions (fire and forget, like the web app's notify).
#[derive(Debug, Clone)]
pub struct Sender {
    tx: mpsc::Sender<String>,
    online: Arc<AtomicBool>,
    action: Arc<AtomicI64>,
}

impl Sender {
    /// A sender with no connection behind it, for tests: it is always offline.
    #[cfg(test)]
    pub fn detached(tx: mpsc::Sender<String>) -> Self {
        Self {
            tx,
            online: Arc::new(AtomicBool::new(false)),
            action: Arc::new(AtomicI64::new(0)),
        }
    }

    pub fn send(&self, action: &str, data: impl serde::Serialize) -> Result<(), Offline> {
        if !self.online.load(Ordering::Acquire) {
            return Err(Offline);
        }
        let seq = self.action.fetch_add(1, Ordering::Relaxed) + 1;
        let frame = serde_json::json!({ "seq": seq, "action": action, "data": data }).to_string();
        self.tx.try_send(frame).map_err(|_| Offline)
    }
}

/// The connection: its events, and a handle to send with.
#[derive(Debug)]
pub struct Socket {
    pub events: mpsc::Receiver<Event>,
    pub sender: Sender,
    /// Fires once the first connection is established.
    pub ready: Arc<tokio::sync::Notify>,
    pub ready_flag: Arc<AtomicBool>,
}

impl Socket {
    /// Starts keeping the connection up until `cancel` fires.
    pub fn start(cfg: &Config, cancel: CancellationToken) -> Self {
        let (ev_tx, events) = mpsc::channel(1024);
        let (out_tx, out_rx) = mpsc::channel(256);
        let online = Arc::new(AtomicBool::new(false));
        let ready = Arc::new(tokio::sync::Notify::new());
        let ready_flag = Arc::new(AtomicBool::new(false));
        let task = Task {
            url: cfg.socket_url(),
            token: cfg.auth_token.clone(),
            events: ev_tx,
            out: out_rx,
            online: online.clone(),
            ready: ready.clone(),
            ready_flag: ready_flag.clone(),
            conn_id: String::new(),
            next_seq: 0,
            cancel,
        };
        tokio::spawn(task.run());
        Self {
            events,
            sender: Sender {
                tx: out_tx,
                online,
                action: Arc::new(AtomicI64::new(0)),
            },
            ready,
            ready_flag,
        }
    }

    /// Waits for the first connection.
    pub async fn wait_ready(&self) {
        while !self.ready_flag.load(Ordering::Acquire) {
            self.ready.notified().await;
        }
    }
}

struct Task {
    url: String,
    token: String,
    events: mpsc::Sender<Event>,
    out: mpsc::Receiver<String>,
    online: Arc<AtomicBool>,
    ready: Arc<tokio::sync::Notify>,
    ready_flag: Arc<AtomicBool>,
    conn_id: String,
    next_seq: i64,
    cancel: CancellationToken,
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

impl Task {
    async fn run(mut self) {
        let mut backoff = Duration::from_secs(1);
        let mut first = true;
        while !self.cancel.is_cancelled() {
            let ws = match self.dial().await {
                Ok(ws) => ws,
                Err(e) => {
                    warn!(error = %e, retry_ms = backoff.as_millis() as u64, "realtime connection failed");
                    tokio::select! {
                        () = self.cancel.cancelled() => return,
                        () = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(BACKOFF_MAX);
                    continue;
                }
            };
            backoff = Duration::from_secs(1);
            self.online.store(true, Ordering::Release);
            let result = self.serve(ws, first).await;
            first = false;
            self.online.store(false, Ordering::Release);
            // Actions queued for a connection that is gone are not replayed on the next.
            while self.out.try_recv().is_ok() {}
            if !self.cancel.is_cancelled() {
                warn!(error = %result.unwrap_err_or_default(), "realtime connection lost");
            }
        }
    }

    async fn dial(&self) -> Result<Ws, String> {
        let mut url = self.url.clone();
        if !self.conn_id.is_empty() {
            url = format!(
                "{url}?connection_id={}&sequence_number={}",
                self.conn_id, self.next_seq
            );
        }
        let mut req = url.into_client_request().map_err(|e| e.to_string())?;
        req.headers_mut().insert(
            "Authorization",
            format!("Bearer {}", self.token)
                .parse()
                .map_err(|_| "the token is not a header value".to_owned())?,
        );
        match timeout(DIAL_TIMEOUT, tokio_tungstenite::connect_async(req)).await {
            Err(_) => Err("dial timed out".into()),
            Ok(Err(e)) => Err(e.to_string()),
            Ok(Ok((ws, _))) => Ok(ws),
        }
    }

    /// Delivers events until the connection breaks. The hello tells whether the server resumed our
    /// connection; a sequence gap means events were lost. Either way the recorder resyncs.
    async fn serve(&mut self, ws: Ws, first: bool) -> Result<(), String> {
        let (mut write, mut read) = ws.split();
        let mut ping = tokio::time::interval_at(Instant::now() + PING_EVERY, PING_EVERY);
        let mut first = first;
        loop {
            tokio::select! {
                () = self.cancel.cancelled() => {
                    let _ = write.send(Message::Close(None)).await;
                    return Ok(());
                }
                _ = ping.tick() => {
                    timeout(WRITE_TIMEOUT, write.send(Message::Ping(Vec::new().into())))
                        .await
                        .map_err(|_| "ping timed out".to_owned())?
                        .map_err(|e| e.to_string())?;
                }
                Some(frame) = self.out.recv() => {
                    timeout(WRITE_TIMEOUT, write.send(Message::text(frame)))
                        .await
                        .map_err(|_| "write timed out".to_owned())?
                        .map_err(|e| e.to_string())?;
                }
                msg = read.next() => {
                    let msg = match msg {
                        None => return Err("closed".into()),
                        Some(Err(e)) => return Err(e.to_string()),
                        Some(Ok(m)) => m,
                    };
                    let text = match msg {
                        Message::Text(t) => t,
                        Message::Close(_) => return Err("closed by the server".into()),
                        _ => continue,
                    };
                    let ev: Event = match serde_json::from_str(&text) {
                        Ok(e) => e,
                        Err(e) => return Err(format!("unreadable frame: {e}")),
                    };
                    if ev.event.is_empty() {
                        continue; // a reply to one of our actions
                    }
                    if ev.event == "hello" {
                        self.hello(&ev, &mut first).await;
                        continue;
                    }
                    if ev.seq != self.next_seq {
                        self.deliver(Event::named(EVENT_RESYNC)).await;
                    }
                    self.next_seq = ev.seq + 1;
                    self.deliver(ev).await;
                }
            }
        }
    }

    async fn hello(&mut self, ev: &Event, first: &mut bool) {
        #[derive(Deserialize)]
        struct Hello {
            #[serde(default)]
            connection_id: String,
        }
        let parsed = serde_json::from_str::<Hello>(ev.data());
        if let Err(e) = &parsed {
            warn!(error = %e, "unreadable hello from the realtime connection");
        }
        let id = parsed.map(|h| h.connection_id).unwrap_or_default();
        // A hello that names no connection cannot prove the server kept ours.
        let resumed = !id.is_empty() && id == self.conn_id;
        self.conn_id = id;
        self.next_seq = ev.seq + 1;
        if *first {
            *first = false;
            self.ready_flag.store(true, Ordering::Release);
            self.ready.notify_waiters();
            self.ready.notify_one();
        } else {
            if !resumed {
                self.deliver(Event::named(EVENT_RESYNC)).await;
            }
        }
    }

    async fn deliver(&self, ev: Event) {
        tokio::select! {
            _ = self.events.send(ev) => {}
            () = self.cancel.cancelled() => {}
        }
    }
}

trait UnwrapErrOrDefault {
    fn unwrap_err_or_default(self) -> String;
}

impl UnwrapErrOrDefault for Result<(), String> {
    fn unwrap_err_or_default(self) -> String {
        self.err().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::{Value, json};
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_hdr_async;
    use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};

    use super::*;

    type Conn = tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>;

    /// Plays the app's websocket: `serve` handles each connection (its query, authorization and
    /// index, from zero).
    async fn server<F, Fut>(serve: F) -> String
    where
        F: Fn(Conn, String, String, usize) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let serve = Arc::new(serve);
        tokio::spawn(async move {
            let mut n = 0;
            while let Ok((tcp, _)) = listener.accept().await {
                let seen = Arc::new(Mutex::new((String::new(), String::new())));
                let s2 = seen.clone();
                #[allow(clippy::result_large_err)]
                let cb = move |req: &Request, resp: Response| {
                    *s2.lock().unwrap() = (
                        req.uri().query().unwrap_or("").to_owned(),
                        req.headers()
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or("")
                            .to_owned(),
                    );
                    Ok(resp)
                };
                let Ok(ws) = accept_hdr_async(tcp, cb).await else {
                    continue;
                };
                let (q, a) = seen.lock().unwrap().clone();
                let serve = serve.clone();
                let i = n;
                n += 1;
                tokio::spawn(async move { serve(ws, q, a, i).await });
            }
        });
        url
    }

    async fn say(ws: &mut Conn, v: Value) {
        ws.send(Message::text(v.to_string())).await.unwrap();
    }

    fn hello(id: &str, seq: i64) -> Value {
        json!({"event": "hello", "seq": seq, "data": {"connection_id": id}})
    }

    fn config(url: &str) -> Config {
        Config {
            site_url: url.into(),
            auth_token: "bot-token".into(),
            recording_id: "r".repeat(26),
            max_duration: Duration::from_secs(60),
            max_data: 2 << 30,
        }
    }

    async fn next(s: &mut Socket) -> Event {
        timeout(Duration::from_secs(5), s.events.recv())
            .await
            .expect("no event")
            .expect("closed")
    }

    async fn names(s: &mut Socket, n: usize) -> Vec<String> {
        let mut out = Vec::new();
        for _ in 0..n {
            out.push(next(s).await.event);
        }
        out
    }

    #[tokio::test]
    async fn events_come_in_order_and_replies_are_skipped() {
        let auth = Arc::new(Mutex::new(String::new()));
        let a2 = auth.clone();
        let url = server(move |mut ws, _, a, _| {
            let a2 = a2.clone();
            async move {
                *a2.lock().unwrap() = a;
                say(&mut ws, hello("c1", 0)).await;
                say(&mut ws, json!({"status": "OK", "seq_reply": 1})).await;
                say(
                    &mut ws,
                    json!({"event": "posted", "seq": 1, "broadcast": {"channel_id": "ch"}}),
                )
                .await;
                let _ = ws.next().await;
            }
        })
        .await;
        let cancel = CancellationToken::new();
        let mut s = Socket::start(&config(&url), cancel.clone());
        timeout(Duration::from_secs(5), s.wait_ready())
            .await
            .expect("never ready");
        let ev = next(&mut s).await;
        assert_eq!(
            (ev.event.as_str(), ev.broadcast.channel_id.as_str()),
            ("posted", "ch")
        );
        assert_eq!(*auth.lock().unwrap(), "Bearer bot-token");
        assert!(s.events.try_recv().is_err());
        cancel.cancel();
    }

    #[tokio::test]
    async fn a_gap_in_sequence_resyncs_once_before_the_event_that_revealed_it() {
        let url = server(|mut ws, _, _, _| async move {
            say(&mut ws, hello("c1", 0)).await;
            say(&mut ws, json!({"event": "a", "seq": 1})).await;
            say(&mut ws, json!({"event": "b", "seq": 5})).await; // 2-4 were lost
            say(&mut ws, json!({"event": "c", "seq": 6})).await;
            let _ = ws.next().await;
        })
        .await;
        let cancel = CancellationToken::new();
        let mut s = Socket::start(&config(&url), cancel.clone());
        assert_eq!(names(&mut s, 4).await, ["a", EVENT_RESYNC, "b", "c"]);
        cancel.cancel();
    }

    #[tokio::test]
    async fn it_reconnects_resuming_or_resyncing() {
        let queries = Arc::new(Mutex::new(Vec::new()));
        let q2 = queries.clone();
        let url = server(move |mut ws, q, _, n| {
            let q2 = q2.clone();
            async move {
                q2.lock().unwrap().push(q);
                match n {
                    0 => {
                        say(&mut ws, hello("c1", 0)).await;
                        say(&mut ws, json!({"event": "a", "seq": 1})).await;
                    }
                    // The server resumes the same connection.
                    1 => {
                        say(&mut ws, hello("c1", 2)).await;
                        say(&mut ws, json!({"event": "b", "seq": 3})).await;
                    }
                    // It cannot: a new connection.
                    _ => {
                        say(&mut ws, hello("c2", 0)).await;
                        let _ = ws.next().await;
                    }
                }
            }
        })
        .await;
        let cancel = CancellationToken::new();
        let mut s = Socket::start(&config(&url), cancel.clone());
        assert_eq!(names(&mut s, 3).await, ["a", "b", EVENT_RESYNC]);
        let q = queries.lock().unwrap().clone();
        assert_eq!(q[0], "", "the first connection asks to resume nothing");
        assert_eq!(
            q[1], "connection_id=c1&sequence_number=2",
            "the second resumes where the first stopped"
        );
        assert_eq!(
            q[2], "connection_id=c1&sequence_number=4",
            "the third resumes after event 3"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn actions_carry_increasing_sequence_numbers() {
        let offline = Socket::start(&config("http://127.0.0.1:1"), CancellationToken::new());
        assert!(offline.sender.send("x", ()).is_err());

        let frames = Arc::new(Mutex::new(Vec::<Value>::new()));
        let f2 = frames.clone();
        let url = server(move |mut ws, _, _, _| {
            let f2 = f2.clone();
            async move {
                say(&mut ws, hello("c1", 0)).await;
                while let Some(Ok(Message::Text(t))) = ws.next().await {
                    f2.lock().unwrap().push(serde_json::from_str(&t).unwrap());
                }
            }
        })
        .await;
        let cancel = CancellationToken::new();
        let s = Socket::start(&config(&url), cancel.clone());
        s.wait_ready().await;
        while s.sender.send("first", json!({"k": "v"})).is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await; // the connection is marked just after ready
        }
        s.sender.send("second", ()).unwrap();
        timeout(Duration::from_secs(5), async {
            while frames.lock().unwrap().len() < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let f = frames.lock().unwrap().clone();
        assert_eq!(
            (f[0]["action"].as_str(), f[1]["action"].as_str()),
            (Some("first"), Some("second"))
        );
        assert_eq!(
            f[0]["seq"].as_i64().unwrap() + 1,
            f[1]["seq"].as_i64().unwrap()
        );
        assert_eq!(f[0]["data"], json!({"k": "v"}));
        cancel.cancel();
    }

    #[tokio::test]
    async fn a_refused_dial_is_retried() {
        // The first connection attempt is refused at the TCP level; the second is served.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let (first, _) = listener.accept().await.unwrap();
            drop(first); // closed before the handshake
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            say(&mut ws, hello("c1", 0)).await;
            let _ = ws.next().await;
        });
        let cancel = CancellationToken::new();
        let s = Socket::start(&config(&url), cancel.clone());
        timeout(Duration::from_secs(10), s.wait_ready())
            .await
            .expect("never connected after a refusal");
        cancel.cancel();
    }

    #[tokio::test]
    async fn it_stops_with_its_token() {
        let cancel = CancellationToken::new();
        let mut s = Socket::start(&config("http://127.0.0.1:1"), cancel.clone());
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel.cancel();
        // The task ends and drops its end of the events channel.
        assert!(
            timeout(Duration::from_secs(5), s.events.recv())
                .await
                .expect("kept going")
                .is_none()
        );
    }

    #[tokio::test]
    async fn a_nameless_hello_is_never_taken_as_resumed() {
        let url = server(|mut ws, _, _, n| async move {
            match n {
                0 => say(&mut ws, hello("c1", 0)).await,
                1 => say(&mut ws, json!({"event": "hello", "seq": 5, "data": {}})).await,
                _ => {
                    say(
                        &mut ws,
                        json!({"event": "hello", "seq": 9, "data": "garbage"}),
                    )
                    .await;
                    let _ = ws.next().await;
                }
            }
        })
        .await;
        let cancel = CancellationToken::new();
        let mut s = Socket::start(&config(&url), cancel.clone());
        assert_eq!(names(&mut s, 2).await, [EVENT_RESYNC, EVENT_RESYNC]);
        cancel.cancel();
    }
}
