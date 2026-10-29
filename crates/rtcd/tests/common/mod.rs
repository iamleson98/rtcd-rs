//! A whole service in-process: the HTTP and WebSocket API and the SFU on loopback, for tests.
#![allow(dead_code)]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use rtcd::app::App;
use rtcd::config::Config;
use rtcd_proto::{ClientMessage, JoinRequest, Message, MessageType, SessionRef};
use rtcd_sfu::config::LocalAddress;
use rtcd_testkit::{Browser, Signal};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{self, Message as WsMessage};

pub const KEY: &str = "0123456789abcdef0123456789abcdef";
pub const ADMIN_KEY: &str = "admin-secret-admin-secret-0123456";

pub struct TestServer {
    pub app: Option<App>,
    pub base: String,
    pub dir: PathBuf,
    pub media: SocketAddr,
    pub http: reqwest::Client,
}

impl TestServer {
    pub async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    pub async fn start_with(configure: impl FnOnce(&mut Config)) -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "rtcd-it-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = Config::default();
        cfg.api.http.listen_address = "127.0.0.1:0".into();
        cfg.api.security.allow_self_registration = true;
        cfg.store.data_source = dir.display().to_string();
        cfg.logger.enable_file = false;
        configure(&mut cfg);
        let addresses = vec![LocalAddress {
            bind: "127.0.0.1".parse().unwrap(),
            public: None,
            udp_port: 0,
            advertised_udp_port: None,
            tcp_port: None,
            advertised_tcp_port: None,
            advertise: true,
        }];
        let app = App::start_with(cfg, addresses)
            .await
            .expect("the service starts");
        let media = app.service.sfu.addresses()[0].advertised_udp();
        let base = format!("http://{}", app.http_addr);
        Self {
            app: Some(app),
            base,
            dir,
            media,
            http: reqwest::Client::new(),
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    pub async fn register(&self, id: &str) {
        let r = self
            .http
            .post(self.url("/register"))
            .json(&serde_json::json!({ "clientID": id, "authKey": KEY }))
            .send()
            .await
            .unwrap();
        assert_eq!(r.status(), 201, "{:?}", r.text().await);
    }

    pub fn basic(id: &str, key: &str) -> String {
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{id}:{key}"))
        )
    }

    /// Opens a websocket as `id` and returns it after its hello.
    pub async fn connect(&self, id: &str) -> WsClient {
        self.connect_with(&Self::basic(id, KEY))
            .await
            .expect("the websocket opens")
    }

    pub async fn connect_with(&self, authorization: &str) -> Result<WsClient, tungstenite::Error> {
        let mut req = format!("ws://{}/ws", self.base.trim_start_matches("http://"))
            .into_client_request()
            .unwrap();
        req.headers_mut()
            .insert("Authorization", authorization.parse().unwrap());
        let (stream, _) = tokio_tungstenite::connect_async(req).await?;
        let mut client = WsClient {
            stream,
            conn_id: String::new(),
            client_id: String::new(),
            reports: Vec::new(),
        };
        let ClientMessage::Hello(h) = client.next_message().await else {
            panic!("the first message is a hello")
        };
        client.conn_id = h.conn_id;
        client.client_id = h.client_id;
        Ok(client)
    }

    pub async fn shutdown(mut self) {
        if let Some(app) = self.app.take() {
            app.shutdown(Some(Duration::from_millis(200))).await;
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub struct WsClient {
    stream: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    pub conn_id: String,
    pub client_id: String,
    /// The recording reports (`record_file`, `record_ended`) a participant's pass read past, in
    /// the order they came.
    pub reports: Vec<ClientMessage>,
}

impl WsClient {
    pub async fn send(&mut self, msg: &ClientMessage) {
        self.stream
            .send(WsMessage::Binary(msg.pack().into()))
            .await
            .unwrap();
    }

    pub async fn join(&mut self, call: &str, session: &str) {
        self.send(&ClientMessage::Join(JoinRequest {
            call_id: call.into(),
            user_id: format!("user-{session}"),
            session_id: session.into(),
            channel_id: "channel".into(),
            av1_support: false,
            dc_signaling: true,
            host: false,
            observer: false,
        }))
        .await;
    }

    pub async fn signal(&mut self, session: &str, s: Signal) {
        self.send(&ClientMessage::Rtc(Message {
            group_id: "default".into(),
            user_id: String::new(),
            session_id: session.into(),
            call_id: "call".into(),
            kind: s.kind,
            data: s.data,
        }))
        .await;
    }

    pub async fn leave(&mut self, session: &str) {
        self.send(&ClientMessage::Leave(SessionRef {
            session_id: session.into(),
        }))
        .await;
    }

    /// The next message, or `None` if none arrives within `wait`.
    pub async fn try_next(&mut self, wait: Duration) -> Option<ClientMessage> {
        loop {
            match tokio::time::timeout(wait, self.stream.next()).await {
                Err(_) | Ok(None) => return None,
                Ok(Some(Err(_))) => return None,
                Ok(Some(Ok(WsMessage::Binary(b)))) => {
                    return Some(
                        ClientMessage::unpack(&b).expect("a message the service wrote decodes"),
                    );
                }
                Ok(Some(Ok(WsMessage::Close(_)))) => return None,
                Ok(Some(Ok(_))) => {}
            }
        }
    }

    pub async fn next_message(&mut self) -> ClientMessage {
        self.try_next(Duration::from_secs(10))
            .await
            .expect("a message arrives")
    }

    pub async fn closed(&mut self, wait: Duration) -> bool {
        let end = tokio::time::Instant::now() + wait;
        while tokio::time::Instant::now() < end {
            match tokio::time::timeout(Duration::from_millis(50), self.stream.next()).await {
                Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(WsMessage::Close(_)))) => return true,
                _ => {}
            }
        }
        false
    }
}

/// An emulated browser whose signaling goes through a websocket of the service.
pub struct Participant {
    pub browser: Browser,
    pub session: String,
    pub voice: Vec<bool>,
    pub closed: bool,
}

impl Participant {
    /// Joins `call` through `ws` and negotiates until the media connection and data channel are up.
    pub async fn join(
        ws: &mut WsClient,
        media: SocketAddr,
        name: &str,
        call: &str,
        ssrc: u32,
    ) -> Self {
        let session = format!("session-{name}");
        ws.join(call, &session).await;
        let (browser, first) = Browser::new(name, &session, media, ssrc);
        let mut p = Self {
            browser,
            session,
            voice: vec![],
            closed: false,
        };
        for s in first {
            ws.signal(&p.session, s).await;
        }
        let end = tokio::time::Instant::now() + Duration::from_secs(15);
        while !(p.browser.connected() && p.browser.data_channel_open()) {
            assert!(tokio::time::Instant::now() < end, "{name} did not connect");
            p.pump(ws).await;
        }
        p
    }

    /// One pass: the browser's sockets, then whatever the service has for it.
    pub async fn pump(&mut self, ws: &mut WsClient) {
        for s in self.browser.poll() {
            ws.signal(&self.session, s).await;
        }
        while let Some(m) = ws.try_next(Duration::from_millis(1)).await {
            match m {
                ClientMessage::Rtc(m) if m.session_id == self.session => {
                    for s in self.browser.handle_signal(m.kind, &m.data) {
                        ws.signal(&self.session, s).await;
                    }
                }
                ClientMessage::Vad(m) if m.session_id == self.session => {
                    self.voice.push(m.kind == MessageType::VoiceOn)
                }
                ClientMessage::Close(r) if r.session_id == self.session => self.closed = true,
                m @ (ClientMessage::RecordFile(_) | ClientMessage::RecordEnded(_)) => {
                    ws.reports.push(m)
                }
                _ => {}
            }
        }
    }

    pub async fn speak(&mut self, ws: &mut WsClient, frames: usize) {
        for f in 0..frames {
            self.browser.send_voice(if f % 2 == 0 { 20 } else { 70 });
            self.pump(ws).await;
            tokio::time::sleep(Duration::from_millis(15)).await;
        }
    }
}
