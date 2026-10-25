//! A real rtcd in this process, and a client of its websocket API.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use base64::Engine;
use futures_util::{SinkExt, StreamExt};
use rtcd::app::App;
use rtcd::config::Config;
use rtcd_proto::ClientMessage;
use rtcd_sfu::config::LocalAddress;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

pub const KEY: &str = "0123456789abcdef0123456789abcdef";
pub const CLIENT: &str = "sim";

pub struct Rtcd {
    app: Option<App>,
    pub http: String,
    /// Where clients send media: rtcd's own address, or the lossy relay in front of it.
    pub media: SocketAddr,
    dir: PathBuf,
    relay: Option<Arc<Relay>>,
}

impl std::fmt::Debug for Rtcd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rtcd")
            .field("http", &self.http)
            .field("media", &self.media)
            .finish()
    }
}

impl Rtcd {
    /// Starts the service on loopback with one registered client.
    pub async fn start() -> Self {
        Self::start_lossy(0.0).await
    }

    /// Like [`start`](Self::start), with a fraction of the packets from rtcd to the clients that are
    /// not exempt (see [`exempt`](Self::exempt)) lost on the way.
    pub async fn start_lossy(loss: f64) -> Self {
        Self::start_with(loss, 0).await
    }

    /// Like [`start_lossy`](Self::start_lossy), and every 5 s the path to the clients that are not
    /// exempt is cut for `blackout_ms`: more than a retransmission can make up for.
    pub async fn start_with(loss: f64, blackout_ms: u64) -> Self {
        let _ = rustls::crypto::ring::default_provider().install_default();
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lms-sim-rtcd-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = Config::default();
        cfg.api.http.listen_address = "127.0.0.1:0".into();
        cfg.api.security.allow_self_registration = true;
        cfg.store.data_source = dir.display().to_string();
        cfg.logger.enable_file = false;
        cfg.logger.enable_console = false;
        // Behind a lossy relay, the candidates rtcd offers name the relay's port.
        let front = if loss > 0.0 || blackout_ms > 0 {
            Some(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap())
        } else {
            None
        };
        let addresses = vec![LocalAddress {
            bind: "127.0.0.1".parse().unwrap(),
            public: None,
            udp_port: 0,
            advertised_udp_port: front.as_ref().map(|f| f.local_addr().unwrap().port()),
            tcp_port: None,
            advertised_tcp_port: None,
            advertise: true,
        }];
        let app = App::start_with(cfg, addresses)
            .await
            .expect("the service starts");
        let local = &app.service.sfu.addresses()[0];
        let media = local.advertised_udp();
        let relay = front.map(|front| {
            Relay::start(
                front,
                SocketAddr::new(local.bind, local.udp_port),
                loss,
                blackout_ms,
            )
        });
        let http = format!("http://{}", app.http_addr);
        let r = reqwest::Client::new()
            .post(format!("{http}/register"))
            .json(&serde_json::json!({ "clientID": CLIENT, "authKey": KEY }))
            .send()
            .await
            .expect("register");
        assert_eq!(r.status(), 201);
        Self {
            app: Some(app),
            http,
            media,
            dir,
            relay,
        }
    }

    /// Opens a websocket as the registered client.
    pub async fn connect(&self) -> RtcdConn {
        let auth = format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{CLIENT}:{KEY}"))
        );
        let mut req = format!("ws://{}/ws", self.http.trim_start_matches("http://"))
            .into_client_request()
            .unwrap();
        req.headers_mut()
            .insert("Authorization", auth.parse().unwrap());
        let (stream, _) = tokio_tungstenite::connect_async(req)
            .await
            .expect("the websocket opens");
        let (mut write, mut read) = stream.split();
        let (tx, mut out) = mpsc::unbounded_channel::<ClientMessage>();
        let (in_tx, rx) = mpsc::unbounded_channel::<ClientMessage>();
        tokio::spawn(async move {
            while let Some(m) = out.recv().await {
                if write
                    .send(WsMessage::Binary(m.pack().into()))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        });
        tokio::spawn(async move {
            while let Some(Ok(m)) = read.next().await {
                if let WsMessage::Binary(b) = m
                    && let Ok(msg) = ClientMessage::unpack(&b)
                    && in_tx.send(msg).is_err()
                {
                    break;
                }
            }
        });
        RtcdConn { tx, rx }
    }

    /// Packets to this client are not lost (the synthetic participants: only the recorder is tried).
    pub fn exempt(&self, client: SocketAddr) {
        if let Some(r) = &self.relay {
            r.exempt.lock().unwrap().insert(client);
        }
    }

    pub fn sessions(&self) -> usize {
        self.app
            .as_ref()
            .map_or(0, |a| a.service.sfu.session_count())
    }

    pub async fn shutdown(mut self) {
        if let Some(app) = self.app.take() {
            app.shutdown(Some(std::time::Duration::from_millis(200)))
                .await;
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A websocket to the service: messages in and out.
#[derive(Debug)]
pub struct RtcdConn {
    pub tx: mpsc::UnboundedSender<ClientMessage>,
    pub rx: mpsc::UnboundedReceiver<ClientMessage>,
}

impl RtcdConn {
    pub fn send(&self, m: ClientMessage) {
        let _ = self.tx.send(m);
    }
}

/// A UDP relay in front of rtcd's media port that loses packets on their way to the clients.
struct Relay {
    exempt: Mutex<HashSet<SocketAddr>>,
}

impl Relay {
    fn start(
        front: tokio::net::UdpSocket,
        upstream: SocketAddr,
        loss: f64,
        blackout_ms: u64,
    ) -> Arc<Self> {
        let t0 = std::time::Instant::now();
        let front = Arc::new(front);
        let relay = Arc::new(Self {
            exempt: Mutex::new(HashSet::new()),
        });
        let r = relay.clone();
        tokio::spawn(async move {
            let mut upstreams: std::collections::HashMap<SocketAddr, Arc<tokio::net::UdpSocket>> =
                std::collections::HashMap::new();
            let mut buf = vec![0u8; 2048];
            while let Ok((n, from)) = front.recv_from(&mut buf).await {
                let up = match upstreams.get(&from) {
                    Some(u) => u.clone(),
                    None => {
                        // One socket per client, so rtcd sees each as a peer of its own.
                        let u = Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
                        u.connect(upstream).await.unwrap();
                        let (u2, front2, r2) = (u.clone(), front.clone(), r.clone());
                        tokio::spawn(async move {
                            let mut b = vec![0u8; 2048];
                            let mut rng =
                                (from.port() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                            while let Ok(n) = u2.recv(&mut b).await {
                                rng ^= rng << 13;
                                rng ^= rng >> 7;
                                rng ^= rng << 17;
                                let cut = blackout_ms > 0
                                    && t0.elapsed().as_millis() as u64 % 5000 < blackout_ms;
                                let drop = cut || (rng >> 11) as f64 / (1u64 << 53) as f64 <= loss;
                                if drop && !r2.exempt.lock().unwrap().contains(&from) {
                                    continue;
                                }
                                let _ = front2.send_to(&b[..n], from).await;
                            }
                        });
                        upstreams.insert(from, u.clone());
                        u
                    }
                };
                let _ = up.send(&buf[..n]).await;
            }
        });
        relay
    }
}
