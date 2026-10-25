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

/// What the network between the clients and rtcd does to the media. Packets to clients that are
/// [`exempt`](Rtcd::exempt) (the synthetic participants, which only send) are spared, except that
/// `up_loss`, `delay` and `jitter` also act on the RTP they send.
#[derive(Debug, Clone, Copy, Default)]
pub struct Impair {
    /// Fraction of packets rtcd sends to a client that are lost.
    pub down_loss: f64,
    /// Fraction of the RTP packets clients send to rtcd that are lost.
    pub up_loss: f64,
    /// Every 5 s the path to the clients that are not exempt is cut for this long.
    pub blackout_ms: u64,
    /// One-way delay added in each direction, and the most that is added or taken off at random
    /// (so packets overtake one another).
    pub delay_ms: u64,
    pub jitter_ms: u64,
    /// The downlink's capacity, bits per second, for clients that are not exempt; a queue beyond
    /// 200 ms is dropped.
    pub down_rate_bps: Option<u64>,
}

impl Impair {
    fn is_none(&self) -> bool {
        self.down_loss <= 0.0
            && self.up_loss <= 0.0
            && self.blackout_ms == 0
            && self.delay_ms == 0
            && self.jitter_ms == 0
            && self.down_rate_bps.is_none()
    }
}

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
        Self::start_impaired(Impair {
            down_loss: loss,
            blackout_ms,
            ..Default::default()
        })
        .await
    }

    /// Like [`start`](Self::start), behind a network that does what `impair` says.
    pub async fn start_impaired(impair: Impair) -> Self {
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
        let front = if !impair.is_none() {
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
        let relay = front
            .map(|front| Relay::start(front, SocketAddr::new(local.bind, local.udp_port), impair));
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

    /// The service's metrics, as Prometheus text.
    pub fn metrics(&self) -> String {
        self.app
            .as_ref()
            .map_or_else(String::new, |a| a.service.metrics.render())
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

/// A UDP relay in front of rtcd's media port that makes the network between it and the clients
/// behave as [`Impair`] says.
struct Relay {
    exempt: Mutex<HashSet<SocketAddr>>,
}

fn next_random(rng: &mut u64) -> f64 {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    (*rng >> 11) as f64 / (1u64 << 53) as f64
}

/// RTP (not RTCP, STUN or DTLS): the packets a lossy uplink may lose without breaking the call.
fn is_rtp(p: &[u8]) -> bool {
    p.len() > 12 && p[0] >> 6 == 2 && !(192..=223).contains(&p[1])
}

/// A random one-way delay.
fn delay_of(impair: &Impair, rng: &mut u64) -> std::time::Duration {
    let jitter = (next_random(rng) * 2.0 - 1.0) * impair.jitter_ms as f64;
    std::time::Duration::from_secs_f64(((impair.delay_ms as f64 + jitter).max(0.0)) / 1000.0)
}

impl Relay {
    fn start(front: tokio::net::UdpSocket, upstream: SocketAddr, impair: Impair) -> Arc<Self> {
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
            let mut up_rng = 0x2545_F491_4F6C_DD1Du64;
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
                            // When the downlink's queue is empty again.
                            let mut link_free = std::time::Instant::now();
                            while let Ok(n) = u2.recv(&mut b).await {
                                let exempt = r2.exempt.lock().unwrap().contains(&from);
                                if !exempt {
                                    let cut = impair.blackout_ms > 0
                                        && t0.elapsed().as_millis() as u64 % 5000
                                            < impair.blackout_ms;
                                    if cut || next_random(&mut rng) <= impair.down_loss {
                                        continue;
                                    }
                                }
                                let mut wait = delay_of(&impair, &mut rng);
                                if let (Some(rate), false) = (impair.down_rate_bps, exempt) {
                                    // A queue of 200 ms drains at `rate`; beyond that it is full.
                                    let now = std::time::Instant::now();
                                    link_free = link_free.max(now);
                                    let backlog = link_free - now;
                                    if backlog > std::time::Duration::from_millis(200) {
                                        continue;
                                    }
                                    link_free += std::time::Duration::from_secs_f64(
                                        (n as f64 + 28.0) * 8.0 / rate as f64,
                                    );
                                    wait += link_free - now;
                                }
                                let packet = b[..n].to_vec();
                                let front3 = front2.clone();
                                if wait.is_zero() {
                                    let _ = front3.send_to(&packet, from).await;
                                } else {
                                    tokio::spawn(async move {
                                        tokio::time::sleep(wait).await;
                                        let _ = front3.send_to(&packet, from).await;
                                    });
                                }
                            }
                        });
                        upstreams.insert(from, u.clone());
                        u
                    }
                };
                if is_rtp(&buf[..n]) && next_random(&mut up_rng) <= impair.up_loss {
                    continue;
                }
                let wait = delay_of(&impair, &mut up_rng);
                let packet = buf[..n].to_vec();
                if wait.is_zero() {
                    let _ = up.send(&packet).await;
                } else {
                    tokio::spawn(async move {
                        tokio::time::sleep(wait).await;
                        let _ = up.send(&packet).await;
                    });
                }
            }
        });
        relay
    }
}
