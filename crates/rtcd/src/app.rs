//! Assembling the service from its configuration and running it.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rtcd_sfu::config::{EngineConfig, LocalAddress};
use rtcd_sfu::engine::Event;
use rtcd_sfu::runtime::{ScalingPolicy, Sfu, SfuConfig};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tracing::{info, warn};

use crate::auth::{AuthService, SessionCache};
use crate::config::{Config, split_list};
use crate::metrics::Metrics;
use crate::network::{Environment, discover_public, plan_addresses, resolve_host};
use crate::service::Service;
use crate::store::{DiskStore, Store};

/// How long a STUN server gets to say what our public address is.
const STUN_TIMEOUT: Duration = Duration::from_secs(5);

/// Why the service could not start.
#[derive(Debug, thiserror::Error)]
pub enum StartError {
    #[error(transparent)]
    Config(#[from] crate::config::ConfigError),
    #[error("failed to open the store: {0}")]
    Store(#[from] crate::store::StoreError),
    #[error("failed to start the SFU: {0}")]
    Sfu(std::io::Error),
    #[error("failed to listen on {addr}: {source}")]
    Listen {
        addr: String,
        source: std::io::Error,
    },
    #[error("failed to set up TLS: {0}")]
    Tls(std::io::Error),
}

/// Where the media plane listens, decided from the configuration and the machine.
pub fn media_addresses(cfg: &Config) -> Result<Vec<LocalAddress>, StartError> {
    let rtc = &cfg.rtc;
    let mut local = rtcd_net::local_addresses(rtc.enable_ipv6).map_err(|e| StartError::Listen {
        addr: "interfaces".into(),
        source: e,
    })?;
    // Interface addresses narrowed by the settings, for the STUN lookups below.
    let wanted: Vec<_> = split_list(&rtc.ice_address_udp)
        .into_iter()
        .chain(split_list(&rtc.ice_address_tcp))
        .filter_map(|s| s.parse().ok())
        .collect();
    if !wanted.is_empty() {
        local = wanted;
    }
    let discovered = if rtc.ice_host_override.is_empty() {
        match rtc.ice_servers.iter().find(|s| s.is_stun()) {
            Some(stun) => discover_public(&local, &stun.urls[0], STUN_TIMEOUT),
            None => Default::default(),
        }
    } else {
        Default::default()
    };
    let env = Environment {
        local,
        discovered,
        resolve: &resolve_host,
    };
    Ok(plan_addresses(rtc, &cfg.host_port_override()?, &env)?)
}

/// The runtime settings the configuration implies.
pub fn sfu_config(cfg: &Config, addresses: Vec<LocalAddress>) -> SfuConfig {
    let rtc = &cfg.rtc;
    let mut engine = EngineConfig {
        addresses,
        ..Default::default()
    };
    if rtc.nack_buffer_size != 0 {
        engine.nack_buffer_size = rtc.nack_buffer_size as u16;
    }
    engine.audio_loudest = rtc.audio_loudest.clamp(0, 64) as usize;
    engine.egress_budget_bps = rtc.egress_budget_mbps.clamp(0, 100_000) as u64 * 1_000_000;
    engine.egress_budget_pps = rtc.egress_budget_pps.clamp(0, 100_000_000) as u64;
    // A shard is a thread, a call lives on one, and one carries a few hundred participants before it
    // saturates (measured: a single shard dropped 3 % of the voice packets at 600 participants on
    // four cores, and lost two thirds at 900, where two shards lost 7 %). Another is not free
    // either: its sockets batch less and datagrams are passed between threads. So by default the
    // SFU starts with one and adds a shard when all of them have been busy for a few seconds, up to
    // one fewer than the cores (the rest serve the API, the kernel's network work and the
    // recorder), and stops one that has been empty for a while. An operator can pin the number.
    let (shards, max_shards) = if rtc.shards > 0 {
        (rtc.shards as usize, 0)
    } else {
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        let max = cores.saturating_sub(1).clamp(1, 32);
        // Two from the start where there are cores for two: a class that begins at the bell is
        // placed call by call before its participants arrive, so with one shard they all land on it
        // (three lectures of 130 to 180 people filled one thread to 96 to 100 % while a second sat
        // idle, then refused people; on two shards the threads ran at 51 and 70 %).
        (max.min(2), max)
    };
    SfuConfig {
        engine,
        shards,
        max_shards,
        scaling: ScalingPolicy {
            min_shards: shards,
            ..Default::default()
        },
        refuse_when_saturated: rtc.refuse_when_saturated,
        ..Default::default()
    }
}

/// The running service.
pub struct App {
    pub service: Arc<Service>,
    pub http_addr: SocketAddr,
    stop: watch::Sender<bool>,
    server: JoinHandle<()>,
    events: JoinHandle<()>,
}

impl std::fmt::Debug for App {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("App")
            .field("http_addr", &self.http_addr)
            .finish_non_exhaustive()
    }
}

/// `:8045` means every address; Go's listener does, and so does this.
fn listen_candidates(addr: &str) -> Vec<String> {
    match addr.strip_prefix(':') {
        Some(port) => vec![format!("[::]:{port}"), format!("0.0.0.0:{port}")],
        None => vec![addr.to_owned()],
    }
}

async fn listen(addr: &str) -> Result<TcpListener, StartError> {
    let mut last = None;
    for candidate in listen_candidates(addr) {
        match TcpListener::bind(&candidate).await {
            Ok(l) => return Ok(l),
            Err(e) => last = Some(e),
        }
    }
    Err(StartError::Listen {
        addr: addr.to_owned(),
        source: last.expect("at least one candidate"),
    })
}

impl App {
    /// Starts everything the configuration describes, finding the addresses to offer from the
    /// machine.
    pub async fn start(cfg: Config) -> Result<Self, StartError> {
        cfg.validate()?;
        let cfg2 = cfg.clone();
        let addresses = tokio::task::spawn_blocking(move || media_addresses(&cfg2))
            .await
            .expect("address planning does not panic")?;
        Self::start_with(cfg, addresses).await
    }

    /// Starts with the media addresses given.
    pub async fn start_with(cfg: Config, addresses: Vec<LocalAddress>) -> Result<Self, StartError> {
        let cfg = Arc::new(cfg);
        let store: Arc<dyn Store> = Arc::new(DiskStore::open(std::path::Path::new(
            &cfg.store.data_source,
        ))?);
        let sessions = Arc::new(SessionCache::new(
            cfg.api.security.session_cache.expiration_minutes as u64,
        ));
        let auth = Arc::new(AuthService::new(store, sessions));
        let metrics = Arc::new(Metrics::new());

        let (tx, rx) = mpsc::unbounded_channel::<Event>();
        let sink = move |event: Event| {
            let _ = tx.send(event);
        };
        let sfu = Arc::new(
            Sfu::start(sfu_config(&cfg, addresses), metrics.clone(), Arc::new(sink))
                .map_err(StartError::Sfu)?,
        );
        let service = Service::new(cfg.clone(), auth, sfu, metrics);
        service.system.spawn();
        let events = tokio::spawn(service.clone().run_events(rx));

        let listener = listen(&cfg.api.http.listen_address).await?;
        let http_addr = listener.local_addr().map_err(|e| StartError::Listen {
            addr: cfg.api.http.listen_address.clone(),
            source: e,
        })?;
        let router = crate::api::router(service.clone());
        let (stop, stop_rx) = watch::channel(false);
        let tls = &cfg.api.http.tls;
        let server = if tls.enable {
            let acceptor = crate::tls::acceptor(
                std::path::Path::new(&tls.cert_file),
                std::path::Path::new(&tls.cert_key),
            )
            .map_err(StartError::Tls)?;
            info!(addr = %http_addr, "api: serving with tls");
            tokio::spawn(crate::tls::serve(listener, acceptor, router, stop_rx))
        } else {
            info!(addr = %http_addr, "api: serving plaintext");
            let mut stop_rx = stop_rx;
            tokio::spawn(async move {
                let shutdown = async move {
                    let _ = stop_rx.changed().await;
                };
                if let Err(err) = axum::serve(listener, router)
                    .with_graceful_shutdown(shutdown)
                    .await
                {
                    warn!(%err, "api server ended with an error");
                }
            })
        };
        Ok(Self {
            service,
            http_addr,
            stop,
            server,
            events,
        })
    }

    /// Stops the service: no new sessions, running ones get up to `drain` to end (forever when
    /// `None`), then everything is closed.
    pub async fn shutdown(self, drain: Option<Duration>) {
        let sfu = self.service.sfu.clone();
        let drain = drain.unwrap_or(Duration::from_secs(60 * 60 * 24 * 365));
        let drained = tokio::task::spawn_blocking(move || sfu.shutdown(drain));
        // Connections stay open while sessions drain: their clients still send and receive.
        let _ = drained.await;
        self.service.close_connections();
        let _ = self.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), self.server).await;
        self.events.abort();
        info!("service: shut down");
    }
}
