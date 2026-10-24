//! The media plane on real sockets: shard threads running engines, and the thread-safe handle the
//! service uses to start sessions and pass messages in and out.

mod config;
mod directory;
mod registry;
mod shard;
mod tcp;

use std::io;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mio::{Events, Interest, Poll, Token, Waker};
use rtcd_net::bind_udp;
use rtcd_proto::{Message, SessionConfig};
use tracing::{error, info, warn};

pub use config::{SfuConfig, SfuConfigError};
pub use registry::AdmitError;

use self::directory::Directory;
use self::registry::Registry;
use self::shard::{Shard, ShardCommand, ShardHandle, ShardParts, UdpSock, Undelivered};
use crate::config::LocalAddress;
use crate::engine::{CloseReason, Command, Engine, Event};
use crate::ids::Rng;
use crate::metrics::Metrics;

/// Receives what the SFU reports about sessions. Called from the SFU's threads: it must not block.
pub trait EventSink: Send + Sync + 'static {
    fn event(&self, event: Event);
}

impl<F: Fn(Event) + Send + Sync + 'static> EventSink for F {
    fn event(&self, event: Event) {
        self(event);
    }
}

/// Why a session could not be started.
#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("invalid session config: {0}")]
    Invalid(&'static str),
    #[error(transparent)]
    Admit(#[from] AdmitError),
    #[error("the SFU is overloaded")]
    Overloaded,
}

/// Why a message could not be delivered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SignalError {
    #[error("no such session")]
    UnknownSession,
    #[error("the SFU is overloaded")]
    Overloaded,
}

const COMMAND_QUEUE: usize = 65_536;
const ACCEPTOR_WAKE: Token = Token(usize::MAX);

/// A running SFU.
pub struct Sfu {
    shards: Vec<ShardHandle>,
    registry: Arc<Registry>,
    addresses: Vec<LocalAddress>,
    threads: Mutex<Vec<JoinHandle<()>>>,
    acceptor: Option<(Arc<Waker>, Arc<std::sync::atomic::AtomicBool>)>,
}

impl std::fmt::Debug for Sfu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sfu")
            .field("shards", &self.shards.len())
            .field("sessions", &self.registry.len())
            .finish()
    }
}

impl Sfu {
    /// Binds the sockets and starts the threads. Fails, starting nothing, if an address cannot be
    /// bound.
    pub fn start(
        mut cfg: SfuConfig,
        metrics: Arc<dyn Metrics>,
        sink: Arc<dyn EventSink>,
    ) -> io::Result<Self> {
        cfg.validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        // Bind everything before spawning anything, so a bad address fails the start. The first
        // socket of each address fixes its port when asked for port 0.
        let n = cfg.shards;
        let mut sockets: Vec<Vec<UdpSock>> = (0..n).map(|_| Vec::new()).collect();
        let mut listeners: Vec<(std::net::TcpListener, SocketAddr)> = Vec::new();
        for a in &mut cfg.engine.addresses {
            for shard in sockets.iter_mut() {
                let bind = SocketAddr::new(a.bind, a.udp_port);
                let socket = bind_udp(bind, n > 1, cfg.udp_recv_buffer, cfg.udp_send_buffer)?;
                a.udp_port = socket.local_addr()?.port();
                let advertised = SocketAddr::new(
                    a.advertised_ip(),
                    a.advertised_udp_port.unwrap_or(a.udp_port),
                );
                shard.push(UdpSock {
                    socket: mio::net::UdpSocket::from_std(socket),
                    advertised,
                });
            }
            if let Some(port) = a.tcp_port {
                let listener = rtcd_net::bind_tcp(SocketAddr::new(a.bind, port))?;
                a.tcp_port = Some(listener.local_addr()?.port());
                let advertised = a.advertised_tcp().expect("a tcp port is set");
                listeners.push((listener, advertised));
            }
        }

        let cfg = Arc::new(cfg);
        let registry = Arc::new(Registry::new(n));
        let directory = (n > 1).then(|| Arc::new(Directory::default()));
        let peers: Arc<OnceLock<Vec<ShardHandle>>> = Arc::new(OnceLock::new());
        let mut handles = Vec::with_capacity(n);
        let mut threads = Vec::with_capacity(n + 1);
        for (id, udp) in sockets.into_iter().enumerate() {
            let poll = Poll::new()?;
            let waker = Arc::new(Waker::new(poll.registry(), Token(0))?);
            let (tx, rx) = sync_channel(COMMAND_QUEUE);
            handles.push(ShardHandle::new(tx, waker));
            let engine = Engine::new(
                Arc::new(cfg.engine.clone()),
                metrics.clone(),
                Rng::from_os(),
            );
            let parts = ShardParts {
                id,
                engine,
                poll,
                commands: rx,
                udp,
                sink: sink.clone(),
                registry: registry.clone(),
                cfg: cfg.clone(),
                metrics: metrics.clone(),
                directory: directory.clone(),
                peers: peers.clone(),
            };
            let shard = Shard::new(parts)?;
            let sink = sink.clone();
            let registry = registry.clone();
            threads.push(
                std::thread::Builder::new()
                    .name(format!("sfu-shard-{id}"))
                    .spawn(move || {
                        if catch_unwind(AssertUnwindSafe(move || shard.run())).is_err() {
                            error!(shard = id, "shard thread panicked; its sessions are lost");
                            // Nothing of the engine survives a panic outside a session, so tell the
                            // service its sessions are gone and let participants rejoin.
                            abandon_sessions(&registry, id, &*sink);
                        }
                    })?,
            );
        }

        let _ = peers.set(handles.clone());
        let acceptor = if listeners.is_empty() {
            None
        } else {
            let poll = Poll::new()?;
            let waker = Arc::new(Waker::new(poll.registry(), ACCEPTOR_WAKE)?);
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (stop2, shards2) = (stop.clone(), handles.clone());
            threads.push(
                std::thread::Builder::new()
                    .name("sfu-accept".into())
                    .spawn(move || accept_loop(poll, listeners, shards2, stop2))?,
            );
            Some((waker, stop))
        };

        info!(shards = n, addresses = ?cfg.engine.addresses.iter().map(LocalAddress::advertised_udp).collect::<std::collections::BTreeSet<_>>(), "sfu started");
        Ok(Self {
            shards: handles,
            registry,
            addresses: cfg.engine.addresses.clone(),
            threads: Mutex::new(threads),
            acceptor,
        })
    }

    /// A session by id alone, whichever group and call it is in.
    pub fn session_by_id(&self, session_id: &str) -> Option<SessionConfig> {
        self.registry.lookup(session_id)
    }

    /// The addresses in use, with the ports that were actually bound.
    pub fn addresses(&self) -> &[LocalAddress] {
        &self.addresses
    }

    /// Starts a session. Its messages and its end are reported to the event sink.
    pub fn join(&self, cfg: SessionConfig) -> Result<(), JoinError> {
        cfg.validate().map_err(JoinError::Invalid)?;
        let shard = self.registry.admit(&cfg)?;
        let session_id = cfg.session_id.clone();
        match self.shards[shard].send(ShardCommand::Engine(Command::Join(cfg))) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.registry.release(&session_id);
                Err(JoinError::Overloaded)
            }
        }
    }

    /// Ends a session.
    pub fn leave(&self, session_id: &str) {
        if let Some(shard) = self.registry.shard_of(session_id) {
            let _ = self.shards[shard].send(ShardCommand::Engine(Command::Leave {
                session_id: session_id.to_owned(),
            }));
        }
    }

    /// Delivers a message from a session's client.
    pub fn signal(&self, msg: Message) -> Result<(), SignalError> {
        let shard = self
            .registry
            .shard_of(&msg.session_id)
            .ok_or(SignalError::UnknownSession)?;
        self.shards[shard]
            .send(ShardCommand::Engine(Command::Signal(msg)))
            .map_err(|e| match e {
                Undelivered::Full => SignalError::Overloaded,
                Undelivered::Stopped => SignalError::UnknownSession,
            })
    }

    /// A session's configuration, if it exists in this group and call.
    pub fn session(&self, group: &str, call: &str, session_id: &str) -> Option<SessionConfig> {
        self.registry.get(group, call, session_id)
    }

    /// The sessions of a call.
    pub fn call_sessions(&self, group: &str, call: &str) -> Vec<SessionConfig> {
        self.registry.call(group, call)
    }

    pub fn session_count(&self) -> usize {
        self.registry.len()
    }

    /// How many threads serve sessions.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Stops the SFU: refuses new sessions, waits up to `drain` for the running ones to end on
    /// their own, then ends the rest and stops the threads.
    pub fn shutdown(&self, drain: Duration) {
        self.registry.close();
        let end = Instant::now() + drain;
        while self.registry.len() > 0 && Instant::now() < end {
            std::thread::sleep(Duration::from_millis(50));
        }
        if self.registry.len() > 0 {
            warn!(
                sessions = self.registry.len(),
                "ending sessions still running"
            );
        }
        for shard in &self.shards {
            let _ = shard.send(ShardCommand::Shutdown);
        }
        if let Some((waker, stop)) = &self.acceptor {
            stop.store(true, Ordering::Release);
            let _ = waker.wake();
        }
        let threads = std::mem::take(
            &mut *self
                .threads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        for t in threads {
            let _ = t.join();
        }
    }
}

/// Reports every session of a dead shard as lost.
fn abandon_sessions(registry: &Registry, shard: usize, sink: &dyn EventSink) {
    for cfg in registry.take_shard(shard) {
        sink.event(Event::SessionClosed {
            group_id: cfg.group_id,
            session_id: cfg.session_id,
            reason: CloseReason::ConnectionLost,
        });
    }
}

/// Accepts TCP connections and hands each to a shard; the shard learns whose it is from its first
/// packet.
fn accept_loop(
    mut poll: Poll,
    listeners: Vec<(std::net::TcpListener, SocketAddr)>,
    shards: Vec<ShardHandle>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    let mut mio_listeners: Vec<(mio::net::TcpListener, SocketAddr)> = Vec::new();
    for (i, (l, adv)) in listeners.into_iter().enumerate() {
        let mut l = mio::net::TcpListener::from_std(l);
        if poll
            .registry()
            .register(&mut l, Token(i), Interest::READABLE)
            .is_err()
        {
            error!("cannot watch a tcp listener");
            return;
        }
        mio_listeners.push((l, adv));
    }
    let next = AtomicUsize::new(0);
    let mut events = Events::with_capacity(64);
    while !stop.load(Ordering::Acquire) {
        if poll
            .poll(&mut events, Some(Duration::from_secs(1)))
            .is_err()
        {
            continue;
        }
        for ev in events.iter() {
            let Some((listener, advertised)) = mio_listeners.get(ev.token().0) else {
                continue;
            };
            loop {
                match listener.accept() {
                    Ok((stream, peer)) => {
                        // mio hands back a registered-type stream; the shard re-wraps a std one.
                        let std_stream = into_std(stream);
                        let shard = next.fetch_add(1, Ordering::Relaxed) % shards.len();
                        if shards[shard]
                            .send(ShardCommand::Tcp {
                                stream: std_stream,
                                local: *advertised,
                                peer,
                            })
                            .is_err()
                        {
                            warn!(%peer, "tcp connection dropped: the shard is not taking it");
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                    Err(e) => {
                        warn!(%e, "tcp accept failed");
                        break;
                    }
                }
            }
        }
    }
}

fn into_std(stream: mio::net::TcpStream) -> std::net::TcpStream {
    use std::os::fd::{FromRawFd, IntoRawFd};
    // SAFETY: the descriptor comes from a live stream we own and are consuming.
    unsafe { std::net::TcpStream::from_raw_fd(stream.into_raw_fd()) }
}
