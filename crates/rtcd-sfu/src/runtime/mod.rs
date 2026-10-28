//! The media plane on real sockets: shard threads running engines, and the thread-safe handle the
//! service uses to start sessions and pass messages in and out.

mod config;
mod directory;
mod registry;
mod scaler;
mod shard;
mod table;
mod tcp;

use std::io;
use std::net::SocketAddr;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use mio::{Events, Interest, Poll, Token, Waker};
use rtcd_net::bind_udp;
use rtcd_proto::{Message, SessionConfig};
use tracing::{error, info, warn};

pub use config::{SfuConfig, SfuConfigError};
pub use registry::AdmitError;
pub use scaler::ScalingPolicy;

use self::directory::Directory;
use self::registry::Registry;
use self::scaler::{Scaler, ShardLoad, Step};
use self::shard::{Shard, ShardCommand, ShardHandle, ShardParts, UdpSock, Undelivered};
use self::table::ShardTable;
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

/// What starting and stopping shards needs, shared by the service's handle and the thread that
/// scales.
struct Core {
    cfg: Arc<SfuConfig>,
    metrics: Arc<dyn Metrics>,
    sink: Arc<dyn EventSink>,
    registry: Arc<Registry>,
    directory: Option<Arc<Directory>>,
    table: Arc<ShardTable>,
    /// The thread of each running shard, by slot.
    threads: Mutex<Vec<(usize, JoinHandle<()>)>>,
    /// Held while shards are added or stopped, so two changes cannot interleave.
    resize: Mutex<()>,
}

impl Core {
    /// Creates the shard of `slot` on `udp` and starts its thread. It is reachable by the other
    /// shards from here on, but takes no new session until the registry is told.
    fn spawn(&self, slot: usize, udp: Vec<UdpSock>) -> io::Result<JoinHandle<()>> {
        let poll = Poll::new()?;
        let waker = Arc::new(Waker::new(poll.registry(), Token(0))?);
        let (tx, rx) = sync_channel(COMMAND_QUEUE);
        let busy = Arc::new(AtomicU32::new(0));
        let handle = ShardHandle::new(tx, waker, busy.clone());
        let engine = Engine::new(
            Arc::new(self.cfg.engine.clone()),
            self.metrics.clone(),
            Rng::from_os(),
        );
        let shard = Shard::new(ShardParts {
            id: slot,
            engine,
            poll,
            commands: rx,
            udp,
            sink: self.sink.clone(),
            registry: self.registry.clone(),
            cfg: self.cfg.clone(),
            metrics: self.metrics.clone(),
            directory: self.directory.clone(),
            peers: self.table.clone(),
            busy,
        })?;
        let sink = self.sink.clone();
        let registry = self.registry.clone();
        let thread = std::thread::Builder::new()
            .name(format!("sfu-shard-{slot}"))
            .spawn(move || {
                if catch_unwind(AssertUnwindSafe(move || shard.run())).is_err() {
                    error!(shard = slot, "shard thread panicked; its sessions are lost");
                    // Nothing of the engine survives a panic outside a session, so tell the
                    // service its sessions are gone and let participants rejoin.
                    abandon_sessions(&registry, slot, &*sink);
                }
            })?;
        self.table.set(slot, Some(handle));
        Ok(thread)
    }

    /// Starts one more shard, on sockets bound to the same ports as the others'.
    fn add_shard(&self) -> io::Result<usize> {
        let _resize = self.resize.lock().unwrap_or_else(|e| e.into_inner());
        self.add_shard_locked()
    }

    /// [`add_shard`](Self::add_shard) for a caller that holds the resize lock.
    fn add_shard_locked(&self) -> io::Result<usize> {
        let slot = self
            .table
            .free_slot()
            .ok_or_else(|| io::Error::other("every shard slot is in use"))?;
        let mut udp = Vec::with_capacity(self.cfg.engine.addresses.len());
        for a in &self.cfg.engine.addresses {
            let socket = bind_udp(
                SocketAddr::new(a.bind, a.udp_port),
                true,
                self.cfg.udp_recv_buffer,
                self.cfg.udp_send_buffer,
            )?;
            udp.push(UdpSock {
                socket: mio::net::UdpSocket::from_std(socket),
                advertised: SocketAddr::new(
                    a.advertised_ip(),
                    a.advertised_udp_port.unwrap_or(a.udp_port),
                ),
            });
        }
        let thread = self.spawn(slot, udp)?;
        self.threads
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((slot, thread));
        self.registry.activate(slot);
        info!(shard = slot, shards = self.table.len(), "sfu shard added");
        Ok(slot)
    }

    /// Stops the shard of `slot` if it has no sessions and is not the last one.
    fn retire_shard(&self, slot: usize) -> bool {
        let _resize = self.resize.lock().unwrap_or_else(|e| e.into_inner());
        if !self.registry.retire(slot) {
            return false;
        }
        if let Some(handle) = self.table.get(slot) {
            let _ = handle.send(ShardCommand::Shutdown);
        }
        let thread = {
            let mut threads = self.threads.lock().unwrap_or_else(|e| e.into_inner());
            threads
                .iter()
                .position(|(s, _)| *s == slot)
                .map(|i| threads.swap_remove(i).1)
        };
        if let Some(thread) = thread {
            let _ = thread.join();
        }
        self.table.set(slot, None);
        info!(shard = slot, shards = self.table.len(), "sfu shard stopped");
        true
    }

    /// Makes room for a call that is about to start: if the shards that run are all holding as many
    /// sessions as the policy allows, starts another, so the call is placed on a shard that has
    /// room. Checked again under the lock, so that a burst of joins adds one shard and not one
    /// each.
    fn make_room(&self, cfg: &SessionConfig) {
        let per_shard = self.cfg.scaling.sessions_per_shard;
        if per_shard == 0
            || self.cfg.shard_slots() <= self.table.len()
            || !self.registry.needs_new_shard(cfg, per_shard)
        {
            return;
        }
        let _resize = self.resize.lock().unwrap_or_else(|e| e.into_inner());
        if self.registry.needs_new_shard(cfg, per_shard)
            && let Err(err) = self.add_shard_locked()
        {
            warn!(%err, "cannot start another shard");
        }
    }

    /// What the scaler is told about each running shard.
    fn loads(&self) -> Vec<ShardLoad> {
        self.registry
            .running()
            .into_iter()
            .map(|(slot, sessions)| ShardLoad {
                slot,
                busy: self.table.get(slot).map_or(0.0, |h| h.busy()),
                sessions,
            })
            .collect()
    }
}

/// Keeps the number of shards right for the load, until told to stop.
fn scale(core: Arc<Core>, stop: Arc<std::sync::atomic::AtomicBool>) {
    let max = core.cfg.shard_slots();
    let policy = core.cfg.scaling;
    let mut scaler = Scaler::new(max, policy);
    while !stop.load(Ordering::Acquire) {
        std::thread::park_timeout(policy.tick);
        if stop.load(Ordering::Acquire) {
            return;
        }
        match scaler.step(Instant::now(), &core.loads(), max) {
            Step::Hold => {}
            Step::Add => {
                if let Err(err) = core.add_shard() {
                    warn!(%err, "cannot start another shard");
                }
            }
            Step::Retire(slot) => {
                core.retire_shard(slot);
            }
        }
    }
}

/// A running SFU.
pub struct Sfu {
    core: Arc<Core>,
    addresses: Vec<LocalAddress>,
    /// The threads that are not shards: the TCP acceptor and the scaler.
    helpers: Mutex<Vec<JoinHandle<()>>>,
    acceptor: Option<(Arc<Waker>, Arc<std::sync::atomic::AtomicBool>)>,
    scaler_stop: Arc<std::sync::atomic::AtomicBool>,
}

impl std::fmt::Debug for Sfu {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sfu")
            .field("shards", &self.core.table.len())
            .field("sessions", &self.core.registry.len())
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
        // socket of each address fixes its port when asked for port 0. Sockets share their port
        // whenever there may be more than one shard.
        let n = cfg.shards;
        let slots = cfg.shard_slots();
        let shared_port = slots > 1;
        let mut sockets: Vec<Vec<UdpSock>> = (0..n).map(|_| Vec::new()).collect();
        let mut listeners: Vec<(std::net::TcpListener, SocketAddr)> = Vec::new();
        for a in &mut cfg.engine.addresses {
            for shard in sockets.iter_mut() {
                let bind = SocketAddr::new(a.bind, a.udp_port);
                let socket = bind_udp(bind, shared_port, cfg.udp_recv_buffer, cfg.udp_send_buffer)?;
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
        let core = Arc::new(Core {
            cfg: cfg.clone(),
            metrics,
            sink,
            registry: Arc::new(
                Registry::with_slots(slots, n).expecting_calls_of(cfg.scaling.expected_call_size),
            ),
            directory: shared_port.then(|| Arc::new(Directory::default())),
            table: Arc::new(ShardTable::new(slots)),
            threads: Mutex::new(Vec::with_capacity(slots)),
            resize: Mutex::new(()),
        });
        for (slot, udp) in sockets.into_iter().enumerate() {
            let thread = core.spawn(slot, udp)?;
            core.threads
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .push((slot, thread));
        }

        let mut helpers = Vec::new();
        let acceptor = if listeners.is_empty() {
            None
        } else {
            let poll = Poll::new()?;
            let waker = Arc::new(Waker::new(poll.registry(), ACCEPTOR_WAKE)?);
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let (stop2, table) = (stop.clone(), core.table.clone());
            helpers.push(
                std::thread::Builder::new()
                    .name("sfu-accept".into())
                    .spawn(move || accept_loop(poll, listeners, table, stop2))?,
            );
            Some((waker, stop))
        };

        let scaler_stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        if slots > n {
            let (core, stop) = (core.clone(), scaler_stop.clone());
            helpers.push(
                std::thread::Builder::new()
                    .name("sfu-scaler".into())
                    .spawn(move || scale(core, stop))?,
            );
        }

        info!(shards = n, up_to = slots, addresses = ?cfg.engine.addresses.iter().map(LocalAddress::advertised_udp).collect::<std::collections::BTreeSet<_>>(), "sfu started");
        Ok(Self {
            core,
            addresses: cfg.engine.addresses.clone(),
            helpers: Mutex::new(helpers),
            acceptor,
            scaler_stop,
        })
    }

    /// A session by id alone, whichever group and call it is in.
    pub fn session_by_id(&self, session_id: &str) -> Option<SessionConfig> {
        self.core.registry.lookup(session_id)
    }

    /// The addresses in use, with the ports that were actually bound.
    pub fn addresses(&self) -> &[LocalAddress] {
        &self.addresses
    }

    /// Starts a session. Its messages and its end are reported to the event sink.
    pub fn join(&self, cfg: SessionConfig) -> Result<(), JoinError> {
        cfg.validate().map_err(JoinError::Invalid)?;
        self.core.make_room(&cfg);
        let shard = self.core.registry.admit(&cfg)?;
        let session_id = cfg.session_id.clone();
        let sent = match self.core.table.get(shard) {
            Some(handle) => handle.send(ShardCommand::Engine(Command::Join(cfg))),
            None => Err(Undelivered::Stopped),
        };
        match sent {
            Ok(()) => Ok(()),
            Err(_) => {
                self.core.registry.release(&session_id);
                Err(JoinError::Overloaded)
            }
        }
    }

    /// Ends a session.
    pub fn leave(&self, session_id: &str) {
        if let Some(handle) = self
            .core
            .registry
            .shard_of(session_id)
            .and_then(|shard| self.core.table.get(shard))
        {
            let _ = handle.send(ShardCommand::Engine(Command::Leave {
                session_id: session_id.to_owned(),
            }));
        }
    }

    /// Delivers a message from a session's client.
    pub fn signal(&self, msg: Message) -> Result<(), SignalError> {
        let shard = self
            .core
            .registry
            .shard_of(&msg.session_id)
            .ok_or(SignalError::UnknownSession)?;
        self.core
            .table
            .get(shard)
            .ok_or(SignalError::UnknownSession)?
            .send(ShardCommand::Engine(Command::Signal(msg)))
            .map_err(|e| match e {
                Undelivered::Full => SignalError::Overloaded,
                Undelivered::Stopped => SignalError::UnknownSession,
            })
    }

    /// A session's configuration, if it exists in this group and call.
    pub fn session(&self, group: &str, call: &str, session_id: &str) -> Option<SessionConfig> {
        self.core.registry.get(group, call, session_id)
    }

    /// The sessions of a call.
    pub fn call_sessions(&self, group: &str, call: &str) -> Vec<SessionConfig> {
        self.core.registry.call(group, call)
    }

    pub fn session_count(&self) -> usize {
        self.core.registry.len()
    }

    /// What the node sends now, in bits per second (the shards' smoothed total).
    pub fn egress_bps(&self) -> u64 {
        self.core.cfg.engine.egress.rate_bps()
    }

    /// Whether receivers are held to the small video layers because the send budget is reached.
    pub fn egress_squeezed(&self) -> bool {
        self.core.cfg.engine.egress.squeezed()
    }

    /// How many threads serve sessions right now.
    pub fn shard_count(&self) -> usize {
        self.core.table.len()
    }

    /// Starts one more shard, if the configuration leaves room for it. The SFU does this itself
    /// when the shards are busy; it is public for operators and tests.
    pub fn add_shard(&self) -> io::Result<usize> {
        if self.core.cfg.shard_slots() <= self.core.table.len() {
            return Err(io::Error::other(
                "the configured number of shards is running",
            ));
        }
        self.core.add_shard()
    }

    /// Stops a shard that has no sessions; returns whether it did. The last shard is never
    /// stopped.
    pub fn retire_shard(&self, slot: usize) -> bool {
        self.core.retire_shard(slot)
    }

    /// Stops the SFU: refuses new sessions, waits up to `drain` for the running ones to end on
    /// their own, then ends the rest and stops the threads.
    pub fn shutdown(&self, drain: Duration) {
        let core = &self.core;
        core.registry.close();
        // No shard is started or stopped from here on.
        self.scaler_stop.store(true, Ordering::Release);
        let helpers = std::mem::take(&mut *self.helpers.lock().unwrap_or_else(|e| e.into_inner()));
        for helper in &helpers {
            helper.thread().unpark();
        }
        let end = Instant::now() + drain;
        while core.registry.len() > 0 && Instant::now() < end {
            std::thread::sleep(Duration::from_millis(50));
        }
        if core.registry.len() > 0 {
            warn!(
                sessions = core.registry.len(),
                "ending sessions still running"
            );
        }
        let _resize = core.resize.lock().unwrap_or_else(|e| e.into_inner());
        for (_, shard) in core.table.handles() {
            let _ = shard.send(ShardCommand::Shutdown);
        }
        if let Some((waker, stop)) = &self.acceptor {
            stop.store(true, Ordering::Release);
            let _ = waker.wake();
        }
        let threads = std::mem::take(&mut *core.threads.lock().unwrap_or_else(|e| e.into_inner()));
        for (_, t) in threads {
            let _ = t.join();
        }
        for helper in helpers {
            let _ = helper.join();
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
    shards: Arc<ShardTable>,
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
                        let taken = shards
                            .nth(next.fetch_add(1, Ordering::Relaxed))
                            .is_some_and(|shard| {
                                shard
                                    .send(ShardCommand::Tcp {
                                        stream: std_stream,
                                        local: *advertised,
                                        peer,
                                    })
                                    .is_ok()
                            });
                        if !taken {
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
