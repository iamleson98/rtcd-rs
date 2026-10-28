//! One thread serving a share of the calls: it reads the sockets, hands datagrams to its engine,
//! and writes what the engine wants sent, in batches.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError, TrySendError};
use std::time::{Duration, Instant};

use bytes::BytesMut;
use mio::net::{TcpStream, UdpSocket};
use mio::{Events, Interest, Poll, Token, Waker};
use rtcd_net::{RecvBatch, SendBatch};
use tracing::{debug, error, warn};

use super::EventSink;
use super::config::SfuConfig;
use super::directory::Directory;
use super::registry::Registry;
use super::table::ShardTable;
use super::tcp::TcpConn;
use crate::engine::{Command, Engine, Event, FastMap, Proto, RouteUpdate};
use crate::metrics::{Metrics, NetTraffic};

const WAKER: Token = Token(0);
const TCP_BASE: usize = 1 << 16;
const PEER_BASE: usize = 1 << 20;
/// Sockets connected to one participant each, at most per shard.
const MAX_PEER_SOCKETS: usize = 16_384;
/// Receive buffer asked for each of them: one participant's media is a few hundred kilobits.
const PEER_RECV_BUFFER: usize = 256 * 1024;
/// Read rounds per socket per turn; more would starve the other sockets and the timers.
const MAX_ROUNDS: usize = 16;
/// How often idle TCP connections are looked for.
const SWEEP_EVERY: Duration = Duration::from_secs(1);
/// How long the busy fraction is measured over before it is published.
const BUSY_WINDOW: Duration = Duration::from_secs(1);
/// The busy fraction is published in parts per thousand.
const BUSY_SCALE: f32 = 1000.0;

/// A datagram another shard read and found to be this one's.
#[derive(Debug)]
pub(crate) struct Forwarded {
    pub proto: Proto,
    pub local: SocketAddr,
    pub peer: SocketAddr,
    pub data: BytesMut,
}

/// What a shard is told to do.
#[derive(Debug)]
pub(crate) enum ShardCommand {
    Engine(Command),
    /// Datagrams another shard read that belong to this one's sessions.
    Datagrams(Vec<Forwarded>),
    /// A TCP connection another shard accepted whose first packet named a session of this one,
    /// with the packets read from it so far.
    TcpHandover {
        conn: Box<TcpConn>,
        frames: Vec<BytesMut>,
    },
    /// A TCP connection accepted on `local` (the address as advertised).
    Tcp {
        stream: std::net::TcpStream,
        local: SocketAddr,
        peer: SocketAddr,
    },
    Shutdown,
}

/// Why a command did not reach a shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Undelivered {
    /// The shard has too much queued already.
    Full,
    /// The shard has stopped.
    Stopped,
}

/// The way to reach a running shard.
#[derive(Debug, Clone)]
pub(crate) struct ShardHandle {
    tx: SyncSender<ShardCommand>,
    waker: Arc<Waker>,
    /// The fraction of its time the shard spent working over its last window, in parts per
    /// thousand.
    busy: Arc<AtomicU32>,
}

impl ShardHandle {
    pub fn new(tx: SyncSender<ShardCommand>, waker: Arc<Waker>, busy: Arc<AtomicU32>) -> Self {
        Self { tx, waker, busy }
    }

    /// How busy the shard has been lately, 0 to 1.
    pub fn busy(&self) -> f32 {
        self.busy.load(Ordering::Relaxed) as f32 / BUSY_SCALE
    }

    /// Queues a command and wakes the shard; fails if its queue is full or it has stopped.
    pub fn send(&self, cmd: ShardCommand) -> Result<(), Undelivered> {
        self.tx.try_send(cmd).map_err(|e| match e {
            TrySendError::Full(_) => Undelivered::Full,
            TrySendError::Disconnected(_) => Undelivered::Stopped,
        })?;
        // A wake that cannot be delivered still leaves the command queued for the next turn.
        let _ = self.waker.wake();
        Ok(())
    }
}

/// A bound UDP socket and the address the engine knows it by.
#[derive(Debug)]
pub(crate) struct UdpSock {
    pub socket: UdpSocket,
    pub advertised: SocketAddr,
}

/// A UDP socket connected to one participant, bound to the same port as the shard's shared ones.
///
/// The kernel hands a datagram from that exact address to a connected socket rather than to the
/// shard the port's hash would pick, so once a shard has learned a participant's address its
/// packets arrive here directly instead of through another shard. Sending still goes through the
/// shared socket, which batches across all participants.
#[derive(Debug)]
struct PeerSock {
    socket: UdpSocket,
    /// The shared socket it sits next to (an index into `udp`).
    local: usize,
    peer: SocketAddr,
}

/// Everything a shard needs to run.
pub(crate) struct ShardParts {
    pub id: usize,
    pub engine: Engine,
    pub poll: Poll,
    pub commands: Receiver<ShardCommand>,
    pub udp: Vec<UdpSock>,
    pub sink: Arc<dyn EventSink>,
    pub registry: Arc<Registry>,
    pub cfg: Arc<SfuConfig>,
    /// Present when there are several shards.
    pub metrics: Arc<dyn Metrics>,
    pub directory: Option<Arc<Directory>>,
    /// The shards that are running, this one included once it has been started.
    pub peers: Arc<ShardTable>,
    /// Where this shard publishes how busy it is.
    pub busy: Arc<AtomicU32>,
}

pub(crate) struct Shard {
    id: usize,
    engine: Engine,
    poll: Poll,
    commands: Receiver<ShardCommand>,
    udp: Vec<UdpSock>,
    /// Sockets whose read stopped at the round limit with datagrams left.
    udp_backlog: Vec<bool>,
    recv: RecvBatch,
    send: Vec<SendBatch>,
    peer_socks: Vec<Option<PeerSock>>,
    peer_free: Vec<usize>,
    peer_by_addr: FastMap<SocketAddr, usize>,
    /// Connected sockets that have something to read.
    peer_ready: Vec<usize>,
    tcp: Vec<Option<TcpConn>>,
    tcp_free: Vec<usize>,
    tcp_by_peer: FastMap<SocketAddr, usize>,
    sink: Arc<dyn EventSink>,
    registry: Arc<Registry>,
    cfg: Arc<SfuConfig>,
    metrics: Arc<dyn Metrics>,
    /// What crossed the sockets since it was last reported.
    traffic: NetTraffic,
    directory: Option<Arc<Directory>>,
    peers: Arc<ShardTable>,
    /// Datagrams read for other shards, collected per shard until the end of a read round. Grown
    /// as shards appear.
    forward: Vec<Vec<Forwarded>>,
    busy: Arc<AtomicU32>,
    /// Time spent working since `window_start`.
    worked: Duration,
    window_start: Instant,
    stopping: bool,
    next_sweep: Instant,
}

impl Shard {
    pub fn new(parts: ShardParts) -> io::Result<Self> {
        let ShardParts {
            id,
            engine,
            poll,
            commands,
            mut udp,
            sink,
            registry,
            cfg,
            metrics,
            directory,
            peers,
            busy,
        } = parts;
        for (i, u) in udp.iter_mut().enumerate() {
            poll.registry()
                .register(&mut u.socket, Token(1 + i), Interest::READABLE)?;
        }
        let n = udp.len();
        let mut engine = engine;
        engine.publish_routes(directory.is_some());
        Ok(Self {
            id,
            engine,
            poll,
            commands,
            udp_backlog: vec![false; n],
            recv: RecvBatch::new(cfg.recv_batch),
            send: (0..n).map(|_| SendBatch::new()).collect(),
            udp,
            peer_socks: Vec::new(),
            peer_free: Vec::new(),
            peer_by_addr: FastMap::default(),
            peer_ready: Vec::new(),
            tcp: Vec::new(),
            tcp_free: Vec::new(),
            tcp_by_peer: FastMap::default(),
            sink,
            registry,
            cfg,
            metrics,
            traffic: NetTraffic::default(),
            directory,
            peers,
            forward: Vec::new(),
            busy,
            worked: Duration::ZERO,
            window_start: Instant::now(),
            stopping: false,
            next_sweep: Instant::now() + SWEEP_EVERY,
        })
    }

    /// Serves until told to shut down.
    pub fn run(mut self) {
        let mut events = Events::with_capacity(1024);
        loop {
            let now = Instant::now();
            let timeout = self.wait_time(now);
            if let Err(err) = self.poll.poll(&mut events, Some(timeout)) {
                if err.kind() != io::ErrorKind::Interrupted {
                    error!(shard = self.id, %err, "poll failed");
                    return;
                }
                continue;
            }
            let now = Instant::now();

            let mut commands_ready = false;
            for ev in events.iter() {
                match ev.token() {
                    WAKER => commands_ready = true,
                    Token(t) if t >= PEER_BASE => self.peer_ready.push(t - PEER_BASE),
                    Token(t) if t >= TCP_BASE => {
                        self.tcp_ready(t - TCP_BASE, ev.is_readable(), ev.is_writable(), now)
                    }
                    Token(t) => self.udp_backlog[t - 1] = true,
                }
            }
            for i in 0..self.udp.len() {
                if std::mem::take(&mut self.udp_backlog[i]) {
                    self.drain_udp(i, now);
                }
            }
            for slot in std::mem::take(&mut self.peer_ready) {
                self.drain_peer(slot, now);
            }
            if commands_ready {
                self.drain_commands(now);
            }
            if self.engine.poll_timeout().is_some_and(|t| t <= now) {
                self.engine.handle_timeout(now);
            }
            if now >= self.next_sweep {
                self.next_sweep = now + SWEEP_EVERY;
                self.sweep_tcp(now);
            }
            self.flush();
            self.publish_busy(now);
            if self.stopping && self.engine.session_count() == 0 {
                debug!(shard = self.id, "stopped");
                return;
            }
        }
    }

    /// Counts the work of the turn that began at `woke`, and once a window has passed publishes
    /// what fraction of it was work. The scaler reads it to tell a shard that has room from one
    /// that is about to drop packets.
    fn publish_busy(&mut self, woke: Instant) {
        let done = Instant::now();
        self.worked += done.saturating_duration_since(woke);
        let window = done.saturating_duration_since(self.window_start);
        if window >= BUSY_WINDOW {
            let fraction = (self.worked.as_secs_f32() / window.as_secs_f32()).clamp(0.0, 1.0);
            self.busy
                .store((fraction * BUSY_SCALE) as u32, Ordering::Relaxed);
            self.worked = Duration::ZERO;
            self.window_start = done;
        }
    }

    /// How long the next poll may sleep.
    fn wait_time(&self, now: Instant) -> Duration {
        if self.udp_backlog.iter().any(|&b| b) || !self.peer_ready.is_empty() {
            return Duration::ZERO;
        }
        let engine = self
            .engine
            .poll_timeout()
            .map_or(SWEEP_EVERY, |t| t.saturating_duration_since(now));
        engine.min(SWEEP_EVERY)
    }

    // ----------------------------------------------------------------------------------- UDP

    fn drain_udp(&mut self, i: usize, now: Instant) {
        let advertised = self.udp[i].advertised;
        for round in 0..MAX_ROUNDS {
            let n = match self.recv.recv(&self.udp[i].socket) {
                Ok(n) => n,
                Err(err) => {
                    warn!(shard = self.id, %err, "udp receive failed");
                    return;
                }
            };
            for k in 0..n {
                let Some((peer, data)) = self.recv.get(k) else {
                    continue;
                };
                self.traffic.udp_in_bytes += data.len() as u64;
                self.traffic.udp_in_packets += 1;
                if let Some(dir) = &self.directory
                    && !self.engine.would_route(Proto::Udp, peer, data)
                    && let Some(owner) = dir.owner(Proto::Udp, peer, data)
                    && owner != self.id
                {
                    if owner >= self.forward.len() {
                        self.forward.resize_with(owner + 1, Vec::new);
                    }
                    self.forward[owner].push(Forwarded {
                        proto: Proto::Udp,
                        local: advertised,
                        peer,
                        data: BytesMut::from(data),
                    });
                    continue;
                }
                self.engine.handle_datagram(
                    now,
                    Proto::Udp,
                    advertised,
                    peer,
                    BytesMut::from(data),
                );
            }
            self.send_forwards();
            if n < self.cfg.recv_batch {
                return;
            }
            if round + 1 == MAX_ROUNDS {
                // Edge-triggered: what is left will not announce itself again.
                self.udp_backlog[i] = true;
            }
        }
    }

    /// Which shared socket is the one advertised as `local`. There are a handful of them, one per
    /// address, so a scan beats hashing the address for every packet sent.
    fn udp_index(&self, local: SocketAddr) -> Option<usize> {
        self.udp.iter().position(|u| u.advertised == local)
    }

    /// Opens a socket connected to `peer` next to the shared socket it was heard on, so its
    /// datagrams stop taking a detour through whichever shard the kernel would have picked.
    fn open_peer_socket(&mut self, local: SocketAddr, peer: SocketAddr) {
        if self.directory.is_none()
            || self.peer_by_addr.contains_key(&peer)
            || self.peer_by_addr.len() >= MAX_PEER_SOCKETS
        {
            return;
        }
        let Some(idx) = self.udp_index(local) else {
            return;
        };
        let Ok(bind) = self.udp[idx].socket.local_addr() else {
            return;
        };
        let socket = match rtcd_net::bind_udp(bind, true, PEER_RECV_BUFFER, 0)
            .and_then(|s| s.connect(peer).map(|()| s))
        {
            Ok(s) => s,
            Err(err) => {
                debug!(shard = self.id, %err, %peer, "no socket of its own for a participant");
                return;
            }
        };
        let mut socket = UdpSocket::from_std(socket);
        let slot = self.peer_free.pop().unwrap_or_else(|| {
            self.peer_socks.push(None);
            self.peer_socks.len() - 1
        });
        if self
            .poll
            .registry()
            .register(&mut socket, Token(PEER_BASE + slot), Interest::READABLE)
            .is_err()
        {
            self.peer_free.push(slot);
            return;
        }
        self.peer_socks[slot] = Some(PeerSock {
            socket,
            local: idx,
            peer,
        });
        self.peer_by_addr.insert(peer, slot);
        // What arrived before the socket existed went through the shared one; what is queued there
        // now is read there. Nothing to catch up on here.
    }

    fn close_peer_socket(&mut self, peer: SocketAddr) {
        let Some(slot) = self.peer_by_addr.remove(&peer) else {
            return;
        };
        if let Some(mut ps) = self.peer_socks.get_mut(slot).and_then(Option::take) {
            let _ = self.poll.registry().deregister(&mut ps.socket);
            self.peer_free.push(slot);
        }
    }

    /// Reads what a connected socket has queued.
    fn drain_peer(&mut self, slot: usize, now: Instant) {
        for round in 0..MAX_ROUNDS {
            let Some(ps) = self.peer_socks.get(slot).and_then(Option::as_ref) else {
                return;
            };
            let advertised = self.udp[ps.local].advertised;
            let peer_addr = ps.peer;
            let n = match self.recv.recv(&ps.socket) {
                Ok(n) => n,
                // An ICMP error about the peer is reported on the next call; the next read is fine.
                Err(err) if err.kind() == io::ErrorKind::ConnectionRefused => continue,
                Err(err) => {
                    warn!(shard = self.id, %err, "udp receive failed on a participant's socket");
                    self.close_peer_socket(peer_addr);
                    return;
                }
            };
            for k in 0..n {
                let Some((peer, data)) = self.recv.get(k) else {
                    continue;
                };
                self.traffic.udp_in_bytes += data.len() as u64;
                self.traffic.udp_in_packets += 1;
                self.engine.handle_datagram(
                    now,
                    Proto::Udp,
                    advertised,
                    peer,
                    BytesMut::from(data),
                );
            }
            if n < self.cfg.recv_batch {
                return;
            }
            if round + 1 == MAX_ROUNDS {
                self.peer_ready.push(slot);
            }
        }
    }

    /// Passes the datagrams read for other shards on, one wake-up per shard.
    fn send_forwards(&mut self) {
        for (owner, batch) in self.forward.iter_mut().enumerate() {
            if batch.is_empty() {
                continue;
            }
            self.traffic.udp_forwarded_packets += batch.len() as u64;
            // A shard that is full or gone loses them, as a congested network would.
            match self.peers.get(owner) {
                Some(peer) => {
                    let _ = peer.send(ShardCommand::Datagrams(std::mem::take(batch)));
                }
                None => batch.clear(),
            }
        }
    }

    // ----------------------------------------------------------------------------------- TCP

    /// Takes over a connection another shard accepted, and the packets it has already read.
    fn tcp_adopt(&mut self, mut conn: TcpConn, frames: Vec<BytesMut>, now: Instant) {
        let idx = self.tcp_free.pop().unwrap_or_else(|| {
            self.tcp.push(None);
            self.tcp.len() - 1
        });
        if let Err(err) = self.poll.registry().register(
            &mut conn.stream,
            Token(TCP_BASE + idx),
            Interest::READABLE,
        ) {
            warn!(shard = self.id, %err, "tcp registration failed");
            self.tcp_free.push(idx);
            return;
        }
        conn.wants_write = false;
        let (local, peer) = (conn.local, conn.peer);
        if let Some(old) = self.tcp_by_peer.insert(peer, idx) {
            self.tcp_close(old);
        }
        self.tcp[idx] = Some(conn);
        let mut identified = false;
        for frame in frames {
            identified |= self
                .engine
                .handle_datagram(now, Proto::Tcp, local, peer, frame);
        }
        if let Some(c) = self.tcp.get_mut(idx).and_then(Option::as_mut) {
            c.identified = identified;
        }
        if !identified {
            self.tcp_close(idx);
        }
    }

    /// Moves connection `idx` to the shard `owner`, with `frames` (the packets read from it that
    /// are not handled yet).
    fn tcp_hand_over(&mut self, idx: usize, owner: usize, frames: Vec<BytesMut>) {
        let Some(mut conn) = self.tcp.get_mut(idx).and_then(Option::take) else {
            return;
        };
        let _ = self.poll.registry().deregister(&mut conn.stream);
        if self.tcp_by_peer.get(&conn.peer) == Some(&idx) {
            self.tcp_by_peer.remove(&conn.peer);
        }
        self.tcp_free.push(idx);
        if let Some(peer) = self.peers.get(owner) {
            // If the owner cannot take it the connection is dropped, which the participant
            // answers by connecting again.
            let _ = peer.send(ShardCommand::TcpHandover {
                conn: Box::new(conn),
                frames,
            });
        }
    }

    fn tcp_add(
        &mut self,
        stream: std::net::TcpStream,
        local: SocketAddr,
        peer: SocketAddr,
        now: Instant,
    ) {
        if stream.set_nonblocking(true).is_err() {
            return;
        }
        let _ = stream.set_nodelay(true);
        let mut stream = TcpStream::from_std(stream);
        let idx = self.tcp_free.pop().unwrap_or_else(|| {
            self.tcp.push(None);
            self.tcp.len() - 1
        });
        if let Err(err) =
            self.poll
                .registry()
                .register(&mut stream, Token(TCP_BASE + idx), Interest::READABLE)
        {
            warn!(shard = self.id, %err, %peer, "tcp registration failed");
            self.tcp_free.push(idx);
            return;
        }
        // A reconnecting participant may reuse its port before the old connection is cleaned up.
        if let Some(old) = self.tcp_by_peer.insert(peer, idx) {
            self.tcp_close(old);
        }
        self.tcp[idx] = Some(TcpConn::new(stream, local, peer, now));
    }

    fn tcp_ready(&mut self, idx: usize, readable: bool, writable: bool, now: Instant) {
        if readable {
            let Some(conn) = self.tcp.get_mut(idx).and_then(Option::as_mut) else {
                return;
            };
            let (frames, open) = conn.read(now);
            let (local, peer) = (conn.local, conn.peer);
            let mut identified = conn.identified;
            let mut frames = frames.into_iter();
            while let Some(frame) = frames.next() {
                self.traffic.tcp_in_bytes += frame.len() as u64;
                self.traffic.tcp_in_packets += 1;
                if !identified
                    && let Some(dir) = &self.directory
                    && !self.engine.would_route(Proto::Tcp, peer, &frame)
                    && let Some(owner) = dir.owner(Proto::Tcp, peer, &frame)
                    && owner != self.id
                {
                    let mut rest = vec![frame];
                    rest.extend(frames);
                    self.tcp_hand_over(idx, owner, rest);
                    return;
                }
                let routed = self
                    .engine
                    .handle_datagram(now, Proto::Tcp, local, peer, frame);
                if routed {
                    identified = true;
                } else if !identified {
                    // The first packet names a session or the connection is nobody's.
                    self.tcp_close(idx);
                    return;
                }
            }
            if let Some(conn) = self.tcp.get_mut(idx).and_then(Option::as_mut) {
                conn.identified = identified;
            }
            if !open {
                self.tcp_close(idx);
                return;
            }
        }
        if writable {
            self.tcp_flush(idx);
        }
    }

    /// Writes what a connection owes; keeps the socket registered for writability only while it owes.
    fn tcp_flush(&mut self, idx: usize) {
        let Some(conn) = self.tcp.get_mut(idx).and_then(Option::as_mut) else {
            return;
        };
        if !conn.flush() {
            self.tcp_close(idx);
            return;
        }
        let owes = conn.has_output();
        if owes != conn.wants_write {
            conn.wants_write = owes;
            let interest = if owes {
                Interest::READABLE | Interest::WRITABLE
            } else {
                Interest::READABLE
            };
            if self
                .poll
                .registry()
                .reregister(&mut conn.stream, Token(TCP_BASE + idx), interest)
                .is_err()
            {
                self.tcp_close(idx);
            }
        }
    }

    fn tcp_send(&mut self, peer: SocketAddr, data: &[u8]) {
        let Some(&idx) = self.tcp_by_peer.get(&peer) else {
            return;
        };
        let Some(conn) = self.tcp.get_mut(idx).and_then(Option::as_mut) else {
            return;
        };
        if !conn.queue(data, self.cfg.tcp_write_limit) {
            debug!(shard = self.id, %peer, "tcp connection is not keeping up, closing it");
            self.tcp_close(idx);
            return;
        }
        self.tcp_flush(idx);
    }

    fn tcp_close(&mut self, idx: usize) {
        if let Some(mut conn) = self.tcp.get_mut(idx).and_then(Option::take) {
            let _ = self.poll.registry().deregister(&mut conn.stream);
            if self.tcp_by_peer.get(&conn.peer) == Some(&idx) {
                self.tcp_by_peer.remove(&conn.peer);
            }
            self.tcp_free.push(idx);
        }
    }

    /// Closes connections that never said whose they are, or went quiet.
    fn sweep_tcp(&mut self, now: Instant) {
        let stale: Vec<usize> = self
            .tcp
            .iter()
            .enumerate()
            .filter_map(|(i, c)| {
                let c = c.as_ref()?;
                let unidentified = !c.identified
                    && now.saturating_duration_since(c.opened_at) >= self.cfg.tcp_handshake_timeout;
                let silent = now.saturating_duration_since(c.last_rx) >= self.cfg.tcp_idle_timeout;
                (unidentified || silent).then_some(i)
            })
            .collect();
        for i in stale {
            self.tcp_close(i);
        }
    }

    // -------------------------------------------------------------------------------- commands

    fn drain_commands(&mut self, now: Instant) {
        loop {
            match self.commands.try_recv() {
                Ok(ShardCommand::Engine(cmd)) => self.engine.handle_command(now, cmd),
                Ok(ShardCommand::Datagrams(batch)) => {
                    for d in batch {
                        self.engine
                            .handle_datagram(now, d.proto, d.local, d.peer, d.data);
                    }
                }
                Ok(ShardCommand::TcpHandover { conn, frames }) => {
                    self.tcp_adopt(*conn, frames, now)
                }
                Ok(ShardCommand::Tcp {
                    stream,
                    local,
                    peer,
                }) => self.tcp_add(stream, local, peer, now),
                Ok(ShardCommand::Shutdown) => {
                    self.stopping = true;
                    self.engine.shutdown(now);
                }
                Err(TryRecvError::Empty) => return,
                Err(TryRecvError::Disconnected) => {
                    // The handle is gone: nobody can give orders any more.
                    self.stopping = true;
                    self.engine.shutdown(now);
                    return;
                }
            }
        }
    }

    // ----------------------------------------------------------------------------------- output

    /// Sends what the engine wants sent and reports what it wants reported.
    fn flush(&mut self) {
        if self.directory.is_some() {
            let updates = self.engine.take_route_updates();
            for update in &updates {
                match update {
                    RouteUpdate::AddAddr(Proto::Udp, local, peer) => {
                        self.open_peer_socket(*local, *peer)
                    }
                    RouteUpdate::RemoveAddr(Proto::Udp, peer) => self.close_peer_socket(*peer),
                    _ => {}
                }
            }
            if let Some(dir) = &self.directory {
                dir.apply(self.id, updates);
            }
        }
        while let Some(t) = self.engine.poll_transmit() {
            match t.proto {
                Proto::Udp => {
                    self.traffic.udp_out_bytes += t.data.len() as u64;
                    self.traffic.udp_out_packets += 1;
                    let idx = self.udp_index(t.local).unwrap_or(0);
                    self.send[idx].push(t.peer, t.data);
                }
                Proto::Tcp => {
                    self.traffic.tcp_out_bytes += t.data.len() as u64;
                    self.traffic.tcp_out_packets += 1;
                    self.tcp_send(t.peer, &t.data)
                }
            }
        }
        for (i, batch) in self.send.iter_mut().enumerate() {
            if !batch.is_empty() {
                let report = batch.flush(&self.udp[i].socket);
                self.traffic.udp_dropped_packets += report.dropped as u64;
                if report.dropped > 0 {
                    debug!(
                        shard = self.id,
                        dropped = report.dropped,
                        "the kernel refused datagrams"
                    );
                }
            }
        }
        if !self.traffic.is_empty() {
            self.metrics.net_traffic(std::mem::take(&mut self.traffic));
        }
        while let Some(event) = self.engine.poll_event() {
            if let Event::SessionClosed { session_id, .. } = &event {
                self.registry.release(session_id);
            }
            self.sink.event(event);
        }
    }
}
