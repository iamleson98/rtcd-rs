//! UDP in batches.
//!
//! A forwarding loop handles hundreds of datagrams a turn, and a system call each would spend more
//! time crossing into the kernel than moving media. On Linux receives go through `recvmmsg`, one
//! call for a whole batch, and sends through `sendmmsg` with UDP segmentation offload for runs of
//! equal-sized datagrams to the same peer. Elsewhere the same interface is served by one call per
//! datagram, so development on a laptop behaves like production, only slower.

use std::io;
use std::net::SocketAddr;
use std::os::fd::{AsRawFd, RawFd};

use bytes::BytesMut;
use socket2::{Domain, Protocol, SockAddr, Socket, Type};

mod sockaddr;

use sockaddr::{from_storage, to_storage};

/// The largest datagram received; anything bigger than a network MTU is not media.
pub const MAX_DATAGRAM: usize = 2048;

/// Binds a non-blocking UDP socket. With `reuse_port` several sockets can share the address, the
/// kernel spreading incoming datagrams over them. Buffer sizes are requests: the system may grant
/// less, and a refusal is not an error.
pub fn bind_udp(
    addr: SocketAddr,
    reuse_port: bool,
    recv_buffer: usize,
    send_buffer: usize,
) -> io::Result<std::net::UdpSocket> {
    let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    // No SO_REUSEADDR: for UDP, Linux takes it to mean that any number of sockets may bind the same
    // address, so a second server started by mistake would share the port and split its traffic.
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    if reuse_port {
        socket.set_reuse_port(true)?;
    }
    #[cfg(not(all(unix, not(any(target_os = "solaris", target_os = "illumos")))))]
    let _ = reuse_port;
    if addr.is_ipv6() {
        // A socket bound to a specific IPv6 address is v6-only anyway; for [::] keep the two
        // families apart so an IPv4 socket on the same port is possible.
        socket.set_only_v6(true)?;
    }
    if recv_buffer > 0 {
        let _ = socket.set_recv_buffer_size(recv_buffer);
    }
    if send_buffer > 0 {
        let _ = socket.set_send_buffer_size(send_buffer);
    }
    socket.set_nonblocking(true)?;
    socket.bind(&SockAddr::from(addr))?;
    Ok(socket.into())
}

/// A batch of received datagrams.
#[derive(Debug)]
pub struct RecvBatch {
    buf: Vec<u8>,
    lens: Vec<usize>,
    peers: Vec<Option<SocketAddr>>,
    filled: usize,
}

impl RecvBatch {
    /// Room for `slots` datagrams per receive.
    pub fn new(slots: usize) -> Self {
        assert!(slots > 0);
        Self {
            buf: vec![0; slots * MAX_DATAGRAM],
            lens: vec![0; slots],
            peers: vec![None; slots],
            filled: 0,
        }
    }

    /// How many datagrams the last receive brought.
    pub fn len(&self) -> usize {
        self.filled
    }

    pub fn is_empty(&self) -> bool {
        self.filled == 0
    }

    /// The `i`th datagram: who sent it, and what.
    pub fn get(&self, i: usize) -> Option<(SocketAddr, &[u8])> {
        if i >= self.filled {
            return None;
        }
        let at = i * MAX_DATAGRAM;
        Some((self.peers[i]?, &self.buf[at..at + self.lens[i]]))
    }

    /// Reads what is queued on the socket, up to the batch size, without waiting. Returns how many
    /// datagrams arrived, which is zero when there are none.
    pub fn recv(&mut self, socket: &impl AsRawFd) -> io::Result<usize> {
        self.filled = 0;
        let n = recv_into(socket.as_raw_fd(), self)?;
        self.filled = n;
        Ok(n)
    }
}

#[cfg(target_os = "linux")]
fn recv_into(fd: RawFd, batch: &mut RecvBatch) -> io::Result<usize> {
    use std::mem::{MaybeUninit, zeroed};

    let slots = batch.lens.len();
    // SAFETY: the arrays are fully initialised before the call and every pointer in them points
    // into `batch.buf` or the local `names`, both alive and unmoved until the call returns.
    unsafe {
        let mut names: Vec<MaybeUninit<libc::sockaddr_storage>> =
            (0..slots).map(|_| MaybeUninit::zeroed()).collect();
        let mut iovs: Vec<libc::iovec> = (0..slots)
            .map(|i| libc::iovec {
                iov_base: batch.buf.as_mut_ptr().add(i * MAX_DATAGRAM).cast(),
                iov_len: MAX_DATAGRAM,
            })
            .collect();
        let mut hdrs: Vec<libc::mmsghdr> = (0..slots)
            .map(|i| {
                let mut h: libc::mmsghdr = zeroed();
                h.msg_hdr.msg_name = names[i].as_mut_ptr().cast();
                h.msg_hdr.msg_namelen = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
                h.msg_hdr.msg_iov = &mut iovs[i];
                h.msg_hdr.msg_iovlen = 1;
                h
            })
            .collect();
        loop {
            let n = libc::recvmmsg(
                fd,
                hdrs.as_mut_ptr(),
                slots as u32,
                // glibc takes a `c_int`, musl a `c_uint`.
                libc::MSG_DONTWAIT as _,
                std::ptr::null_mut(),
            );
            if n >= 0 {
                let mut kept = 0;
                for i in 0..n as usize {
                    let truncated = hdrs[i].msg_hdr.msg_flags & libc::MSG_TRUNC != 0;
                    batch.lens[kept] = if truncated {
                        0
                    } else {
                        hdrs[i].msg_len as usize
                    };
                    batch.peers[kept] = from_storage(&*names[i].as_ptr());
                    if truncated {
                        continue;
                    }
                    if kept != i {
                        let (from, to) = (i * MAX_DATAGRAM, kept * MAX_DATAGRAM);
                        batch.buf.copy_within(from..from + batch.lens[kept], to);
                    }
                    kept += 1;
                }
                break Ok(kept);
            }
            let err = io::Error::last_os_error();
            match err.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => break Ok(0),
                _ => break Err(err),
            }
        }
    }
}

#[cfg(all(unix, not(target_os = "linux")))]
fn recv_into(fd: RawFd, batch: &mut RecvBatch) -> io::Result<usize> {
    use std::mem::{MaybeUninit, size_of};

    let slots = batch.lens.len();
    let mut n = 0;
    while n < slots {
        let mut name = MaybeUninit::<libc::sockaddr_storage>::zeroed();
        let mut name_len = size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let at = n * MAX_DATAGRAM;
        // SAFETY: the buffer slice and the name storage are valid for the lengths given.
        let got = unsafe {
            libc::recvfrom(
                fd,
                batch.buf[at..at + MAX_DATAGRAM].as_mut_ptr().cast(),
                MAX_DATAGRAM,
                libc::MSG_DONTWAIT,
                name.as_mut_ptr().cast(),
                &mut name_len,
            )
        };
        if got < 0 {
            let err = io::Error::last_os_error();
            match err.kind() {
                io::ErrorKind::Interrupted => continue,
                io::ErrorKind::WouldBlock => break,
                _ if n > 0 => break,
                _ => return Err(err),
            }
        }
        // SAFETY: recvfrom wrote `name_len` bytes of a socket address into the zeroed storage.
        let peer = unsafe { from_storage(&*name.as_ptr()) };
        let _ = name_len;
        // A datagram that fills the slot may have been cut short: it is not media, drop it.
        if got as usize >= MAX_DATAGRAM {
            continue;
        }
        batch.lens[n] = got as usize;
        batch.peers[n] = peer;
        n += 1;
    }
    Ok(n)
}

/// A datagram to send.
#[derive(Debug)]
pub struct Outgoing {
    pub peer: SocketAddr,
    pub data: BytesMut,
}

/// What a flush did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SendReport {
    /// Datagrams handed to the kernel.
    pub sent: usize,
    /// Datagrams the kernel refused (a full send buffer, an unreachable peer); UDP does not retry.
    pub dropped: usize,
    /// System calls made.
    pub syscalls: usize,
}

/// Datagrams to send together.
#[derive(Debug)]
pub struct SendBatch {
    items: Vec<Outgoing>,
    /// UDP segmentation offload is usable on this host (cleared the first time the kernel refuses).
    gso: bool,
}

impl Default for SendBatch {
    fn default() -> Self {
        Self::new()
    }
}

impl SendBatch {
    pub fn new() -> Self {
        Self {
            items: Vec::with_capacity(256),
            gso: cfg!(target_os = "linux"),
        }
    }

    pub fn push(&mut self, peer: SocketAddr, data: BytesMut) {
        self.items.push(Outgoing { peer, data });
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Sends everything pushed and empties the batch.
    pub fn flush(&mut self, socket: &impl AsRawFd) -> SendReport {
        let report = if self.items.is_empty() {
            SendReport::default()
        } else {
            send_all(socket.as_raw_fd(), &mut self.items, &mut self.gso)
        };
        self.items.clear();
        report
    }
}

#[cfg(any(target_os = "linux", test))]
/// A run of datagrams sent as one message: items `start..end`, all to one peer, in segments of
/// `size` bytes (the last may be shorter).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg(any(target_os = "linux", test))]
struct Run {
    start: usize,
    end: usize,
    size: usize,
}

#[cfg(any(target_os = "linux", test))]
/// Orders `items` so that those for one peer are together, keeping each peer's datagrams in order.
fn group_by_peer(items: &mut [Outgoing]) {
    let mut first_seen =
        rustc_hash::FxHashMap::with_capacity_and_hasher(items.len().min(64), Default::default());
    // Each session's datagrams are queued together, so a batch is usually grouped already.
    let (mut grouped, mut last) = (true, 0);
    for o in items.iter() {
        let next = first_seen.len();
        let rank = *first_seen.entry(o.peer).or_insert(next);
        grouped &= rank >= last;
        last = rank;
    }
    if !grouped {
        items.sort_by_cached_key(|o| first_seen[&o.peer]);
    }
}

#[cfg(any(target_os = "linux", test))]
/// Cuts grouped `items` into runs a single `sendmsg` with segmentation offload can carry: equal
/// sizes to one peer, a shorter one only last, at most `max_segments` and `max_bytes` per run.
/// Without offload every datagram is a run of its own.
fn plan_runs(items: &[Outgoing], gso: bool, max_segments: usize, max_bytes: usize) -> Vec<Run> {
    let mut runs = Vec::new();
    let mut i = 0;
    while i < items.len() {
        let size = items[i].data.len();
        let mut j = i + 1;
        let mut total = size;
        if gso && size > 0 {
            while j < items.len()
                && j - i < max_segments
                && items[j].peer == items[i].peer
                && !items[j].data.is_empty()
                && items[j].data.len() <= size
                && total + items[j].data.len() <= max_bytes
            {
                let len = items[j].data.len();
                total += len;
                j += 1;
                if len < size {
                    break;
                }
            }
        }
        runs.push(Run {
            start: i,
            end: j,
            size,
        });
        i = j;
    }
    runs
}

#[cfg(target_os = "linux")]
fn send_all(fd: RawFd, items: &mut [Outgoing], gso: &mut bool) -> SendReport {
    use std::mem::zeroed;

    const MAX_SEGMENTS: usize = 64;
    const MAX_GSO_BYTES: usize = 65_000;
    const UDP_SEGMENT: libc::c_int = 103;
    const MESSAGES_PER_CALL: usize = 64;

    #[repr(C, align(8))]
    struct Control([u8; 32]);

    if *gso {
        group_by_peer(items);
    }
    let runs = plan_runs(items, *gso, MAX_SEGMENTS, MAX_GSO_BYTES);
    let mut report = SendReport::default();

    for chunk in runs.chunks(MESSAGES_PER_CALL) {
        let addrs: Vec<(libc::sockaddr_storage, libc::socklen_t)> = chunk
            .iter()
            .map(|r| to_storage(items[r.start].peer))
            .collect();
        let mut iovs: Vec<libc::iovec> = Vec::new();
        let mut spans: Vec<(usize, usize)> = Vec::with_capacity(chunk.len());
        let mut controls: Vec<Control> = Vec::with_capacity(chunk.len());
        for run in chunk {
            let first = iovs.len();
            for o in &items[run.start..run.end] {
                iovs.push(libc::iovec {
                    iov_base: o.data.as_ptr().cast_mut().cast(),
                    iov_len: o.data.len(),
                });
            }
            spans.push((first, run.end - run.start));
            let mut control = Control([0; 32]);
            if run.end - run.start > 1 {
                // SAFETY: the buffer is 8-aligned and CMSG_SPACE(2) fits in its 32 bytes.
                unsafe {
                    let cm = control.0.as_mut_ptr().cast::<libc::cmsghdr>();
                    (*cm).cmsg_level = libc::SOL_UDP;
                    (*cm).cmsg_type = UDP_SEGMENT;
                    (*cm).cmsg_len = libc::CMSG_LEN(2) as _;
                    std::ptr::write_unaligned(libc::CMSG_DATA(cm).cast::<u16>(), run.size as u16);
                }
            }
            controls.push(control);
        }
        // SAFETY: every pointer stored below refers to `addrs`, `iovs`, `controls` or the datagram
        // buffers in `items`, all of which outlive the sendmmsg calls that use them.
        let mut msgs: Vec<libc::mmsghdr> = (0..chunk.len())
            .map(|k| unsafe {
                let mut m: libc::mmsghdr = zeroed();
                m.msg_hdr.msg_name = (&addrs[k].0 as *const libc::sockaddr_storage)
                    .cast_mut()
                    .cast();
                m.msg_hdr.msg_namelen = addrs[k].1;
                m.msg_hdr.msg_iov = iovs.as_mut_ptr().add(spans[k].0);
                m.msg_hdr.msg_iovlen = spans[k].1 as _;
                if spans[k].1 > 1 {
                    m.msg_hdr.msg_control = controls[k].0.as_mut_ptr().cast();
                    m.msg_hdr.msg_controllen = libc::CMSG_SPACE(2) as _;
                }
                m
            })
            .collect();

        let mut sent = 0;
        while sent < msgs.len() {
            report.syscalls += 1;
            // SAFETY: msgs[sent..] are initialised and point at live buffers (see above).
            let n = unsafe {
                libc::sendmmsg(fd, msgs[sent..].as_mut_ptr(), (msgs.len() - sent) as u32, 0)
            };
            if n > 0 {
                for run in &chunk[sent..sent + n as usize] {
                    report.sent += run.end - run.start;
                }
                sent += n as usize;
                continue;
            }
            let errno = io::Error::last_os_error().raw_os_error().unwrap_or(0);
            let run = chunk[sent];
            if run.end - run.start > 1
                && matches!(
                    errno,
                    libc::EIO | libc::EINVAL | libc::EOPNOTSUPP | libc::ENOPROTOOPT
                )
            {
                // The kernel or the NIC does not do offload: stop asking for it, send this run one
                // datagram at a time.
                *gso = false;
                for o in &items[run.start..run.end] {
                    report.syscalls += 1;
                    if send_one(fd, o) {
                        report.sent += 1;
                    } else {
                        report.dropped += 1;
                    }
                }
            } else {
                // A full socket buffer or an unreachable peer: UDP drops it.
                report.dropped += run.end - run.start;
            }
            sent += 1;
        }
    }
    report
}

#[cfg(all(unix, not(target_os = "linux")))]
fn send_all(fd: RawFd, items: &mut [Outgoing], _gso: &mut bool) -> SendReport {
    let mut report = SendReport::default();
    for o in items.iter() {
        report.syscalls += 1;
        if send_one(fd, o) {
            report.sent += 1;
        } else {
            report.dropped += 1;
        }
    }
    report
}

/// One datagram with `sendto`; false if the kernel did not take it.
fn send_one(fd: RawFd, o: &Outgoing) -> bool {
    let (addr, addr_len) = to_storage(o.peer);
    loop {
        // SAFETY: the data and the address are valid for the lengths given.
        let n = unsafe {
            libc::sendto(
                fd,
                o.data.as_ptr().cast(),
                o.data.len(),
                0,
                (&addr as *const libc::sockaddr_storage).cast(),
                addr_len,
            )
        };
        if n >= 0 {
            return true;
        }
        if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
            return false;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::UdpSocket;
    use std::time::{Duration, Instant};

    use super::*;

    fn out(peer: &str, len: usize, fill: u8) -> Outgoing {
        Outgoing {
            peer: peer.parse().unwrap(),
            data: BytesMut::from(&vec![fill; len][..]),
        }
    }

    #[test]
    fn grouping_keeps_each_peers_datagrams_in_order() {
        let mut items = vec![
            out("10.0.0.1:1", 10, 1),
            out("10.0.0.2:1", 10, 2),
            out("10.0.0.1:1", 10, 3),
            out("10.0.0.2:1", 10, 4),
            out("10.0.0.1:1", 10, 5),
        ];
        group_by_peer(&mut items);
        let order: Vec<u8> = items.iter().map(|o| o.data[0]).collect();
        assert_eq!(order, vec![1, 3, 5, 2, 4]);
    }

    #[test]
    fn a_batch_that_is_grouped_already_is_left_as_it_is() {
        let mut items = vec![
            out("10.0.0.1:1", 10, 1),
            out("10.0.0.1:1", 10, 2),
            out("10.0.0.2:1", 10, 3),
            out("10.0.0.3:1", 10, 4),
            out("10.0.0.3:1", 10, 5),
        ];
        group_by_peer(&mut items);
        let order: Vec<u8> = items.iter().map(|o| o.data[0]).collect();
        assert_eq!(order, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn peers_in_the_order_they_first_appear() {
        let mut items = vec![
            out("10.0.0.3:1", 10, 1),
            out("10.0.0.1:1", 10, 2),
            out("10.0.0.3:1", 10, 3),
            out("10.0.0.2:1", 10, 4),
            out("10.0.0.1:1", 10, 5),
        ];
        group_by_peer(&mut items);
        let order: Vec<u8> = items.iter().map(|o| o.data[0]).collect();
        assert_eq!(order, vec![1, 3, 2, 5, 4]);
    }

    #[test]
    fn equal_sized_datagrams_to_one_peer_become_a_run() {
        let items = vec![
            out("10.0.0.1:1", 1200, 0),
            out("10.0.0.1:1", 1200, 0),
            out("10.0.0.1:1", 1200, 0),
        ];
        assert_eq!(
            plan_runs(&items, true, 64, 65_000),
            vec![Run {
                start: 0,
                end: 3,
                size: 1200
            }]
        );
    }

    #[test]
    fn a_shorter_datagram_ends_a_run_and_a_longer_one_starts_the_next() {
        let items = vec![
            out("10.0.0.1:1", 1200, 0),
            out("10.0.0.1:1", 1200, 0),
            out("10.0.0.1:1", 300, 0),
            out("10.0.0.1:1", 1200, 0),
        ];
        assert_eq!(
            plan_runs(&items, true, 64, 65_000),
            vec![
                Run {
                    start: 0,
                    end: 3,
                    size: 1200
                },
                Run {
                    start: 3,
                    end: 4,
                    size: 1200
                }
            ]
        );
        let longer = vec![out("10.0.0.1:1", 500, 0), out("10.0.0.1:1", 1200, 0)];
        assert_eq!(
            plan_runs(&longer, true, 64, 65_000).len(),
            2,
            "a segment cannot be larger than the first"
        );
    }

    #[test]
    fn runs_are_cut_at_the_peer_and_at_the_limits() {
        let items = vec![out("10.0.0.1:1", 100, 0), out("10.0.0.2:1", 100, 0)];
        assert_eq!(plan_runs(&items, true, 64, 65_000).len(), 2);
        let many: Vec<_> = (0..10).map(|_| out("10.0.0.1:1", 100, 0)).collect();
        let runs = plan_runs(&many, true, 4, 65_000);
        assert_eq!(
            runs.iter().map(|r| r.end - r.start).collect::<Vec<_>>(),
            vec![4, 4, 2]
        );
        let big: Vec<_> = (0..10).map(|_| out("10.0.0.1:1", 1000, 0)).collect();
        assert_eq!(
            plan_runs(&big, true, 64, 3500)
                .iter()
                .map(|r| r.end - r.start)
                .collect::<Vec<_>>(),
            vec![3, 3, 3, 1]
        );
    }

    #[test]
    fn without_offload_every_datagram_stands_alone() {
        let items = vec![out("10.0.0.1:1", 100, 0), out("10.0.0.1:1", 100, 0)];
        assert_eq!(plan_runs(&items, false, 64, 65_000).len(), 2);
    }

    fn pair() -> (UdpSocket, UdpSocket) {
        let a = bind_udp("127.0.0.1:0".parse().unwrap(), false, 1 << 20, 1 << 20).unwrap();
        let b = bind_udp("127.0.0.1:0".parse().unwrap(), false, 1 << 20, 1 << 20).unwrap();
        (a, b)
    }

    /// Receives until `want` datagrams have come or a second has passed.
    fn receive(socket: &UdpSocket, want: usize) -> Vec<(SocketAddr, Vec<u8>)> {
        let mut batch = RecvBatch::new(32);
        let mut got = Vec::new();
        let end = Instant::now() + Duration::from_secs(2);
        while got.len() < want && Instant::now() < end {
            let n = batch.recv(socket).unwrap();
            for i in 0..n {
                let (peer, data) = batch.get(i).unwrap();
                got.push((peer, data.to_vec()));
            }
            if n == 0 {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        got
    }

    #[test]
    fn a_batch_of_mixed_sizes_arrives_whole_and_in_order() {
        let (tx, rx) = pair();
        let dest = rx.local_addr().unwrap();
        let mut batch = SendBatch::new();
        let sizes = [1200usize, 1200, 1200, 400, 1200, 1200, 20, 1100];
        for (i, s) in sizes.iter().enumerate() {
            batch.push(dest, BytesMut::from(&vec![i as u8; *s][..]));
        }
        let report = batch.flush(&tx);
        assert_eq!(report.sent, sizes.len());
        assert_eq!(report.dropped, 0);
        assert!(batch.is_empty());

        let got = receive(&rx, sizes.len());
        assert_eq!(got.len(), sizes.len());
        for (i, (peer, data)) in got.iter().enumerate() {
            assert_eq!(*peer, tx.local_addr().unwrap());
            assert_eq!(data.len(), sizes[i], "datagram {i}");
            assert!(data.iter().all(|&b| b == i as u8));
        }
    }

    #[test]
    fn datagrams_for_two_peers_each_arrive_at_their_own() {
        let (tx, a) = pair();
        let b = bind_udp("127.0.0.1:0".parse().unwrap(), false, 0, 0).unwrap();
        let mut batch = SendBatch::new();
        for i in 0..6u8 {
            let dest = if i % 2 == 0 {
                a.local_addr().unwrap()
            } else {
                b.local_addr().unwrap()
            };
            batch.push(dest, BytesMut::from(&[i; 800][..]));
        }
        assert_eq!(batch.flush(&tx).sent, 6);
        let to_a: Vec<u8> = receive(&a, 3).iter().map(|(_, d)| d[0]).collect();
        let to_b: Vec<u8> = receive(&b, 3).iter().map(|(_, d)| d[0]).collect();
        assert_eq!(to_a, vec![0, 2, 4]);
        assert_eq!(to_b, vec![1, 3, 5]);
    }

    #[test]
    fn receiving_from_an_empty_socket_returns_zero_without_blocking() {
        let (_tx, rx) = pair();
        let mut batch = RecvBatch::new(8);
        assert_eq!(batch.recv(&rx).unwrap(), 0);
        assert!(batch.is_empty());
    }

    #[test]
    fn a_receive_is_limited_to_the_batch_size() {
        let (tx, rx) = pair();
        let dest = rx.local_addr().unwrap();
        for i in 0..20u8 {
            tx.send_to(&[i; 100], dest).unwrap();
        }
        std::thread::sleep(Duration::from_millis(50));
        let mut batch = RecvBatch::new(8);
        let n = batch.recv(&rx).unwrap();
        assert!((1..=8).contains(&n), "{n}");
        let (_, first) = batch.get(0).unwrap();
        assert_eq!(first[0], 0);
        assert!(batch.get(n).is_none());
    }

    #[test]
    fn an_oversized_datagram_is_dropped_not_truncated() {
        let (tx, rx) = pair();
        let dest = rx.local_addr().unwrap();
        tx.send_to(&vec![9u8; MAX_DATAGRAM + 500], dest).unwrap();
        tx.send_to(&[1u8; 50], dest).unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let got = receive(&rx, 2);
        assert_eq!(got.len(), 1, "only the small datagram is delivered");
        assert_eq!(got[0].1.len(), 50);
    }

    #[test]
    fn sockets_can_share_a_port_when_asked() {
        let first = bind_udp("127.0.0.1:0".parse().unwrap(), true, 0, 0).unwrap();
        let addr = first.local_addr().unwrap();
        #[cfg(unix)]
        assert!(
            bind_udp(addr, true, 0, 0).is_ok(),
            "reuse_port lets two sockets bind one address"
        );
        assert!(
            bind_udp(addr, false, 0, 0).is_err() || cfg!(not(unix)),
            "and without it the address is taken"
        );
    }
}
