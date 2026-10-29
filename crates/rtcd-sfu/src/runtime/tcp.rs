//! ICE over TCP: media that cannot take UDP travels through a TCP connection to the same port,
//! each packet framed as RFC 4571 describes.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::Instant;

use mio::net::TcpStream;
use rtcd_net::{FrameReader, encode_frame};

/// Written bytes are dropped from the front of an [`Outbox`] once there are this many, and as many
/// as the bytes still owed: moving the owed bytes costs no more than the writes that made room.
const COMPACT_AT: usize = 32 * 1024;

/// Framed bytes waiting for a socket.
#[derive(Debug, Default)]
struct Outbox {
    buf: Vec<u8>,
    /// How many of the bytes at the front the socket has taken.
    written: usize,
}

impl Outbox {
    /// What the socket still owes.
    fn pending(&self) -> &[u8] {
        &self.buf[self.written..]
    }

    /// Records that the socket took `n` bytes. A connection that is never fully caught up (a slow
    /// network) must not keep every byte it ever sent: the written front is dropped once it is as
    /// long as what is still owed, so that a backlog of megabytes is not moved at every 32 KiB.
    fn took(&mut self, n: usize) {
        self.written += n;
        if self.written == self.buf.len() {
            self.buf.clear();
            self.written = 0;
        } else if self.written >= COMPACT_AT && self.written * 2 >= self.buf.len() {
            self.buf.drain(..self.written);
            self.written = 0;
        }
    }
}

/// One TCP connection from a participant.
#[derive(Debug)]
pub(crate) struct TcpConn {
    pub stream: TcpStream,
    /// The address the connection came in on, as the SFU advertised it.
    pub local: SocketAddr,
    pub peer: SocketAddr,
    reader: FrameReader,
    out: Outbox,
    pub opened_at: Instant,
    pub last_rx: Instant,
    /// The connection has carried a packet some session claimed (an ICE check naming it).
    pub identified: bool,
    /// The socket is registered for writability (it has output the kernel would not take).
    pub wants_write: bool,
}

impl TcpConn {
    pub fn new(stream: TcpStream, local: SocketAddr, peer: SocketAddr, now: Instant) -> Self {
        Self {
            stream,
            local,
            peer,
            reader: FrameReader::new(),
            out: Outbox::default(),
            opened_at: now,
            last_rx: now,
            identified: false,
            wants_write: false,
        }
    }

    /// Reads until the socket has nothing more. Returns the packets that arrived and whether the
    /// connection is still open.
    pub fn read(&mut self, now: Instant) -> (Vec<bytes::BytesMut>, bool) {
        let mut frames = Vec::new();
        let mut buf = [0u8; 16 * 1024];
        loop {
            match self.stream.read(&mut buf) {
                Ok(0) => return (frames, false),
                Ok(n) => {
                    self.last_rx = now;
                    self.reader.feed(&buf[..n]);
                    while let Some(f) = self.reader.next_frame() {
                        frames.push(f);
                    }
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return (frames, true),
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return (frames, false),
            }
        }
    }

    /// Queues a packet. Returns false, and queues nothing, if the connection already owes more
    /// than `limit` bytes (a participant that stopped reading must not hold memory forever) or the
    /// packet is too long to frame.
    pub fn queue(&mut self, data: &[u8], limit: usize) -> bool {
        if self.out.pending().len() > limit {
            return false;
        }
        encode_frame(data, &mut self.out.buf)
    }

    /// Writes what the socket will take. Returns whether the connection is still usable.
    pub fn flush(&mut self) -> bool {
        while !self.out.pending().is_empty() {
            match self.stream.write(self.out.pending()) {
                Ok(0) => return false,
                Ok(n) => self.out.took(n),
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
        true
    }

    /// Whether output is waiting for the socket.
    pub fn has_output(&self) -> bool {
        !self.out.pending().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_backlog_that_never_clears_does_not_keep_what_was_written() {
        // A network that takes 700 bytes of the 1,200 it is offered every round: the connection
        // always owes something, so the buffer is never emptied in one go.
        let mut out = Outbox::default();
        let mut widest = 0;
        for _ in 0..100_000 {
            if out.pending().len() <= 256 * 1024 {
                out.buf.extend_from_slice(&[0u8; 1200]);
            }
            out.took(700.min(out.pending().len()));
            widest = widest.max(out.buf.len());
        }
        assert!(
            widest < 2 * (256 * 1024 + 2 * 1200) + COMPACT_AT,
            "the buffer reached {widest} bytes"
        );
    }

    #[test]
    fn a_big_backlog_is_not_moved_for_every_few_bytes_written() {
        let mut out = Outbox::default();
        out.buf.resize(4 * 1024 * 1024, 7);
        for _ in 0..4 {
            out.took(COMPACT_AT);
        }
        assert_eq!(
            out.written,
            4 * COMPACT_AT,
            "128 KiB of 4 MiB written is not worth moving the other 3.9"
        );
        out.took(2 * 1024 * 1024);
        assert_eq!(out.written, 0, "half of it written: the front goes");
        assert_eq!(out.pending().len(), 2 * 1024 * 1024 - 4 * COMPACT_AT);
    }

    #[test]
    fn a_drained_buffer_starts_again_from_nothing() {
        let mut out = Outbox::default();
        out.buf.extend_from_slice(&[1, 2, 3, 4, 5]);
        out.took(2);
        assert_eq!(out.pending(), &[3, 4, 5]);
        out.took(3);
        assert!(out.pending().is_empty());
        assert!(out.buf.is_empty());
    }

    #[test]
    fn what_is_left_is_kept_in_order_when_the_front_is_dropped() {
        let mut out = Outbox::default();
        out.buf
            .extend((0..COMPACT_AT + 10).map(|i| (i % 251) as u8));
        out.took(COMPACT_AT);
        assert_eq!(out.written, 0, "the written front was dropped");
        let want: Vec<u8> = (COMPACT_AT..COMPACT_AT + 10)
            .map(|i| (i % 251) as u8)
            .collect();
        assert_eq!(out.pending(), &want[..]);
    }
}
