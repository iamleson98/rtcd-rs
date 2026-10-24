//! ICE over TCP: media that cannot take UDP travels through a TCP connection to the same port,
//! each packet framed as RFC 4571 describes.

use std::io::{self, Read, Write};
use std::net::SocketAddr;
use std::time::Instant;

use mio::net::TcpStream;
use rtcd_net::{FrameReader, encode_frame};

/// One TCP connection from a participant.
#[derive(Debug)]
pub(crate) struct TcpConn {
    pub stream: TcpStream,
    /// The address the connection came in on, as the SFU advertised it.
    pub local: SocketAddr,
    pub peer: SocketAddr,
    reader: FrameReader,
    /// Framed bytes waiting for the socket, and how many of them are already written.
    out: Vec<u8>,
    written: usize,
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
            out: Vec::new(),
            written: 0,
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
        if self.out.len() - self.written > limit {
            return false;
        }
        encode_frame(data, &mut self.out)
    }

    /// Writes what the socket will take. Returns whether the connection is still usable.
    pub fn flush(&mut self) -> bool {
        while self.written < self.out.len() {
            match self.stream.write(&self.out[self.written..]) {
                Ok(0) => return false,
                Ok(n) => self.written += n,
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => return true,
                Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
                Err(_) => return false,
            }
        }
        self.out.clear();
        self.written = 0;
        true
    }

    /// Whether output is waiting for the socket.
    pub fn has_output(&self) -> bool {
        self.written < self.out.len()
    }
}
