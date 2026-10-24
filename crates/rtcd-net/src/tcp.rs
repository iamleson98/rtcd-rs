//! RFC 4571 framing: RTP, RTCP, STUN and DTLS over a TCP byte stream, each packet preceded by its
//! length as a 16-bit big-endian number.

use std::io;
use std::net::{SocketAddr, TcpListener};

use bytes::{Buf, BytesMut};
use socket2::{Domain, Protocol, Socket, Type};

/// Binds a non-blocking TCP listener that can be rebound at once after a restart.
pub fn bind_tcp(addr: SocketAddr) -> io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    if addr.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    Ok(socket.into())
}

/// The largest packet a frame can carry.
pub const MAX_FRAME: usize = u16::MAX as usize;

/// Splits a TCP byte stream back into the packets that were framed into it.
#[derive(Debug, Default)]
pub struct FrameReader {
    buf: BytesMut,
}

impl FrameReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds bytes read from the stream.
    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    /// The next complete packet, if the stream has delivered one. Empty frames carry nothing and
    /// are skipped.
    pub fn next_frame(&mut self) -> Option<BytesMut> {
        loop {
            if self.buf.len() < 2 {
                return None;
            }
            let len = usize::from(u16::from_be_bytes([self.buf[0], self.buf[1]]));
            if self.buf.len() < 2 + len {
                return None;
            }
            self.buf.advance(2);
            let frame = self.buf.split_to(len);
            if !frame.is_empty() {
                return Some(frame);
            }
        }
    }

    /// Bytes held that do not make a complete packet yet.
    pub fn buffered(&self) -> usize {
        self.buf.len()
    }
}

/// Appends `data` to `out` as one frame. Returns false, writing nothing, if it is too long to frame.
pub fn encode_frame(data: &[u8], out: &mut Vec<u8>) -> bool {
    let Ok(len) = u16::try_from(data.len()) else {
        return false;
    };
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(data);
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framed(packets: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for p in packets {
            assert!(encode_frame(p, &mut out));
        }
        out
    }

    #[test]
    fn frames_survive_any_split_of_the_stream() {
        let stream = framed(&[b"first", b"", b"second packet", &[7u8; 3000]]);
        for chunk in [1, 2, 3, 7, 100, stream.len()] {
            let mut r = FrameReader::new();
            let mut got = Vec::new();
            for piece in stream.chunks(chunk) {
                r.feed(piece);
                while let Some(f) = r.next_frame() {
                    got.push(f.to_vec());
                }
            }
            assert_eq!(
                got,
                vec![
                    b"first".to_vec(),
                    b"second packet".to_vec(),
                    vec![7u8; 3000]
                ],
                "chunk {chunk}"
            );
            assert_eq!(r.buffered(), 0);
        }
    }

    #[test]
    fn a_partial_frame_waits() {
        let mut r = FrameReader::new();
        r.feed(&[0, 5, 1, 2]);
        assert!(r.next_frame().is_none());
        assert_eq!(r.buffered(), 4);
        r.feed(&[3, 4, 5, 0]);
        assert_eq!(&r.next_frame().unwrap()[..], &[1, 2, 3, 4, 5]);
        assert_eq!(r.buffered(), 1, "the first byte of the next length");
    }

    #[test]
    fn a_listener_binds_and_accepts() {
        let l = bind_tcp("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = l.local_addr().unwrap();
        let _c = std::net::TcpStream::connect(addr).unwrap();
        let end = std::time::Instant::now() + std::time::Duration::from_secs(2);
        loop {
            match l.accept() {
                Ok(_) => break,
                Err(e)
                    if e.kind() == io::ErrorKind::WouldBlock && std::time::Instant::now() < end =>
                {
                    std::thread::sleep(std::time::Duration::from_millis(5))
                }
                Err(e) => panic!("{e}"),
            }
        }
    }

    #[test]
    fn the_largest_packet_frames_and_a_larger_one_does_not() {
        let mut out = Vec::new();
        assert!(encode_frame(&vec![0u8; MAX_FRAME], &mut out));
        assert_eq!(out.len(), MAX_FRAME + 2);
        let mut too_long = Vec::new();
        assert!(!encode_frame(&vec![0u8; MAX_FRAME + 1], &mut too_long));
        assert!(too_long.is_empty());
    }
}
