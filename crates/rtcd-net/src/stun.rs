//! Learning the public address of a local one from a STUN server (RFC 5389 binding request).

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr, ToSocketAddrs, UdpSocket};
use std::time::Duration;

use thiserror::Error;

const MAGIC_COOKIE: [u8; 4] = [0x21, 0x12, 0xa4, 0x42];
const BINDING_REQUEST: [u8; 2] = [0x00, 0x01];
const BINDING_SUCCESS: [u8; 2] = [0x01, 0x01];
const XOR_MAPPED_ADDRESS: u16 = 0x0020;
const MAPPED_ADDRESS: u16 = 0x0001;

/// Why the public address could not be learned.
#[derive(Debug, Error)]
pub enum StunError {
    #[error("no STUN server URL was given")]
    NoServer,
    #[error("{0:?} is not a stun: or stuns: URL")]
    BadUrl(String),
    #[error("the STUN server did not answer with a usable address")]
    NoAnswer,
    #[error(transparent)]
    Io(#[from] io::Error),
}

/// The host and port of a `stun:host[:port]` or `stuns:host[:port]` URL (port 3478 by default).
pub fn parse_stun_url(url: &str) -> Result<(String, u16), StunError> {
    let rest = url
        .strip_prefix("stun:")
        .or_else(|| url.strip_prefix("stuns:"))
        .ok_or_else(|| StunError::BadUrl(url.to_owned()))?;
    // A query such as ?transport=udp is allowed on the URL and means nothing here.
    let rest = rest.split('?').next().unwrap_or(rest);
    let bad = || StunError::BadUrl(url.to_owned());
    let (host, port) = if let Some(bracketed) = rest.strip_prefix('[') {
        // [v6]:port or [v6]
        let (host, after) = bracketed.split_once(']').ok_or_else(bad)?;
        match after.strip_prefix(':') {
            Some(p) => (host, p.parse().map_err(|_| bad())?),
            None if after.is_empty() => (host, 3478),
            None => return Err(bad()),
        }
    } else {
        match rest.split_once(':') {
            Some((host, p)) => (host, p.parse().map_err(|_| bad())?),
            None => (rest, 3478),
        }
    };
    if host.is_empty() {
        return Err(bad());
    }
    Ok((host.to_owned(), port))
}

fn binding_request(transaction: &[u8; 12]) -> [u8; 20] {
    let mut m = [0u8; 20];
    m[..2].copy_from_slice(&BINDING_REQUEST);
    m[4..8].copy_from_slice(&MAGIC_COOKIE);
    m[8..].copy_from_slice(transaction);
    m
}

/// The mapped address of a binding success response to the request with `transaction`.
fn parse_response(data: &[u8], transaction: &[u8; 12]) -> Option<IpAddr> {
    if data.len() < 20
        || data[..2] != BINDING_SUCCESS
        || data[4..8] != MAGIC_COOKIE
        || data[8..20] != transaction[..]
    {
        return None;
    }
    let end = (20 + usize::from(u16::from_be_bytes([data[2], data[3]]))).min(data.len());
    let mut at = 20;
    let mut plain = None;
    while at + 4 <= end {
        let kind = u16::from_be_bytes([data[at], data[at + 1]]);
        let len = usize::from(u16::from_be_bytes([data[at + 2], data[at + 3]]));
        let value = data.get(at + 4..at + 4 + len)?;
        match kind {
            XOR_MAPPED_ADDRESS => return decode_address(value, true, transaction),
            MAPPED_ADDRESS => plain = decode_address(value, false, transaction),
            _ => {}
        }
        at += 4 + len.div_ceil(4) * 4;
    }
    plain
}

fn decode_address(value: &[u8], xored: bool, transaction: &[u8; 12]) -> Option<IpAddr> {
    let family = *value.get(1)?;
    let mask: Vec<u8> = MAGIC_COOKIE
        .iter()
        .chain(transaction.iter())
        .copied()
        .collect();
    let unmask = |i: usize, b: u8| if xored { b ^ mask[i] } else { b };
    match family {
        0x01 if value.len() >= 8 => {
            let o: [u8; 4] = std::array::from_fn(|i| unmask(i, value[4 + i]));
            Some(IpAddr::V4(Ipv4Addr::from(o)))
        }
        0x02 if value.len() >= 20 => {
            let o: [u8; 16] = std::array::from_fn(|i| unmask(i, value[4 + i]));
            Some(IpAddr::V6(Ipv6Addr::from(o)))
        }
        _ => None,
    }
}

/// Asks the STUN server at `server_url` (`stun:host:port`) which address it sees a datagram from
/// `local` come from. The request leaves from `local` itself (port 0 picks one), so the answer is
/// the public address of that interface.
pub fn discover_public_ip(
    local: IpAddr,
    server_url: &str,
    timeout: Duration,
) -> Result<IpAddr, StunError> {
    if server_url.is_empty() {
        return Err(StunError::NoServer);
    }
    let (host, port) = parse_stun_url(server_url)?;
    let server = (host.as_str(), port)
        .to_socket_addrs()?
        .find(|a| a.is_ipv4() == local.is_ipv4())
        .ok_or(StunError::NoAnswer)?;
    let socket = UdpSocket::bind(SocketAddr::new(local, 0))?;
    socket.set_read_timeout(Some(timeout))?;
    let transaction: [u8; 12] = std::array::from_fn(|_| rand_byte());
    socket.send_to(&binding_request(&transaction), server)?;
    let mut buf = [0u8; 1280];
    loop {
        let (n, from) = socket.recv_from(&mut buf).map_err(|e| match e.kind() {
            io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => StunError::NoAnswer,
            _ => StunError::Io(e),
        })?;
        if from == server
            && let Some(ip) = parse_response(&buf[..n], &transaction)
        {
            return Ok(ip);
        }
    }
}

/// Unpredictable enough for a transaction id, without a randomness dependency for twelve bytes:
/// the std hasher is seeded per process from the operating system.
fn rand_byte() -> u8 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u8(0);
    h.finish() as u8
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_are_split_into_host_and_port() {
        assert_eq!(
            parse_stun_url("stun:stun.example.com:19302").unwrap(),
            ("stun.example.com".into(), 19302)
        );
        assert_eq!(
            parse_stun_url("stun:stun.example.com").unwrap(),
            ("stun.example.com".into(), 3478)
        );
        assert_eq!(
            parse_stun_url("stuns:h:5349?transport=tcp").unwrap(),
            ("h".into(), 5349)
        );
        assert_eq!(
            parse_stun_url("stun:[2001:db8::1]:3478").unwrap(),
            ("2001:db8::1".into(), 3478)
        );
        assert_eq!(
            parse_stun_url("stun:192.0.2.1:3478").unwrap(),
            ("192.0.2.1".into(), 3478)
        );
        for bad in ["", "turn:h:3478", "http://h", "stun:", "stun::3478"] {
            assert!(parse_stun_url(bad).is_err(), "{bad:?}");
        }
    }

    /// A binding success response carrying `ip` as XOR-MAPPED-ADDRESS.
    fn response(ip: Ipv4Addr, transaction: &[u8; 12]) -> Vec<u8> {
        let mut value = vec![0, 0x01, 0x12, 0x34];
        value.extend(
            ip.octets()
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ MAGIC_COOKIE[i]),
        );
        let mut m = BINDING_SUCCESS.to_vec();
        m.extend_from_slice(&12u16.to_be_bytes());
        m.extend_from_slice(&MAGIC_COOKIE);
        m.extend_from_slice(transaction);
        m.extend_from_slice(&XOR_MAPPED_ADDRESS.to_be_bytes());
        m.extend_from_slice(&8u16.to_be_bytes());
        m.extend_from_slice(&value);
        m
    }

    #[test]
    fn the_xor_mapped_address_is_decoded() {
        let t = [3u8; 12];
        assert_eq!(
            parse_response(&response("203.0.113.9".parse().unwrap(), &t), &t),
            Some("203.0.113.9".parse().unwrap())
        );
    }

    #[test]
    fn answers_to_other_requests_are_ignored() {
        let t = [3u8; 12];
        assert_eq!(
            parse_response(&response("203.0.113.9".parse().unwrap(), &t), &[4u8; 12]),
            None
        );
        assert_eq!(parse_response(&[], &t), None);
        assert_eq!(parse_response(&[0u8; 40], &t), None);
    }

    #[test]
    fn a_stun_server_on_loopback_is_asked_and_answers() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = server.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let mut buf = [0u8; 100];
            let (n, from) = server.recv_from(&mut buf).unwrap();
            assert_eq!(n, 20);
            assert_eq!(&buf[..2], &BINDING_REQUEST);
            let t: [u8; 12] = buf[8..20].try_into().unwrap();
            server
                .send_to(&response("198.51.100.7".parse().unwrap(), &t), from)
                .unwrap();
        });
        let ip = discover_public_ip(
            "127.0.0.1".parse().unwrap(),
            &format!("stun:127.0.0.1:{}", addr.port()),
            Duration::from_secs(5),
        )
        .unwrap();
        assert_eq!(ip, "198.51.100.7".parse::<IpAddr>().unwrap());
        handle.join().unwrap();
    }

    #[test]
    fn a_silent_server_times_out() {
        let server = UdpSocket::bind("127.0.0.1:0").unwrap();
        let url = format!("stun:127.0.0.1:{}", server.local_addr().unwrap().port());
        let err = discover_public_ip(
            "127.0.0.1".parse().unwrap(),
            &url,
            Duration::from_millis(100),
        )
        .unwrap_err();
        assert!(matches!(err, StunError::NoAnswer), "{err:?}");
    }
}
