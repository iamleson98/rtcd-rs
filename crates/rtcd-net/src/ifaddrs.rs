//! The machine's own addresses.

use std::io;
use std::net::IpAddr;

use nix::ifaddrs::getifaddrs;
use nix::net::if_::InterfaceFlags;

/// The addresses of every interface that is up, loopback included: a test client on the same
/// machine connects through it. IPv6 is included only when asked for, and then only addresses a
/// peer elsewhere could reach (not link-local ones, which need a scope).
pub fn local_addresses(ipv6: bool) -> io::Result<Vec<IpAddr>> {
    let mut out: Vec<IpAddr> = Vec::new();
    for iface in getifaddrs().map_err(io::Error::from)? {
        if !iface.flags.contains(InterfaceFlags::IFF_UP) {
            continue;
        }
        let Some(addr) = iface.address else { continue };
        let ip = if let Some(v4) = addr.as_sockaddr_in() {
            IpAddr::V4(v4.ip())
        } else if let Some(v6) = addr.as_sockaddr_in6() {
            let ip = v6.ip();
            if !ipv6 || is_link_local(&ip) {
                continue;
            }
            IpAddr::V6(ip)
        } else {
            continue;
        };
        if !out.contains(&ip) {
            out.push(ip);
        }
    }
    Ok(out)
}

fn is_link_local(ip: &std::net::Ipv6Addr) -> bool {
    ip.segments()[0] & 0xffc0 == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_is_always_there() {
        let addrs = local_addresses(false).unwrap();
        assert!(
            addrs.contains(&"127.0.0.1".parse::<IpAddr>().unwrap()),
            "{addrs:?}"
        );
        assert!(addrs.iter().all(IpAddr::is_ipv4));
    }

    #[test]
    fn link_local_is_recognised() {
        assert!(is_link_local(&"fe80::1".parse().unwrap()));
        assert!(is_link_local(&"febf::1".parse().unwrap()));
        assert!(!is_link_local(&"fec0::1".parse().unwrap()));
        assert!(!is_link_local(&"2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn ipv6_addresses_are_only_listed_on_request() {
        let with = local_addresses(true).unwrap();
        let without = local_addresses(false).unwrap();
        assert!(without.iter().all(IpAddr::is_ipv4));
        assert!(with.len() >= without.len());
    }
}
