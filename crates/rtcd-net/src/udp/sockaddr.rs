//! Conversions between `std` socket addresses and the C structures the system calls take.

use std::mem::{size_of, zeroed};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// The address in `ss`, if it is an IPv4 or IPv6 one.
pub(super) fn from_storage(ss: &libc::sockaddr_storage) -> Option<SocketAddr> {
    match libc::c_int::from(ss.ss_family) {
        libc::AF_INET => {
            // SAFETY: the family says the storage holds a sockaddr_in, which it is large enough for.
            let sin =
                unsafe { &*(ss as *const libc::sockaddr_storage).cast::<libc::sockaddr_in>() };
            Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::from(u32::from_be(sin.sin_addr.s_addr))),
                u16::from_be(sin.sin_port),
            ))
        }
        libc::AF_INET6 => {
            // SAFETY: as above, for a sockaddr_in6.
            let sin6 =
                unsafe { &*(ss as *const libc::sockaddr_storage).cast::<libc::sockaddr_in6>() };
            Some(SocketAddr::new(
                IpAddr::V6(Ipv6Addr::from(sin6.sin6_addr.s6_addr)),
                u16::from_be(sin6.sin6_port),
            ))
        }
        _ => None,
    }
}

/// `addr` as a C socket address and its length.
pub(super) fn to_storage(addr: SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    // SAFETY: an all-zero sockaddr_storage is valid, and the casts below stay within its size.
    let mut ss: libc::sockaddr_storage = unsafe { zeroed() };
    let len = match addr {
        SocketAddr::V4(a) => {
            let sin = (&mut ss as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in>();
            // SAFETY: see above.
            unsafe {
                (*sin).sin_family = libc::AF_INET as _;
                (*sin).sin_port = a.port().to_be();
                (*sin).sin_addr = libc::in_addr {
                    s_addr: u32::from(*a.ip()).to_be(),
                };
                #[cfg(target_vendor = "apple")]
                {
                    (*sin).sin_len = size_of::<libc::sockaddr_in>() as u8;
                }
            }
            size_of::<libc::sockaddr_in>()
        }
        SocketAddr::V6(a) => {
            let sin6 = (&mut ss as *mut libc::sockaddr_storage).cast::<libc::sockaddr_in6>();
            // SAFETY: see above.
            unsafe {
                (*sin6).sin6_family = libc::AF_INET6 as _;
                (*sin6).sin6_port = a.port().to_be();
                (*sin6).sin6_flowinfo = a.flowinfo();
                (*sin6).sin6_addr = libc::in6_addr {
                    s6_addr: a.ip().octets(),
                };
                (*sin6).sin6_scope_id = a.scope_id();
                #[cfg(target_vendor = "apple")]
                {
                    (*sin6).sin6_len = size_of::<libc::sockaddr_in6>() as u8;
                }
            }
            size_of::<libc::sockaddr_in6>()
        }
    };
    (ss, len as libc::socklen_t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_survive_the_round_trip() {
        for s in [
            "127.0.0.1:8443",
            "10.1.2.3:1",
            "255.255.255.255:65535",
            "[::1]:443",
            "[2001:db8::7]:5000",
        ] {
            let addr: SocketAddr = s.parse().unwrap();
            let (ss, _) = to_storage(addr);
            assert_eq!(from_storage(&ss), Some(addr), "{s}");
        }
    }

    #[test]
    fn an_unknown_family_is_not_an_address() {
        // SAFETY: zeroed is a valid sockaddr_storage; family 0 is AF_UNSPEC.
        let ss: libc::sockaddr_storage = unsafe { zeroed() };
        assert_eq!(from_storage(&ss), None);
    }
}
