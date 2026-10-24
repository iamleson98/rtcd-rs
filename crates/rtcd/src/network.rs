//! Deciding what the SFU listens on and what it tells browsers to connect to.
//!
//! The Go service listens on every interface address and, behind NAT, rewrites the candidates it
//! offers: `ice_host_override` names the public address (or maps several), STUN discovers it when
//! nothing does, and `ice_host_port_override` changes the advertised port for a load balancer in
//! front. The same settings produce the same behaviour here.

use std::collections::HashMap;
use std::net::IpAddr;
use std::time::Duration;

use rtcd_sfu::config::LocalAddress;
use tracing::{debug, info, warn};

use crate::config::{ConfigError, HostPortOverride, RtcConfig, split_list};

/// What the host override setting says about the public addresses.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Override {
    None,
    /// `external/internal` pairs: the internal address is offered as the external one.
    Pairs(HashMap<IpAddr, IpAddr>),
    /// One public address for the interface(s).
    Host(IpAddr),
}

fn parse_override(
    value: &str,
    resolve: &dyn Fn(&str) -> Option<IpAddr>,
) -> Result<Override, ConfigError> {
    if value.is_empty() {
        return Ok(Override::None);
    }
    let bad = |m: String| ConfigError::Invalid {
        field: "ICEHostOverride",
        message: m,
    };
    if value.contains('/') {
        let mut pairs = HashMap::new();
        for pair in value.split(',') {
            let (external, internal) = pair
                .split_once('/')
                .ok_or_else(|| bad(format!("{pair:?} is not an external/internal pair")))?;
            let external: IpAddr = external
                .trim()
                .parse()
                .map_err(|_| bad(format!("{external:?} is not an IP address")))?;
            let internal: IpAddr = internal
                .trim()
                .parse()
                .map_err(|_| bad(format!("{internal:?} is not an IP address")))?;
            pairs.insert(internal, external);
        }
        return Ok(Override::Pairs(pairs));
    }
    value
        .parse::<IpAddr>()
        .ok()
        .or_else(|| resolve(value))
        .map(Override::Host)
        .ok_or_else(|| bad(format!("failed to resolve host {value:?}")))
}

/// The public address to offer for each local one, `None` meaning the local one itself.
fn public_addresses(
    local: &[IpAddr],
    over: &Override,
    discovered: &HashMap<IpAddr, IpAddr>,
) -> HashMap<IpAddr, IpAddr> {
    let mut out = HashMap::new();
    match over {
        Override::Pairs(pairs) => {
            for ip in local {
                if let Some(ext) = pairs.get(ip) {
                    out.insert(*ip, *ext);
                }
            }
        }
        Override::Host(host) => {
            let same_stack = |ip: &IpAddr| ip.is_ipv4() == host.is_ipv4();
            if local.len() == 1 && same_stack(&local[0]) {
                out.insert(local[0], *host);
            } else {
                for ip in local {
                    // The loopback address stays itself, so a client on the same machine connects.
                    if !ip.is_loopback() && same_stack(ip) {
                        out.insert(*ip, *host);
                    }
                }
            }
        }
        Override::None => {
            for ip in local {
                if let Some(public) = discovered.get(ip) {
                    out.insert(*ip, *public);
                }
            }
        }
    }
    out
}

/// Everything the planning needs from the machine, so it can be tested without one.
#[allow(missing_debug_implementations)]
pub struct Environment<'a> {
    /// The addresses of the machine's interfaces.
    pub local: Vec<IpAddr>,
    /// Public addresses learned from a STUN server, by local address.
    pub discovered: HashMap<IpAddr, IpAddr>,
    /// Resolves a host name to an address.
    pub resolve: &'a dyn Fn(&str) -> Option<IpAddr>,
}

/// Plans the addresses to listen on and offer.
pub fn plan_addresses(
    rtc: &RtcConfig,
    host_ports: &HostPortOverride,
    env: &Environment<'_>,
) -> Result<Vec<LocalAddress>, ConfigError> {
    let udp_list = parse_ips("ICEAddressUDP", &rtc.ice_address_udp)?;
    let tcp_list = parse_ips("ICEAddressTCP", &rtc.ice_address_tcp)?;

    // Listen where asked; with nothing asked, everywhere.
    let mut local: Vec<IpAddr> = if udp_list.is_empty() && tcp_list.is_empty() {
        env.local.clone()
    } else {
        let mut wanted = udp_list;
        for ip in tcp_list {
            if !wanted.contains(&ip) {
                wanted.push(ip);
            }
        }
        wanted
    };
    local.retain(|ip| rtc.enable_ipv6 || ip.is_ipv4());
    if local.is_empty() {
        return Err(ConfigError::Invalid {
            field: "ICEAddressUDP",
            message: "no valid address to listen on was found".into(),
        });
    }

    let over = parse_override(&rtc.ice_host_override, env.resolve)?;
    let public = public_addresses(&local, &over, &env.discovered);

    let mut offered: Vec<(IpAddr, u16)> = Vec::new();
    let mut out = Vec::new();
    for ip in local {
        let public_ip = public.get(&ip).copied();
        let advertised_port = host_ports.port_for(&ip);
        let udp_port = u16::try_from(rtc.ice_port_udp).map_err(|_| ConfigError::Invalid {
            field: "ICEPortUDP",
            message: rtc.ice_port_udp.to_string(),
        })?;
        let tcp_port = u16::try_from(rtc.ice_port_tcp).map_err(|_| ConfigError::Invalid {
            field: "ICEPortTCP",
            message: rtc.ice_port_tcp.to_string(),
        })?;
        let a = LocalAddress {
            bind: ip,
            public: public_ip,
            udp_port,
            advertised_udp_port: advertised_port,
            tcp_port: Some(tcp_port),
            advertised_tcp_port: advertised_port,
            advertise: true,
        };
        // Two local addresses behind one public one are both listened on, but offered once.
        let key = (
            a.advertised_ip(),
            a.advertised_udp_port.unwrap_or(a.udp_port),
        );
        let advertise = !offered.contains(&key);
        if advertise {
            offered.push(key);
        }
        out.push(LocalAddress { advertise, ..a });
    }
    Ok(out)
}

fn parse_ips(field: &'static str, list: &str) -> Result<Vec<IpAddr>, ConfigError> {
    split_list(list)
        .into_iter()
        .map(|s| {
            s.parse().map_err(|_| ConfigError::Invalid {
                field,
                message: format!("{s} is not a valid IP address"),
            })
        })
        .collect()
}

/// Asks a STUN server for the public address of each local one, in parallel, and gives up on
/// those it cannot learn. Loopback and unspecified addresses have no public face.
pub fn discover_public(
    local: &[IpAddr],
    stun_url: &str,
    timeout: Duration,
) -> HashMap<IpAddr, IpAddr> {
    let handles: Vec<_> = local
        .iter()
        .filter(|ip| !ip.is_loopback() && !ip.is_unspecified())
        .map(|&ip| {
            let url = stun_url.to_owned();
            std::thread::spawn(move || (ip, rtcd_net::discover_public_ip(ip, &url, timeout)))
        })
        .collect();
    let mut out = HashMap::new();
    for h in handles {
        match h.join() {
            Ok((ip, Ok(public))) => {
                info!(local = %ip, public = %public, "learned the public address of an interface");
                out.insert(ip, public);
            }
            Ok((ip, Err(err))) => {
                warn!(local = %ip, %err, "failed to get the public address of an interface")
            }
            Err(_) => debug!("a STUN lookup panicked"),
        }
    }
    out
}

/// The system resolver.
pub fn resolve_host(host: &str) -> Option<IpAddr> {
    use std::net::ToSocketAddrs;
    (host, 0).to_socket_addrs().ok()?.next().map(|a| a.ip())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(s: &str) -> IpAddr {
        s.parse().unwrap()
    }

    fn no_resolve(_: &str) -> Option<IpAddr> {
        None
    }

    fn plan(
        rtc: &RtcConfig,
        ports: &HostPortOverride,
        local: &[&str],
        discovered: &[(&str, &str)],
    ) -> Result<Vec<LocalAddress>, ConfigError> {
        let env = Environment {
            local: local.iter().map(|s| ip(s)).collect(),
            discovered: discovered.iter().map(|(a, b)| (ip(a), ip(b))).collect(),
            resolve: &no_resolve,
        };
        plan_addresses(rtc, ports, &env)
    }

    #[test]
    fn with_no_settings_every_interface_is_listened_on_and_offered_as_it_is() {
        let out = plan(
            &RtcConfig::default(),
            &HostPortOverride::None,
            &["127.0.0.1", "10.0.0.5"],
            &[],
        )
        .unwrap();
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|a| a.public.is_none()
            && a.advertise
            && a.udp_port == 8443
            && a.tcp_port == Some(8443)));
    }

    #[test]
    fn ipv6_addresses_are_left_out_unless_enabled() {
        let v4 = plan(
            &RtcConfig::default(),
            &HostPortOverride::None,
            &["10.0.0.5", "2001:db8::1"],
            &[],
        )
        .unwrap();
        assert_eq!(v4.len(), 1);
        let mut rtc = RtcConfig::default();
        rtc.enable_ipv6 = true;
        let both = plan(
            &rtc,
            &HostPortOverride::None,
            &["10.0.0.5", "2001:db8::1"],
            &[],
        )
        .unwrap();
        assert_eq!(both.len(), 2);
    }

    #[test]
    fn listing_addresses_restricts_where_the_sfu_listens() {
        let mut rtc = RtcConfig::default();
        rtc.ice_address_udp = "10.0.0.5".into();
        let out = plan(
            &rtc,
            &HostPortOverride::None,
            &["127.0.0.1", "10.0.0.5", "10.0.0.6"],
            &[],
        )
        .unwrap();
        assert_eq!(
            out.iter().map(|a| a.bind).collect::<Vec<_>>(),
            vec![ip("10.0.0.5")]
        );
        rtc.ice_address_tcp = "10.0.0.6".into();
        let out = plan(
            &rtc,
            &HostPortOverride::None,
            &["127.0.0.1", "10.0.0.5", "10.0.0.6"],
            &[],
        )
        .unwrap();
        assert_eq!(out.len(), 2, "the union of the two lists");
    }

    #[test]
    fn a_host_override_is_offered_in_place_of_the_only_interface() {
        let mut rtc = RtcConfig::default();
        rtc.ice_host_override = "203.0.113.9".into();
        let out = plan(&rtc, &HostPortOverride::None, &["10.0.0.5"], &[]).unwrap();
        assert_eq!(out[0].public, Some(ip("203.0.113.9")));
        assert_eq!(out[0].advertised_ip(), ip("203.0.113.9"));
    }

    #[test]
    fn with_several_interfaces_the_override_covers_the_real_ones_and_loopback_stays_itself() {
        let mut rtc = RtcConfig::default();
        rtc.ice_host_override = "203.0.113.9".into();
        let out = plan(
            &rtc,
            &HostPortOverride::None,
            &["127.0.0.1", "10.0.0.5", "172.16.0.5"],
            &[],
        )
        .unwrap();
        assert_eq!(out[0].public, None, "loopback");
        assert_eq!(out[1].public, Some(ip("203.0.113.9")));
        assert_eq!(out[2].public, Some(ip("203.0.113.9")));
        assert!(out[1].advertise);
        assert!(!out[2].advertise, "the same public address is offered once");
    }

    #[test]
    fn explicit_pairs_map_each_interface() {
        let mut rtc = RtcConfig::default();
        rtc.ice_host_override = "8.8.8.8/10.0.0.5,8.8.4.4/10.0.0.6".into();
        let out = plan(
            &rtc,
            &HostPortOverride::None,
            &["10.0.0.5", "10.0.0.6", "10.0.0.7"],
            &[],
        )
        .unwrap();
        assert_eq!(out[0].public, Some(ip("8.8.8.8")));
        assert_eq!(out[1].public, Some(ip("8.8.4.4")));
        assert_eq!(out[2].public, None);
    }

    #[test]
    fn a_host_name_is_resolved() {
        let mut rtc = RtcConfig::default();
        rtc.ice_host_override = "sfu.example.com".into();
        let resolve = |h: &str| (h == "sfu.example.com").then(|| ip("198.51.100.4"));
        let env = Environment {
            local: vec![ip("10.0.0.5")],
            discovered: HashMap::new(),
            resolve: &resolve,
        };
        let out = plan_addresses(&rtc, &HostPortOverride::None, &env).unwrap();
        assert_eq!(out[0].public, Some(ip("198.51.100.4")));
        rtc.ice_host_override = "unknown.example.com".into();
        assert!(plan_addresses(&rtc, &HostPortOverride::None, &env).is_err());
    }

    #[test]
    fn discovered_public_addresses_are_used_when_nothing_is_overridden() {
        let out = plan(
            &RtcConfig::default(),
            &HostPortOverride::None,
            &["10.0.0.5", "10.0.0.6"],
            &[("10.0.0.5", "203.0.113.9")],
        )
        .unwrap();
        assert_eq!(out[0].public, Some(ip("203.0.113.9")));
        assert_eq!(
            out[1].public, None,
            "an interface STUN could not learn is offered as it is"
        );
    }

    #[test]
    fn an_override_wins_over_discovery() {
        let mut rtc = RtcConfig::default();
        rtc.ice_host_override = "203.0.113.9".into();
        let out = plan(
            &rtc,
            &HostPortOverride::None,
            &["10.0.0.5"],
            &[("10.0.0.5", "192.0.2.77")],
        )
        .unwrap();
        assert_eq!(out[0].public, Some(ip("203.0.113.9")));
    }

    #[test]
    fn the_advertised_port_follows_the_override() {
        let single = plan(
            &RtcConfig::default(),
            &HostPortOverride::Single(30443),
            &["10.0.0.5"],
            &[],
        )
        .unwrap();
        assert_eq!(
            (single[0].advertised_udp_port, single[0].advertised_tcp_port),
            (Some(30443), Some(30443))
        );
        assert_eq!(single[0].udp_port, 8443, "the listening port is unchanged");
        let mut map = std::collections::BTreeMap::new();
        map.insert("10.0.0.5".to_owned(), 8444);
        let mapped = plan(
            &RtcConfig::default(),
            &HostPortOverride::Map(map),
            &["10.0.0.5", "10.0.0.6"],
            &[],
        )
        .unwrap();
        assert_eq!(mapped[0].advertised_udp_port, Some(8444));
        assert_eq!(mapped[1].advertised_udp_port, None);
    }

    #[test]
    fn no_usable_address_is_an_error() {
        assert!(plan(&RtcConfig::default(), &HostPortOverride::None, &[], &[]).is_err());
        assert!(
            plan(
                &RtcConfig::default(),
                &HostPortOverride::None,
                &["2001:db8::1"],
                &[]
            )
            .is_err()
        );
    }

    #[test]
    fn a_malformed_pair_is_reported() {
        let mut rtc = RtcConfig::default();
        rtc.ice_host_override = "8.8.8.8/nonsense".into();
        let err = plan(&rtc, &HostPortOverride::None, &["10.0.0.5"], &[])
            .unwrap_err()
            .to_string();
        assert!(err.contains("ICEHostOverride"), "{err}");
    }
}
