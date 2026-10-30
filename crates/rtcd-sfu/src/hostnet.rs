//! What the machine's network interface sends, from `/proc/net` (Linux; elsewhere it is not
//! known).
//!
//! The SFU counts what it sends itself, but the card carries more than that: recordings being
//! uploaded, the API, other pods. The interface's own counters are what the card and the
//! hypervisor behind it see, so the two are kept side by side.

use std::collections::VecDeque;
use std::time::Instant;

/// What an interface has sent since it came up.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Counters {
    pub tx_bytes: u64,
    pub tx_packets: u64,
    /// Packets the driver dropped (a full transmit ring).
    pub tx_dropped: u64,
}

/// The interface's send rate over an interval.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Rate {
    /// Bits per second, link-layer headers included.
    pub tx_bps: f64,
    pub tx_pps: f64,
    pub tx_dropped_per_sec: f64,
}

/// Reads the send rate of the interface that carries the default route over the last few calls.
#[derive(Debug)]
pub struct Sampler {
    history: VecDeque<(Instant, Counters)>,
    window: usize,
}

impl Default for Sampler {
    fn default() -> Self {
        Self::new(1)
    }
}

impl Sampler {
    /// A sampler whose rates cover the last `window` intervals between calls (at least one).
    pub fn new(window: usize) -> Self {
        Self {
            history: VecDeque::new(),
            window: window.max(1),
        }
    }

    /// The rate over the last `window` intervals; `None` until there are two calls and wherever
    /// the system does not say.
    pub fn sample(&mut self, now: Instant) -> Option<Rate> {
        self.observe(now, read())
    }

    fn observe(&mut self, now: Instant, counters: Option<Counters>) -> Option<Rate> {
        self.history.push_back((now, counters?));
        while self.history.len() > self.window + 1 {
            self.history.pop_front();
        }
        let ((from, was), (to, is)) = (self.history.front()?, self.history.back()?);
        rate(was, is, to.saturating_duration_since(*from).as_secs_f64())
    }
}

#[cfg(target_os = "linux")]
fn read() -> Option<Counters> {
    let route = std::fs::read_to_string("/proc/net/route").ok()?;
    let dev = std::fs::read_to_string("/proc/net/dev").ok()?;
    parse_counters(&dev, &default_interface(&route)?)
}

#[cfg(not(target_os = "linux"))]
fn read() -> Option<Counters> {
    None
}

/// The rate between two readings `secs` apart; `None` if no time passed or a counter went back
/// (the interface was reset).
pub fn rate(before: &Counters, after: &Counters, secs: f64) -> Option<Rate> {
    if secs <= 0.0 || after.tx_bytes < before.tx_bytes || after.tx_packets < before.tx_packets {
        return None;
    }
    Some(Rate {
        tx_bps: (after.tx_bytes - before.tx_bytes) as f64 * 8.0 / secs,
        tx_pps: (after.tx_packets - before.tx_packets) as f64 / secs,
        tx_dropped_per_sec: after.tx_dropped.saturating_sub(before.tx_dropped) as f64 / secs,
    })
}

/// The interface of the default route of `/proc/net/route` (the one with the lowest metric).
pub fn default_interface(route: &str) -> Option<String> {
    route
        .lines()
        .skip(1)
        .filter_map(|l| {
            let f: Vec<&str> = l.split_whitespace().collect();
            // Iface Destination Gateway Flags RefCnt Use Metric Mask ...
            (f.len() > 7 && f[1] == "00000000" && f[7] == "00000000")
                .then(|| (f[6].parse::<u32>().unwrap_or(u32::MAX), f[0]))
        })
        .min()
        .map(|(_, name)| name.to_owned())
}

/// The transmit counters of `interface` in `/proc/net/dev`.
pub fn parse_counters(dev: &str, interface: &str) -> Option<Counters> {
    let line = dev.lines().find_map(|l| {
        let (name, rest) = l.split_once(':')?;
        (name.trim() == interface).then_some(rest)
    })?;
    // Eight receive columns, then the transmit ones: bytes packets errs drop ...
    let f: Vec<u64> = line
        .split_whitespace()
        .map(|v| v.parse().unwrap_or(0))
        .collect();
    Some(Counters {
        tx_bytes: *f.get(8)?,
        tx_packets: *f.get(9)?,
        tx_dropped: *f.get(11)?,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    const ROUTE: &str = "Iface\tDestination\tGateway \tFlags\tRefCnt\tUse\tMetric\tMask\t\tMTU\tWindow\tIRTT\n\
ens3\t00000000\t0100A8C0\t0003\t0\t0\t100\t00000000\t0\t0\t0\n\
ens3\t0000A8C0\t00000000\t0001\t0\t0\t100\t00FFFFFF\t0\t0\t0\n\
wg0\t00000000\t0100000A\t0003\t0\t0\t50\t00000000\t0\t0\t0\n";

    const DEV: &str = "Inter-|   Receive                                                |  Transmit\n\
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed\n\
    lo:  100  1 0 0 0 0 0 0   100  1 0 0 0 0 0 0\n\
  ens3: 5000 40 0 0 0 0 0 0 9000000 6000 2 7 0 0 0 0\n";

    #[test]
    fn the_default_route_names_the_interface() {
        assert_eq!(
            default_interface(ROUTE).as_deref(),
            Some("wg0"),
            "lowest metric"
        );
        assert_eq!(default_interface("Iface\tDestination\n"), None);
    }

    #[test]
    fn the_transmit_columns_of_the_interface_are_read() {
        assert_eq!(
            parse_counters(DEV, "ens3"),
            Some(Counters {
                tx_bytes: 9_000_000,
                tx_packets: 6000,
                tx_dropped: 7
            })
        );
        assert_eq!(parse_counters(DEV, "eth9"), None);
    }

    #[test]
    fn the_rate_is_what_was_sent_over_the_interval() {
        let a = Counters {
            tx_bytes: 1_000,
            tx_packets: 10,
            tx_dropped: 0,
        };
        let b = Counters {
            tx_bytes: 126_000,
            tx_packets: 110,
            tx_dropped: 4,
        };
        let r = rate(&a, &b, 2.0).unwrap();
        assert_eq!(r.tx_bps, 500_000.0);
        assert_eq!(r.tx_pps, 50.0);
        assert_eq!(r.tx_dropped_per_sec, 2.0);
        assert_eq!(rate(&b, &a, 2.0), None, "a counter that went back");
        assert_eq!(rate(&a, &b, 0.0), None);
    }

    #[test]
    fn the_sampler_needs_two_readings() {
        let mut s = Sampler::default();
        let t = Instant::now();
        let c = |bytes| {
            Some(Counters {
                tx_bytes: bytes,
                tx_packets: bytes / 100,
                tx_dropped: 0,
            })
        };
        assert_eq!(s.observe(t, c(0)), None);
        let r = s.observe(t + Duration::from_secs(1), c(125_000)).unwrap();
        assert_eq!(r.tx_bps, 1_000_000.0);
        assert_eq!(s.observe(t + Duration::from_secs(2), None), None);
    }

    #[test]
    fn a_window_covers_several_intervals() {
        let mut s = Sampler::new(2);
        let t = Instant::now();
        let c = |bytes| {
            Some(Counters {
                tx_bytes: bytes,
                tx_packets: bytes / 100,
                tx_dropped: 0,
            })
        };
        s.observe(t, c(0));
        // 1 Mbit in the first second, 3 in each of the next two: the window is the last two.
        s.observe(t + Duration::from_secs(1), c(125_000));
        s.observe(t + Duration::from_secs(2), c(125_000 + 375_000));
        let r = s
            .observe(t + Duration::from_secs(3), c(125_000 + 750_000))
            .unwrap();
        assert_eq!(r.tx_bps, 3_000_000.0);
    }
}
