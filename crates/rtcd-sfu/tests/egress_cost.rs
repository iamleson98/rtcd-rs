//! What counting the node's sends costs the shards: an atomic add per datagram on counters all of
//! them share, against one add per batch. A measurement, not a check:
//! `cargo test -p rtcd-sfu --release --test egress_cost -- --ignored --nocapture`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use rtcd_sfu::egress::Gauge;

const THREADS: usize = 4;
const DATAGRAMS: usize = 20_000_000;
/// A shard's flush sends a few dozen datagrams at once at the loads that matter.
const BATCH: usize = 32;

fn run(per_batch: bool) -> f64 {
    let gauge = Arc::new(Gauge::default());
    let peer: SocketAddr = "10.0.0.1:5000".parse().unwrap();
    let started = Instant::now();
    let workers: Vec<_> = (0..THREADS)
        .map(|_| {
            let gauge = gauge.clone();
            std::thread::spawn(move || {
                let (mut bytes, mut packets) = (0u64, 0u64);
                for i in 0..DATAGRAMS / THREADS {
                    let wire = Gauge::wire_len(100 + i % 1000, peer, false);
                    if per_batch {
                        bytes += wire;
                        packets += 1;
                        if packets == BATCH as u64 {
                            gauge.add(bytes, packets);
                            (bytes, packets) = (0, 0);
                        }
                    } else {
                        gauge.add(wire, 1);
                    }
                }
                gauge.add(bytes, packets);
            })
        })
        .collect();
    for w in workers {
        w.join().unwrap();
    }
    started.elapsed().as_secs_f64() * 1e9 / DATAGRAMS as f64 * THREADS as f64
}

#[test]
#[ignore = "a measurement, not a check"]
fn counting_per_datagram_against_per_batch() {
    let cores = std::thread::available_parallelism().map_or(1, usize::from);
    let each = run(false);
    let batched = run(true);
    println!(
        "{THREADS} threads on {cores} cores: {each:.1} ns of wall time per datagram per thread with an add each, \
         {batched:.1} ns with one add per {BATCH}"
    );
}
