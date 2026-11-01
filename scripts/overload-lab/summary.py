#!/usr/bin/env python3
"""Summarises runs of the overload lab: summary.py out/A.jsonl out/B.jsonl ... One row each."""
import json
import sys


def rows(path):
    out = []
    for line in open(path):
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def peak(rs, key):
    return max((r.get(key, 0) or 0 for r in rs), default=0)


def counter(rs, key):
    vals = [r[key] for r in rs if key in r]
    return int(vals[-1] - vals[0]) if vals else 0


def labelled(rs, prefix):
    keys = {k for r in rs for k in r if k.startswith(prefix)}
    return sum(counter(rs, k) for k in keys)


cols = (
    ("run", 18), ("sess", 5), ("rss_pk", 7), ("hwm", 5), ("egr_pk", 7), ("turn_ms", 8),
    ("tx/turn", 8), ("rd/turn", 8), ("fwdq", 6), ("fwd_drop", 8), ("refused", 8), ("sndbuf", 8),
)
print(" ".join(f"{n:>{w}}" for n, w in cols))
for path in sys.argv[1:]:
    rs = rows(path)
    name = path.rsplit("/", 1)[-1].replace(".jsonl", "")
    vals = (
        name[:18],
        peak(rs, "sessions"),
        peak(rs, "rss_mb"),
        peak(rs, "hwm_mb"),
        round(peak(rs, "egress_bps") / 1e6),
        round(peak(rs, "rtcd_shard_turn_longest_seconds") * 1000),
        int(peak(rs, "rtcd_shard_turn_peak_transmits")),
        int(peak(rs, "rtcd_shard_turn_peak_reads")),
        int(peak(rs, "rtcd_forward_queued_packets")),
        int(peak(rs, "rtcd_forward_dropped_packets_total")),
        labelled(rs, "rtcd_rtc_sessions_refused_total"),
        counter(rs, "udp_sndbuf_err"),
    )
    print(" ".join(f"{v:>{w}}" for v, (_, w) in zip(vals, cols)))
