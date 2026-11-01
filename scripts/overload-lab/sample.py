#!/usr/bin/env python3
"""Samples a running rtcd from inside its container, once every INTERVAL seconds, one JSON object
per line: what the process holds (VmRSS, the cgroup's memory.current), what the kernel did with
its datagrams (UdpSndbufErrors, the interface's qdisc drops) and what rtcd says (/system,
selected /metrics). Usage: sample.py [interval_seconds] [seconds]."""
import json
import os
import re
import subprocess
import sys
import time
import urllib.request

INTERVAL = float(sys.argv[1]) if len(sys.argv) > 1 else 5
TOTAL = float(sys.argv[2]) if len(sys.argv) > 2 else 3600
API = os.environ.get("RTCD_API", "http://127.0.0.1:8045")


def pid():
    if os.environ.get("RTCD_PID"):
        return os.environ["RTCD_PID"]
    out = subprocess.run(["pgrep", "-x", "rtcd"], capture_output=True, text=True).stdout.split()
    return out[0] if out else None


def status(p):
    if not os.path.exists(f"/proc/{p}/status"):
        # Not Linux: ps knows the resident set, not its high-water mark.
        out = subprocess.run(["ps", "-o", "rss=", "-p", p], capture_output=True, text=True).stdout.strip()
        return {"rss_mb": int(out) // 1024} if out else {}
    try:
        text = open(f"/proc/{p}/status").read()
    except OSError:
        return {}
    m = re.search(r"VmRSS:\s+(\d+) kB", text)
    h = re.search(r"VmHWM:\s+(\d+) kB", text)
    return {"rss_mb": int(m.group(1)) // 1024 if m else 0, "hwm_mb": int(h.group(1)) // 1024 if h else 0}


def cgroup():
    out = {}
    for name, path in (("cg_mb", "/sys/fs/cgroup/memory.current"),):
        try:
            out[name] = int(open(path).read()) // (1 << 20)
        except OSError:
            pass
    try:
        for line in open("/sys/fs/cgroup/memory.events"):
            k, v = line.split()
            if k in ("oom_kill",):
                out[k] = int(v)
    except OSError:
        pass
    return out


def snmp_udp():
    if not os.path.exists("/proc/net/snmp"):
        return {}
    lines = [l.split() for l in open("/proc/net/snmp") if l.startswith("Udp:")]
    keys, vals = lines[0], lines[1]
    d = dict(zip(keys[1:], map(int, vals[1:])))
    return {"udp_out": d.get("OutDatagrams", 0), "udp_sndbuf_err": d.get("SndbufErrors", 0), "udp_rcvbuf_err": d.get("RcvbufErrors", 0)}


def get(path):
    try:
        with urllib.request.urlopen(API + path, timeout=2) as r:
            return r.read().decode()
    except Exception:
        return ""


METRICS = re.compile(r"^(rtcd_[a-z0-9_]+)(\{[^}]*\})? ([0-9.e+-]+)$")
KEEP = (
    "rtcd_rtc_receiver_loss_ratio_",
    "rtcd_rtc_receiver_feedback_total",
    "rtcd_rtc_retransmitted_packets_total",
    "rtcd_rtc_keyframe_requests_total",
    "rtcd_rtc_sessions_refused_total",
    "rtcd_rtc_retransmits_limited_total",
    "rtcd_net_packets_total",
    "rtcd_net_dropped",
    "rtcd_forward_",
    "rtcd_shard_turn",
    "rtcd_shard_",
    "rtcd_admission_",
    "rtcd_rtp_dropped_stale",
)


def metrics():
    out = {}
    for line in get("/metrics").splitlines():
        m = METRICS.match(line)
        if m and any(m.group(1).startswith(k) for k in KEEP):
            out[m.group(1) + (m.group(2) or "")] = float(m.group(3))
    return out


start = time.time()
while time.time() - start < TOTAL:
    t0 = time.time()
    row = {"t": int(t0 - start)}
    p = pid()
    if p:
        row.update(status(p))
    row.update(cgroup())
    row.update(snmp_udp())
    try:
        s = json.loads(get("/system") or "{}")
        for k in (
            "sessions",
            "shards",
            "shards_pressured",
            "shards_saturated",
            "egress_bps",
            "egress_pps",
            "kernel_send_drops",
            "egress_squeezed",
            "egress_refusing",
            "egress_choked",
            "egress_ceiling_bps",
            "cpu_load",
            "rss_bytes",
            "rss_limit_bytes",
        ):
            if k in s:
                row[k] = s[k]
    except ValueError:
        pass
    row.update(metrics())
    print(json.dumps(row), flush=True)
    time.sleep(max(0.0, INTERVAL - (time.time() - t0)))
