#!/usr/bin/env python3
"""Samples one SFU process from outside its container, the same way for every server (the Go one
is a distroless image with nothing to run inside it). Run with --pid=host and --privileged:

    sample.py PID IMPL OUT.jsonl INTERVAL_S SECONDS

One `proc` line per interval, in the soak JSONL schema: rss_mb, cpu_pct (of one core, so 200 is two
cores), threads, fds, and the UDP receive / send buffer errors of the process's network namespace.
Stops when the process is gone, with an `error` event.
"""
import json
import os
import platform
import sys
import time

pid, impl, out, interval, seconds = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4]), float(sys.argv[5])
arch = platform.machine()
ticks = os.sysconf("SC_CLK_TCK")


def read(path):
    with open(path) as f:
        return f.read()


def cpu_seconds():
    # utime and stime follow the command's closing parenthesis (fields 14 and 15).
    fields = read(f"/proc/{pid}/stat").rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / ticks


def status():
    values = {}
    for line in read(f"/proc/{pid}/status").splitlines():
        key, _, rest = line.partition(":")
        values[key] = rest.split()
    return float(values["VmRSS"][0]) / 1024, int(values["Threads"][0])


def udp_errors():
    rows = [l.split() for l in read(f"/proc/{pid}/net/snmp").splitlines() if l.startswith("Udp:")]
    table = dict(zip(rows[0], rows[1]))
    return int(table.get("RcvbufErrors", 0)), int(table.get("SndbufErrors", 0))


def write(f, kind, **fields):
    line = {"ts": int(time.time() * 1000), "impl": impl, "arch": arch, "kind": kind, "id": "sfu"}
    line.update(fields)
    f.write(json.dumps(line) + "\n")
    f.flush()


end = time.time() + seconds
with open(out, "a") as f:
    last_cpu, last_t = cpu_seconds(), time.time()
    while time.time() < end:
        time.sleep(interval)
        try:
            rss, threads = status()
            cpu, now = cpu_seconds(), time.time()
            rcv, snd = udp_errors()
            write(
                f,
                "proc",
                rss_mb=round(rss, 1),
                cpu_pct=round(100 * (cpu - last_cpu) / (now - last_t), 1),
                threads=threads,
                fds=len(os.listdir(f"/proc/{pid}/fd")),
                udp_rcvbuf_err=rcv,
                udp_sndbuf_err=snd,
            )
            last_cpu, last_t = cpu, now
        except (OSError, KeyError, IndexError):
            write(f, "event", what="error", detail="the SFU process is gone")
            break
