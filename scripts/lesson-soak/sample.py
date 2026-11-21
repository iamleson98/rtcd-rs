#!/usr/bin/env python3
"""Samples one SFU process from outside its container, the same way for every server (the Go one
is a distroless image with nothing to run inside it). Run with --pid=host and --privileged:

    sample.py PID|@PIDFILE IMPL OUT.jsonl INTERVAL_S SECONDS [CLIENT_PID_DIR]

One `proc` line per interval, in the soak JSONL schema: rss_mb, cpu_pct (of one core, so 200 is two
cores), threads, fds, and the UDP receive / send buffer errors of the process's network namespace.
Stops when the process is gone, with an `error` event.

With `@PIDFILE` the process is the one the file names, and the file is read again every interval: a
service that is restarted on purpose (the restart profile) is followed to its new process, and only
one that is not back within a minute is gone. CLIENT_PID_DIR holds one file per client container
(named for it, holding the pid of its main process); each is sampled as `proc` lines with the id
`client:<name>`: cpu_pct of the whole container (its cgroup, so the browsers' children count) and
`cores`, the container's CPU limit (its cpuset, or its quota when that is lower). Run with
--cgroupns=host so that the host's cgroups are what it reads.
"""
import json
import math
import os
import platform
import sys
import time


pid_spec, impl, out, interval, seconds = sys.argv[1], sys.argv[2], sys.argv[3], float(sys.argv[4]), float(sys.argv[5])
client_dir = sys.argv[6] if len(sys.argv) > 6 else None
arch = platform.machine()
ticks = os.sysconf("SC_CLK_TCK")
# How long a service that is restarted on purpose may take to be back.
RESTART_GRACE = 60.0
CGROUP = "/sys/fs/cgroup"


def read(path):
    with open(path) as f:
        return f.read()


def current_pid():
    return int(read(pid_spec[1:]).strip()) if pid_spec.startswith("@") else int(pid_spec)


def cpu_seconds(pid):
    # utime and stime follow the command's closing parenthesis (fields 14 and 15).
    fields = read(f"/proc/{pid}/stat").rsplit(")", 1)[1].split()
    return (int(fields[11]) + int(fields[12])) / ticks


def status(pid):
    values = {}
    for line in read(f"/proc/{pid}/status").splitlines():
        key, _, rest = line.partition(":")
        values[key] = rest.split()
    return float(values["VmRSS"][0]) / 1024, int(values["Threads"][0])


def udp_errors(pid):
    rows = [l.split() for l in read(f"/proc/{pid}/net/snmp").splitlines() if l.startswith("Udp:")]
    table = dict(zip(rows[0], rows[1]))
    return int(table.get("RcvbufErrors", 0)), int(table.get("SndbufErrors", 0))


def cgroup_dir(pid):
    for line in read(f"/proc/{pid}/cgroup").splitlines():
        if line.startswith("0::"):
            return CGROUP + line[3:]
    raise OSError(f"no cgroup v2 for {pid}")


def cgroup_cpu_seconds(path):
    for line in read(f"{path}/cpu.stat").splitlines():
        if line.startswith("usage_usec "):
            return int(line.split()[1]) / 1e6
    raise OSError(f"no usage_usec in {path}/cpu.stat")


def cpu_count(spec):
    """The CPUs in a cpuset list such as `0-1,4`."""
    total = 0
    for part in spec.strip().split(","):
        if part:
            lo, _, hi = part.partition("-")
            total += int(hi or lo) - int(lo) + 1
    return total


def cgroup_cores(path):
    cores = os.cpu_count() or 1
    try:
        cores = cpu_count(read(f"{path}/cpuset.cpus.effective")) or cores
    except OSError:
        pass
    try:
        quota, period = read(f"{path}/cpu.max").split()
        if quota != "max":
            cores = min(cores, math.ceil(int(quota) / int(period)))
    except (OSError, ValueError):
        pass
    return cores


def write(f, kind, ident="sfu", **fields):
    line = {"ts": int(time.time() * 1000), "impl": impl, "arch": arch, "kind": kind, "id": ident}
    line.update(fields)
    f.write(json.dumps(line) + "\n")
    f.flush()


def sample_clients(f, state, now):
    """One `proc` line for each client container whose pid file is there and whose process runs."""
    for name in sorted(os.listdir(client_dir)):
        try:
            pid = int(read(os.path.join(client_dir, name)).strip())
            path = cgroup_dir(pid)
            used = cgroup_cpu_seconds(path)
        except (OSError, ValueError):
            state.pop(name, None)
            continue
        before = state.get(name)
        state[name] = (pid, used, now)
        if before and before[0] == pid and now > before[2]:
            write(f, "proc", f"client:{name}", cpu_pct=round(100 * (used - before[1]) / (now - before[2]), 1), cores=cgroup_cores(path))


end = time.time() + seconds
with open(out, "a") as f:
    pid = current_pid()
    last_cpu, last_t = cpu_seconds(pid), time.time()
    gone_since = None
    clients = {}
    while time.time() < end:
        time.sleep(interval)
        if client_dir:
            sample_clients(f, clients, time.time())
        try:
            if pid_spec.startswith("@"):
                latest = current_pid()
                if latest != pid:
                    # Restarted: the new process is measured from its own start.
                    pid = latest
                    last_cpu, last_t = cpu_seconds(pid), time.time()
                    continue
            rss, threads = status(pid)
            cpu, now = cpu_seconds(pid), time.time()
            rcv, snd = udp_errors(pid)
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
            gone_since = None
        except (OSError, KeyError, IndexError, ValueError):
            if pid_spec.startswith("@"):
                gone_since = gone_since or time.time()
                if time.time() - gone_since < RESTART_GRACE:
                    continue
            write(f, "event", what="error", detail="the SFU process is gone")
            break
