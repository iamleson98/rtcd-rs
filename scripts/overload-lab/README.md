# The overload lab

What a node does when it is asked for more than it carries, with a real kernel between the node and
its people. Two containers on one Docker network (Colima or any Linux Docker): `lab-rtcd` (rtcd, three
CPUs, a memory limit, a token bucket on its interface so that the qdisc drops what a card would) and
`lab-gen` (`lms-sim load`: classes of synthetic participants that send voice and cameras and watch
cameras). The 2026-11-01 scale test saw a 4-vCPU node past ~720 sessions offer 6.6 Gbit/s, have the
kernel refuse a million datagrams and grow from 0.5 to 2.26 GB until the 3 GiB limit killed it; this
reproduces the shape at a smaller size.

```bash
docker build -f scripts/overload-lab/Dockerfile -t rtcd-lab:NAME .        # from the repository root
scripts/overload-lab/run.sh rtcd-lab:NAME NAME-run1 [RTCD_RTC_...=...]
```

`run.sh` takes its settings from the environment (`RATE_MBIT`, `MEM`, `CLASSES`, `STUDENTS`,
`CAMERAS`, `WATCH`, `PER_SECOND`, `HOLD`; defaults in the script). The people need the media files
`lms-sim` makes with ffmpeg the first time (`$TMPDIR/lms-sim-assets`, or `ASSETS=`).

`out/NAME.jsonl` has one line per sample (every 5 s) taken inside the node's container: the
process's resident set and high-water mark, the cgroup's memory and OOM kills, the kernel's UDP
counters (`udp_sndbuf_err` is the qdisc dropping what the node sent), `/system`'s figures and the
queue gauges (`rtcd_forward_queued_packets`, `rtcd_shard_turn_*`).

The container's CPUs are the VM's: the numbers say what a change does, run against the run before it
on the same machine, not what an OVH node carries.
