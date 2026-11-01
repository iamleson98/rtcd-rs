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

## Does the node find its card? (`RTCD_RTC_EGRESSAUTO`)

Runs of 2026-11-01 of the node that finds its card's ceilings (README, "Finding the card's
ceilings") against the release before it (`baseline`), on a 100 Mbit/s bucket. The lab's card is a tenth of the OVH
video node's, so the node's start is scaled the same way (`RTCD_RTC_EGRESSLEARNMINMBPS=40
RTCD_RTC_EGRESSLEARNMINPPS=8000`, 40 % of the card as the 400 and 80000 defaults are of 850 Mbit/s
and 160 thousand packets); the colima VM's six CPUs, three of them the node's. Each row is one run
of 150 s after everyone is in (`PER_SECOND=1`), samples every 2 s, the rate and the loss over the
time at least 85 % of the peak sessions were in. `deliv` is what the interface carried; `sndbuf` the
datagrams the kernel refused; `loss` the receiver-reported loss the node itself records.

| load | node | people in (peak) | deliv Mbit/s | kernel refused | joins refused | receiver loss | RSS peak MB | ceiling at end |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| 16 people, healthy | before | 16 | 39 | 0 | 0 | 0.00 % | 45 | none |
| | now | 16 | 39 | 0 | 0 | 0.00 % | 45 | 44 (start 40) |
| 123 people, `CAMERAS=40 WATCH=1` | before | 71 | 92 | 108,439 | 51 | 3.70 % | 119 | none |
| | before, `EGRESSCAPACITYMBPS=90` (refuse at 70 %) | 48 | 64 | 1,708 | 71 | 0.04 % | 87 | 90 |
| | now | 91 | 78 | 19,799 | 30 | 0.36 % | 110 | 84 |
| 123 people, `CAMERAS=8 WATCH=3` | before | 64 | 71 | 21,695 | 58 | 1.40 % | 68 | none |
| | before, `EGRESSCAPACITYMBPS=90` | 24 | 60 | 6,560 | 97 | 0.12 % | 55 | 90 |
| | now | 59 | 76 | 22,679 | 61 | 0.86 % | 71 | 73 |
| 366 people (10 times the card) | before | 73 | 96 | 138,909 | 289 | 4.33 % | 84 | none |
| | now | 29 | 71 | 30,548 | 332 | 0.86 % | 65 | 81 |

What it shows: a healthy node is not touched; a node that is asked for more than its card carries
takes 28 % more people than before with a tenth of the loss (the kernel refuses 5 times fewer
datagrams), and 1.9 times the people the planned-capacity rule (70 % of a figure that had to be
written down) carried; under ten times the card's demand the memory does not grow (65 MB against
84) and the kernel's refusals fall by four fifths. The last row is the price of starting low: a
cold node that is asked for ten times its card within a minute admits 29 people where the one
without a brake let 73 in (4.3 % loss for them), because its ceilings were still climbing from the
start when the storm was over; the generator does not retry a refused join, a browser does.

What it does not show, and the lab cannot: the kernel refuses this node at 55 to 80 % of the bucket's
rate, not at 100 %, because the refusals here are mostly send *buffers* overflowing in bursts (a
keyframe fanned out to a class), so the "wall" the node learns is where its bursts start to overflow
and the delivered rate is 76 to 78 % of the bucket, not 90 or more; a card that refuses cleanly up to
its rate (the OVH one: 800 to 850 Mbit/s of 1,000 with none refused) would be learned at its rate.
The CPUs are the VM's, so what a shard costs is not the OVH node's. The sustained climb from the
start to the wall (a minute or two on the OVH node) was not run against a real card.
