# Lesson soak

Users report calls that start fine and freeze video or lose audio near the end of a lesson. This
plays the same two-hour lesson against rtcd-rs and against the upstream Go rtcd it replaced, on x64
and arm64, and compares how each one ages: counter wraparound, slow leaks, growing queues, and
recovery after loss and outages. rtcd-rs is gated on being at least as stable as the Go service on the
same run: it may not be worse than the Go baseline (see Rules).

## What it simulates

One class in one call, run by two `lms-sim lesson` processes (`crates/lms-sim/src/lesson.rs`):

| | |
|---|---|
| teacher (clean network) | microphone, camera, screen share in two simulcast layers |
| 25 students | 17 on the clean network, 8 on the impaired one; students 0 to 3 send a camera; one student's microphone is open at a time, 30 s each, round the class by the clock |
| watching | every student receives the teacher's voice, camera and screen and the four student cameras |
| Chromium, one per network | headless Chromium (Playwright 1.65.0) loads a page that joins as one more student, answers the SFU's offers over the `calls-dc` data channel like lms-web, watches the teacher's camera and screen and reports `getStats` |
| wraps | every sender starts its RTP sequence numbers 300 packets short of the wrap and its timestamps 20 s short (video 90 kHz, audio 48 kHz), so each run crosses every wrap; senders and one receiver per group log them as `wrap` events |

The **impaired** group is shaped **per participant** by `vendor/rtc/benchmarks/soak/netem.sh`,
through repeating phases: clean, 2 % loss, bursty loss, a bandwidth cap (2.5 Mbit/s down and 1 Mbit/s
up each) with 80 ms delay, a 5 s outage, recover. Each synthetic impaired student binds its media
sockets to a block of four UDP ports (`lms-sim lesson --udp-base 40000`, student k gets `40000 + 4k`),
and `netem.sh` (`NETEM_PORTS=40000:8`) gives each block its own htb class and netem leaf, on `eth0`
(what the student sends) and on its `ifb` copy (what it receives); the impaired Chromium has a network
namespace of its own, shaped on all its UDP by a second `netem.sh` (`NETEM_PORTS=udp`). A cap is
therefore each participant's, not shared by the group: in the bw phase each one's measured receive
rate (`bytes` of its `recv` lines, all tracks together) sits at its cap. The teacher and the **clean**
group are never touched, so drift on the server shows in the clean group and what the network does
shows in the impaired one.

**Signaling.** In production the browser's signaling websocket goes to the LMS server, which holds a
connection to rtcd on a datacenter link; only media, ICE, DTLS and SCTP cross the user's network. The
lesson mirrors that: the participants' websocket to the SFU (the LMS stand-in) is TCP and is not
shaped, and renegotiation after the first offer rides the `calls-dc` data channel (SCTP over DTLS,
on the shaped UDP path), as lms-web does. A first offer, the ICE candidates and `video_subscribe` go
over the websocket.

**Rejoin.** A participant whose session the SFU closes, whose websocket drops, whose media path
stays `failed` for 25 s (lms-web's `ICE_RESTART_GRACE_MS`; the simulator does not restart ICE) or
that does not connect within 30 s takes its seat back like lms-web (`features/calls/rejoin.ts`):
pauses of 1, 3, 8, 15 and 30 s, no new try later than 180 s after the drop, the same session id, a
fresh peer connection on a fresh port of its block. It logs `error` (the cause) when it loses the
seat and `event rejoin` with `downtime_ms` and `attempts` when it is back; the series of a later try
is `name#N`. The Chromium page does the same.

The SFU runs in a container held to 2 CPUs (cpuset) and 2 GiB, the clients on the other CPUs. Both servers
get the same settings through the `RTCD_*` variables they share: API address, self registration, ICE
host override, UDP and TCP port, log level.

## What is measured

Everything is JSONL, one object per line with `ts` (unix ms), `impl`, `arch` (`x86_64`, `aarch64`),
`kind` and `id`; the shared report tool (`vendor/rtc/benchmarks/soak/report.py`) reads it.

* `recv`, every 5 s per received track, counters since the start: `frames`, `freezes`, `freeze_ms`,
  `packets`, `bytes`, `duplicates`, `lost`, `nack`, `pli`, `jitter_ms`, `rtt_ms`, `audio_gap_ms`, `stalled`.
  `packets` counts every arrival; `lost` is what the sequence numbers say never came, after the
  retransmissions and without the duplicates, so a copy never hides a loss.
  `impl` is `rtcd-rs` or `rtcd-go` for the synthetic receivers (`lms_sim::stats`) and `rtcd-rs/chrome` or
  `rtcd-go/chrome` for Chromium. Each group measures its first 6 students (the teacher's voice, camera
  and screen, and the four student cameras) and one Chromium.
  * A freeze is a frame later than `max(3 x avg, avg + 150 ms)` after the last, over the last 30 frames
    (the WebRTC definition, which Chromium reports itself). The audio gap is the time audio was
    silent for more than 60 ms, summed (Chromium: concealed samples at 48 kHz). `stalled` is no media for 2 s.
  * Synthetic receivers send a PLI when a video has had no frame for a second. Their `nack` is the
    number of sequence numbers they saw skipped (the NACK generator asks for exactly those); Chromium's
    is its own `nackCount`. They are alike in kind, not in value.
* `proc`, every 5 s for the SFU process, read from outside the container (`sample.py`, with
  `--pid=host`) so it is the same for the distroless Go image and for ours: `rss_mb`, `cpu_pct` (of one
  core), `threads`, `fds`, `udp_rcvbuf_err`, `udp_sndbuf_err`.
* `event`: `ice_state`, `wrap`, `join`, `leave`, `rejoin`, `error`, each with the participant's
  `group`. `phase` comes from `netem.sh`.

## Rules

`rules.json` holds `rtcd-rs` against `rtcd-go` and `rtcd-rs/chrome` against `rtcd-go/chrome`, per
architecture, group and phase, on freezes, freeze time, audio gaps, residual loss, stalls, recovery
after outages, SFU-closed sessions and rejoin downtime, signaling closes, ICE disconnects, SFU CPU, RSS
(peak and slope), threads and file descriptors: the subject may exceed the baseline by 25 % plus a
small slack, and a count may not exceed the baseline's. The clean group must show zero stalls, freezes,
SFU closes and ICE disconnects; the SFU process must not die; after 15 minutes of warmup the first and the
last 20-minute clean window must not differ by more than 2x in freeze rate (plus 0.5/min) or audio gap
(plus 0.5 %) and the resident set must not grow more than 30 MB/h. Missing data for either side
fails the report. The details are in `vendor/rtc/benchmarks/soak/README.md`; the report is
`summary.md` (side by side per metric and phase, then the breaches) and `report.html` (charts of
rtcd-rs against Go).

## The Go baseline

`mattermost/rtcd` v1.2.6, the latest stable release (2026-06-29), pinned by tag and by the digest of
its multi-arch index (`sha256:a8c3c1c3...`, linux/amd64 and linux/arm64). The image speaks our wire
protocol and takes the same `RTCD_*` variables, so it runs as published: nothing is built from source.

## Run it locally (Colima)

```bash
colima start --cpu 6 --memory 10      # the SFU gets 2 CPUs, the clients and the browsers the rest
scripts/lesson-soak/run.sh rs 4 2 /tmp/soak     # 4 minutes, a 2-minute cycle, rtcd-rs
scripts/lesson-soak/run.sh go 4 2 /tmp/soak     # the same against the Go service
python3 vendor/rtc/benchmarks/soak/report.py --rules scripts/lesson-soak/rules.json --out /tmp/soak/report \
    /tmp/soak/rtcd-rs-*.jsonl /tmp/soak/rtcd-go-*.jsonl
```

The first run builds three images (`rtcd-rs:soak`, `rtcd-lesson:soak`, `rtcd-lesson-chrome:soak`; a few
GB); they stay for the next run and go with `docker rmi`. The merged file of a run is
`<outdir>/rtcd-<impl>-<arch>.jsonl`; the per-process files, `machine.txt` (architecture, kernel, cores,
CPU) and the containers' whole logs (the SFU's included, gzipped when over 10 MB) stay beside it.
`PROFILE=abuse` also needs the report's `rules-abuse.json`. Short runs only prove the harness: the gates need a warmup of 15 minutes and two 20-minute windows.
On a Mac the architecture is aarch64; x64 is exercised by CI.

## In CI

`.github/workflows/lesson-soak.yml` runs, on x64 (`ubuntu-latest`) and arm64 (`ubuntu-24.04-arm`), the
`lesson` profile and the `abuse` profile (below) for `rs` and `go` in parallel: weekly on Saturday, by
hand (`minutes`, default 120) and on pull requests that touch `crates/lms-sim`, this directory or the
workflow (10 minutes, 5-minute cycle, a short calibration ramp). One `report` job per architecture and
profile runs `report.py` over the files of both services (`rules.json` for `lesson`, `rules-abuse.json`
for `abuse`), writes `summary.md` to the run's summary and uploads `report.html` and the JSONL; it fails
when rtcd-rs broke a rule, when a soak job failed (an SFU crash fails `run.sh`) or when a soak left no
measurements. Both services run on the same runner type for each architecture. Artifacts are named
`lesson-soak-<profile>-<impl>-<arch>`.

## Abuse profile

`PROFILE=abuse` runs the lesson above with the SFU held at its limit for hours, the same for rtcd-rs
and the Go rtcd, deterministic from one seed, and nothing leaves the runner. It is for faults that
only a long, hostile run shows: leaks, a crash, a server that stops recovering.

```bash
SEED=7 PROFILE=abuse LOAD_CLASSES=10 scripts/lesson-soak/run.sh rs 10 5 out
```

`SEED` defaults to today's date; the seed is printed, written to `<outdir>/seed.txt` and is in every
`attack` row, so a failing run is replayed with it (the network's schedule and the attack order replay;
the timing of the machine does not). CI uses the run id, so both services and both architectures of a
run meet the same attacks.

**What it does**, on top of the lesson (`lesson.rs` and `abuse.rs` of `crates/lms-sim`; the network
classes are `vendor/rtc/benchmarks/soak/netem.sh`):

* **Load at the limit.** `lms-sim load` holds `LOAD_CLASSES` background classes (30 students each,
  4 cameras) in a container of its own for the whole run. CI sets it to 90 % of the lower of the two
  services' ceilings (below), so neither is held to a load the other could not carry.
* **Churn.** The teacher leaves and rejoins every 15 minutes (`--teacher-reconnect-every 15`); the abuse
  client's participants join and leave in the same call (`join-leave`, `camera-toggle`, `mic-toggle`,
  `screen-toggle`, `layer-flap`, `teacher-reconnect`).
* **Network abuse per participant.** `NETEM_PROFILE=abuse`: every impaired student and the impaired
  Chromium has its own schedule of 3 to 10 s scenes from the seed: `reorder`, `duplicate`, `jitter`,
  `asym-loss`, `bw-flap` and, rarely, an outage (`outage-30`, `outage-60`, `outage-120`). The impaired
  students also change their source port every 30 s (`nat-rebind`, `--nat-rebind-every 30`) and send
  media at a path MTU that drops large packets (`mtu`, `--mtu`).
* **Hostile clients** (`lms-sim abuse`, its own container and address, not shaped), as participants of the
  same call: `rtp-jump`, `ssrc-change`, `pli-flood`, `nack-flood`, `bogus-rtcp`, `rtp-size`,
  `rtcp-garbage`, `dc-flood`, `dc-oversize`, `renego-storm`, `glare`, `late-answer`.
* **Junk at the media port**: `udp-junk` (random UDP), `stun-flood` (binding requests with made-up
  credentials), `dtls-junk` (garbage DTLS records).
* **Signaling abuse**: `ws-storm` (websocket connections opened and dropped in bursts), `dup-join` (the same
  session joining twice), `unknown-call` (messages for calls that do not exist).

`ABUSE_ONLY=a,b` restricts the classes and `ABUSE_INTENSITY=1..5` scales them (`--only`, `--intensity`).
Every window is an `attack` row pair (`start`, `end`) in the merged JSONL, with the participants it
concerned in `who`.

**What it measures** (the report, see `vendor/rtc/benchmarks/soak/README.md`):

* crashes: `run.sh` writes an `event` row (`id sfu`, `what error`) when the SFU container stopped,
  restarted (it never restarts by itself) or was killed for memory (`OOMKilled`), and when its whole log
  has `panic`, `panicked at`, `fatal error`, `SIGSEGV` or `SIGABRT` (any case). Both profiles do this, and
  `run.sh` then exits non-zero; it also does when a lesson container failed or the abuse clients wrote
  no `attack` row;
* leaks: the slopes of RSS, threads and file descriptors after warmup, against the baseline;
* CPU saturation: the share of `proc` samples at or above `cpu_sat_pct` (190 of the two cores' 200);
* recovery: for each attack window, what the clean (innocent) students' measured receivers saw in it and
  how long their tracks took to flow again after it (`recover_s`, tracks that `never` did), per attack class;
* the windows are listed in `summary.md` with their impact.

**What gates** (`rules-abuse.json`; the same pairs as `rules.json`, rtcd-rs against the Go rtcd).
Each deviation from `rules.json` is on purpose:

| Key | Value | Why |
|---|---|---|
| `ratio` | 1.5 | at the limit both services are noisy; 1.25 would breach on chance |
| `recover_window_s` | 240 | ICE gives a lost path 240 s; a track not back after that never recovers |
| `slack.freezes` | 1000000 | freezes are not gated as a count, the freeze rate is |
| `slack.stalls`, `stall_max` | 15, 10 s | an attack window overlaps whatever network scene is running |
| `slack.ice_disc` | 3 | outages of up to 120 s disconnect ICE |
| `slack.recover_s`, `rec_med`, `rec_p90` | 120, 10, 30 s | recovery after the longest outage |
| `slack.threads_slope`, `fds_slope` | 40, 200 per hour | connection churn moves them; a leak still shows as a slope above the baseline |
| `slack.cpu_sat`, `cpu` | 40 points, 10 | the load is at 90 % of the ceiling, so some saturation is expected on both |
| `freeze_rate_ratio_max`, `audio_gap_ratio_max` | 3 | the first and last clean windows differ under constant abuse |
| `rss_slope_mb_per_h_max` | 50 | the churn and the load keep growing the working set for longer |
| `clean_zero` | none | no group is clean while the server is under attack; the clean group is judged window by window |

Not relaxed: SFU process errors must be zero (a crash fails), tracks that never recover and SFU closes
stay at the baseline, as does everything not listed. The slack values are a first guess, to be tuned
from the first long runs.

**The load ceiling** (`calibrate.sh <rs|go> <outdir>`). On the same SFU container (2 CPUs) it ramps
`RAMP_CLASSES_PER_STEP` (1) classes every `RAMP_STEP_SECS` (60) up to `RAMP_MAX_CLASSES` (40) with `lms-sim
ramp` while a small measured lesson (a teacher and two students) runs through it. The ramp ends when the
SFU's `cpu_pct` is at 180 or more for 30 s (`cpu`), or the lesson's receivers freeze `RAMP_FREEZE_MAX` (6)
times a minute or stall (`quality`), or it reaches the top (`max`); the ceiling is the classes present when
that began less the step that began it. `ceiling-<impl>-<arch>.json` has `ceiling_classes`, `reason`,
`cpu_peak` and `students_per_class`; the ramp, the SFU samples and the lesson's rows stay beside it
(`calibrate-*-<impl>.jsonl`). The client containers share the runner's remaining CPUs: when they run out
before the SFU does, the lesson's quality is what ends the ramp, which is the real limit of that machine.
`hold.py <arch> ceiling-rs-<arch>.json ceiling-go-<arch>.json` prints `hold_classes=floor(0.9 x min of the
two)` and fails loudly when a file is missing or is for another architecture. In CI the `calibrate`
job runs both services on the same runner type as the arch's soak jobs (steps of 40 s, 2 classes, at most
30 classes: 10 minutes each; 20 s, 1 class and 6 classes on pull requests) and the abuse jobs read
`hold.txt` from its artifact.

Two things need a real run: whether the thresholds above (180 % for 30 s, 6 freezes a minute) end a ramp
where the node really breaks, and the slack values.

## Reading the report

Start with the summary table: per service and architecture, the freeze rate, audio gap, RSS slope,
stalls and recoveries of the first and last clean window. rtcd-rs against Go on the same architecture
tells whether a problem is ours; the same service first against last window tells drift. A `clean`
group that drifts and an `impaired` one that does not means the server; the other way round, the
network or the recovery path. Then `report.html`: the series per track with the netem phases
shaded, and the `wrap` events to see what happened as a counter turned over. A stall that outlasts
an outage, or an `error` event, is a join or session that did not survive.

## Versions

Rust 1.99.1 (`Dockerfile`s), Debian bookworm, Playwright 1.65.0 with
`mcr.microsoft.com/playwright:v1.65.0-noble@sha256:9beb5460...` (its Chromium, not Google Chrome: there is
none for linux/arm64), `mattermost/rtcd` v1.2.6 above.
