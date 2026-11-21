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
| cameras | as lms-web sends them (`cameraEncodings`): rid `l` 320x180, 15 fps, 150 kbit/s and rid `h` 640x360 (the teacher's 24 fps, 600 kbit/s, a student's 20 fps, 400 kbit/s), VP8 with three temporal layers (frames in layers 0, 2, 1, 2), a keyframe at the start and on each PLI or FIR for either layer. `--camera-layers single` (`lesson` and `abuse`) sends the one 640x360, 30 fps stream of before, to reproduce those runs |
| 25 students | 17 on the clean network, 8 on the impaired one; students 0 to 3 send a camera; one student's microphone is open at a time, 30 s each, round the class by the clock |
| watching | every student receives the teacher's voice, camera and screen and the four student cameras |
| Chromium, one per network | headless Chromium (Playwright 1.65.0) loads a page that joins as one more student, answers the SFU's offers over the `calls-dc` data channel like lms-web, watches the teacher's camera and screen, reports `getStats` and scores the teacher's picture against the clip it was sent from (below) |
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
Before the SFU is used, `lib.sh` turns UDP segmentation offload off on its `eth0` (and records it in
`machine.txt`): rtcd-rs sends a peer's datagrams as one GSO super-packet, which the shaper would drop and
count as one packet, so the stack must split it into the wire packets Go sends and a real link carries.

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

The SFU runs in a container held to 2 CPUs (cpuset) and 4 GiB, the clients on the other CPUs. Both servers
get the same settings through the `RTCD_*` variables they share: API address, self registration, ICE
host override, UDP and TCP port, log level.

## What is measured

Everything is JSONL, one object per line with `ts` (unix ms), `impl`, `arch` (`x86_64`, `aarch64`),
`kind` and `id`; the shared report tool (`vendor/rtc/benchmarks/soak/report.py`) reads it.

* `recv`, every 5 s per received track, counters since the start: `frames`, `freezes`, `freeze_ms`,
  `packets`, `bytes`, `duplicates`, `lost`, `nack`, `pli`, `jitter_ms`, `rtt_ms`, `audio_gap_ms`, `stalled`; Chromium rows of voices also carry `playout_delay_ms` (its mean jitter buffer delay).
  `delay_p50_ms` and `delay_p95_ms` are the one-way delay over the interval (null when there is none): of
  the synthetic receivers, from the sender's RTP clock (each sender says where each of its layers' clocks
  stands on the wall clock, in `rtp_clock` events and in `rtp.clocks` next to the output, which the lesson
  processes of the run share: one host, one wall clock, so the impaired group has delays too) to the arrival of each audio packet and of each frame's marker packet; of Chromium, from
  `getSynchronizationSources()` once a second (the playout time of the RTP timestamp it last played,
  mapped to the sender's clock by the relay; the jitter buffer's delay plus half the round trip when
  Chromium gives no timestamp), so it includes the jitter buffer. A group on another machine (`remote`)
  has no sender clock and writes null. Chromium rows of voices also carry `concealed_samples`,
  `concealment_events` and `fec_packets` (the interval's counts), and each page logs an `sdp_codecs`
  event with the audio `rtpmap` lines it negotiated and the audio codecs the browser can receive
  (`receiver capabilities`, to see that it has `audio/red`). A synthetic receiver started with `--red` negotiates
  redundant audio (`recovered` counts the voice packets it made up for, and a silence they fill is not an
  audio gap); `PROFILE=lossy` gives it to every second impaired student, the others take plain Opus.
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
  core), `threads`, `fds`, `udp_rcvbuf_err`, `udp_sndbuf_err`. The same sampler writes a `proc` row with
  the id `client:<container>` for every client container (the lessons, the browsers, the load): `cpu_pct`
  of the whole container (its cgroup) and `cores`, its CPU limit, so that a run whose clients starved
  can be told from one whose SFU did.
* `quality`, every 5 s per Chromium: the picture score of the teacher's camera, PSNR and SSIM against
  the clip (see Picture quality).
* `event`: `ice_state`, `wrap`, `join`, `leave`, `rejoin`, `error`, each with the participant's
  `group`. `phase` comes from `netem.sh`; `sfu_restart` (detail: its number) from `run.sh`, at the moment
  it restarts the SFU (the restart profile).

## Rules

`rules.json` holds `rtcd-rs` against `rtcd-go` and `rtcd-rs/chrome` against `rtcd-go/chrome`, per
architecture, group and phase, on freezes, freeze time, audio gaps, residual loss, stalls, recovery
after outages, SFU-closed sessions and rejoin downtime, signaling closes, ICE disconnects, SFU CPU, RSS
(peak and slope), threads and file descriptors: the subject may exceed the baseline by 25 % plus a
small slack, and a count may not exceed the baseline's. The clean group must show zero stalls, freezes,
SFU closes and ICE disconnects (for the Chromium receivers, freezes are left to the comparison: a headless
browser on a shared runner freezes on its own, on either server); the SFU process must not die. A Go
baseline that dies (killed for memory, crashed) does not fail the run: the report says when it stopped
and compares rtcd-rs over the minutes it ran; after 15 minutes of warmup the first and the
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
# Colima mounts only $HOME into its VM: keep the output directory under it.
colima start --cpu 6 --memory 10      # the SFU gets 2 CPUs, the clients and the browsers the rest
scripts/lesson-soak/run.sh rs 4 2 ~/soak     # 4 minutes, a 2-minute cycle, rtcd-rs
scripts/lesson-soak/run.sh rs-budget 4 2 ~/soak   # rtcd-rs with the camera budget on (rs: off)
scripts/lesson-soak/run.sh go 4 2 ~/soak     # the same against the Go service
# the pairs for the services that ran (rs-budget, as rtcd-rs ships, against go gates; rs is report only)
python3 scripts/lesson-soak/pairs.py scripts/lesson-soak/rules.json ~/soak/pairs.json rs rs-budget go
python3 vendor/rtc/benchmarks/soak/report.py --rules ~/soak/pairs.json --out ~/soak/report \
    ~/soak/rtcd-rs-*.jsonl ~/soak/rtcd-rs-budget-*.jsonl ~/soak/rtcd-go-*.jsonl
```

The first run builds three images (`rtcd-rs:soak`, `rtcd-lesson:soak`, `rtcd-lesson-chrome:soak`; a few
GB); they stay for the next run and go with `docker rmi`. The merged file of a run is
`<outdir>/rtcd-<impl>-<arch>.jsonl`; the per-process files, `machine.txt` (architecture, kernel, cores,
CPU) and the containers' whole logs (the SFU's included, gzipped when over 10 MB) stay beside it.
`PROFILE=abuse` also needs the report's `rules-abuse.json`. Short runs only prove the harness: the gates need a warmup of 15 minutes and two 20-minute windows.
On a Mac the architecture is aarch64; x64 is exercised by CI.

## In CI

`.github/workflows/lesson-soak.yml` runs, on x64 (`ubuntu-latest`) and arm64 (`ubuntu-24.04-arm`), the
`lesson` profile and the `abuse` profile (below) for `rs`, `rs-budget` and `go` in parallel: weekly on
Saturday, by hand (`minutes`, default 120; `budget`: `both`, the default, `on` for rs-budget and go, `off` for
rs and go) and on pull requests that touch `crates/lms-sim`, this directory or the
workflow (10 minutes, 5-minute cycle, a short calibration ramp). By hand `profile` picks what runs (`all`, the default,
or one of `lesson`, `abuse`, `lossy`, `restart`, `big`) and `nopace` adds the `rs-nopace` leg; the Meet-style
profiles (below) run weekly and by hand, never on pull requests. `rs` and `rs-budget` are the same image with `RTCD_RTC_CAMERABUDGET` set to `false` and `true` (always set, so
a change of the service's default moves neither), labelled `rtcd-rs` and `rtcd-rs-budget`; every leg uses one `SEED`
(the run id). One `report` job per architecture and
profile runs `report.py` over the files of the services that ran (`rules.json` for `lesson`, `rules-abuse.json`
for `abuse`, turned into pairs by `pairs.py`: rtcd-rs as it ships, rtcd-rs-budget (the camera budget is on by default), is
gated against Go, and rtcd-rs with the budget off, against Go and against it, is in the report only;
with `budget: off` rtcd-rs is gated against Go), writes `summary.md` to the run's summary and uploads `report.html` and the JSONL; it fails
when rtcd-rs-budget (rtcd-rs when the budget legs are off) broke a rule, when a soak job failed (an SFU crash fails `run.sh`) or when a soak left no
measurements. Both services run on the same runner type for each architecture. Artifacts are named
`lesson-soak-<profile>-<impl>-<arch>`.

## Meet-style profiles

Three short profiles hold the SFU to what a video-meeting service promises, beside the same service
as the Go baseline (`rules-lossy.json`, `rules-restart.json` and `rules-big.json`: the pairs of
`rules.json` at the same 1.25 ratio plus slack, and a list of absolute `targets` that are not gated, shown
in the report as PASS or MISS, except the one hard limit of the restart profile). Each is `run.sh` with
`PROFILE=` and runs as its own `meet` job of the workflow.

| `PROFILE` | What it does | In CI | Meet targets (not gated) |
|---|---|---|---|
| `lossy` | the impaired group's random loss goes through `clean`, `loss10`, `loss20`, `loss30` and `recover` (`NETEM_PROFILE=loss-sweep`; one sweep is one cycle, so `minutes` and `cycle` are the same) | 25 minutes | audio gap and concealed audio at `loss20` at most 5 %; audio delay p95 at most 250 ms (clean group) |
| `restart` | the SFU's container is killed (SIGKILL) and started again at 30 % and at 70 % of the lesson (3 and 7 minutes of 10) on a network that is not shaped; each logs an `sfu_restart` event at the kill and an `sfu_up` event when the SFU answers | 10 minutes | every track receives again within 30 s; audio delay p95 at most 250 ms; gated: within 180 s (a track that does not come back in the report's 150 s window counts as never) |
| `big` | `STUDENTS_CLEAN=80`, `STUDENTS_IMPAIRED=12`, 25 cameras, the teacher's screen share, a speaker every 20 s (`TALK_SECS=20`) and `--watch-max 12`: each student receives the teacher and the first 11 other cameras, what lms-web's stage policy receives on a wide screen | 30 minutes | audio delay p95 at most 250 ms (clean group) |

```bash
PROFILE=lossy scripts/lesson-soak/run.sh rs-budget 25 25 ~/soak
PROFILE=restart scripts/lesson-soak/run.sh rs-budget 10 10 ~/soak
PROFILE=big scripts/lesson-soak/run.sh rs-budget 30 15 ~/soak
python3 scripts/lesson-soak/pairs.py scripts/lesson-soak/rules-restart.json ~/soak/pairs.json rs-budget go
```

The restart is abrupt, the same for both services (a crash or a reboot): the container is killed and started again with the same address
(`restart_sfu` in `lib.sh`). Its network namespace is made anew, so the segmentation offload is turned
off again, the sampler follows the SFU to its new process (`sfu.pid`), and `run.sh` does not take
the absence for a crash: the health check ignores the restarts that `restarts.jsonl` says were made. The
browser's recovery is lms-web's (`call-client.ts`): an ICE restart after 4 s of "disconnected" or at
once on "failed" (with 25 s to come back), an offer the SFU does not answer sent again after 10 s and
the seat given up after the second wait; with `--chrome-recovery` (`run.sh` sets it, and the page's `recovery=1`, for these three profiles only) the page's signaling websocket stays open and the relay opens a
session on the SFU for each `join`, as the LMS server does; the lesson and abuse profiles keep the older page. The synthetic students rejoin as they do in
the abuse profile.

A run is invalid (the report says so) when a client container was above 90 % of its CPU limit in more
than 5 % of its samples, or when more than 1 % of the delay samples fall outside 0 to 2000 ms (the clock
mapping is broken); the `big` profile is the one that needs a runner with CPUs to spare.

`rs-nopace` (by hand only) is `rs-budget` with `RTCD_RTC_PACING=false`: the video is sent as each frame
is produced, as the Go service does. It is in the report only.

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

## Picture quality

The counters above say whether the video froze or lost packets, not how it looks. Each Chromium also
scores the teacher's camera as it receives it against the clip the teacher sent, as a conferencing
product is tested: PSNR (dB) and SSIM of the luma (`crates/lms-sim/src/quality.js`, run by the page
`crates/lms-sim/src/page.html`).

* **The reference.** The lesson process serves the clips the teacher's camera is sent from, one per
  simulcast layer (`/reference/0.ivf` the large layer, `1.ivf` the small one; the plain 640x360 stream
  with `--camera-layers single` has the first only). The page decodes them once with WebCodecs (VP8,
  declared limited-range BT.601, which is how Chromium shows a WebRTC VP8 picture: without that
  WebCodecs gives the same frames another range, and a perfect picture scores 24 dB), so a reference is
  what an undamaged stream decodes to, by the same libvpx as the browser's own decoder; a clean path
  scores 60 dB and SSIM 1. The page is not a secure context, so `chrome/run.mjs` tells Chromium to
  treat its origin as one (`--unsafely-treat-insecure-origin-as-secure`; WebCodecs needs it). Without
  WebCodecs the page logs `no picture score` and the soak goes on without the rows.
* **Matching a frame to its source.** By looking, not by counting. The sender starts its clip over on
  every keyframe request while its RTP timestamps run on, and the two layers have other frame rates,
  so neither the RTP timestamp nor the arrival time says which frame of which clip a received one is.
  Twice a second the frame on screen (if it is a new one: a frozen picture is the freeze counters'
  business) is drawn at 320x180; in each clip the three frames nearest to it on 32x18 thumbnails are
  compared at full size, and the best PSNR over both clips is the score. That is the usual no-clock
  way to score, and it reads a little better than the picture looks when a frame is destroyed beyond
  recognition (the match is the best of a clip, not the frame that was sent). The small layer is a
  render of the test pattern of its own, not the large layer's picture made small, so a picture is
  scored against its own layer: the score is fidelity to what the sender sent, and the resolution
  the viewer got is the `height` of the row.
* **What the numbers mean.** Both pictures are drawn to the same canvas (320x180, luma by BT.601). PSNR
  stops at 60 dB (identical). SSIM is the mean of 8x8 windows every 4 pixels.
* **What it can show.** A browser shows a VP8 frame decoded exactly or not at all (a frame with a
  missing packet is dropped and a keyframe asked for), so on a healthy forwarding SFU the score is 60 dB
  and SSIM 1 even through loss, and a freeze costs nothing here (the freeze counters have it). It falls
  when a frame is decoded against the wrong reference: a rewritten picture id or temporal layer index,
  a layer switch that is not on a keyframe, a frame the receiver did not notice was missing, the smeared
  picture of a call that "looks bad" after an outage. Which layer the viewer got is `height` (360 or 180)
  and the `large layer` column of `quality.py`.
* **Output.** One `quality` row per Chromium and 5 seconds, id `chrome-<group>/video:teacher`: `samples`,
  `psnr_db` and `ssim` (means), `psnr_min_db` and `ssim_min` (worst sample), `height` (of the picture on
  screen at the last sample: 360 large layer, 180 small). Cost: about 2 frames a second at a few milliseconds each.
* **Reading it.** `python3 scripts/lesson-soak/quality.py ~/soak/rtcd-*.jsonl` prints one row per
  service, architecture, group and netem phase. It only shows values: `report.py` lives in the `vendor/rtc`
  submodule and ignores unknown kinds, so nothing gates on the picture yet (a gate needs a threshold
  from real runs first, then a rule there). CI appends the table to each report job's summary.

## A client across the internet

Everything above runs on one box, where the network is a shaper. To see what a browser at home gets, a
Chromium can run on another machine, with no shaping, against the SFU that `run.sh` started:

```bash
# on the SFU host: the address the other machine reaches it on (a public one, or a LAN address to try it)
REMOTE_CLIENTS=1 PUBLIC_IP=203.0.113.7 scripts/lesson-soak/run.sh rs-budget 20 10 ~/soak
# on the other machine (Docker and this directory), once run.sh has printed "remote client:"
scripts/lesson-soak/remote-client.sh 203.0.113.7 20 ~/remote
```

`REMOTE_CLIENTS=1` changes three things and nothing else: the SFU's media port 8443 (UDP and TCP) is
published on the host; one more lesson process with no students (`--group remote`) serves the page and
the relay of the remote browser, ports 8099 and 8100, also published; and that relay tells the
browser the SFU is at `PUBLIC_IP` (`--chrome-advertise`: it replaces the SFU's address on the
lesson network in the descriptions and candidates it passes on, as an SFU behind NAT offers its public
address with `ice_host_override`, here only to the one client that needs it). The SFU's own settings, the
synthetic students and the local Chromium are untouched, and the SFU's API port (8045, which accepts self
registration) is not published. In production the browser's signaling also crosses the internet to a
server that relays it, so the page and relay are the realistic path; the media goes straight to the SFU.
`REMOTE_BIND` (default every address) limits which host address the ports are published on; the relay has no
authentication, so open the three ports to the other machine's address only, for the length of the run.

The browser is the same container and page as the local ones, so it also reports the picture score. What
it measures goes back over the relay and is written on the SFU host, with the other clients', as
`remote.jsonl` and into the merged file, as `impl` `rtcd-<service>/chrome-remote` (for example `rtcd-rs-budget/chrome-remote`),
`group` `remote`. That is not an impl the rules pair, so `report.py` shows it as its own series without
gating on it, and `quality.py` prints its picture score beside the others. The other machine's `<outdir>`
gets only `chrome-remote.log`, the page's console. `remote-client.sh` waits for the SFU host's page to answer
(`WAIT_SECS`, 300), runs the browser for the given minutes (the soak's) and stops it; it fails when the
browser stops early. The image is built from `chrome/` on first use (`CHROME_IMAGE` takes a prebuilt one).
To try it on one machine, point both at the machine's own address (not 127.0.0.1: the browser runs in a
container).

## Reading the report

Start with the summary table: per service and architecture, the freeze rate, audio gap, RSS slope,
stalls and recoveries of the first and last clean window. rtcd-rs against Go on the same architecture
tells whether a problem is ours; the same service first against last window tells drift. A `clean`
group that drifts and an `impaired` one that does not means the server; the other way round, the
network or the recovery path. Then `report.html`: the series per track with the netem phases
shaded, and the `wrap` events to see what happened as a counter turned over. A stall that outlasts
an outage, or an `error` event, is a join or session that did not survive. `quality.py` (above) says
how the teacher's picture looked in each group and phase: a falling `large layer` share is the SFU or the
network moving the browser to the small layer, a drop in PSNR and SSIM is damage to the frames.

## Versions

Rust 1.99.1 (`Dockerfile`s), Debian bookworm, Playwright 1.65.0 with
`mcr.microsoft.com/playwright:v1.65.0-noble@sha256:9beb5460...` (its Chromium, not Google Chrome: there is
none for linux/arm64), `mattermost/rtcd` v1.2.6 above.
