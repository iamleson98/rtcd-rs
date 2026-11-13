# Lesson soak

Users report calls that start fine and freeze video or lose audio near the end of a lesson. This
plays the same two-hour lesson against rtcd-rs and against the upstream Go rtcd it replaced, on x64
and arm64, and compares how each one ages: counter wraparound, slow leaks, growing queues, and
recovery after loss and outages. Only rtcd-rs gates; the Go service is the baseline.

## What it simulates

One class in one call, run by two `lms-sim lesson` processes (`crates/lms-sim/src/lesson.rs`):

| | |
|---|---|
| teacher (clean network) | microphone, camera, screen share in two simulcast layers |
| 25 students | 17 on the clean network, 8 on the impaired one; students 0 to 3 send a camera; one student's microphone is open at a time, 30 s each, round the class by the clock |
| watching | every student receives the teacher's voice, camera and screen and the four student cameras |
| Chromium, one per network | headless Chromium (Playwright 1.65.0) loads a page that joins as one more student, answers the SFU's offers over the `calls-dc` data channel like lms-web, watches the teacher's camera and screen and reports `getStats` |
| wraps | every sender starts its RTP sequence numbers 300 packets short of the wrap and its timestamps 20 s short (video 90 kHz, audio 48 kHz), so each run crosses every wrap; senders and one receiver per group log them as `wrap` events |

The **impaired** container (own network namespace) is shaped by `vendor/rtc/benchmarks/soak/netem.sh` on
its `eth0`, in both directions, through repeating phases: clean, 2 % loss, bursty loss, 1.5 Mbit/s
with 80 ms delay, a 5 s outage, recover. The teacher and the **clean** group are never touched, so
drift on the server shows in the clean group and what the network does shows in the impaired one.
Chromium shares its group's network namespace, so it is shaped with its group.

The SFU runs in a container held to 2 CPUs (cpuset) and 2 GiB, the clients on the other CPUs. Both servers
get the same settings through the `RTCD_*` variables they share: API address, self registration, ICE
host override, UDP and TCP port, log level.

## What is measured

Everything is JSONL, one object per line with `ts` (unix ms), `impl`, `arch` (`x86_64`, `aarch64`),
`kind` and `id`; the shared report tool (`vendor/rtc/benchmarks/soak/report.py`) reads it.

* `recv`, every 5 s per received track, counters since the start: `frames`, `freezes`, `freeze_ms`,
  `packets`, `lost`, `nack`, `pli`, `jitter_ms`, `rtt_ms`, `audio_gap_ms`, `stalled`.
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
* `event`: `ice_state`, `wrap`, `join`, `leave`, `error`. `phase` comes from `netem.sh`.

`rules.json` gates `rtcd-rs` and `rtcd-rs/chrome`: after 15 minutes of warmup, the first and the last
20-minute clean window must not differ by more than 2x in freeze rate (plus 0.5/min) or audio gap
(plus 0.5 %); the resident set must not grow more than 30 MB/h; no stall may last over 10 s and
every outage must be recovered from within 10 s.

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
`<outdir>/rtcd-<impl>-<arch>.jsonl`; the per-process files and the containers' logs stay beside it.
Short runs only prove the harness: the gates need a warmup of 15 minutes and two 20-minute windows.
On a Mac the architecture is aarch64; x64 is exercised by CI.

## In CI

`.github/workflows/lesson-soak.yml` runs `rs` and `go` on x64 (`ubuntu-latest`) and arm64
(`ubuntu-24.04-arm`) in parallel: weekly on Saturday, by hand (`minutes`, default 120) and on pull
requests that touch `crates/lms-sim`, this directory or the workflow (10 minutes, 5-minute cycle). A
`report` job runs `report.py` over all the files, writes `summary.md` to the run's summary and
uploads `report.html` and the JSONL; it fails when rtcd-rs broke a rule.

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
