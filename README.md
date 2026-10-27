# rtcd-rust

An SFU for calls, written in Rust: a replacement for the Go `rtcd` (the Mattermost Calls SFU, as the
LMS runs it). It is built on the sans-I/O [`rtc`](https://github.com/iamleson98/rtc) crate, speaks the
same wire protocol, reads the same configuration, and can be swapped in for the Go service without
changing the LMS server or the web client.

## Compatibility with the Go rtcd

| | |
|---|---|
| Control plane | the same HTTP API (`/version`, `/register`, `/login`, `/unregister`, `/system`, `/calls/...`, `/metrics`) and the same msgpack websocket messages. Checked against golden vectors produced by the Go code (`crates/rtcd-proto/tests/golden`) and by the original Go service client |
| Configuration | the same `config.toml` keys and the same `RTCD_*` environment variables, including `:8045`-style addresses, `ice_host_override` NAT maps, STUN discovery, TURN credential generation, TLS |
| Container | the `Dockerfile` uses the LMS image's entrypoint and config template, so environment, ports (8045, 8443), user (2001) and the `/data` volume are unchanged |
| Data | a data directory the Go rtcd used is opened as is: its registered clients are taken over on the first start (`crates/rtcd/src/legacy.rs`) |
| Extension | a data channel frame of type 10 (`SharpScreen(true)`) asks for a shared screen at its large layer whatever the bandwidth estimate says, until the receiver's link is starved. The session recorder sends it; the Go service never sees it from a browser |
| Media | ICE-lite over UDP and ICE-TCP, DTLS-SRTP, the `calls-dc` data channel (lock protocol, zlib SDP, media map), Opus voice with VAD events, VP8 camera and screen share with simulcast layer switching that needs no renegotiation, `video_subscribe`, keyframe relay, bandwidth-driven screen layer choice |
| Metrics | the Prometheus metric names are the Go service's, plus the network counters below |

The LMS web app's own call client runs against it in headless Chrome, Firefox and Safari, next to the
Go service, with the same checks. That comparison lives in the
[webrtc](https://github.com/iamleson98/webrtc) repository (`compare/`), which includes this one as a
submodule.

## Measuring network usage

The metrics are Prometheus counters, meant for a time-series database that measures traffic and
predicts the network bill (`increase(...[1d])` is GB per day):

| metric | what it counts |
|---|---|
| `rtcd_net_bytes_total{direction,transport}` | everything that crossed the media sockets (RTP, RTCP, STUN, DTLS), in and out, UDP and TCP. IP and UDP headers add 28 bytes per IPv4 datagram on top |
| `rtcd_net_packets_total{direction,transport}` | the same in packets, for the header overhead |
| `rtcd_net_dropped_packets_total` | datagrams the kernel refused to send (a full send buffer) |
| `rtcd_net_forwarded_packets_total` | datagrams one shard read and passed to the shard that owns the session |
| `rtcd_rtc_rtp_bytes_sent_total{type}` | RTP bytes sent to participants by `voice`, `video`, `screen`, `screen-audio` (the Go service's metric) |
| `rtcd_rtc_rtp_bytes_received_total{type}` | the uplink, by the same types |
| `rtcd_rtc_rtp_tracks_writes_time{groupID,type}` | time to write one packet to one receiver (a sample of the writes) |

What the receivers experience, read from each receiver's connection statistics every five seconds:

| metric | what it counts |
|---|---|
| `rtcd_rtc_receiver_feedback_total{type}` | retransmission (`nack`), keyframe (`pli`) and full intra (`fir`) requests receivers sent |
| `rtcd_rtc_retransmitted_packets_total`, `_bytes_total` | what was sent again over RTX because they asked (receivers that negotiate RTX) |
| `rtcd_rtc_receiver_loss_ratio`, `rtcd_rtc_receiver_rtt` | histograms of the loss fraction and round trip time receivers report |
| `rtcd_rtc_bandwidth_estimate_bps` | histogram of the receivers' bandwidth estimates |
| `rtcd_rtc_screen_layer_seconds_total{layer}` | seconds receivers spent on the small and the large layer of a shared screen (`rate()` is the number of receivers on each) |
| `rtcd_rtc_keyframe_requests_total{result}` | keyframes asked of senders, `sent` or `held_back` (asked a moment ago) |

What is spent on the wire: only video takes part in the bandwidth estimate, so voice packets carry no
transport-wide sequence number (8 bytes on a 65-byte packet, both ways) and nobody reports on them.

## Running several instances

`GET /system` returns `cpu_load` (the Go service's key) and, in addition, `sessions` and `shards`, so
whatever places new calls (the LMS server does, from its DNS-discovered pool) can send them to the
lighter instance. Calls stay on the instance they start on.

Within an instance, `RTCD_RTC_SHARDS` threads serve the calls (0 chooses one per four cores). The shards
share the media port; the kernel hands a datagram to one of them, and a shard that gets one that is
not its own passes it on. Once a shard has learned a participant's address it opens a socket
connected to that address, bound to the same port, which the kernel serves directly: after the first
packets almost nothing takes the detour (`rtcd_net_forwarded_packets_total` shows how much does).

## Using it from class-management

- Bring it in as a submodule: `git submodule add https://github.com/iamleson98/rtcd-rs.git rtcd-rust`
  and `git submodule update --init --recursive` (the `rtc` submodule comes with it).
- The image takes the environment, ports (8045, 8443), user (2001) and `/data` volume of `lms-rtcd`,
  and the entrypoint and config template are the ones that image uses: the Kubernetes manifests change
  only in the image name. Clients registered with the Go rtcd in `/data` are taken over on the first
  start.
- Its instances are found by the server's DNS discovery like the Go ones, and `GET /system` lets the
  server put new calls on the lighter instance.
- The web client and the server's `custom_calls_*` actions need no change (tested in Chrome, Firefox and Safari). The session recorder is a crate of this workspace too (below).

## Layout

```
crates/rtcd-proto   wire formats (msgpack messages, data channel framing, session config, TURN)
crates/rtcd-net     batched UDP (recvmmsg/sendmmsg/GSO on Linux), RFC 4571 framing, interfaces, STUN
crates/rtcd-sfu     the engine (one per shard thread, no locks) and the runtime around real sockets
crates/rtcd         the service: config, auth, store, HTTP and websocket API, TLS, metrics, logging
crates/rtcd-testkit an emulated browser used by the engine tests
crates/recorder     the LMS session recorder: joins a class call as a bot and writes it as WebM files and a board log
crates/lms-sim      a stand-in LMS around a real rtcd with synthetic participants, to test and measure the recorder
vendor/rtc          the WebRTC stack, pinned (git submodule)
compat/golden       regenerates the golden vectors from the Go sources
deploy/             the entrypoint and config template the image uses; recorder.Dockerfile
```

## Build, test, run

```bash
git submodule update --init --recursive
cargo build --release -p rtcd
cargo test --workspace
./target/release/rtcd -config config.toml      # the same arguments as the Go binary
```

```bash
docker build -t rtcd-rust .                     # the Rust version is a build argument (RUST_VERSION)
docker run --rm -p 8045:8045 -p 8443:8443/udp -p 8443:8443 \
  -e RTCD_ICE_HOST_OVERRIDE=<public ip> rtcd-rust
```

### What the engine costs per packet

```bash
cargo test --release -p rtcd-sfu --test engine_cost -- --ignored --nocapture
```

A call of 32 participants, three of them talking, run in virtual time: it prints the nanoseconds the
engine spends per delivery, the best of five rounds. That number moves by a few percent from one run to
the next, where a CPU percentage under load moves by tens, so it is what to compare before and after a
change to the engine or to `vendor/rtc`. `ENGINE_COST_FRAMES` and `ENGINE_COST_ROUNDS` lengthen it for
profiling.

## Continuous integration

`ci.yml` runs rustfmt, clippy (warnings are errors), the whole test suite on Linux and macOS, a
dependency audit, and the Docker image started through its entrypoint. `image.yml` publishes
`ghcr.io/<owner>/rtcd-rust` on a version tag. The Go-versus-Rust comparison runs in the webrtc
repository.

## The session recorder

`crates/recorder` replaces the Go recorder of the LMS (one short-lived job per recording: it joins the
call listen-only and writes every voice, camera and screen share as WebM, with no re-encoding, and the
whiteboard as a log). Same environment, endpoints, manifest and log shape; `deploy/recorder.Dockerfile`
builds a static image on `scratch`. It uses about a quarter of the Go recorder's CPU and a fifth of its
memory; see [crates/recorder/README.md](crates/recorder/README.md) for the numbers, the method
(`scripts/bench-recorder.py`) and what differs.

## Fuzzing

`fuzz/` holds libFuzzer targets for what comes from outside: data channel frames, websocket frames, the
engine given a session and any SDP, ICE and announcements a client can send, VP8 payloads, the recorder's
stream writer and its signal parser. `cd fuzz && cargo +nightly fuzz run <target>`; the `rtc` crates have
targets of their own for STUN, RTP, RTCP, SDP, DTLS, SCTP and SRTP.

## Not done yet

- AV1: the LMS server never offers it (`av1Support` is false on every join), so nothing would use it.
- Splitting one call across instances (calls stay on the instance they start on). One instance carries
  300 participants at about half a core.
