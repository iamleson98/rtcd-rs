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

What the receivers experience, read from the connection statistics of each session that sends or receives video every five seconds (a voice-only session is not read: the snapshot allocates, and the voice thread pays for it):

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

Within an instance, threads called shards serve the calls, and a call lives on one of them. With
`RTCD_RTC_SHARDS=0` (the default) the number follows the load, from two shards (one where there
are fewer than two cores) up to every CPU the process may use, as long as the machine has most of a
core idle to give the next one (a shard is added only while the CPUs have 0.8 of a core spare, so
the kernel's network work, the API and the recorders keep theirs: the node grows into the room it
has and stops by itself where there is none; the pod needs no CPU limit to be safe). Two from the start because a class
that begins at the bell is placed call by call before its participants arrive: with one shard,
three lectures of 130 to 180 people landed on it together and ran it at 96 to 100 % while a
second sat idle, then refused people; on two shards the threads ran at 51 and 70 %. A new call that finds every shard holding 300 sessions' worth (each call counts for at
least 20, since a class fills over the seconds after it starts, and a bell starts many at once) gets
a shard of its own, which a burst of classes needs because they are all placed before their
participants have arrived; and a shard is added as well when every shard has been over half busy for three seconds. A shard above the two that are kept and that has had no
sessions for two minutes is stopped. A call stays on the shard it started on, and new calls go to
the shard carrying the least. Measured on four cores at 900 participants, one shard dropped a third
of the voice packets and the automatic two dropped about 5 % (the load generator shares the machine,
and more shards did not do better and cost a fifth more CPU); a shard that is not needed costs a
little (about 7 % more CPU and 9 MiB at 300 participants). A positive `RTCD_RTC_SHARDS` pins the
number. The shards
share the media port; the kernel hands a datagram to one of them, and a shard that gets one that is
not its own passes it on. Once a shard has learned a participant's address it opens a socket
connected to that address, bound to the same port, which the kernel serves directly: after the first
packets almost nothing takes the detour (`rtcd_net_forwarded_packets_total` shows how much does).

## Large calls: the loudest voices

Every open microphone is a stream to every listener, so a call of `n` people with `m` microphones
open costs `m × n` voice streams, while a person follows two or three speakers at most. With
`RTCD_RTC_AUDIOLOUDEST=N` (`rtc.audio_loudest`) a call that has more than `N` open microphones
forwards only the `N` loudest, the rule Meet (three loudest, on three fixed streams) and Jitsi
(`route-loudest-only`) use. The loudness is the audio level the browser already sends (RFC 6464),
smoothed so that a speaker is heard from the first syllables and a pause does not give the slot
away, and a source that holds a slot keeps it until another is clearly louder (`src/loudest.rs`).
A call with no more open microphones than `N` is untouched, a sender that reports no level is
always forwarded, and 0 forwards everything. The default is 3, because without a limit the cost
grows with the square of the class: on the OVH video VPS three lectures of 126 to 182 people with a
tenth of the students unmuted (13 open microphones each) made the node send 226 to 268 thousand
packets a second (`rtcd_net_packets_total`), 140 Mbit/s of it voice; the shard threads were at
100 % and everyone lost packets from about 180 participants on. Packets of a voice that is
not forwarded are dropped before any receiver is written to, so nothing is renegotiated and the
people's voice indicators (`voice_on`/`voice_off`) do not change.


### Voice packet time

Opus fits 20 ms in a packet by default, so a microphone is 50 packets a second, and an open
microphone is that to every listener. `RTCD_RTC_AUDIOPTIMEMS=40` (`rtc.audio_ptime_ms`) asks the
browsers to send 40 ms in a packet instead, by writing `a=ptime` and `a=maxptime` into every audio
section the node receives on, in each description it sends (a browser given a description without
one goes back to 20). That halves the voice packets of a call and the header bytes with them, at 20
ms more delay and 40 ms lost to a lost packet instead of 20 (Opus's in-band repair covers it).
Chrome 156, Firefox 158 and Safari 26.6 apply it to what they send (checked: 50.8 packets a second
without, 25.6 with 40 and 17.6 with 60 in Chromium; in a call through this node with 40, 25 a second
from each of the three, and each heard the others at 25 a second without loss); a browser that
ignores it stays at 20. Forwarded voice keeps the packet time it was sent with, and the voice
activity detector follows it. The value is 0 (the browser decides, the default) or a multiple of 20
up to 120.

## Large calls: a send budget

A node asked for more than its network carries does not slow down evenly: its send queues overflow
and every receiver loses packets at once (the production load test saw 50 to 75 % loss for
everyone past about 320 Mbit/s). With `RTCD_RTC_EGRESSBUDGETMBPS=N` (`rtc.egress_budget_mbps`) the
shards add what they send to one gauge, and when its smoothed rate reaches 85 % of `N` every
receiver's cameras and screen shares fall back to their small layer (no renegotiation, the same
switch a receiver's own request makes); they go back up only after the rate has stayed under 65 %
for twenty seconds, so the drop itself does not release it. 0 (the default) sets no budget, and the
node finds its own ceiling instead: the first time the kernel refuses datagrams (50 or more a
second, `SndbufErrors`) the rate it was sending becomes the ceiling, at 90 % of that rate, the
squeeze starts at once, and the ceiling is lifted by a tenth after every five quiet minutes, so a
node is held to what its network can carry now and not to its worst moment. A refusal at the
ceiling teaches a lower one; one while squeezed well below it does not. A budget that is set
still applies, whichever is lower. `GET /system` reports `egress_bps`, `egress_squeezed` and the
learned `egress_ceiling_bps` and `egress_ceiling_pps` (0 until the kernel has refused something),
for whatever places new calls (`src/egress.rs`).

The card has a second ceiling, in packets: `RTCD_RTC_EGRESSBUDGETPPS=N` (`rtc.egress_budget_pps`)
is the same rule for packets per second, and either budget squeezes. Voice packets are about 100
bytes, so a call with many open microphones reaches the packet ceiling at a fraction of the bit
rate: the OVH video VPS sent 160 thousand 140-byte packets a second without loss and 200 thousand
with 0.1 % loss (a plain UDP test), but under the lecture load above, with 120 to 157 thousand
packets a second leaving at 250 to 520 Mbit/s, its transmit ring stayed full and the kernel refused
280 to 570 thousand datagrams every five seconds. Set it to about 80 % of what the card was
measured to take.
While the node is squeezed, a call also forwards at most two voices (see above), so the load that
brought the node to its ceiling is cut where it is largest.

The kernel does not tell the sender: a datagram the transmit queue refuses is counted
(`UdpSndbufErrors`) and `sendmmsg` still succeeds, so the node counted those packets as sent.
`GET /system` therefore reports `kernel_send_drops` (that counter since the start) and `egress_pps`;
a node whose `kernel_send_drops` grows is sending more than its card takes.

## Large calls: a shard with no room

A call lives on one shard and cannot move, so a shard whose thread has no time left drops packets
for every call on it while another shard may be idle. A shard that has been at least 90 % busy for
two seconds is under pressure: its receivers are held to the small layers and its calls forward two
voices at most, the same fall back as the send budget, but local to the shard. It is released after
thirty seconds under 70 % (`src/runtime/scaler.rs`, `Pressure`).

Past that there is a cliff. Measured on the OVH video VPS, a shard thread at 97 % lost 0.2 % of its
packets and at 100 % about a third, because lost packets are asked for again and the thread has no
time left for them, so it does not recover by itself. A shard at 95 % busy for three seconds is
saturated: its calls forward the loudest voice only (a second speaker is heard from about half a
second after starting, the selector's hysteresis and release; this is the emergency setting, and
the price of keeping the other calls clean), and it refuses new sessions (a call that lives
on it cannot move, so its next participant is refused; a new call goes to a shard that is not
saturated, and is refused when none is), so the sessions it has keep their quality. It leaves that
state after ten seconds under 90 %. The call's host (the join request's `host` field, which the LMS
server sets for a class's teacher) and its recorder (`observer`) are never refused: a teacher whose
connection dropped comes back as a new session, a node with no room still lets the teacher open the
class, and a class that is being recorded keeps its recorder. Nor is a participant refused who has a
session in the call or whose last one ended in the last minute: a student's connection dropped, and
they come back as a new session, not as someone the shard was not already serving (a recorder costs about 3 % of a core
and 12 Mbit/s for a class of 30 with 24 cameras, measured with `lms-sim`).
`RTCD_RTC_REFUSEWHENSATURATED=false` turns the refusal off.
`GET /system` reports `shards_pressured` and `shards_saturated`.

## Upgrading from the previous release

With no setting changed, a node that takes this release behaves differently in four ways:

- It forwards the three loudest voices of a call, not all of them (`RTCD_RTC_AUDIOLOUDEST=0` forwards
  all, as before).
- It starts two shards where there are cores for two, not one (`RTCD_RTC_SHARDS` pins the number).
- It refuses a join to a shard that has been at 95 % busy for three seconds, except the call's host
  and recorder and a participant already in the call or just gone from it
  (`RTCD_RTC_REFUSEWHENSATURATED=false` turns that off).
- Under load (90 % busy for two seconds, or a send budget reached) it holds receivers to the small
  video layers and forwards two voices.

Off unless set: the send budgets (`RTCD_RTC_EGRESSBUDGETMBPS`, `RTCD_RTC_EGRESSBUDGETPPS`; without
them the node learns its ceiling from the kernel refusing datagrams) and the packet time (`RTCD_RTC_AUDIOPTIMEMS`). `GET /system` gains `shards_pressured`, `shards_saturated`,
`egress_pps` and `kernel_send_drops`. The recorder reads the length of each Opus packet from the
packet, so recordings are right whatever the packet time is.

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
crates/recorder     the LMS session recorder: joins a class call as a bot and writes it as WebM files
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
call listen-only and writes every voice, camera and screen share as WebM, with no re-encoding; the app
server logs the whiteboard). Same environment, endpoints and manifest; `deploy/recorder.Dockerfile`
builds a static image on `scratch`. It uses about a quarter of the Go recorder's CPU and a fifth of its
memory; see [crates/recorder/README.md](crates/recorder/README.md) for the numbers, the method
(`scripts/bench-recorder.py`) and what differs.

### Recording inside rtcd

With `[record] enabled = true` (`RTCD_RECORD_ENABLED`) a node records calls itself, without a recorder
participant: the server sends `record_start` over the websocket, each published stream is copied once
to a writer thread (`rtcd_sfu::record`), the files are the recorder crate's, rotated every two minutes
(bounding what a node shutting down still holds) and numbered from the start's `fileBase` in the order they close, and each is uploaded as it closes
through the endpoints the bot uses, then reported (`record_file`) and deleted. `record_ended` follows
once every file is stored. Settings: `RTCD_RECORD_DATADIR`, `_MAXBYTES` (one recording's total),
`_MAXPENDINGBYTES` (closed files waiting for upload, all recordings; above it packets are left out and
counted, not the recording ended) and `_UPLOADMBPS` (the node's upload rate, so uploads never compete
with media). Files a dead process left in the data directory are removed at start. Metrics:
`rtcd_record_{recordings,files_written_total,pending_bytes,uploaded_bytes_total,upload_failures_total,
writer_dropped_packets_total,writer_busy_seconds_total}`. `lms-sim run --builtin` records a synthetic
class this way; the writer threads used 0.14 % of a core for 8 people and 7 video streams, and 0.41 %
for 100 people and 32 (release build, loopback).

## Fuzzing

`fuzz/` holds libFuzzer targets for what comes from outside: data channel frames, websocket frames, the
engine given a session and any SDP, ICE and announcements a client can send, VP8 payloads, the recorder's
stream writer and its signal parser. `cd fuzz && cargo +nightly fuzz run <target>`; the `rtc` crates have
targets of their own for STUN, RTP, RTCP, SDP, DTLS, SCTP and SRTP.

## Not done yet

- AV1: the LMS server never offers it (`av1Support` is false on every join), so nothing would use it.
- Splitting one call across instances (calls stay on the instance they start on). One instance carries
  300 participants at about half a core.
