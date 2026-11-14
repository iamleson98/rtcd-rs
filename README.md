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
| Extension | a data channel frame of type 10 (`SharpScreen(true)`) asks for a shared screen at its large layer whatever the bandwidth estimate says, until the receiver's link is starved. Only the standalone session recorder, since removed, sent it; the Go service never sees it from a browser |
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
| `rtcd_rtc_retransmitted_packets_total`, `_bytes_total` | what was sent again over a separate RTX stream because they asked. rtcd offers no RTX: it answers a `nack` by sending the packet again on its own stream, which the connection statistics cannot tell from a first send, so these stay 0; the `nack` count above shows how often receivers ask |
| `rtcd_rtc_receiver_loss_ratio`, `rtcd_rtc_receiver_rtt` | histograms of the loss fraction receivers report, and of the round trip from a packet leaving to the transport feedback that reports it (the library leaves the receiver reports' round trip at zero) |
| `rtcd_rtc_bandwidth_estimate_bps` | histogram of the receivers' bandwidth estimates |
| `rtcd_rtc_screen_layer_seconds_total{layer}` | seconds receivers spent on the small and the large layer of a shared screen (`rate()` is the number of receivers on each) |
| `rtcd_rtc_keyframe_requests_total{result}` | keyframes asked of senders, `sent` or `held_back` (asked a moment ago) |

The node itself, the same figures as `GET /system` (read when the metrics are, gauges unless it says
counter; the host figures average the last five seconds):

| metric | what it is |
|---|---|
| `rtcd_sessions`, `rtcd_shards`, `rtcd_shards_pressured`, `rtcd_shards_saturated` | sessions, the threads serving them, and how many of those shed load or refuse new sessions |
| `rtcd_egress_bps`, `rtcd_egress_pps` | what the shards hand to their sockets, link-layer headers included (the card's own counter is `rtcd_host_tx_bps`) |
| `rtcd_egress_ceiling_bps`, `rtcd_egress_ceiling_pps` | what the node takes its card to carry, in bits and packets a second: learned by sending, or said (0 when nothing is estimated); `rtcd_egress_capacity_bps` is the one in bits |
| `rtcd_egress_squeezed`, `rtcd_egress_refusing`, `rtcd_egress_choked` | 1 while video is held to the small layers; while the kernel refuses datagrams (recording uploads slow down); while it keeps refusing after the squeeze (one voice per call) |
| `rtcd_kernel_send_drops_total` (counter) | datagrams the kernel refused to queue (UDP `SndbufErrors`), from when the process first read the count |
| `rtcd_host_cpu_load`, `rtcd_host_cpu_busiest` | busy share of all CPUs and of the busiest one (waiting for I/O counts as idle) |
| `rtcd_host_cpu_iowait`, `rtcd_host_cpu_softirq` | share of all CPU time waiting for I/O, and in the kernel's deferred work (sending and receiving packets) |
| `rtcd_host_mem_used`, `rtcd_host_mem_total_bytes`, `rtcd_host_mem_available_bytes` | memory in use as a share (what the kernel could not hand out without swapping or killing), and the figures it is made of |
| `rtcd_rss_limit_bytes` | what the process's control group may hold (0: none); its use is `rtcd_process_resident_memory_bytes` |
| `rtcd_host_tx_bps`, `rtcd_host_tx_pps`, `rtcd_host_tx_dropped_pps` | what the default-route network interface sends and its driver drops, everything on the machine counted (media, recording uploads, the rest) |

What is spent on the wire: only video takes part in the bandwidth estimate, so voice packets carry no
transport-wide sequence number (8 bytes on a 65-byte packet, both ways) and nobody reports on them.

## What of a shared screen a viewer gets

A browser sends a shared screen as two simulcast layers (500 kbit/s at 5 frames a second and 2.5 Mbit/s at
15, from lms-web), each in three temporal layers (`L1T3`, what Chrome does unasked): the base layer has every
fourth frame and about two fifths of the bits, the second the frames halfway between and a fifth more. A
viewer is sent one rung of that ladder (`layers.rs`): the small layer's base, its base and second, all of it,
then the same of the large layer, so a path that carries 1 to 2 Mbit/s gets the full picture at a quarter or
half the frame rate instead of the small picture. Leaving temporal layers out costs nothing and needs no
keyframe (`temporal.rs`; the frames after them are numbered on without a gap, `rewrite.rs`); moving between
the small and the large layer costs a keyframe of the other.

What decides is what the viewer's transport feedback says of its path (`bwe.rs`):

- the queue that stands on it: the one-way delay of the first packet of each burst above the lowest of the
  last half minute. A path given more than it carries holds the excess there (a home router or a mobile
  network buffers seconds of it), and the voice on the same link waits behind it; the delay-based bandwidth
  estimate reads only a queue that grows, and a full one does not;
- what it delivers (the bytes that arrived over the last second), what it loses (of the packets sent the
  first time: a packet sent again finds its first copy there and is dropped unreported), and what its
  narrowest link carries (a burst leaves at once and arrives spread out by it).

A queue of 150 ms that lasts a second without shrinking (a keyframe's queue shrinks once it has gone in), one
that grows fast, one of 1.5 s, or a sixth of what is sent lost, is congestion, pinned viewers included: the
viewer is moved at once to the rung that what the path delivered carries with room to drain the queue within
seconds. While the path stays clear the rung above is tried, one at a time, once it fits the narrowest link
with room to spare (with the keyframe a move onto the large layer costs drained within 2.5 s); a rung that
brought congestion waits 20 s, then twice as long each time (up to 5 min) before it is tried again. A path
that loses more than a tenth of the packets at random is given nothing more. Lost packets are sent again on
request, and lms-web plays a shared screen a second late, so the repair lands before the frame is due: the
picture neither stops nor breaks (`tests/screen_congestion.rs` runs viewers behind 1 to 4 Mbit/s links that
buffer 2 s, 230 ms away, losing 1 to 2 % at random). A screen share and its sound go to viewers in a stream
of their own: a browser keeps one video of a stream in step with its audio, which is to be the camera with the
voice, not the share played a second late.

## Running several instances

`GET /system` returns `cpu_load` (the Go service's key, the busy share of all CPUs over the last
five seconds) and, in addition, `sessions` and `shards`, so whatever places new calls (the LMS server
does, from its DNS-discovered pool) can send them to the lighter instance. Calls stay on the instance
they start on. For admitting more work (recordings) it also has `cpu_busiest` (the busiest single
CPU), `cpu_iowait`, `cpu_softirq`, and memory: `mem_used` (the share of the machine's RAM in use, from
`MemAvailable`), `mem_total_bytes`, `mem_available_bytes`, `rss_bytes` (this process) and
`rss_limit_bytes` (its control group's limit, 0 for none). `host_tx_bps`, `host_tx_pps` and
`host_tx_dropped_pps` are the network interface's own counters. Every key is also a metric (see
above).

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

## A receiver's path that carries less than its cameras

A receiver whose downlink carries less than the cameras it is sent loses the excess at random, and
every picture breaks. With `RTCD_RTC_CAMERABUDGET=true` (`rtc.camera_budget`, the default) the SFU
holds that receiver's cameras to what its path was measured to deliver (less its screen share): the
host's camera first, then the others in turn, each whole, or without its upper temporal layers when
the browser sends them, or paused (restarted at a keyframe) until there is room again. Cameras
that fit are untouched; `false` sends every camera whole, whatever the path drops (`src/camera_budget.rs`).

## Large calls: a send budget

A node asked for more than its network carries does not slow down evenly: its send queues overflow
and every receiver loses packets at once (the production load test saw 50 to 75 % loss for
everyone past about 320 Mbit/s). With `RTCD_RTC_EGRESSBUDGETMBPS=N` (`rtc.egress_budget_mbps`) the
shards add what they send to one gauge, and when its smoothed rate reaches 85 % of `N` every
receiver's cameras and screen shares fall back to their small layer (no renegotiation, the same
switch a receiver's own request makes); they go back up only after the rate has stayed under 65 %
for twenty seconds, so the drop itself does not release it. 0 (the default) sets no budget, and
nothing else squeezes video: `GET /system` reports `egress_bps`, `egress_squeezed` and the kernel's
refusals either way, for whatever places new calls (`src/egress.rs`).

### Finding the card's ceilings: `RTCD_RTC_EGRESSAUTO` (default on)

Nobody can say in advance what a virtual card carries. The OVH 4-vCPU video node took 800 to 850
Mbit/s of a "1 Gbit/s" card and 117 thousand packets a second, and not both at once with small packets;
the card's reported speed is wrong or missing on virtual NICs; a number written in a setting leaves a
third of the card unused (70 % of 850) or none of it protected (unset, as production was). So the
node finds it by sending, the way a congestion controller does (`src/capacity.rs`): one ceiling for
bits and one for packets, each on its own, decided on the *sustained* rate (the median of the last
seven seconds, so a keyframe fanned out to a class does not close the node, and a class joining is
followed three seconds late).

1. **Start.** At `RTCD_RTC_EGRESSLEARNMINMBPS` (default 400) and `RTCD_RTC_EGRESSLEARNMINPPS` (default
   80000), about half of what the OVH video card was measured to take. This is also the least the
   node ever concludes: refusals while it sends under it are taken for the machine's (the kernel's
   count is the whole machine's, and rtcd runs with `hostNetwork`), not for the card's limit. A link
   that reports a speed under 1 Gbit/s (`/sys/class/net/<interface>/speed`) starts at 40 % of it, and
   no ceiling in bits passes 97 % of the link; a link reporting more, or nothing, changes nothing
   (virtual cards say 10000 for a hypervisor that gives them 1000, and a link that says less than it
   gives is why `RTCD_RTC_EGRESSCAPACITYMBPS` exists).
2. **Rise.** A ceiling is *pressed* when the sustained rate is within 85 % of it. After ten seconds in
   a row pressed, without two refusing windows in a row (one is a burst), it rises by 10 %, then 20 %,
   then 25 % for every rise that held, never to more than a quarter above the rate, and not while the
   node holds its people down (the rate is then its own choice). A ceiling nothing presses on does not
   move. The node climbs only as fast as its people ask for more and the kernel accepts it; from 400
   to 850 Mbit/s takes about a minute of pressing.
3. **Wall.** Three refusing windows in a row (a window refuses when the kernel refused at least 50
   datagrams a second and 0.5 % of those sent; a stall of the machine or the card is over in a second
   or two) are a wall: the limit that was being met takes the highest rate the kernel accepted whole
   just before (the sustained rate of the last window that refused nothing and was not squeezed). Not
   the rate that failed, which is a burst, and not the rate less the kernel's share of refusals,
   which reads low (the kernel counts a call, not a datagram, and the node sends 64 at a time). A
   node asked for more in one step takes the rate from before the step, which it could carry, and
   finds the rest by probing. A wall already known is lowered by a tenth at most per run. Which limit
   was met is read from the rates: the one near its ceiling; the kernel does not say.
4. **Probing the wall.** A rise made within two minutes before a wall failed: the next one waits
   twice as long (20 s, 40 s ... up to ten minutes) and is 5 %. A rise that held (the next came with
   no refusal in between) puts the wait and the step back. A node sitting at its wall tests it ever
   less often (one refusing spell of a few seconds every ten minutes at most); one whose card got
   faster, or whose wall was a stall's, finds out.
5. **Said.** `RTCD_RTC_EGRESSCAPACITYMBPS` and `RTCD_RTC_EGRESSCAPACITYPPS` replace what the node
   would learn for their limit, and nothing moves them. With `RTCD_RTC_EGRESSAUTO=false` only what is
   said is known and a limit not said has no ceiling.

The ceilings are in `GET /system` (`egress_capacity_bps`, `egress_ceiling_pps`, and
`egress_capacity_source`: `probing` before the card has refused anything, `learned` after, `fixed` for
the operator's) and in the metrics above. They are in memory: a restarted node starts again at the
start (the first class of the day climbs for a minute or two, and nothing stale is kept in a file).

What the node does near them is the admission's (below): it takes no newcomer from 94 % of the nearer
ceiling (`RTCD_RTC_REFUSEEGRESSPERCENT`, released at 89 %), and, once a ceiling is a wall and not a
probe and the kernel is refusing, sheds quality from 110 % of it (`RTCD_RTC_SHEDEGRESSPERCENT`),
held at least twenty seconds.

Failure modes, in the order they are likely:

- **A wall that is too low** (a stall of three seconds that fell in a rise, a step in demand, a
  neighbour on the card): the node refuses newcomers earlier than it needs to until a rise finds
  the rest, a minute or two. It never sheds against it unless the kernel is refusing as well.
- **A wall that is too high**: the node sends past what the card takes until the kernel refuses
  three seconds in a row, then it is lowered to what was carried whole before (by a tenth at most per
  run). Joins stop at 94 % of it, and a call that grows past that loses what the kernel refuses.
- **The kernel's count is the machine's**, so a process on the node that sends UDP and overflows its
  own queue is read as the card's refusals. Refusals while the node sends under its start are never
  taken for a wall, and the kernel's count is only the trigger: the rate learned is the node's own.
- **A card slower than the start the link does not name** (a 200 Mbit/s VPS whose NIC reports 10000)
  is never learned, and the node goes by memory and the kernel's refusals alone, as before. Set
  `RTCD_RTC_EGRESSLEARNMINMBPS` and `_PPS` under such a card, or say it.
- **A restarted node** has forgotten and climbs again from the start.

### Acting on the kernel's refusals: `RTCD_RTC_EGRESSADAPT` (default off)

The node counts the datagrams the kernel refuses to queue and reports them (`kernel_send_drops`,
`rtcd_kernel_send_drops_total`, `egress_refusing`) whatever its settings. With
`RTCD_RTC_EGRESSADAPT=true` (`rtc.egress_adapt`) it also acts on them, in steps. The refusals are the
whole machine's signal (rtcd runs with `hostNetwork`, so any UDP sender on the node adds to the
count, which also counts calls and not datagrams), so these are guesses until a run on the real card
has shown them right, and they are off until then; off, only a send budget and the admission's hold
squeeze, no voice is cut and uploads go at their full rate, as in the release before. (The ceilings
above are learned from the same refusals but act through the admission only, whatever this setting
says.)

1. Two refusing windows in a row slow recording uploads to a quarter of their rate for as long as the
   refusals last and ten seconds after (`egress_refusing`), unless the files waiting are half of
   `max_pending_bytes` (checked every second). Uploads share the card and can wait.
2. Three in a row squeeze video to the small layers and calls to two voices for as long as they last
   and ten seconds after (`egress_squeezed`). A stall of the machine or the card lasts seconds and
   teaches nothing. A squeeze that comes back within two minutes is held twice as long (up to five
   minutes), so a demand just over the limit does not switch layers and voices every few seconds.
3. A squeeze the node decided on (a budget, or the admission's hold near the ceiling) that the kernel
   refuses through for five windows in a row *chokes* the node (`egress_choked`): calls forward their
   loudest voice only, until the kernel has accepted everything for ten seconds. A choke that comes
   back within a minute begins at the first refusing window and is held twice as long (up to two
   minutes). A clean window between the refusals starts the count again, so an overload that stutters
   never chokes; and a stall that falls in a squeeze and lasts five windows costs one voice per call
   for ten seconds.

A wall is taught by what the card *took*, so a demand at twice the card's limit learns the limit, not
twice it. The gauge cannot tell a stall from an overload except by how long it lasts and how much the
card took: a stall that took everything teaches nothing, one that lasts three windows and took at
least the start is taken for a wall (and probed again in twenty seconds), so the longest real stall has
to be measured on the card.

The service also brings these figures up to date every second whether or not any session is left,
and a window in which nothing was sent clears the squeeze and the choke, so an idle node reports no
squeeze and no rate. The rate is what the shards hand to their sockets, link-layer headers included:
receiver reports, retransmissions, ICE and DTLS are in it with the media, as the card carries them.
That makes `egress_bps` read higher than before, by up to half on voice-heavy traffic (every
datagram now adds 52 bytes of headers and encryption to a payload of about 100): during a rollout,
with old and new instances side by side, the two are on different scales, and a server that compares
them (its placement margin) compares unlike numbers.

### Changes since 0357b0d (for whoever takes this release)

- `GET /system` `cpu_load` is the busy share over the last five seconds (it was the last second);
  anything that admits work on it (the Go server's recording admission) sees a steadier figure.
- `kernel_send_drops` counts from when rtcd first read the kernel (it read the machine's count since
  boot), IPv4 and IPv6 together, and never goes back; `rtcd_kernel_send_drops_total` is its counter,
  and the rate of that is what a dashboard wants.
- `egress_squeezed` and the learned ceiling were set by one window of 50 refusals even though nothing
  acted on them, and stayed set after the last session left (a server that skips squeezed nodes then
  skipped the node for good). They are set by refusals only with `RTCD_RTC_EGRESSADAPT`; otherwise
  only a send budget sets `egress_squeezed`, and an empty node clears it. `egress_adapt` and
  `rtcd_egress_adapt` say which it is.
- `egress_bps` and `egress_pps` count every datagram handed to a socket, headers up to the link layer
  included (receiver reports, retransmissions, ICE and DTLS too): they read 30 to 50 % higher than
  before for the same traffic, so a configured send budget trips earlier than it did. Where a budget
  is set, raise it by that much, and compare `rtcd_egress_bps` with `rtcd_host_tx_bps`.
- New fields and metrics: see the table above (`cpu_busiest`, `mem_*`, `host_tx_*`, `egress_*`).

The card has a second ceiling, in packets: `RTCD_RTC_EGRESSBUDGETPPS=N` (`rtc.egress_budget_pps`)
is the same rule for packets per second, and either budget squeezes. Voice packets are about 100
bytes, so a call with many open microphones reaches the packet ceiling at a fraction of the bit
rate: the OVH video VPS sent 160 thousand 140-byte packets a second without loss and 200 thousand
with 0.1 % loss (a plain UDP test), but under the lecture load above, with 120 to 157 thousand
packets a second leaving at 250 to 520 Mbit/s, its transmit ring stayed full and the kernel refused
280 to 570 thousand datagrams every five seconds. Set it to about 80 % of what the card was
measured to take.
While the node is squeezed, a call also forwards at most two voices, so the load that brought the
node to its ceiling is cut where it is largest.

The kernel does not tell the sender: a datagram the transmit queue refuses is counted
(`UdpSndbufErrors`) and `sendmmsg` still succeeds, so the node counted those packets as sent.
`GET /system` therefore reports `kernel_send_drops` (that counter since the process first read it) and
`egress_pps`; a node whose `kernel_send_drops` grows is sending more than its card takes. The count
is IPv4 and IPv6 together and is the network namespace's: rtcd runs with `hostNetwork`, so it counts
every process on the machine that sends UDP, and the node's reaction (small video, fewer voices,
slower uploads) answers a refusal that may not be its own. It counts calls, not datagrams: a
segmented send of up to 64 datagrams that fails adds one, so the rate is read low, the safe way.

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

## Large calls: a node near its limits

A node that is asked for more than it carries must refuse and shed before it runs away. The kernel
does not push back on UDP (a full qdisc is success for the sender, counted in `SndbufErrors`), so
nothing in a shard's send path blocks. Two things grow instead, measured in `scripts/overload-lab`
(a node with a token bucket on its interface, three CPUs and a 3 GiB limit, loaded with classes):

1. **What a shard that has fallen behind keeps.** Its turn read up to 1,024 datagrams from each of
   hundreds of sockets and queued a datagram for every receiver of every video packet before it sent
   one; the other shards passed it more through a channel that held 65,536 batches. One turn queued
   875,000 datagrams and took 8.8 s, resident memory went from 0.1 to 2.8 GB of 3 GiB, and the lag grew
   with the memory. (On the OVH node, 0.5 to 2.26 GB, then the kill.)
2. **What receivers ask for.** Under loss every receiver asks again for the same packets, and the node
   answered every time: 36 receivers made it offer 1.7 Gbit/s, 20 times what they had asked it to
   carry, the kernel refused 27 million datagrams, and the refusals were the loss. (The production
   runaway offered 6.6 Gbit/s from a node that carried 820 cleanly.) With the NACK history cut to 32
   packets the same load offered 0.2 Gbit/s.

Bounds (always on):

- Datagrams one shard passes on to another (the kernel hands a packet to the shard its port hash
  picks, not the one that owns the session) are limited to 4,096 waiting per receiving shard, about
  100 ms of a busy one. Past that only handshakes (STUN, DTLS) go; past twice that, nothing.
- A turn of the loop stops reading at 20 ms (and at 8,192 datagrams); the sockets that still have
  datagrams are read in the next turn, after the timers.
- The engine's queued datagrams are handed to the sockets at 2,048, not at the end of the turn.
- A retransmission request is answered whole or not at all, up to a tenth of the packets the node
  sends (never below 500 a second, so a lossy receiver on a quiet node is answered in full) and to a
  trickle while the kernel refuses its datagrams (`Gauge::allow_retransmits`,
  `src/nack_limit.rs`). `rtcd_rtc_retransmits_limited_total` counts what was left unanswered.

Limits (`src/runtime/guard.rs`; a thread of its own reads the memory every 250 ms, not the async
runtime a saturated node starves):

- From `RTCD_RTC_REFUSEMEMORYPERCENT` (default 70) of the memory the process may hold (its control
  group's limit) a newcomer to a call is refused as at a saturated shard (a host, a recorder and a
  participant in the call or just gone are not) and the node sheds: small video layers, fewer voices.
- From `RTCD_RTC_REFUSEHARDMEMORYPERCENT` (85) nobody is taken, a host included, and calls forward
  their loudest voice only. Each limit is released ten points lower; 0 turns it off.
- While the kernel refuses the node's datagrams (`egress_refusing`) newcomers are refused
  (`RTCD_RTC_REFUSEWHENKERNELREFUSES`, default true).
- Near the card's ceilings (the learned ones, or said by `RTCD_RTC_EGRESSCAPACITYMBPS` and `_PPS`; see
  "Finding the card's ceilings") the node takes no newcomer once it sends `RTCD_RTC_REFUSEEGRESSPERCENT`
  (94) of the nearer one, released at 89, so that a node uses all but a few percent of what its card
  carries and stops before the kernel refuses. A call cannot move and keeps growing as people rejoin
  or arrive late, so the few percent left are what those calls grow into. It sheds (small video
  layers, fewer voices) from `RTCD_RTC_SHEDEGRESSPERCENT` (110) of a ceiling that is a fact, a wall
  the card showed or a figure said, never against a probe, and only while the kernel refuses: past a
  wall everyone loses packets, a demand a few percent over loses a few percent which receivers
  recover, and cutting every receiver's video for that costs more. A shed is held for twenty seconds at least (twice as long
  each time it returns within two minutes, up to five minutes) and released when the rate is 15
  points under its mark; the node stays closed to newcomers while it sheds and for thirty seconds
  after, because the rate it reports is low only because of the shed.
- `GET /system` reports `admission_closed` (`memory`, `memory_hard`, `egress`, `refusals`; empty
  when open), `egress_capacity_bps` (the ceiling in bits: said, learned or still probing, with
  `egress_capacity_source`) and `egress_headroom_bps` (the refusal mark less what it sends), which
  the server reads to place calls (a call goes only where the headroom covers what the class is
  expected to send). A node that has not yet seen its wall reports the start it probes from, so
  the server prefers a node that has shown it carries more. Metrics: `rtcd_admission_closed{reason}`,
  `rtcd_egress_capacity_bps`, `rtcd_egress_headroom_bps`, `rtcd_rtc_sessions_refused_total{reason}`
  (`shard` too), `rtcd_forward_queued_packets`, `rtcd_forward_dropped_packets_total`,
  `rtcd_shard_turn_peak_reads`, `rtcd_shard_turn_peak_transmits`, `rtcd_shard_turn_longest_seconds`.

A refused session is closed, as at a saturated shard; the web app treats that as a lost connection
and tries again with growing pauses (`lms-web/src/lib/features/calls/rejoin.ts`).

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

On by default since the release that finds the card's ceilings: `RTCD_RTC_EGRESSAUTO` (the node learns
what its card carries and takes no newcomer within 6 % of it; set `RTCD_RTC_EGRESSAUTO=false` for a
node that goes by memory and the kernel's refusals alone). `RTCD_RTC_REFUSEEGRESSPERCENT` and
`RTCD_RTC_SHEDEGRESSPERCENT` now apply to those ceilings (94 and 110, were 70 and 90 of a figure
that had to be written down); a figure written in `RTCD_RTC_EGRESSCAPACITYMBPS` is what the card was
measured to take, not a margin under it.

Off unless set: the send budgets (`RTCD_RTC_EGRESSBUDGETMBPS`, `RTCD_RTC_EGRESSBUDGETPPS`), acting
on the kernel's refusals (`RTCD_RTC_EGRESSADAPT`) and the packet time (`RTCD_RTC_AUDIOPTIMEMS`). `GET /system` gains `shards_pressured`, `shards_saturated`,
`egress_pps` and `kernel_send_drops`. A recording reads the length of each Opus packet from the
packet, so it is right whatever the packet time is.

## Using it from class-management

- Bring it in as a submodule: `git submodule add https://github.com/iamleson98/rtcd-rs.git rtcd-rust`
  and `git submodule update --init --recursive` (the `rtc` submodule comes with it).
- The image takes the environment, ports (8045, 8443), user (2001) and `/data` volume of `lms-rtcd`,
  and the entrypoint and config template are the ones that image uses: the Kubernetes manifests change
  only in the image name. Clients registered with the Go rtcd in `/data` are taken over on the first
  start.
- Its instances are found by the server's DNS discovery like the Go ones, and `GET /system` lets the
  server put new calls on the lighter instance.
- The web client and the server's `custom_calls_*` actions need no change (tested in Chrome, Firefox and Safari). rtcd records calls itself (below).

## Layout

```
crates/rtcd-proto   wire formats (msgpack messages, data channel framing, session config, TURN)
crates/rtcd-net     batched UDP (recvmmsg/sendmmsg/GSO on Linux), RFC 4571 framing, interfaces, STUN
crates/rtcd-sfu     the engine (one per shard thread, no locks) and the runtime around real sockets
crates/rtcd         the service: config, auth, store, HTTP and websocket API, TLS, metrics, logging
crates/rtcd-testkit an emulated browser used by the engine tests
crates/recorder     the files of a session recording (WebM from a call's streams) and their upload; rtcd records with it
crates/lms-sim      a stand-in LMS around a real rtcd with synthetic participants, to test and measure its recording
vendor/rtc          the WebRTC stack, pinned (git submodule)
compat/golden       regenerates the golden vectors from the Go sources
deploy/             the entrypoint and config template the image uses
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

## Recording inside rtcd

With `[record] enabled = true` (`RTCD_RECORD_ENABLED`) a node records calls itself (no participant
joins the call): the server sends `record_start` over the websocket, each published stream is copied once
to a writer thread (`rtcd_sfu::record`), the files are the recorder crate's, rotated every two minutes
(bounding what a node shutting down still holds) and numbered from the start's `fileBase` in the order they close, and each is uploaded as it closes
through the LMS's recorder endpoints with the recording's token, then reported (`record_file`) and deleted. `record_ended` follows
once every file is stored. Every `record_start` is answered once: `refused` at
once when it is not taken (the node is draining and holds nothing of it, the recording is held for another
call or first file number, the call has another recording, or the numbers reach those of a capture whose
end is not sent yet), otherwise by the one `record_ended` of its run. The start a node already holds is
not answered again, draining or not; once the run's end has been sent, a resend of it (the server did not get the end) is answered with that end again for five minutes, and one numbered into its files with the end and a `refused` for itself. An optional `maxFiles` in `record_start` bounds the run: it never
opens more files than that, and ends as `file_limit` as soon as the last number is taken, closing and uploading what it has (absent or zero: no limit). Settings: `RTCD_RECORD_DATADIR`, `_MAXBYTES` (what one capture may write, 15 GiB by default: under the 16 GiB the server keeps of a recording, since a capture stops a moment after it crosses the cap),
`_MAXPENDINGBYTES` (closed files waiting for upload, all recordings; above it packets are left out and
counted, not the recording ended) and `_UPLOADMBPS` (the node's upload rate, so uploads never compete
with media). Files a dead process left in the data directory are removed at start. Metrics:
`rtcd_record_{recordings,files_written_total,pending_bytes,uploaded_bytes_total,upload_failures_total,
writer_dropped_packets_total,writer_busy_seconds_total}`. `lms-sim run` records a synthetic
class this way; the writer threads used 0.14 % of a core for 8 people and 7 video streams, and 0.41 %
for 100 people and 32 (release build, loopback).

## Fuzzing

`fuzz/` holds libFuzzer targets for what comes from outside: data channel frames, websocket frames, the
engine given a session and any SDP, ICE and announcements a client can send, VP8 payloads and the recording's
stream writer. `cd fuzz && cargo +nightly fuzz run <target>`; the `rtc` crates have
targets of their own for STUN, RTP, RTCP, SDP, DTLS, SCTP and SRTP.

## Not done yet

- AV1: the LMS server never offers it (`av1Support` is false on every join), so nothing would use it.
- Splitting one call across instances (calls stay on the instance they start on). One instance carries
  300 participants at about half a core.
