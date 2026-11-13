//! `lms-sim abuse` is the hostile half of a lesson soak: it runs in a container of its own (not
//! impaired, with its own address) against the same call as the lesson (`lms-sim lesson`), and
//! keeps rtcd under attack for the whole run, so that the hours measure how rtcd behaves when
//! people misbehave around a class, not only when the network does.
//!
//! ```text
//! lms-sim abuse --http http://rtcd:8045 --media 10.0.0.2:8443 --minutes 120 --impl rtcd-rs \
//!     --seed 42 --out abuse.jsonl [--call channel-lesson] [--only rtp-jump,ws-storm] [--intensity 3]
//! ```
//!
//! The run is a function of `--seed`: a seeded scheduler (see [`schedule`], a pure function) lays
//! out attack windows in `1 + intensity` lanes (the first never rests), so that one or several are
//! always going on and every few seconds something starts or stops; the class of each window, its length, which of the
//! abuse participants it uses and how hard it hits come from the seed. The same seed gives the
//! same schedule against `rtcd-rs` and the Go rtcd, which speak the same protocol. Everything is
//! sent to the address given as `--media` and the one of `--http`; nothing leaves the runner.
//!
//! Every window writes two rows with [`Sink`] (`"impl":"abuse"`, `"kind":"attack"`), one with
//! `"state":"start"` and one with `"state":"end"`, with `id`, `name` (the class), `group`
//! (`all`), `who` (participant names, when the window uses some), `seed` and `detail`; a `seed`
//! event opens the file. `scripts/lesson-soak/report.py` lays the windows over the measurements.
//!
//! Participants. Beside the lesson's (`s..` students, `teacher`, never touched, never measured)
//! the process holds `x<NN>` participants (the churners: a camera, a screen share on every other
//! one, a muted microphone) and `h<NN>` (the hostile peers: a camera, a screen share). They watch
//! the teacher and the student cameras, join through [`crate::class::join`] like anyone and are
//! not measured. Attacks reach only their sessions, never the lesson's.
//!
//! The classes, by the group the contract names them in:
//!
//! Churn, on the `x` participants:
//! - `join-leave`: leaves and joins again, the same session id, every few seconds.
//! - `camera-toggle`: camera off and on, with `video_off` and `video_on` to the SFU.
//! - `mic-toggle`: microphone unmuted and muted, with `unmute` and `mute`.
//! - `screen-toggle`: screen share stopped and started, with `screen_off` and `screen_on`.
//! - `layer-flap`: one layer of a screen share stops sending frames and starts again.
//! - `renego-storm`: bursts of client offers (the same offer again while its answer is out),
//!   on the data channel and the websocket alternately.
//! - `glare`: the participant answers the SFU's offers with an offer of its own; subscription
//!   changes make the SFU offer.
//! - `late-answer`: it answers the SFU's offers two to eight seconds late, or twice at once.
//!
//! Network (client side), on the `h` participants:
//! - `nat-rebind`: the participant's UDP source port changes under the same peer connection.
//! - `mtu`: datagrams of 1400 to 1472 bytes, 3000 bytes (they fragment) and a 1380-byte audio
//!   packet, now and then.
//!
//! Hostile media and protocol, on the `h` participants (see [`crate::hostile`]):
//! `rtp-jump`, `ssrc-change`, `pli-flood`, `nack-flood`, `bogus-rtcp`, `rtp-size`,
//! `rtcp-garbage`, `dc-flood`, `dc-oversize`.
//!
//! Junk, raw datagrams to rtcd's media port from many source ports (8 to 64, by level):
//! - `udp-junk`: random bytes of random sizes whose first byte lands in each of the demux ranges
//!   (STUN, DTLS, TURN, RTP/RTCP, nothing).
//! - `stun-flood`: STUN binding requests with random transaction ids, plain, with an unknown
//!   username, with attributes cut short or a length that lies; no valid integrity.
//! - `dtls-junk`: DTLS records (handshake, alert, application data) with random epochs, sequence
//!   numbers and bodies, and ClientHello-shaped handshakes with random fields.
//!
//! Signaling, through rtcd's websocket as a client of the LMS server would (aimed at the `x`
//! sessions):
//! - `ws-storm`: connections opened and dropped by the dozen, a `reconnect` for an existing
//!   session id before the drop, malformed, truncated, text and oversized (megabytes) frames.
//! - `dup-join`: a join for a session id that is in the call already (and twice in a row),
//!   sometimes with another user id, sometimes followed by a leave.
//! - `unknown-call`: joins for calls nobody made (empty, 4 KiB and random ids), messages, leaves
//!   and reconnects for sessions that do not exist.
//!
//! `lms-sim lesson` has the client-side attacks that belong to a lesson's own participants
//! (`teacher-reconnect`, `nat-rebind`, `mtu`) and writes their rows into its own file; the
//! helpers it uses are here ([`LessonAttacks`]).

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use base64::Engine;
use futures_util::SinkExt;
use rtcd_proto::{ClientMessage, JoinRequest, MessageType, SessionRef};
use serde_json::json;
use tokio::net::UdpSocket;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;

use crate::assets::Assets;
use crate::class::{self, Ctl, Participant, Quirk, Spec};
use crate::host::{CLIENT, KEY, Rtcd};
use crate::hostile::{Hostile, Rng};
use crate::stats::{Sink, unix_ms};

/// Which participants an attack class uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Pool {
    /// The churners.
    X,
    /// The hostile peers.
    H,
    /// None: raw sockets, or connections of its own.
    None,
}

/// One class of attack.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    JoinLeave,
    CameraToggle,
    MicToggle,
    ScreenToggle,
    LayerFlap,
    RenegoStorm,
    Glare,
    LateAnswer,
    NatRebind,
    Mtu,
    Hostile(Hostile),
    UdpJunk,
    StunFlood,
    DtlsJunk,
    WsStorm,
    DupJoin,
    UnknownCall,
}

impl Class {
    /// Every class this process runs.
    pub fn all() -> Vec<Class> {
        let mut all = vec![
            Self::JoinLeave,
            Self::CameraToggle,
            Self::MicToggle,
            Self::ScreenToggle,
            Self::LayerFlap,
            Self::RenegoStorm,
            Self::Glare,
            Self::LateAnswer,
            Self::NatRebind,
            Self::Mtu,
        ];
        all.extend(Hostile::ALL.map(Self::Hostile));
        all.extend([
            Self::UdpJunk,
            Self::StunFlood,
            Self::DtlsJunk,
            Self::WsStorm,
            Self::DupJoin,
            Self::UnknownCall,
        ]);
        all
    }

    /// The name in the attack rows and on `--only`.
    pub fn name(self) -> &'static str {
        match self {
            Self::JoinLeave => "join-leave",
            Self::CameraToggle => "camera-toggle",
            Self::MicToggle => "mic-toggle",
            Self::ScreenToggle => "screen-toggle",
            Self::LayerFlap => "layer-flap",
            Self::RenegoStorm => "renego-storm",
            Self::Glare => "glare",
            Self::LateAnswer => "late-answer",
            Self::NatRebind => "nat-rebind",
            Self::Mtu => "mtu",
            Self::Hostile(h) => h.name(),
            Self::UdpJunk => "udp-junk",
            Self::StunFlood => "stun-flood",
            Self::DtlsJunk => "dtls-junk",
            Self::WsStorm => "ws-storm",
            Self::DupJoin => "dup-join",
            Self::UnknownCall => "unknown-call",
        }
    }

    pub fn parse(name: &str) -> Option<Class> {
        Self::all().into_iter().find(|c| c.name() == name)
    }

    pub fn pool(self) -> Pool {
        match self {
            Self::JoinLeave
            | Self::CameraToggle
            | Self::MicToggle
            | Self::ScreenToggle
            | Self::LayerFlap
            | Self::RenegoStorm
            | Self::Glare
            | Self::LateAnswer => Pool::X,
            Self::NatRebind | Self::Mtu | Self::Hostile(_) => Pool::H,
            _ => Pool::None,
        }
    }

    /// The range of a window's length, in seconds.
    fn len_secs(self) -> (u64, u64) {
        match self.pool() {
            Pool::X => (15, 60),
            Pool::H => (10, 45),
            Pool::None => match self {
                Self::WsStorm | Self::DupJoin | Self::UnknownCall => (10, 30),
                _ => (10, 40),
            },
        }
    }
}

/// How many churners and hostile peers a run holds.
pub fn pools(intensity: u8) -> (u32, u32) {
    let i = u32::from(intensity.clamp(1, 5));
    (3 + i, 2 + i)
}

/// One attack window of the schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Window {
    pub id: String,
    pub class: Class,
    pub start_ms: u64,
    pub len_ms: u64,
    /// Indexes into the class's pool.
    pub who: Vec<u32>,
    /// How hard, 1 to 5.
    pub level: u8,
    /// Seeds what the window does at random.
    pub salt: u64,
}

/// The schedule of a run: a pure function of its arguments. `only` empty means every class.
pub fn schedule(
    seed: u64,
    minutes: u64,
    intensity: u8,
    only: &[Class],
    (nx, nh): (u32, u32),
) -> Vec<Window> {
    let intensity = intensity.clamp(1, 5);
    let classes = if only.is_empty() {
        Class::all()
    } else {
        only.to_vec()
    };
    let total = minutes * 60_000;
    let mut rng = Rng::new(seed);
    let mut out: Vec<(u64, usize, Window)> = Vec::new();
    for lane in 0..=usize::from(intensity) {
        let mut r = Rng::new(rng.word());
        let mut t = 3_000 + r.below(10_000);
        while t < total {
            let class = *r.pick(&classes);
            let (lo, hi) = class.len_secs();
            let len = (r.range(lo, hi) * 1000).min(total - t);
            let size = match class.pool() {
                Pool::X => nx,
                Pool::H => nh,
                Pool::None => 0,
            };
            let level = (i64::from(intensity) + r.range(0, 2) as i64 - 1).clamp(1, 5) as u8;
            let mut who: Vec<u32> = (0..size).collect();
            let keep = (r.range(1, 1 + u64::from(level)) as usize).min(who.len());
            for k in 0..keep {
                let j = k + r.below((who.len() - k) as u64) as usize;
                who.swap(k, j);
            }
            who.truncate(keep);
            out.push((
                t,
                lane,
                Window {
                    id: String::new(),
                    class,
                    start_ms: t,
                    len_ms: len,
                    who,
                    level,
                    salt: r.word(),
                },
            ));
            // The first lane never rests, so that something is always going on; the others do.
            t += len;
            if lane > 0 {
                t += 1_000 + r.below(u64::from(12 - 2 * u32::from(intensity)) * 1000);
            }
        }
    }
    out.sort_by_key(|(t, lane, _)| (*t, *lane));
    out.into_iter()
        .enumerate()
        .map(|(i, (_, _, mut w))| {
            w.id = format!("{seed:x}-{i}");
            w
        })
        .collect()
}

/// The seconds, from a start, of a recurring event of one participant: the first at a seeded
/// point in the first `every` seconds, then every `every` seconds give or take a fifth.
pub fn every(seed: u64, name: &str, every_secs: u64, total_secs: u64) -> Vec<u64> {
    let mut h = seed;
    for b in name.bytes() {
        h = h.wrapping_mul(0x100_0000_01B3) ^ u64::from(b);
    }
    let mut r = Rng::new(h);
    let every = every_secs.max(1);
    let mut t = r.range(1, every);
    let mut out = Vec::new();
    while t < total_secs {
        out.push(t);
        t += (every * 4 / 5 + r.below(every * 2 / 5 + 1)).max(1);
    }
    out
}

/// Writes the attack rows.
#[derive(Debug, Clone)]
pub struct Rows {
    sink: Arc<Sink>,
    group: &'static str,
    seed: u64,
    target: String,
}

impl Rows {
    /// `group` is the network the attack is on, `target` the rtcd under test.
    pub fn new(sink: Arc<Sink>, group: &'static str, seed: u64, target: &str) -> Self {
        Self {
            sink,
            group,
            seed,
            target: target.to_owned(),
        }
    }

    /// The first line of a run: its seed.
    pub fn seed_event(&self, detail: &str) {
        self.sink.write(
            "abuse",
            "event",
            "abuse",
            json!({ "what": "seed", "detail": detail, "group": self.group,
                    "seed": self.seed, "target": self.target }),
        );
    }

    pub fn start(&self, id: &str, name: &str, who: &[String], detail: &str) {
        self.row(id, name, "start", who, detail);
    }

    pub fn end(&self, id: &str, name: &str, who: &[String], detail: &str) {
        self.row(id, name, "end", who, detail);
    }

    fn row(&self, id: &str, name: &str, state: &str, who: &[String], detail: &str) {
        let mut f = json!({ "name": name, "state": state, "group": self.group,
                            "seed": self.seed, "detail": detail, "target": self.target });
        if !who.is_empty() {
            f["who"] = json!(who);
        }
        self.sink.write("abuse", "attack", id, f);
    }
}

/// One of the pool's participants.
#[derive(Debug)]
struct Slot {
    spec: Spec,
    index: u32,
    p: tokio::sync::Mutex<Option<Participant>>,
    tally: Arc<AtomicU64>,
}

impl Slot {
    async fn control(&self, c: Ctl) {
        if let Some(p) = &*self.p.lock().await {
            p.control(c);
        }
    }

    async fn join(&self, env: &Env) {
        *self.p.lock().await = Some(
            class::join(
                &env.rtcd,
                env.assets.clone(),
                &env.call,
                self.spec.clone(),
                self.index,
            )
            .await,
        );
    }

    fn session(&self) -> String {
        format!("session-{}", self.spec.name)
    }
}

/// Everything the attacks act on: the call, the abuse participants, the rows.
#[derive(Debug)]
pub struct Env {
    rtcd: Arc<Rtcd>,
    assets: Arc<Assets>,
    call: String,
    rows: Rows,
    x: Vec<Slot>,
    h: Vec<Slot>,
    /// Windows that started and have not ended.
    open: Mutex<HashMap<String, (Class, Vec<String>)>>,
    /// What each class did so far (actions, packets or messages sent).
    fired: Mutex<HashMap<&'static str, u64>>,
}

/// The cameras the abuse participants watch: the teacher's and the first students'.
const WATCH_DEFAULT: u32 = 0b1_1111;

fn slot(prefix: char, k: u32, base: u32, seed: u64, screen: bool) -> Slot {
    let name = format!("{prefix}{k:02}");
    let mut h = seed;
    for b in name.bytes() {
        h = h.wrapping_mul(0x100_0000_01B3) ^ u64::from(b);
    }
    let tally = Arc::new(AtomicU64::new(0));
    let mut watch = vec!["teacher".to_owned()];
    watch.extend((0..4).map(|i| format!("s{i:02}")));
    Slot {
        spec: Spec {
            name,
            camera: true,
            screen,
            watch,
            seed: h,
            tally: Some(tally.clone()),
            ..Default::default()
        },
        index: base + k,
        p: tokio::sync::Mutex::new(None),
        tally,
    }
}

impl Env {
    /// Joins `nx` churners and `nh` hostile peers to `call`.
    pub async fn start(
        rtcd: Arc<Rtcd>,
        assets: Arc<Assets>,
        call: &str,
        rows: Rows,
        nx: u32,
        nh: u32,
    ) -> Self {
        let seed = rows.seed;
        let env = Self {
            rtcd,
            assets,
            call: call.to_owned(),
            rows,
            x: (0..nx)
                .map(|k| slot('x', k, 500, seed, k % 2 == 0))
                .collect(),
            h: (0..nh).map(|k| slot('h', k, 700, seed, true)).collect(),
            open: Mutex::default(),
            fired: Mutex::default(),
        };
        for s in env.x.iter().chain(&env.h) {
            s.join(&env).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        env
    }

    /// What each class did so far.
    pub fn fired(&self) -> HashMap<&'static str, u64> {
        self.fired.lock().unwrap().clone()
    }

    fn pick(&self, pool: Pool, who: &[u32]) -> Vec<&Slot> {
        let slots = match pool {
            Pool::X => &self.x,
            Pool::H => &self.h,
            Pool::None => return Vec::new(),
        };
        who.iter().filter_map(|i| slots.get(*i as usize)).collect()
    }

    /// Ends what is open (the run was stopped) and takes every abuse participant out.
    pub async fn finish(&self) {
        let open: Vec<_> = self.open.lock().unwrap().drain().collect();
        for (id, (class, who)) in open {
            self.rows
                .end(&id, class.name(), &who, "stopped with the run");
        }
        for s in self.x.iter().chain(&self.h) {
            if let Some(p) = s.p.lock().await.take() {
                p.leave().await;
            }
        }
    }
}

/// Runs one window: its rows, and what it does between them. Returns what it did (a count of
/// actions, packets or messages).
pub async fn run_window(env: &Env, w: &Window) -> u64 {
    let who: Vec<String> = env
        .pick(w.class.pool(), &w.who)
        .iter()
        .map(|s| s.spec.name.clone())
        .collect();
    env.open
        .lock()
        .unwrap()
        .insert(w.id.clone(), (w.class, who.clone()));
    env.rows.start(
        &w.id,
        w.class.name(),
        &who,
        &format!("{} ms at level {}", w.len_ms, w.level),
    );
    let limit = Duration::from_millis(w.len_ms) + Duration::from_secs(30);
    let (n, detail) = match tokio::time::timeout(limit, exec(env, w)).await {
        Ok(n) => (n, format!("{n} sent")),
        Err(_) => (0, "did not finish in time".to_owned()),
    };
    *env.fired.lock().unwrap().entry(w.class.name()).or_default() += n;
    if env.open.lock().unwrap().remove(&w.id).is_some() {
        env.rows.end(&w.id, w.class.name(), &who, &detail);
    }
    n
}

async fn exec(env: &Env, w: &Window) -> u64 {
    let end = Instant::now() + Duration::from_millis(w.len_ms);
    let mut rng = Rng::new(w.salt);
    match w.class {
        Class::JoinLeave => join_leave(env, w, end, &mut rng).await,
        Class::CameraToggle
        | Class::MicToggle
        | Class::ScreenToggle
        | Class::LayerFlap
        | Class::RenegoStorm
        | Class::Glare
        | Class::LateAnswer => churn(env, w, end, &mut rng).await,
        Class::NatRebind | Class::Mtu | Class::Hostile(_) => hostile(env, w, end, &mut rng).await,
        Class::UdpJunk | Class::StunFlood | Class::DtlsJunk => junk(env, w, end, &mut rng).await,
        Class::WsStorm => ws_storm(env, w, end).await,
        Class::DupJoin => dup_join(env, w, end, &mut rng).await,
        Class::UnknownCall => unknown_call(env, w, end, &mut rng).await,
    }
}

/// A pause that gets shorter as the level rises.
fn pause(rng: &mut Rng, lo: u64, hi: u64, level: u8) -> Duration {
    Duration::from_millis(rng.range(lo, hi) * (6 - u64::from(level)) / 3)
}

/// Leaves and joins again with the same session id.
async fn join_leave(env: &Env, w: &Window, end: Instant, rng: &mut Rng) -> u64 {
    let slots = env.pick(Pool::X, &w.who);
    let mut n = 0;
    while Instant::now() < end && !slots.is_empty() {
        let s = slots[rng.below(slots.len() as u64) as usize];
        let mut g = s.p.lock().await;
        if let Some(p) = g.take() {
            p.leave().await;
            n += 1;
        }
        // Sometimes at once, before the SFU has let go of the seat.
        if rng.chance(0.5) {
            tokio::time::sleep(Duration::from_millis(rng.below(1500))).await;
        }
        drop(g);
        s.join(env).await;
        n += 1;
        tokio::time::sleep(pause(rng, 500, 3000, w.level)).await;
    }
    // Whoever is out is back.
    for s in slots {
        if s.p.lock().await.is_none() {
            s.join(env).await;
        }
    }
    n
}

/// The toggles and storms that are controls to a joined participant.
async fn churn(env: &Env, w: &Window, end: Instant, rng: &mut Rng) -> u64 {
    let slots = env.pick(Pool::X, &w.who);
    let mut n = 0;
    // Per slot: whether it is on (camera, mic, screen) and which layers are.
    let mut on = vec![true; slots.len()];
    let mut layers = vec![[true; 2]; slots.len()];
    for (i, s) in slots.iter().enumerate() {
        match w.class {
            Class::Glare => s.control(Ctl::Quirk(Quirk::Glare)).await,
            Class::LateAnswer => {
                let q = if i % 2 == 0 {
                    Quirk::Late(Duration::from_millis(rng.range(2000, 8000)))
                } else {
                    Quirk::Twice
                };
                s.control(Ctl::Quirk(q)).await;
            }
            Class::MicToggle => on[i] = false,
            _ => {}
        }
    }
    while Instant::now() < end && !slots.is_empty() {
        let i = rng.below(slots.len() as u64) as usize;
        let s = slots[i];
        let controls: Vec<Ctl> = match w.class {
            Class::CameraToggle => {
                on[i] = !on[i];
                vec![
                    Ctl::Camera(on[i]),
                    Ctl::Announce(if on[i] {
                        MessageType::VideoOn
                    } else {
                        MessageType::VideoOff
                    }),
                ]
            }
            Class::MicToggle => {
                on[i] = !on[i];
                vec![
                    Ctl::Voice(on[i]),
                    Ctl::Announce(if on[i] {
                        MessageType::Unmute
                    } else {
                        MessageType::Mute
                    }),
                ]
            }
            Class::ScreenToggle => {
                on[i] = !on[i];
                vec![
                    Ctl::Screen(on[i]),
                    Ctl::Announce(if on[i] {
                        MessageType::ScreenOn
                    } else {
                        MessageType::ScreenOff
                    }),
                ]
            }
            Class::LayerFlap => {
                let l = rng.below(2) as usize;
                layers[i][l] = !layers[i][l];
                vec![Ctl::Layer(l, layers[i][l])]
            }
            Class::RenegoStorm => {
                vec![Ctl::Renego(rng.range(2, 4 * u64::from(w.level) + 2) as u32)]
            }
            // A subscription that changes makes the SFU offer.
            _ => vec![Ctl::Watch(
                WATCH_DEFAULT ^ (1 << rng.range(1, 25)) ^ (rng.word() as u32 & 0xF0),
            )],
        };
        n += controls.len() as u64;
        for c in controls {
            s.control(c).await;
        }
        tokio::time::sleep(pause(rng, 250, 2500, w.level)).await;
    }
    // Back to what the churners are.
    for s in &slots {
        for c in [
            Ctl::Quirk(Quirk::None),
            Ctl::Watch(WATCH_DEFAULT),
            Ctl::Camera(true),
            Ctl::Announce(MessageType::VideoOn),
            Ctl::Voice(false),
            Ctl::Announce(MessageType::Mute),
            Ctl::Screen(true),
            Ctl::Announce(MessageType::ScreenOn),
            Ctl::Layer(0, true),
            Ctl::Layer(1, true),
        ] {
            s.control(c).await;
        }
    }
    n
}

/// What the `h` participants do: misbehave, change port, send big datagrams.
async fn hostile(env: &Env, w: &Window, end: Instant, rng: &mut Rng) -> u64 {
    let slots = env.pick(Pool::H, &w.who);
    let before: u64 = slots.iter().map(|s| s.tally.load(Ordering::Relaxed)).sum();
    let secs = (w.len_ms / 1000 + 1) as u32;
    for s in &slots {
        match w.class {
            Class::Hostile(kind) => {
                s.control(Ctl::Hostile {
                    kind,
                    secs,
                    level: w.level,
                })
                .await;
            }
            Class::Mtu => s.control(Ctl::Mtu(true)).await,
            _ => {}
        }
    }
    while Instant::now() < end {
        let step = if w.class == Class::NatRebind {
            for s in &slots {
                s.control(Ctl::Rebind).await;
            }
            pause(rng, 1000, 4000, w.level)
        } else {
            Duration::from_millis(500)
        };
        tokio::time::sleep(step.min(end.saturating_duration_since(Instant::now()))).await;
    }
    for s in &slots {
        match w.class {
            Class::Mtu => s.control(Ctl::Mtu(false)).await,
            // The window is over, and the misbehaviour with it.
            Class::Hostile(kind) => {
                s.control(Ctl::Hostile {
                    kind,
                    secs: 0,
                    level: w.level,
                })
                .await
            }
            _ => {}
        }
    }
    slots
        .iter()
        .map(|s| s.tally.load(Ordering::Relaxed))
        .sum::<u64>()
        .saturating_sub(before)
}

/// A datagram of random bytes whose first byte lands in one of the ranges rtcd tells apart.
pub fn udp_noise(rng: &mut Rng) -> Vec<u8> {
    let mut d = rng.noise(1, 1500);
    d[0] = match rng.below(6) {
        0 => rng.below(4) as u8,
        1 => 20 + rng.below(44) as u8,
        2 => 64 + rng.below(16) as u8,
        3 => 128 + rng.below(64) as u8,
        4 => 192 + rng.below(32) as u8,
        _ => rng.word() as u8,
    };
    d
}

/// A STUN binding request: a valid header with a random transaction id, plain, with a username
/// no one owns, with an attribute cut short or with a length that lies.
pub fn stun_packet(rng: &mut Rng) -> Vec<u8> {
    let mut body = Vec::new();
    match rng.below(4) {
        0 => {}
        1 => {
            // USERNAME (0x0006): "a:b", padded to four bytes.
            let user = rng.noise(3, 20);
            body.extend_from_slice(&[0x00, 0x06]);
            body.extend_from_slice(&(user.len() as u16).to_be_bytes());
            body.extend_from_slice(&user);
            body.resize(body.len().div_ceil(4) * 4, 0);
        }
        2 => {
            // An attribute that says 40 bytes and has 3.
            body.extend_from_slice(&[0x00, 0x24, 0x00, 40, 1, 2, 3]);
        }
        _ => body = rng.noise(0, 60),
    }
    let claimed = if rng.chance(0.25) {
        rng.word() as u16
    } else {
        body.len() as u16
    };
    let mut p = vec![0x00, 0x01];
    p.extend_from_slice(&claimed.to_be_bytes());
    p.extend_from_slice(&0x2112_A442u32.to_be_bytes());
    p.extend(rng.bytes(12));
    p.extend(body);
    p
}

/// A DTLS record: a handshake shaped like a ClientHello with random fields, or a handshake,
/// alert or application data record with random epoch, sequence number and body.
pub fn dtls_packet(rng: &mut Rng) -> Vec<u8> {
    let (content, epoch, body) = if rng.chance(0.4) {
        // ClientHello: handshake type 1, length, message seq, fragment offset and length, then
        // version, random, session id length 0, cookie length, cipher suites, compression.
        let mut hello = vec![0xFE, 0xFD];
        hello.extend(rng.bytes(32));
        hello.push(0);
        let cookie = rng.noise(0, 20);
        hello.push(cookie.len() as u8);
        hello.extend(cookie);
        let suites = rng.noise(2, 12);
        hello.extend_from_slice(&((suites.len() & !1) as u16).to_be_bytes());
        hello.extend(&suites[..suites.len() & !1]);
        hello.extend_from_slice(&[1, 0]);
        let n = hello.len() as u32;
        let mut h = vec![1];
        h.extend_from_slice(&n.to_be_bytes()[1..]);
        h.extend_from_slice(&[0, 0, 0, 0, 0]);
        h.extend_from_slice(&n.to_be_bytes()[1..]);
        h.extend(hello);
        (22u8, 0u16, h)
    } else {
        (
            *rng.pick(&[20u8, 21, 22, 23]),
            rng.below(3) as u16,
            rng.noise(0, 1200),
        )
    };
    let mut p = vec![content, 0xFE, 0xFD];
    p.extend_from_slice(&epoch.to_be_bytes());
    p.extend_from_slice(&rng.word().to_be_bytes()[2..]);
    let len = if rng.chance(0.1) {
        rng.word() as u16
    } else {
        body.len() as u16
    };
    p.extend_from_slice(&len.to_be_bytes());
    p.extend(body);
    p
}

/// Raw datagrams to the media port from many source ports.
async fn junk(env: &Env, w: &Window, end: Instant, rng: &mut Rng) -> u64 {
    let level = usize::from(w.level.clamp(1, 5)) - 1;
    let local = std::env::var("LMS_SIM_LOCAL_IP").unwrap_or_else(|_| "127.0.0.1".into());
    let mut socks = Vec::new();
    for _ in 0..[8, 12, 16, 32, 64][level] {
        if let Ok(s) = UdpSocket::bind(format!("{local}:0")).await {
            socks.push(s);
        }
    }
    if socks.is_empty() {
        return 0;
    }
    let per_tick = [2, 5, 12, 30, 80][level];
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut n = 0;
    while Instant::now() < end {
        tick.tick().await;
        for _ in 0..per_tick {
            let d = match w.class {
                Class::StunFlood => stun_packet(rng),
                Class::DtlsJunk => dtls_packet(rng),
                _ => udp_noise(rng),
            };
            let s = &socks[rng.below(socks.len() as u64) as usize];
            if s.send_to(&d, env.rtcd.media).await.is_ok() {
                n += 1;
            }
        }
    }
    n
}

type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A websocket to rtcd as the registered client, with none of the conveniences of
/// [`Rtcd::connect`](crate::host::Rtcd::try_connect): the bytes sent are the caller's.
async fn raw_ws(rtcd: &Rtcd) -> Option<Ws> {
    let auth = format!(
        "Basic {}",
        base64::engine::general_purpose::STANDARD.encode(format!("{CLIENT}:{KEY}"))
    );
    let mut req = format!("ws://{}/ws", rtcd.http.trim_start_matches("http://"))
        .into_client_request()
        .ok()?;
    req.headers_mut()
        .insert("Authorization", auth.parse().ok()?);
    Some(tokio_tungstenite::connect_async(req).await.ok()?.0)
}

fn join_message(call: &str, user: &str, session: &str) -> ClientMessage {
    ClientMessage::Join(JoinRequest {
        call_id: call.into(),
        user_id: user.into(),
        session_id: session.into(),
        channel_id: format!("channel-{call}"),
        av1_support: false,
        dc_signaling: true,
        host: false,
        observer: false,
    })
}

async fn send(ws: &mut Ws, m: WsMessage) -> bool {
    ws.send(m).await.is_ok()
}

async fn send_client(ws: &mut Ws, m: &ClientMessage) -> bool {
    send(ws, WsMessage::Binary(m.pack().into())).await
}

/// Connections opened and dropped, reconnects for sessions that exist, frames that are not
/// messages.
async fn ws_storm(env: &Env, w: &Window, end: Instant) -> u64 {
    let conc = [1u64, 2, 4, 6, 8][usize::from(w.level.clamp(1, 5)) - 1];
    let sessions: Vec<String> = env.x.iter().map(Slot::session).collect();
    let n = AtomicU64::new(0);
    let storm = |lane: u64| {
        let (n, sessions) = (&n, &sessions);
        async move {
            let mut rng = Rng::new(w.salt ^ (lane + 1).wrapping_mul(0x9E37_79B9));
            while Instant::now() < end {
                let Some(mut ws) = raw_ws(&env.rtcd).await else {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    continue;
                };
                n.fetch_add(1, Ordering::Relaxed);
                for _ in 0..rng.range(1, 4) {
                    let sent = match rng.below(6) {
                        0 if !sessions.is_empty() => {
                            let s = rng.pick(sessions).clone();
                            send_client(
                                &mut ws,
                                &ClientMessage::Reconnect(SessionRef { session_id: s }),
                            )
                            .await
                        }
                        1 => send(&mut ws, WsMessage::Binary(rng.noise(1, 4096).into())).await,
                        2 => {
                            let mut m =
                                join_message(&env.call, "user-storm", "session-storm").pack();
                            m.truncate(m.len() / 2);
                            send(&mut ws, WsMessage::Binary(m.into())).await
                        }
                        3 => send(&mut ws, WsMessage::Text("{\"type\":\"join\"}".into())).await,
                        4 if rng.chance(0.2) => {
                            let big = vec![0x5Au8; rng.range(1, 12) as usize * 1024 * 1024];
                            send(&mut ws, WsMessage::Binary(big.into())).await
                        }
                        _ => send(&mut ws, WsMessage::Ping(rng.noise(0, 100).into())).await,
                    };
                    if sent {
                        n.fetch_add(1, Ordering::Relaxed);
                    }
                }
                tokio::time::sleep(Duration::from_millis(rng.range(20, 300))).await;
                // Dropped without a close.
                drop(ws);
                tokio::time::sleep(pause(&mut rng, 50, 400, w.level)).await;
            }
        }
    };
    futures_util::future::join_all((0..conc).map(storm)).await;
    n.load(Ordering::Relaxed)
}

/// Joins for session ids that are in the call.
async fn dup_join(env: &Env, w: &Window, end: Instant, rng: &mut Rng) -> u64 {
    let mut n = 0;
    while Instant::now() < end && !env.x.is_empty() {
        let s = &env.x[rng.below(env.x.len() as u64) as usize];
        let session = s.session();
        let user = if rng.chance(0.3) {
            format!("user-dup-{}", rng.below(1000))
        } else {
            format!("user-{}", s.spec.name)
        };
        if let Some(mut ws) = raw_ws(&env.rtcd).await {
            let join = join_message(&env.call, &user, &session);
            for _ in 0..rng.range(1, 2) {
                n += u64::from(send_client(&mut ws, &join).await);
            }
            tokio::time::sleep(Duration::from_millis(rng.range(100, 1500))).await;
            if rng.chance(0.3) {
                n += u64::from(
                    send_client(
                        &mut ws,
                        &ClientMessage::Leave(SessionRef {
                            session_id: session,
                        }),
                    )
                    .await,
                );
            }
        }
        tokio::time::sleep(pause(rng, 300, 2000, w.level)).await;
    }
    n
}

/// Joins for calls nobody made, and messages for sessions that do not exist.
async fn unknown_call(env: &Env, w: &Window, end: Instant, rng: &mut Rng) -> u64 {
    let mut n = 0;
    while Instant::now() < end {
        let Some(mut ws) = raw_ws(&env.rtcd).await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let ghost = format!("ghost-{:x}", rng.word());
        let call = match rng.below(4) {
            0 => String::new(),
            1 => "c".repeat(4096),
            _ => format!("unknown-{:x}", rng.word()),
        };
        let mut sends = vec![join_message(&call, &format!("user-{ghost}"), &ghost)];
        for kind in [
            MessageType::Sdp,
            MessageType::Ice,
            MessageType::Mute,
            MessageType::VideoOn,
            MessageType::VideoSubscribe,
        ] {
            sends.push(class::message(&ghost, &call, kind, rng.noise(0, 200)));
            sends.push(class::message(
                &format!("nope-{ghost}"),
                &env.call,
                kind,
                rng.noise(0, 50),
            ));
        }
        sends.push(ClientMessage::Leave(SessionRef {
            session_id: format!("nope-{ghost}"),
        }));
        sends.push(ClientMessage::Reconnect(SessionRef {
            session_id: format!("nope-{ghost}"),
        }));
        for m in &sends {
            n += u64::from(send_client(&mut ws, m).await);
        }
        tokio::time::sleep(Duration::from_millis(rng.range(100, 800))).await;
        drop(ws);
        tokio::time::sleep(pause(rng, 200, 1500, w.level)).await;
    }
    n
}

/// The arguments of `lms-sim abuse`.
#[derive(Debug)]
pub struct Args {
    pub http: String,
    pub media: SocketAddr,
    pub minutes: u64,
    pub imp: String,
    pub seed: u64,
    pub out: PathBuf,
    pub call: String,
    pub only: Vec<Class>,
    pub intensity: u8,
}

pub fn parse(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut a = Args {
        http: String::new(),
        media: "127.0.0.1:8443".parse().unwrap(),
        minutes: 120,
        imp: "rtcd-rs".into(),
        seed: unix_ms(),
        out: PathBuf::new(),
        call: "channel-lesson".into(),
        only: Vec::new(),
        intensity: 3,
    };
    let mut it = args;
    while let Some(k) = it.next() {
        let v = it.next().ok_or_else(|| format!("{k} needs a value"))?;
        let n = || {
            v.parse::<u64>()
                .map_err(|_| format!("{k}: {v} is not a number"))
        };
        match k.as_str() {
            "--http" => a.http = v.clone(),
            "--media" => {
                a.media = v
                    .parse()
                    .map_err(|_| format!("{k}: {v} is not an address"))?
            }
            "--minutes" => a.minutes = n()?,
            "--impl" => a.imp = v.clone(),
            "--seed" => a.seed = n()?,
            "--out" => a.out = v.into(),
            "--call" => a.call = v.clone(),
            "--only" => {
                a.only = v
                    .split(',')
                    .map(|c| {
                        Class::parse(c).ok_or_else(|| format!("{k}: {c} is not an attack class"))
                    })
                    .collect::<Result<_, _>>()?;
            }
            "--intensity" => a.intensity = n()?.clamp(1, 5) as u8,
            other => return Err(format!("unknown option {other}")),
        }
    }
    if a.http.is_empty() || a.out.as_os_str().is_empty() {
        return Err("--http and --out are required".into());
    }
    Ok(a)
}

/// Runs `lms-sim abuse`.
pub async fn run(args: impl Iterator<Item = String>, assets: PathBuf) {
    let a = match parse(args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let assets = Arc::new(Assets::load(&assets).expect("the media is made (ffmpeg)"));
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sink = Sink::open(&a.out).expect("the output file opens");
    let rows = Rows::new(sink, "all", a.seed, &a.imp);
    let rtcd = Arc::new(Rtcd::remote(&a.http, a.media).await);
    let (nx, nh) = pools(a.intensity);
    let plan = schedule(a.seed, a.minutes, a.intensity, &a.only, (nx, nh));
    let detail = format!(
        "seed {} intensity {} minutes {} windows {} churners {nx} hostile {nh}",
        a.seed,
        a.intensity,
        a.minutes,
        plan.len()
    );
    println!("abuse {detail}");
    rows.seed_event(&detail);
    let env = Arc::new(Env::start(rtcd, assets, &a.call, rows, nx, nh).await);
    let t0 = Instant::now();
    let mut tasks = JoinSet::new();
    for w in plan {
        let env = env.clone();
        tasks.spawn(async move {
            tokio::time::sleep_until((t0 + Duration::from_millis(w.start_ms)).into()).await;
            run_window(&env, &w).await;
        });
    }
    tokio::select! {
        () = async { while tasks.join_next().await.is_some() {} } => {}
        _ = tokio::signal::ctrl_c() => tasks.abort_all(),
    }
    println!("abuse fired {:?}", env.fired());
    env.finish().await;
}

/// The attacks `lms-sim lesson` runs on its own participants, and their rows: the teacher drops
/// its session and rejoins with the same session id (`teacher-reconnect`), each student of the
/// impaired group changes its UDP source port (`nat-rebind`), and the impaired group sends
/// datagrams of MTU size and over (`mtu`).
#[derive(Debug)]
pub struct LessonAttacks {
    rows: Rows,
    started: Instant,
    /// Per student: the name, its index in the lesson and the seconds of its rebinds still to come.
    nat: Vec<(String, u32, VecDeque<u64>)>,
    reconnects: VecDeque<u64>,
    /// Windows to close: when, id, class, who.
    ends: Vec<(Instant, String, &'static str, Vec<String>)>,
    mtu: Option<(String, Vec<String>)>,
    seq: u32,
    /// How long a rebind or a reconnect is a window.
    settle: Duration,
}

/// How long a rebind or a reconnect is a window.
const SETTLE: Duration = Duration::from_secs(10);

impl LessonAttacks {
    /// `students` are the impaired group's names and indexes (empty for a clean group), `nat_secs` and
    /// `reconnect_mins` the intervals, `teacher` whether this process has the teacher.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        rows: Rows,
        students: &[(String, u32)],
        nat_secs: Option<u64>,
        reconnect_mins: Option<u64>,
        teacher: bool,
        mtu: bool,
        minutes: u64,
    ) -> Self {
        let seed = rows.seed;
        let total = minutes * 60;
        let nat = nat_secs
            .map(|s| {
                students
                    .iter()
                    .map(|(n, i)| (n.clone(), *i, every(seed, n, s, total).into()))
                    .collect()
            })
            .unwrap_or_default();
        let reconnects = reconnect_mins
            .filter(|_| teacher)
            .map(|m| every(seed, "teacher", m * 60, total).into())
            .unwrap_or_default();
        let mut a = Self {
            rows,
            started: Instant::now(),
            nat,
            reconnects,
            ends: Vec::new(),
            mtu: None,
            seq: 0,
            settle: SETTLE,
        };
        if mtu && !students.is_empty() {
            let who: Vec<String> = students.iter().map(|(n, _)| n.clone()).collect();
            let id = format!("{seed:x}-mtu-{}", who[0]);
            a.rows
                .start(&id, "mtu", &who, "datagrams of 1400 to 3000 bytes");
            a.mtu = Some((id, who));
        }
        a
    }

    fn id(&mut self, class: &str, who: &str) -> String {
        self.seq += 1;
        format!("{:x}-{class}-{who}-{}", self.rows.seed, self.seq)
    }

    /// Once a second: the rebinds that are due, and the windows to close.
    pub fn tick(&mut self, people: &[(u32, Participant)]) {
        let now = self.started.elapsed().as_secs();
        for i in 0..self.nat.len() {
            while self.nat[i].2.front().is_some_and(|t| *t <= now) {
                self.nat[i].2.pop_front();
                let (name, index) = (self.nat[i].0.clone(), self.nat[i].1);
                let Some((_, p)) = people.iter().find(|(k, _)| *k == index) else {
                    continue;
                };
                let id = self.id("nat-rebind", &name);
                let who = vec![name];
                self.rows
                    .start(&id, "nat-rebind", &who, "the UDP source port changes");
                p.control(Ctl::Rebind);
                self.ends
                    .push((Instant::now() + self.settle, id, "nat-rebind", who));
            }
        }
        self.close_due();
    }

    fn close_due(&mut self) {
        let now = Instant::now();
        let (due, rest): (Vec<_>, Vec<_>) = self.ends.drain(..).partition(|e| e.0 <= now);
        self.ends = rest;
        for (_, id, class, who) in due {
            self.rows.end(&id, class, &who, "settled");
        }
    }

    /// Whether the teacher is due to drop its session.
    pub fn teacher_due(&mut self) -> bool {
        let now = self.started.elapsed().as_secs();
        let due = self.reconnects.front().is_some_and(|t| *t <= now);
        if due {
            self.reconnects.pop_front();
        }
        due
    }

    /// The teacher leaves and joins again with the same session id (the spec's name).
    pub async fn teacher_reconnect(
        &mut self,
        rtcd: &Arc<Rtcd>,
        assets: Arc<Assets>,
        call: &str,
        spec: Spec,
        teacher: Participant,
    ) -> Participant {
        let id = self.id("teacher-reconnect", "teacher");
        let who = vec!["teacher".to_owned()];
        self.rows.start(
            &id,
            "teacher-reconnect",
            &who,
            "the teacher drops its session and joins again",
        );
        teacher.leave().await;
        let back = class::join(rtcd, assets, call, spec, 0).await;
        self.ends
            .push((Instant::now() + self.settle, id, "teacher-reconnect", who));
        back
    }

    /// Closes what is open: the run is over.
    pub fn finish(&mut self) {
        for (_, id, class, who) in std::mem::take(&mut self.ends) {
            self.rows.end(&id, class, &who, "run over");
        }
        if let Some((id, who)) = self.mtu.take() {
            self.rows.end(&id, "mtu", &who, "run over");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schedule_is_a_function_of_its_seed() {
        let p = pools(3);
        let a = schedule(42, 120, 3, &[], p);
        assert_eq!(a, schedule(42, 120, 3, &[], p));
        assert_ne!(a, schedule(43, 120, 3, &[], p));
        // Something starts every few seconds for the whole run, with classes of every kind.
        assert!(a.len() > 300, "{} windows", a.len());
        // From the first minute to the last, a window is open at every second.
        for t in (60..119 * 60).step_by(1) {
            let ms = t * 1000;
            assert!(
                a.iter()
                    .any(|w| w.start_ms <= ms && ms < w.start_ms + w.len_ms),
                "nothing happens at {t} s"
            );
        }
        for c in Class::all() {
            assert!(a.iter().any(|w| w.class == c), "no {} window", c.name());
        }
        let mut ids: Vec<&str> = a.iter().map(|w| w.id.as_str()).collect();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), a.len());
        for w in &a {
            assert!(w.start_ms + w.len_ms <= 120 * 60_000 && w.len_ms >= 1000);
            assert!((1..=5).contains(&w.level));
            let size = match w.class.pool() {
                Pool::X => p.0,
                Pool::H => p.1,
                Pool::None => 0,
            };
            assert_eq!(w.who.is_empty(), size == 0);
            assert!(w.who.iter().all(|i| *i < size));
        }
        // `only` and the intensity are respected.
        let only = [
            Class::parse("ws-storm").unwrap(),
            Class::parse("rtp-jump").unwrap(),
        ];
        assert!(
            schedule(7, 30, 3, &only, p)
                .iter()
                .all(|w| only.contains(&w.class))
        );
        assert!(
            schedule(7, 120, 5, &[], pools(5)).len() > schedule(7, 120, 1, &[], pools(1)).len()
        );
    }

    #[test]
    fn the_class_names_round_trip_and_the_events_are_seeded() {
        for c in Class::all() {
            assert_eq!(Class::parse(c.name()), Some(c));
        }
        assert_eq!(Class::parse("nonsense"), None);
        let (a, b) = (every(5, "s01", 60, 3600), every(5, "s01", 60, 3600));
        assert_eq!(a, b);
        assert_ne!(a, every(5, "s02", 60, 3600));
        assert!(a.len() > 40 && a.windows(2).all(|p| p[1] > p[0]));
    }

    #[test]
    fn the_junk_has_the_shape_it_claims() {
        let mut rng = Rng::new(3);
        for _ in 0..500 {
            let s = stun_packet(&mut rng);
            assert!(s.len() >= 20 && s[..2] == [0, 1] && s[4..8] == [0x21, 0x12, 0xA4, 0x42]);
            let d = dtls_packet(&mut rng);
            assert!(d.len() >= 13 && d[1..3] == [0xFE, 0xFD]);
            let u = udp_noise(&mut rng);
            assert!(!u.is_empty() && u.len() <= 1500);
        }
    }

    // The attacks against a real rtcd in this process, with a lesson's participants in the call.
    use std::sync::OnceLock;

    use crate::stats::Probe;

    const SEED: u64 = 1234;

    fn assets() -> Option<Arc<Assets>> {
        static ASSETS: OnceLock<Option<Arc<Assets>>> = OnceLock::new();
        ASSETS
            .get_or_init(|| {
                // Where the binary keeps them: made once, then reused.
                Assets::load(&std::env::temp_dir().join("lms-sim-assets"))
                    .ok()
                    .map(Arc::new)
            })
            .clone()
    }

    fn lines(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect()
    }

    async fn wait_joins(path: &std::path::Path, name: &str, n: usize) -> bool {
        for _ in 0..200 {
            let joins = lines(path)
                .iter()
                .filter(|l| l["kind"] == "event" && l["id"] == name && l["what"] == "join")
                .count();
            if joins >= n {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    struct World {
        rtcd: Arc<Rtcd>,
        assets: Arc<Assets>,
        env: Env,
        sink: Arc<Sink>,
        path: PathBuf,
        lesson: Vec<Participant>,
    }

    impl World {
        fn probe(&self, measure: bool) -> Probe {
            Probe {
                sink: self.sink.clone(),
                imp: "rtcd-rs".into(),
                group: "clean",
                teacher: "session-teacher".into(),
                measure,
                leader: false,
            }
        }

        /// rtcd still serves someone new: a fresh participant connects.
        async fn survives(&self, name: &str) {
            let spec = Spec {
                name: name.into(),
                watch: vec!["teacher".into()],
                probe: Some(self.probe(false)),
                ..Default::default()
            };
            let p = class::join(&self.rtcd, self.assets.clone(), "channel-lesson", spec, 40).await;
            assert!(
                wait_joins(&self.path, name, 1).await,
                "rtcd does not serve a new participant after the attack"
            );
            assert!(
                self.rtcd.metrics().contains("rtcd_"),
                "the metrics are not readable"
            );
            p.leave().await;
        }

        fn window(&self, id: &str, class: &str, who: &[u32], ms: u64) -> Window {
            Window {
                id: id.into(),
                class: Class::parse(class).unwrap(),
                start_ms: 0,
                len_ms: ms,
                who: who.to_vec(),
                level: 5,
                salt: SEED ^ u64::from(who.first().copied().unwrap_or(0)),
            }
        }

        /// Runs the windows together; each must have fired and written its rows.
        async fn attack(&self, windows: &[Window]) {
            let env = &self.env;
            let done = futures_util::future::join_all(
                windows
                    .iter()
                    .map(|w| async move { (w, run_window(env, w).await) }),
            )
            .await;
            let rows = lines(&self.path);
            for (w, n) in done {
                assert!(n >= 1, "{} did nothing", w.class.name());
                for state in ["start", "end"] {
                    let row = rows
                        .iter()
                        .find(|r| {
                            r["kind"] == "attack" && r["id"] == w.id.as_str() && r["state"] == state
                        })
                        .unwrap_or_else(|| panic!("no {state} row for {}", w.class.name()));
                    assert_eq!(
                        (&row["impl"], &row["name"]),
                        (&json!("abuse"), &json!(w.class.name()))
                    );
                    assert_eq!((&row["group"], &row["seed"]), (&json!("all"), &json!(SEED)));
                    assert_eq!(row["who"].is_array(), w.class.pool() != Pool::None);
                }
            }
            eprintln!(
                "fired {:?}, sessions {}",
                self.env.fired(),
                self.rtcd.sessions()
            );
            assert!(self.env.open.lock().unwrap().is_empty());
        }

        async fn end(self) {
            // The lesson's participants were never thrown out of the call, nor lost the media.
            for l in lines(&self.path) {
                assert!(
                    !(l["kind"] == "event"
                        && ["error", "rejoin"].contains(&l["what"].as_str().unwrap_or(""))),
                    "the attacks cost {} its seat: {l}",
                    l["id"]
                );
            }
            self.env.finish().await;
            for p in self.lesson {
                p.leave().await;
            }
            self.rtcd.metrics();
        }
    }

    /// An rtcd with a teacher and one student of a lesson in a call, `nx` churners and `nh`
    /// hostile peers (`None` when ffmpeg is missing).
    async fn world(name: &str, nx: u32, nh: u32) -> Option<World> {
        let assets = assets()?;
        let path =
            std::env::temp_dir().join(format!("lms-sim-abuse-{}-{name}.jsonl", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let sink = Sink::open(&path).unwrap();
        let rtcd = Arc::new(Rtcd::start().await);
        let rows = Rows::new(sink.clone(), "all", SEED, "rtcd-rs");
        rows.seed_event("test");
        let mut w = World {
            rtcd: rtcd.clone(),
            assets: assets.clone(),
            env: Env::start(rtcd.clone(), assets.clone(), "channel-lesson", rows, nx, nh).await,
            sink,
            path,
            lesson: Vec::new(),
        };
        let teacher = Spec {
            name: "teacher".into(),
            voice: true,
            camera: true,
            screen: true,
            wrap: true,
            probe: Some(w.probe(false)),
            ..Default::default()
        };
        let student = Spec {
            name: "s00".into(),
            watch: vec!["teacher".into()],
            wrap: true,
            probe: Some(w.probe(true)),
            ..Default::default()
        };
        w.lesson
            .push(class::join(&rtcd, assets.clone(), "channel-lesson", teacher, 0).await);
        w.lesson
            .push(class::join(&rtcd, assets, "channel-lesson", student, 1).await);
        assert!(wait_joins(&w.path, "teacher", 1).await && wait_joins(&w.path, "s00", 1).await);
        Some(w)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn churn_fires_and_rtcd_serves_on() {
        let Some(w) = world("churn", 6, 0).await else {
            return;
        };
        let ws: Vec<Window> = [
            ("join-leave", 0),
            ("camera-toggle", 1),
            ("mic-toggle", 1),
            ("screen-toggle", 2),
            ("layer-flap", 2),
            ("renego-storm", 3),
            ("glare", 4),
            ("late-answer", 5),
        ]
        .iter()
        .enumerate()
        .map(|(i, (c, x))| w.window(&format!("churn-{i}"), c, &[*x], 4000))
        .collect();
        w.attack(&ws).await;
        w.survives("s09").await;
        w.end().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn hostile_peers_fire_and_rtcd_serves_on() {
        let Some(w) = world("hostile", 0, 3).await else {
            return;
        };
        // Three at a time, one on each peer: a peer has one thing to do at once.
        for (n, batch) in [
            ["rtp-jump", "pli-flood", "dc-flood"],
            ["ssrc-change", "nack-flood", "dc-oversize"],
            ["rtp-size", "bogus-rtcp", "rtcp-garbage"],
            ["nat-rebind", "mtu", "rtp-jump"],
        ]
        .iter()
        .enumerate()
        {
            let ws: Vec<Window> = batch
                .iter()
                .enumerate()
                .map(|(i, c)| w.window(&format!("hostile-{n}-{i}"), c, &[i as u32], 2500))
                .collect();
            w.attack(&ws).await;
        }
        w.survives("s09").await;
        w.end().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn junk_and_signaling_fire_and_rtcd_serves_on() {
        let Some(w) = world("junk", 2, 0).await else {
            return;
        };
        let ws: Vec<Window> = [
            "udp-junk",
            "stun-flood",
            "dtls-junk",
            "ws-storm",
            "dup-join",
            "unknown-call",
        ]
        .iter()
        .enumerate()
        .map(|(i, c)| w.window(&format!("junk-{i}"), c, &[], 3000))
        .collect();
        w.attack(&ws).await;
        w.survives("s09").await;
        w.end().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_lesson_attacks_fire_and_rtcd_serves_on() {
        let Some(mut w) = world("lesson", 0, 0).await else {
            return;
        };
        let tally = Arc::new(AtomicU64::new(0));
        let rows = Rows::new(w.sink.clone(), "impaired", SEED, "rtcd-rs");
        let mut at = LessonAttacks::new(
            rows,
            &[("s00".into(), 1)],
            Some(3600),
            Some(60),
            true,
            true,
            60,
        );
        at.settle = Duration::from_secs(1);
        at.nat[0].2 = VecDeque::from([0]);
        at.reconnects = VecDeque::from([0]);
        let mut people: Vec<(u32, Participant)> = Vec::new();
        let mut lesson = std::mem::take(&mut w.lesson).into_iter();
        people.push((u32::MAX, lesson.next().unwrap()));
        people.push((1, lesson.next().unwrap()));
        let spec = Spec {
            name: "teacher".into(),
            voice: true,
            camera: true,
            screen: true,
            wrap: true,
            probe: Some(w.probe(false)),
            tally: Some(tally),
            ..Default::default()
        };
        for _ in 0..4 {
            at.tick(&people);
            if at.teacher_due() {
                let (_, old) = people.remove(0);
                let back = at
                    .teacher_reconnect(
                        &w.rtcd,
                        w.assets.clone(),
                        "channel-lesson",
                        spec.clone(),
                        old,
                    )
                    .await;
                people.insert(0, (u32::MAX, back));
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        assert!(
            wait_joins(&w.path, "teacher", 2).await,
            "the teacher did not come back"
        );
        tokio::time::sleep(Duration::from_secs(2)).await;
        at.tick(&people);
        at.finish();
        let rows = lines(&w.path);
        for class in ["teacher-reconnect", "nat-rebind", "mtu"] {
            for state in ["start", "end"] {
                let row = rows
                    .iter()
                    .find(|r| r["kind"] == "attack" && r["name"] == class && r["state"] == state)
                    .unwrap_or_else(|| panic!("no {state} row for {class}"));
                assert_eq!(
                    (&row["group"], &row["seed"]),
                    (&json!("impaired"), &json!(SEED))
                );
                assert_eq!(
                    row["who"][0],
                    if class == "teacher-reconnect" {
                        "teacher"
                    } else {
                        "s00"
                    }
                );
            }
        }
        w.survives("s09").await;
        for (_, p) in people {
            p.leave().await;
        }
        w.env.finish().await;
    }
}
