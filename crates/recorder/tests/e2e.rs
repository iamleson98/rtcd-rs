//! The recorder against a real rtcd, end to end: a class of synthetic participants sends real VP8 and
//! Opus, and what the recorder uploads is checked with ffprobe.
//!
//! The media is made with ffmpeg the first time; without it these tests do nothing.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lms_sim::assets::Assets;
use lms_sim::class::{self, Ctl, Participant, Spec};
use lms_sim::host::{Impair, Rtcd, RtcdConn};
use lms_sim::lms::{CHAN_ID, Lms, REC_ID};
use recorder::api::Api;
use recorder::config::Config;
use recorder::session::{RunError, run};
use rtcd_proto::{ClientMessage, RecordEnded, RecordStart, RecordStop};
use serde_json::{Value, json};
use tokio_util::sync::CancellationToken;

fn ffmpeg_available() -> bool {
    Command::new("ffmpeg")
        .arg("-version")
        .output()
        .is_ok_and(|o| o.status.success())
        && Command::new("ffprobe")
            .arg("-version")
            .output()
            .is_ok_and(|o| o.status.success())
}

fn assets() -> Option<Arc<Assets>> {
    if !ffmpeg_available() {
        eprintln!("ffmpeg is not installed: skipped");
        return None;
    }
    // One set of files for every test run on this machine, made by whichever test gets there first.
    static ONCE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = ONCE.lock().unwrap_or_else(|p| p.into_inner());
    let dir = std::env::temp_dir().join("rtcd-recorder-e2e-assets");
    Some(Arc::new(Assets::load(&dir).expect("the media is made")))
}

struct Class {
    rtcd: Arc<Rtcd>,
    lms: Lms,
    assets: Arc<Assets>,
    people: Vec<Participant>,
    dir: PathBuf,
    n: u32,
}

impl Class {
    async fn start(tag: &str, assets: Arc<Assets>) -> Self {
        Self::start_impaired(tag, assets, Impair::default()).await
    }

    async fn start_impaired(tag: &str, assets: Arc<Assets>, impair: Impair) -> Self {
        Self::start_on(tag, assets, impair, false).await
    }

    /// A class on an rtcd that can record calls itself.
    async fn start_builtin(tag: &str, assets: Arc<Assets>) -> Self {
        Self::start_on(tag, assets, Impair::default(), true).await
    }

    async fn start_on(tag: &str, assets: Arc<Assets>, impair: Impair, builtin: bool) -> Self {
        let dir = std::env::temp_dir().join(format!("recorder-e2e-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let data = dir.join("rtcd-data").display().to_string();
        let rtcd = Arc::new(
            Rtcd::start_configured(impair, move |cfg| {
                cfg.record.enabled = builtin;
                cfg.record.data_dir = data;
            })
            .await,
        );
        let lms = Lms::start(Some(rtcd.clone()), dir.join("lms")).await;
        Self {
            rtcd,
            lms,
            assets,
            people: Vec::new(),
            dir,
            n: 0,
        }
    }

    async fn join(&mut self, spec: Spec) {
        let p = class::join(&self.rtcd, self.assets.clone(), CHAN_ID, spec, self.n).await;
        self.n += 1;
        let want = self.people.len() + 1;
        let end = Instant::now() + Duration::from_secs(20);
        while self.rtcd.sessions() < want && Instant::now() < end {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.lms.person_joined(&p.session_id, &p.user_id);
        self.people.push(p);
    }

    fn config(&self) -> Config {
        Config {
            site_url: self.lms.url.clone(),
            auth_token: "bot-token".into(),
            recording_id: REC_ID.into(),
            max_duration: Duration::from_secs(300),
            max_data: 4 << 30,
        }
    }

    /// Starts the recorder, which runs until the returned handle is awaited.
    fn record(&self) -> tokio::task::JoinHandle<Result<(), RunError>> {
        let cfg = self.config();
        let api = Api::new(&cfg);
        let dir = self.dir.join("data");
        std::fs::create_dir_all(&dir).unwrap();
        tokio::spawn(async move { run(CancellationToken::new(), cfg, api, &dir).await })
    }

    /// Waits until the recorder is in the call, with everyone else.
    async fn recorder_in(&self) {
        let want = self.people.len() + 1;
        let end = Instant::now() + Duration::from_secs(40);
        while self.rtcd.sessions() < want && Instant::now() < end {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(
            self.rtcd.sessions() >= want,
            "the recorder never joined the call"
        );
        // And its media is flowing.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    fn stop(&self) {
        self.lms.emit(
            "custom_lms_recording_stop",
            "",
            json!({ "recording_id": REC_ID }),
        );
    }

    /// Asks rtcd to record the class's call, numbering files from `file_base`.
    fn builtin_start(&self, control: &RtcdConn, file_base: u32) {
        control.send(ClientMessage::RecordStart(RecordStart {
            recording_id: REC_ID.into(),
            call_id: CHAN_ID.into(),
            t0: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
            file_base,
            site_url: self.lms.url.clone(),
            token: "bot-token".into(),
        }));
    }

    fn builtin_stop(control: &RtcdConn) {
        control.send(ClientMessage::RecordStop(RecordStop {
            recording_id: REC_ID.into(),
        }));
    }

    /// Waits for rtcd to say its recording ended, and returns what it reported, in order: the
    /// files, then the end. The files are those stored in the fake LMS.
    async fn builtin_report(&self) -> (Vec<Entry>, RecordEnded) {
        let end = Instant::now() + Duration::from_secs(60);
        loop {
            let all = self.rtcd.reports();
            if let Some(at) = all
                .iter()
                .position(|m| matches!(m, ClientMessage::RecordEnded(_)))
            {
                let ClientMessage::RecordEnded(ended) = all[at].clone() else {
                    unreachable!()
                };
                assert_eq!(at, all.len() - 1, "nothing is reported after the end");
                let files = all[..at]
                    .iter()
                    .map(|m| {
                        let ClientMessage::RecordFile(f) = m else {
                            panic!("only files come before the end: {m:?}")
                        };
                        Entry {
                            kind: f.kind.clone(),
                            session: f.session_id.clone(),
                            user: f.user_id.clone(),
                            start_ms: f.start_ms,
                            end_ms: f.end_ms,
                            width: u64::from(f.width),
                            height: u64::from(f.height),
                            size: f.size,
                            path: self
                                .lms
                                .read(|s| s.files.get(&f.index.to_string()).cloned())
                                .unwrap_or_else(|| panic!("file {} was not stored", f.index)),
                        }
                    })
                    .collect();
                return (files, ended);
            }
            assert!(Instant::now() < end, "rtcd never said the recording ended");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// The manifest the recorder completed with, and where each uploaded file is.
    fn manifest(&self) -> Vec<Entry> {
        let done = self
            .lms
            .read(|s| s.complete.clone())
            .expect("the recording was completed");
        done["files"]
            .as_array()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, f)| Entry {
                kind: f["kind"].as_str().unwrap().to_owned(),
                session: f["session_id"].as_str().unwrap_or("").to_owned(),
                user: f["user_id"].as_str().unwrap_or("").to_owned(),
                start_ms: f["start_ms"].as_i64().unwrap(),
                end_ms: f["end_ms"].as_i64().unwrap(),
                width: f["width"].as_u64().unwrap_or(0),
                height: f["height"].as_u64().unwrap_or(0),
                size: f["size"].as_u64().unwrap(),
                path: self
                    .lms
                    .read(|s| s.files.get(&i.to_string()).cloned())
                    .expect("the file was uploaded"),
            })
            .collect()
    }

    async fn finish(self) {
        for p in self.people {
            p.leave().await;
        }
        if let Ok(rtcd) = Arc::try_unwrap(self.rtcd) {
            rtcd.shutdown().await;
        }
        let _ = std::fs::remove_dir_all(self.dir);
    }
}

#[derive(Debug)]
struct Entry {
    kind: String,
    session: String,
    user: String,
    start_ms: i64,
    end_ms: i64,
    width: u64,
    height: u64,
    size: u64,
    path: PathBuf,
}

struct Probe {
    codec: String,
    width: u64,
    height: u64,
    frames: u64,
    duration: f64,
}

/// What ffprobe sees in a file, and whether ffmpeg decodes all of it without an error.
fn probe(e: &Entry) -> Probe {
    assert_eq!(
        std::fs::metadata(&e.path).unwrap().len(),
        e.size,
        "{}: the uploaded size is the manifest's",
        e.kind
    );
    let out = Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-count_packets",
            "-show_entries",
            "format=duration:stream=codec_name,width,height,nb_read_packets",
            "-of",
            "json",
        ])
        .arg(&e.path)
        .output()
        .unwrap();
    assert!(
        out.stderr.is_empty(),
        "{:?}: ffprobe says {}",
        e,
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    let st = &v["streams"][0];
    let decode = Command::new("ffmpeg")
        .args(["-v", "error", "-i"])
        .arg(&e.path)
        .args(["-f", "null", "-"])
        .output()
        .unwrap();
    assert!(
        decode.stderr.is_empty(),
        "{:?}: ffmpeg says {}",
        e,
        String::from_utf8_lossy(&decode.stderr)
    );
    Probe {
        codec: st["codec_name"].as_str().unwrap_or("").to_owned(),
        width: st["width"].as_u64().unwrap_or(0),
        height: st["height"].as_u64().unwrap_or(0),
        frames: st["nb_read_packets"]
            .as_str()
            .and_then(|n| n.parse().ok())
            .unwrap_or(0),
        duration: v["format"]["duration"]
            .as_str()
            .and_then(|n| n.parse().ok())
            .unwrap_or(0.0),
    }
}

fn of_kind<'a>(m: &'a [Entry], kind: &str) -> Vec<&'a Entry> {
    m.iter().filter(|e| e.kind == kind).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_class_is_recorded_whole() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start("whole", assets).await;
    c.join(Spec {
        name: "p0".into(),
        voice: true,
        camera: true,
        screen: true,
    })
    .await;
    c.join(Spec {
        name: "p1".into(),
        voice: true,
        camera: true,
        screen: false,
    })
    .await;
    c.join(Spec {
        name: "p2".into(),
        voice: true,
        camera: false,
        screen: false,
    })
    .await;
    let rec = c.record();
    c.recorder_in().await;
    tokio::time::sleep(Duration::from_secs(8)).await;
    c.stop();
    rec.await.unwrap().expect("the recording completes");

    let m = c.manifest();
    assert_eq!(of_kind(&m, "voice").len(), 3, "a voice per person: {m:#?}");
    assert_eq!(of_kind(&m, "camera").len(), 2);
    assert_eq!(of_kind(&m, "screen").len(), 1);
    let duration = c.lms.read(|s| {
        s.complete.as_ref().unwrap()["duration_ms"]
            .as_i64()
            .unwrap()
    });
    assert!((9_000..30_000).contains(&duration), "{duration}");
    for e in &m {
        assert!(
            e.start_ms >= 0 && e.end_ms > e.start_ms && e.end_ms <= duration + 500,
            "{e:?} within the recording ({duration} ms)"
        );
        assert!(
            e.session.starts_with("session-p") && e.user == e.session.replace("session-", "user-"),
            "{e:?}: whose it is"
        );
        let p = probe(e);
        let seconds = (e.end_ms - e.start_ms) as f64 / 1000.0;
        assert!(
            (p.duration - seconds).abs() < 0.5,
            "{e:?}: the file lasts {} s, the manifest says {seconds}",
            p.duration
        );
        match e.kind.as_str() {
            "voice" => {
                assert_eq!(p.codec, "opus");
                assert!(
                    p.frames as f64 >= seconds * 45.0,
                    "{e:?}: {} voice frames in {seconds} s",
                    p.frames
                );
            }
            "camera" => {
                assert_eq!(
                    (p.codec.as_str(), p.width, p.height, e.width, e.height),
                    ("vp8", 640, 360, 640, 360)
                );
                assert!(
                    p.frames as f64 >= seconds * 25.0,
                    "{e:?}: {} frames in {seconds} s",
                    p.frames
                );
            }
            "screen" => {
                assert_eq!((p.codec.as_str(), p.width, p.height), ("vp8", 1280, 720));
                assert!(
                    p.frames as f64 >= seconds * 12.0,
                    "{e:?}: {} frames in {seconds} s",
                    p.frames
                );
            }
            _ => unreachable!(),
        }
    }
    assert!(
        c.lms
            .has(&format!("POST {}/started", lms_sim::lms::REC_PATH))
    );
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_muted_microphone_splits_the_voice_file() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start("mute", assets).await;
    c.join(Spec {
        name: "p0".into(),
        voice: true,
        camera: false,
        screen: false,
    })
    .await;
    c.join(Spec {
        name: "p1".into(),
        voice: true,
        camera: false,
        screen: false,
    })
    .await;
    let rec = c.record();
    c.recorder_in().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    c.people[0].control(Ctl::Voice(false));
    tokio::time::sleep(Duration::from_secs(5)).await;
    c.people[0].control(Ctl::Voice(true));
    tokio::time::sleep(Duration::from_secs(3)).await;
    c.stop();
    rec.await.unwrap().expect("the recording completes");

    let m = c.manifest();
    let mut p0: Vec<_> = of_kind(&m, "voice")
        .into_iter()
        .filter(|e| e.session == "session-p0")
        .collect();
    p0.sort_by_key(|e| e.start_ms);
    let p1 = of_kind(&m, "voice")
        .into_iter()
        .filter(|e| e.session == "session-p1")
        .count();
    assert_eq!(p0.len(), 2, "the muted stretch splits the file: {m:#?}");
    assert_eq!(p1, 1, "the other voice is one file");
    let hole = p0[1].start_ms - p0[0].end_ms;
    assert!(
        (3_000..7_000).contains(&hole),
        "the hole is the muted stretch (about 5 s), not bridged: {hole} ms"
    );
    for e in p0 {
        probe(e);
    }
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_camera_turned_off_and_on_makes_two_files() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start("camera", assets).await;
    c.join(Spec {
        name: "p0".into(),
        voice: true,
        camera: true,
        screen: false,
    })
    .await;
    let rec = c.record();
    c.recorder_in().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    c.people[0].control(Ctl::Camera(false));
    c.lms.emit(
        "custom_calls_user_video_off",
        CHAN_ID,
        json!({ "userID": c.people[0].user_id, "session_id": c.people[0].session_id }),
    );
    tokio::time::sleep(Duration::from_secs(4)).await;
    c.people[0].control(Ctl::Camera(true));
    tokio::time::sleep(Duration::from_secs(5)).await;
    c.stop();
    rec.await.unwrap().expect("the recording completes");

    let m = c.manifest();
    let mut cams = of_kind(&m, "camera");
    cams.sort_by_key(|e| e.start_ms);
    assert_eq!(cams.len(), 2, "{m:#?}");
    assert!(
        cams[1].start_ms - cams[0].end_ms >= 3_000,
        "the file starts again where the camera did"
    );
    for e in cams {
        let p = probe(e);
        assert_eq!(p.codec, "vp8");
    }
    c.finish().await;
}

/// A shared screen that does not change sends nothing for seconds: the replay must still show it.
#[tokio::test(flavor = "multi_thread")]
async fn a_still_screen_share_is_one_file_and_ends_when_the_share_does() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start("still-screen", assets).await;
    c.join(Spec {
        name: "p0".into(),
        voice: false,
        camera: false,
        screen: true,
    })
    .await;
    let rec = c.record();
    c.recorder_in().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Nothing is sent for 6 s, longer than a voice is waited for.
    c.people[0].control(Ctl::Screen(false));
    tokio::time::sleep(Duration::from_secs(6)).await;
    c.people[0].control(Ctl::Screen(true));
    tokio::time::sleep(Duration::from_secs(3)).await;
    c.people[0].control(Ctl::Screen(false));
    c.lms.emit(
        "custom_calls_user_screen_off",
        CHAN_ID,
        json!({ "userID": c.people[0].user_id, "session_id": c.people[0].session_id }),
    );
    tokio::time::sleep(Duration::from_secs(3)).await;
    c.stop();
    rec.await.unwrap().expect("the recording completes");

    let m = c.manifest();
    let screens = of_kind(&m, "screen");
    assert_eq!(
        screens.len(),
        1,
        "the silence must not split the share: {m:#?}"
    );
    let s = screens[0];
    let p = probe(s);
    assert_eq!(p.codec, "vp8");
    // About 12 s of share: 3 s, 6 s of silence, 3 s; the picture is held to where it ended and
    // not to the end of the recording.
    let span = s.end_ms - s.start_ms;
    assert!((11_000..=14_500).contains(&span), "{span} ms: {m:#?}");
    c.finish().await;
}

/// The value of a metric line in Prometheus text, summed over its labels.
fn metric(text: &str, name: &str) -> f64 {
    text.lines()
        .filter(|l| l.starts_with(name) && l[name.len()..].starts_with([' ', '{']))
        .filter_map(|l| l.rsplit(' ').next()?.parse::<f64>().ok())
        .sum()
}

/// 5 % of the packets lost in each direction, 40 ms each way with 30 ms of jitter (so packets
/// overtake one another): the SFU asks the senders for what it is missing, the recorder asks the
/// SFU, and the files come out as if the network were fine.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_network_costs_the_recording_nothing_it_can_get_back() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start_impaired(
        "bad-network",
        assets,
        Impair {
            down_loss: 0.05,
            up_loss: 0.05,
            delay_ms: 40,
            jitter_ms: 30,
            ..Default::default()
        },
    )
    .await;
    for (name, camera, screen) in [("p0", true, true), ("p1", true, false)] {
        c.join(Spec {
            name: name.into(),
            voice: true,
            camera,
            screen,
        })
        .await;
    }
    let rec = c.record();
    c.recorder_in().await;
    tokio::time::sleep(Duration::from_secs(14)).await;
    let metrics = c.rtcd.metrics();
    c.stop();
    rec.await.unwrap().expect("the recording completes");

    let m = c.manifest();
    for e in &m {
        let p = probe(e);
        let seconds = (e.end_ms - e.start_ms) as f64 / 1000.0;
        let (fps, floor) = match e.kind.as_str() {
            "voice" => (50.0, 0.95),
            "camera" => (30.0, 0.85),
            "screen" => (15.0, 0.85),
            _ => unreachable!(),
        };
        // Written frames over the frames of the time the file covers. The recorder's silence
        // bridging counts for voice; a video frame lost for good is skipped until a keyframe.
        let got = p.frames as f64 / (seconds * fps);
        eprintln!("{} {:.0} %", e.kind, got * 100.0);
        assert!(
            got >= floor,
            "{e:?}: {} frames in {seconds} s is {:.0} % of what was sent",
            p.frames,
            got * 100.0
        );
    }
    // The SFU saw the loss, and its metrics say so.
    assert!(
        metric(&metrics, "rtcd_rtc_receiver_feedback_total") > 0.0,
        "receivers asked for packets again"
    );
    assert!(metric(&metrics, "rtcd_rtc_receiver_rtt_count") > 0.0);
    assert!(metric(&metrics, "rtcd_rtc_bandwidth_estimate_bps_count") > 0.0);
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_recording_ends_when_the_call_does() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start("end", assets).await;
    c.join(Spec {
        name: "p0".into(),
        voice: true,
        camera: true,
        screen: false,
    })
    .await;
    c.join(Spec {
        name: "p1".into(),
        voice: true,
        camera: false,
        screen: false,
    })
    .await;
    let rec = c.record();
    c.recorder_in().await;
    tokio::time::sleep(Duration::from_secs(3)).await;
    // Everyone leaves: the server tells the room, and the call ends.
    let sessions: Vec<String> = c.people.iter().map(|p| p.session_id.clone()).collect();
    for s in &sessions {
        c.lms.person_left(s);
    }
    let result = tokio::time::timeout(Duration::from_secs(60), rec)
        .await
        .expect("it ends by itself")
        .unwrap();
    result.expect("the recording completes");
    let m = c.manifest();
    assert_eq!(of_kind(&m, "voice").len(), 2);
    assert_eq!(of_kind(&m, "camera").len(), 1);
    c.finish().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_realtime_connection_costs_nothing() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start("drop", assets).await;
    c.join(Spec {
        name: "p0".into(),
        voice: true,
        camera: true,
        screen: false,
    })
    .await;
    let rec = c.record();
    c.recorder_in().await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    c.lms.drop_sockets();
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert!(c.lms.read(|s| s.sockets) >= 2, "the recorder reconnected");
    c.stop();
    rec.await.unwrap().expect("the recording completes");

    let m = c.manifest();
    assert_eq!(
        of_kind(&m, "voice").len(),
        1,
        "the media never noticed: {m:#?}"
    );
    assert_eq!(of_kind(&m, "camera").len(), 1);
    let e = of_kind(&m, "voice")[0];
    assert!(e.end_ms - e.start_ms > 6_000, "{e:?}");
    let joins = c
        .lms
        .actions()
        .iter()
        .filter(|a| *a == "custom_calls_join")
        .count();
    assert_eq!(
        joins,
        1,
        "the recorder stayed in the call: {:?}",
        c.lms.actions()
    );
    c.finish().await;
}

fn full(name: &str) -> Spec {
    Spec {
        name: name.into(),
        voice: true,
        camera: true,
        screen: true,
    }
}

/// Joins the class `people` and holds everything they send back until `release` is called.
async fn quiet_class(c: &mut Class) {
    for spec in [
        full("p0"),
        Spec {
            screen: false,
            ..full("p1")
        },
        Spec {
            camera: false,
            screen: false,
            ..full("p2")
        },
    ] {
        c.join(spec).await;
    }
    for p in &c.people {
        p.control(Ctl::Voice(false));
        p.control(Ctl::Camera(false));
        p.control(Ctl::Screen(false));
    }
}

fn media(c: &Class, on: bool) {
    for p in &c.people {
        p.control(Ctl::Voice(on));
        p.control(Ctl::Camera(on));
        p.control(Ctl::Screen(on));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn rtcd_records_a_class_by_itself_and_reports_every_file_then_the_end() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start_builtin("builtin", assets).await;
    quiet_class(&mut c).await;
    let control = c.rtcd.connect().await;
    c.builtin_start(&control, 300);
    tokio::time::sleep(Duration::from_secs(1)).await;
    media(&c, true);
    tokio::time::sleep(Duration::from_secs(8)).await;
    Class::builtin_stop(&control);
    let (m, ended) = c.builtin_report().await;

    assert_eq!(of_kind(&m, "voice").len(), 3, "a voice per person: {m:#?}");
    assert_eq!(of_kind(&m, "camera").len(), 2);
    assert_eq!(of_kind(&m, "screen").len(), 1);
    assert_eq!(
        (ended.reason.as_str(), ended.file_base, ended.files as usize),
        ("stopped", 300, m.len()),
        "the end counts the files reported before it"
    );
    assert!(
        (8_000..16_000).contains(&ended.duration_ms),
        "{}",
        ended.duration_ms
    );
    for e in &m {
        assert!(
            e.start_ms >= 0 && e.end_ms > e.start_ms && e.end_ms <= ended.duration_ms + 500,
            "{e:?} within the recording ({} ms)",
            ended.duration_ms
        );
        assert!(
            e.session.starts_with("session-p") && e.user == e.session.replace("session-", "user-"),
            "{e:?}: whose it is"
        );
        let p = probe(e);
        let seconds = (e.end_ms - e.start_ms) as f64 / 1000.0;
        assert!(
            (p.duration - seconds).abs() < 0.5,
            "{e:?}: the file lasts {} s, the manifest says {seconds}",
            p.duration
        );
        match e.kind.as_str() {
            "voice" => assert_eq!(p.codec, "opus"),
            "camera" => assert_eq!(
                (p.codec.as_str(), p.width, p.height, e.width, e.height),
                ("vp8", 640, 360, 640, 360)
            ),
            "screen" => assert_eq!((p.codec.as_str(), p.width, p.height), ("vp8", 1280, 720)),
            other => panic!("{other}"),
        }
    }
    c.finish().await;
}

/// The bot and rtcd record the same class at the same time; they must write the same files.
#[tokio::test(flavor = "multi_thread")]
async fn the_bot_and_rtcd_write_the_same_files_for_the_same_class() {
    let Some(assets) = assets() else { return };
    let mut c = Class::start_builtin("parity", assets).await;
    quiet_class(&mut c).await;
    let rec = c.record();
    c.recorder_in().await;
    let control = c.rtcd.connect().await;
    c.builtin_start(&control, 1000);
    tokio::time::sleep(Duration::from_secs(1)).await;
    // Both are in place: from here they see the same packets.
    media(&c, true);
    tokio::time::sleep(Duration::from_secs(8)).await;
    media(&c, false);
    // Long enough for voice and cameras to be taken for ended (a screen is held until the stop).
    tokio::time::sleep(Duration::from_secs(7)).await;
    c.stop();
    Class::builtin_stop(&control);
    rec.await.unwrap().expect("the bot's recording completes");
    let bot = c.manifest();
    let (built_in, _) = c.builtin_report().await;

    // What each file is, and how long it lasts.
    let shape = |m: &[Entry]| -> Vec<(String, String, String, i64)> {
        let mut v: Vec<_> = m
            .iter()
            .map(|e| {
                (
                    e.kind.clone(),
                    e.session.clone(),
                    e.user.clone(),
                    e.end_ms - e.start_ms,
                )
            })
            .collect();
        v.sort();
        v
    };
    let (a, b) = (shape(&bot), shape(&built_in));
    let names = |v: &[(String, String, String, i64)]| -> Vec<String> {
        v.iter()
            .map(|(k, s, u, _)| format!("{k} {s} {u}"))
            .collect()
    };
    assert_eq!(
        names(&a),
        names(&b),
        "the same files, kinds and owners\nbot: {a:#?}\nrtcd: {b:#?}"
    );
    for ((kind, session, _, bot_ms), (_, _, _, rtcd_ms)) in a.iter().zip(&b) {
        // A frame at 30 fps is 33 ms; a screen is held to the stop, which each path sees a little
        // apart.
        let allowed = if kind == "screen" { 400 } else { 66 };
        assert!(
            (bot_ms - rtcd_ms).abs() <= allowed,
            "{kind} {session}: the bot's file lasts {bot_ms} ms, rtcd's {rtcd_ms} ms"
        );
    }
    // And each decodes, with the same number of frames.
    let by_key = |m: &'_ [Entry], e: &Entry| -> Option<usize> {
        m.iter()
            .position(|x| x.kind == e.kind && x.session == e.session)
    };
    for e in &built_in {
        let theirs = &bot[by_key(&bot, e).expect("the bot has this file")];
        let (mine, other) = (probe(e), probe(theirs));
        assert_eq!(
            (mine.codec.as_str(), mine.width, mine.height),
            (other.codec.as_str(), other.width, other.height),
            "{e:?}"
        );
        let apart = mine.frames.abs_diff(other.frames);
        assert!(
            apart <= 3,
            "{} {}: {} frames from rtcd, {} from the bot",
            e.kind,
            e.session,
            mine.frames,
            other.frames
        );
        assert!((mine.duration - other.duration).abs() < 0.4);
    }
    c.finish().await;
}
