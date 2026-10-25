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
use lms_sim::host::Rtcd;
use lms_sim::lms::{CHAN_ID, Lms, REC_ID};
use recorder::api::Api;
use recorder::config::Config;
use recorder::session::{RunError, run};
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
        let dir = std::env::temp_dir().join(format!("recorder-e2e-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let rtcd = Arc::new(Rtcd::start().await);
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
    assert_eq!(of_kind(&m, "board").len(), 1);
    assert_eq!(of_kind(&m, "voice").len(), 3, "a voice per person: {m:#?}");
    assert_eq!(of_kind(&m, "camera").len(), 2);
    assert_eq!(of_kind(&m, "screen").len(), 1);
    let duration = c.lms.read(|s| {
        s.complete.as_ref().unwrap()["duration_ms"]
            .as_i64()
            .unwrap()
    });
    assert!((9_000..30_000).contains(&duration), "{duration}");
    for e in m.iter().filter(|e| e.kind != "board") {
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
    // The board is the whole timeline.
    let board = of_kind(&m, "board")[0];
    assert_eq!((board.start_ms, board.end_ms), (0, duration));
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
