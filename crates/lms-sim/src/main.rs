//! `lms-sim run` has rtcd record a synthetic class and reports what the recording wrote and how much
//! of a core the writer threads used.
//!
//! ```text
//! lms-sim run --label builtin --duration 30 --people 30 --cameras 10 --screens 1
//! lms-sim soak --classes 40 --people 12 --cameras 4 --screens 1 --seconds 20 --record
//! ```
//!
//! `soak` runs class after class and reports the resident set after each, to find a leak.
//! `load` puts classes on an rtcd that runs elsewhere; `lesson` plays one real lesson on it for hours
//! and measures what the receivers see (see `lesson.rs`).

mod lesson;
mod load;
mod soak;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use lms_sim::assets::Assets;
use lms_sim::class::{self, Spec};
use lms_sim::host::{Impair, Rtcd};
use lms_sim::lms::{CHAN_ID, Lms, REC_ID};
use rtcd_proto::{ClientMessage, RecordStart, RecordStop};
use serde_json::{Value, json};

struct Args {
    label: String,
    duration: u64,
    people: u32,
    cameras: u32,
    screens: u32,
    assets: PathBuf,
    out: PathBuf,
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        label: "builtin".into(),
        duration: 30,
        people: 8,
        cameras: 6,
        screens: 1,
        assets: std::env::temp_dir().join("lms-sim-assets"),
        out: std::env::temp_dir().join("lms-sim-out"),
    };
    let mut it = std::env::args().skip(1);
    if it.next().as_deref() != Some("run") {
        return Err("usage: lms-sim run [--label NAME] [--duration S] [--people N] [--cameras N] [--screens N] [--assets DIR] [--out DIR]".into());
    }
    while let Some(k) = it.next() {
        let v = it.next().ok_or_else(|| format!("{k} needs a value"))?;
        let n = |v: &str| {
            v.parse::<u64>()
                .map_err(|_| format!("{k}: {v} is not a number"))
        };
        match k.as_str() {
            "--label" => a.label = v,
            "--duration" => a.duration = n(&v)?,
            "--people" => a.people = n(&v)? as u32,
            "--cameras" => a.cameras = n(&v)? as u32,
            "--screens" => a.screens = n(&v)? as u32,
            "--assets" => a.assets = v.into(),
            "--out" => a.out = v.into(),
            other => return Err(format!("unknown option {other}")),
        }
    }
    Ok(a)
}

fn ffprobe(path: &Path) -> Value {
    let out = std::process::Command::new("ffprobe")
        .args([
            "-v",
            "error",
            "-count_packets",
            "-show_entries",
            "format=duration,format_name:stream=codec_name,width,height,nb_read_packets",
            "-of",
            "json",
        ])
        .arg(path)
        .output();
    match out {
        Ok(o) => {
            let mut v: Value = serde_json::from_slice(&o.stdout).unwrap_or(Value::Null);
            v["stderr"] = String::from_utf8_lossy(&o.stderr).into_owned().into();
            v
        }
        Err(e) => json!({ "error": e.to_string() }),
    }
}

/// Where the media is: `LMS_SIM_ASSETS`, or a directory in the temporary one.
fn assets_dir() -> PathBuf {
    std::env::var("LMS_SIM_ASSETS")
        .map_or_else(|_| std::env::temp_dir().join("lms-sim-assets"), Into::into)
}

#[tokio::main]
async fn main() {
    if std::env::args().nth(1).as_deref() == Some("soak") {
        let assets = std::env::temp_dir().join("lms-sim-assets");
        soak::run(std::env::args().skip(2), assets).await;
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("lesson") {
        lesson::run(std::env::args().skip(2), assets_dir()).await;
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("assets") {
        // Makes the media (needs ffmpeg), for an image that has none.
        let dir = std::env::args().nth(2).expect("usage: lms-sim assets DIR");
        Assets::load(Path::new(&dir)).expect("the media is made (ffmpeg)");
        return;
    }
    if std::env::args().nth(1).as_deref() == Some("load") {
        load::run(std::env::args().skip(2), assets_dir()).await;
        return;
    }
    let a = match parse() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let report = run(a).await;
    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

/// The value of a counter or gauge in Prometheus text.
fn metric(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
        .unwrap_or(0.0)
}

/// Has rtcd record the class, and reports what it wrote and what the writer cost: the seconds its
/// threads spent writing (not waiting) over the length of the recording is the share of one core
/// the recording takes.
async fn run(a: Args) -> Value {
    let _ = std::fs::remove_dir_all(&a.out);
    std::fs::create_dir_all(&a.out).unwrap();
    let assets = Arc::new(Assets::load(&a.assets).expect("the media is made (ffmpeg)"));
    let recording_data = a.out.join("rtcd-data").display().to_string();
    let rtcd = Arc::new(
        Rtcd::start_configured(Impair::default(), move |cfg| {
            cfg.record.enabled = true;
            cfg.record.data_dir = recording_data;
        })
        .await,
    );
    let lms = Lms::start(a.out.join("lms")).await;

    // The class: everyone speaks, some have cameras, some share a screen.
    let mut people = Vec::new();
    for i in 0..a.people {
        let spec = Spec {
            name: format!("p{i}"),
            voice: true,
            camera: i < a.cameras,
            screen: i < a.screens,
            ..Default::default()
        };
        people.push(class::join(&rtcd, assets.clone(), CHAN_ID, spec, i).await);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let ready = Instant::now();
    while rtcd.sessions() < a.people as usize && ready.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let control = rtcd.connect().await;
    let busy_before = metric(&rtcd.metrics(), "rtcd_record_writer_busy_seconds_total");
    let started = Instant::now();
    control.send(ClientMessage::RecordStart(RecordStart {
        recording_id: REC_ID.into(),
        call_id: CHAN_ID.into(),
        t0: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64,
        file_base: 0,
        max_files: 0,
        site_url: lms.url.clone(),
        token: "bot-token".into(),
    }));
    tokio::time::sleep(Duration::from_secs(a.duration)).await;
    control.send(ClientMessage::RecordStop(RecordStop {
        recording_id: REC_ID.into(),
    }));
    let capture = started.elapsed();
    let busy = metric(&rtcd.metrics(), "rtcd_record_writer_busy_seconds_total") - busy_before;
    let wait = Instant::now();
    let ended = loop {
        let found = rtcd.reports().into_iter().find_map(|m| match m {
            ClientMessage::RecordEnded(e) => Some(e),
            _ => None,
        });
        if found.is_some() || wait.elapsed() > Duration::from_secs(120) {
            break found;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let mut files = Vec::new();
    for m in rtcd.reports() {
        if let ClientMessage::RecordFile(f) = m {
            let mut entry = json!({
                "index": f.index, "kind": f.kind, "session_id": f.session_id,
                "start_ms": f.start_ms, "end_ms": f.end_ms,
                "width": f.width, "height": f.height, "size": f.size,
            });
            if let Some(p) = lms.file(&f.index.to_string()) {
                entry["probe"] = ffprobe(&p);
            }
            files.push(entry);
        }
    }
    let text = rtcd.metrics();
    let report = json!({
        "label": a.label,
        "people": a.people, "cameras": a.cameras, "screens": a.screens, "duration_s": a.duration,
        "capture_s": capture.as_secs_f64(),
        "writer_busy_s": busy,
        "writer_core_percent": 100.0 * busy / capture.as_secs_f64().max(0.001),
        "uploaded_bytes": metric(&text, "rtcd_record_uploaded_bytes_total"),
        "upload_failures": metric(&text, "rtcd_record_upload_failures_total"),
        "writer_dropped_packets": metric(&text, "rtcd_record_writer_dropped_packets_total"),
        "ended": ended.map(|e| json!({
            "reason": e.reason, "file_base": e.file_base, "files": e.files,
            "duration_ms": e.duration_ms, "dropped": e.dropped,
        })),
        "files": files,
    });
    for p in people {
        p.leave().await;
    }
    report
}
