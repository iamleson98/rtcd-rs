//! `lms-sim run` records a synthetic class with a recorder binary and reports what it cost and what
//! it wrote, so two recorders can be compared on the same load. With `--builtin` rtcd records the
//! class itself, and the report says how much of a core its writer threads used.
//!
//! ```text
//! lms-sim run --recorder ./recorder --label rust --duration 30 --people 8 --cameras 6 --screens 1
//! lms-sim run --builtin --label builtin --duration 30 --people 30 --cameras 10 --screens 1
//! ```

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lms_sim::assets::Assets;
use lms_sim::class::{self, Spec};
use lms_sim::host::{Impair, Rtcd};
use lms_sim::lms::{CHAN_ID, Lms, REC_ID};
use rtcd_proto::{ClientMessage, RecordStart, RecordStop};
use serde_json::{Value, json};
use tokio::process::Command;

struct Args {
    recorder: PathBuf,
    /// Record with rtcd itself instead of a recorder binary.
    builtin: bool,
    label: String,
    duration: u64,
    people: u32,
    cameras: u32,
    screens: u32,
    assets: PathBuf,
    out: PathBuf,
    data_env: String,
    loss: f64,
    blackout: u64,
}

fn parse() -> Result<Args, String> {
    let mut a = Args {
        recorder: PathBuf::new(),
        builtin: false,
        label: "recorder".into(),
        duration: 30,
        people: 8,
        cameras: 6,
        screens: 1,
        assets: std::env::temp_dir().join("lms-sim-assets"),
        out: std::env::temp_dir().join("lms-sim-out"),
        data_env: "RECORDER_DATA_DIR".into(),
        loss: 0.0,
        blackout: 0,
    };
    let mut it = std::env::args().skip(1);
    if it.next().as_deref() != Some("run") {
        return Err("usage: lms-sim run (--recorder PATH | --builtin) [--label NAME] [--duration S] [--people N] [--cameras N] [--screens N] [--assets DIR] [--out DIR] [--data-env NAME] [--loss PERCENT] [--blackout-ms MS]".into());
    }
    while let Some(k) = it.next() {
        if k == "--builtin" {
            a.builtin = true;
            continue;
        }
        let v = it.next().ok_or_else(|| format!("{k} needs a value"))?;
        let n = |v: &str| {
            v.parse::<u64>()
                .map_err(|_| format!("{k}: {v} is not a number"))
        };
        match k.as_str() {
            "--recorder" => a.recorder = v.into(),
            "--label" => a.label = v,
            "--duration" => a.duration = n(&v)?,
            "--people" => a.people = n(&v)? as u32,
            "--cameras" => a.cameras = n(&v)? as u32,
            "--screens" => a.screens = n(&v)? as u32,
            "--assets" => a.assets = v.into(),
            "--out" => a.out = v.into(),
            "--data-env" => a.data_env = v,
            "--blackout-ms" => a.blackout = n(&v)?,
            "--loss" => {
                a.loss = v
                    .parse::<f64>()
                    .map_err(|_| format!("{k}: {v} is not a number"))?
                    / 100.0
            }
            other => return Err(format!("unknown option {other}")),
        }
    }
    if a.recorder.as_os_str().is_empty() == !a.builtin {
        return Err("give --recorder PATH or --builtin, not both and not neither".into());
    }
    Ok(a)
}

/// `user sys` seconds and the peak resident set (bytes) from `/usr/bin/time`'s report.
fn parse_time(report: &str) -> (f64, f64, u64) {
    let (mut user, mut sys, mut rss) = (0.0, 0.0, 0);
    for line in report.lines() {
        let l = line.trim();
        if l.contains(" real ") && l.contains(" user ") {
            // macOS: "1.23 real         0.45 user         0.12 sys"
            let w: Vec<&str> = l.split_whitespace().collect();
            for (i, t) in w.iter().enumerate() {
                match *t {
                    "user" => user = w[i - 1].parse().unwrap_or(0.0),
                    "sys" => sys = w[i - 1].parse().unwrap_or(0.0),
                    _ => {}
                }
            }
        } else if l.ends_with("maximum resident set size") {
            rss = l
                .split_whitespace()
                .next()
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
        } else if let Some(v) = l.strip_prefix("User time (seconds):") {
            user = v.trim().parse().unwrap_or(0.0);
        } else if let Some(v) = l.strip_prefix("System time (seconds):") {
            sys = v.trim().parse().unwrap_or(0.0);
        } else if let Some(v) = l.strip_prefix("Maximum resident set size (kbytes):") {
            rss = v.trim().parse::<u64>().unwrap_or(0) * 1024;
        }
    }
    (user, sys, rss)
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

#[tokio::main]
async fn main() {
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

/// Records the class with rtcd itself, and reports what it wrote and what the writer cost: the
/// seconds its threads spent writing (not waiting) over the length of the recording is the share
/// of one core the recording takes.
async fn record_in_rtcd(
    a: Args,
    rtcd: Arc<Rtcd>,
    lms: Lms,
    people: Vec<class::Participant>,
) -> Value {
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
            if let Some(p) = lms.read(|s| s.files.get(&f.index.to_string()).cloned()) {
                entry["probe"] = ffprobe(&p);
            }
            files.push(entry);
        }
    }
    let text = rtcd.metrics();
    let report = json!({
        "label": a.label, "builtin": true,
        "loss_percent": a.loss * 100.0, "blackout_ms": a.blackout,
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

async fn run(a: Args) -> Value {
    let _ = std::fs::remove_dir_all(&a.out);
    std::fs::create_dir_all(&a.out).unwrap();
    let assets = Arc::new(Assets::load(&a.assets).expect("the media is made (ffmpeg)"));
    let impair = Impair {
        down_loss: a.loss,
        blackout_ms: a.blackout,
        ..Default::default()
    };
    let recording_data = a.out.join("rtcd-data").display().to_string();
    let builtin = a.builtin;
    let rtcd = Arc::new(
        Rtcd::start_configured(impair, move |cfg| {
            cfg.record.enabled = builtin;
            cfg.record.data_dir = recording_data;
        })
        .await,
    );
    let lms = Lms::start(Some(rtcd.clone()), a.out.join("lms")).await;

    // The class: everyone speaks, some have cameras, some share a screen.
    let mut people = Vec::new();
    for i in 0..a.people {
        let spec = Spec {
            name: format!("p{i}"),
            voice: true,
            camera: i < a.cameras,
            screen: i < a.screens,
        };
        people.push(class::join(&rtcd, assets.clone(), CHAN_ID, spec, i).await);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let ready = Instant::now();
    while rtcd.sessions() < a.people as usize && ready.elapsed() < Duration::from_secs(20) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    for p in &people {
        lms.person_joined(&p.session_id, &p.user_id);
    }

    if a.builtin {
        return record_in_rtcd(a, rtcd, lms, people).await;
    }

    // The recorder, under `time` for its CPU and memory.
    let data = a.out.join("data");
    std::fs::create_dir_all(&data).unwrap();
    let (timer, flag) = if cfg!(target_os = "macos") {
        ("/usr/bin/time", "-l")
    } else {
        ("/usr/bin/time", "-v")
    };
    let mut cmd = Command::new(timer);
    cmd.arg(flag)
        .arg(&a.recorder)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    for (k, v) in lms.config_env() {
        cmd.env(k, v);
    }
    cmd.env(&a.data_env, &data).env("LOG_LEVEL", "info");
    let started = Instant::now();
    let mut child = cmd.spawn().expect("the recorder starts");
    let mut stdout = child.stdout.take().unwrap();
    let mut stderr = child.stderr.take().unwrap();
    let out_task = tokio::spawn(async move {
        let mut s = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stdout, &mut s)
            .await
            .ok();
        s
    });
    let err_task = tokio::spawn(async move {
        let mut s = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut stderr, &mut s)
            .await
            .ok();
        s
    });

    // Wait for it to be in the call.
    let wait = Instant::now();
    while rtcd.sessions() <= a.people as usize && wait.elapsed() < Duration::from_secs(40) {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let joined = rtcd.sessions() > a.people as usize;

    // Let the class run.
    tokio::time::sleep(Duration::from_secs(a.duration)).await;
    lms.emit(
        "custom_lms_recording_stop",
        "",
        json!({"recording_id": REC_ID}),
    );
    let finished = tokio::time::timeout(Duration::from_secs(180), child.wait()).await;
    let exit = match finished {
        Ok(Ok(s)) => s.code(),
        _ => {
            let _ = child.kill().await;
            None
        }
    };
    let total = started.elapsed();
    let log = out_task.await.unwrap_or_default();
    let time_report = err_task.await.unwrap_or_default();
    let (user, sys, rss) = parse_time(&time_report);

    // What it wrote.
    let complete = lms.read(|s| s.complete.clone());
    let failed = lms.read(|s| s.failed.clone());
    let mut files = Vec::new();
    if let Some(c) = &complete {
        for (i, f) in c["files"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .enumerate()
        {
            let path = lms.read(|s| s.files.get(&i.to_string()).cloned());
            let mut entry = f.clone();
            if let Some(p) = path {
                entry["uploaded_bytes"] =
                    std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0).into();
                let mut named = a
                    .out
                    .join(format!("{i:03}-{}.webm", f["kind"].as_str().unwrap_or("x")));
                if std::fs::copy(&p, &named).is_err() {
                    named = p.clone();
                }
                entry["probe"] = ffprobe(&named);
            }
            files.push(entry);
        }
    }
    let sessions_open = rtcd.sessions();
    for p in people {
        p.leave().await;
    }
    drop(rtcd);
    json!({
        "label": a.label,
        "loss_percent": a.loss * 100.0, "blackout_ms": a.blackout,
        "people": a.people, "cameras": a.cameras, "screens": a.screens, "duration_s": a.duration,
        "recorder_joined": joined,
        "exit": exit,
        "wall_s": total.as_secs_f64(),
        "cpu_user_s": user, "cpu_sys_s": sys, "cpu_s": user + sys,
        "max_rss_mib": rss as f64 / 1048576.0,
        "failed": failed,
        "duration_ms": complete.as_ref().map(|c| c["duration_ms"].clone()),
        "files": files,
        "sessions_left_on_sfu": sessions_open,
        "log_tail": log.lines().rev().take(12).collect::<Vec<_>>().into_iter().rev().collect::<Vec<_>>(),
    })
}
