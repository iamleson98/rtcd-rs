//! `lms-sim run` records a synthetic class with a recorder binary and reports what it cost and what
//! it wrote, so two recorders can be compared on the same load.
//!
//! ```text
//! lms-sim run --recorder ./recorder --label rust --duration 30 --people 8 --cameras 6 --screens 1
//! ```

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lms_sim::assets::Assets;
use lms_sim::class::{self, Spec};
use lms_sim::host::Rtcd;
use lms_sim::lms::{CHAN_ID, Lms, REC_ID};
use serde_json::{Value, json};
use tokio::process::Command;

struct Args {
    recorder: PathBuf,
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
        return Err("usage: lms-sim run --recorder PATH [--label NAME] [--duration S] [--people N] [--cameras N] [--screens N] [--assets DIR] [--out DIR] [--data-env NAME] [--loss PERCENT] [--blackout-ms MS]".into());
    }
    while let Some(k) = it.next() {
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
    if a.recorder.as_os_str().is_empty() {
        return Err("--recorder is required".into());
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

async fn run(a: Args) -> Value {
    let _ = std::fs::remove_dir_all(&a.out);
    std::fs::create_dir_all(&a.out).unwrap();
    let assets = Arc::new(Assets::load(&a.assets).expect("the media is made (ffmpeg)"));
    let rtcd = Arc::new(Rtcd::start_with(a.loss, a.blackout).await);
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
