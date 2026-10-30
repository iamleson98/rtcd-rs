//! `lms-sim soak` runs class after class against one rtcd and reports the process's memory after
//! each, to find what a session, a recording or a call leaves behind: a leak shows as a resident
//! set that keeps growing from class to class once the first few have warmed everything up.
//!
//! ```text
//! lms-sim soak --classes 40 --people 12 --cameras 4 --screens 1 --seconds 20 --record
//! ```
//!
//! rtcd and the synthetic participants share the process, so the figure is theirs together; what
//! matters is its trend, and the report also says how many sessions and recordings rtcd still
//! holds when a class is over (none should be).

use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rtcd_proto::{ClientMessage, RecordStart, RecordStop};
use serde_json::{Value, json};

use lms_sim::assets::Assets;
use lms_sim::class::{self, Spec};
use lms_sim::host::{Impair, Rtcd};
use lms_sim::lms::{CHAN_ID, Lms, REC_ID};

struct Args {
    classes: u32,
    people: u32,
    cameras: u32,
    screens: u32,
    seconds: u64,
    record: bool,
    out: std::path::PathBuf,
}

fn parse(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut a = Args {
        classes: 20,
        people: 12,
        cameras: 4,
        screens: 1,
        seconds: 20,
        record: false,
        out: std::env::temp_dir().join("lms-sim-soak"),
    };
    let mut it = args;
    while let Some(k) = it.next() {
        if k == "--record" {
            a.record = true;
            continue;
        }
        let v = it.next().ok_or_else(|| format!("{k} needs a value"))?;
        let n = || {
            v.parse::<u64>()
                .map_err(|_| format!("{k}: {v} is not a number"))
        };
        match k.as_str() {
            "--classes" => a.classes = n()? as u32,
            "--people" => a.people = n()? as u32,
            "--cameras" => a.cameras = n()? as u32,
            "--screens" => a.screens = n()? as u32,
            "--seconds" => a.seconds = n()?,
            "--out" => a.out = v.into(),
            other => return Err(format!("unknown option {other}")),
        }
    }
    Ok(a)
}

/// This process's resident set in bytes, as `ps` reports it.
fn resident_bytes() -> u64 {
    Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map_or(0, |kib| kib * 1024)
}

/// The value of a gauge or counter in Prometheus text.
fn metric(text: &str, name: &str) -> f64 {
    text.lines()
        .find_map(|l| l.strip_prefix(name)?.strip_prefix(' ')?.trim().parse().ok())
        .unwrap_or(0.0)
}

pub async fn run(args: impl Iterator<Item = String>, assets: std::path::PathBuf) {
    let a = match parse(args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let _ = std::fs::remove_dir_all(&a.out);
    std::fs::create_dir_all(&a.out).unwrap();
    let assets = Arc::new(Assets::load(&assets).expect("the media is made (ffmpeg)"));
    let record = a.record;
    let data = a.out.join("rtcd-data").display().to_string();
    let rtcd = Arc::new(
        Rtcd::start_configured(Impair::default(), move |cfg| {
            cfg.record.enabled = record;
            cfg.record.data_dir = data;
        })
        .await,
    );
    let lms = Lms::start(Some(rtcd.clone()), a.out.join("lms")).await;
    let control = rtcd.connect().await;
    let mut rows: Vec<Value> = Vec::new();
    for class_no in 0..a.classes {
        let mut people = Vec::new();
        for i in 0..a.people {
            let spec = Spec {
                name: format!("c{class_no}p{i}"),
                voice: true,
                camera: i < a.cameras,
                screen: i < a.screens,
            };
            people.push(class::join(&rtcd, assets.clone(), CHAN_ID, spec, i).await);
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let ready = Instant::now();
        while rtcd.sessions() < a.people as usize && ready.elapsed() < Duration::from_secs(20) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        for p in &people {
            lms.person_joined(&p.session_id, &p.user_id);
        }
        if record {
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
        }
        tokio::time::sleep(Duration::from_secs(a.seconds)).await;
        if record {
            control.send(ClientMessage::RecordStop(RecordStop {
                recording_id: REC_ID.into(),
            }));
            let wait = Instant::now();
            let ended = loop {
                let n = rtcd
                    .reports()
                    .iter()
                    .filter(|m| matches!(m, ClientMessage::RecordEnded(_)))
                    .count();
                if n > class_no as usize || wait.elapsed() > Duration::from_secs(60) {
                    break n > class_no as usize;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            };
            if !ended {
                eprintln!("class {class_no}: the recording did not end in 60 s");
            }
        }
        for p in people {
            lms.person_left(&p.session_id);
            p.leave().await;
        }
        let gone = Instant::now();
        while rtcd.sessions() > 0 && gone.elapsed() < Duration::from_secs(30) {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        // Let the sweeps that run on a timer have their turn before looking.
        tokio::time::sleep(Duration::from_secs(3)).await;
        let text = rtcd.metrics();
        let row = json!({
            "class": class_no,
            "rss_mib": resident_bytes() as f64 / 1048576.0,
            "sessions_left": rtcd.sessions(),
            "ws_connections": metric(&text, "rtcd_ws_connections_total{clientID=\"sim\"}"),
            "recordings_open": metric(&text, "rtcd_record_recordings"),
            "pending_bytes": metric(&text, "rtcd_record_pending_bytes"),
            "files_written": metric(&text, "rtcd_record_files_written_total"),
        });
        println!("{row}");
        rows.push(row);
    }
    let rss: Vec<f64> = rows
        .iter()
        .map(|r| r["rss_mib"].as_f64().unwrap_or(0.0))
        .collect();
    let half = rss.len() / 2;
    let mean = |s: &[f64]| s.iter().sum::<f64>() / s.len().max(1) as f64;
    println!(
        "{}",
        json!({
            "classes": a.classes, "people": a.people, "record": a.record,
            "rss_first_half_mean_mib": mean(&rss[..half]),
            "rss_second_half_mean_mib": mean(&rss[half..]),
            "rss_growth_per_class_mib": if rss.len() > 1 {
                (rss[rss.len() - 1] - rss[half]) / (rss.len() - 1 - half).max(1) as f64
            } else { 0.0 },
        })
    );
    drop(control);
    if let Ok(rtcd) = Arc::try_unwrap(rtcd) {
        rtcd.shutdown().await;
    }
}
