//! `lms-sim ramp` adds classes to an rtcd that runs elsewhere, a few at a time, to find how many it
//! carries (see `scripts/lesson-soak/calibrate.sh`):
//!
//! ```text
//! lms-sim ramp --http http://rtcd:8045 --media 10.0.0.2:8443 --step-secs 60 --per-step 1 \
//!     --max-classes 40 --students 30 --cameras 4 --watch 2 --seconds 3600
//! ```
//!
//! The classes are the ones `lms-sim load` makes (a teacher and `students` students each). Every
//! `step-secs` another `per-step` classes join, up to `max-classes`; the process then stays until
//! `seconds` after the last class joined. After each step it prints one JSON line,
//! `{"ts": <unix ms>, "classes": <classes in the call now>}`, so that the SFU's CPU and the
//! receivers' quality can be read against the number of classes.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use lms_sim::assets::Assets;
use lms_sim::class::{self, Spec};
use lms_sim::host::Rtcd;

struct Args {
    http: String,
    media: SocketAddr,
    step_secs: u64,
    per_step: u32,
    max_classes: u32,
    students: u32,
    cameras: u32,
    watch: u32,
    talk_every: u32,
    per_second: u32,
    seconds: u64,
}

fn parse(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut a = Args {
        http: String::new(),
        media: "127.0.0.1:8443".parse().unwrap(),
        step_secs: 60,
        per_step: 1,
        max_classes: 40,
        students: 30,
        cameras: 4,
        watch: 2,
        talk_every: 10,
        per_second: 20,
        seconds: 3600,
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
            "--step-secs" => a.step_secs = n()?.max(1),
            "--per-step" => a.per_step = n()?.max(1) as u32,
            "--max-classes" => a.max_classes = n()? as u32,
            "--students" => a.students = n()? as u32,
            "--cameras" => a.cameras = n()? as u32,
            "--watch" => a.watch = n()? as u32,
            "--talk-every" => a.talk_every = n()?.max(1) as u32,
            "--per-second" => a.per_second = n()?.max(1) as u32,
            "--seconds" => a.seconds = n()?,
            other => return Err(format!("unknown option {other}")),
        }
    }
    if a.http.is_empty() {
        return Err("--http is required".into());
    }
    Ok(a)
}

pub async fn run(args: impl Iterator<Item = String>, assets: std::path::PathBuf) {
    let a = match parse(args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let assets = Arc::new(Assets::load(&assets).expect("the media is made (ffmpeg)"));
    let _ = rustls::crypto::ring::default_provider().install_default();
    let rtcd = Arc::new(Rtcd::remote(&a.http, a.media).await);
    let gap = Duration::from_micros(1_000_000 / u64::from(a.per_second));
    let mut people = Vec::new();
    let mut index = 0u32;
    let mut classes = 0u32;
    while classes < a.max_classes {
        let step = Instant::now();
        let upto = (classes + a.per_step).min(a.max_classes);
        for c in classes..upto {
            let call = format!("channel-lab-{c}");
            let cameras: Vec<String> = (0..a.cameras).map(|i| format!("c{c}s{i}")).collect();
            let teacher = Spec {
                name: format!("c{c}t"),
                voice: true,
                camera: true,
                ..Default::default()
            };
            people.push(class::join(&rtcd, assets.clone(), &call, teacher, index).await);
            index += 1;
            for i in 0..a.students {
                let mut watch = vec![format!("c{c}t")];
                for k in 0..a.watch {
                    if let Some(cam) = cameras.get(((i + k) % a.cameras.max(1)) as usize)
                        && *cam != format!("c{c}s{i}")
                    {
                        watch.push(cam.clone());
                    }
                }
                let spec = Spec {
                    name: format!("c{c}s{i}"),
                    voice: i % a.talk_every == 0,
                    camera: i < a.cameras,
                    screen: false,
                    watch,
                    ..Default::default()
                };
                people.push(class::join(&rtcd, assets.clone(), &call, spec, index).await);
                index += 1;
                tokio::time::sleep(gap).await;
            }
        }
        classes = upto;
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_millis());
        println!("{{\"ts\": {ts}, \"classes\": {classes}}}");
        if classes < a.max_classes {
            tokio::time::sleep(Duration::from_secs(a.step_secs).saturating_sub(step.elapsed()))
                .await;
        }
    }
    tokio::time::sleep(Duration::from_secs(a.seconds)).await;
    for p in people {
        p.leave().await;
    }
}
