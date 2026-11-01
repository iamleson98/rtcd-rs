//! `lms-sim load` puts classes of synthetic participants on an rtcd that runs elsewhere, to load
//! it past what its card or its threads carry:
//!
//! ```text
//! lms-sim load --http http://rtcd:8045 --media 10.0.0.2:8443 --classes 4 --students 60 \
//!     --cameras 6 --watch 3 --per-second 10 --seconds 600
//! ```
//!
//! Every class has a teacher (microphone and camera) and `students` students; the first `cameras`
//! of them send a camera; each student watches the teacher and `watch` of those cameras, so the
//! node sends `students x (1 + watch)` video streams for a class. One student in
//! `--talk-every` has an open microphone. The participants join at `per-second` and then stay for
//! `seconds`. It reports once a second how many were started; what the node does is read from the
//! node's own `/system` and `/metrics`.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lms_sim::assets::Assets;
use lms_sim::class::{self, Spec};
use lms_sim::host::Rtcd;

struct Args {
    http: String,
    media: SocketAddr,
    classes: u32,
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
        classes: 1,
        students: 30,
        cameras: 4,
        watch: 2,
        talk_every: 10,
        per_second: 10,
        seconds: 120,
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
            "--media" => a.media = v.parse().map_err(|_| format!("{k}: {v} is not an address"))?,
            "--classes" => a.classes = n()? as u32,
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
    let rtcd = Rtcd::remote(&a.http, a.media).await;
    let started = Instant::now();
    let gap = Duration::from_micros(1_000_000 / u64::from(a.per_second));
    let mut people = Vec::new();
    let mut index = 0u32;
    for c in 0..a.classes {
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
            // Each student watches the next `watch` cameras after their own place in the ring.
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
            };
            people.push(class::join(&rtcd, assets.clone(), &call, spec, index).await);
            index += 1;
            tokio::time::sleep(gap).await;
            if index.is_multiple_of(a.per_second) {
                println!(
                    "{{\"t\": {}, \"started\": {}}}",
                    started.elapsed().as_secs(),
                    index
                );
            }
        }
    }
    println!("{{\"t\": {}, \"started\": {}, \"all\": true}}", started.elapsed().as_secs(), index);
    tokio::time::sleep(Duration::from_secs(a.seconds)).await;
    for p in people {
        p.leave().await;
    }
}
