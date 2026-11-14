//! `lms-sim lesson` is one lesson, as a real class has it, on an rtcd that runs elsewhere: a
//! teacher (microphone, camera, a screen share in two layers), about 25 students of whom four have
//! a camera, and one student's microphone open at a time, in turn. Every sender starts its
//! sequence numbers and timestamps just short of their wrap, so a run crosses each of them.
//!
//! The lesson is run by two processes against the same call, so that one group of students can be on
//! a network that is made to misbehave while the teacher and the rest are not (what drifts on the
//! server then shows in the clean group, what the network does in the impaired one):
//!
//! ```text
//! lms-sim lesson --http http://rtcd:8045 --media 10.0.0.2:8443 --minutes 120 --impl rtcd-rs \
//!     --group clean --teacher --first 0 --students 17 --out clean.jsonl --chrome 8099
//! lms-sim lesson --http http://rtcd:8045 --media 10.0.0.2:8443 --minutes 120 --impl rtcd-rs \
//!     --group impaired --first 17 --students 8 --out impaired.jsonl --chrome 8099
//! ```
//!
//! With `--udp-base PORT` (a multiple of four) student `k` of the process binds its media sockets to
//! the ports `PORT + 4k` to `PORT + 4k + 3`, so that a network that shapes each participant on its own
//! (`vendor/rtc/benchmarks/soak/netem.sh` with `NETEM_PORTS`) can tell them apart.
//!
//! Attacks of the lesson's own, each logged as `attack` rows into `--out` (see `lms_sim::abuse`), all
//! seeded by `--seed` (printed, and the `seed` event): `--teacher-reconnect-every MIN` (the teacher
//! drops its session and joins again with the same session id), `--nat-rebind-every SECS` (each
//! student of an impaired group changes its UDP source port now and then) and `--mtu` (an impaired
//! group sends datagrams of MTU size and over).
//!
//! `--camera-layers single|simulcast` (default `simulcast`) sets how cameras are sent: the browser's
//! two layers with three temporal layers each, or one plain stream.
//!
//! The first `--measure` students of a process measure what they receive (the teacher's voice,
//! camera and screen, and the student cameras) and write it every 5 s, see `lms_sim::stats`. With
//! `--chrome PORT` the process also serves the page for a headless Chrome (on loopback, for one in the
//! same network namespace, or on `--chrome-bind ADDR` for one in a namespace of its own), which joins as one more student and writes what the browser measures.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use lms_sim::abuse::{LessonAttacks, Rows};
use lms_sim::assets::{Assets, CameraLayers};
use lms_sim::chrome;
use lms_sim::class::{self, Ctl, Spec};
use lms_sim::host::Rtcd;
use lms_sim::stats::{Probe, Sink, unix_ms};

struct Args {
    http: String,
    media: SocketAddr,
    minutes: u64,
    imp: String,
    group: &'static str,
    teacher: bool,
    first: u32,
    students: u32,
    total: u32,
    cameras: u32,
    measure: u32,
    talk_secs: u64,
    chrome: Option<u16>,
    chrome_bind: String,
    udp_base: Option<u16>,
    seed: Option<u64>,
    teacher_reconnect_mins: Option<u64>,
    nat_rebind_secs: Option<u64>,
    mtu: bool,
    camera_layers: CameraLayers,
    call: String,
    out: PathBuf,
}

fn parse(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut a = Args {
        http: String::new(),
        media: "127.0.0.1:8443".parse().unwrap(),
        minutes: 120,
        imp: "rtcd-rs".into(),
        group: "clean",
        teacher: false,
        first: 0,
        students: 17,
        total: 25,
        cameras: 4,
        measure: 6,
        talk_secs: 30,
        chrome: None,
        chrome_bind: "127.0.0.1".into(),
        udp_base: None,
        seed: None,
        teacher_reconnect_mins: None,
        nat_rebind_secs: None,
        mtu: false,
        camera_layers: CameraLayers::default(),
        call: "channel-lesson".into(),
        out: PathBuf::new(),
    };
    let mut it = args;
    while let Some(k) = it.next() {
        if k == "--teacher" {
            a.teacher = true;
            continue;
        }
        if k == "--mtu" {
            a.mtu = true;
            continue;
        }
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
            "--group" => {
                a.group = match v.as_str() {
                    "clean" => "clean",
                    "impaired" => "impaired",
                    _ => return Err("--group is clean or impaired".into()),
                }
            }
            "--first" => a.first = n()? as u32,
            "--students" => a.students = n()? as u32,
            "--total" => a.total = n()?.max(1) as u32,
            "--cameras" => a.cameras = n()? as u32,
            "--measure" => a.measure = n()? as u32,
            "--talk-secs" => a.talk_secs = n()?.max(1),
            "--chrome" => a.chrome = Some(n()? as u16),
            "--chrome-bind" => a.chrome_bind = v.clone(),
            "--udp-base" => a.udp_base = Some(n()? as u16),
            "--seed" => a.seed = Some(n()?),
            "--teacher-reconnect-every" => a.teacher_reconnect_mins = Some(n()?.max(1)),
            "--nat-rebind-every" => a.nat_rebind_secs = Some(n()?.max(1)),
            "--camera-layers" => a.camera_layers = CameraLayers::parse(&v)?,
            "--call" => a.call = v.clone(),
            "--out" => a.out = v.into(),
            other => return Err(format!("unknown option {other}")),
        }
    }
    if a.http.is_empty() || a.out.as_os_str().is_empty() {
        return Err("--http and --out are required".into());
    }
    if (a.mtu || a.nat_rebind_secs.is_some() || a.teacher_reconnect_mins.is_some())
        && a.seed.is_none()
    {
        return Err("--mtu, --nat-rebind-every and --teacher-reconnect-every need --seed".into());
    }
    Ok(a)
}

pub async fn run(args: impl Iterator<Item = String>, assets: PathBuf) {
    let a = match parse(args) {
        Ok(a) => a,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };
    let assets = Arc::new(
        Assets::load(&assets)
            .expect("the media is made (ffmpeg)")
            .with_camera_layers(a.camera_layers),
    );
    let _ = rustls::crypto::ring::default_provider().install_default();
    let sink = Sink::open(&a.out).expect("the output file opens");
    let rtcd = Arc::new(Rtcd::remote(&a.http, a.media).await);
    let probe = |imp: &str, measure: bool, leader: bool| Probe {
        sink: sink.clone(),
        imp: imp.to_owned(),
        group: a.group,
        teacher: "session-teacher".into(),
        measure,
        leader,
    };

    if let Some(port) = a.chrome {
        let chrome_probe = probe(&format!("{}/chrome", a.imp), false, false);
        chrome::serve(
            rtcd.clone(),
            a.call.clone(),
            chrome_probe,
            std::iter::once("teacher".to_owned())
                .chain((0..a.cameras).map(|i| format!("s{i:02}")))
                .map(|n| format!("session-{n}"))
                .collect(),
            &a.chrome_bind,
            port,
        )
        .await
        .expect("the page for Chrome is served");
    }

    let started = Instant::now();
    let mut people = Vec::new();
    // The attacks of the lesson's own: those on students are for the impaired group.
    let seed = a.seed.unwrap_or_default();
    let impaired: Vec<(String, u32)> = if a.group == "impaired" {
        (a.first..a.first + a.students)
            .map(|i| (format!("s{i:02}"), i))
            .collect()
    } else {
        Vec::new()
    };
    let mut attacks = a.seed.map(|seed| {
        let rows = Rows::new(sink.clone(), a.group, seed, &a.imp);
        rows.seed_event(&format!("lesson {} seed {seed}", a.group));
        LessonAttacks::new(
            rows,
            &impaired,
            a.nat_rebind_secs,
            a.teacher_reconnect_mins,
            a.teacher,
            a.mtu,
            a.minutes,
        )
    });
    let mut teacher_spec = None;
    // The teacher is on the clean network, and sends everything a teacher does.
    if a.teacher {
        let spec = Spec {
            name: "teacher".into(),
            voice: true,
            camera: true,
            screen: true,
            wrap: true,
            probe: Some(probe(&a.imp, false, false)),
            ..Default::default()
        };
        teacher_spec = Some(spec.clone());
        people.push((
            u32::MAX,
            class::join(&rtcd, assets.clone(), &a.call, spec, 0).await,
        ));
    }
    let cameras: Vec<String> = (0..a.cameras).map(|i| format!("s{i:02}")).collect();
    for i in a.first..a.first + a.students {
        let name = format!("s{i:02}");
        let mut watch = vec!["teacher".to_owned()];
        watch.extend(cameras.iter().filter(|c| **c != name).cloned());
        let spec = Spec {
            name,
            voice: false,
            camera: i < a.cameras,
            watch,
            wrap: true,
            probe: Some(probe(&a.imp, i - a.first < a.measure, i == a.first)),
            udp_base: a.udp_base.map(|b| b + 4 * (i - a.first) as u16),
            mtu: a.mtu && a.group == "impaired",
            seed: seed ^ u64::from(i),
            ..Default::default()
        };
        people.push((
            i,
            class::join(&rtcd, assets.clone(), &a.call, spec, i + 1).await,
        ));
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    println!(
        "{{\"t\": {}, \"joined\": {}, \"group\": \"{}\"}}",
        started.elapsed().as_secs(),
        people.len(),
        a.group
    );

    // One student speaks at a time, each for `talk_secs`, round the class, by the clock: the
    // processes agree on whose turn it is without talking to one another.
    let end = Instant::now() + Duration::from_secs(a.minutes * 60);
    let mut speaker = None;
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let stop = tokio::signal::ctrl_c();
    tokio::pin!(stop);
    while Instant::now() < end {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = &mut stop => break,
        }
        let turn = ((unix_ms() / 1000 / a.talk_secs) % u64::from(a.total)) as u32;
        if let Some(at) = attacks.as_mut() {
            at.tick(&people);
            if let Some(spec) = &teacher_spec
                && at.teacher_due()
                && let Some(pos) = people.iter().position(|(i, _)| *i == u32::MAX)
            {
                let (_, old) = people.remove(pos);
                let back = at
                    .teacher_reconnect(&rtcd, assets.clone(), &a.call, spec.clone(), old)
                    .await;
                people.push((u32::MAX, back));
            }
        }
        if speaker != Some(turn) {
            for (i, p) in &people {
                if *i == turn || Some(*i) == speaker {
                    p.control(Ctl::Voice(*i == turn));
                }
            }
            speaker = Some(turn);
        }
    }
    if let Some(at) = attacks.as_mut() {
        at.finish();
    }
    for (_, p) in people {
        p.leave().await;
    }
}
