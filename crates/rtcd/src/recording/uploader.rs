//! Sending closed files to the server and saying so.
//!
//! One task serves every recording of the node. A file the writer closed is uploaded through the
//! endpoints the recorder bot uses (straight to storage at presigned URLs when the server allows
//! it, otherwise to the server), with the recording's own bot session as its credential. Only once
//! the bytes are stored is the file reported (`record_file`) and the local copy removed. When the
//! capture has ended and every file is uploaded, or given up on, the recording is reported ended.
//!
//! A multipart upload is not finished here: rtcd holds no storage credentials, and the server,
//! which handed out the parts, joins them when it reads the file's report.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use recorder::media::Entry;
use recorder::upload::UploadError;
use rtcd_proto::{ClientMessage, RecordEnded, RecordFile};
use tokio::sync::{Semaphore, mpsc};
use tracing::{debug, info, warn};

use super::pending::Pending;
use super::reports::Reports;
use super::{Cmd, Ended, Registry, Run};
use crate::metrics::Metrics;

/// How a file's upload is retried, beyond the retries inside each request.
#[derive(Debug, Clone, Copy)]
pub struct Policy {
    /// Times a whole file is tried before it is given up on.
    pub attempts: u32,
    /// The wait before the second try; the n-th is n times as long.
    pub pause: Duration,
    /// Files uploaded at once, all recordings together.
    pub concurrency: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            attempts: 3,
            pause: Duration::from_secs(15),
            concurrency: 4,
        }
    }
}

/// What the uploader works with.
#[derive(Debug, Clone)]
pub struct Deps {
    pub reports: Arc<Reports>,
    pub pending: Arc<Pending>,
    pub metrics: Arc<Metrics>,
    pub policy: Policy,
    /// Captures started and not yet over for the server: counted up when one starts, down here
    /// when its end has been reported. A node that is shutting down waits for it to reach zero.
    pub unfinished: Arc<AtomicUsize>,
    /// Told when a run's end has been sent, which frees the file numbers it took.
    pub registry: Arc<Registry>,
}

/// A recording's progress through the uploader.
struct State {
    run: Arc<Run>,
    /// Files handed over and not finished with.
    outstanding: usize,
    ended: Option<Ended>,
}

/// Starts the uploader and returns the way to give it work. It stops when every sender is gone.
pub fn spawn(deps: Deps) -> mpsc::UnboundedSender<Cmd> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run(rx, tx.downgrade(), deps));
    tx
}

async fn run(mut rx: mpsc::UnboundedReceiver<Cmd>, tx: mpsc::WeakUnboundedSender<Cmd>, deps: Deps) {
    let slots = Arc::new(Semaphore::new(deps.policy.concurrency.max(1)));
    let mut runs: HashMap<u64, State> = HashMap::new();
    while let Some(cmd) = rx.recv().await {
        let id = match cmd {
            Cmd::File { run, index, entry } => {
                let id = run.id;
                runs.entry(id)
                    .or_insert_with(|| State {
                        run: run.clone(),
                        outstanding: 0,
                        ended: None,
                    })
                    .outstanding += 1;
                match tx.upgrade() {
                    Some(tx) => {
                        tokio::spawn(upload(run, index, entry, slots.clone(), tx, deps.clone()));
                    }
                    // Nobody can hear the answer any more: the service is going away.
                    None => return,
                }
                continue;
            }
            Cmd::Ended { run, ended } => {
                let id = run.id;
                let state = runs.entry(id).or_insert_with(|| State {
                    run,
                    outstanding: 0,
                    ended: None,
                });
                state.ended = Some(ended);
                id
            }
            Cmd::Uploaded { run } => {
                if let Some(state) = runs.get_mut(&run) {
                    state.outstanding = state.outstanding.saturating_sub(1);
                }
                run
            }
            Cmd::Deadline => {
                // The node is out of time: a capture that is over is reported now, with the
                // files that are in. The rest are not coming (the server leaves them out).
                let over: Vec<u64> = runs
                    .iter()
                    .filter(|(_, s)| s.ended.is_some())
                    .map(|(id, _)| *id)
                    .collect();
                for id in over {
                    if let Some(state) = runs.remove(&id)
                        && let Some(ended) = state.ended
                    {
                        finish(&state.run, ended, &deps).await;
                    }
                }
                continue;
            }
        };
        if runs
            .get(&id)
            .is_some_and(|s| s.outstanding == 0 && s.ended.is_some())
            && let Some(state) = runs.remove(&id)
            && let Some(ended) = state.ended
        {
            finish(&state.run, ended, &deps).await;
        }
    }
}

/// Reports the end of a recording, now that its files are in, and clears its directory.
async fn finish(run: &Run, ended: Ended, deps: &Deps) {
    let report = (!ended.reason.is_empty()).then(|| RecordEnded {
        recording_id: run.recording_id.clone(),
        reason: ended.reason.to_owned(),
        file_base: run.file_base,
        files: ended.files,
        duration_ms: ended.duration_ms,
        dropped: ended.dropped,
    });
    if let Some(report) = &report {
        deps.reports
            .send(&run.client_id, ClientMessage::RecordEnded(report.clone()));
    }
    // As soon as the end is sent, not after the directory is removed: a resend of the same start
    // in between would be taken for a new run. The end is kept for a resend: the connection it
    // went on may have dropped before the server read it.
    deps.registry.reported(&run.recording_id, run.id, report);
    info!(recording = %run.recording_id, reason = ended.reason, files = ended.files, "recording over");
    if let Err(e) = tokio::fs::remove_dir_all(&run.dir).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(dir = %run.dir.display(), error = %e, "could not remove a recording's directory");
    }
    if let Some(parent) = run.dir.parent() {
        // Only if no later run of the recording is using it.
        let _ = tokio::fs::remove_dir(parent).await;
    }
    let _ = deps
        .unfinished
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
            Some(n.saturating_sub(1))
        });
}

/// Whether trying again could help: a 4xx answer will not change.
fn hopeless(e: &UploadError) -> bool {
    match e {
        UploadError::Api(a) => !a.retryable(),
        UploadError::File { source, .. } | UploadError::Part { source, .. } => hopeless(source),
        UploadError::Protocol(_) => true,
        UploadError::Empty | UploadError::Io(_) => false,
    }
}

/// Uploads one file, reports it, removes it, and tells the uploader it is done with.
async fn upload(
    run: Arc<Run>,
    index: u32,
    entry: Entry,
    slots: Arc<Semaphore>,
    tx: mpsc::UnboundedSender<Cmd>,
    deps: Deps,
) {
    let stored = {
        let _slot = slots.acquire().await;
        send(&run, index, &entry, &deps.policy).await
    };
    match stored {
        Ok(()) => {
            deps.metrics.record_uploaded(entry.size);
            deps.reports.send(
                &run.client_id,
                ClientMessage::RecordFile(RecordFile {
                    recording_id: run.recording_id.clone(),
                    index,
                    kind: entry.kind.clone(),
                    session_id: entry.session_id.clone(),
                    user_id: entry.user_id.clone(),
                    start_ms: entry.start_ms,
                    end_ms: entry.end_ms,
                    width: entry.width,
                    height: entry.height,
                    size: entry.size,
                }),
            );
            debug!(recording = %run.recording_id, index, bytes = entry.size, "file stored");
        }
        Err(e) => {
            deps.metrics.record_upload_failed();
            warn!(recording = %run.recording_id, index, kind = %entry.kind, error = %e, "giving a file up: it could not be uploaded");
        }
    }
    if let Err(e) = tokio::fs::remove_file(&entry.path).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        warn!(path = %entry.path.display(), error = %e, "could not remove an uploaded file");
    }
    deps.pending.release(entry.size);
    let _ = tx.send(Cmd::Uploaded { run: run.id });
}

async fn send(run: &Run, index: u32, entry: &Entry, policy: &Policy) -> Result<(), UploadError> {
    let mut attempt = 1;
    loop {
        match run.api.upload_file(index as usize, entry).await {
            Ok(_) => return Ok(()),
            Err(e) if attempt < policy.attempts && !hopeless(&e) => {
                let wait = policy.pause * attempt;
                warn!(recording = %run.recording_id, index, attempt, wait_ms = wait.as_millis() as u64, error = %e, "upload failed; trying the file again");
                tokio::time::sleep(wait).await;
                attempt += 1;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Instant;

    use recorder::api::Api;
    use recorder::testing::{Fake, Lms, REC_ID, REC_PATH};
    use serde_json::{Value, json};

    use super::super::Busy;
    use super::*;
    use crate::service::{ConnHandle, Outbound};

    struct Harness {
        cmds: mpsc::UnboundedSender<Cmd>,
        out: mpsc::Receiver<Outbound>,
        pending: Arc<Pending>,
        metrics: Arc<Metrics>,
        dir: PathBuf,
        unfinished: Arc<AtomicUsize>,
        registry: Arc<Registry>,
    }

    fn harness(tag: &str, policy: Policy) -> Harness {
        let dir = std::env::temp_dir().join(format!("rtcd-uploader-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let metrics = Arc::new(Metrics::new());
        let reports = Arc::new(Reports::new(metrics.clone()));
        let (tx, out) = mpsc::channel(64);
        reports.attach("c1", ConnHandle::new("mm".into(), tx));
        let pending = Arc::new(Pending::new(u64::MAX, metrics.clone()));
        let unfinished = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(Registry::default());
        let cmds = spawn(Deps {
            reports,
            pending: pending.clone(),
            metrics: metrics.clone(),
            policy,
            unfinished: unfinished.clone(),
            registry: registry.clone(),
        });
        Harness {
            unfinished,
            registry,
            cmds,
            out,
            pending,
            metrics,
            dir,
        }
    }

    fn quick() -> Policy {
        Policy {
            attempts: 3,
            pause: Duration::from_millis(1),
            concurrency: 2,
        }
    }

    impl Harness {
        fn run(&self, api: Api, file_base: u32) -> Arc<Run> {
            Arc::new(Run {
                id: 1,
                recording_id: REC_ID.into(),
                client_id: "mm".into(),
                call_id: "call".into(),
                file_base,
                max_files: 0,
                dir: self.dir.join("run"),
                api,
                t0: std::time::Instant::now(),
            })
        }

        /// A closed file, as the writer hands it over.
        fn file(&self, run: &Run, name: &str, body: &[u8], kind: &str) -> Entry {
            std::fs::create_dir_all(&run.dir).unwrap();
            let path = run.dir.join(name);
            std::fs::write(&path, body).unwrap();
            self.pending.add(body.len() as u64);
            Entry {
                kind: kind.into(),
                session_id: "sess".into(),
                user_id: "user".into(),
                start_ms: 1500,
                end_ms: 61_500,
                width: if kind == "camera" { 640 } else { 0 },
                height: if kind == "camera" { 360 } else { 0 },
                size: body.len() as u64,
                path,
            }
        }

        async fn next(&mut self) -> ClientMessage {
            let out = tokio::time::timeout(Duration::from_secs(20), self.out.recv())
                .await
                .expect("a report arrives")
                .expect("the connection is open");
            let Outbound::Binary(b) = out else {
                panic!("not a message")
            };
            ClientMessage::unpack(&b).unwrap()
        }

        async fn quiet(&mut self) {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(self.out.try_recv().is_err(), "nothing more is reported");
        }
    }

    fn ended(reason: &'static str, files: u32) -> Ended {
        Ended {
            reason,
            files,
            duration_ms: 61_500,
            dropped: 3,
        }
    }

    #[tokio::test]
    async fn files_go_through_the_server_and_are_reported_once_stored() {
        let lms = Lms::start().await;
        let mut h = harness("server", quick());
        let run = h.run(lms.api(), 1000);
        let voice = h.file(&run, "00001-voice.webm", b"voice-bytes", "voice");
        let camera = h.file(&run, "00002-camera.webm", b"camera-bytes!", "camera");
        h.cmds
            .send(Cmd::File {
                run: run.clone(),
                index: 1000,
                entry: voice,
            })
            .unwrap();
        h.cmds
            .send(Cmd::File {
                run: run.clone(),
                index: 1001,
                entry: camera,
            })
            .unwrap();
        h.cmds
            .send(Cmd::Ended {
                run: run.clone(),
                ended: ended("call_ended", 2),
            })
            .unwrap();

        let mut files = Vec::new();
        for _ in 0..2 {
            let ClientMessage::RecordFile(f) = h.next().await else {
                panic!("a file is reported before the end")
            };
            // It is reported only once the server has the bytes.
            let stored = lms
                .state
                .lock()
                .unwrap()
                .uploads
                .get(&f.index.to_string())
                .cloned();
            assert!(stored.is_some(), "file {} was not stored yet", f.index);
            files.push(f);
        }
        files.sort_by_key(|f| f.index);
        assert_eq!(
            (
                files[0].index,
                files[0].kind.as_str(),
                files[0].size,
                files[0].end_ms
            ),
            (1000, "voice", 11, 61_500)
        );
        assert_eq!(
            (
                files[1].index,
                files[1].width,
                files[1].height,
                files[1].session_id.as_str(),
                files[1].user_id.as_str()
            ),
            (1001, 640, 360, "sess", "user")
        );
        assert_eq!(files[1].recording_id, REC_ID);
        let ClientMessage::RecordEnded(e) = h.next().await else {
            panic!("then the end")
        };
        assert_eq!(
            (
                e.reason.as_str(),
                e.file_base,
                e.files,
                e.duration_ms,
                e.dropped
            ),
            ("call_ended", 1000, 2, 61_500, 3)
        );
        let uploads = lms.state.lock().unwrap().uploads.clone();
        assert_eq!(uploads["1000"], b"voice-bytes");
        assert_eq!(uploads["1001"], b"camera-bytes!");
        h.quiet().await;
        assert_eq!(h.pending.bytes(), 0);
        assert!(!run.dir.exists(), "the run's directory is gone");
        assert!(
            h.metrics
                .render()
                .contains("rtcd_record_uploaded_bytes_total 24")
        );
    }

    #[tokio::test]
    async fn files_go_straight_to_storage_in_parts_and_the_token_stays_home() {
        let fake = Fake::start(|r, attempt, site| {
            if r.path == format!("{REC_PATH}/uploads") {
                let v: Value = serde_json::from_slice(&r.body).unwrap();
                let ask = &v["files"][0];
                let index = ask["index"].as_u64().unwrap();
                if ask["kind"] == "voice" {
                    (200, Some(json!({"direct": true, "uploads": [
                        {"index": index, "content_type": "audio/webm", "url": format!("{site}/store/voice")}
                    ]})))
                } else {
                    let part = |n: u64| json!({"number": n, "url": format!("{site}/store/up/{n}")});
                    (200, Some(json!({"direct": true, "uploads": [
                        {"index": index, "content_type": "video/webm", "upload_id": "up", "part_size": 8, "parts": [part(1), part(2)]}
                    ]})))
                }
            } else if r.path == "/store/up/2" && attempt == 1 {
                (502, None)
            } else {
                (200, None)
            }
        })
        .await;
        let mut h = harness("direct", quick());
        let run = h.run(fake.api(), 40);
        let voice = h.file(&run, "a.webm", b"voice-bytes", "voice");
        let camera = h.file(&run, "b.webm", b"0123456789abcdef", "camera");
        for (index, entry) in [(40, voice), (41, camera)] {
            h.cmds
                .send(Cmd::File {
                    run: run.clone(),
                    index,
                    entry,
                })
                .unwrap();
        }
        h.cmds
            .send(Cmd::Ended {
                run: run.clone(),
                ended: ended("stopped", 2),
            })
            .unwrap();
        let mut seen = Vec::new();
        loop {
            match h.next().await {
                ClientMessage::RecordFile(f) => seen.push(f.index),
                ClientMessage::RecordEnded(e) => {
                    assert_eq!((e.file_base, e.files), (40, 2));
                    break;
                }
                other => panic!("{other:?}"),
            }
        }
        seen.sort_unstable();
        assert_eq!(seen, [40, 41]);
        let requests = fake.seen();
        let put = |p: &str| {
            requests
                .iter()
                .rfind(|r| r.method == "PUT" && r.path == p)
                .unwrap_or_else(|| panic!("no PUT {p}"))
        };
        assert_eq!(put("/store/voice").body, b"voice-bytes");
        assert_eq!(put("/store/up/1").body, b"01234567");
        assert_eq!(put("/store/up/2").body, b"89abcdef");
        for r in &requests {
            if r.path.starts_with("/store/") {
                assert!(
                    r.auth.is_empty(),
                    "the bot's token went to storage: {}",
                    r.path
                );
            } else {
                assert_eq!(r.auth, "Bearer bot-token");
            }
        }
        assert!(
            requests.iter().all(|r| !(r.path.ends_with("/complete"))),
            "the server joins the parts of a multipart upload itself"
        );
    }

    #[tokio::test]
    async fn a_file_is_tried_again_and_a_hopeless_one_is_given_up_on_unreported() {
        // The server is down for the first try of the first file, so its whole upload fails
        // and is tried again; the second file is refused for good.
        let down = std::sync::atomic::AtomicBool::new(true);
        let fake = Fake::start(move |r, _, _| {
            if r.path == format!("{REC_PATH}/uploads") {
                let v: Value = serde_json::from_slice(&r.body).unwrap();
                match v["files"][0]["index"].as_u64().unwrap() {
                    5 if down.swap(false, std::sync::atomic::Ordering::SeqCst) => (503, None),
                    6 => (409, None),
                    _ => (200, Some(json!({"direct": false, "uploads": []}))),
                }
            } else {
                (200, None)
            }
        })
        .await;
        let mut h = harness("retry", quick());
        let api = fake.api().retrying(0, Duration::from_millis(1));
        let run = h.run(api, 5);
        let a = h.file(&run, "a.webm", b"aaa", "voice");
        let b = h.file(&run, "b.webm", b"bbbb", "voice");
        let (b_path, a_path) = (b.path.clone(), a.path.clone());
        for (index, entry) in [(5, a), (6, b)] {
            h.cmds
                .send(Cmd::File {
                    run: run.clone(),
                    index,
                    entry,
                })
                .unwrap();
        }
        h.cmds
            .send(Cmd::Ended {
                run: run.clone(),
                ended: ended("stopped", 2),
            })
            .unwrap();
        let ClientMessage::RecordFile(f) = h.next().await else {
            panic!("the first file is reported")
        };
        assert_eq!(f.index, 5);
        let ClientMessage::RecordEnded(e) = h.next().await else {
            panic!("the end follows, with both indexes counted")
        };
        assert_eq!((e.files, e.reason.as_str()), (2, "stopped"));
        h.quiet().await;
        let asked = fake
            .seen()
            .iter()
            .filter(|r| {
                r.path.ends_with("/uploads") && r.body.windows(9).any(|w| w == b"\"index\":6")
            })
            .count();
        assert_eq!(asked, 1, "a refusal is not retried");
        assert!(
            !a_path.exists() && !b_path.exists(),
            "both local files are removed"
        );
        assert_eq!(h.pending.bytes(), 0);
        assert!(
            h.metrics
                .render()
                .contains("rtcd_record_upload_failures_total 1")
        );
    }

    #[tokio::test]
    async fn nothing_is_reported_for_a_capture_that_ends_silently() {
        let mut h = harness("silent", quick());
        let lms = Lms::start().await;
        let run = h.run(lms.api(), 0);
        h.cmds
            .send(Cmd::Ended {
                run,
                ended: ended("", 0),
            })
            .unwrap();
        h.quiet().await;
    }

    #[tokio::test]
    async fn the_numbers_of_a_run_stay_taken_until_its_end_has_been_sent_and_a_while_after() {
        let lms = Lms::start().await;
        let mut h = harness("numbers", quick());
        let run = h.run(lms.api(), 500);
        let draining = std::sync::atomic::AtomicBool::new(false);
        let want = super::super::Want {
            recording_id: REC_ID,
            client_id: "mm",
            call_id: "call",
            file_base: 500,
            max_files: 0,
        };
        h.registry.begin(want, run.id, &draining).unwrap();
        h.registry.finish(REC_ID, run.id, 1);
        let file = h.file(&run, "00001-voice.webm", b"voice-bytes", "voice");
        // The file is handed over and the end follows once it is in.
        h.cmds
            .send(Cmd::File {
                run: run.clone(),
                index: 500,
                entry: file,
            })
            .unwrap();
        h.cmds
            .send(Cmd::Ended {
                run: run.clone(),
                ended: ended("stopped", 1),
            })
            .unwrap();
        let ClientMessage::RecordFile(_) = h.next().await else {
            panic!("the file first")
        };
        let ClientMessage::RecordEnded(e) = h.next().await else {
            panic!("then the end")
        };
        assert_eq!(e.file_base, 500);
        h.quiet().await;
        assert_eq!(h.registry.unreported(), 0, "its end is sent");
        // A resend computed before the end arrived (or after the connection it went on dropped)
        // is answered with the end again, for a while.
        match h.registry.begin(want, 2, &draining) {
            Err(Busy::Resend { end, refuse: None }) => {
                assert_eq!(
                    (end.file_base, end.files, end.reason.as_str()),
                    (500, 1, "stopped")
                );
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn uploads_wait_for_the_rate_of_the_node() {
        let lms = Lms::start().await;
        let mut h = harness("rate", quick());
        // 400 kB at 1 MB/s with a quarter second of burst saved: at least 150 ms.
        let api = lms
            .api()
            .throttled(Arc::new(recorder::throttle::Throttle::new(1_000_000)));
        let run = h.run(api, 0);
        let big = h.file(&run, "big.webm", &vec![9u8; 400_000], "voice");
        let started = Instant::now();
        h.cmds
            .send(Cmd::File {
                run: run.clone(),
                index: 0,
                entry: big,
            })
            .unwrap();
        let ClientMessage::RecordFile(_) = h.next().await else {
            panic!("reported")
        };
        assert!(
            started.elapsed() >= Duration::from_millis(140),
            "{:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn at_the_deadline_a_capture_that_is_over_is_reported_with_the_files_that_are_in() {
        // One recording has its file stored. Another's upload crawls (a rate of 10 bytes a
        // second), so when time runs out its capture is over but its file is not in.
        let lms = Lms::start().await;
        let mut h = harness("deadline", quick());
        h.unfinished.store(1, Ordering::SeqCst);
        let good = h.run(lms.api(), 10);
        let stored = h.file(&good, "a.webm", b"stored", "voice");
        h.cmds
            .send(Cmd::File {
                run: good.clone(),
                index: 10,
                entry: stored,
            })
            .unwrap();
        let ClientMessage::RecordFile(f) = h.next().await else {
            panic!("the first file is reported")
        };
        assert_eq!(f.index, 10);

        let slow = Arc::new(Run {
            id: 2,
            api: lms
                .api()
                .retrying(0, Duration::from_millis(1))
                .throttled(Arc::new(recorder::throttle::Throttle::new(10))),
            dir: h.dir.join("slow"),
            ..Arc::into_inner(h.run(lms.api(), 20)).unwrap()
        });
        let late = h.file(&slow, "b.webm", &vec![1u8; 100_000], "voice");
        h.cmds
            .send(Cmd::File {
                run: slow.clone(),
                index: 20,
                entry: late,
            })
            .unwrap();
        h.cmds
            .send(Cmd::Ended {
                run: slow.clone(),
                ended: ended("call_ended", 1),
            })
            .unwrap();
        h.quiet().await;
        assert_eq!(h.unfinished.load(Ordering::SeqCst), 1, "still uploading");

        h.cmds.send(Cmd::Deadline).unwrap();
        let ClientMessage::RecordEnded(e) = h.next().await else {
            panic!("the end is reported without waiting for the file")
        };
        assert_eq!(
            (e.file_base, e.files, e.reason.as_str()),
            (20, 1, "call_ended")
        );
        // The count goes down once the end is out and the directory removed, just after.
        let gone = tokio::time::Instant::now() + Duration::from_secs(2);
        while h.unfinished.load(Ordering::SeqCst) != 0 && tokio::time::Instant::now() < gone {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(h.unfinished.load(Ordering::SeqCst), 0);
    }
}
