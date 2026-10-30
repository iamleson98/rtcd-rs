//! Recording a call inside the service, end to end: `record_start` over the websocket, emulated
//! browsers speaking over real UDP, files uploaded to a fake LMS, and the reports that come back.

mod common;

use std::path::PathBuf;
use std::time::Duration;

use common::{Participant, TestServer, WsClient};
use recorder::testing::{Lms, REC_ID};
use rtcd_proto::{ClientMessage, RecordEnded, RecordFile, RecordStart, RecordStop};

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn data_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("rtcd-rec-it-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn recording_server(dir: &std::path::Path) -> TestServer {
    let dir = dir.display().to_string();
    TestServer::start_with(move |cfg| {
        cfg.record.enabled = true;
        cfg.record.data_dir = dir;
    })
    .await
}

fn start(lms: &Lms, recording: &str, call: &str, file_base: u32) -> ClientMessage {
    ClientMessage::RecordStart(RecordStart {
        recording_id: recording.into(),
        call_id: call.into(),
        t0: now_ms(),
        file_base,
        max_files: 0,
        site_url: lms.url.clone(),
        token: "bot-token".into(),
    })
}

fn start_limited(
    lms: &Lms,
    recording: &str,
    call: &str,
    file_base: u32,
    max_files: u32,
) -> ClientMessage {
    let ClientMessage::RecordStart(start) = start(lms, recording, call, file_base) else {
        unreachable!()
    };
    ClientMessage::RecordStart(RecordStart { max_files, ..start })
}

/// A report of any recording, read from the connections the participants use.
fn reports(clients: [&WsClient; 2]) -> Vec<ClientMessage> {
    clients
        .iter()
        .flat_map(|c| c.reports.iter().cloned())
        .collect()
}

fn ended(all: &[ClientMessage], recording: &str) -> Option<RecordEnded> {
    all.iter().find_map(|m| match m {
        ClientMessage::RecordEnded(e) if e.recording_id == recording => Some(e.clone()),
        _ => None,
    })
}

fn files(all: &[ClientMessage], recording: &str) -> Vec<RecordFile> {
    let mut f: Vec<_> = all
        .iter()
        .filter_map(|m| match m {
            ClientMessage::RecordFile(f) if f.recording_id == recording => Some(f.clone()),
            _ => None,
        })
        .collect();
    f.sort_by_key(|f| f.index);
    f
}

struct Call {
    s: TestServer,
    wa: WsClient,
    wb: WsClient,
    a: Participant,
    b: Participant,
    lms: Lms,
}

impl Call {
    async fn speak(&mut self, frames: usize) {
        for _ in 0..frames / 5 {
            self.a.speak(&mut self.wa, 5).await;
            self.b.speak(&mut self.wb, 5).await;
        }
    }

    /// Pumps both browsers until `done` holds of the reports.
    async fn until(&mut self, what: &str, done: impl Fn(&[ClientMessage]) -> bool) {
        let end = tokio::time::Instant::now() + Duration::from_secs(30);
        while !done(&reports([&self.wa, &self.wb])) {
            assert!(tokio::time::Instant::now() < end, "never got {what}");
            self.a.pump(&mut self.wa).await;
            self.b.pump(&mut self.wb).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

async fn call_of_two(tag: &str) -> (Call, PathBuf) {
    let dir = data_dir(tag);
    let s = recording_server(&dir).await;
    s.register("mm").await;
    let (mut wa, mut wb) = (s.connect("mm").await, s.connect("mm").await);
    let a = Participant::join(&mut wa, s.media, "alice", "call1", 7001).await;
    let b = Participant::join(&mut wb, s.media, "bob", "call1", 7002).await;
    let lms = Lms::start().await;
    (
        Call {
            s,
            wa,
            wb,
            a,
            b,
            lms,
        },
        dir,
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn a_call_is_recorded_uploaded_and_reported_file_by_file() {
    let (mut c, dir) = call_of_two("whole").await;
    c.wa.send(&start(&c.lms, REC_ID, "call1", 50)).await;
    c.speak(150).await;
    c.wa.send(&ClientMessage::RecordStop(RecordStop {
        recording_id: REC_ID.into(),
    }))
    .await;
    c.until("the end of the recording", |r| ended(r, REC_ID).is_some())
        .await;

    let all = reports([&c.wa, &c.wb]);
    let end = ended(&all, REC_ID).unwrap();
    let got = files(&all, REC_ID);
    assert_eq!(end.reason, "stopped");
    assert_eq!((end.file_base, end.files), (50, 2), "{all:?}");
    assert!(end.duration_ms > 1000, "{end:?}");
    assert_eq!(
        got.iter().map(|f| f.index).collect::<Vec<_>>(),
        [50, 51],
        "every index the end counts was reported"
    );
    let sessions: Vec<_> = got.iter().map(|f| f.session_id.as_str()).collect();
    assert!(
        sessions.contains(&"session-alice") && sessions.contains(&"session-bob"),
        "{got:?}"
    );
    for f in &got {
        assert_eq!(f.kind, "voice");
        assert_eq!(f.user_id, format!("user-{}", f.session_id));
        assert!(f.start_ms >= 0 && f.end_ms > f.start_ms, "{f:?}");
        let stored = c
            .lms
            .state
            .lock()
            .unwrap()
            .uploads
            .get(&f.index.to_string())
            .cloned()
            .expect("a file is stored before it is reported");
        assert_eq!(stored.len() as u64, f.size);
        assert_eq!(
            stored[..4],
            [0x1a, 0x45, 0xdf, 0xa3],
            "a WebM file (EBML header)"
        );
    }
    assert!(
        c.lms.state.lock().unwrap().uploads.len() == 2,
        "nothing else was uploaded"
    );

    // The local copies are gone once the recording is over.
    tokio::time::sleep(Duration::from_millis(300)).await;
    let left: Vec<_> = std::fs::read_dir(&dir)
        .map(|d| d.flatten().collect())
        .unwrap_or_default();
    assert!(left.is_empty(), "left in the data dir: {left:?}");

    let metrics = c.s.http.get(c.s.url("/metrics")).send().await.unwrap();
    let metrics = metrics.text().await.unwrap();
    for line in [
        "rtcd_record_files_written_total 2",
        "rtcd_record_pending_bytes 0",
        "rtcd_record_recordings 0",
    ] {
        assert!(metrics.contains(line), "{line} not in\n{metrics}");
    }
    c.s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recording_asked_for_before_its_call_begins_with_the_call() {
    let dir = data_dir("early");
    let s = recording_server(&dir).await;
    s.register("mm").await;
    let mut wa = s.connect("mm").await;
    let mut wb = s.connect("mm").await;
    let lms = Lms::start().await;
    wa.send(&start(&lms, REC_ID, "call1", 0)).await;
    let a = Participant::join(&mut wa, s.media, "alice", "call1", 7001).await;
    let b = Participant::join(&mut wb, s.media, "bob", "call1", 7002).await;
    let mut c = Call {
        s,
        wa,
        wb,
        a,
        b,
        lms,
    };
    c.speak(100).await;
    c.wa.send(&ClientMessage::RecordStop(RecordStop {
        recording_id: REC_ID.into(),
    }))
    .await;
    c.until("the end", |r| ended(r, REC_ID).is_some()).await;
    let all = reports([&c.wa, &c.wb]);
    assert_eq!(files(&all, REC_ID).len(), 2, "{all:?}");
    c.s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shutdown_mid_recording_uploads_every_file_and_reports_the_end() {
    let (mut c, dir) = call_of_two("shutdown").await;
    c.wa.send(&start(&c.lms, REC_ID, "call1", 70)).await;
    c.speak(100).await;

    // The node is told to stop while the lesson runs and the recording is open.
    let app = c.s.app.take().unwrap();
    let down = tokio::spawn(app.shutdown(Some(Duration::from_secs(30))));
    c.until("the end of the recording", |r| ended(r, REC_ID).is_some())
        .await;
    assert!(
        !c.a.closed && !c.b.closed,
        "reported while the sessions still drain"
    );

    let all = reports([&c.wa, &c.wb]);
    let end = ended(&all, REC_ID).unwrap();
    assert_eq!(
        end.reason, "call_ended",
        "the reason that lets the server carry the recording on elsewhere"
    );
    assert_eq!((end.file_base, end.files), (70, 2), "{all:?}");
    let got = files(&all, REC_ID);
    assert_eq!(got.iter().map(|f| f.index).collect::<Vec<_>>(), [70, 71]);
    for f in &got {
        let stored = c
            .lms
            .state
            .lock()
            .unwrap()
            .uploads
            .get(&f.index.to_string())
            .cloned();
        assert_eq!(
            stored.map(|b| b.len() as u64),
            Some(f.size),
            "{f:?} is stored whole"
        );
    }

    // A recording asked for on a node that is going is refused, so the server tries elsewhere.
    c.wa.send(&start(&c.lms, "late", "call1", 0)).await;
    c.until("the refusal", |r| ended(r, "late").is_some()).await;
    assert_eq!(
        ended(&reports([&c.wa, &c.wb]), "late").unwrap().reason,
        "refused"
    );

    // The lesson's sessions end and the shutdown completes.
    c.wa.leave("session-alice").await;
    c.wb.leave("session-bob").await;
    tokio::time::timeout(Duration::from_secs(30), down)
        .await
        .expect("the shutdown completes")
        .unwrap();
    let left: Vec<_> = std::fs::read_dir(&dir)
        .map(|d| d.flatten().collect())
        .unwrap_or_default();
    assert!(left.is_empty(), "left in the data dir: {left:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_second_recording_of_a_call_is_refused_and_a_repeated_start_changes_nothing() {
    let (mut c, _) = call_of_two("busy").await;
    c.wa.send(&start(&c.lms, REC_ID, "call1", 0)).await;
    c.speak(30).await;
    // The same recording asked for again, and another for the same call.
    c.wa.send(&start(&c.lms, REC_ID, "call1", 0)).await;
    c.wa.send(&start(&c.lms, "other", "call1", 9)).await;
    c.until("the refusal", |r| ended(r, "other").is_some())
        .await;
    let all = reports([&c.wa, &c.wb]);
    let refused = ended(&all, "other").unwrap();
    assert_eq!(
        (refused.reason.as_str(), refused.file_base, refused.files),
        ("refused", 9, 0)
    );
    assert!(ended(&all, REC_ID).is_none(), "the first one goes on");

    // It records on, and ends once, with both files.
    c.speak(60).await;
    c.wa.send(&ClientMessage::RecordStop(RecordStop {
        recording_id: REC_ID.into(),
    }))
    .await;
    c.until("the end", |r| ended(r, REC_ID).is_some()).await;
    c.speak(10).await;
    let all = reports([&c.wa, &c.wb]);
    let ends = all
        .iter()
        .filter(|m| matches!(m, ClientMessage::RecordEnded(e) if e.recording_id == REC_ID))
        .count();
    assert_eq!(ends, 1);
    assert_eq!(files(&all, REC_ID).len(), 2);
    c.s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_recording_that_ends_with_its_call_says_so() {
    let (mut c, _) = call_of_two("callend").await;
    c.wa.send(&start(&c.lms, REC_ID, "call1", 0)).await;
    c.speak(60).await;
    c.wa.leave("session-alice").await;
    c.wb.leave("session-bob").await;
    c.until("the end", |r| ended(r, REC_ID).is_some()).await;
    let all = reports([&c.wa, &c.wb]);
    assert_eq!(ended(&all, REC_ID).unwrap().reason, "call_ended");
    assert_eq!(files(&all, REC_ID).len(), 2);
    c.s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn what_a_dead_process_left_behind_is_removed_at_start() {
    let dir = data_dir("leftovers");
    let old = dir.join("oldrec").join("1");
    std::fs::create_dir_all(&old).unwrap();
    std::fs::write(old.join("00001-voice.webm"), b"half a file").unwrap();
    std::fs::write(dir.join("stray"), b"x").unwrap();
    let s = recording_server(&dir).await;
    let left: Vec<_> = std::fs::read_dir(&dir).unwrap().flatten().collect();
    assert!(left.is_empty(), "{left:?}");
    s.shutdown().await;
}

fn ends(all: &[ClientMessage], recording: &str) -> Vec<RecordEnded> {
    all.iter()
        .filter_map(|m| match m {
            ClientMessage::RecordEnded(e) if e.recording_id == recording => Some(e.clone()),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_start_for_another_number_or_call_than_the_one_held_is_refused_and_its_resend_is_not() {
    let (mut c, _) = call_of_two("held").await;
    c.wa.send(&start(&c.lms, REC_ID, "call1", 20_000)).await;
    c.speak(30).await;
    // The same start twice more (two senders), then one for another number, one for another call.
    c.wa.send(&start(&c.lms, REC_ID, "call1", 20_000)).await;
    c.wb.send(&start(&c.lms, REC_ID, "call1", 20_000)).await;
    c.wa.send(&start(&c.lms, REC_ID, "call1", 20_007)).await;
    c.wa.send(&start(&c.lms, REC_ID, "call2", 40_000)).await;
    c.until("both refusals", |r| {
        ends(r, REC_ID)
            .iter()
            .filter(|e| e.reason == "refused")
            .count()
            == 2
    })
    .await;
    let refused: Vec<_> = ends(&reports([&c.wa, &c.wb]), REC_ID)
        .into_iter()
        .map(|e| (e.reason, e.file_base, e.files))
        .collect();
    assert!(
        refused.contains(&("refused".into(), 20_007, 0)),
        "{refused:?}"
    );
    assert!(
        refused.contains(&("refused".into(), 40_000, 0)),
        "{refused:?}"
    );

    // The run goes on undisturbed and ends once, with its files from its own number.
    c.speak(60).await;
    c.wa.send(&ClientMessage::RecordStop(RecordStop {
        recording_id: REC_ID.into(),
    }))
    .await;
    c.until("the end", |r| {
        ends(r, REC_ID).iter().any(|e| e.reason == "stopped")
    })
    .await;
    c.speak(10).await;
    let all = reports([&c.wa, &c.wb]);
    let real: Vec<_> = ends(&all, REC_ID)
        .into_iter()
        .filter(|e| e.reason != "refused")
        .collect();
    assert_eq!(real.len(), 1, "one end for the one start taken: {all:?}");
    assert_eq!((real[0].file_base, real[0].files), (20_000, 2));
    assert_eq!(
        files(&all, REC_ID)
            .iter()
            .map(|f| f.index)
            .collect::<Vec<_>>(),
        [20_000, 20_001]
    );
    c.s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_new_call_started_while_the_old_run_ends_is_answered_and_taken_when_sent_again() {
    let (mut c, _) = call_of_two("reopen").await;
    c.wa.send(&start(&c.lms, REC_ID, "call1", 0)).await;
    c.speak(30).await;
    // The class reconnects: the next call's start comes before the old run has ended, and is
    // answered at once instead of being dropped.
    c.wa.send(&start(&c.lms, REC_ID, "call2", 20_000)).await;
    c.until("the refusal", |r| {
        ends(r, REC_ID).iter().any(|e| e.reason == "refused")
    })
    .await;
    let refused = ends(&reports([&c.wa, &c.wb]), REC_ID);
    assert_eq!(
        (refused[0].file_base, refused[0].files),
        (20_000, 0),
        "{refused:?}"
    );

    // The old call ends; its run says so; the start sent again is taken.
    c.wa.leave("session-alice").await;
    c.wb.leave("session-bob").await;
    c.until("the old run's end", |r| {
        ends(r, REC_ID).iter().any(|e| e.reason == "call_ended")
    })
    .await;
    let mut wa = c.s.connect("mm").await;
    let mut wb = c.s.connect("mm").await;
    let a = common::Participant::join(&mut wa, c.s.media, "alice", "call2", 7003).await;
    let b = common::Participant::join(&mut wb, c.s.media, "bob", "call2", 7004).await;
    c.wa = wa;
    c.wb = wb;
    c.a = a;
    c.b = b;
    c.wa.send(&start(&c.lms, REC_ID, "call2", 20_000)).await;
    c.speak(60).await;
    c.wa.send(&ClientMessage::RecordStop(RecordStop {
        recording_id: REC_ID.into(),
    }))
    .await;
    c.until("the new run's end", |r| {
        ends(r, REC_ID).iter().any(|e| e.reason == "stopped")
    })
    .await;
    let all = reports([&c.wa, &c.wb]);
    let stopped = ends(&all, REC_ID)
        .into_iter()
        .find(|e| e.reason == "stopped")
        .unwrap();
    assert_eq!((stopped.file_base, stopped.files), (20_000, 2), "{all:?}");
    c.s.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_run_never_takes_a_number_past_its_file_limit() {
    let (mut c, _) = call_of_two("limit").await;
    // Room for one file: alice's voice takes it, bob's has none and is left out.
    c.wa.send(&start_limited(&c.lms, REC_ID, "call1", 300, 1))
        .await;
    c.speak(150).await;
    c.wa.send(&ClientMessage::RecordStop(RecordStop {
        recording_id: REC_ID.into(),
    }))
    .await;
    c.until("the end", |r| ends(r, REC_ID).len() == 1).await;
    let all = reports([&c.wa, &c.wb]);
    let end = ends(&all, REC_ID).remove(0);
    assert_eq!((end.file_base, end.files), (300, 1), "{end:?}");
    let got = files(&all, REC_ID);
    assert_eq!(got.iter().map(|f| f.index).collect::<Vec<_>>(), [300]);
    let uploads = c
        .lms
        .state
        .lock()
        .unwrap()
        .uploads
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(uploads, ["300"], "nothing was uploaded past the limit");
    c.s.shutdown().await;
}
