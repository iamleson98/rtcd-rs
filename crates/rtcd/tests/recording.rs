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
        site_url: lms.url.clone(),
        token: "bot-token".into(),
    })
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
