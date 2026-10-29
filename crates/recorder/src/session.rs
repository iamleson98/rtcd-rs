//! One recording in progress: the event loop that keeps the recorder in the call until the
//! recording ends, and the run around it (sign in, record, upload, complete).

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use tokio::time::{interval_at, timeout};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::api::Api;
use crate::call::{Call, Phase, REJOIN_AFTER};
use crate::config::Config;
use crate::media::Recording;
use crate::socket::{EVENT_RESYNC, Event, Socket};
use crate::upload::manifest_files;

/// Bounds getting into the room (sign-in).
pub const READY_TIMEOUT: Duration = Duration::from_secs(90);
/// A call nobody but the recorder is in ends the recording after this long.
pub const EMPTY_ROOM_AFTER: Duration = Duration::from_secs(120);
/// Re-reads the recording's status, in case the stop event was missed.
const STATUS_EVERY: Duration = Duration::from_secs(30);
/// Bounds one call to the server made from the loop.
const CALL_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error)]
pub enum RunError {
    /// SIGTERM: the server removed the job.
    #[error("stopped by the server")]
    Stopped,
    #[error("{0}")]
    Failed(String),
}

fn failed(what: &str, e: impl std::fmt::Display) -> RunError {
    RunError::Failed(format!("{what}: {e}"))
}

/// Records the session, then uploads and completes the recording.
pub async fn run(
    cancel: CancellationToken,
    cfg: Config,
    api: Api,
    dir: &Path,
) -> Result<(), RunError> {
    let room = timeout(READY_TIMEOUT, api.room())
        .await
        .map_err(|_| failed("read the room", "timed out"))?
        .map_err(|e| failed("read the room", e))?;

    let sock_cancel = cancel.child_token();
    let mut socket = Socket::start(&cfg, sock_cancel.clone());
    tokio::select! {
        () = socket.wait_ready() => {}
        () = cancel.cancelled() => return Err(RunError::Stopped),
        () = tokio::time::sleep(READY_TIMEOUT) => {
            return Err(RunError::Failed("the realtime connection did not open".into()));
        }
    }

    let work = dir.join(&cfg.recording_id);
    std::fs::create_dir_all(&work).map_err(|e| RunError::Failed(e.to_string()))?;
    let _cleanup = RemoveOnDrop(work.clone());

    timeout(Duration::from_secs(60), api.started())
        .await
        .map_err(|_| failed("report the start", "timed out"))?
        .map_err(|e| failed("report the start", e))?;

    let start = Instant::now();
    let rec = Arc::new(Recording::new(&work, start, cfg.capture_limit()));
    let call = Call::new(
        &room.chat_channel_id,
        socket.sender.clone(),
        rec.clone(),
        start,
    );
    let mut s = Session {
        cfg: cfg.clone(),
        api: api.clone(),
        room,
        rec: rec.clone(),
        call,
        call_running: false,
        was_in_call: false,
        alone_since: None,
    };

    let reason = s.event_loop(&cancel, &mut socket).await;
    let end = Instant::now();
    s.call.leave(end).await;
    let files = rec.files();
    sock_cancel.cancel();
    if reason == "terminated" {
        return Err(RunError::Stopped);
    }
    info!(
        reason,
        media_files = files.len(),
        bytes = rec.disk.written(),
        "recording ended"
    );
    let duration = end.saturating_duration_since(start);
    let files = manifest_files(files).map_err(|e| RunError::Failed(e.to_string()))?;
    // A lesson in which no one spoke or filmed has nothing to upload; the server adds the board.
    let multipart = if files.is_empty() {
        Vec::new()
    } else {
        tokio::select! {
            r = api.upload(&files) => r.map_err(|e| failed("upload", e))?,
            () = cancel.cancelled() => return Err(RunError::Stopped),
        }
    };
    tokio::select! {
        r = api.complete(duration, &files, &multipart) => r.map_err(|e| failed("complete", e))?,
        () = cancel.cancelled() => return Err(RunError::Stopped),
    }
    Ok(())
}

struct RemoveOnDrop(std::path::PathBuf);

impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if let Err(e) = std::fs::remove_dir_all(&self.0) {
            warn!(dir = %self.0.display(), error = %e, "could not remove the working files");
        }
    }
}

#[derive(Debug)]
pub struct Session {
    pub cfg: Config,
    pub api: Api,
    pub room: crate::api::Room,
    pub rec: Arc<Recording>,
    pub call: Call,
    pub call_running: bool,
    /// The recorder joined the call at least once, so the end of the call ends the recording
    /// (before that, the teacher may not have started it yet).
    pub was_in_call: bool,
    pub alone_since: Option<Instant>,
}

impl Session {
    /// Runs until the recording should end and says why.
    pub async fn event_loop(
        &mut self,
        cancel: &CancellationToken,
        socket: &mut Socket,
    ) -> &'static str {
        let mut limit = self.cfg.max_duration;
        if self.room.stop_at > 0 {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis() as i64);
            limit = limit.min(Duration::from_millis(
                (self.room.stop_at - now_ms).max(0) as u64
            ));
        }
        let limit = tokio::time::sleep(limit);
        tokio::pin!(limit);
        let mut tick = interval_at(
            tokio::time::Instant::now() + Duration::from_secs(1),
            Duration::from_secs(1),
        );
        let mut status = interval_at(tokio::time::Instant::now() + STATUS_EVERY, STATUS_EVERY);
        let disk = self.rec.disk.clone();

        self.check_call(Instant::now()).await;
        loop {
            tokio::select! {
                () = cancel.cancelled() => return "terminated",
                () = &mut limit => return "time",
                () = disk.full() => {
                    warn!(bytes = disk.written(), "the recording reached its disk limit; uploading what it has");
                    return "disk";
                }
                _ = status.tick() => {
                    if self.stopped_on_server().await {
                        return "stopped";
                    }
                }
                _ = tick.tick() => {
                    let reason = self.tick(Instant::now()).await;
                    if !reason.is_empty() {
                        return reason;
                    }
                }
                ev = socket.events.recv() => {
                    let Some(ev) = ev else { return "terminated" };
                    let reason = self.handle(ev).await;
                    if !reason.is_empty() {
                        return reason;
                    }
                }
                ev = self.call.peer_event() => self.call.on_peer_event(ev),
            }
        }
    }

    pub async fn tick(&mut self, now: Instant) -> &'static str {
        if self.call.stalled(now) {
            warn!("could not get into the call; trying again");
            self.call.leave(now).await;
        }
        if self.call_running
            && self.call.phase == Phase::Idle
            && now.saturating_duration_since(self.call.phase_at) >= REJOIN_AFTER
        {
            self.call.join(now);
        }
        if self.call.phase == Phase::Joined && self.call.alone() {
            match self.alone_since {
                None => self.alone_since = Some(now),
                Some(since) if now.saturating_duration_since(since) >= EMPTY_ROOM_AFTER => {
                    return "empty";
                }
                Some(_) => {}
            }
        } else {
            self.alone_since = None;
        }
        ""
    }

    pub async fn handle(&mut self, ev: Event) -> &'static str {
        let now = Instant::now();
        let in_room = ev.broadcast.channel_id == self.room.chat_channel_id;
        match ev.event.as_str() {
            EVENT_RESYNC => {
                self.call.reconnected();
                self.check_call(now).await;
                if self.stopped_on_server().await {
                    return "stopped";
                }
            }
            "custom_lms_recording_stop" => {
                #[derive(Deserialize)]
                struct Stop {
                    #[serde(default)]
                    recording_id: String,
                }
                if serde_json::from_str::<Stop>(ev.data())
                    .is_ok_and(|d| d.recording_id == self.cfg.recording_id)
                {
                    return "stopped";
                }
            }
            "custom_calls_call_start" if in_room => {
                self.call_running = true;
                if self.call.phase == Phase::Idle {
                    self.call.join(now);
                }
            }
            "custom_calls_call_end" if in_room => {
                self.call_running = false;
                if self.was_in_call {
                    return "call-ended";
                }
            }
            "custom_calls_join" => {
                if let Some(data) = &ev.data {
                    self.call.joined(data, now).await;
                }
                self.was_in_call = self.was_in_call || self.call.phase == Phase::Joined;
            }
            "custom_calls_error" => {
                // A refused join (the call ended in between): try again later.
                if self.call.phase == Phase::Joining {
                    self.call.leave(now).await;
                }
            }
            "custom_calls_signal" => {
                if let Some(data) = &ev.data {
                    self.call.signal(data);
                }
            }
            "custom_calls_call_state" => {
                if let Some(data) = &ev.data {
                    self.call.state(data);
                }
            }
            "custom_calls_user_joined" if in_room => {
                if let Some(data) = &ev.data {
                    self.call.presence(data, true);
                }
            }
            "custom_calls_user_left" if in_room => {
                if let Some(data) = &ev.data {
                    self.call.presence(data, false);
                }
            }
            "custom_calls_user_video_off" if in_room => {
                if let Some(data) = &ev.data {
                    self.call.stopped(data, crate::media::CAMERA);
                }
            }
            "custom_calls_user_screen_off" if in_room => {
                if let Some(data) = &ev.data {
                    self.call.stopped(data, crate::media::SCREEN);
                    // Its sound goes with it.
                    self.call.stopped(data, crate::media::SCREEN_AUDIO);
                }
            }
            "custom_calls_call_start"
            | "custom_calls_call_end"
            | "custom_calls_user_joined"
            | "custom_calls_user_left" => {}
            _ => {}
        }
        ""
    }

    /// Reads whether the class call runs, and joins it if so.
    pub async fn check_call(&mut self, now: Instant) {
        let running = match timeout(
            CALL_TIMEOUT,
            self.api.call_active(&self.room.chat_channel_id),
        )
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => {
                warn!(error = %e, "cannot read the call");
                return;
            }
            Err(_) => {
                warn!("cannot read the call: timed out");
                return;
            }
        };
        self.call_running = running;
        if running && self.call.phase == Phase::Idle {
            self.call.join(now);
        }
    }

    pub async fn stopped_on_server(&self) -> bool {
        match timeout(CALL_TIMEOUT, self.api.room()).await {
            Ok(Ok(rm)) => rm.status != "STARTING" && rm.status != "RECORDING",
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use serde_json::json;
    use serde_json::value::RawValue;

    use super::*;
    use crate::socket::Broadcast;
    use crate::testing::{CHAN_ID, Lms, REC_ID, REC_PATH, SESS_ID};

    fn work_dir(tag: &str) -> std::path::PathBuf {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let d = std::env::temp_dir().join(format!("session-{}-{n}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn it_records_and_completes() {
        let lms = Lms::start().await;
        let stop = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 1500;
        lms.set(|s| s.stop_at_ms = stop);
        let cfg = lms.config();
        run(CancellationToken::new(), cfg, lms.api(), &work_dir("run"))
            .await
            .unwrap();

        assert!(
            lms.has(&format!("POST {REC_PATH}/started"))
                && lms.has(&format!("POST {REC_PATH}/complete")),
            "{:?}",
            lms.paths()
        );
        assert!(
            !lms.paths().iter().any(|p| p.contains("whiteboard")),
            "the server logs the board; the recorder does not read it"
        );
        let st = lms.state.lock().unwrap();
        let done = st.complete.clone().unwrap();
        assert!(
            done["files"].as_array().unwrap().is_empty(),
            "no one spoke or filmed: nothing to upload"
        );
        let d = done["duration_ms"].as_i64().unwrap();
        assert!(
            (1000..4000).contains(&d),
            "duration {d} ms for a recording that ran about 1.5 s"
        );
    }

    #[tokio::test]
    async fn the_server_can_stop_it() {
        let lms = Lms::start().await;
        lms.on_socket(|tx| async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let _ = tx.send(json!({"event": "custom_lms_recording_stop", "seq": 1, "data": {"recording_id": "someone-elses-recording00"}}));
            let _ = tx.send(json!({"event": "custom_lms_recording_stop", "seq": 2, "data": {"recording_id": REC_ID}}));
        });
        let started = std::time::Instant::now();
        run(
            CancellationToken::new(),
            lms.config(),
            lms.api(),
            &work_dir("stop"),
        )
        .await
        .unwrap();
        assert!(started.elapsed() < Duration::from_secs(20));
        assert!(
            lms.has(&format!("POST {REC_PATH}/complete")),
            "a stop for this recording ends it and uploads what it has"
        );
    }

    #[tokio::test]
    async fn removing_the_job_ends_it_without_uploading() {
        let lms = Lms::start().await;
        let cancel = CancellationToken::new();
        let c2 = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(500)).await;
            c2.cancel();
        });
        let dir = work_dir("removed");
        let err = run(cancel, lms.config(), lms.api(), &dir)
            .await
            .unwrap_err();
        assert!(matches!(err, RunError::Stopped), "{err:?}");
        assert!(
            !lms.has(&format!("POST {REC_PATH}/complete")),
            "a removed recording must not be completed"
        );
        assert_eq!(
            std::fs::read_dir(&dir).unwrap().count(),
            0,
            "the working files must be removed"
        );
    }

    #[tokio::test]
    async fn it_fails_where_it_cannot_start() {
        let lms = Lms::start().await;
        lms.set(|s| s.room_code = 404);
        let err = run(
            CancellationToken::new(),
            lms.config(),
            lms.api(),
            &work_dir("noroom"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("read the room"), "{err}");

        let lms = Lms::start().await;
        lms.set(|s| s.start_err = 409);
        let err = run(
            CancellationToken::new(),
            lms.config(),
            lms.api(),
            &work_dir("nostart"),
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("report the start"), "{err}");
        assert!(
            !lms.has(&format!("POST {REC_PATH}/complete")),
            "nothing to complete"
        );
    }

    // ── The parts of the loop, one at a time ──

    async fn test_session() -> (Session, Lms, CancellationToken, Socket) {
        let lms = Lms::start().await;
        let cfg = lms.config();
        let cancel = CancellationToken::new();
        let socket = Socket::start(&cfg, cancel.clone());
        socket.wait_ready().await;
        while socket.sender.send("ping", ()).is_err() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let now = Instant::now();
        let rec = Arc::new(Recording::new(&work_dir("unit"), now, u64::MAX));
        let call = Call::new(CHAN_ID, socket.sender.clone(), rec.clone(), now);
        let room = crate::api::Room {
            session_id: SESS_ID.into(),
            chat_channel_id: CHAN_ID.into(),
            stop_at: 0,
            status: "RECORDING".into(),
        };
        let s = Session {
            api: lms.api(),
            cfg,
            room,
            rec,
            call,
            call_running: false,
            was_in_call: false,
            alone_since: None,
        };
        (s, lms, cancel, socket)
    }

    fn evt(name: &str, channel: &str, data: &str) -> Event {
        Event {
            event: name.into(),
            data: Some(RawValue::from_string(data.into()).unwrap()),
            broadcast: Broadcast {
                channel_id: channel.into(),
            },
            seq: 0,
        }
    }

    async fn actions_after(lms: &Lms, n: usize) -> Vec<String> {
        for _ in 0..200 {
            let a: Vec<String> = lms
                .actions()
                .into_iter()
                .filter(|a| a != "ping" && a != "custom_calls_ice")
                .collect();
            if a.len() >= n {
                return a;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("only {:?} of {n} actions", lms.actions());
    }

    #[tokio::test]
    async fn call_events_move_the_recorder_in_and_out_of_the_call() {
        let (mut s, lms, cancel, _sock) = test_session().await;

        assert_eq!(
            s.handle(evt("custom_calls_call_start", "another-channel", "{}"))
                .await,
            ""
        );
        assert!(
            !s.call_running && s.call.phase == Phase::Idle,
            "another channel's call is none of ours"
        );
        assert_eq!(
            s.handle(evt("custom_calls_call_start", CHAN_ID, "{}"))
                .await,
            ""
        );
        assert!(
            s.call_running && s.call.phase == Phase::Joining,
            "our class's call starting: join it"
        );
        assert_eq!(actions_after(&lms, 1).await, ["custom_calls_join"]);

        // A refused join (the call ended in between) goes back to idle.
        s.handle(evt("custom_calls_error", CHAN_ID, "{}")).await;
        assert_eq!(s.call.phase, Phase::Idle);
        s.handle(evt("custom_calls_error", CHAN_ID, "{}")).await;

        // The call ends before the recorder ever got in: keep waiting.
        assert_eq!(
            s.handle(evt("custom_calls_call_end", CHAN_ID, "{}")).await,
            ""
        );
        assert!(!s.call_running);

        // Joined once: the end of the call ends the recording.
        s.call.set_phase(Phase::Joining, Instant::now());
        s.handle(evt(
            "custom_calls_join",
            CHAN_ID,
            r#"{"connID":"me","iceServers":[]}"#,
        ))
        .await;
        assert!(
            s.was_in_call && s.call.phase == Phase::Joined,
            "joined: remember it"
        );
        assert_eq!(
            s.handle(evt("custom_calls_call_end", "another-channel", "{}"))
                .await,
            ""
        );
        assert_eq!(
            s.handle(evt("custom_calls_call_end", CHAN_ID, "{}")).await,
            "call-ended"
        );

        s.handle(evt(
            "custom_calls_call_state",
            CHAN_ID,
            r#"{"call":"{\"sessions\":[{\"id\":\"s9\",\"user_id\":\"u9\"}]}"}"#,
        ))
        .await;
        assert!(!s.call.alone(), "call_state must reach the call");
        s.handle(evt(
            "custom_calls_user_left",
            "another-channel",
            r#"{"user_id":"u9","session_id":"s9"}"#,
        ))
        .await;
        assert!(
            !s.call.alone(),
            "another channel's departures are none of ours"
        );
        s.handle(evt(
            "custom_calls_user_left",
            CHAN_ID,
            r#"{"user_id":"u9","session_id":"s9"}"#,
        ))
        .await;
        assert!(s.call.alone(), "the last person left");
        s.handle(evt(
            "custom_calls_user_joined",
            CHAN_ID,
            r#"{"user_id":"u8","session_id":"s8"}"#,
        ))
        .await;
        assert!(!s.call.alone(), "a person joined");
        s.handle(evt("custom_calls_signal", CHAN_ID, r#"{"data":""}"#))
            .await; // nothing to apply
        s.call.leave(Instant::now()).await;
        cancel.cancel();
    }

    #[tokio::test]
    async fn joining_offers_then_mutes_and_leaving_says_so_once() {
        let (mut s, lms, cancel, _sock) = test_session().await;
        let now = Instant::now();
        s.call.join(now);
        assert_eq!(s.call.phase, Phase::Joining);
        // An ack with no connection id, or garbage, builds nothing.
        let raw = |t: &str| RawValue::from_string(t.into()).unwrap();
        s.call.joined(&raw(r#"{"iceServers":[]}"#), now).await;
        s.call.joined(&raw(r#""nope""#), now).await;
        assert!(
            s.call.phase == Phase::Joining && !s.call.has_peer(),
            "an unusable ack must be ignored"
        );

        s.call
            .joined(&raw(r#"{"connID":"call-session-1","iceServers":[]}"#), now)
            .await;
        assert!(
            s.call.phase == Phase::Joined
                && s.call.session_id == "call-session-1"
                && s.call.has_peer()
        );
        // A second ack for the same attempt is not a second connection.
        s.call
            .joined(&raw(r#"{"connID":"call-session-2","iceServers":[]}"#), now)
            .await;
        assert_eq!(s.call.session_id, "call-session-1");
        let got = actions_after(&lms, 3).await;
        assert_eq!(
            got[..3],
            ["custom_calls_join", "custom_calls_sdp", "custom_calls_mute"],
            "after the ack the recorder offers, then mutes itself"
        );

        s.call.leave(now).await;
        assert!(s.call.phase == Phase::Idle && !s.call.has_peer() && s.call.session_id.is_empty());
        s.call.leave(now).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let leaves = lms
            .actions()
            .iter()
            .filter(|a| *a == "custom_calls_leave")
            .count();
        assert_eq!(leaves, 1, "leaving twice must say it once");
        cancel.cancel();
    }

    #[tokio::test]
    async fn another_recordings_stop_is_not_ours() {
        let (mut s, _lms, cancel, _sock) = test_session().await;
        assert_eq!(
            s.handle(evt(
                "custom_lms_recording_stop",
                "",
                r#"{"recording_id":"other"}"#
            ))
            .await,
            ""
        );
        assert_eq!(
            s.handle(evt("custom_lms_recording_stop", "", "\"nope\""))
                .await,
            ""
        );
        assert_eq!(
            s.handle(evt(
                "custom_lms_recording_stop",
                "",
                &format!(r#"{{"recording_id":"{REC_ID}"}}"#)
            ))
            .await,
            "stopped"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn a_resync_reads_the_call_and_the_status_again() {
        let (mut s, lms, cancel, _sock) = test_session().await;
        lms.set(|l| l.call_up = true);
        assert_eq!(
            s.handle(Event::named(EVENT_RESYNC)).await,
            "",
            "a resync with the recording still going"
        );
        assert!(
            s.call_running && s.call.phase == Phase::Joining,
            "the call is checked and joined"
        );
        assert_eq!(actions_after(&lms, 1).await, ["custom_calls_join"]);

        lms.set(|l| l.status = "COMPLETED".into());
        assert_eq!(
            s.handle(Event::named(EVENT_RESYNC)).await,
            "stopped",
            "a resync that finds the recording over"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn the_servers_status_decides_whether_the_recording_is_over() {
        let (s, lms, cancel, _sock) = test_session().await;
        for (status, want) in [
            ("STARTING", false),
            ("RECORDING", false),
            ("COMPLETED", true),
            ("FAILED", true),
            ("CANCELLED", true),
        ] {
            lms.set(|l| l.status = status.into());
            assert_eq!(s.stopped_on_server().await, want, "status {status}");
        }
        lms.set(|l| l.room_code = 500);
        assert!(
            !s.stopped_on_server().await,
            "an unreadable status must not end the recording"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn a_failing_api_changes_nothing() {
        let (mut s, _lms, cancel, _sock) = test_session().await;
        s.api = Api::new(&Config {
            site_url: "http://127.0.0.1:1".into(),
            ..s.cfg.clone()
        });
        s.call_running = true;
        s.check_call(Instant::now()).await;
        assert!(
            s.call_running,
            "an unreadable call state must not change what we know"
        );
        cancel.cancel();
    }

    #[tokio::test]
    async fn the_tick_rejoins_drops_stalled_joins_and_ends_an_empty_room() {
        let (mut s, lms, cancel, _sock) = test_session().await;
        let t0 = Instant::now();

        // Idle with a running call: rejoin only after the wait.
        s.call_running = true;
        s.call.set_phase(Phase::Idle, t0);
        s.tick(t0 + REJOIN_AFTER - Duration::from_millis(1)).await;
        assert_eq!(s.call.phase, Phase::Idle, "too soon to try again");
        s.tick(t0 + REJOIN_AFTER).await;
        assert_eq!(s.call.phase, Phase::Joining, "time to try again");
        actions_after(&lms, 1).await;

        // A join that never got its ack is dropped.
        s.tick(t0 + REJOIN_AFTER + crate::call::JOIN_ACK_TIMEOUT + Duration::from_secs(1))
            .await;
        assert_eq!(s.call.phase, Phase::Idle, "a stalled join must be left");
        assert_eq!(actions_after(&lms, 2).await[1], "custom_calls_leave");

        // Alone in the call: the recording ends after the wait, not before.
        s.call_running = false;
        s.call.force(Phase::Joined, "me", t0);
        s.call.on_peer_event(crate::peer::PeerEvent::State(
            rtc::peer_connection::state::RTCPeerConnectionState::Connected,
        ));
        let base = t0 + Duration::from_secs(3600);
        assert_eq!(s.tick(base).await, "");
        assert!(s.alone_since.is_some(), "start counting");
        assert_eq!(
            s.tick(base + EMPTY_ROOM_AFTER - Duration::from_millis(1))
                .await,
            "",
            "not yet"
        );
        s.call.presence(
            &RawValue::from_string(r#"{"user_id":"u","session_id":"s1"}"#.into()).unwrap(),
            true,
        );
        assert_eq!(
            s.tick(base + EMPTY_ROOM_AFTER + Duration::from_secs(3600))
                .await,
            ""
        );
        assert!(
            s.alone_since.is_none(),
            "someone is back: the count starts over"
        );
        s.call.presence(
            &RawValue::from_string(r#"{"user_id":"u","session_id":"s1"}"#.into()).unwrap(),
            false,
        );
        s.tick(base).await;
        assert_eq!(s.tick(base + EMPTY_ROOM_AFTER).await, "empty");
        cancel.cancel();
    }

    #[tokio::test]
    async fn the_loop_ends_for_each_reason() {
        // The room's stop time.
        let (mut s, _l, cancel, mut sock) = test_session().await;
        s.room.stop_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 100;
        assert_eq!(
            s.event_loop(&CancellationToken::new(), &mut sock).await,
            "time"
        );
        // The longest recording allowed.
        s.room.stop_at = 0;
        s.cfg.max_duration = Duration::from_millis(100);
        assert_eq!(
            s.event_loop(&CancellationToken::new(), &mut sock).await,
            "time"
        );
        // A full disk.
        s.cfg.max_duration = Duration::from_secs(60);
        s.rec.disk.wrote(u64::MAX / 2);
        let full = Recording::new(&std::env::temp_dir(), Instant::now(), 10);
        full.disk.wrote(11);
        s.rec = Arc::new(full);
        assert_eq!(
            s.event_loop(&CancellationToken::new(), &mut sock).await,
            "disk"
        );
        // Termination.
        let stop = CancellationToken::new();
        stop.cancel();
        s.rec = Arc::new(Recording::new(
            &std::env::temp_dir(),
            Instant::now(),
            u64::MAX,
        ));
        assert_eq!(s.event_loop(&stop, &mut sock).await, "terminated");
        cancel.cancel();
    }
}
