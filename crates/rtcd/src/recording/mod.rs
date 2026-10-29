//! Recording calls in rtcd itself, without a recorder participant.
//!
//! The SFU copies each recorded stream's packets to a queue (`rtcd_sfu::record`); this module takes
//! it from there:
//!
//! * [`writer`]: threads that drain the queues and write WebM files with the `recorder` crate,
//!   rotating them every ten minutes and numbering them in the order they close;
//! * [`uploader`]: a task that sends each closed file to the server with the recording's bot
//!   session, then reports it, then deletes it, under one bandwidth cap for the whole node;
//! * [`reports`]: what is told to the server over the clients' websockets, kept until a
//!   connection can carry it.
//!
//! [`Recordings`] is what the service talks to: `record_start` and `record_stop` come in, files
//! and the end of the recording go out as `record_file` and `record_ended`.

mod pending;
mod reports;
mod uploader;
mod writer;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use recorder::api::{Api, HttpClients};
use recorder::media::Entry;
use recorder::throttle::Throttle;
use rtcd_proto::{ClientMessage, RecordEnded, RecordStart};
use rtcd_sfu::record::{DEFAULT_QUEUE_BYTES, record_channel_for};
use rtcd_sfu::runtime::Sfu;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, info, warn};

pub use reports::Reports;

use self::pending::Pending;
use self::writer::{Job, Links, Settings, WriterPool};
use crate::config::RecordConfig;
use crate::metrics::Metrics;

/// One capture of a call, from its `record_start` to the end of its media. The same recording may
/// be captured more than once (the call ends and starts again), each time as a run of its own.
#[derive(Debug)]
pub struct Run {
    /// Unique in this process.
    pub id: u64,
    pub recording_id: String,
    /// The client that asked, and so the one that is told.
    pub client_id: String,
    pub call_id: String,
    /// The index the run's first file gets.
    pub file_base: u32,
    /// Where the run's files are written until they are uploaded.
    pub dir: PathBuf,
    /// Calls the server with the recording's bot session.
    pub api: Api,
    /// The recording's start.
    pub t0: Instant,
}

/// How a capture ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ended {
    /// What `record_ended` says; empty when the server is to hear nothing.
    pub reason: &'static str,
    /// The indexes the files took, counted from the run's `file_base`.
    pub files: u32,
    pub duration_ms: i64,
    pub dropped: u64,
}

/// What the writers tell the uploader.
#[derive(Debug)]
pub enum Cmd {
    /// A file closed: upload it as object `index`.
    File {
        run: Arc<Run>,
        index: u32,
        entry: Entry,
    },
    /// The capture is over; no more files come.
    Ended { run: Arc<Run>, ended: Ended },
    /// An upload task finished with a file (stored or given up on).
    Uploaded { run: u64 },
}

/// Why a recording could not be registered.
#[derive(Debug, PartialEq, Eq)]
pub enum Busy {
    /// This recording is captured already.
    Same,
    /// Another recording of the same call is.
    Other,
}

#[derive(Debug)]
struct Active {
    run: u64,
    client_id: String,
    call_id: String,
}

/// The captures running on this node.
#[derive(Debug, Default)]
pub struct Registry {
    active: Mutex<HashMap<String, Active>>,
}

impl Registry {
    pub fn begin(
        &self,
        recording_id: &str,
        client_id: &str,
        call_id: &str,
        run: u64,
    ) -> Result<(), Busy> {
        let mut active = self.active.lock();
        if active.contains_key(recording_id) {
            return Err(Busy::Same);
        }
        if active
            .values()
            .any(|a| a.client_id == client_id && a.call_id == call_id)
        {
            return Err(Busy::Other);
        }
        active.insert(
            recording_id.to_owned(),
            Active {
                run,
                client_id: client_id.to_owned(),
                call_id: call_id.to_owned(),
            },
        );
        Ok(())
    }

    /// The capture is over (only if `run` is the one registered).
    pub fn remove(&self, recording_id: &str, run: u64) {
        let mut active = self.active.lock();
        if active.get(recording_id).is_some_and(|a| a.run == run) {
            active.remove(recording_id);
        }
    }

    /// The call a client's recording captures.
    pub fn call_of(&self, client_id: &str, recording_id: &str) -> Option<String> {
        self.active
            .lock()
            .get(recording_id)
            .filter(|a| a.client_id == client_id)
            .map(|a| a.call_id.clone())
    }

    pub fn len(&self) -> usize {
        self.active.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// What exists on a node that records.
struct Live {
    writers: WriterPool,
    uploads: UnboundedSender<Cmd>,
    pending: Arc<Pending>,
    registry: Arc<Registry>,
    clients: HttpClients,
    throttle: Arc<Throttle>,
    runs: AtomicU64,
    data_dir: PathBuf,
    max_bytes: u64,
}

/// Recording on this node.
pub struct Recordings {
    live: Option<Live>,
    reports: Arc<Reports>,
    metrics: Arc<Metrics>,
}

impl std::fmt::Debug for Recordings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Recordings")
            .field("enabled", &self.live.is_some())
            .finish_non_exhaustive()
    }
}

/// Removes what a dead process left in `dir`, with one warning.
fn clear_leftovers(dir: &std::path::Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    let mut removed = 0;
    for e in entries.flatten() {
        let path = e.path();
        let gone = if path.is_dir() {
            std::fs::remove_dir_all(&path)
        } else {
            std::fs::remove_file(&path)
        };
        match gone {
            Ok(()) => removed += 1,
            Err(err) => {
                warn!(path = %path.display(), error = %err, "cannot remove a leftover of an earlier recording")
            }
        }
    }
    if removed > 0 {
        warn!(dir = %dir.display(), removed, "removed what a process that died left of its recordings; they were not uploaded");
    }
}

/// The recording's start on this node's monotonic clock, from its start in epoch milliseconds.
fn instant_of(t0_ms: i64, now: Instant, wall: SystemTime) -> Instant {
    let now_ms = wall
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as i64);
    let ago = Duration::from_millis(now_ms.saturating_sub(t0_ms).max(0) as u64);
    now.checked_sub(ago).unwrap_or(now)
}

/// Ids name directories and appear in URL paths.
fn valid_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn valid_site(url: &str) -> bool {
    url.split_once("://").is_some_and(|(scheme, rest)| {
        (scheme == "http" || scheme == "https") && !rest.split('/').next().unwrap_or("").is_empty()
    })
}

impl Recordings {
    /// Sets recording up as the configuration says. Must be called inside the runtime (the
    /// uploader is a task of it).
    pub fn new(cfg: &RecordConfig, reports: Arc<Reports>, metrics: Arc<Metrics>) -> Self {
        if !cfg.enabled {
            return Self {
                live: None,
                reports,
                metrics,
            };
        }
        let data_dir = PathBuf::from(&cfg.data_dir);
        if let Err(e) = std::fs::create_dir_all(&data_dir) {
            error!(dir = %data_dir.display(), error = %e, "cannot create the recordings directory; recordings will fail");
        }
        clear_leftovers(&data_dir);
        let pending = Arc::new(Pending::new(
            cfg.max_pending_bytes.max(1) as u64,
            metrics.clone(),
        ));
        let uploads = uploader::spawn(uploader::Deps {
            reports: reports.clone(),
            pending: pending.clone(),
            metrics: metrics.clone(),
            policy: uploader::Policy::default(),
        });
        // The writers only append to files; a few threads serve every recording.
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        info!(dir = %data_dir.display(), "recording is enabled on this node");
        Self {
            live: Some(Live {
                writers: WriterPool::start((cores / 2).clamp(1, 4)),
                uploads,
                pending,
                registry: Arc::new(Registry::default()),
                clients: HttpClients::new(),
                throttle: Arc::new(Throttle::new(
                    (cfg.upload_mbps.max(1) as u64).saturating_mul(125_000),
                )),
                runs: AtomicU64::new(0),
                data_dir,
                max_bytes: cfg.max_bytes.max(1) as u64,
            }),
            reports,
            metrics,
        }
    }

    /// Tells the client its recording could not be captured.
    fn refuse(&self, client_id: &str, req: &RecordStart, reason: &str, why: &str) {
        warn!(recording = %req.recording_id, call = %req.call_id, why, "not recording");
        self.reports.send(
            client_id,
            ClientMessage::RecordEnded(RecordEnded {
                recording_id: req.recording_id.clone(),
                reason: reason.to_owned(),
                file_base: req.file_base,
                ..RecordEnded::default()
            }),
        );
    }

    /// Starts capturing the call `req` names, for the client that asked.
    pub fn start(&self, sfu: &Sfu, client_id: &str, req: RecordStart) {
        let Some(live) = &self.live else {
            return self.refuse(
                client_id,
                &req,
                "refused",
                "recording is not enabled on this node",
            );
        };
        if !valid_id(&req.recording_id) || req.call_id.is_empty() {
            return self.refuse(
                client_id,
                &req,
                "refused",
                "the request names no valid recording or call",
            );
        }
        if !valid_site(&req.site_url) || req.token.is_empty() {
            return self.refuse(
                client_id,
                &req,
                "refused",
                "the request has no valid place to upload to",
            );
        }
        let run_no = live.runs.fetch_add(1, Ordering::Relaxed) + 1;
        match live
            .registry
            .begin(&req.recording_id, client_id, &req.call_id, run_no)
        {
            Ok(()) => {}
            Err(Busy::Same) => {
                debug!(recording = %req.recording_id, "asked again to record what is being recorded");
                return;
            }
            Err(Busy::Other) => {
                return self.refuse(client_id, &req, "refused", "the call has another recording");
            }
        }
        let dir = live
            .data_dir
            .join(&req.recording_id)
            .join(run_no.to_string());
        if let Err(e) = std::fs::create_dir_all(&dir) {
            error!(dir = %dir.display(), error = %e, "cannot create the recording's directory");
            live.registry.remove(&req.recording_id, run_no);
            return self.refuse(
                client_id,
                &req,
                "failed",
                "the recording's directory cannot be made",
            );
        }
        let t0 = instant_of(req.t0, Instant::now(), SystemTime::now());
        let api = Api::with_clients(&live.clients, &req.site_url, &req.recording_id, &req.token)
            .throttled(live.throttle.clone());
        let run = Arc::new(Run {
            id: run_no,
            recording_id: req.recording_id.clone(),
            client_id: client_id.to_owned(),
            call_id: req.call_id.clone(),
            file_base: req.file_base,
            dir,
            api,
            t0,
        });
        let (sink, rx) = record_channel_for(DEFAULT_QUEUE_BYTES);
        let job = Job::new(
            run,
            rx,
            live.max_bytes,
            Links {
                uploads: live.uploads.clone(),
                pending: live.pending.clone(),
                registry: live.registry.clone(),
                metrics: self.metrics.clone(),
            },
            Settings::default(),
        );
        self.metrics.record_capture(1);
        live.writers.add(job);
        info!(recording = %req.recording_id, call = %req.call_id, file_base = req.file_base, "recording a call");
        // If the SFU cannot take the recording it drops the sink, which the writer takes for a
        // recording that failed and reports.
        if let Err(e) = sfu.record_start(client_id, &req.call_id, &req.recording_id, t0, sink) {
            error!(recording = %req.recording_id, error = %e, "the SFU would not start the recording");
        }
    }

    /// Stops a recording: the SFU ends it, the writer closes what is open and the uploader
    /// finishes.
    pub fn stop(&self, sfu: &Sfu, client_id: &str, recording_id: &str) {
        let Some(call) = self
            .live
            .as_ref()
            .and_then(|l| l.registry.call_of(client_id, recording_id))
        else {
            debug!(
                recording = recording_id,
                "asked to stop a recording this node is not capturing"
            );
            return;
        };
        if let Err(e) = sfu.record_stop(client_id, &call, recording_id) {
            warn!(recording = recording_id, error = %e, "the SFU would not stop the recording");
        }
    }

    /// Captures running on this node.
    pub fn running(&self) -> usize {
        self.live.as_ref().map_or(0, |l| l.registry.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_start_is_found_on_the_monotonic_clock() {
        let now = Instant::now();
        let wall = UNIX_EPOCH + Duration::from_millis(1_000_000);
        let t0 = instant_of(990_000, now, wall);
        assert_eq!(now.duration_since(t0), Duration::from_secs(10));
        // A start in the future (clocks differ a little) is now; so is one before the clock began.
        assert_eq!(instant_of(1_002_000, now, wall), now);
        assert_eq!(instant_of(-5, now, wall).max(now), now);
    }

    #[test]
    fn a_call_has_one_recording_and_a_recording_one_capture() {
        let r = Registry::default();
        assert_eq!(r.begin("rec1", "mm", "call", 1), Ok(()));
        assert_eq!(r.begin("rec1", "mm", "call", 2), Err(Busy::Same));
        assert_eq!(r.begin("rec2", "mm", "call", 3), Err(Busy::Other));
        assert_eq!(
            r.begin("rec2", "other", "call", 4),
            Ok(()),
            "another client's call is another call"
        );
        assert_eq!(r.call_of("mm", "rec1").as_deref(), Some("call"));
        assert_eq!(
            r.call_of("other", "rec1"),
            None,
            "a client stops only its own"
        );
    }

    #[test]
    fn ids_are_safe_to_use_in_paths() {
        for ok in ["rrrrrrrrrrrrrrrrrrrrrrrrrr", "rec-1_A"] {
            assert!(valid_id(ok), "{ok}");
        }
        for bad in ["", "../x", "a/b", "a b", &"x".repeat(65)] {
            assert!(!valid_id(bad), "{bad}");
        }
        assert!(valid_site("https://lms.example"));
        assert!(valid_site("http://127.0.0.1:8065/prefix"));
        for bad in ["", "ftp://x", "https://", "lms.example"] {
            assert!(!valid_site(bad), "{bad}");
        }
    }
}
