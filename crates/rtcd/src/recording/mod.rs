//! Recording calls in rtcd itself, without a recorder participant.
//!
//! The SFU copies each recorded stream's packets to a queue (`rtcd_sfu::record`); this module takes
//! it from there:
//!
//! * [`writer`]: threads that drain the queues and write WebM files with the `recorder` crate,
//!   rotating them every two minutes and numbering them in the order they close;
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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
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
    /// The most files the run may write (zero: no limit): it ends, as `file_limit`, once it has
    /// numbered `file_base + max_files` of them.
    pub max_files: u32,
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
    /// The node is out of time to shut down: report the captures that are over, with the files
    /// that are in.
    Deadline,
}

/// What `record_ended` says when a node shuts down under a recording.
///
/// The server's vocabulary has no reason for it. `call_ended` is the one that keeps the recording
/// going: the server notes that the room closed and, if the call opens again within its grace
/// (five minutes, `rtcdCallGrace`), which a rolling deploy does when the class reconnects to
/// another node, sends the start again with the next file numbers and the same `t0`; otherwise it
/// stops the recording. `failed` would end the capture for good.
pub const SHUTDOWN_REASON: &str = "call_ended";

/// Why a start was not registered.
#[derive(Debug, PartialEq, Eq)]
pub enum Busy {
    /// This very start (the same recording, client, call and first file number) is held: running,
    /// or over with its `record_ended` not sent yet. That one end answers it, so nothing more is
    /// said.
    Same,
    /// The start belongs to a run that is over and whose `record_ended` was sent a moment ago
    /// (the numbers are held for a while after that). Whoever sends it again has not got the end,
    /// or computed the start before it did: the end is sent again, which the server takes as it
    /// takes any end it sees twice. `refuse` is set when the start itself is not that run's but
    /// numbered into its files, and so also needs its own answer.
    Resend {
        end: Box<RecordEnded>,
        refuse: Option<&'static str>,
    },
    /// The start is not taken and is answered `refused`; the text is for the log.
    Refused(&'static str),
}

/// What a start asks for, as the registry compares it with what it holds.
#[derive(Debug, Clone, Copy)]
pub struct Want<'a> {
    pub recording_id: &'a str,
    pub client_id: &'a str,
    pub call_id: &'a str,
    pub file_base: u32,
    pub max_files: u32,
}

#[derive(Debug)]
struct Active {
    run: u64,
    client_id: String,
    call_id: String,
    file_base: u32,
}

/// How long the numbers of a run stay taken after its `record_ended` was sent. A resend that the
/// server computed before the end reached it carries the same numbers; taken as a new run it would
/// write over the files just uploaded. The server numbers every later start past an end it knows,
/// so no start it means to make is held up.
const HOLD_AFTER_REPORT: Duration = Duration::from_secs(300);

/// A capture that is over: its files are still being uploaded, or its `record_ended` was sent a
/// moment ago, and the numbers they took are not to be taken again.
#[derive(Debug)]
struct Over {
    run: u64,
    /// The `record_ended` that was sent, kept to be sent again; `None` until it is, and for a
    /// capture that reports nothing.
    end: Option<RecordEnded>,
    client_id: String,
    call_id: String,
    file_base: u32,
    /// The indexes the files took, counted from `file_base`.
    files: u32,
    /// When its `record_ended` was sent; `None` until then.
    reported_at: Option<Instant>,
}

impl Over {
    /// Whether file number `n` is one this run took, or would have been the next to take.
    fn holds(&self, n: u32) -> bool {
        n >= self.file_base
            && u64::from(n) < u64::from(self.file_base) + u64::from(self.files.max(1))
    }
}

#[derive(Debug, Default)]
struct Held {
    active: HashMap<String, Active>,
    over: HashMap<String, Vec<Over>>,
}

/// The captures running on this node, and those over and not reported yet.
///
/// Every start is answered once. One that is taken is answered by the `record_ended` of its run,
/// and is not taken again, whoever sends it again, until that end has been sent. One that is not
/// taken is answered `refused` at once. Both are decided here, under one lock, so that two senders
/// and a node that starts draining at the same time cannot both be right.
#[derive(Debug)]
pub struct Registry {
    held: Mutex<Held>,
    hold: Duration,
}

impl Default for Registry {
    fn default() -> Self {
        Self::with_hold(HOLD_AFTER_REPORT)
    }
}

impl Registry {
    /// A registry that keeps the numbers of a run taken for `hold` after its end was sent.
    pub fn with_hold(hold: Duration) -> Self {
        Self {
            held: Mutex::new(Held::default()),
            hold,
        }
    }

    /// Registers the start as run `run`, unless something held says otherwise. What is already
    /// held comes first, so a resend of a start the node holds is never answered `refused`
    /// because the node is draining.
    pub fn begin(&self, want: Want<'_>, run: u64, draining: &AtomicBool) -> Result<(), Busy> {
        let mut held = self.held.lock();
        self.forget_old(&mut held);
        if let Some(a) = held.active.get(want.recording_id) {
            return if a.client_id == want.client_id
                && a.call_id == want.call_id
                && a.file_base == want.file_base
            {
                Err(Busy::Same)
            } else {
                Err(Busy::Refused(
                    "the recording is captured for another call or from another file number",
                ))
            };
        }
        for o in held.over.get(want.recording_id).into_iter().flatten() {
            if o.holds(want.file_base) {
                let same_run = o.client_id == want.client_id && o.call_id == want.call_id;
                let same_start = same_run && o.file_base == want.file_base;
                return match (&o.end, same_run) {
                    (Some(end), true) => Err(Busy::Resend {
                        end: Box::new(end.clone()),
                        refuse: (!same_start).then_some(
                            "the file numbers are those of a capture whose end was just sent",
                        ),
                    }),
                    _ if same_start => Err(Busy::Same),
                    _ => Err(Busy::Refused(
                        "the file numbers are those of a capture that is not reported yet",
                    )),
                };
            }
            // A run that would number its way into a capture that is not reported yet.
            if want.max_files > 0
                && o.file_base > want.file_base
                && o.file_base - want.file_base < want.max_files
            {
                return Err(Busy::Refused(
                    "the file numbers reach those of a capture that is not reported yet",
                ));
            }
        }
        // Sequentially consistent with the store in `begin_drain`, which lists the captures
        // after setting it: a start registered before that listing is stopped by the drain, and
        // one that is not is refused here.
        if draining.load(Ordering::SeqCst) {
            return Err(Busy::Refused("this node is shutting down"));
        }
        if held
            .active
            .values()
            .any(|a| a.client_id == want.client_id && a.call_id == want.call_id)
        {
            return Err(Busy::Refused("the call has another recording"));
        }
        held.active.insert(
            want.recording_id.to_owned(),
            Active {
                run,
                client_id: want.client_id.to_owned(),
                call_id: want.call_id.to_owned(),
                file_base: want.file_base,
            },
        );
        Ok(())
    }

    /// The run never began (only if `run` is the one registered).
    pub fn remove(&self, recording_id: &str, run: u64) {
        let mut held = self.held.lock();
        if held.active.get(recording_id).is_some_and(|a| a.run == run) {
            held.active.remove(recording_id);
        }
    }

    /// The capture is over, with `files` indexes taken (only if `run` is the one registered): it
    /// no longer takes a stop, and its numbers stay taken until [`reported`](Self::reported).
    pub fn finish(&self, recording_id: &str, run: u64, files: u32) {
        let mut held = self.held.lock();
        if !held.active.get(recording_id).is_some_and(|a| a.run == run) {
            return;
        }
        if let Some(a) = held.active.remove(recording_id) {
            held.over
                .entry(recording_id.to_owned())
                .or_default()
                .push(Over {
                    run,
                    client_id: a.client_id,
                    call_id: a.call_id,
                    file_base: a.file_base,
                    files,
                    end: None,
                    reported_at: None,
                });
        }
    }

    /// The end of the run has been sent, as `end` (`None`: there was nothing to say): its numbers
    /// are free once the time they are held for after that has passed, and a start that comes for
    /// them meanwhile gets `end` again.
    pub fn reported(&self, recording_id: &str, run: u64, end: Option<RecordEnded>) {
        let mut held = self.held.lock();
        if let Some(o) = held
            .over
            .get_mut(recording_id)
            .and_then(|over| over.iter_mut().find(|o| o.run == run))
        {
            o.end = end;
            o.reported_at = Some(Instant::now());
        }
        self.forget_old(&mut held);
    }

    /// Lets go of the runs whose end was sent longer ago than they are held for.
    fn forget_old(&self, held: &mut Held) {
        let now = Instant::now();
        held.over.retain(|_, over| {
            over.retain(|o| {
                o.reported_at
                    .is_none_or(|at| now.saturating_duration_since(at) < self.hold)
            });
            !over.is_empty()
        });
    }

    /// The call a client's recording captures.
    pub fn call_of(&self, client_id: &str, recording_id: &str) -> Option<String> {
        self.held
            .lock()
            .active
            .get(recording_id)
            .filter(|a| a.client_id == client_id)
            .map(|a| a.call_id.clone())
    }

    /// Every capture running as (client, recording, call).
    pub fn all(&self) -> Vec<(String, String, String)> {
        self.held
            .lock()
            .active
            .iter()
            .map(|(id, a)| (a.client_id.clone(), id.clone(), a.call_id.clone()))
            .collect()
    }

    /// Captures running.
    pub fn len(&self) -> usize {
        self.held.lock().active.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Captures that are over and whose end has not been sent.
    #[cfg(test)]
    pub(crate) fn unreported(&self) -> usize {
        self.held
            .lock()
            .over
            .values()
            .flatten()
            .filter(|o| o.reported_at.is_none())
            .count()
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
    /// The node is shutting down: nothing new is taken, and what ends reports `SHUTDOWN_REASON`.
    draining: Arc<AtomicBool>,
    /// Captures started whose end the server has not been told yet.
    unfinished: Arc<AtomicUsize>,
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

/// Hands a registered capture to the SFU. If the SFU cannot take the recording it drops the sink,
/// which the writer takes for a recording that failed and reports.
///
/// The drain stops the captures it finds in the registry. One registered after it looked is
/// refused, but one registered before it looked can be handed to the SFU after the drain asked
/// the SFU to stop it, which the SFU, not knowing it yet, ignores: so the flag is read again after
/// the hand-over (both sides sequentially consistent: either this sees the flag, or the drain's
/// stop is later than the hand-over) and the capture is stopped here.
fn hand_over<E: std::fmt::Display>(
    draining: &AtomicBool,
    recording_id: &str,
    start: impl FnOnce() -> Result<(), E>,
    stop: impl FnOnce() -> Result<(), E>,
) {
    if let Err(e) = start() {
        error!(recording = %recording_id, error = %e, "the SFU would not start the recording");
    }
    if draining.load(Ordering::SeqCst)
        && let Err(e) = stop()
    {
        warn!(recording = %recording_id, error = %e, "the SFU would not stop a recording for the shutdown");
    }
}

/// The share of their rate, in thousandths, at which uploads go while the node's media is refused.
const YIELD_SHARE: u32 = 250;

impl Recordings {
    /// Sets recording up as the configuration says. Must be called inside the runtime (the
    /// uploader is a task of it).
    ///
    /// `yields`: uploads may be made to stand aside later (`yield_to_media`), which the upload
    /// time allowance has to allow for.
    pub fn new(
        cfg: &RecordConfig,
        yields: bool,
        reports: Arc<Reports>,
        metrics: Arc<Metrics>,
    ) -> Self {
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
        // Once, before this process has recorded anything, so what is found is not its own. The
        // directory must be this process's alone (an emptyDir of its pod): a second process using
        // it would have its files removed by the first to start.
        clear_leftovers(&data_dir);
        let pending = Arc::new(Pending::new(
            cfg.max_pending_bytes.max(1) as u64,
            metrics.clone(),
        ));
        let unfinished = Arc::new(AtomicUsize::new(0));
        let registry = Arc::new(Registry::default());
        let uploads = uploader::spawn(uploader::Deps {
            reports: reports.clone(),
            pending: pending.clone(),
            metrics: metrics.clone(),
            policy: uploader::Policy::default(),
            unfinished: unfinished.clone(),
            registry: registry.clone(),
        });
        // The writers only append to files; a few threads serve every recording.
        let cores = std::thread::available_parallelism().map_or(1, usize::from);
        info!(dir = %data_dir.display(), "recording is enabled on this node");
        Self {
            live: Some(Live {
                writers: WriterPool::start((cores / 2).clamp(1, 4), &metrics),
                uploads,
                pending,
                registry,
                clients: HttpClients::new(),
                throttle: {
                    let throttle =
                        Throttle::new((cfg.upload_mbps.max(1) as u64).saturating_mul(125_000));
                    if yields {
                        throttle.may_yield_to(YIELD_SHARE);
                    }
                    Arc::new(throttle)
                },
                runs: AtomicU64::new(0),
                data_dir,
                max_bytes: cfg.max_bytes.max(1) as u64,
                draining: Arc::new(AtomicBool::new(false)),
                unfinished,
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
    ///
    /// Every start is answered once: `refused` at once when it is not taken, otherwise by the
    /// `record_ended` of its run, whatever order starts, stops, drains and ends come in. A start
    /// the node already holds (the same recording, call and first file number, running or over and
    /// not reported yet) is not answered again: the end of that run answers it. Once that end has
    /// been sent the numbers stay held for a while, and a resend (the server may not have got the
    /// end) is answered with the end again; one for a different number inside the run's files
    /// gets the end and also `refused` for itself. See [`Registry::begin`].
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
        let want = Want {
            recording_id: &req.recording_id,
            client_id,
            call_id: &req.call_id,
            file_base: req.file_base,
            max_files: req.max_files,
        };
        match live.registry.begin(want, run_no, &live.draining) {
            Ok(()) => {}
            Err(Busy::Same) => {
                debug!(recording = %req.recording_id, file_base = req.file_base, "asked again to record what this node holds");
                return;
            }
            Err(Busy::Resend { end, refuse }) => {
                info!(recording = %req.recording_id, file_base = req.file_base, "asked again to record what this node has just reported ended; reporting it again");
                self.reports
                    .send(client_id, ClientMessage::RecordEnded(*end));
                if let Some(why) = refuse {
                    self.refuse(client_id, &req, "refused", why);
                }
                return;
            }
            Err(Busy::Refused(why)) => return self.refuse(client_id, &req, "refused", why),
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
            max_files: req.max_files,
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
                draining: live.draining.clone(),
            },
            Settings::default(),
        );
        self.metrics.record_capture(1);
        live.unfinished.fetch_add(1, Ordering::AcqRel);
        live.writers.add(job);
        info!(recording = %req.recording_id, call = %req.call_id, file_base = req.file_base, max_files = req.max_files, "recording a call");
        hand_over(
            &live.draining,
            &req.recording_id,
            || sfu.record_start(client_id, &req.call_id, &req.recording_id, t0, sink),
            || sfu.record_stop(client_id, &req.call_id, &req.recording_id),
        );
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

    /// Uploads go at a quarter of their rate while `yielding`, the whole of it otherwise: the
    /// network card is refusing the media this node sends, and a file can wait. Not once the
    /// files waiting are half of what the node holds: they would reach the cap, and recordings
    /// would start leaving packets out.
    pub fn yield_to_media(&self, yielding: bool) {
        if let Some(live) = &self.live {
            let slow = yielding && !live.pending.half_full();
            live.throttle
                .set_share(if slow { YIELD_SHARE } else { 1000 });
        }
    }

    /// Records files of `n` bytes as closed and waiting for upload, as the writers do.
    #[cfg(test)]
    pub(crate) fn hold(&self, n: u64) {
        if let Some(live) = &self.live {
            live.pending.add(n);
        }
    }

    /// How much of its rate the uploads are let through now, in thousandths (`None` where
    /// recording is off).
    pub fn upload_share(&self) -> Option<u32> {
        self.live.as_ref().map(|l| l.throttle.share())
    }

    /// The node is shutting down: takes no new recording, lifts the upload rate (the media this
    /// node sends is about to end anyway, and the files must get out first), and stops every
    /// capture at once so that its open files close and are uploaded. Call it before the SFU
    /// starts to drain, then [`wait_drained`](Self::wait_drained).
    pub fn begin_drain(&self, sfu: &Sfu) {
        let Some(live) = &self.live else {
            return;
        };
        live.draining.store(true, Ordering::SeqCst);
        live.throttle.lift();
        let all = live.registry.all();
        info!(
            captures = all.len(),
            "shutting down: stopping recordings and uploading their files"
        );
        for (client, recording, call) in all {
            if let Err(e) = sfu.record_stop(&client, &call, &recording) {
                warn!(recording = %recording, error = %e, "the SFU would not stop a recording for the shutdown");
            }
        }
    }

    /// Waits until every capture has ended and been reported, or `within` has passed. At the
    /// deadline the captures that are over are reported with the files that are in, and the files
    /// that are not are lost: they are counted and logged once. Returns how many were lost.
    pub async fn wait_drained(&self, within: Duration) -> u64 {
        let Some(live) = &self.live else {
            return 0;
        };
        let end = tokio::time::Instant::now() + within;
        while live.unfinished.load(Ordering::Acquire) > 0 {
            if tokio::time::Instant::now() >= end {
                let lost = live.pending.files();
                warn!(
                    lost_files = lost,
                    lost_bytes = live.pending.bytes(),
                    unfinished = live.unfinished.load(Ordering::Acquire),
                    "out of time to shut down: recording files that were not uploaded are lost"
                );
                let _ = live.uploads.send(Cmd::Deadline);
                // Let the uploader's reports reach the connections, which close after this.
                tokio::time::sleep(Duration::from_millis(200)).await;
                return lost;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        0
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

    #[tokio::test]
    async fn uploads_stand_aside_while_the_node_yields_to_media() {
        let dir = std::env::temp_dir().join(format!("rtcd-yield-{}", std::process::id()));
        let metrics = Arc::new(Metrics::new());
        let cfg = RecordConfig {
            enabled: true,
            data_dir: dir.to_string_lossy().into_owned(),
            ..RecordConfig::default()
        };
        let r = Recordings::new(&cfg, true, Arc::new(Reports::new(metrics.clone())), metrics);
        assert_eq!(r.upload_share(), Some(1000));
        r.yield_to_media(true);
        assert_eq!(r.upload_share(), Some(250));
        r.yield_to_media(false);
        assert_eq!(r.upload_share(), Some(1000));
        // Files piling up past half of what the node holds end the courtesy: they would reach
        // the cap and the recordings would start leaving packets out.
        r.hold(1 << 30);
        r.yield_to_media(true);
        assert_eq!(r.upload_share(), Some(250), "a quarter of the cap waits");
        r.hold(1 << 30);
        r.yield_to_media(true);
        assert_eq!(r.upload_share(), Some(1000), "half of it is full speed");
        let _ = std::fs::remove_dir_all(dir);
        assert_eq!(
            Recordings::new(
                &RecordConfig::default(),
                false,
                Arc::new(Reports::new(Arc::new(Metrics::new()))),
                Arc::new(Metrics::new())
            )
            .upload_share(),
            None,
            "recording is off by default"
        );
    }

    fn want<'a>(recording: &'a str, client: &'a str, call: &'a str, base: u32) -> Want<'a> {
        Want {
            recording_id: recording,
            client_id: client,
            call_id: call,
            file_base: base,
            max_files: 0,
        }
    }

    static SERVING: AtomicBool = AtomicBool::new(false);

    #[test]
    fn a_call_has_one_recording_and_a_recording_one_capture() {
        let r = Registry::default();
        assert_eq!(r.begin(want("rec1", "mm", "call", 0), 1, &SERVING), Ok(()));
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 0), 2, &SERVING),
            Err(Busy::Same),
            "the same start again is the one held"
        );
        assert!(matches!(
            r.begin(want("rec2", "mm", "call", 0), 3, &SERVING),
            Err(Busy::Refused(_))
        ));
        assert_eq!(
            r.begin(want("rec2", "other", "call", 0), 4, &SERVING),
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
    fn a_start_for_another_call_or_number_than_the_one_held_is_refused() {
        let r = Registry::default();
        assert_eq!(r.begin(want("rec1", "mm", "call", 40), 1, &SERVING), Ok(()));
        for other in [
            want("rec1", "mm", "call2", 40),
            want("rec1", "mm", "call", 41),
            want("rec1", "mm", "call", 0),
            want("rec1", "other", "call", 40),
        ] {
            assert!(
                matches!(r.begin(other, 9, &SERVING), Err(Busy::Refused(_))),
                "{other:?}"
            );
        }
        assert_eq!(r.len(), 1, "the one held is undisturbed");
        assert_eq!(r.call_of("mm", "rec1").as_deref(), Some("call"));
    }

    #[test]
    fn what_a_draining_node_holds_is_answered_by_its_end_and_nothing_else_is_taken() {
        let r = Registry::default();
        let draining = AtomicBool::new(false);
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 40), 1, &draining),
            Ok(())
        );
        draining.store(true, Ordering::SeqCst);
        // The resend of the start it holds is not a refusal (the server would write one as the
        // start's end); another start is, and so is a new recording.
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 40), 2, &draining),
            Err(Busy::Same)
        );
        assert!(matches!(
            r.begin(want("rec1", "mm", "call2", 20_040), 3, &draining),
            Err(Busy::Refused(_))
        ));
        assert!(matches!(
            r.begin(want("rec9", "mm", "call9", 0), 4, &draining),
            Err(Busy::Refused("this node is shutting down"))
        ));
        // Once its run is over and not reported, the resend is still the one start.
        r.finish("rec1", 1, 3);
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 40), 5, &draining),
            Err(Busy::Same)
        );
        r.reported("rec1", 1, None);
        assert_eq!(r.unreported(), 0);
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 40), 6, &draining),
            Err(Busy::Same),
            "a resend computed before the end arrived is still the one start"
        );
        let r = Registry::with_hold(Duration::ZERO);
        assert_eq!(r.begin(want("rec1", "mm", "call", 40), 1, &SERVING), Ok(()));
        r.finish("rec1", 1, 3);
        r.reported("rec1", 1, None);
        assert!(matches!(
            r.begin(want("rec1", "mm", "call", 40), 7, &draining),
            Err(Busy::Refused("this node is shutting down"))
        ));
    }

    #[test]
    fn a_run_that_ended_keeps_its_numbers_until_its_end_is_sent() {
        let r = Registry::default();
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 1, &SERVING),
            Ok(())
        );
        r.finish("rec1", 1, 4);
        assert!(r.is_empty(), "a stop has nothing to stop");
        assert_eq!(r.call_of("mm", "rec1"), None);
        assert_eq!(r.unreported(), 1);

        // The same start again is answered by the end that is coming.
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 2, &SERVING),
            Err(Busy::Same)
        );
        // Numbers it took (or would take next) are not given to another start.
        for base in [101, 103, 100] {
            let other = Want {
                call_id: "call2",
                ..want("rec1", "mm", "call", base)
            };
            assert!(
                matches!(r.begin(other, 3, &SERVING), Err(Busy::Refused(_))),
                "{base}"
            );
        }
        assert!(matches!(
            r.begin(want("rec1", "mm", "call", 101), 3, &SERVING),
            Err(Busy::Refused(_))
        ));
        // A run that would number its way into them is not taken either.
        let reaching = Want {
            max_files: 10,
            ..want("rec1", "mm", "call", 95)
        };
        assert!(matches!(
            r.begin(reaching, 4, &SERVING),
            Err(Busy::Refused(_))
        ));
        let before = Want {
            max_files: 5,
            ..want("rec1", "mm", "call", 95)
        };
        assert_eq!(
            r.begin(before, 5, &SERVING),
            Ok(()),
            "a run that ends before them is fine"
        );
        r.remove("rec1", 5);
        assert_eq!(
            r.begin(want("rec1", "mm", "call2", 104), 6, &SERVING),
            Ok(()),
            "the numbers after the last one it took are free"
        );
        r.remove("rec1", 6);
    }

    fn ended(reason: &str, base: u32, files: u32) -> RecordEnded {
        RecordEnded {
            recording_id: "rec1".into(),
            reason: reason.into(),
            file_base: base,
            files,
            ..RecordEnded::default()
        }
    }

    #[test]
    fn a_resend_of_a_start_whose_end_was_lost_gets_the_end_again() {
        // Nothing reported (the resend has the same number), and some files reported (the server
        // numbered the resend past them): the end went out on a connection that dropped, or the
        // server failed to keep it. Both are answered by the end, and the second also refused.
        for (files, base, refuse) in [(0u32, 100u32, false), (3, 101, true)] {
            let r = Registry::default();
            assert_eq!(
                r.begin(want("rec1", "mm", "call", 100), 1, &SERVING),
                Ok(())
            );
            r.finish("rec1", 1, files);
            let before = r.begin(want("rec1", "mm", "call", base), 2, &SERVING);
            if files == 0 {
                assert_eq!(before, Err(Busy::Same), "the run's own end answers it");
            } else {
                assert!(matches!(before, Err(Busy::Refused(_))), "{before:?}");
            }
            let end = ended("stopped", 100, files);
            r.reported("rec1", 1, Some(end.clone()));
            let got = r.begin(want("rec1", "mm", "call", base), 3, &SERVING);
            match got {
                Err(Busy::Resend {
                    end: sent,
                    refuse: r,
                }) => {
                    assert_eq!(*sent, end);
                    assert_eq!(r.is_some(), refuse);
                }
                other => panic!("files {files}: {other:?}"),
            }
        }
        // Another call's start, or a run that reported nothing, is not answered with an end.
        let r = Registry::default();
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 1, &SERVING),
            Ok(())
        );
        r.finish("rec1", 1, 3);
        r.reported("rec1", 1, Some(ended("stopped", 100, 3)));
        let other_call = Want {
            call_id: "call2",
            ..want("rec1", "mm", "call", 101)
        };
        assert!(matches!(
            r.begin(other_call, 2, &SERVING),
            Err(Busy::Refused(_))
        ));
        let r = Registry::default();
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 1, &SERVING),
            Ok(())
        );
        r.finish("rec1", 1, 3);
        r.reported("rec1", 1, None);
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 2, &SERVING),
            Err(Busy::Same)
        );
    }

    #[test]
    fn the_numbers_of_a_run_are_held_for_a_while_after_its_end_was_sent() {
        let r = Registry::default();
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 1, &SERVING),
            Ok(())
        );
        r.finish("rec1", 1, 4);
        r.reported("rec1", 1, None);
        // A resend the server computed before the end reached it: not a new run that would
        // write over the files just uploaded.
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 2, &SERVING),
            Err(Busy::Same)
        );
        let other = Want {
            call_id: "call2",
            ..want("rec1", "mm", "call", 102)
        };
        assert!(matches!(r.begin(other, 3, &SERVING), Err(Busy::Refused(_))));
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 104), 4, &SERVING),
            Ok(()),
            "what the server numbers past the end is free at once"
        );

        let r = Registry::with_hold(Duration::from_millis(30));
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 1, &SERVING),
            Ok(())
        );
        r.finish("rec1", 1, 4);
        r.reported("rec1", 1, None);
        std::thread::sleep(Duration::from_millis(60));
        assert_eq!(
            r.begin(want("rec1", "mm", "call", 100), 2, &SERVING),
            Ok(()),
            "forgotten after the hold"
        );
    }

    #[test]
    fn a_drain_that_begins_between_the_registration_and_the_hand_over_stops_the_capture() {
        use std::cell::{Cell, RefCell};
        let draining = AtomicBool::new(false);
        let registry = Registry::default();
        assert_eq!(
            registry.begin(want("rec1", "mm", "call", 0), 1, &draining),
            Ok(())
        );
        let calls = RefCell::new(Vec::new());
        let listed = Cell::new(0);
        hand_over(
            &draining,
            "rec1",
            || -> Result<(), String> {
                // The drain begins now: it sets the flag, lists the capture (which the SFU does
                // not know yet) and asks the SFU to stop it, which does nothing.
                draining.store(true, Ordering::SeqCst);
                listed.set(registry.all().len());
                calls
                    .borrow_mut()
                    .push("drain stops it (ignored: not started yet)");
                calls.borrow_mut().push("started");
                Ok(())
            },
            || {
                calls.borrow_mut().push("stopped by the hand-over");
                Ok(())
            },
        );
        assert_eq!(listed.get(), 1);
        assert_eq!(
            calls.into_inner().last(),
            Some(&"stopped by the hand-over"),
            "the capture is stopped after the SFU has it"
        );

        // Without a drain, nothing is stopped.
        let stops = Cell::new(0);
        hand_over(
            &AtomicBool::new(false),
            "rec2",
            || -> Result<(), String> { Ok(()) },
            || {
                stops.set(stops.get() + 1);
                Ok(())
            },
        );
        assert_eq!(stops.get(), 0);
    }

    #[test]
    fn a_start_racing_a_drain_is_either_listed_by_it_or_refused() {
        // `begin_drain` sets the flag and then lists the captures; a start that is registered
        // before the listing is listed (and stopped), one after sees the flag. The registry's own
        // lock already orders these two, so this holds the contract (and would catch a check
        // moved out of the lock); the hand-over after the listing is tested below.
        for round in 0..300 {
            let registry = Arc::new(Registry::default());
            let draining = Arc::new(AtomicBool::new(false));
            let starters: Vec<_> = (0..4u64)
                .map(|n| {
                    let (registry, draining) = (registry.clone(), draining.clone());
                    std::thread::spawn(move || {
                        let id = format!("rec{n}");
                        let call = format!("call{n}");
                        let got = registry.begin(
                            Want {
                                recording_id: &id,
                                client_id: "mm",
                                call_id: &call,
                                file_base: 0,
                                max_files: 0,
                            },
                            n,
                            &draining,
                        );
                        (id, got)
                    })
                })
                .collect();
            draining.store(true, Ordering::SeqCst);
            let listed: Vec<String> = registry.all().into_iter().map(|(_, id, _)| id).collect();
            for starter in starters {
                let (id, got) = starter.join().unwrap();
                match got {
                    Ok(()) => assert!(
                        listed.contains(&id),
                        "round {round}: {id} was taken but the drain did not list it"
                    ),
                    Err(Busy::Refused(why)) => assert_eq!(why, "this node is shutting down"),
                    Err(other) => panic!("nothing was held: {other:?}"),
                }
            }
        }
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
