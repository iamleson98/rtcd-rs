//! Command recorder records one live session of the LMS. It runs as a short-lived job (a
//! Kubernetes Job or a Docker container, launched by the server's recorderjob package) and joins
//! the class call as the recorder bot, listen-only. Every voice, camera and shared screen the SFU
//! (rtcd) forwards is written as it arrives, without re-encoding (the whiteboard is logged by the
//! app server, not here). Each file is uploaded on its own (straight to storage when
//! the server allows it), with a manifest that places each on the timeline; the web app's player
//! puts the session back together.
//!
//! `recorder version` prints the build and exits, without reading the environment: the cluster
//! runs it to pull the image onto every node before the first recording (the image is scratch, so
//! it has no shell).
//!
//! Environment: SITE_URL, AUTH_TOKEN (the bot session for this recording), RECORDING_ID,
//! MAX_DURATION_SEC, MAX_DATA_BYTES (the disk it may use; it stops capturing and uploads before
//! that is full). SIGTERM (the server removed the job) ends it without uploading.

use std::path::Path;
use std::process::ExitCode;
use std::time::Duration;

use recorder::api::{Api, install_tls_provider};
use recorder::config::Config;
use recorder::logging;
use recorder::session::{RunError, run};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Holds the recording until it is uploaded.
const DATA_DIR: &str = "/data";

fn version() -> String {
    let revision = env!("RECORDER_REVISION");
    if revision.is_empty() {
        format!("recorder rust{}", env!("RECORDER_RUSTC"))
    } else {
        format!("recorder rust{} {revision}", env!("RECORDER_RUSTC"))
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() == Some("version") && args.next().is_none() {
        println!("{}", version());
        return ExitCode::SUCCESS;
    }
    // A panic anywhere ends the process: a recorder whose peer connection task died would go on
    // looking alive and record nothing.
    let hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        error!(panic = %info, "the recorder panicked");
        hook(info);
        std::process::exit(101);
    }));
    let level = std::env::var("LOG_LEVEL").unwrap_or_default();
    let level_ok = logging::init(&level, &std::env::var("RECORDING_ID").unwrap_or_default());
    if !level_ok {
        warn!(log_level = level, "unknown LOG_LEVEL; logging at info");
    }
    install_tls_provider();
    let cfg = match Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e, "invalid configuration");
            return ExitCode::from(2);
        }
    };

    let cancel = CancellationToken::new();
    tokio::spawn(terminate(cancel.clone()));

    let api = Api::new(&cfg);
    let dir = std::env::var("RECORDER_DATA_DIR").unwrap_or_else(|_| DATA_DIR.into());
    match run(cancel, cfg, api.clone(), Path::new(&dir)).await {
        Ok(()) => {
            info!("recording complete");
            ExitCode::SUCCESS
        }
        Err(RunError::Stopped) => {
            info!("recording removed by the server; exiting");
            ExitCode::SUCCESS
        }
        Err(RunError::Failed(reason)) => {
            error!(error = reason, "recording failed");
            let _ = tokio::time::timeout(Duration::from_secs(30), api.fail(&reason)).await;
            ExitCode::FAILURE
        }
    }
}

/// SIGTERM (and SIGINT): the server removed the job.
async fn terminate(cancel: CancellationToken) {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        return;
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
    cancel.cancel();
}
