//! The rtcd service.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::Parser;
use rtcd::app::App;
use rtcd::config::Config;
use tracing::{error, info, warn};

/// WebRTC SFU for calls: a drop-in replacement for the Go rtcd.
#[derive(Parser)]
#[command(version)]
struct Cli {
    /// Path to the configuration file for the rtcd service.
    #[arg(long, default_value = "config/config.toml")]
    config: PathBuf,
}

fn main() -> ExitCode {
    // Go's flag package takes -config as well as --config; so do old service files.
    let args: Vec<String> = std::env::args()
        .map(|a| {
            if a == "-config" {
                "--config".to_owned()
            } else {
                a
            }
        })
        .collect();
    let cli = Cli::parse_from(args);

    let env: BTreeMap<String, String> = std::env::vars()
        .filter(|(k, _)| k.starts_with("RTCD_"))
        .collect();
    let cfg = match Config::load(&cli.config, &env) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("rtcd: failed to load config: {e}");
            return ExitCode::FAILURE;
        }
    };
    if let Err(e) = cfg.validate() {
        eprintln!("rtcd: failed to validate config: {e}");
        return ExitCode::FAILURE;
    }
    let _log = match rtcd::logging::init(&cfg.logger) {
        Ok(g) => g,
        Err(e) => {
            eprintln!("rtcd: failed to init logger: {e}");
            return ExitCode::FAILURE;
        }
    };
    if !cli.config.exists() {
        warn!(path = %cli.config.display(), "config file not found, using defaults");
    }

    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("rtcd-api")
        .worker_threads(2)
        .build()
    {
        Ok(r) => r,
        Err(e) => {
            eprintln!("rtcd: failed to start the runtime: {e}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(run(cfg))
}

async fn run(cfg: Config) -> ExitCode {
    let drain = match cfg.rtc.drain_timeout_seconds {
        0 => None,
        s => Some(Duration::from_secs(s as u64)),
    };
    let info = rtcd::info::VersionInfo::current();
    info!(version = %info.build_version, hash = %info.build_hash, "service: starting up");
    let app = match App::start(cfg).await {
        Ok(a) => a,
        Err(e) => {
            error!(%e, "failed to start the service");
            eprintln!("rtcd: failed to start service: {e}");
            return ExitCode::FAILURE;
        }
    };

    wait_for_signal().await;
    info!("service: shutting down; a second signal ends it at once");
    tokio::select! {
        () = app.shutdown(drain) => ExitCode::SUCCESS,
        () = wait_for_signal() => {
            warn!("service: forced shutdown");
            ExitCode::SUCCESS
        }
    }
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let (Ok(mut term), Ok(mut int)) = (
        signal(SignalKind::terminate()),
        signal(SignalKind::interrupt()),
    ) else {
        std::future::pending::<()>().await;
        return;
    };
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
