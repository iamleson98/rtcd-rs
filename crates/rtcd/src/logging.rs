//! Logging: console and/or file, plain or JSON, each at its own level, with the same settings as
//! the Go service.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::Mutex;
use tracing::Level;
use tracing_subscriber::Layer;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::LoggerConfig;

/// The Go service's level names mapped onto tracing's; `critical` is the most severe there.
pub fn parse_level(name: &str) -> Option<Level> {
    match name.to_ascii_lowercase().as_str() {
        "debug" => Some(Level::DEBUG),
        "info" => Some(Level::INFO),
        "warn" => Some(Level::WARN),
        "error" | "critical" => Some(Level::ERROR),
        _ => None,
    }
}

/// Whether a log target belongs to one of the libraries underneath, which say a lot at `info`
/// that an operator has no use for.
fn is_library(target: &str) -> bool {
    [
        "rtc::",
        "rtc_",
        "hyper",
        "tungstenite",
        "tokio_tungstenite",
        "h2",
        "rustls",
        "tokio_rustls",
    ]
    .iter()
    .any(|p| target.starts_with(p))
}

/// The library's packet pipeline, which warns for every packet the sender sent twice (a
/// retransmission that arrived after the original: "srtp ... duplicated"): hundreds a minute on an
/// ordinary call, and the packet is dropped as it should be. Its other records are errors.
fn is_packet_pipeline(target: &str) -> bool {
    target == "rtc::peer_connection::handler"
}

/// Whether to keep a record of `level` from `target`, given the configured level: the service's
/// own records at that level and above, the libraries' at `warn` and above (everything at `debug`),
/// the library's packet pipeline at `error` and above.
fn keeps(target: &str, level: Level, configured: Level) -> bool {
    let limit = if configured == Level::DEBUG {
        configured
    } else if is_packet_pipeline(target) {
        Level::ERROR
    } else if is_library(target) {
        Level::WARN
    } else {
        configured
    };
    level <= limit
}

fn filter_for(
    configured: Level,
) -> impl tracing_subscriber::layer::Filter<tracing_subscriber::Registry> + Clone {
    // The hint lets the `log` facade and the tracing macros drop what no layer wants without
    // calling the filter, which matters because the libraries log per packet at `debug`.
    tracing_subscriber::filter::filter_fn(move |meta| {
        keeps(meta.target(), *meta.level(), configured)
    })
    .with_max_level_hint(LevelFilter::from_level(configured))
}

/// Appends to a file, moving it aside when it grows past a size, and keeping a few old ones.
#[derive(Debug, Clone)]
pub struct RotatingFile {
    inner: Arc<Mutex<RotatingState>>,
}

#[derive(Debug)]
struct RotatingState {
    path: PathBuf,
    file: std::fs::File,
    size: u64,
    max_size: u64,
    keep: usize,
}

impl RotatingFile {
    pub fn open(path: &Path, max_size: u64, keep: usize) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            inner: Arc::new(Mutex::new(RotatingState {
                path: path.to_owned(),
                file,
                size,
                max_size,
                keep,
            })),
        })
    }
}

impl RotatingState {
    fn rotate(&mut self) -> io::Result<()> {
        // path.N -> path.N+1, dropping the oldest, then path -> path.1.
        for n in (1..self.keep).rev() {
            let from = suffixed(&self.path, n);
            if from.exists() {
                std::fs::rename(&from, suffixed(&self.path, n + 1))?;
            }
        }
        if self.keep > 0 {
            std::fs::rename(&self.path, suffixed(&self.path, 1))?;
        }
        self.file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.size = 0;
        Ok(())
    }
}

fn suffixed(path: &Path, n: usize) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(format!(".{n}"));
    PathBuf::from(s)
}

impl Write for RotatingFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut st = self.inner.lock();
        if st.size > 0 && st.size + buf.len() as u64 > st.max_size {
            // A failed rotation must not lose the line; keep appending to the current file.
            let _ = st.rotate();
        }
        let n = st.file.write(buf)?;
        st.size += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.lock().file.flush()
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RotatingFile {
    type Writer = RotatingFile;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

/// Keeps the log file's writer alive.
#[derive(Debug)]
pub struct LogGuard {
    _file: Option<RotatingFile>,
}

/// Installs the global logger. Call once.
pub fn init(cfg: &LoggerConfig) -> io::Result<LogGuard> {
    let mut layers: Vec<Box<dyn Layer<_> + Send + Sync>> = Vec::new();
    let mut file_handle = None;
    if cfg.enable_console {
        let level = parse_level(&cfg.console_level)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid console level"))?;
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(io::stdout)
            .with_ansi(cfg.enable_color);
        let layer: Box<dyn Layer<_> + Send + Sync> = if cfg.console_json {
            Box::new(layer.json().with_filter(filter_for(level)))
        } else {
            Box::new(layer.with_filter(filter_for(level)))
        };
        layers.push(layer);
    }
    if cfg.enable_file {
        let level = parse_level(&cfg.file_level)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid file level"))?;
        let file = RotatingFile::open(Path::new(&cfg.file_location), 100 * 1024 * 1024, 5)?;
        let layer = tracing_subscriber::fmt::layer()
            .with_writer(file.clone())
            .with_ansi(false);
        let layer: Box<dyn Layer<_> + Send + Sync> = if cfg.file_json {
            Box::new(layer.json().with_filter(filter_for(level)))
        } else {
            Box::new(layer.with_filter(filter_for(level)))
        };
        layers.push(layer);
        file_handle = Some(file);
    }
    tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .map_err(|e| io::Error::other(e.to_string()))?;
    Ok(LogGuard { _file: file_handle })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_go_level_names_map_to_tracing_levels() {
        assert_eq!(parse_level("DEBUG"), Some(Level::DEBUG));
        assert_eq!(parse_level("Info"), Some(Level::INFO));
        assert_eq!(parse_level("warn"), Some(Level::WARN));
        assert_eq!(parse_level("error"), Some(Level::ERROR));
        assert_eq!(parse_level("critical"), Some(Level::ERROR));
        assert_eq!(parse_level("trace"), None);
        assert_eq!(parse_level(""), None);
    }

    #[test]
    fn libraries_are_quieter_than_the_service() {
        // The service's own records follow the configured level.
        assert!(keeps("rtcd::service", Level::INFO, Level::INFO));
        assert!(!keeps("rtcd::service", Level::DEBUG, Level::INFO));
        assert!(
            keeps("rtcd_sfu::engine::join", Level::INFO, Level::INFO),
            "rtcd_sfu is not the rtc library"
        );
        // The libraries are held to warn, unless debugging.
        assert!(!keeps(
            "rtc::peer_connection::handler::dtls",
            Level::INFO,
            Level::INFO
        ));
        assert!(keeps(
            "rtc::peer_connection::handler::dtls",
            Level::WARN,
            Level::INFO
        ));
        assert!(!keeps("rtc_ice::agent", Level::INFO, Level::INFO));
        assert!(keeps(
            "rtc::peer_connection::handler::dtls",
            Level::DEBUG,
            Level::DEBUG
        ));
        // The packet pipeline's duplicates are not worth a warning, its errors are.
        assert!(!keeps(
            "rtc::peer_connection::handler",
            Level::WARN,
            Level::INFO
        ));
        assert!(keeps(
            "rtc::peer_connection::handler",
            Level::ERROR,
            Level::INFO
        ));
        assert!(keeps(
            "rtc::peer_connection::handler",
            Level::WARN,
            Level::DEBUG
        ));
        assert!(!keeps("hyper_util::server", Level::INFO, Level::INFO));
        assert!(keeps("anything", Level::ERROR, Level::ERROR));
        assert!(!keeps("rtcd", Level::WARN, Level::ERROR));
    }

    #[test]
    fn the_file_is_moved_aside_when_it_grows_and_old_ones_are_capped() {
        let dir = std::env::temp_dir().join(format!("rtcd-log-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rtcd.log");
        let mut f = RotatingFile::open(&path, 100, 2).unwrap();
        for i in 0..10u8 {
            f.write_all(format!("line {i} {}\n", "x".repeat(40)).as_bytes())
                .unwrap();
        }
        f.flush().unwrap();
        assert!(path.exists());
        assert!(suffixed(&path, 1).exists());
        assert!(suffixed(&path, 2).exists());
        assert!(!suffixed(&path, 3).exists(), "only two old files are kept");
        assert!(std::fs::metadata(&path).unwrap().len() <= 100);
        // The newest line is in the current file.
        assert!(std::fs::read_to_string(&path).unwrap().contains("line 9"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_existing_file_is_appended_to() {
        let dir = std::env::temp_dir().join(format!("rtcd-log-append-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rtcd.log");
        std::fs::write(&path, "old\n").unwrap();
        let mut f = RotatingFile::open(&path, 1000, 2).unwrap();
        f.write_all(b"new\n").unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "old\nnew\n");
        std::fs::remove_dir_all(&dir).ok();
    }
}
