//! The process logger: one JSON object per line, shaped like the server's (`timestamp`,
//! lower-case `level`, `msg`), so a single query finds a recording's lines next to the server's in
//! the log database. Every line carries the recording id.

use std::fmt::{self, Write as _};
use std::io::Write;
use std::sync::Mutex;
use std::time::SystemTime;

use serde_json::{Map, Value};
use tracing::field::{Field, Visit};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{Layer, registry::LookupSpan};

/// Parses a level name (`LOG_LEVEL`); empty means info.
pub fn parse_level(name: &str) -> Option<Level> {
    match name.to_ascii_lowercase().as_str() {
        "" | "info" => Some(Level::INFO),
        "debug" => Some(Level::DEBUG),
        "warn" | "warning" => Some(Level::WARN),
        "error" => Some(Level::ERROR),
        _ => None,
    }
}

/// Whether to keep a record of `level` from `target`: the recorder's own at the configured level
/// and above, the libraries underneath at warn and above (everything at debug).
fn keeps(target: &str, level: Level, configured: Level) -> bool {
    let library = [
        "rtc::",
        "rtc_",
        "hyper",
        "tungstenite",
        "tokio_tungstenite",
        "h2",
        "rustls",
        "reqwest",
    ]
    .iter()
    .any(|p| target.starts_with(p));
    let limit = if library && configured != Level::DEBUG {
        Level::WARN
    } else {
        configured
    };
    level <= limit
}

/// `2026-10-24T11:06:05.123456Z`.
pub fn timestamp(t: SystemTime) -> String {
    let d = t.duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default();
    let secs = d.as_secs() as i64;
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // Civil date from days since the epoch (H. Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60,
        d.subsec_micros()
    )
}

struct Fields<'a>(&'a mut Map<String, Value>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, f: &Field, v: &str) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_i64(&mut self, f: &Field, v: i64) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_u64(&mut self, f: &Field, v: u64) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_bool(&mut self, f: &Field, v: bool) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_f64(&mut self, f: &Field, v: f64) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_debug(&mut self, f: &Field, v: &dyn fmt::Debug) {
        let mut s = String::new();
        let _ = write!(s, "{v:?}");
        self.0.insert(f.name().into(), s.into());
    }
}

struct JsonLines<W> {
    out: Mutex<W>,
    recording_id: String,
    level: Level,
}

impl<S, W> Layer<S> for JsonLines<W>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    W: Write + 'static,
{
    fn enabled(&self, meta: &tracing::Metadata<'_>, _: Context<'_, S>) -> bool {
        keeps(meta.target(), *meta.level(), self.level)
    }

    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let mut fields = Map::new();
        event.record(&mut Fields(&mut fields));
        let msg = fields.remove("message").unwrap_or(Value::Null);
        let mut line = Map::new();
        line.insert("timestamp".into(), timestamp(SystemTime::now()).into());
        line.insert(
            "level".into(),
            event
                .metadata()
                .level()
                .as_str()
                .to_ascii_lowercase()
                .into(),
        );
        line.insert("msg".into(), msg);
        line.insert("recording_id".into(), self.recording_id.clone().into());
        for (k, v) in fields {
            line.insert(k, v);
        }
        let mut text = serde_json::to_string(&Value::Object(line)).unwrap_or_default();
        text.push('\n');
        if let Ok(mut out) = self.out.lock() {
            let _ = out.write_all(text.as_bytes());
        }
    }
}

/// Installs the global logger writing to stdout. Returns whether `level` named a known level (an
/// unknown one logs at info).
pub fn init(level: &str, recording_id: &str) -> bool {
    let (lvl, ok) = match parse_level(level) {
        Some(l) => (l, true),
        None => (Level::INFO, false),
    };
    let layer = JsonLines {
        out: Mutex::new(std::io::stdout()),
        recording_id: recording_id.to_owned(),
        level: lvl,
    };
    let _ = tracing_subscriber::registry().with(layer).try_init();
    ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[derive(Clone)]
    struct Buf(Arc<Mutex<Vec<u8>>>);
    impl Write for Buf {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn lines(level: Level, f: impl FnOnce()) -> Vec<Value> {
        let buf = Buf(Arc::new(Mutex::new(Vec::new())));
        let layer = JsonLines {
            out: Mutex::new(buf.clone()),
            recording_id: "rec1".into(),
            level,
        };
        let sub = tracing_subscriber::registry().with(layer);
        tracing::subscriber::with_default(sub, f);
        let text = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        text.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn a_line_is_shaped_like_the_servers() {
        let out = lines(Level::INFO, || {
            tracing::info!(files = 3, kind = "voice", "uploaded files");
            tracing::warn!("careful");
            tracing::debug!("hidden");
        });
        assert_eq!(out.len(), 2);
        let l = &out[0];
        assert_eq!(l["level"], "info");
        assert_eq!(l["msg"], "uploaded files");
        assert_eq!(l["recording_id"], "rec1");
        assert_eq!(l["files"], 3);
        assert_eq!(l["kind"], "voice");
        let ts = l["timestamp"].as_str().unwrap();
        assert!(
            ts.ends_with('Z') && ts.len() == 27 && &ts[10..11] == "T",
            "{ts}"
        );
        assert_eq!(out[1]["level"], "warn");
    }

    #[test]
    fn the_level_comes_from_the_environment() {
        assert_eq!(parse_level(""), Some(Level::INFO));
        assert_eq!(parse_level("DEBUG"), Some(Level::DEBUG));
        assert_eq!(parse_level("warn"), Some(Level::WARN));
        assert_eq!(parse_level("loud"), None);
        let out = lines(Level::DEBUG, || tracing::debug!("shown"));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn libraries_are_quieter_than_the_recorder() {
        assert!(keeps("recorder::call", Level::INFO, Level::INFO));
        assert!(!keeps("rtc_ice::agent", Level::INFO, Level::INFO));
        assert!(keeps("rtc_ice::agent", Level::WARN, Level::INFO));
        assert!(keeps("rtc_ice::agent", Level::DEBUG, Level::DEBUG));
    }

    #[test]
    fn timestamps_are_civil_dates() {
        let t = SystemTime::UNIX_EPOCH
            + std::time::Duration::from_secs(1_791_285_965)
            + std::time::Duration::from_micros(123_456);
        assert_eq!(timestamp(t), "2026-10-06T11:26:05.123456Z");
        assert_eq!(
            timestamp(SystemTime::UNIX_EPOCH),
            "1970-01-01T00:00:00.000000Z"
        );
        let leap = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(951_782_400);
        assert_eq!(timestamp(leap), "2000-02-29T00:00:00.000000Z");
    }
}
