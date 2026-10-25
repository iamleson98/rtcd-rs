//! The whiteboard of a recording is not filmed: its changes are logged as they reach the room,
//! with their offset into the recording, and the player draws them again. The log is gzipped
//! NDJSON, one record per line:
//!
//! ```text
//! {"t":0,"k":"snapshot","elements":[…],"locked":false}  the whole board
//! {"t":1520,"k":"elements","u":"<user>","elements":[…]} strokes, moves, deletes
//! {"t":9100,"k":"clear"}
//! {"t":9300,"k":"lock","locked":true}
//! {"t":2040,"k":"presence","u":"<user>","p":{"cursor":[x,y],"preview":…}}
//! ```
//!
//! Elements are the server's last-writer-wins envelopes, passed through unchanged; a snapshot
//! after a reconnect merges like any other batch.

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use flate2::Compression;
use flate2::write::GzEncoder;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use tracing::warn;

use crate::config::PRESENCE_EVERY;
use crate::disk::Disk;

#[derive(Serialize)]
struct Record<'a> {
    t: i64,
    k: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    u: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    elements: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    locked: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    p: Option<&'a RawValue>,
}

/// Counts what is written against the disk.
struct Counted<W> {
    inner: W,
    disk: Disk,
}

impl<W: Write> Write for Counted<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.disk.wrote(n as u64);
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

pub struct BoardLog {
    path: PathBuf,
    start: Instant,
    gz: Option<GzEncoder<BufWriter<Counted<File>>>>,
    presence: HashMap<String, Instant>,
    err: Option<io::Error>,
}

impl std::fmt::Debug for BoardLog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoardLog")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

impl BoardLog {
    pub fn create(path: &Path, start: Instant, disk: Disk) -> io::Result<Self> {
        let file = File::create(path)?;
        let out = BufWriter::with_capacity(16 << 10, Counted { inner: file, disk });
        Ok(Self {
            path: path.to_owned(),
            start,
            gz: Some(GzEncoder::new(out, Compression::default())),
            presence: HashMap::new(),
            err: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn write(&mut self, at: Instant, mut r: Record<'_>) {
        let Some(gz) = self.gz.as_mut() else { return };
        if self.err.is_some() {
            return;
        }
        r.t = at.saturating_duration_since(self.start).as_millis() as i64;
        let result = serde_json::to_writer(&mut *gz, &r)
            .map_err(io::Error::from)
            .and_then(|()| gz.write_all(b"\n"));
        if let Err(e) = result {
            self.err = Some(e);
        }
    }

    fn rec<'a>(k: &'a str) -> Record<'a> {
        Record {
            t: 0,
            k,
            u: "",
            elements: None,
            locked: None,
            p: None,
        }
    }

    pub fn snapshot(&mut self, at: Instant, elements: &RawValue, locked: bool) {
        self.write(
            at,
            Record {
                elements: Some(elements),
                locked: Some(locked),
                ..Self::rec("snapshot")
            },
        );
    }

    pub fn elements(&mut self, at: Instant, user: &str, elements: &RawValue) {
        self.write(
            at,
            Record {
                u: user,
                elements: Some(elements),
                ..Self::rec("elements")
            },
        );
    }

    pub fn clear(&mut self, at: Instant) {
        self.write(at, Self::rec("clear"));
    }

    pub fn lock(&mut self, at: Instant, locked: bool) {
        self.write(
            at,
            Record {
                locked: Some(locked),
                ..Self::rec("lock")
            },
        );
    }

    /// Logs a person's pointer and in-progress stroke. A pointer that only moved is logged at
    /// most every [`PRESENCE_EVERY`]; one that draws is always logged (a stroke arrives as its new
    /// points, which the player adds up), and so is a pointer leaving or a stroke ending, so
    /// nothing stays on screen that the room no longer showed.
    pub fn pointer(&mut self, at: Instant, user: &str, presence: &RawValue) {
        /// A field as written, so that `null` is told from absent.
        fn raw<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<&'de RawValue>, D::Error> {
            <&RawValue>::deserialize(d).map(Some)
        }
        #[derive(Deserialize)]
        struct Pointer<'a> {
            #[serde(borrow, default, deserialize_with = "raw")]
            cursor: Option<&'a RawValue>,
            #[serde(borrow, default, deserialize_with = "raw")]
            preview: Option<&'a RawValue>,
        }
        let p: Pointer<'_> = match serde_json::from_str(presence.get()) {
            Ok(p) => p,
            Err(e) => {
                warn!(user_id = user, error = %e, "unreadable pointer on the board");
                return;
            }
        };
        let is_null = |v: &Option<&RawValue>| v.is_some_and(|v| v.get() == "null");
        let drawing = p.preview.is_some_and(|v| v.get() != "null");
        let ending = is_null(&p.cursor) || is_null(&p.preview);
        let last = self.presence.get(user).copied();
        let keep = drawing
            || ending
            || last.is_none_or(|l| at.saturating_duration_since(l) >= PRESENCE_EVERY);
        if keep {
            self.presence.insert(user.to_owned(), at);
            self.write(
                at,
                Record {
                    u: user,
                    p: Some(presence),
                    ..Self::rec("presence")
                },
            );
        }
    }

    /// Finishes the log and reports the first write error, if any.
    pub fn close(&mut self) -> io::Result<()> {
        if let Some(gz) = self.gz.take() {
            let result = gz.finish().and_then(|mut w| w.flush());
            if let Err(e) = result
                && self.err.is_none()
            {
                self.err = Some(e);
            }
        }
        match self.err.take() {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::time::Duration;

    use flate2::read::GzDecoder;

    use super::*;

    fn raw(s: &str) -> Box<RawValue> {
        RawValue::from_string(s.to_owned()).unwrap()
    }

    fn log(tag: &str) -> (BoardLog, Instant, PathBuf) {
        let path =
            std::env::temp_dir().join(format!("board-{}-{tag}.ndjson.gz", std::process::id()));
        let start = Instant::now();
        (
            BoardLog::create(&path, start, Disk::new(u64::MAX)).unwrap(),
            start,
            path,
        )
    }

    fn read(path: &Path) -> Vec<serde_json::Value> {
        let mut text = String::new();
        GzDecoder::new(File::open(path).unwrap())
            .read_to_string(&mut text)
            .unwrap();
        std::fs::remove_file(path).unwrap();
        text.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn the_log_keeps_what_the_player_needs() {
        let (mut b, start, path) = log("log");
        let at = |ms: u64| start + Duration::from_millis(ms);
        b.snapshot(at(0), &raw(r#"[{"id":"a"}]"#), false);
        b.pointer(at(1000), "u1", &raw(r#"{"cursor":[1,2]}"#));
        b.pointer(at(1050), "u1", &raw(r#"{"cursor":[3,4]}"#)); // too soon: skipped
        b.pointer(at(1060), "u2", &raw(r#"{"cursor":[5,6]}"#)); // someone else
        b.pointer(at(1070), "u1", &raw(r#"{"cursor":null,"preview":null}"#)); // leaving: kept
        b.pointer(
            at(1080),
            "u2",
            &raw(r#"{"cursor":[5,7],"preview":{"from":3,"points":[5,7,0.5]}}"#),
        ); // drawing
        b.pointer(
            at(1090),
            "u2",
            &raw(r#"{"cursor":[5,8],"preview":{"from":4,"points":[5,8,0.5]}}"#),
        ); // every one
        b.elements(at(1500), "u1", &raw(r#"[{"id":"b"}]"#));
        b.lock(at(2000), true);
        b.clear(at(3000));
        b.close().unwrap();

        let got: Vec<String> = read(&path)
            .iter()
            .map(|r| {
                format!(
                    "{}@{}:{}",
                    r["k"].as_str().unwrap(),
                    r["t"],
                    r.get("u").and_then(|u| u.as_str()).unwrap_or("")
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                "snapshot@0:",
                "presence@1000:u1",
                "presence@1060:u2",
                "presence@1070:u1",
                "presence@1080:u2",
                "presence@1090:u2",
                "elements@1500:u1",
                "lock@2000:",
                "clear@3000:"
            ]
        );
    }

    #[test]
    fn records_carry_their_payloads_unchanged() {
        let (mut b, start, path) = log("payload");
        b.snapshot(start, &raw(r#"[ {"id":"a", "v":1.50} ]"#), true);
        b.lock(start, false);
        b.close().unwrap();
        let rows = read(&path);
        assert_eq!(rows[0]["locked"], true);
        assert_eq!(rows[0]["elements"][0]["v"], 1.5);
        assert_eq!(rows[1]["locked"], false);
    }

    #[test]
    fn an_unreadable_pointer_is_skipped_and_costs_no_slot() {
        let (mut b, start, path) = log("bad");
        b.pointer(start, "u1", &raw(r#""not a pointer""#));
        b.pointer(start, "u1", &raw(r#"{"cursor":[1,2]}"#));
        b.close().unwrap();
        assert_eq!(read(&path).len(), 1);
    }

    #[test]
    fn bytes_are_counted_against_the_disk() {
        let path =
            std::env::temp_dir().join(format!("board-{}-disk.ndjson.gz", std::process::id()));
        let disk = Disk::new(u64::MAX);
        let start = Instant::now();
        let mut b = BoardLog::create(&path, start, disk.clone()).unwrap();
        for i in 0..200 {
            b.elements(start, "u", &raw(&format!(r#"[{{"id":"{i}"}}]"#)));
        }
        b.close().unwrap();
        assert_eq!(disk.written(), std::fs::metadata(&path).unwrap().len());
        std::fs::remove_file(&path).unwrap();
    }
}
