//! Reads the data files of the Go rtcd's store (bitcask), so a Rust rtcd started on the Go
//! deployment's data directory keeps the clients that are registered there.
//!
//! A data file is a sequence of records, each
//! `key size (u32) | value size (u64) | key | value | crc32 of the value (u32) | expiry (u64)`,
//! all big-endian. A delete is a record with an empty value. Files are replayed in numeric order.

use std::collections::BTreeMap;
use std::path::Path;

const HEADER: usize = 12;
const TRAILER: usize = 12;

/// The live entries of the bitcask data files in `dir`, or none if there are no such files.
/// Records after the first damaged one in a file are ignored, as the Go store does on recovery.
pub fn read_bitcask(dir: &Path) -> std::io::Result<BTreeMap<String, String>> {
    let mut files: Vec<(u64, std::path::PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().is_some_and(|e| e == "data")
            && let Some(id) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse().ok())
        {
            files.push((id, path));
        }
    }
    files.sort();
    let mut live = BTreeMap::new();
    for (_, path) in files {
        replay(&std::fs::read(path)?, &mut live);
    }
    Ok(live)
}

fn replay(data: &[u8], live: &mut BTreeMap<String, String>) {
    let mut at = 0;
    while data.len() - at >= HEADER {
        let key_len = u32::from_be_bytes(data[at..at + 4].try_into().unwrap()) as usize;
        let value_len = u64::from_be_bytes(data[at + 4..at + 12].try_into().unwrap());
        let Some(end) = usize::try_from(value_len)
            .ok()
            .and_then(|v| (at + HEADER).checked_add(key_len)?.checked_add(v))
            .and_then(|e| e.checked_add(TRAILER))
            .filter(|&e| e <= data.len())
        else {
            return;
        };
        let key_at = at + HEADER;
        let value_at = key_at + key_len;
        let value_end = end - TRAILER;
        let value = &data[value_at..value_end];
        let crc = u32::from_be_bytes(data[value_end..value_end + 4].try_into().unwrap());
        if crc != crc32fast::hash(value) {
            return;
        }
        if let (Ok(key), Ok(value)) = (
            std::str::from_utf8(&data[key_at..value_at]),
            std::str::from_utf8(value),
        ) {
            if value.is_empty() {
                live.remove(key);
            } else {
                live.insert(key.to_owned(), value.to_owned());
            }
        }
        at = end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &str, value: &str) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend((key.len() as u32).to_be_bytes());
        out.extend((value.len() as u64).to_be_bytes());
        out.extend(key.as_bytes());
        out.extend(value.as_bytes());
        out.extend(crc32fast::hash(value.as_bytes()).to_be_bytes());
        out.extend(0u64.to_be_bytes());
        out
    }

    fn live(data: &[u8]) -> BTreeMap<String, String> {
        let mut m = BTreeMap::new();
        replay(data, &mut m);
        m
    }

    #[test]
    fn a_record_written_by_the_go_store_is_read() {
        // The data file the Go rtcd wrote when "lms" was registered.
        let value = "$2a$10$mBbEvz1cpOneYRq.LhOqXOFqg7c2AimGVSRJOCg/37JBYnxXFxwKu";
        let mut file = Vec::new();
        file.extend([0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0x3c]);
        file.extend(b"lms");
        file.extend(value.as_bytes());
        file.extend([0x86, 0xb0, 0x28, 0x55]);
        file.extend([0u8; 8]);
        assert_eq!(live(&file).get("lms").map(String::as_str), Some(value));
    }

    #[test]
    fn a_later_record_replaces_and_an_empty_one_deletes() {
        let mut file = record("a", "1");
        file.extend(record("b", "2"));
        file.extend(record("a", "3"));
        file.extend(record("b", ""));
        let m = live(&file);
        assert_eq!(m.len(), 1);
        assert_eq!(m["a"], "3");
    }

    #[test]
    fn a_torn_tail_keeps_what_came_before() {
        let mut file = record("a", "1");
        let second = record("b", "2");
        file.extend(&second[..second.len() - 3]);
        assert_eq!(live(&file).len(), 1);
    }

    #[test]
    fn a_bad_checksum_ends_the_file() {
        let mut file = record("a", "1");
        let mut bad = record("b", "2");
        let n = bad.len();
        bad[n - 9] ^= 0xff;
        file.extend(bad);
        file.extend(record("c", "3"));
        let m = live(&file);
        assert_eq!(m.keys().collect::<Vec<_>>(), ["a"]);
    }

    #[test]
    fn files_are_replayed_in_numeric_order() {
        let dir = tempdir();
        std::fs::write(dir.join("000000010.data"), record("a", "new")).unwrap();
        std::fs::write(dir.join("000000002.data"), record("a", "old")).unwrap();
        std::fs::write(dir.join("config.json"), "{}").unwrap();
        assert_eq!(read_bitcask(&dir).unwrap()["a"], "new");
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn tempdir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("rtcd-legacy-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
