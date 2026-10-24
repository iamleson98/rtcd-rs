//! Persistent key/value storage for the credentials of registered clients.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use parking_lot::Mutex;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

/// Why a store operation failed.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("error: not found")]
    NotFound,
    #[error("error: empty key")]
    EmptyKey,
    #[error("error: conflict")]
    Conflict,
    #[error("{0}")]
    Other(String),
}

fn other(e: impl std::fmt::Display) -> StoreError {
    StoreError::Other(e.to_string())
}

/// A string to string map that survives restarts.
pub trait Store: Send + Sync {
    /// Stores a value under a key that does not exist yet.
    fn put(&self, key: &str, value: &str) -> Result<(), StoreError>;
    /// Stores a value, replacing any.
    fn set(&self, key: &str, value: &str) -> Result<(), StoreError>;
    fn get(&self, key: &str) -> Result<String, StoreError>;
    fn delete(&self, key: &str) -> Result<(), StoreError>;
}

const CLIENTS: TableDefinition<&str, &str> = TableDefinition::new("clients");
const FILE: &str = "rtcd.redb";

/// A store in a directory, on an embedded transactional database. Every write is durable when it
/// returns.
pub struct DiskStore {
    db: Database,
}

impl std::fmt::Debug for DiskStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("DiskStore")
    }
}

impl DiskStore {
    /// Opens the store in directory `dir`, creating it (readable by its owner only) if needed.
    pub fn open(dir: &Path) -> Result<Self, StoreError> {
        create_private_dir(dir).map_err(other)?;
        let path: PathBuf = dir.join(FILE);
        let fresh = !path.exists();
        let db = Database::create(&path).map_err(other)?;
        // Make sure the table exists so a first read of an empty store is "not found", not an error.
        let tx = db.begin_write().map_err(other)?;
        {
            let mut clients = tx.open_table(CLIENTS).map_err(other)?;
            // A directory the Go rtcd used: take over its registered clients, once.
            if fresh {
                let legacy = crate::legacy::read_bitcask(dir).map_err(other)?;
                for (id, hash) in &legacy {
                    clients.insert(id.as_str(), hash.as_str()).map_err(other)?;
                }
                if !legacy.is_empty() {
                    tracing::info!(
                        clients = legacy.len(),
                        "store: imported clients from the Go rtcd's data files"
                    );
                }
            }
        }
        tx.commit().map_err(other)?;
        Ok(Self { db })
    }
}

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(dir)
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

fn check(key: &str) -> Result<(), StoreError> {
    if key.is_empty() {
        Err(StoreError::EmptyKey)
    } else {
        Ok(())
    }
}

impl Store for DiskStore {
    fn put(&self, key: &str, value: &str) -> Result<(), StoreError> {
        check(key)?;
        let tx = self.db.begin_write().map_err(other)?;
        {
            let mut t = tx.open_table(CLIENTS).map_err(other)?;
            if t.get(key).map_err(other)?.is_some() {
                return Err(StoreError::Conflict);
            }
            t.insert(key, value).map_err(other)?;
        }
        tx.commit().map_err(other)
    }

    fn set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        check(key)?;
        let tx = self.db.begin_write().map_err(other)?;
        {
            tx.open_table(CLIENTS)
                .map_err(other)?
                .insert(key, value)
                .map_err(other)?;
        }
        tx.commit().map_err(other)
    }

    fn get(&self, key: &str) -> Result<String, StoreError> {
        check(key)?;
        let tx = self.db.begin_read().map_err(other)?;
        let t = tx.open_table(CLIENTS).map_err(other)?;
        let v = t.get(key).map_err(other)?.ok_or(StoreError::NotFound)?;
        Ok(v.value().to_owned())
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        check(key)?;
        let tx = self.db.begin_write().map_err(other)?;
        {
            tx.open_table(CLIENTS)
                .map_err(other)?
                .remove(key)
                .map_err(other)?;
        }
        tx.commit().map_err(other)
    }
}

/// A store that forgets everything when dropped; for tests.
#[derive(Debug, Default)]
pub struct MemoryStore {
    map: Mutex<HashMap<String, String>>,
}

impl Store for MemoryStore {
    fn put(&self, key: &str, value: &str) -> Result<(), StoreError> {
        check(key)?;
        let mut m = self.map.lock();
        if m.contains_key(key) {
            return Err(StoreError::Conflict);
        }
        m.insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    fn set(&self, key: &str, value: &str) -> Result<(), StoreError> {
        check(key)?;
        self.map.lock().insert(key.to_owned(), value.to_owned());
        Ok(())
    }

    fn get(&self, key: &str) -> Result<String, StoreError> {
        check(key)?;
        self.map
            .lock()
            .get(key)
            .cloned()
            .ok_or(StoreError::NotFound)
    }

    fn delete(&self, key: &str) -> Result<(), StoreError> {
        check(key)?;
        self.map.lock().remove(key);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exercise(s: &dyn Store) {
        assert!(matches!(s.get("a"), Err(StoreError::NotFound)));
        s.put("a", "1").unwrap();
        assert_eq!(s.get("a").unwrap(), "1");
        assert!(
            matches!(s.put("a", "2"), Err(StoreError::Conflict)),
            "put does not overwrite"
        );
        s.set("a", "3").unwrap();
        assert_eq!(s.get("a").unwrap(), "3", "set does");
        s.delete("a").unwrap();
        assert!(matches!(s.get("a"), Err(StoreError::NotFound)));
        s.delete("a").unwrap();
        for op in [
            s.get(""),
            s.put("", "x").map(|()| String::new()),
            s.set("", "x").map(|()| String::new()),
            s.delete("").map(|()| String::new()),
        ] {
            assert!(matches!(op, Err(StoreError::EmptyKey)));
        }
    }

    #[test]
    fn the_memory_store_behaves() {
        exercise(&MemoryStore::default());
    }

    fn temp_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rtcd-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn the_disk_store_behaves() {
        let dir = temp_dir("behaves");
        exercise(&DiskStore::open(&dir).unwrap());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn values_survive_a_restart() {
        let dir = temp_dir("restart");
        {
            let s = DiskStore::open(&dir).unwrap();
            s.put("client", "$2b$10$hash").unwrap();
        }
        let s = DiskStore::open(&dir).unwrap();
        assert_eq!(s.get("client").unwrap(), "$2b$10$hash");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[cfg(unix)]
    #[test]
    fn the_directory_is_private() {
        use std::os::unix::fs::PermissionsExt;
        let dir = temp_dir("private");
        DiskStore::open(&dir).unwrap();
        assert_eq!(
            std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
            0o700
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn clients_of_the_go_stores_data_files_are_taken_over_once() {
        let dir = std::env::temp_dir().join(format!("rtcd-takeover-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&dir).unwrap();
        let value = "$2a$10$mBbEvz1cpOneYRq.LhOqXOFqg7c2AimGVSRJOCg/37JBYnxXFxwKu";
        let mut file = vec![0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0x3c];
        file.extend(b"lms");
        file.extend(value.as_bytes());
        file.extend([0x86, 0xb0, 0x28, 0x55]);
        file.extend([0u8; 8]);
        std::fs::write(dir.join("000000000.data"), file).unwrap();
        {
            let store = DiskStore::open(&dir).unwrap();
            assert_eq!(store.get("lms").unwrap(), value);
            store.delete("lms").unwrap();
        }
        let store = DiskStore::open(&dir).unwrap();
        assert!(
            matches!(store.get("lms"), Err(StoreError::NotFound)),
            "a client deleted afterwards is not brought back by the old files"
        );
        std::fs::remove_dir_all(dir).unwrap();
    }
}
