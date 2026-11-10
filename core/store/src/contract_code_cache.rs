//! Node-local cache of raw contract code (wasm), keyed by code hash.
//!
//! Contract code is global: every node that executes or validates a call to a
//! contract needs the same bytes, whichever shard the calling account lives
//! in. In state the bytes are kept per shard, and a stateless validator that
//! tracks no shard keeps no raw code at all: bytes arrive with a witness or a
//! code response, get compiled, and are dropped. This cache keeps those bytes,
//! so a node can recompile locally when the compiled-contract cache is re-keyed
//! (protocol or binary upgrade) or evicted, instead of fetching the code over
//! the network again.
//!
//! It is only ever a cache, never a source of truth. Whether an account holds a
//! contract, which hash it has and how long the code is are always read from
//! the trie (or the witness); the cache only supplies the bytes for a hash that
//! state has already proven. Entries are content addressed, so they are
//! immutable, and a stale or corrupt entry is harmless: [`ContractCodeCache::get`]
//! verifies the hash and treats a mismatch as a miss.
use crate::metrics;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives_core::code::ContractCode;
use parking_lot::Mutex;
use std::io;
use std::path::{Path, PathBuf};
use std::str::FromStr;

pub trait ContractCodeCache: Send + Sync {
    /// The code for `code_hash`, if the cache holds it.
    fn get(&self, code_hash: &CryptoHash) -> Option<ContractCode>;

    /// Whether the cache holds the code for `code_hash`. Does not read the bytes.
    fn has(&self, code_hash: &CryptoHash) -> bool;

    /// Keeps `code`. A no-op when the cache already holds it.
    fn put(&self, code: &ContractCode);

    /// TESTING ONLY: removes every entry.
    #[cfg(feature = "test_features")]
    fn test_only_clear(&self);
}

impl std::fmt::Debug for dyn ContractCodeCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ContractCodeCache")
    }
}

/// A cache that holds nothing: for nodes that disabled the cache and for code paths that do not
/// execute contracts. Every lookup misses and every `put` is dropped.
#[derive(Default, Clone, Copy, Debug)]
pub struct NoContractCodeCache;

impl ContractCodeCache for NoContractCodeCache {
    fn get(&self, _code_hash: &CryptoHash) -> Option<ContractCode> {
        None
    }

    fn has(&self, _code_hash: &CryptoHash) -> bool {
        false
    }

    fn put(&self, _code: &ContractCode) {}

    #[cfg(feature = "test_features")]
    fn test_only_clear(&self) {}
}

/// Byte-bounded LRU index of the files in the cache directory.
struct Index {
    entries: lru::LruCache<CryptoHash, u64>,
    total_bytes: u64,
}

impl Index {
    fn new() -> Self {
        Self { entries: lru::LruCache::unbounded(), total_bytes: 0 }
    }

    fn insert(&mut self, code_hash: CryptoHash, len: u64) {
        if let Some((_, old_len)) = self.entries.push(code_hash, len) {
            self.total_bytes -= old_len;
        }
        self.total_bytes += len;
    }

    fn remove(&mut self, code_hash: &CryptoHash) -> Option<u64> {
        let len = self.entries.pop(code_hash)?;
        self.total_bytes -= len;
        Some(len)
    }

    /// Pops least recently used entries until the total fits in `max_bytes`.
    fn evict_over(&mut self, max_bytes: u64) -> Vec<CryptoHash> {
        let mut evicted = Vec::new();
        while self.total_bytes > max_bytes {
            let Some((code_hash, len)) = self.entries.pop_lru() else {
                break;
            };
            self.total_bytes -= len;
            evicted.push(code_hash);
        }
        evicted
    }

    fn report(&self) {
        metrics::CONTRACT_CODE_CACHE_SIZE_BYTES.set(self.total_bytes as i64);
        metrics::CONTRACT_CODE_CACHE_ENTRIES.set(self.entries.len() as i64);
    }
}

/// [`ContractCodeCache`] keeping one file per contract, named by the code hash,
/// in a directory next to the compiled-contract cache.
///
/// The directory is a byte-bounded LRU: `put` evicts the least
/// recently read entries once the total exceeds the limit. The index is
/// rebuilt from the directory at startup, ordered by file modification time.
pub struct FilesystemContractCodeCache {
    dir: PathBuf,
    max_bytes: u64,
    index: Mutex<Index>,
    _test_temp_dir: Option<tempfile::TempDir>,
}

impl FilesystemContractCodeCache {
    /// Opens (creating if needed) the cache at `dir`, holding at most `max_bytes` of code.
    pub fn new(dir: &Path, max_bytes: u64) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let index = Self::scan(dir, max_bytes)?;
        index.report();
        tracing::debug!(
            target: "store",
            path = %dir.display(),
            entries = index.entries.len(),
            total_bytes = index.total_bytes,
            "opened the contract code cache"
        );
        Ok(Self {
            dir: dir.to_path_buf(),
            max_bytes,
            index: Mutex::new(index),
            _test_temp_dir: None,
        })
    }

    /// A cache in a fresh temporary directory, with no effective size limit.
    pub fn test() -> io::Result<Self> {
        let temp_dir = tempfile::TempDir::new()?;
        let mut cache = Self::new(&temp_dir.path().join("contract.code"), u64::MAX)?;
        cache._test_temp_dir = Some(temp_dir);
        Ok(cache)
    }

    pub fn len(&self) -> usize {
        self.index.lock().entries.len()
    }

    pub fn total_bytes(&self) -> u64 {
        self.index.lock().total_bytes
    }

    /// Builds the index from the files in `dir`, least recently modified first,
    /// trimming the oldest entries if the directory is over `max_bytes`.
    /// Modification times are coarse, so files written within the same clock tick come in an
    /// arbitrary order; that only matters when the directory is already over the limit.
    /// Leftover temporary files from an interrupted `put` are removed.
    fn scan(dir: &Path, max_bytes: u64) -> io::Result<Index> {
        let mut found = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let Ok(entry) = entry else {
                continue;
            };
            let file_name = entry.file_name();
            let Some(file_name) = file_name.to_str() else {
                continue;
            };
            if file_name.ends_with(TEMP_SUFFIX) {
                let _ = std::fs::remove_file(entry.path());
                continue;
            }
            let Ok(code_hash) = CryptoHash::from_str(file_name) else {
                continue;
            };
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if !metadata.is_file() {
                continue;
            }
            found.push((metadata.modified().ok(), code_hash, metadata.len()));
        }
        // Ascending by modification time, so the newest file ends up most recently used.
        found.sort_by_key(|(modified, _, _)| *modified);
        let mut index = Index::new();
        for (_, code_hash, len) in found {
            index.insert(code_hash, len);
        }
        let evicted = index.evict_over(max_bytes);
        if !evicted.is_empty() {
            tracing::info!(
                target: "store",
                trimmed = evicted.len(),
                max_bytes,
                "contract code cache exceeded its size limit at startup; trimmed the oldest entries"
            );
        }
        for code_hash in evicted {
            Self::remove_file(dir, &code_hash);
        }
        Ok(index)
    }

    fn path_for(dir: &Path, code_hash: &CryptoHash) -> PathBuf {
        dir.join(code_hash.to_string())
    }

    fn remove_file(dir: &Path, code_hash: &CryptoHash) {
        match std::fs::remove_file(Self::path_for(dir, code_hash)) {
            Ok(()) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => tracing::debug!(
                target: "store",
                %code_hash,
                err = &err as &dyn std::error::Error,
                "failed to remove a contract code cache file; the cache may exceed its size limit"
            ),
        }
    }

    /// Drops `code_hash` from the index and the directory.
    fn forget(&self, code_hash: &CryptoHash) {
        {
            let mut index = self.index.lock();
            index.remove(code_hash);
            index.report();
        }
        Self::remove_file(&self.dir, code_hash);
    }
}

const TEMP_SUFFIX: &str = ".tmp";

impl ContractCodeCache for FilesystemContractCodeCache {
    fn get(&self, code_hash: &CryptoHash) -> Option<ContractCode> {
        let bytes = match std::fs::read(Self::path_for(&self.dir, code_hash)) {
            Ok(bytes) => bytes,
            Err(err) => {
                if err.kind() != io::ErrorKind::NotFound {
                    tracing::debug!(
                        target: "store",
                        %code_hash,
                        err = &err as &dyn std::error::Error,
                        "failed to read a contract code cache file"
                    );
                }
                self.forget(code_hash);
                metrics::CONTRACT_CODE_CACHE_MISSES.inc();
                return None;
            }
        };
        if hash(&bytes) != *code_hash {
            tracing::warn!(
                target: "store",
                %code_hash,
                "contract code cache file does not match its hash; removing it"
            );
            self.forget(code_hash);
            metrics::CONTRACT_CODE_CACHE_MISSES.inc();
            return None;
        }
        self.index.lock().entries.promote(code_hash);
        metrics::CONTRACT_CODE_CACHE_HITS.inc();
        Some(ContractCode::new(bytes, Some(*code_hash)))
    }

    fn has(&self, code_hash: &CryptoHash) -> bool {
        if !self.index.lock().entries.contains(code_hash) {
            return false;
        }
        // The index is authoritative for what `put` stored, but the directory may have been
        // cleaned up out from under us; do not promise code that `get` would then fail to read.
        if Self::path_for(&self.dir, code_hash).is_file() {
            true
        } else {
            self.forget(code_hash);
            false
        }
    }

    fn put(&self, code: &ContractCode) {
        let code_hash = *code.hash();
        let len = code.code().len() as u64;
        if len > self.max_bytes || self.index.lock().entries.contains(&code_hash) {
            return;
        }
        // Write to a temporary name and rename into place, so a reader never sees a partial
        // file. No fsync: a file truncated by a crash fails the hash check in `get`.
        let temp_name = format!("{code_hash}.{:016x}{TEMP_SUFFIX}", rand::random::<u64>());
        let temp_path = self.dir.join(temp_name);
        if let Err(err) = std::fs::write(&temp_path, code.code()) {
            tracing::debug!(
                target: "store",
                %code_hash,
                err = &err as &dyn std::error::Error,
                "failed to write a contract code cache file"
            );
            let _ = std::fs::remove_file(&temp_path);
            return;
        }
        let evicted = {
            let mut index = self.index.lock();
            if index.entries.contains(&code_hash) {
                drop(index);
                let _ = std::fs::remove_file(&temp_path);
                return;
            }
            if let Err(err) = std::fs::rename(&temp_path, Self::path_for(&self.dir, &code_hash)) {
                drop(index);
                tracing::debug!(
                    target: "store",
                    %code_hash,
                    err = &err as &dyn std::error::Error,
                    "failed to move a contract code cache file into place"
                );
                let _ = std::fs::remove_file(&temp_path);
                return;
            }
            index.insert(code_hash, len);
            let evicted = index.evict_over(self.max_bytes);
            index.report();
            evicted
        };
        metrics::CONTRACT_CODE_CACHE_PUTS.inc();
        for victim in evicted {
            Self::remove_file(&self.dir, &victim);
            metrics::CONTRACT_CODE_CACHE_EVICTIONS.inc();
        }
    }

    #[cfg(feature = "test_features")]
    fn test_only_clear(&self) {
        let mut index = self.index.lock();
        let victims: Vec<CryptoHash> =
            index.entries.iter().map(|(code_hash, _)| *code_hash).collect();
        for code_hash in victims {
            index.remove(&code_hash);
            Self::remove_file(&self.dir, &code_hash);
        }
        index.report();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(byte: u8, len: usize) -> ContractCode {
        ContractCode::new(vec![byte; len], None)
    }

    fn cache_in(dir: &Path, max_bytes: u64) -> FilesystemContractCodeCache {
        FilesystemContractCodeCache::new(&dir.join("contract.code"), max_bytes).unwrap()
    }

    #[test]
    fn put_get_has() {
        let cache = FilesystemContractCodeCache::test().unwrap();
        let contract = code(1, 100);
        let other = code(2, 100);
        assert!(!cache.has(contract.hash()));
        assert!(cache.get(contract.hash()).is_none());

        cache.put(&contract);
        assert!(cache.has(contract.hash()));
        assert!(!cache.has(other.hash()));
        let read = cache.get(contract.hash()).unwrap();
        assert_eq!(read.hash(), contract.hash());
        assert_eq!(read.code(), contract.code());
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.total_bytes(), 100);

        // Putting the same code again changes nothing.
        cache.put(&contract);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.total_bytes(), 100);
    }

    #[test]
    fn evicts_least_recently_read() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = cache_in(temp_dir.path(), 250);
        let (a, b, c) = (code(1, 100), code(2, 100), code(3, 100));
        cache.put(&a);
        cache.put(&b);
        // Reading `a` makes `b` the least recently used entry.
        assert!(cache.get(a.hash()).is_some());
        cache.put(&c);
        assert_eq!(cache.len(), 2);
        assert!(cache.has(a.hash()));
        assert!(!cache.has(b.hash()));
        assert!(cache.has(c.hash()));
        assert!(!temp_dir.path().join("contract.code").join(b.hash().to_string()).exists());
    }

    #[test]
    fn oversized_code_is_not_stored() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = cache_in(temp_dir.path(), 50);
        let contract = code(1, 100);
        cache.put(&contract);
        assert!(!cache.has(contract.hash()));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn corrupt_file_is_a_miss_and_gets_removed() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = cache_in(temp_dir.path(), u64::MAX);
        let contract = code(1, 100);
        cache.put(&contract);
        let path = temp_dir.path().join("contract.code").join(contract.hash().to_string());
        std::fs::write(&path, b"not the code").unwrap();
        assert!(cache.get(contract.hash()).is_none());
        assert!(!path.exists());
        assert!(!cache.has(contract.hash()));
    }

    #[test]
    fn missing_file_is_forgotten() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache = cache_in(temp_dir.path(), u64::MAX);
        let contract = code(1, 100);
        cache.put(&contract);
        let path = temp_dir.path().join("contract.code").join(contract.hash().to_string());
        std::fs::remove_file(path).unwrap();
        assert!(!cache.has(contract.hash()));
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn reopening_rebuilds_the_index_and_trims_to_the_limit() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let (a, b, c) = (code(1, 100), code(2, 100), code(3, 100));
        {
            let cache = cache_in(temp_dir.path(), u64::MAX);
            cache.put(&a);
            cache.put(&b);
            cache.put(&c);
            // A leftover temporary file from an interrupted put.
            std::fs::write(temp_dir.path().join("contract.code").join("leftover.tmp"), b"x")
                .unwrap();
        }
        // Modification times are what orders the index on reopen, and files written within the
        // same clock tick share one, so pin them: `a` oldest, `c` newest.
        for (contract, secs_ago) in [(&a, 30), (&b, 20), (&c, 10)] {
            let path = temp_dir.path().join("contract.code").join(contract.hash().to_string());
            let modified = std::time::SystemTime::now() - std::time::Duration::from_secs(secs_ago);
            let file = std::fs::File::options().write(true).open(path).unwrap();
            file.set_modified(modified).unwrap();
        }
        let cache = cache_in(temp_dir.path(), u64::MAX);
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.total_bytes(), 300);
        assert!(cache.get(a.hash()).is_some());
        assert!(!temp_dir.path().join("contract.code").join("leftover.tmp").exists());

        // Reopening with a smaller limit keeps the newest files.
        let cache = cache_in(temp_dir.path(), 150);
        assert_eq!(cache.len(), 1);
        assert!(cache.has(c.hash()));
        assert!(!cache.has(a.hash()));
        assert!(!cache.has(b.hash()));
    }

    #[cfg(feature = "test_features")]
    #[test]
    fn clear_removes_everything() {
        let cache = FilesystemContractCodeCache::test().unwrap();
        let contract = code(1, 100);
        cache.put(&contract);
        cache.test_only_clear();
        assert!(!cache.has(contract.hash()));
        assert_eq!(cache.total_bytes(), 0);
    }
}
