// cspell:ignore NOENT, RDONLY, RGRP, RUSR, TRUNC, WGRP, WRONLY, WUSR
// cspell:ignore mikan, fstat, openat, renameat, unlinkat

use crate::ContractCode;
use crate::errors::ContractPrecompilatonResult;
use crate::logic::Config;
use crate::logic::errors::{CacheError, CompilationError};
use crate::runner::VMKindExt;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ProtocolVersion;
use parking_lot::Mutex;
#[cfg(not(windows))]
use rand::Rng as _;
#[cfg(not(windows))]
use rustix::fd::OwnedFd;
#[cfg(not(windows))]
use rustix::fs::{
    Access, AtFlags, Dir, Mode, OFlags, Timespec, Timestamps, UTIME_NOW, UTIME_OMIT, accessat,
    fstat, open, openat, renameat, statat, unlinkat, utimensat,
};
#[cfg(not(windows))]
use rustix::io::Errno;
use std::any::Any;
use std::collections::HashMap;
use std::fmt;
#[cfg(not(windows))]
use std::io::{Read, Write};
use std::num::NonZeroUsize;
#[cfg(not(windows))]
use std::str::FromStr;
use std::sync::Arc;
#[cfg(not(windows))]
use std::time::{Duration, Instant};

#[cfg(feature = "wasmtime_vm")]
// FIXME(ProtocolSchema): this isn't really part of the protocol schema??
#[derive(Debug, Clone, BorshSerialize, near_schema_checker_lib::ProtocolSchema)]
enum ContractCacheKey {
    _Version1,
    _Version2,
    _Version3,
    _Version4,
    Version5 {
        code_hash: CryptoHash,
        vm_config_non_crypto_hash: u64,
        vm_kind: near_parameters::vm::VMKind,
        vm_hash: u64,
    },
}

#[cfg(feature = "wasmtime_vm")]
pub(crate) fn get_contract_cache_key(
    code_hash: CryptoHash,
    config: &Config,
    vm_hash: u64,
) -> CryptoHash {
    let key = ContractCacheKey::Version5 {
        code_hash,
        vm_config_non_crypto_hash: config.non_crypto_hash(),
        vm_kind: config.vm_kind,
        vm_hash,
    };
    CryptoHash::hash_borsh(key)
}

/// Cache-key signature for `config`, independent of any specific contract.
/// Hashes the same inputs as [`get_contract_cache_key`] minus `code_hash`,
/// so two signatures compare equal iff any given contract would land under
/// the same on-disk cache key under either config.
///
/// Use this to compare two configs without enumerating cache-key inputs by
/// hand — adding a new field to [`ContractCacheKey`] flows through here
/// automatically.
#[cfg(feature = "wasmtime_vm")]
pub fn config_cache_key_signature(config: Arc<Config>) -> CryptoHash {
    let vm_kind = config.vm_kind;
    let runtime = vm_kind
        .runtime(Arc::clone(&config))
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));
    get_contract_cache_key(CryptoHash::default(), &config, runtime.vm_hash())
}

#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum CompiledContract {
    CompileModuleError(crate::logic::errors::CompilationError) = 0,
    Code(Vec<u8>) = 1,
}

impl CompiledContract {
    /// Return the length of the compiled contract data.
    ///
    /// If the `CompiledContract` represents a compilation failure, returns `0`.
    pub fn debug_len(&self) -> usize {
        match self {
            CompiledContract::CompileModuleError(_) => 0,
            CompiledContract::Code(c) => c.len(),
        }
    }
}

/// Contains result of contract compilation with auxiliary data
#[derive(Debug, Clone, PartialEq, BorshDeserialize, BorshSerialize)]
pub struct CompiledContractInfo {
    pub wasm_bytes: u64,
    pub compiled: CompiledContract,
}

impl CompiledContractInfo {
    /// size [bytes] of the source wasm module
    pub fn wasm_size(&self) -> u64 {
        self.wasm_bytes
    }

    /// Size [bytes] of the compiled module.
    ///
    /// In case of compilation error, returns a heuristic minimum weight for the
    /// error entry in the cache, rather than the raw `CompilationError` struct
    /// size, which would underestimate heap allocations (e.g. error messages).
    pub fn compiled_size(&self) -> u64 {
        match &self.compiled {
            CompiledContract::CompileModuleError(err) => err.size_bytes_approximate() as u64,
            CompiledContract::Code(code) => code.len() as u64,
        }
    }
}

/// Cache for compiled modules
pub trait ContractRuntimeCache: Send + Sync {
    fn handle(&self) -> Box<dyn ContractRuntimeCache>;
    fn memory_cache(&self) -> &AnyCache {
        // This method returns a reference, so we need to store an instance somewhere.
        static ZERO_ANY_CACHE: std::sync::LazyLock<AnyCache> =
            std::sync::LazyLock::new(|| AnyCache::new(0, 0));
        &ZERO_ANY_CACHE
    }
    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()>;
    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>>;
    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        self.get(key).map(|entry| entry.is_some())
    }

    /// Things to be performed on protocol version change. The default is a no-op.
    fn on_protocol_version_update(&self, _new_protocol_version: ProtocolVersion) {}

    /// Notify the cache that `key` has been touched. The default is a no-op.
    fn touch(&self, _key: &CryptoHash) {}

    /// TESTING ONLY: Clears the cache including in-memory and persistent data (if any).
    ///
    /// This should be used only for testing, since the implementations may not provide
    /// a consistent view when the cache is both cleared and accessed as the same time.
    ///
    /// Default implementation panics; the implementations for which this method is called
    /// should provide a proper implementation.
    #[cfg(feature = "test_features")]
    fn test_only_clear(&self) -> std::io::Result<()> {
        unimplemented!("test_only_clear is not implemented for this cache");
    }
}

impl fmt::Debug for dyn ContractRuntimeCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Compiled contracts cache")
    }
}

impl ContractRuntimeCache for Box<dyn ContractRuntimeCache> {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        <dyn ContractRuntimeCache>::handle(&**self)
    }

    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        <dyn ContractRuntimeCache>::put(&**self, key, value)
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        <dyn ContractRuntimeCache>::get(&**self, key)
    }

    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        <dyn ContractRuntimeCache>::has(&**self, key)
    }

    fn on_protocol_version_update(&self, new_protocol_version: ProtocolVersion) {
        <dyn ContractRuntimeCache>::on_protocol_version_update(&**self, new_protocol_version)
    }

    fn touch(&self, key: &CryptoHash) {
        <dyn ContractRuntimeCache>::touch(&**self, key)
    }
}

impl<C: ContractRuntimeCache> ContractRuntimeCache for &C {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        <C as ContractRuntimeCache>::handle(self)
    }

    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        <C as ContractRuntimeCache>::put(self, key, value)
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        <C as ContractRuntimeCache>::get(self, key)
    }

    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        <C as ContractRuntimeCache>::has(self, key)
    }

    fn on_protocol_version_update(&self, new_protocol_version: ProtocolVersion) {
        <C as ContractRuntimeCache>::on_protocol_version_update(self, new_protocol_version)
    }

    fn touch(&self, key: &CryptoHash) {
        <C as ContractRuntimeCache>::touch(self, key)
    }
}

#[derive(Default, Clone)]
pub struct NoContractRuntimeCache;

impl ContractRuntimeCache for NoContractRuntimeCache {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        Box::new(self.clone())
    }

    fn put(&self, _: &CryptoHash, _: CompiledContractInfo) -> std::io::Result<()> {
        Ok(())
    }

    fn get(&self, _: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        Ok(None)
    }
}

#[derive(Default, Clone)]
pub struct MockContractRuntimeCache {
    store: Arc<Mutex<HashMap<CryptoHash, CompiledContractInfo>>>,
    put_count: Arc<std::sync::atomic::AtomicU32>,
}

impl MockContractRuntimeCache {
    pub fn len(&self) -> usize {
        self.store.lock().len()
    }

    pub fn put_count(&self) -> u32 {
        self.put_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

impl ContractRuntimeCache for MockContractRuntimeCache {
    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        self.put_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.store.lock().insert(*key, value);
        Ok(())
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        Ok(self.store.lock().get(key).map(Clone::clone))
    }

    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        Box::new(self.clone())
    }
}

impl fmt::Debug for MockContractRuntimeCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.store.lock();
        let hm: &HashMap<_, _> = &*guard;
        fmt::Debug::fmt(hm, f)
    }
}

/// A cache that stores precompiled contract executables in a directory of a filesystem.
///
/// This directory can optionally be a temporary directory. If created with [`Self::test`] the
/// directory will be removed when the last instance of this cache is dropped.
///
/// Clones of this type share the same underlying state and information. The cache is thread safe
/// and atomic.
///
/// This cache implements a size-bounded, best-effort on-disk eviction policy.
/// Files are tracked by key and on-disk byte size; LRU victims are unlinked on `put`.
/// (The in-memory cache is separate and VM-specific.)
/// Sink for fire-and-forget background maintenance jobs.
pub type BackgroundJobSpawner = Arc<dyn Fn(Box<dyn FnOnce() + Send>) + Send + Sync>;

/// A [`BackgroundJobSpawner`] that sends every job to the void.
pub fn noop_background_spawner() -> BackgroundJobSpawner {
    Arc::new(|_job| {})
}

#[cfg(not(windows))]
#[derive(Clone)]
pub struct FilesystemContractRuntimeCache {
    state: Arc<FilesystemContractRuntimeCacheState>,
}

#[cfg(not(windows))]
struct FilesystemContractRuntimeCacheState {
    /// file descriptor to the cache directory
    dir: OwnedFd,
    any_cache: AnyCache,
    /// Tracks files present in `dir`, keyed by the same `CryptoHash` as the
    /// on-disk filename, weighted by on-disk byte size. The value is the
    /// instant the entry's on-disk atime was last refreshed/created.
    disk_index: Mutex<LruWeightedCache<CryptoHash, Instant>>,
    /// Minimum age of an entry's last atime on-disk refresh before `touch` enqueues another.
    /// Mirrors `relatime` behavior.
    access_time_refresh_throttle: Duration,
    /// Off-loads the on-disk atime refresh; see [`BackgroundJobSpawner`].
    bg_spawner: BackgroundJobSpawner,
    test_temp_dir: Option<tempfile::TempDir>,
}

/// Default minimum age of a tracked entry's last atime refresh before [`touch`] will enqueue another one.
///
/// [`touch`]: FilesystemContractRuntimeCache::touch
#[cfg(not(windows))]
const ACCESS_TIME_REFRESH_THROTTLE: Duration = Duration::from_mins(10);

#[cfg(not(windows))]
impl FilesystemContractRuntimeCache {
    /// Largest accepted value for the on-disk cache size limit
    pub const MAX_DISK_CACHE_BYTES: u64 = LruWeightedCache::<CryptoHash, ()>::MAX_WEIGHT;

    /// `max_disk_cache_bytes` is the maximum total size of compiled-contract files kept on disk
    pub fn new<StorePath, ContractCachePath>(
        home_dir: &std::path::Path,
        store_path: Option<&StorePath>,
        contract_cache_path: &ContractCachePath,
        max_disk_cache_bytes: u64,
    ) -> std::io::Result<Self>
    where
        StorePath: AsRef<std::path::Path> + ?Sized,
        ContractCachePath: AsRef<std::path::Path> + ?Sized,
    {
        Self::with_memory_cache(
            home_dir,
            store_path,
            contract_cache_path,
            0,
            None,
            max_disk_cache_bytes,
            noop_background_spawner(),
        )
    }

    /// When setting up a cache of compiled contracts, also set-up a `size` element in-memory
    /// cache.
    ///
    /// This additional cache is usually used to store loaded artifacts, but data stored can really
    /// be anything and depends on the specific VM kind.
    ///
    /// Note though, that this memory cache is *not* used to additionally cache files from the
    /// filesystem – OS page cache already does that for us transparently.
    ///
    /// `max_disk_cache_bytes` maximum total size of compiled-contract files kept on disk
    pub fn with_memory_cache<StorePath, ContractCachePath>(
        home_dir: &std::path::Path,
        store_path: Option<&StorePath>,
        contract_cache_path: &ContractCachePath,
        memcache_expected_item_count: usize,
        memcache_metrics_identifier: Option<String>,
        max_disk_cache_bytes: u64,
        bg_spawner: BackgroundJobSpawner,
    ) -> std::io::Result<Self>
    where
        StorePath: AsRef<std::path::Path> + ?Sized,
        ContractCachePath: AsRef<std::path::Path> + ?Sized,
    {
        if max_disk_cache_bytes > Self::MAX_DISK_CACHE_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "contract_cache_max_size must be at most {} bytes",
                    Self::MAX_DISK_CACHE_BYTES
                ),
            ));
        }
        let store_path = store_path.map(AsRef::as_ref).unwrap_or_else(|| "data".as_ref());
        let legacy_path: std::path::PathBuf =
            [home_dir, store_path, "contracts".as_ref()].into_iter().collect();
        let path: std::path::PathBuf =
            [home_dir, contract_cache_path.as_ref()].into_iter().collect();
        // Rename the old contracts directory to a new name. This should only succeed the first
        // time this code encounters the legacy contract directory. If this fails the first time
        // for some reason, a new directory will be created for the new cache anyway, and future
        // launches won't be able to overwrite it anymore. This is also fine.
        let _ = std::fs::rename(&legacy_path, &path);
        if std::fs::exists(legacy_path).ok() == Some(true) {
            tracing::warn!(
                target: "vm",
                path = %path.display(),
                message = "the legacy compiled contract cache path still exists after migration; consider removing it"
            );
        }
        std::fs::create_dir_all(&path)?;
        let dir = open(&path, OFlags::DIRECTORY, Mode::empty())?;
        tracing::debug!(
            target: "vm",
            path = %path.display(),
            message = "opened a contract executable cache directory"
        );

        // Contract weight multiplier to map the user-provided max items cap to the memory
        // requirements. Estimated from looking at `data/contract_cache` directory. Results in a
        // reasonable 4GB max cache memory footprint for the default value of 256 items cap.
        const AVG_COMPILED_CONTRACT_WEIGHT: u64 = 16 * 1024 * 1024; // 16 MiB
        // x4 to accommodate for a long tail of smaller contracts / compilation errors and not hit
        // items cap too often as it is cache memory footprint which is really important to limit.
        // The constant is chosen somewhat arbitrarily.
        let expected_cache_item_count = memcache_expected_item_count * 4;

        let any_cache = AnyCache::new(
            expected_cache_item_count,
            memcache_expected_item_count as u64 * AVG_COMPILED_CONTRACT_WEIGHT,
        );
        #[cfg(feature = "metrics")]
        let any_cache = if let Some(id) = memcache_metrics_identifier {
            any_cache.with_metrics_identifier(id)
        } else {
            any_cache
        };
        #[cfg(not(feature = "metrics"))]
        debug_assert!(
            memcache_metrics_identifier.is_none(),
            "memcache_metrics_identifier is only supported with the `metrics` feature"
        );

        let disk_index = Mutex::new(build_disk_index(&dir, max_disk_cache_bytes)?);

        Ok(Self {
            state: Arc::new(FilesystemContractRuntimeCacheState {
                dir,
                any_cache,
                disk_index,
                access_time_refresh_throttle: ACCESS_TIME_REFRESH_THROTTLE,
                bg_spawner,
                test_temp_dir: None,
            }),
        })
    }

    #[cfg(test)]
    fn test_set_background_job_spawner(&mut self, spawner: BackgroundJobSpawner) {
        Arc::get_mut(&mut self.state)
            .expect("background job spawner must be set before the cache is shared")
            .bg_spawner = spawner;
    }

    /// TEST ONLY: override the atime-refresh throttle window. Setting it to
    /// [`Duration::ZERO`] makes every tracked key eligible on the next
    /// [`Self::touch`] — deterministic, with no `Instant` arithmetic that could
    /// underflow. Must be called before the cache is shared.
    #[cfg(test)]
    fn test_set_access_time_refresh_throttle(&mut self, throttle: Duration) {
        Arc::get_mut(&mut self.state)
            .expect("throttle must be set before the cache is shared")
            .access_time_refresh_throttle = throttle;
    }

    pub fn test() -> std::io::Result<Self> {
        // Tests that don't exercise eviction want no effective limit.
        Self::test_with_disk_cache_bytes(Self::MAX_DISK_CACHE_BYTES)
    }

    /// Like [`Self::test`], but with the on-disk eviction limit set explicitly.
    /// Tests for the eviction feature use this; everything else stays on
    /// [`Self::test`].
    pub fn test_with_disk_cache_bytes(max_disk_cache_bytes: u64) -> std::io::Result<Self> {
        let tempdir = tempfile::TempDir::new()?;
        let mut cache =
            Self::new(tempdir.path(), None::<&str>, "contract.cache", max_disk_cache_bytes)?;
        Arc::get_mut(&mut cache.state).unwrap().test_temp_dir = Some(tempdir);
        Ok(cache)
    }

    /// Stamp `key`'s on-disk file with the current access time, leaving its
    /// modification time untouched. Best-effort: failures are logged and dropped.
    fn refresh_disk_atime(&self, key: &CryptoHash) {
        let filename = key.to_string();
        let times = Timestamps {
            last_access: Timespec { tv_sec: 0, tv_nsec: UTIME_NOW },
            last_modification: Timespec { tv_sec: 0, tv_nsec: UTIME_OMIT },
        };
        match utimensat(&self.state.dir, &filename, &times, AtFlags::empty()) {
            // Success, or the file was evicted/removed out from under us.
            Ok(()) | Err(Errno::NOENT) => {}
            Err(err) => tracing::debug!(
                target: "vm",
                key = %key,
                err = &err as &dyn std::error::Error,
                "failed to refresh contract cache file atime",
            ),
        }
    }
}

/// Byte added after a serialized payload representing a compilation failure.
///
/// This is ASCII LF.
#[cfg(not(windows))]
const ERROR_TAG: u8 = 0b00001010;
/// Byte added after a serialized payload representing the contract code.
///
/// Value is fairly arbitrarily chosen such that a couple of bit flips do not make this an
/// [`ERROR_TAG`].
#[cfg(not(windows))]
const CODE_TAG: u8 = 0b10010101;

/// Total bytes [`FilesystemContractRuntimeCache::put`] writes for `value`,
/// including the trailing tag and the `wasm_bytes` length suffix. Used to
/// weight the on-disk LRU index.
#[cfg(not(windows))]
fn entry_disk_size(value: &CompiledContractInfo) -> u64 {
    // Trailer `put` appends after the payload: one tag byte ([`CODE_TAG`] or
    // [`ERROR_TAG`]) plus 8 bytes of little-endian `wasm_bytes`.
    const PUT_TRAILER_BYTES: u64 = 1 + 8;
    let payload_bytes = match &value.compiled {
        CompiledContract::Code(code) => code.len() as u64,
        CompiledContract::CompileModuleError(err) => borsh::object_length(err)
            .expect("CompilationError serialization length should be infallible")
            as u64,
    };
    payload_bytes + PUT_TRAILER_BYTES
}

/// Scan `dir` and build an [`LruWeightedCache`] tracking each on-disk
/// cache file by its byte size, ordered from least- to most-recently-
/// accessed by `st_atime`.
///
/// Files whose names don't parse as a [`CryptoHash`] are skipped.
/// If the scanned valid total already exceeds `max_bytes`, the oldest
/// tracked entries are unlinked until the index fits.
#[cfg(not(windows))]
fn build_disk_index(
    dir: &rustix::fd::OwnedFd,
    max_bytes: u64,
) -> std::io::Result<LruWeightedCache<CryptoHash, Instant>> {
    let mut index = LruWeightedCache::<CryptoHash, Instant>::without_item_cap(max_bytes);
    let mut entries: Vec<(CryptoHash, u64, i64, i64)> = Vec::new();

    let read_dir = Dir::read_from(dir)?;
    for entry in read_dir {
        let entry = match entry {
            Ok(e) => e,
            Err(_) => continue,
        };
        let filename = entry.file_name();
        if !entry.file_type().is_file() {
            continue;
        }
        let Some(key) = filename.to_str().ok().and_then(|s| CryptoHash::from_str(s).ok()) else {
            continue;
        };
        let stat = match statat(dir, filename, AtFlags::empty()) {
            Ok(s) => s,
            // Missing/unreadable: leave it alone for now; `get` will treat
            // it as a miss and the next compile will overwrite it.
            Err(_) => continue,
        };
        let size = u64::try_from(stat.st_size).unwrap_or(0);
        entries.push((key, size, stat.st_atime as i64, stat.st_atime_nsec as i64));
    }

    // Ascending by atime: most-recently-accessed ends up MRU.
    entries.sort_unstable_by_key(|(_, _, sec, nsec)| (*sec, *nsec));

    // Sizes by key, so we can total the bytes of any entries we trim below
    // (`insert` only hands back the evicted key, not its weight).
    let sizes: HashMap<CryptoHash, u64> =
        entries.iter().map(|&(key, size, _, _)| (key, size)).collect();

    let refreshed_at = Instant::now();
    let mut over_limit_trimmed: usize = 0;
    let mut trimmed_bytes: u64 = 0;
    for (key, size, _, _) in entries {
        for (victim, _) in index.insert(key, size, refreshed_at) {
            trimmed_bytes += sizes.get(&victim).copied().unwrap_or(0);
            match unlinkat(dir, victim.to_string(), AtFlags::empty()) {
                Ok(()) | Err(Errno::NOENT) => {}
                Err(err) => tracing::debug!(
                    target: "vm",
                    victim = %victim,
                    err = &err as &dyn std::error::Error,
                    "failed to unlink evicted compiled-contract cache file; on-disk cache may exceed its size limit"
                ),
            }
            over_limit_trimmed += 1;
        }
    }

    if 0 < over_limit_trimmed {
        tracing::info!(
            target: "vm",
            trimmed = over_limit_trimmed,
            trimmed_bytes,
            max_bytes,
            "compiled-contract cache exceeded its on-disk size limit at startup; trimmed oldest entries"
        );
    }

    Ok(index)
}

/// Cache for compiled contracts code in plain filesystem.
#[cfg(not(windows))]
impl ContractRuntimeCache for FilesystemContractRuntimeCache {
    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        Box::new(self.clone())
    }

    fn memory_cache(&self) -> &AnyCache {
        &self.state.any_cache
    }

    #[tracing::instrument(
        level = "trace",
        target = "vm",
        "FilesystemContractRuntimeCache::put",
        skip_all,
        fields(key = key.to_string(), value.len = value.compiled.debug_len()),
    )]
    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        let weight = entry_disk_size(&value);

        const MAX_ATTEMPTS: u32 = 5;
        let final_filename = key.to_string();
        let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
        let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
        let mut attempt = 0;
        let (temp_filename, mut file) = loop {
            attempt += 1;
            let mut temporary_filename = final_filename.clone();
            temporary_filename.push('.');
            for b in rand::thread_rng().sample_iter(rand::distributions::Alphanumeric).take(8) {
                temporary_filename.push(b as char);
            }
            temporary_filename.push_str(".temp");
            match openat(&self.state.dir, &temporary_filename, flags, mode) {
                Ok(f) => break (temporary_filename, std::fs::File::from(f)),
                Err(e) if attempt > MAX_ATTEMPTS => return Err(e.into()),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e.into()),
            }
        };

        // This section manually "serializes" the data. The cache is quite sensitive to
        // unnecessary overheads and in order to enable things like mmap-based file access, we want
        // to have full control of what has been written.
        match value.compiled {
            CompiledContract::CompileModuleError(e) => {
                borsh::to_writer(&mut file, &e)?;
                file.write_all(&[ERROR_TAG])?;
            }
            CompiledContract::Code(bytes) => {
                file.write_all(&bytes)?;
                // Writing the tag at the end gives us well aligned buffer of the data above which
                // is necessary for 0-copy deserialization later on.
                file.write_all(&[CODE_TAG])?;
            }
        }
        file.write_all(&value.wasm_bytes.to_le_bytes())?;
        file.sync_data()?;
        drop(file);
        // Rename, index update, and victim unlinks share one lock, so a
        // concurrent `put` of a victim key can't rename a fresh file into place
        // before our unlink and have us delete it. The write above is outside
        // the lock.
        {
            let mut index = self.state.disk_index.lock();
            renameat(&self.state.dir, temp_filename, &self.state.dir, final_filename)?;
            // We just wrote the file, so its atime is current as of now.
            let evicted = index.insert(*key, weight, Instant::now());
            // `insert` returns `key` for a same-key replacement (keep our file)
            // and for an oversized reject (remove it); `contains` tells them apart.
            let key_stored = index.contains(key);
            for (victim, _) in evicted {
                if &victim == key && key_stored {
                    continue;
                }
                match unlinkat(&self.state.dir, victim.to_string(), AtFlags::empty()) {
                    Ok(()) | Err(Errno::NOENT) => {}
                    Err(err) => tracing::debug!(
                        target: "vm",
                        victim = %victim,
                        err = &err as &dyn std::error::Error,
                        "failed to unlink evicted compiled-contract cache file; on-disk cache may exceed its size limit"
                    ),
                }
            }
        }

        // NOTE: we do not remove the temporary file in case of failure in many of the
        // intermediate steps above. This is not considered to be a significant risk: any failure
        // here will result in the node terminating anyway, so the operator will have to fix the
        // issue(s) before too many temporary files gather up in the cache.
        //
        // (Operators are also somewhat encouraged to occasionally clear up their cache.)
        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        target = "vm",
        "FilesystemContractRuntimeCache::get",
        skip_all,
        fields(key = key.to_string()),
    )]
    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        let filename = key.to_string();
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file = openat(&self.state.dir, &filename, flags, mode);
        let file = match file {
            Err(Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(file) => file,
        };
        let stat = fstat(&file)?;
        // TODO: explore mmap-ing the file and lending the map to the caller via a closure callback.
        // This would require some additional refactor work, but would likely help us to reduce the
        // system call overhead in this area.
        let mut buffer = Vec::with_capacity(stat.st_size.try_into().unwrap());
        let mut file = std::fs::File::from(file);
        file.read_to_end(&mut buffer)?;
        if buffer.len() < 9 {
            // The file turns out to be empty/truncated? Treat as if there's no cached file.
            return Ok(None);
        }
        let wasm_bytes = u64::from_le_bytes(buffer[buffer.len() - 8..].try_into().unwrap());
        let tag = buffer[buffer.len() - 9];
        buffer.truncate(buffer.len() - 9);
        let value = match tag {
            CODE_TAG => {
                CompiledContractInfo { wasm_bytes, compiled: CompiledContract::Code(buffer) }
            }
            ERROR_TAG => CompiledContractInfo {
                wasm_bytes,
                compiled: CompiledContract::CompileModuleError(borsh::from_slice(&buffer)?),
            },
            // File is malformed? For this code, since we're talking about a cache lets just treat
            // it as if there is no cached file as well. The cached file may eventually be
            // overwritten with a valid copy. And since we can compile a new copy, there doesn't
            // seem to be much reason to possibly crash the node due to this.
            _ => {
                tracing::debug!(
                    target: "vm",
                    message = "cached contract executable was found to be malformed",
                    key = %key
                );
                return Ok(None);
            }
        };
        // Real cache hit: refresh recency so eviction doesn't drop it next.
        self.touch(key);
        Ok(Some(value))
    }

    fn has(&self, key: &CryptoHash) -> std::io::Result<bool> {
        let filename = key.to_string();
        match accessat(&self.state.dir, &filename, Access::EXISTS, AtFlags::empty()) {
            Ok(()) => Ok(true),
            Err(Errno::NOENT) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    fn touch(&self, key: &CryptoHash) {
        // Promote in the in-memory LRU and check if on-disk atime refresh is needed.
        let needs_refresh = {
            let mut index = self.state.disk_index.lock();
            match index.get_mut(key) {
                Some((_weight, last_refresh)) => {
                    let now = Instant::now();
                    if now >= *last_refresh + self.state.access_time_refresh_throttle {
                        *last_refresh = now;
                        true
                    } else {
                        false
                    }
                }
                None => false,
            }
        };
        if needs_refresh {
            let cache = self.clone();
            let key = *key;
            (self.state.bg_spawner)(Box::new(move || cache.refresh_disk_atime(&key)));
        }
    }

    /// Clears the in-memory cache, the on-disk LRU index, and the files in the
    /// cache directory.
    ///
    /// The cache must be created using `test` method, otherwise this method will panic.
    #[cfg(feature = "test_features")]
    fn test_only_clear(&self) -> std::io::Result<()> {
        let Some(_temp_dir) = &self.state.test_temp_dir else {
            panic!("must be called for testing only");
        };
        self.memory_cache().clear();
        // Drop the index along with the files it mirrors.
        self.state.disk_index.lock().clear();
        for entry in Dir::read_from(&self.state.dir).unwrap() {
            if let Ok(entry) = entry {
                let filename_bytes = entry.file_name().to_bytes();
                if filename_bytes == b"." || filename_bytes == b".." {
                    continue;
                } else if !entry.file_type().is_file() {
                    debug_assert!(
                        false,
                        "contract code cache should only contain file items, but found {:?}",
                        entry.file_name()
                    );
                } else {
                    if let Err(err) = unlinkat(&self.state.dir, entry.file_name(), AtFlags::empty())
                    {
                        tracing::error!(
                            file_name = ?entry.file_name(),
                            err = &err as &dyn std::error::Error,
                            "failed to remove contract cache file",
                        );
                    }
                }
            }
        }
        Ok(())
    }
}

type AnyCacheValue = dyn Any + Send;

/// LRU cache with weight-based eviction policy.
struct LruWeightedCache<K, V> {
    current_weight: u64,
    max_weight: u64,
    cache: lru::LruCache<K, LruWeightedCacheEntry<V>>,
}

type LruWeightedCacheEntry<V> = (u64, V);

impl<K: std::hash::Hash + Eq, V> LruWeightedCache<K, V> {
    /// Largest accepted `max_weight`. Capped one below `u64::MAX / 2` so the
    /// transient `current_weight + weight` sum in `insert` can never overflow.
    const MAX_WEIGHT: u64 = u64::MAX / 2 - 1;

    fn new(item_capacity: NonZeroUsize, max_weight: u64) -> Self {
        Self::with_lru(max_weight, lru::LruCache::new(item_capacity))
    }

    /// Like [`Self::new`], but with no item-count cap — eviction is driven
    /// purely by `max_weight`.
    #[cfg_attr(windows, allow(dead_code))]
    fn without_item_cap(max_weight: u64) -> Self {
        Self::with_lru(max_weight, lru::LruCache::unbounded())
    }

    fn with_lru(max_weight: u64, cache: lru::LruCache<K, LruWeightedCacheEntry<V>>) -> Self {
        assert!(
            max_weight <= Self::MAX_WEIGHT,
            "max_weight must be at most {} to avoid overflows",
            Self::MAX_WEIGHT
        );
        Self { current_weight: 0, max_weight, cache }
    }

    /// Like [`Self::get`], but yields a mutable reference to the stored value.
    /// Also promotes `key` to most-recently-used.
    #[cfg_attr(windows, allow(dead_code))]
    fn get_mut(&mut self, key: &K) -> Option<&mut (u64, V)> {
        self.cache.get_mut(key)
    }

    fn get(&mut self, key: &K) -> Option<&(u64, V)> {
        self.cache.get(key)
    }

    /// Insert `key` with `weight` and `value` as MRU. Returns the entries
    /// that were evicted as a result, in eviction order.
    fn insert(&mut self, key: K, weight: u64, value: V) -> Vec<(K, V)> {
        // Too big to ever fit: don't store it. Also evict any existing entry
        // for this key so an oversized replacement can't leave a stale one.
        if self.max_weight < weight {
            if let Some((old_weight, _)) = self.cache.pop(&key) {
                self.current_weight -= old_weight;
            }
            return vec![(key, value)];
        }

        let mut evicted = Vec::new();
        // `push` (unlike `put`) returns any evicted entry, whether it was a
        // same-key replacement or the LRU entry dropped due to item_capacity.
        if let Some((evicted_key, (evicted_weight, evicted_value))) =
            self.cache.push(key, (weight, value))
        {
            self.current_weight -= evicted_weight;
            evicted.push((evicted_key, evicted_value));
        }
        self.current_weight += weight;

        while self.max_weight < self.current_weight {
            let (evicted_key, (evicted_weight, evicted_value)) = self
                .cache
                .pop_lru()
                .expect("current_weight >= max_weight implies cache is not empty");
            self.current_weight -= evicted_weight;
            evicted.push((evicted_key, evicted_value));
        }

        evicted
    }

    fn put(&mut self, key: K, weight: u64, value: V) {
        let _ = self.insert(key, weight, value);
    }

    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    fn len(&self) -> usize {
        self.cache.len()
    }

    #[cfg_attr(not(feature = "metrics"), allow(dead_code))]
    fn current_weight(&self) -> u64 {
        self.current_weight
    }

    fn clear(&mut self) {
        self.current_weight = 0;
        self.cache.clear();
    }

    fn contains(&self, key: &K) -> bool {
        self.cache.contains(key)
    }
}

/// Cache that can store instances of any type, keyed by a CryptoHash.
///
/// Used primarily for storage of artifacts on a per-VM basis.
pub struct AnyCache {
    cache: Option<Mutex<LruWeightedCache<CryptoHash, Box<AnyCacheValue>>>>,
    /// Optional identifier for this cache instance, used as a label when reporting metrics.
    /// Metrics are only reported when this is set.
    #[cfg(feature = "metrics")]
    identifier: Option<String>,
}

impl AnyCache {
    fn new(max_item_count: usize, max_cache_weight: u64) -> Self {
        Self {
            cache: if let Some(max_item_count) = NonZeroUsize::new(max_item_count) {
                Some(Mutex::new(LruWeightedCache::new(max_item_count, max_cache_weight)))
            } else {
                None
            },
            #[cfg(feature = "metrics")]
            identifier: None,
        }
    }

    #[cfg(feature = "metrics")]
    #[cfg_attr(windows, allow(dead_code))]
    fn with_metrics_identifier(mut self, identifier: String) -> Self {
        self.identifier = Some(identifier);
        self
    }

    pub fn clear(&self) {
        if let Some(cache) = &self.cache {
            let mut cache_guard = cache.lock();
            cache_guard.clear();
            #[cfg(feature = "metrics")]
            if let Some(id) = &self.identifier {
                crate::metrics::set_compiled_contract_cache_metrics(id, 0, 0);
            }
        }
    }

    /// Lookup the key in the cache, generating a new element if absent.
    ///
    /// This function accepts two callbacks as an argument: first is a fallible generation
    /// function which may generate a new value for the cache if there isn't one at the specified
    /// key; and the second to act on the value that has been found (or generated and placed in the
    /// cache.)
    ///
    /// If the `generate` fails to generate a value, the failure will not be cached, but rather
    /// returned to the caller of `try_lookup`. The second callback is not called either, as there
    /// is no value to call it with.
    ///
    /// # Examples
    ///
    /// ```
    /// use near_primitives_core::hash::CryptoHash;
    /// use near_vm_runner::{ContractRuntimeCache, NoContractRuntimeCache};
    /// use std::path::Path;
    ///
    /// let cache = NoContractRuntimeCache;
    /// let result = cache.memory_cache().try_lookup(CryptoHash::hash_bytes(b"my_key"), || {
    ///     // The value is not in the cache, (re-)generate a new one by reading from the file
    ///     // system.
    ///     match std::fs::read("/this/path/does/not/exist/") {
    ///         Err(e) => Err(e),
    ///         Ok(bytes) => Ok((bytes.len() as u64, Box::new(bytes))) // : Result<(u64, Box<dyn std::any::Any + Send>), std::io::Error>
    ///     }
    ///     // If the function above succeeds (returns `Ok`), `Vec<u8>` will end up being stored in
    ///     // the cache.
    /// }, |value| {
    ///     // The value was found in the cache or been just generated successfully. It may not
    ///     // necessarily be a `Vec` however, since there could've been another call to the cache
    ///     // that populated this key with a value of a different type.
    ///     let value: &Vec<u8> = value.downcast_ref()?;
    ///     // If it turned out to be a Vec after all, clone and return it.
    ///     Some(Vec::clone(value))
    /// });
    /// // Since we were reading a path that does not exist, most likely outcome is for the
    /// // generation function to fail...
    /// assert!(result.is_err());
    /// // However if it was to succeed, the 2nd time this is called, the value would potentially
    /// // come from the cache.
    /// ```
    pub fn try_lookup<E, R>(
        &self,
        key: CryptoHash,
        generate: impl FnOnce() -> Result<(u64, Box<AnyCacheValue>), E>,
        with: impl FnOnce(&AnyCacheValue) -> R,
    ) -> Result<R, E> {
        let Some(cache) = &self.cache else {
            let (_, v) = generate()?;
            // NB: The stars and ampersands here are semantics-affecting. e.g. if the star is
            // missing, we end up making an object out of `Box<dyn ...>` rather than using `dyn
            // Any` within the box which is obviously quite wrong.
            return Ok(with(&*v));
        };
        {
            if let Some((_weight, cached_value)) = cache.lock().get(&key) {
                // Same here.
                return Ok(with(&**cached_value));
            }
        }
        let (weight, generated) = generate()?;
        let result = with(&*generated);
        let mut locked = cache.lock();
        locked.put(key, weight, generated);
        #[cfg(feature = "metrics")]
        if let Some(id) = &self.identifier {
            crate::metrics::set_compiled_contract_cache_metrics(
                id,
                locked.len(),
                locked.current_weight(),
            );
        }
        Ok(result)
    }

    /// Checks if the cache contains the key without modifying the cache.
    pub fn contains(&self, key: CryptoHash) -> bool {
        let Some(cache) = &self.cache else { return false };
        cache.lock().contains(&key)
    }
}

/// Precompiles contract for the current default VM, and stores result to the cache.
/// Returns `Ok(true)` if compiled code was added to the cache, and `Ok(false)` if element
/// is already in the cache, or if cache is `None`.
pub fn precompile_contract(
    code: &ContractCode,
    config: Arc<Config>,
    cache: Option<&dyn ContractRuntimeCache>,
) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError> {
    let _span = tracing::debug_span!(target: "vm", "precompile_contract").entered();
    let vm_kind = config.vm_kind;
    let runtime = vm_kind
        .runtime(Arc::clone(&config))
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));
    let cache = match cache {
        Some(it) => it,
        None => return Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable)),
    };
    runtime.precompile(code, cache)
}

/// Like [`precompile_contract`], but returns immediately if another thread is
/// already compiling the same contract. Intended for opportunistic background
/// warming on a low-priority pool: if a higher-priority worker (or any other
/// caller) is already running `compile_and_cache` for this contract's cache
/// key, this call reports `ContractAlreadyInCache` and lets the in-flight
/// compiler populate the cache, instead of blocking a warming worker thread
/// on the per-key compilation lock.
pub fn try_precompile_contract(
    code: &ContractCode,
    config: Arc<Config>,
    cache: Option<&dyn ContractRuntimeCache>,
) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError> {
    let _span = tracing::debug_span!(target: "vm", "try_precompile_contract").entered();
    let vm_kind = config.vm_kind;
    let runtime = vm_kind
        .runtime(Arc::clone(&config))
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));
    let cache = match cache {
        Some(it) => it,
        None => return Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable)),
    };
    runtime.try_precompile(code, cache)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn any_cache_empty() {
        struct TestType;
        let empty = AnyCache::new(0, 0);
        let key = CryptoHash::hash_bytes(b"empty");
        cov_mark::check!(any_cache_empty_generate);
        cov_mark::check!(any_cache_empty_with);
        let result = empty.try_lookup(
            key,
            || {
                cov_mark::hit!(any_cache_empty_generate);
                Ok::<_, ()>((0_u64, Box::new(TestType)))
            },
            |v| {
                cov_mark::hit!(any_cache_empty_with);
                assert!(v.is::<TestType>());
                "banana"
            },
        );
        assert!(matches!(result, Ok("banana")));
    }

    #[test]
    fn any_cache_sized() {
        const CACHE_ITEM_WEIGHT: u64 = 1;
        struct TestType;
        let empty = AnyCache::new(1, 2 * CACHE_ITEM_WEIGHT);
        let key = CryptoHash::hash_bytes(b"sized");
        cov_mark::check!(any_cache_sized_generate);
        cov_mark::check!(any_cache_sized_with);
        let result = empty.try_lookup(
            key,
            || {
                cov_mark::hit!(any_cache_sized_generate);
                Ok::<_, ()>((CACHE_ITEM_WEIGHT, Box::new(TestType)))
            },
            |v| {
                cov_mark::hit!(any_cache_sized_with);
                assert!(v.is::<TestType>());
                "apple" // please no sue
            },
        );
        assert!(matches!(result, Ok("apple")));

        cov_mark::check!(any_cache_sized_with2);
        let result = empty.try_lookup(
            key,
            || unreachable!(),
            |v| {
                cov_mark::hit!(any_cache_sized_with2);
                assert!(v.is::<TestType>());
                "pistachio" // TIL: is also a fruit.
            },
        );
        assert!(matches!(result, Ok::<_, ()>("pistachio")));
    }

    #[test]
    fn any_cache_item_cap_eviction() {
        struct TestType(u32);
        let cache = AnyCache::new(2, 100);
        let key1 = CryptoHash::hash_bytes(b"item1");
        let key2 = CryptoHash::hash_bytes(b"item2");
        let key3 = CryptoHash::hash_bytes(b"item3");

        // Insert first item
        let result1 = cache.try_lookup(
            key1,
            || Ok::<_, ()>((0, Box::new(TestType(1)))),
            |v| v.downcast_ref::<TestType>().unwrap().0,
        );
        assert_eq!(result1.unwrap(), 1);
        assert!(cache.contains(key1));

        // Insert second item
        let result2 = cache.try_lookup(
            key2,
            || Ok::<_, ()>((0, Box::new(TestType(2)))),
            |v| v.downcast_ref::<TestType>().unwrap().0,
        );
        assert_eq!(result2.unwrap(), 2);
        assert!(cache.contains(key1));
        assert!(cache.contains(key2));

        // Insert third item - this should trigger eviction of the least recently used item (key1)
        let result3 = cache.try_lookup(
            key3,
            || Ok::<_, ()>((0, Box::new(TestType(3)))),
            |v| v.downcast_ref::<TestType>().unwrap().0,
        );
        assert_eq!(result3.unwrap(), 3);
        assert!(!cache.contains(key1), "Least recently used item should have been evicted");
        assert!(cache.contains(key2));
        assert!(cache.contains(key3));
    }

    #[test]
    fn any_cache_weight_eviction() {
        const ITEM_WEIGHT: u64 = 100;
        const MAX_CACHE_WEIGHT: u64 = 250; // Can fit 2 items comfortably

        struct TestType(u32);

        // Create cache that can hold ~2 items based on weight
        let cache = AnyCache::new(10, MAX_CACHE_WEIGHT);

        let key1 = CryptoHash::hash_bytes(b"item1");
        let key2 = CryptoHash::hash_bytes(b"item2");
        let key3 = CryptoHash::hash_bytes(b"item3");

        // Insert first item
        let result1 = cache.try_lookup(
            key1,
            || Ok::<_, ()>((ITEM_WEIGHT, Box::new(TestType(1)))),
            |v| v.downcast_ref::<TestType>().unwrap().0,
        );
        assert_eq!(result1.unwrap(), 1);
        assert!(cache.contains(key1));

        // Insert second item
        let result2 = cache.try_lookup(
            key2,
            || Ok::<_, ()>((ITEM_WEIGHT, Box::new(TestType(2)))),
            |v| v.downcast_ref::<TestType>().unwrap().0,
        );
        assert_eq!(result2.unwrap(), 2);
        assert!(cache.contains(key1));
        assert!(cache.contains(key2));

        // Insert third item - this should trigger eviction of older items
        // since 3 * ITEM_WEIGHT (300) > MAX_CACHE_WEIGHT (250)
        let result3 = cache.try_lookup(
            key3,
            || Ok::<_, ()>((ITEM_WEIGHT, Box::new(TestType(3)))),
            |v| v.downcast_ref::<TestType>().unwrap().0,
        );
        assert_eq!(result3.unwrap(), 3);
        assert!(cache.contains(key3));

        // Verify that at least one of the earlier items was evicted
        let some_evicted = !cache.contains(key1) || !cache.contains(key2);
        assert!(
            some_evicted,
            "Cache should have evicted at least one item when exceeding weight limit"
        );
    }

    #[test]
    fn any_cache_errors() {
        let empty = AnyCache::new(0, 0);
        let key = CryptoHash::hash_bytes(b"errors");
        cov_mark::check!(any_cache_errors_generate);
        let result = empty.try_lookup(
            key,
            || {
                cov_mark::hit!(any_cache_errors_generate);
                Err("peach")
            },
            |_| unreachable!(),
        );
        assert!(matches!(result, Err("peach")));
        // Doing it again should not cache the error...
        cov_mark::check!(any_cache_errors_generate_two);
        let result = empty.try_lookup(
            key,
            || {
                cov_mark::hit!(any_cache_errors_generate_two);
                Err("mikan")
            },
            |_| unreachable!(),
        );
        assert!(matches!(result, Err("mikan")));
    }

    // example of why we might want to use the weight-aware eviction
    #[test]
    fn any_cache_weight_based_eviction() {
        const MAX_CACHE_WEIGHT: u64 = 19;

        struct TestType(u32);

        let cache = AnyCache::new(10, MAX_CACHE_WEIGHT);

        let key1 = CryptoHash::hash_bytes(b"weight1");
        let key2 = CryptoHash::hash_bytes(b"weight3");
        let key3 = CryptoHash::hash_bytes(b"weight5");
        let key4 = CryptoHash::hash_bytes(b"weight10a");
        let key5 = CryptoHash::hash_bytes(b"weight10b");

        // Insert item with weight 1
        cache
            .try_lookup(
                key1,
                || Ok::<_, ()>((1, Box::new(TestType(1)))),
                |v| v.downcast_ref::<TestType>().unwrap().0,
            )
            .unwrap();

        // Insert item with weight 3
        cache
            .try_lookup(
                key2,
                || Ok::<_, ()>((3, Box::new(TestType(2)))),
                |v| v.downcast_ref::<TestType>().unwrap().0,
            )
            .unwrap();

        // Insert item with weight 5
        cache
            .try_lookup(
                key3,
                || Ok::<_, ()>((5, Box::new(TestType(3)))),
                |v| v.downcast_ref::<TestType>().unwrap().0,
            )
            .unwrap();

        // Insert item with weight 10 (total would be 19)
        cache
            .try_lookup(
                key4,
                || Ok::<_, ()>((10, Box::new(TestType(4)))),
                |v| v.downcast_ref::<TestType>().unwrap().0,
            )
            .unwrap();

        cache
            .try_lookup(
                key5,
                || Ok::<_, ()>((10, Box::new(TestType(5)))),
                |v| v.downcast_ref::<TestType>().unwrap().0,
            )
            .unwrap();

        // Only the last inserted item should remain in cache
        assert!(!cache.contains(key1), "Item 1 should have been evicted");
        assert!(!cache.contains(key2), "Item 2 should have been evicted");
        assert!(!cache.contains(key3), "Item 3 should have been evicted");
        assert!(!cache.contains(key4), "Item 4 should have been evicted");
        assert!(cache.contains(key5), "Item 5 should be in cache");
    }

    #[test]
    fn lru_weighted_cache_item_capacity_eviction_tracks_weight() {
        // Regression test: when lru::LruCache silently evicts an entry due to
        // item_capacity, the evicted entry's weight must be subtracted from
        // current_weight. Otherwise the tracked weight drifts upward and
        // triggers spurious weight-based evictions.
        let item_capacity = NonZeroUsize::new(2).unwrap();
        let max_weight = 25;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        cache.put("a", 10, 1);
        cache.put("b", 10, 2);
        // At this point: items={a,b}, current_weight should be 20.

        // Inserting "c" causes lru::LruCache to silently evict "a" (the LRU).
        // Total actual weight is now 20 (b=10 + c=10) which fits in max_weight=25.
        cache.put("c", 10, 3);

        // "b" should still be in the cache because total weight (20) <= max_weight (25).
        assert!(
            cache.contains(&"b"),
            "item 'b' was spuriously evicted due to inflated current_weight"
        );
        assert!(cache.contains(&"c"), "item 'c' should be in cache");
        assert!(!cache.contains(&"a"), "item 'a' should have been evicted by item_capacity");
    }

    #[test]
    fn lru_weighted_cache_same_key_replace_tracks_weight() {
        let item_capacity = NonZeroUsize::new(2).unwrap();
        let max_weight = 25;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        cache.put("a", 20, 1); // current_weight = 20
        cache.put("a", 5, 2); // current_weight = 5 (old weight 20 subtracted)
        cache.put("b", 20, 3); // current_weight = 25, fits in max_weight

        assert!(cache.contains(&"a"), "item 'a' should still be in cache");
        assert!(cache.contains(&"b"), "item 'b' should still be in cache");
    }

    #[test]
    fn lru_weighted_cache_insert_returns_weight_evictions_in_order() {
        // Item cap is generous; eviction must be weight-driven.
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 10;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 4, 1).is_empty());
        assert!(cache.insert("b", 4, 2).is_empty()); // current_weight = 8
        // Promote "a" so "b" is LRU.
        cache.get_mut(&"a");

        // Inserting "c" (weight 4) pushes current_weight to 12, must evict
        // exactly the LRU entry "b".
        let evicted = cache.insert("c", 4, 3);
        assert_eq!(evicted, vec![("b", 2)]);
        assert!(cache.contains(&"a"));
        assert!(!cache.contains(&"b"));
        assert!(cache.contains(&"c"));
    }

    #[test]
    fn lru_weighted_cache_insert_oversized_returns_self() {
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 5;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        // weight > max_weight: entry must not be stored, and must be returned
        // so the caller can clean up any side effect.
        let evicted = cache.insert("too_big", 100, 42);
        assert_eq!(evicted, vec![("too_big", 42)]);
        assert!(!cache.contains(&"too_big"));
        assert_eq!(cache.current_weight(), 0);
    }

    #[test]
    fn lru_weighted_cache_insert_oversized_drops_existing_entry() {
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 10;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 5, 1).is_empty());
        assert!(cache.contains(&"a"));

        // An oversized replacement for an existing key must drop the old entry
        // (and reclaim its weight), not leave it tracked.
        let evicted = cache.insert("a", 100, 2);
        assert_eq!(evicted, vec![("a", 2)]);
        assert!(!cache.contains(&"a"), "old entry must be dropped");
        assert_eq!(cache.current_weight(), 0, "old entry's weight must be reclaimed");
    }

    #[test]
    fn lru_weighted_cache_insert_same_key_replace_returns_old() {
        let item_capacity = NonZeroUsize::new(10).unwrap();
        let max_weight = 100;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 10, 1).is_empty());
        // Replacing "a" must report the previous value and weight, and must
        // not cascade into a weight eviction because the new weight fits.
        let evicted = cache.insert("a", 20, 2);
        assert_eq!(evicted, vec![("a", 1)]);
        assert!(cache.contains(&"a"));
        assert_eq!(cache.current_weight(), 20);
    }

    #[test]
    fn lru_weighted_cache_insert_item_capacity_eviction_returned() {
        // When `lru::LruCache` drops the LRU entry due to item_capacity (not
        // weight), `insert` must still surface it so callers can clean up.
        let item_capacity = NonZeroUsize::new(2).unwrap();
        let max_weight = 100;
        let mut cache = LruWeightedCache::<&str, u32>::new(item_capacity, max_weight);

        assert!(cache.insert("a", 1, 1).is_empty());
        assert!(cache.insert("b", 1, 2).is_empty());
        let evicted = cache.insert("c", 1, 3);
        assert_eq!(evicted, vec![("a", 1)]);
    }

    #[test]
    fn lru_weighted_cache_unbounded_evicts_only_on_weight() {
        // With an unbounded item cap, only weight should trigger eviction.
        let max_weight = 10;
        let mut cache = LruWeightedCache::<&str, u32>::without_item_cap(max_weight);

        // Many small entries fit despite there being no item cap.
        for i in 0..10 {
            let evicted = cache.insert(NAMES[i], 1, i as u32);
            assert!(evicted.is_empty(), "insert {i} should not evict");
        }
        assert_eq!(cache.len(), 10);
        assert_eq!(cache.current_weight(), 10);

        // Promote "n0" so it is MRU; "n1" becomes LRU.
        cache.get_mut(&"n0");

        // Inserting one more entry must evict exactly one LRU.
        let evicted = cache.insert("extra", 1, 99);
        assert_eq!(evicted, vec![("n1", 1)]);
        assert!(cache.contains(&"n0"));
        assert!(!cache.contains(&"n1"));
        assert!(cache.contains(&"extra"));
    }

    const NAMES: [&str; 10] = ["n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"];

    #[cfg(feature = "test_features")]
    #[test]
    fn test_clear_compiled_contract_cache() {
        let cache = FilesystemContractRuntimeCache::test().unwrap();

        let contract1 = ContractCode::new(near_test_contracts::sized_contract(100).to_vec(), None);
        let contract2 = ContractCode::new(near_test_contracts::sized_contract(200).to_vec(), None);

        let compiled_contract1 = CompiledContractInfo {
            wasm_bytes: 100,
            compiled: CompiledContract::Code(contract1.code().to_vec()),
        };

        let compiled_contract2 = CompiledContractInfo {
            wasm_bytes: 200,
            compiled: CompiledContract::Code(contract2.code().to_vec()),
        };

        let insert_and_assert_keys_exist = || {
            cache.put(contract1.hash(), compiled_contract1.clone()).unwrap();
            cache.put(contract2.hash(), compiled_contract2.clone()).unwrap();

            assert_eq!(cache.get(contract1.hash()).unwrap().unwrap(), compiled_contract1);
            assert_eq!(cache.get(contract2.hash()).unwrap().unwrap(), compiled_contract2);
        };

        let assert_keys_absent = || {
            assert_eq!(cache.has(contract1.hash()).unwrap(), false);
            assert_eq!(cache.has(contract2.hash()).unwrap(), false);
        };

        // Insert the keys, and then clear the cache, and assert that keys no longer exist after clear.
        insert_and_assert_keys_exist();
        cache.test_only_clear().unwrap();
        assert_keys_absent();

        // Insert the keys again and assert that the cache can be updated after clear.
        insert_and_assert_keys_exist();
    }

    // ----- on-disk eviction feature tests -----
    #[cfg(not(windows))]
    mod eviction {
        use super::*;

        const TEST_PAYLOAD_LEN: usize = 100;

        fn make_test_entry(filler: u8) -> CompiledContractInfo {
            CompiledContractInfo {
                wasm_bytes: TEST_PAYLOAD_LEN as u64,
                compiled: CompiledContract::Code(vec![filler; TEST_PAYLOAD_LEN]),
            }
        }

        /// Bytes a single test entry occupies in the on-disk index — exactly what
        /// `put` writes for a `Code(vec![..; TEST_PAYLOAD_LEN])` entry.
        fn test_entry_weight() -> u64 {
            entry_disk_size(&make_test_entry(0))
        }

        fn cache_dir_path(cache: &FilesystemContractRuntimeCache) -> std::path::PathBuf {
            cache.state.test_temp_dir.as_ref().unwrap().path().join("contract.cache")
        }

        #[test]
        fn touch_keeps_hot_key_from_eviction() {
            // Limit holds exactly 5 entries; the 6th `put` must evict the LRU.
            let cache =
                FilesystemContractRuntimeCache::test_with_disk_cache_bytes(5 * test_entry_weight())
                    .unwrap();

            let target = CryptoHash::hash_bytes(b"target");
            cache.put(&target, make_test_entry(0xAA)).unwrap();

            // `touch` before each cold put keeps the target MRU, so the LRU drop
            // falls on the oldest cold key.
            for i in 0..5 {
                cache.touch(&target);
                let cold = CryptoHash::hash_bytes(format!("cold{i}").as_bytes());
                cache.put(&cold, make_test_entry(0xBB)).unwrap();
            }

            assert!(cache.has(&target).unwrap(), "target should survive — touch kept it MRU");
            assert!(
                !cache.has(&CryptoHash::hash_bytes(b"cold0")).unwrap(),
                "cold0 (the LRU after the promotions) should have been evicted"
            );
        }

        #[test]
        fn without_touch_oldest_is_evicted() {
            // Sanity: same workload, no `touch` — the target (oldest) goes.
            let cache =
                FilesystemContractRuntimeCache::test_with_disk_cache_bytes(5 * test_entry_weight())
                    .unwrap();

            let target = CryptoHash::hash_bytes(b"target");
            cache.put(&target, make_test_entry(0xAA)).unwrap();

            for i in 0..5 {
                let cold = CryptoHash::hash_bytes(format!("cold{i}").as_bytes());
                cache.put(&cold, make_test_entry(0xBB)).unwrap();
            }

            assert!(
                !cache.has(&target).unwrap(),
                "target should be evicted: it was LRU without touch protection"
            );
            // The most-recent cold survives.
            assert!(cache.has(&CryptoHash::hash_bytes(b"cold4")).unwrap());
        }

        #[test]
        fn eviction_unlinks_file_from_disk() {
            // Holds exactly 2 entries; the 3rd `put` must unlink the LRU's file.
            let cache =
                FilesystemContractRuntimeCache::test_with_disk_cache_bytes(2 * test_entry_weight())
                    .unwrap();
            let dir = cache_dir_path(&cache);

            let k1 = CryptoHash::hash_bytes(b"k1");
            let k2 = CryptoHash::hash_bytes(b"k2");
            let k3 = CryptoHash::hash_bytes(b"k3");
            cache.put(&k1, make_test_entry(0x11)).unwrap();
            cache.put(&k2, make_test_entry(0x22)).unwrap();
            cache.put(&k3, make_test_entry(0x33)).unwrap();

            assert!(!dir.join(k1.to_string()).exists(), "k1 file must be unlinked");
            assert!(dir.join(k2.to_string()).exists());
            assert!(dir.join(k3.to_string()).exists());
        }

        #[test]
        fn oversized_replacement_of_tracked_key_is_evicted() {
            // Budget holds exactly one normal entry, so a larger payload for the
            // same key is oversized: the entry must be dropped and its file
            // unlinked, not retained under the stale weight.
            let cache =
                FilesystemContractRuntimeCache::test_with_disk_cache_bytes(test_entry_weight())
                    .unwrap();
            let dir = cache_dir_path(&cache);
            let k = CryptoHash::hash_bytes(b"k");

            cache.put(&k, make_test_entry(0x11)).unwrap();
            assert!(cache.has(&k).unwrap(), "first (fitting) put should be tracked");

            let oversized = CompiledContractInfo {
                wasm_bytes: 2 * TEST_PAYLOAD_LEN as u64,
                compiled: CompiledContract::Code(vec![0x22; 2 * TEST_PAYLOAD_LEN]),
            };
            cache.put(&k, oversized).unwrap();

            assert!(
                !dir.join(k.to_string()).exists(),
                "oversized replacement file must be unlinked, not retained over budget"
            );
            assert!(!cache.has(&k).unwrap(), "oversized replacement must not stay tracked");
        }

        #[test]
        fn disk_limit_at_or_above_cap_is_a_clean_error() {
            // A limit the LRU index can't represent must surface as an `Err`, not a
            // panic, so a bad config doesn't crash the node at startup.
            let too_big = FilesystemContractRuntimeCache::MAX_DISK_CACHE_BYTES + 1;
            match FilesystemContractRuntimeCache::test_with_disk_cache_bytes(too_big) {
                Err(err) => assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput),
                Ok(_) => panic!("limit above the cap must be rejected"),
            }
        }

        #[test]
        fn oversized_artifact_is_not_left_on_disk() {
            // A disk budget smaller than a single entry makes every `put`
            // oversized: `insert` refuses to track it, so `put` must unlink the
            // file it just wrote rather than leave an untracked artifact on disk,
            // permanently over the configured bound.
            let cache = FilesystemContractRuntimeCache::test_with_disk_cache_bytes(1).unwrap();
            let dir = cache_dir_path(&cache);

            let k = CryptoHash::hash_bytes(b"too_big");
            cache.put(&k, make_test_entry(0x55)).unwrap();

            assert!(
                !dir.join(k.to_string()).exists(),
                "oversized artifact's file must be unlinked, not left on disk"
            );
            assert!(!cache.has(&k).unwrap(), "oversized artifact must not be reported as cached");
        }

        #[test]
        fn startup_scan_trims_over_limit_oldest_first() {
            let tempdir = tempfile::TempDir::new().unwrap();
            let cache_dir = tempdir.path().join("contract.cache");
            std::fs::create_dir_all(&cache_dir).unwrap();

            // Three equally-sized files; explicit atimes (via `set_times`) make
            // the test deterministic regardless of the mount's atime mode.
            let payload = vec![0u8; TEST_PAYLOAD_LEN];
            let k_old = CryptoHash::hash_bytes(b"old");
            let k_mid = CryptoHash::hash_bytes(b"mid");
            let k_new = CryptoHash::hash_bytes(b"new");
            for k in [&k_old, &k_mid, &k_new] {
                std::fs::write(cache_dir.join(k.to_string()), &payload).unwrap();
            }
            let set_atime = |k: &CryptoHash, secs: u64| {
                use std::time::{Duration, UNIX_EPOCH};
                let t = UNIX_EPOCH + Duration::from_secs(secs);
                let times = std::fs::FileTimes::new().set_accessed(t).set_modified(t);
                let f = std::fs::File::open(cache_dir.join(k.to_string())).unwrap();
                f.set_times(times).unwrap();
            };
            set_atime(&k_old, 1_000);
            set_atime(&k_mid, 2_000);
            set_atime(&k_new, 3_000);

            // Cap holds 2 of these 100-byte files; the oldest must be trimmed.
            let cache = FilesystemContractRuntimeCache::with_memory_cache(
                tempdir.path(),
                None::<&str>,
                "contract.cache",
                0,
                None,
                2 * TEST_PAYLOAD_LEN as u64,
                noop_background_spawner(),
            )
            .unwrap();

            assert!(!cache_dir.join(k_old.to_string()).exists(), "oldest atime should be trimmed");
            assert!(cache_dir.join(k_mid.to_string()).exists());
            assert!(cache_dir.join(k_new.to_string()).exists());
            // And the survivors should be visible through the cache API.
            assert!(!cache.has(&k_old).unwrap());
            assert!(cache.has(&k_mid).unwrap());
            assert!(cache.has(&k_new).unwrap());
        }

        #[test]
        fn self_heals_when_file_externally_removed() {
            let cache =
                FilesystemContractRuntimeCache::test_with_disk_cache_bytes(1 << 20).unwrap();
            let dir = cache_dir_path(&cache);

            let k = CryptoHash::hash_bytes(b"foo");
            cache.put(&k, make_test_entry(0xAB)).unwrap();
            assert!(cache.has(&k).unwrap());

            // Operator (or anything else) removes the file out from under us.
            std::fs::remove_file(dir.join(k.to_string())).unwrap();
            assert!(cache.get(&k).unwrap().is_none(), "missing file must surface as a miss");
            assert!(!cache.has(&k).unwrap());

            // Re-`put` rebuilds the file (and refreshes the index).
            cache.put(&k, make_test_entry(0xCD)).unwrap();
            let value = cache.get(&k).unwrap().expect("entry must be back");
            match value.compiled {
                CompiledContract::Code(bytes) => {
                    assert_eq!(bytes, vec![0xCD; TEST_PAYLOAD_LEN], "should be the re-put content")
                }
                _ => panic!("expected Code"),
            }
        }

        #[test]
        fn refresh_disk_atime_advances_atime_and_preserves_mtime() {
            use std::time::{Duration, UNIX_EPOCH};
            // An explicit `utimensat` must bump atime regardless of the mount's
            // atime policy, while leaving mtime (which records compile time for the
            // protocol-version sweep) untouched.
            let cache =
                FilesystemContractRuntimeCache::test_with_disk_cache_bytes(1 << 20).unwrap();
            let path = cache_dir_path(&cache).join(CryptoHash::hash_bytes(b"refresh").to_string());
            let k = CryptoHash::hash_bytes(b"refresh");
            cache.put(&k, make_test_entry(0x11)).unwrap();

            // Backdate both timestamps to a fixed, clearly-old instant.
            let old = UNIX_EPOCH + Duration::from_secs(1_000);
            let times = std::fs::FileTimes::new().set_accessed(old).set_modified(old);
            std::fs::File::open(&path).unwrap().set_times(times).unwrap();

            cache.refresh_disk_atime(&k);

            let meta = std::fs::metadata(&path).unwrap();
            assert!(
                meta.accessed().unwrap() > old + Duration::from_secs(60),
                "atime should have been refreshed to ~now"
            );
            assert_eq!(
                meta.modified().unwrap(),
                old,
                "mtime must be preserved by the atime refresh"
            );
        }

        #[test]
        fn touch_throttles_and_enqueues_atime_refresh() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            // A spawner that runs the job inline and counts how many it received.
            let count = Arc::new(AtomicUsize::new(0));
            let observed = Arc::clone(&count);
            let mut cache =
                FilesystemContractRuntimeCache::test_with_disk_cache_bytes(1 << 20).unwrap();
            cache.test_set_background_job_spawner(Arc::new(move |task| {
                observed.fetch_add(1, Ordering::SeqCst);
                task();
            }));

            let k = CryptoHash::hash_bytes(b"hot");
            cache.put(&k, make_test_entry(0x22)).unwrap();

            // Right after `put` the entry is inside its (default) throttle window.
            cache.touch(&k);
            assert_eq!(
                count.load(Ordering::SeqCst),
                0,
                "touch within the throttle must not refresh"
            );

            // With a zero window the entry is eligible: one touch enqueues exactly
            // one refresh and records "refreshed now".
            cache.test_set_access_time_refresh_throttle(Duration::ZERO);
            cache.touch(&k);
            assert_eq!(
                count.load(Ordering::SeqCst),
                1,
                "an eligible touch must enqueue one refresh"
            );

            // Restore the window; the just-recorded refresh time now suppresses.
            cache.test_set_access_time_refresh_throttle(ACCESS_TIME_REFRESH_THROTTLE);
            cache.touch(&k);
            assert_eq!(count.load(Ordering::SeqCst), 1, "the next touch must be throttled again");

            // An untracked key never enqueues anything.
            cache.touch(&CryptoHash::hash_bytes(b"never-put"));
            assert_eq!(count.load(Ordering::SeqCst), 1, "untracked key must not refresh");
        }
    }
}
