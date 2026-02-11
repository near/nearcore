// cspell:ignore NOENT, RDONLY, RGRP, RUSR, TRUNC, WGRP, WRONLY, WUSR
// cspell:ignore mikan, fstat, openat, renameat, unlinkat

use crate::ContractCode;
use crate::errors::ContractPrecompilatonResult;
use crate::logic::Config;
use crate::logic::errors::{CacheError, CompilationError};
use crate::runner::VMKindExt;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use parking_lot::Mutex;

use std::any::Any;
use std::collections::HashMap;
use std::fmt;
use std::num::NonZeroUsize;
use std::sync::Arc;

#[cfg(not(windows))]
use rand::Rng as _;
#[cfg(not(windows))]
use std::io::{Read, Write};

#[cfg(any(feature = "wasmtime_vm", all(feature = "near_vm", target_arch = "x86_64")))]
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

#[cfg(any(feature = "wasmtime_vm", all(feature = "near_vm", target_arch = "x86_64")))]
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
}

impl MockContractRuntimeCache {
    pub fn len(&self) -> usize {
        self.store.lock().len()
    }
}

impl ContractRuntimeCache for MockContractRuntimeCache {
    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
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
/// This cache however does not implement any clean-up policies. While it is possible to truncate
/// a file that has been written to the cache before (`put` an empty buffer), the file will remain
/// in place until an operator (or somebody else) removes files at their own discretion.
#[cfg(not(windows))]
#[derive(Clone)]
pub struct FilesystemContractRuntimeCache {
    state: Arc<FilesystemContractRuntimeCacheState>,
}

#[cfg(not(windows))]
struct FilesystemContractRuntimeCacheState {
    dir: rustix::fd::OwnedFd,
    any_cache: AnyCache,
    test_temp_dir: Option<tempfile::TempDir>,
}

#[cfg(not(windows))]
impl FilesystemContractRuntimeCache {
    pub fn new<StorePath, ContractCachePath>(
        home_dir: &std::path::Path,
        store_path: Option<&StorePath>,
        contract_cache_path: &ContractCachePath,
    ) -> std::io::Result<Self>
    where
        StorePath: AsRef<std::path::Path> + ?Sized,
        ContractCachePath: AsRef<std::path::Path> + ?Sized,
    {
        Self::with_memory_cache(home_dir, store_path, contract_cache_path, 0)
    }

    /// When setting up a cache of compiled contracts, also set-up a `size` element in-memory
    /// cache.
    ///
    /// This additional cache is usually used to store loaded artifacts, but data stored can really
    /// be anything and depends on the specific VM kind.
    ///
    /// Note though, that this memory cache is *not* used to additionally cache files from the
    /// filesystem – OS page cache already does that for us transparently.
    pub fn with_memory_cache<StorePath, ContractCachePath>(
        home_dir: &std::path::Path,
        store_path: Option<&StorePath>,
        contract_cache_path: &ContractCachePath,
        memcache_expected_item_count: usize,
    ) -> std::io::Result<Self>
    where
        StorePath: AsRef<std::path::Path> + ?Sized,
        ContractCachePath: AsRef<std::path::Path> + ?Sized,
    {
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
        let dir =
            rustix::fs::open(&path, rustix::fs::OFlags::DIRECTORY, rustix::fs::Mode::empty())?;
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

        Ok(Self {
            state: Arc::new(FilesystemContractRuntimeCacheState {
                dir,
                any_cache: AnyCache::new(
                    expected_cache_item_count,
                    memcache_expected_item_count as u64 * AVG_COMPILED_CONTRACT_WEIGHT,
                ),
                test_temp_dir: None,
            }),
        })
    }

    pub fn test() -> std::io::Result<Self> {
        let tempdir = tempfile::TempDir::new()?;
        let mut cache = Self::new(tempdir.path(), None::<&str>, "contract.cache")?;
        Arc::get_mut(&mut cache.state).unwrap().test_temp_dir = Some(tempdir);
        Ok(cache)
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
        const MAX_ATTEMPTS: u32 = 5;
        use rustix::fs::{Mode, OFlags};
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
            match rustix::fs::openat(&self.state.dir, &temporary_filename, flags, mode) {
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
        // This is atomic, so there wouldn't be instances where getters see an intermediate state.
        rustix::fs::renameat(&self.state.dir, temp_filename, &self.state.dir, final_filename)?;

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
        use rustix::fs::{Mode, OFlags};
        let filename = key.to_string();
        let mode = Mode::empty();
        let flags = OFlags::RDONLY;
        let file = rustix::fs::openat(&self.state.dir, &filename, flags, mode);
        let file = match file {
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(e) => return Err(e.into()),
            Ok(file) => file,
        };
        let stat = rustix::fs::fstat(&file)?;
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
        Ok(match tag {
            CODE_TAG => {
                Some(CompiledContractInfo { wasm_bytes, compiled: CompiledContract::Code(buffer) })
            }
            ERROR_TAG => Some(CompiledContractInfo {
                wasm_bytes,
                compiled: CompiledContract::CompileModuleError(borsh::from_slice(&buffer)?),
            }),
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
                None
            }
        })
    }

    /// Clears the in-memory cache and files in the cache directory.
    ///
    /// The cache must be created using `test` method, otherwise this method will panic.
    #[cfg(feature = "test_features")]
    fn test_only_clear(&self) -> std::io::Result<()> {
        use rustix::fs::AtFlags;
        let Some(_temp_dir) = &self.state.test_temp_dir else {
            panic!("must be called for testing only");
        };
        self.memory_cache().clear();
        for entry in rustix::fs::Dir::read_from(&self.state.dir).unwrap() {
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
                    if let Err(err) =
                        rustix::fs::unlinkat(&self.state.dir, entry.file_name(), AtFlags::empty())
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
    fn new(item_capacity: NonZeroUsize, max_weight: u64) -> Self {
        assert!(
            max_weight < u64::MAX / 2,
            "cache weight must be capped at u64::MAX / 2 to avoid overflows"
        );
        Self { current_weight: 0, max_weight, cache: lru::LruCache::new(item_capacity) }
    }

    fn get(&mut self, key: &K) -> Option<&(u64, V)> {
        self.cache.get(key)
    }

    fn put(&mut self, key: K, weight: u64, value: V) {
        if self.max_weight < weight {
            return;
        }

        // This may temporarily increase the cache weight up to twice the max.
        self.cache.put(key, (weight, value));
        // overflow is impossible due to assert on max_weight in new() and early return above
        self.current_weight += weight;

        while self.max_weight < self.current_weight {
            let (_, (evicted_weight, _)) = self
                .cache
                .pop_lru()
                .expect("current_weight >= max_weight implies cache is not empty");
            self.current_weight -= evicted_weight;
        }
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
}

impl AnyCache {
    fn new(max_item_count: usize, max_cache_weight: u64) -> Self {
        Self {
            cache: if let Some(max_item_count) = NonZeroUsize::new(max_item_count) {
                Some(Mutex::new(LruWeightedCache::new(max_item_count, max_cache_weight)))
            } else {
                None
            },
        }
    }

    pub fn clear(&self) {
        if let Some(cache) = &self.cache {
            cache.lock().clear();
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
        cache.lock().put(key, weight, generated);
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
}
