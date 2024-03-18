use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{CacheError, CompilationError};
use crate::logic::{CompiledContract, CompiledContractCache, Config};
use crate::runner::VMKindExt;
use crate::ContractCode;
use borsh::BorshSerialize;
use near_parameters::vm::VMKind;
use near_primitives_core::hash::CryptoHash;
use std::collections::HashMap;
use std::fmt;
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, BorshSerialize)]
enum ContractCacheKey {
    _Version1,
    _Version2,
    _Version3,
    Version4 {
        code_hash: CryptoHash,
        vm_config_non_crypto_hash: u64,
        vm_kind: VMKind,
        vm_hash: u64,
    },
}

fn vm_hash(vm_kind: VMKind) -> u64 {
    match vm_kind {
        #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
        VMKind::Wasmer0 => crate::wasmer_runner::wasmer0_vm_hash(),
        #[cfg(not(all(feature = "wasmer0_vm", target_arch = "x86_64")))]
        VMKind::Wasmer0 => panic!("Wasmer0 is not enabled"),
        #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
        VMKind::Wasmer2 => crate::wasmer2_runner::wasmer2_vm_hash(),
        #[cfg(not(all(feature = "wasmer2_vm", target_arch = "x86_64")))]
        VMKind::Wasmer2 => panic!("Wasmer2 is not enabled"),
        #[cfg(feature = "wasmtime_vm")]
        VMKind::Wasmtime => crate::wasmtime_runner::wasmtime_vm_hash(),
        #[cfg(not(feature = "wasmtime_vm"))]
        VMKind::Wasmtime => panic!("Wasmtime is not enabled"),
        #[cfg(all(feature = "near_vm", target_arch = "x86_64"))]
        VMKind::NearVm => crate::near_vm_runner::near_vm_vm_hash(),
        #[cfg(not(all(feature = "near_vm", target_arch = "x86_64")))]
        VMKind::NearVm => panic!("NearVM is not enabled"),
    }
}

#[tracing::instrument(level = "trace", target = "vm", "get_key", skip_all)]
pub fn get_contract_cache_key(code: &ContractCode, config: &Config) -> CryptoHash {
    let key = ContractCacheKey::Version4 {
        code_hash: *code.hash(),
        vm_config_non_crypto_hash: config.non_crypto_hash(),
        vm_kind: config.vm_kind,
        vm_hash: vm_hash(config.vm_kind),
    };
    CryptoHash::hash_borsh(key)
}

#[derive(Default, Clone)]
pub struct MockCompiledContractCache {
    store: Arc<Mutex<HashMap<CryptoHash, CompiledContract>>>,
}

impl MockCompiledContractCache {
    pub fn len(&self) -> usize {
        self.store.lock().unwrap().len()
    }
}

impl CompiledContractCache for MockCompiledContractCache {
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> std::io::Result<()> {
        self.store.lock().unwrap().insert(*key, value);
        Ok(())
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContract>> {
        Ok(self.store.lock().unwrap().get(key).map(Clone::clone))
    }

    fn handle(&self) -> Box<dyn CompiledContractCache> {
        Box::new(self.clone())
    }
}

impl fmt::Debug for MockCompiledContractCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.store.lock().unwrap();
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
#[derive(Clone)]
pub struct FilesystemCompiledContractCache {
    state: Arc<FilesystemCompiledContractCacheState>,
}

struct FilesystemCompiledContractCacheState {
    dir: rustix::fd::OwnedFd,
    test_temp_dir: Option<tempfile::TempDir>,
}

impl FilesystemCompiledContractCache {
    pub fn new<SP: AsRef<std::path::Path> + ?Sized>(
        home_dir: &std::path::Path,
        store_path: Option<&SP>,
    ) -> std::io::Result<Self> {
        let store_path = store_path.map(AsRef::as_ref).unwrap_or_else(|| "data".as_ref());
        let path: std::path::PathBuf =
            [home_dir, store_path, "contracts".as_ref()].into_iter().collect();
        std::fs::create_dir_all(&path)?;
        let dir =
            rustix::fs::open(&path, rustix::fs::OFlags::DIRECTORY, rustix::fs::Mode::empty())?;
        tracing::debug!(
            target: "vm",
            path = %path.display(),
            message = "opened a contract executable cache directory"
        );
        Ok(Self {
            state: Arc::new(FilesystemCompiledContractCacheState { dir, test_temp_dir: None }),
        })
    }

    pub fn test() -> std::io::Result<Self> {
        let tempdir = tempfile::TempDir::new()?;
        let mut cache = Self::new(tempdir.path(), None::<&str>)?;
        Arc::get_mut(&mut cache.state).unwrap().test_temp_dir = Some(tempdir);
        Ok(cache)
    }
}

/// Byte added after a serialized payload representing a compilation failure.
///
/// This is ASCII LF.
const ERROR_TAG: u8 = 0b00001010;
/// Byte added after a serialized payload representing the contract code.
///
/// Value is fairly arbitrarily chosen such that a couple of bit flips do not make this an
/// [`ERROR_TAG`].
const CODE_TAG: u8 = 0b10010101;

/// Cache for compiled contracts code in plain filesystem.
impl CompiledContractCache for FilesystemCompiledContractCache {
    fn handle(&self) -> Box<dyn CompiledContractCache> {
        Box::new(self.clone())
    }

    #[tracing::instrument(
        level = "trace",
        target = "vm",
        "FilesystemCompiledContractCache::put",
        skip_all,
        fields(key = key.to_string(), value.len = value.debug_len()),
    )]
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> std::io::Result<()> {
        use rustix::fs::{Mode, OFlags};
        let final_filename = key.to_string();
        let mut temp_file = tempfile::Builder::new().make_in("", |filename| {
            let mode = Mode::RUSR | Mode::WUSR | Mode::RGRP | Mode::WGRP;
            let flags = OFlags::CREATE | OFlags::TRUNC | OFlags::WRONLY;
            Ok(std::fs::File::from(rustix::fs::openat(&self.state.dir, filename, flags, mode)?))
        })?;
        // This section manually "serializes" the data. The cache is quite sensitive to
        // unnecessary overheads and in order to enable things like mmap-based file access, we want
        // to have full control of what has been written.
        match value {
            CompiledContract::CompileModuleError(e) => {
                borsh::to_writer(&mut temp_file, &e)?;
                temp_file.write_all(&[ERROR_TAG])?;
            }
            CompiledContract::Code(bytes) => {
                temp_file.write_all(&bytes)?;
                // Writing the tag at the end gives us well aligned buffer of the data above which
                // is necessary for 0-copy deserialization later on.
                temp_file.write_all(&[CODE_TAG])?;
            }
        }
        let temp_filename = temp_file.into_temp_path();
        // This is atomic, so there wouldn't be instances where getters see an intermediate state.
        rustix::fs::renameat(&self.state.dir, &*temp_filename, &self.state.dir, final_filename)?;
        // Don't attempt deleting the temporary file now that it has been moved.
        std::mem::forget(temp_filename);
        Ok(())
    }

    #[tracing::instrument(
        level = "trace",
        target = "vm",
        "FilesystemCompiledContractCache::get",
        skip_all,
        fields(key = key.to_string()),
    )]
    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContract>> {
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
        // TODO: explore mmaping the file and lending the map to the caller via a closure callback.
        // This would require some additional refactor work, but would likely help us to reduce the
        // system call overhead in this area.
        let mut buffer = Vec::with_capacity(stat.st_size.try_into().unwrap());
        let mut file = std::fs::File::from(file);
        file.read_to_end(&mut buffer)?;
        match buffer.pop() {
            // The file turns out to be empty/truncated? Treat as if there's no cached file.
            None => Ok(None),
            Some(CODE_TAG) => Ok(Some(CompiledContract::Code(buffer))),
            Some(ERROR_TAG) => {
                Ok(Some(CompiledContract::CompileModuleError(borsh::from_slice(&buffer)?)))
            }
            // File is malformed? For this code, since we're talking about a cache lets just treat
            // it as if there is no cached file as well. The cached file may eventually be
            // overwritten with a valid copy. And since we can compile a new copy, there doesn't
            // seem to be much reason to possibly crash the node due to this.
            Some(_) => {
                tracing::debug!(
                    target: "vm",
                    message = "cached contract executable was found to be malformed",
                    key = %key
                );
                Ok(None)
            }
        }
    }
}

/// Precompiles contract for the current default VM, and stores result to the cache.
/// Returns `Ok(true)` if compiled code was added to the cache, and `Ok(false)` if element
/// is already in the cache, or if cache is `None`.
pub fn precompile_contract(
    code: &ContractCode,
    config: &Config,
    cache: Option<&dyn CompiledContractCache>,
) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError> {
    let _span = tracing::debug_span!(target: "vm", "precompile_contract").entered();
    let vm_kind = config.vm_kind;
    let runtime = vm_kind
        .runtime(config.clone())
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));
    let cache = match cache {
        Some(it) => it,
        None => return Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable)),
    };
    let key = get_contract_cache_key(code, config);
    // Check if we already cached with such a key.
    if cache.has(&key).map_err(CacheError::ReadError)? {
        return Ok(Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
    }
    runtime.precompile(code, cache)
}
