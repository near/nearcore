use crate::errors::ContractPrecompilatonResult;
use crate::vm_kind::VMKind;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::{CacheError, CompilationError};
use near_vm_logic::{ProtocolVersion, VMConfig};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

#[cfg(target_arch = "x86_64")]
use crate::prepare;
#[cfg(target_arch = "x86_64")]
use near_vm_errors::{FunctionCallError, VMError};

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

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
enum CacheRecord {
    CompileModuleError(CompilationError),
    Code(Vec<u8>),
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
    }
}

pub fn get_contract_cache_key(
    code: &ContractCode,
    vm_kind: VMKind,
    config: &VMConfig,
) -> CryptoHash {
    let _span = tracing::debug_span!(target: "vm", "get_key").entered();
    let key = ContractCacheKey::Version4 {
        code_hash: *code.hash(),
        vm_config_non_crypto_hash: config.non_crypto_hash(),
        vm_kind,
        vm_hash: vm_hash(vm_kind),
    };
    near_primitives::hash::hash(&key.try_to_vec().unwrap())
}

#[cfg(target_arch = "x86_64")]
fn cache_error(
    error: &CompilationError,
    key: &CryptoHash,
    cache: &dyn CompiledContractCache,
) -> Result<(), CacheError> {
    let record = CacheRecord::CompileModuleError(error.clone());
    let record = record.try_to_vec().unwrap();
    cache.put(&key.0, &record).map_err(|_io_err| CacheError::ReadError)?;
    Ok(())
}

#[cfg(target_arch = "x86_64")]
pub fn into_vm_result<T>(
    res: Result<Result<T, CompilationError>, CacheError>,
) -> Result<T, VMError> {
    match res {
        Ok(Ok(it)) => Ok(it),
        Ok(Err(err)) => Err(VMError::FunctionCallError(FunctionCallError::CompilationError(err))),
        Err(cache_error) => Err(VMError::CacheError(cache_error)),
    }
}

#[derive(Default)]
pub struct MockCompiledContractCache {
    store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl MockCompiledContractCache {
    pub fn len(&self) -> usize {
        self.store.lock().unwrap().len()
    }
}

impl CompiledContractCache for MockCompiledContractCache {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error> {
        self.store.lock().unwrap().insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        let res = self.store.lock().unwrap().get(key).cloned();
        Ok(res)
    }
}

impl fmt::Debug for MockCompiledContractCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.store.lock().unwrap();
        let hm: &HashMap<_, _> = &*guard;
        fmt::Debug::fmt(hm, f)
    }
}

#[cfg(all(not(feature = "no_cache"), target_arch = "x86_64"))]
const CACHE_SIZE: usize = 128;

#[cfg(all(feature = "wasmer0_vm", not(feature = "no_cache"), target_arch = "x86_64"))]
static WASMER_CACHE: once_cell::sync::Lazy<
    near_cache::SyncLruCache<CryptoHash, Result<wasmer_runtime::Module, CompilationError>>,
> = once_cell::sync::Lazy::new(|| near_cache::SyncLruCache::new(CACHE_SIZE));

#[cfg(all(feature = "wasmer2_vm", not(feature = "no_cache"), target_arch = "x86_64"))]
static WASMER2_CACHE: once_cell::sync::Lazy<
    near_cache::SyncLruCache<
        CryptoHash,
        Result<crate::wasmer2_runner::VMArtifact, CompilationError>,
    >,
> = once_cell::sync::Lazy::new(|| near_cache::SyncLruCache::new(CACHE_SIZE));

#[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
pub mod wasmer0_cache {
    use super::*;
    use near_vm_errors::CompilationError;
    use wasmer_runtime::{compiler_for_backend, Backend};
    use wasmer_runtime_core::cache::Artifact;
    use wasmer_runtime_core::load_cache_with;

    pub(crate) fn compile_module(
        code: &[u8],
        config: &VMConfig,
    ) -> Result<wasmer_runtime::Module, CompilationError> {
        let _span = tracing::debug_span!(target: "vm", "compile_module").entered();

        let prepared_code =
            prepare::prepare_contract(code, config).map_err(CompilationError::PrepareError)?;
        wasmer_runtime::compile(&prepared_code).map_err(|err| match err {
            wasmer_runtime::error::CompileError::ValidationError { .. } => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
            // NOTE: Despite the `InternalError` name, this failure occurs if
            // the input `code` is invalid wasm.
            wasmer_runtime::error::CompileError::InternalError { .. } => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
        })
    }

    pub(crate) fn compile_and_serialize_wasmer(
        wasm_code: &[u8],
        config: &VMConfig,
        key: &CryptoHash,
        cache: &dyn CompiledContractCache,
    ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> {
        let _span = tracing::debug_span!(target: "vm", "compile_and_serialize_wasmer").entered();

        let module = match compile_module(wasm_code, config) {
            Ok(module) => module,
            Err(err) => {
                cache_error(&err, key, cache)?;
                return Ok(Err(err));
            }
        };

        let code = module
            .cache()
            .and_then(|it| it.serialize())
            .map_err(|_e| CacheError::SerializationError { hash: key.0 })?;
        let serialized = CacheRecord::Code(code).try_to_vec().unwrap();
        cache.put(key.as_ref(), &serialized).map_err(|_io_err| CacheError::WriteError)?;
        Ok(Ok(module))
    }

    /// Deserializes contract or error from the binary data. Signature means that we could either
    /// return module or cached error, which both considered to be `Ok()`, or encounter an error during
    /// the deserialization process.
    fn deserialize_wasmer(
        serialized: &[u8],
    ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> {
        let _span = tracing::debug_span!(target: "vm", "deserialize_wasmer").entered();

        let record = CacheRecord::try_from_slice(serialized)
            .map_err(|_e| CacheError::DeserializationError)?;
        let serialized_artifact = match record {
            CacheRecord::CompileModuleError(err) => return Ok(Err(err)),
            CacheRecord::Code(code) => code,
        };
        let artifact = Artifact::deserialize(serialized_artifact.as_slice())
            .map_err(|_e| CacheError::DeserializationError)?;
        unsafe {
            let compiler = compiler_for_backend(Backend::Singlepass).unwrap();
            match load_cache_with(artifact, compiler.as_ref()) {
                Ok(module) => Ok(Ok(module)),
                Err(_) => Err(CacheError::DeserializationError),
            }
        }
    }

    fn compile_module_cached_wasmer_impl(
        key: CryptoHash,
        wasm_code: &[u8],
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> {
        match cache {
            None => Ok(compile_module(wasm_code, config)),
            Some(cache) => {
                let serialized = cache.get(&key.0).map_err(|_io_err| CacheError::ReadError)?;
                match serialized {
                    Some(serialized) => deserialize_wasmer(serialized.as_slice()),
                    None => compile_and_serialize_wasmer(wasm_code, config, &key, cache),
                }
            }
        }
    }

    pub(crate) fn compile_module_cached_wasmer0(
        code: &ContractCode,
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> {
        let key = get_contract_cache_key(code, VMKind::Wasmer0, config);

        #[cfg(not(feature = "no_cache"))]
        return WASMER_CACHE.get_or_try_put(key, |key| {
            compile_module_cached_wasmer_impl(*key, code.code(), config, cache)
        });

        #[cfg(feature = "no_cache")]
        return compile_module_cached_wasmer_impl(key, code.code(), config, cache);
    }
}

#[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
pub mod wasmer2_cache {
    use crate::wasmer2_runner::{VMArtifact, Wasmer2VM};
    use near_primitives::contract::ContractCode;
    use wasmer_engine::Executable;

    use super::*;

    pub(crate) fn compile_module_wasmer2(
        vm: &Wasmer2VM,
        code: &[u8],
        config: &VMConfig,
    ) -> Result<wasmer_engine_universal::UniversalExecutable, CompilationError> {
        let _span = tracing::debug_span!(target: "vm", "compile_module_wasmer2").entered();
        let prepared_code =
            prepare::prepare_contract(code, config).map_err(CompilationError::PrepareError)?;
        vm.compile_uncached(&prepared_code)
    }

    pub(crate) fn compile_and_serialize_wasmer2(
        wasm_code: &[u8],
        key: &CryptoHash,
        config: &VMConfig,
        cache: &dyn CompiledContractCache,
    ) -> Result<Result<VMArtifact, CompilationError>, CacheError> {
        let _span = tracing::debug_span!(target: "vm", "compile_and_serialize_wasmer2").entered();
        let vm = Wasmer2VM::new(config.clone());
        let executable = match compile_module_wasmer2(&vm, wasm_code, config) {
            Ok(module) => module,
            Err(err) => {
                cache_error(&err, key, cache)?;
                return Ok(Err(err));
            }
        };
        let code =
            executable.serialize().map_err(|_e| CacheError::SerializationError { hash: key.0 })?;
        let serialized = CacheRecord::Code(code).try_to_vec().unwrap();
        cache.put(key.as_ref(), &serialized).map_err(|_io_err| CacheError::WriteError)?;
        match vm.engine.load_universal_executable(&executable) {
            Ok(artifact) => Ok(Ok(Arc::new(artifact) as _)),
            Err(err) => {
                let err = CompilationError::WasmerCompileError { msg: err.to_string() };
                cache_error(&err, key, cache)?;
                Ok(Err(err))
            }
        }
    }

    fn deserialize_wasmer2(
        serialized: &[u8],
        config: &VMConfig,
    ) -> Result<Result<VMArtifact, CompilationError>, CacheError> {
        let _span = tracing::debug_span!(target: "vm", "deserialize_wasmer2").entered();

        let record = CacheRecord::try_from_slice(serialized)
            .map_err(|_e| CacheError::DeserializationError)?;
        let serialized_module = match record {
            CacheRecord::CompileModuleError(err) => return Ok(Err(err)),
            CacheRecord::Code(code) => code,
        };
        unsafe {
            // (UN-)SAFETY: the `serialized_module` must have been produced by a prior call to
            // `serialize`.
            //
            // In practice this is not necessarily true. One could have forgotten to change the
            // cache key when upgrading the version of the wasmer library or the database could
            // have had its data corrupted while at rest.
            //
            // There should definitely be some validation in wasmer to ensure we load what we think
            // we load.
            let artifact = Wasmer2VM::new(config.clone())
                .deserialize(&serialized_module)
                .map_err(|_| CacheError::DeserializationError)?;
            Ok(Ok(artifact))
        }
    }

    fn compile_module_cached_wasmer2_impl(
        key: CryptoHash,
        code: &ContractCode,
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<Result<VMArtifact, CompilationError>, CacheError> {
        let vm = Wasmer2VM::new(config.clone());
        match cache {
            None => Ok(compile_module_wasmer2(&vm, code.code(), config).and_then(|executable| {
                vm.engine
                    .load_universal_executable(&executable)
                    .map(|v| Arc::new(v) as _)
                    .map_err(|err| panic!("could not load the executable: {}", err.to_string()))
            })),
            Some(cache) => {
                let serialized = cache.get(&key.0).map_err(|_io_err| CacheError::ReadError)?;
                match serialized {
                    Some(serialized) => deserialize_wasmer2(serialized.as_slice(), config),
                    None => compile_and_serialize_wasmer2(code.code(), &key, config, cache),
                }
            }
        }
    }

    pub(crate) fn compile_module_cached_wasmer2(
        code: &ContractCode,
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<Result<VMArtifact, CompilationError>, CacheError> {
        let key = get_contract_cache_key(code, VMKind::Wasmer2, config);

        #[cfg(not(feature = "no_cache"))]
        return WASMER2_CACHE.get_or_try_put(key, |key| {
            compile_module_cached_wasmer2_impl(*key, code, config, cache)
        });

        #[cfg(feature = "no_cache")]
        return compile_module_cached_wasmer2_impl(key, code, config, cache);
    }
}

pub fn precompile_contract_vm(
    vm_kind: VMKind,
    wasm_code: &ContractCode,
    config: &VMConfig,
    cache: Option<&dyn CompiledContractCache>,
) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError> {
    let cache = match cache {
        None => return Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable)),
        Some(it) => it,
    };
    let key = get_contract_cache_key(wasm_code, vm_kind, config);
    // Check if we already cached with such a key.
    match cache.get(&key.0).map_err(|_io_error| CacheError::ReadError)? {
        // If so - do not override.
        Some(_) => return Ok(Ok(ContractPrecompilatonResult::ContractAlreadyInCache)),
        None => {}
    };
    match vm_kind {
        #[cfg(all(feature = "wasmer0_vm", target_arch = "x86_64"))]
        VMKind::Wasmer0 => {
            Ok(wasmer0_cache::compile_and_serialize_wasmer(wasm_code.code(), config, &key, cache)?
                .map(|_| ContractPrecompilatonResult::ContractCompiled))
        }
        #[cfg(not(all(feature = "wasmer0_vm", target_arch = "x86_64")))]
        VMKind::Wasmer0 => panic!("Wasmer0 is not enabled!"),
        #[cfg(all(feature = "wasmer2_vm", target_arch = "x86_64"))]
        VMKind::Wasmer2 => {
            Ok(wasmer2_cache::compile_and_serialize_wasmer2(wasm_code.code(), &key, config, cache)?
                .map(|_| ContractPrecompilatonResult::ContractCompiled))
        }
        #[cfg(not(all(feature = "wasmer2_vm", target_arch = "x86_64")))]
        VMKind::Wasmer2 => panic!("Wasmer2 is not enabled!"),
        VMKind::Wasmtime => panic!("Not yet supported"),
    }
}

/// Precompiles contract for the current default VM, and stores result to the cache.
/// Returns `Ok(true)` if compiled code was added to the cache, and `Ok(false)` if element
/// is already in the cache, or if cache is `None`.
pub fn precompile_contract(
    wasm_code: &ContractCode,
    config: &VMConfig,
    current_protocol_version: ProtocolVersion,
    cache: Option<&dyn CompiledContractCache>,
) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError> {
    let _span = tracing::debug_span!(target: "vm", "precompile_contract").entered();
    let vm_kind = VMKind::for_protocol_version(current_protocol_version);
    precompile_contract_vm(vm_kind, wasm_code, config, cache)
}
