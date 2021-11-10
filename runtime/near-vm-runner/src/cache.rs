use crate::errors::ContractPrecompilatonResult;
use crate::prepare;
use crate::wasmer2_runner::{default_wasmer2_store, wasmer2_vm_hash};
use crate::wasmer_runner::wasmer0_vm_hash;
use crate::wasmtime_runner::wasmtime_vm_hash;
use crate::VMKind;
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(not(feature = "no_cache"))]
use cached::{cached_key, SizedCache};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::{CacheError, CompilationError, FunctionCallError, VMError};
use near_vm_logic::{ProtocolVersion, VMConfig};
use std::collections::HashMap;
use std::fmt;
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

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
enum CacheRecord {
    CompileModuleError(CompilationError),
    Code(Vec<u8>),
}

fn vm_hash(vm_kind: VMKind) -> u64 {
    match vm_kind {
        VMKind::Wasmer0 => wasmer0_vm_hash(),
        VMKind::Wasmer2 => wasmer2_vm_hash(),
        VMKind::Wasmtime => wasmtime_vm_hash(),
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

#[cfg(not(feature = "no_cache"))]
const CACHE_SIZE: usize = 128;

#[cfg(feature = "wasmer0_vm")]
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

    #[cfg(not(feature = "no_cache"))]
    cached_key! {
        MODULES: SizedCache<CryptoHash, Result<Result<wasmer_runtime::Module, CompilationError>, CacheError>>
            = SizedCache::with_size(CACHE_SIZE);
        Key = {
            key
        };

        fn memcache_compile_module_cached_wasmer(
            key: CryptoHash,
            wasm_code: &[u8],
            config: &VMConfig,
            cache: Option<&dyn CompiledContractCache>
        ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> = {
            compile_module_cached_wasmer_impl(key, wasm_code, config, cache)
        }
    }

    pub(crate) fn compile_module_cached_wasmer0(
        code: &ContractCode,
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<Result<wasmer_runtime::Module, CompilationError>, CacheError> {
        let key = get_contract_cache_key(code, VMKind::Wasmer0, config);
        #[cfg(not(feature = "no_cache"))]
        return memcache_compile_module_cached_wasmer(key, code.code(), config, cache);
        #[cfg(feature = "no_cache")]
        return compile_module_cached_wasmer_impl(key, code.code(), config, cache);
    }
}

#[cfg(feature = "wasmer2_vm")]
pub mod wasmer2_cache {
    use near_primitives::contract::ContractCode;

    use super::*;

    fn compile_module_wasmer2(
        code: &[u8],
        config: &VMConfig,
        store: &wasmer::Store,
    ) -> Result<wasmer::Module, CompilationError> {
        let _span = tracing::debug_span!(target: "vm", "compile_module_wasmer2").entered();

        let prepared_code =
            prepare::prepare_contract(code, config).map_err(CompilationError::PrepareError)?;
        wasmer::Module::new(&store, prepared_code).map_err(|err| match err {
            wasmer::CompileError::Wasm(_) => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
            wasmer::CompileError::Codegen(_) => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
            wasmer::CompileError::Validate(_) => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
            wasmer::CompileError::UnsupportedFeature(_) => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
            wasmer::CompileError::UnsupportedTarget(_) => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
            wasmer::CompileError::Resource(_) => {
                CompilationError::WasmerCompileError { msg: err.to_string() }
            }
        })
    }

    pub(crate) fn compile_and_serialize_wasmer2(
        wasm_code: &[u8],
        key: &CryptoHash,
        config: &VMConfig,
        cache: &dyn CompiledContractCache,
        store: &wasmer::Store,
    ) -> Result<Result<wasmer::Module, CompilationError>, CacheError> {
        let _span = tracing::debug_span!(target: "vm", "compile_and_serialize_wasmer2").entered();

        let module = match compile_module_wasmer2(wasm_code, config, store) {
            Ok(module) => module,
            Err(err) => {
                cache_error(&err, key, cache)?;
                return Ok(Err(err));
            }
        };

        let code =
            module.serialize().map_err(|_e| CacheError::SerializationError { hash: key.0 })?;
        let serialized = CacheRecord::Code(code).try_to_vec().unwrap();
        cache.put(key.as_ref(), &serialized).map_err(|_io_err| CacheError::WriteError)?;
        Ok(Ok(module))
    }

    fn deserialize_wasmer2(
        serialized: &[u8],
        store: &wasmer::Store,
    ) -> Result<Result<wasmer::Module, CompilationError>, CacheError> {
        let _span = tracing::debug_span!(target: "vm", "deserialize_wasmer2").entered();

        let record = CacheRecord::try_from_slice(serialized)
            .map_err(|_e| CacheError::DeserializationError)?;
        let serialized_module = match record {
            CacheRecord::CompileModuleError(err) => return Ok(Err(err)),
            CacheRecord::Code(code) => code,
        };
        unsafe {
            Ok(Ok(wasmer::Module::deserialize(store, serialized_module.as_slice())
                .map_err(|_e| CacheError::DeserializationError)?))
        }
    }

    fn compile_module_cached_wasmer2_impl(
        key: CryptoHash,
        wasm_code: &[u8],
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
        store: &wasmer::Store,
    ) -> Result<Result<wasmer::Module, CompilationError>, CacheError> {
        match cache {
            None => Ok(compile_module_wasmer2(wasm_code, config, store)),
            Some(cache) => {
                let serialized = cache.get(&key.0).map_err(|_io_err| CacheError::WriteError)?;
                match serialized {
                    Some(serialized) => deserialize_wasmer2(serialized.as_slice(), store),
                    None => compile_and_serialize_wasmer2(wasm_code, &key, config, cache, store),
                }
            }
        }
    }

    #[cfg(not(feature = "no_cache"))]
    cached_key! {
        MODULES: SizedCache<CryptoHash, Result<Result<wasmer::Module, CompilationError>, CacheError>>
            = SizedCache::with_size(CACHE_SIZE);
        Key = {
            key
        };

        fn memcache_compile_module_cached_wasmer2(
            key: CryptoHash,
            wasm_code: &[u8],
            config: &VMConfig,
            cache: Option<&dyn CompiledContractCache>,
            store: &wasmer::Store
        ) -> Result<Result<wasmer::Module, CompilationError>, CacheError> = {
            compile_module_cached_wasmer2_impl(key, wasm_code, config, cache, store)
        }
    }

    pub(crate) fn compile_module_cached_wasmer2(
        code: &ContractCode,
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
        store: &wasmer::Store,
    ) -> Result<Result<wasmer::Module, CompilationError>, CacheError> {
        let key = get_contract_cache_key(code, VMKind::Wasmer2, config);
        #[cfg(not(feature = "no_cache"))]
        return memcache_compile_module_cached_wasmer2(key, &code.code(), config, cache, store);
        #[cfg(feature = "no_cache")]
        return compile_module_cached_wasmer2_impl(key, &code.code(), config, cache, store);
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
    let res = match vm_kind {
        VMKind::Wasmer0 => {
            wasmer0_cache::compile_and_serialize_wasmer(wasm_code.code(), config, &key, cache)?
                .map(|_module| ())
        }
        VMKind::Wasmer2 => {
            let store = default_wasmer2_store();
            wasmer2_cache::compile_and_serialize_wasmer2(
                wasm_code.code(),
                &key,
                config,
                cache,
                &store,
            )?
            .map(|_module| ())
        }
        VMKind::Wasmtime => {
            panic!("Not yet supported")
        }
    };
    Ok(res.map(|()| ContractPrecompilatonResult::ContractCompiled))
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
    let vm_kind = VMKind::for_protocol_version(current_protocol_version);
    precompile_contract_vm(vm_kind, wasm_code, config, cache)
}
