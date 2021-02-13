use crate::errors::IntoVMError;
use crate::prepare;
use borsh::{BorshDeserialize, BorshSerialize};
#[cfg(not(feature = "no_cache"))]
use cached::{cached_key, SizedCache};
use near_primitives::hash::CryptoHash;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::CacheError::{DeserializationError, ReadError, SerializationError, WriteError};
use near_vm_errors::{CacheError, VMError};
use near_vm_logic::{VMConfig, VMKind};
use std::convert::TryFrom;

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
enum ContractCacheKey {
    Version1 { code_hash: CryptoHash, vm_config_non_crypto_hash: u64, vm_kind: VMKind },
}

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
enum CacheRecord {
    Error(VMError),
    Code(Vec<u8>),
}

fn get_key(code_hash: &[u8], code: &[u8], vm_kind: VMKind, config: &VMConfig) -> CryptoHash {
    let hash = match CryptoHash::try_from(code_hash) {
        Ok(hash) => hash,
        // Sometimes caller doesn't compute code_hash, so hash the code ourselves.
        Err(_e) => near_primitives::hash::hash(code),
    };
    let key = ContractCacheKey::Version1 {
        code_hash: hash,
        vm_config_non_crypto_hash: config.non_crypto_hash(),
        vm_kind: vm_kind,
    };
    near_primitives::hash::hash(&key.try_to_vec().unwrap())
}

fn cache_error(error: VMError, key: &CryptoHash, cache: &dyn CompiledContractCache) -> VMError {
    let record = CacheRecord::Error(error.clone());
    if cache.put(&(key.0).0, &record.try_to_vec().unwrap()).is_err() {
        VMError::CacheError(WriteError)
    } else {
        error
    }
}

#[cfg(feature = "wasmer0_vm")]
pub mod wasmer0_cache {
    use wasmer_runtime::{compiler_for_backend, Backend};
    use wasmer_runtime_core::cache::Artifact;
    use wasmer_runtime_core::load_cache_with;
    use super::*;

    pub(crate) fn compile_module(
        code: &[u8],
        config: &VMConfig,
    ) -> Result<wasmer_runtime::Module, VMError> {
        let prepared_code = prepare::prepare_contract(code, config)?;
        wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
    }

    pub(crate) fn compile_and_serialize_wasmer(
        wasm_code: &[u8],
        config: &VMConfig,
        key: &CryptoHash,
        cache: &dyn CompiledContractCache,
    ) -> Result<wasmer_runtime::Module, VMError> {
        let module = compile_module(wasm_code, config).map_err(|e| cache_error(e, &key, cache))?;
        let artifact =
            module.cache().map_err(|_e| VMError::CacheError(SerializationError { hash: (key.0).0 }))?;
        let code = artifact
            .serialize()
            .map_err(|_e| VMError::CacheError(SerializationError { hash: (key.0).0 }))?;
        let serialized = CacheRecord::Code(code).try_to_vec().unwrap();
        cache.put(key.as_ref(), &serialized).map_err(|_e| VMError::CacheError(WriteError))?;
        Ok(module)
    }

    /// Deserializes contract or error from the binary data. Signature means that we could either
    /// return module or cached error, which both considered to be `Ok()`, or encounter an error during
    /// the deserialization process.
    fn deserialize_wasmer(
        serialized: &[u8],
    ) -> Result<Result<wasmer_runtime::Module, VMError>, CacheError> {
        let record = CacheRecord::try_from_slice(serialized).map_err(|_e| DeserializationError)?;
        let serialized_artifact = match record {
            CacheRecord::Error(err) => return Ok(Err(err)),
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
    ) -> Result<wasmer_runtime::Module, VMError> {
        if cache.is_none() {
            return compile_module(wasm_code, config);
        }

        let cache = cache.unwrap();
        match cache.get(&(key.0).0) {
            Ok(serialized) => match serialized {
                Some(serialized) => {
                    deserialize_wasmer(serialized.as_slice()).map_err(VMError::CacheError)?
                }
                None => compile_and_serialize_wasmer(wasm_code, config, &key, cache),
            },
            Err(_) => Err(VMError::CacheError(ReadError)),
        }
    }

    #[cfg(not(feature = "no_cache"))]
    const CACHE_SIZE: usize = 128;

    #[cfg(not(feature = "no_cache"))]
    cached_key! {
        MODULES: SizedCache<CryptoHash, Result<wasmer_runtime::Module, VMError>>
            = SizedCache::with_size(CACHE_SIZE);
        Key = {
            key
        };

        fn memcache_compile_module_cached_wasmer(
            key: CryptoHash,
            wasm_code: &[u8],
            config: &VMConfig,
            cache: Option<&dyn CompiledContractCache>) -> Result<wasmer_runtime::Module, VMError> = {
            compile_module_cached_wasmer_impl(key, wasm_code, config, cache)
        }
    }

    pub(crate) fn compile_module_cached_wasmer(
        wasm_code_hash: &[u8],
        wasm_code: &[u8],
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
    ) -> Result<wasmer_runtime::Module, VMError> {
        let key = get_key(wasm_code_hash, wasm_code, VMKind::Wasmer0, config);
        #[cfg(not(feature = "no_cache"))]
            return memcache_compile_module_cached_wasmer(key, wasm_code, config, cache);
        #[cfg(feature = "no_cache")]
            return compile_module_cached_wasmer_impl(key, wasm_code, config, cache);
    }
}

#[cfg(feature = "wasmer1_vm")]
pub mod wasmer1_cache {
    use super::*;

    pub(crate) fn compile_module_cached_wasmer1(
        wasm_code_hash: &[u8],
        wasm_code: &[u8],
        config: &VMConfig,
        cache: Option<&dyn CompiledContractCache>,
        store: &wasmer::Store,
    ) -> Result<wasmer::Module, VMError> {
        let key = get_key(wasm_code_hash, wasm_code, VMKind::Wasmer1, config);
        return compile_module_cached_wasmer1_impl(key, wasm_code, cache, store);
    }

    fn compile_module_wasmer1(
        prepared_code: &[u8],
        store: &wasmer::Store,
    ) -> Result<wasmer::Module, VMError> {
        wasmer::Module::new(&store, prepared_code).map_err(|err| err.into_vm_error())
    }

    pub(crate) fn compile_and_serialize_wasmer1(
        wasm_code: &[u8],
        key: &CryptoHash,
        cache: &dyn CompiledContractCache,
        store: &wasmer::Store,
    ) -> Result<wasmer::Module, VMError> {
        let module =
            compile_module_wasmer1(wasm_code, store).map_err(|e| cache_error(e, &key, cache))?;
        let code = module
            .serialize()
            .map_err(|_e| VMError::CacheError(SerializationError { hash: (key.0).0 }))?;
        let serialized = CacheRecord::Code(code).try_to_vec().unwrap();
        cache.put(key.as_ref(), &serialized).map_err(|_e| VMError::CacheError(WriteError))?;
        Ok(module)
    }

    fn deserialize_wasmer1(
        serialized: &[u8],
        store: &wasmer::Store,
    ) -> Result<Result<wasmer::Module, VMError>, CacheError> {
        let record = CacheRecord::try_from_slice(serialized).map_err(|_e| DeserializationError)?;
        let serialized_module = match record {
            CacheRecord::Error(err) => return Ok(Err(err)),
            CacheRecord::Code(code) => code,
        };
        unsafe {
            Ok(Ok(wasmer::Module::deserialize(store, serialized_module.as_slice())
                .map_err(|_e| CacheError::DeserializationError)?))
        }
    }

    fn compile_module_cached_wasmer1_impl(
        key: CryptoHash,
        wasm_code: &[u8],
        cache: Option<&dyn CompiledContractCache>,
        store: &wasmer::Store,
    ) -> Result<wasmer::Module, VMError> {
        if cache.is_none() {
            return compile_module_wasmer1(wasm_code, store);
        }

        let cache = cache.unwrap();
        match cache.get(&(key.0).0) {
            Ok(serialized) => match serialized {
                Some(serialized) => {
                    deserialize_wasmer1(serialized.as_slice(), store).map_err(VMError::CacheError)?
                }
                None => compile_and_serialize_wasmer1(wasm_code, &key, cache, store),
            },
            Err(_) => Err(VMError::CacheError(ReadError)),
        }
    }
}
