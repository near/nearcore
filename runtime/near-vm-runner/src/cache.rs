use crate::errors::IntoVMError;
use crate::prepare;
use near_primitives::borsh::BorshSerialize;
use near_primitives::hash::CryptoHash;
use near_vm_errors::VMError;
use near_vm_logic::VMConfig;
use std::fmt;
use wasmer_runtime::{compiler_for_backend, Backend};
use wasmer_runtime_core::cache::Artifact;
use wasmer_runtime_core::load_cache_with;

pub(crate) fn compile_module(
    _code_and_config_hash: Vec<u8>,
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let prepared_code = prepare::prepare_contract(code, config)?;
    wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
}

pub fn get_hash(code: &[u8], config: &VMConfig) -> CryptoHash {
    near_primitives::hash::hash(&[code, &config.non_crypto_hash().to_le_bytes()].concat())
}

pub(crate) fn compile_module_cached(
    code_hash: &[u8],
    code: &[u8],
    config: &VMConfig,
    cache: Option<&dyn CompiledContractCache>,
) -> Result<wasmer_runtime::Module, VMError> {
    let crypto_hash = get_hash(code_hash, config);
    let hash = crypto_hash.try_to_vec().unwrap();
    /* Consider adding `|| cfg!(feature = "no_cache")` */
    if cache.is_none() {
        return compile_module(hash.clone(), code, config);
    }
    let cache = cache.unwrap();
    if let Some(serialized) = cache
        .get(&hash)
        .map_err(|_e| VMError::CacheError(format!("Cannot read from cache {:?}", &code_hash)))?
    {
        let artifact = Artifact::deserialize(&serialized).map_err(|_e| {
            VMError::CacheError(format!("Cannot deserialize from cache {:?}", &code_hash))
        })?;
        unsafe {
            load_cache_with(artifact, compiler_for_backend(Backend::Singlepass).unwrap().as_ref())
                .map_err(|_e| {
                    VMError::CacheError(format!("Cannot read from cache {:?}", &code_hash))
                })
        }
    } else {
        // TODO: cache compilation errors as well. Currently it's possible to deploy invalid code,
        // and compile on every call
        let compiled: wasmer_runtime::Module = compile_module(hash.clone(), code, config)?;
        let artifact = compiled.cache().map_err(|_e| {
            VMError::CacheError(format!("Cannot make cached artifact for {:?}", &code_hash))
        })?;

        let serialized = artifact.serialize().map_err(|_e| {
            VMError::CacheError(format!("Cannot serialize cached artifact for {:?}", &code_hash))
        })?;
        cache.put(&hash, &serialized).map_err(|_e| {
            VMError::CacheError(format!("Error saving cached artifact for {:?}", &code_hash))
        })?;
        Ok(compiled)
    }
}

/// Cache for compiled modules
pub trait CompiledContractCache: Send + Sync {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error>;
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error>;
}

impl CompiledContractCache for () {
    fn put(&self, _: &[u8], _: &[u8]) -> Result<(), std::io::Error> {
        Ok(())
    }
    fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        Ok(None)
    }
}

impl fmt::Debug for dyn CompiledContractCache {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Compiled contracts cache")
    }
}
