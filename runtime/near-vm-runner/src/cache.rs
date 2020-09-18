#[cfg(not(feature = "no_cache"))]
use cached::{cached_key, SizedCache};

use crate::errors::IntoVMError;
use crate::prepare;
use near_store::current_store;
use near_store::DBCol::ColCachedContractCode;
use near_vm_errors::VMError;
use near_vm_logic::VMConfig;
use wasmer_runtime::{compiler_for_backend, Backend};
use wasmer_runtime_core::cache::Artifact;
use wasmer_runtime_core::load_cache_with;

/// Cache size in number of cached modules to hold.
#[cfg(not(feature = "no_cache"))]
const CACHE_SIZE: usize = 128;
// TODO: store a larger on-disk cache

#[cfg(not(feature = "no_cache"))]
cached_key! {
    MODULES: SizedCache<(Vec<u8>, u64), Result<wasmer_runtime::Module, VMError>>
        = SizedCache::with_size(CACHE_SIZE);
    Key = {
        (code_hash, config.non_crypto_hash())
    };

    fn compile_module(code_hash: Vec<u8>, code: &[u8], config: &VMConfig
        ) -> Result<wasmer_runtime::Module, VMError> = {
        let prepared_code = prepare::prepare_contract(code, config)?;
        wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
    }
}

#[cfg(feature = "no_cache")]
pub(crate) fn compile_module(
    _code_hash: Vec<u8>,
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let prepared_code = prepare::prepare_contract(code, config)?;
    wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
}

pub(crate) fn put_module(code: &[u8], config: &VMConfig) -> Result<Vec<u8>, VMError> {
    use sha2::Digest;
    let code_hash = sha2::Sha256::digest(&code).to_vec();

    if let Ok(Some(_)) = current_store().get(ColCachedContractCode, &code_hash) {
        return Ok(code_hash);
    }

    let compiled: wasmer_runtime::Module = match compile_module(code_hash.clone(), code, config) {
        Ok(module) => module,
        Err(err) => return Err(err),
    };
    let cached = match compiled.cache() {
        Ok(cached) => cached,
        Err(_e) => {
            return Err(VMError::CacheError(format!(
                "Cannot make cached artifact for {:?}",
                &code_hash
            )))
        }
    };
    let buffer = match cached.serialize() {
        Ok(buffer) => buffer,
        Err(_e) => {
            return Err(VMError::CacheError(format!(
                "Cannot serialize cached artifact for {:?}",
                &code_hash
            )))
        }
    };

    let mut store_update = current_store().store_update();
    store_update.set(ColCachedContractCode, &code_hash, &buffer);
    if let Err(_e) = store_update.commit() {
        return Err(VMError::CacheError(format!("Cannot cache {:?}", &code_hash)));
    }

    Ok(code_hash.clone())
}

pub(crate) fn get_module(
    code_hash: Vec<u8>,
    _config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let bytes = match current_store().get(ColCachedContractCode, &code_hash) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => {
            return Err(VMError::CacheError(format!("Cache doesn't exist for {:?}", &code_hash)))
        }
        Err(_e) => return Err(VMError::CacheError(format!("Error to get cache {:?}", &code_hash))),
    };
    let cache = match Artifact::deserialize(bytes.as_slice()) {
        Ok(cache) => cache,
        Err(_e) => {
            return Err(VMError::CacheError(format!(
                "Cannot deserialize from cache {:?}",
                &code_hash
            )))
        }
    };
    unsafe {
        return match load_cache_with(
            cache,
            compiler_for_backend(Backend::Singlepass).unwrap().as_ref(),
        ) {
            Ok(module) => Ok(module),
            Err(_e) => Err(VMError::CacheError(format!("Cannot read from cache {:?}", &code_hash))),
        };
    }
}

pub(crate) fn compile_module_cached(
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let code_hash = put_module(code, config)?;
    get_module(code_hash, config)
}
