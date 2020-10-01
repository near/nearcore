#[cfg(not(feature = "no_cache"))]
use cached::{cached_key, SizedCache};

use crate::errors::IntoVMError;
use crate::prepare;
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
    MODULES: SizedCache<Vec<u8>, Result<wasmer_runtime::Module, VMError>>
        = SizedCache::with_size(CACHE_SIZE);
    Key = {
        (code_and_config_hash)
    };

    fn compile_module(code_and_config_hash: Vec<u8>, code: &[u8], config: &VMConfig
        ) -> Result<wasmer_runtime::Module, VMError> = {
        let prepared_code = prepare::prepare_contract(code, config)?;
        wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
    }
}

#[cfg(feature = "no_cache")]
pub(crate) fn compile_module(
    _code_and_config_hash: Vec<u8>,
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let prepared_code = prepare::prepare_contract(code, config)?;
    wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
}

pub fn get_hash(code_hash: &[u8], config: &VMConfig) -> Vec<u8> {
    use sha2::digest::{Digest, FixedOutput};
    let mut digest = sha2::Sha256::new();
    digest.input(code_hash);
    // TODO: save hash in config, don't recompute every time
    digest.input(config.non_crypto_hash().to_le_bytes());
    digest.fixed_result().to_vec()
}

pub(crate) fn compile_module_cached(
    code_hash: &[u8],
    code: &[u8],
    config: &VMConfig,
    cache: &dyn WasmCompileCache,
) -> Result<wasmer_runtime::Module, VMError> {
    let hash = get_hash(code_hash, config);
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
pub trait WasmCompileCache {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error>;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error>;
}

impl WasmCompileCache for () {
    fn put(&self, _: &[u8], _: &[u8]) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn get(&self, _: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        Ok(None)
    }
}
