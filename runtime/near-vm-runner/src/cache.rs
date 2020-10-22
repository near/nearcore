use crate::errors::IntoVMError;
use crate::prepare;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_vm_errors::CacheError::{DeserializationError, SerializationError, WriteError};
use near_vm_errors::VMError;
use near_vm_logic::VMConfig;
use std::fmt;
use wasmer_runtime::{compiler_for_backend, Backend};
use wasmer_runtime_core::cache::Artifact;
use wasmer_runtime_core::load_cache_with;
use std::convert::TryFrom;

pub(crate) fn compile_module(
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let prepared_code = prepare::prepare_contract(code, config)?;
    wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
}

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
enum ContractCacheKey {
    Version1 {
        code_hash: CryptoHash,
        vm_config_non_crypto_hash: u64,
    }
}

#[derive(Debug, Clone, BorshDeserialize, BorshSerialize)]
enum CacheRecord {
    Error(VMError),
    Code(Vec<u8>),
}

fn get_key(code_hash: &[u8], code: &[u8], config: &VMConfig) -> Vec<u8> {
    let hash = if code_hash.is_empty() {
        near_primitives::hash::hash(code)
    } else {
        CryptoHash::try_from(code_hash).unwrap()
    };
    let key = ContractCacheKey::Version1 {
        code_hash: hash,
        vm_config_non_crypto_hash: config.non_crypto_hash(),
    };
    key.try_to_vec().unwrap()
}

pub(crate) fn compile_module_cached(
    code_hash: &[u8],
    code: &[u8],
    config: &VMConfig,
    cache: Option<&dyn CompiledContractCache>,
) -> Result<wasmer_runtime::Module, VMError> {
    // Sometimes caller doesn't compute code_hash, so always hash the code ourselves.
    let key = get_key(code_hash, code, config);
    /* Consider adding `|| cfg!(feature = "no_cache")` */
    if cache.is_none() {
        return compile_module(code, config);
    }
    let cache = cache.unwrap();
    match cache.get(key.as_slice()) {
        Ok(serialized) => {
            match serialized {
                Some(serialized) => {
                    // We got cached code or error from DB cache.
                    let record = CacheRecord::try_from_slice(serialized.as_slice()).unwrap();
                    let code = match record {
                        CacheRecord::Error(err) => return Err(err),
                        CacheRecord::Code(code) => code,
                    };
                    let artifact = Artifact::deserialize(code.as_slice())
                        .map_err(|_e| VMError::CacheError(DeserializationError))?;
                    unsafe {
                        load_cache_with(artifact, compiler_for_backend(Backend::Singlepass).unwrap().as_ref())
                            .map_err(|_e| VMError::CacheError(DeserializationError))
                    }
                }
                None => {
                    // Nothing found in cache, create new record.
                    let compiled: wasmer_runtime::Module =
                        compile_module(code, config).map_err(|e| {
                            let record = CacheRecord::Error(e.clone());
                            let e1 = cache.put(key.as_slice(), &record.try_to_vec().unwrap());
                            if e1.is_err() {
                                // That's fine, just cannot cache compilation error.
                                println!("Cannot cache an error");
                            }
                            e
                        })?;

                    let artifact = compiled.cache().map_err(|_e| {
                        let e = VMError::CacheError(SerializationError);
                        let record = CacheRecord::Error(e.clone());
                        let e1 = cache.put(key.as_slice(), &record.try_to_vec().unwrap());
                        if e1.is_err() {
                            // That's fine, just cannot cache compilation error.
                            println!("Cannot cache an error");
                        }
                        e
                    })?;

                    let code = artifact.serialize().map_err(|_e| {
                        let e = VMError::CacheError(SerializationError);
                        let record = CacheRecord::Error(e.clone());
                        cache.put(key.as_slice(), &record.try_to_vec().unwrap()).unwrap();
                        e
                    })?;
                    let record = CacheRecord::Code(code);
                    cache
                        .put(key.as_slice(), &record.try_to_vec().unwrap())
                        .map_err(|_e| VMError::CacheError(WriteError))?;
                    Ok(compiled)
                }
            }
        }
        Err(_) => {
            // Cache access error happened, avoid attempts to cache.
            println!("Cannot use cache");
            return compile_module(code, config);
        }
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
