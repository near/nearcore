use crate::errors::IntoVMError;
use crate::prepare;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::CacheError::{DeserializationError, ReadError, SerializationError, WriteError};
use near_vm_errors::{CacheError, VMError};
use near_vm_logic::{VMConfig, VMKind};
use std::convert::TryFrom;
use wasmer_runtime::{compiler_for_backend, Backend};
use wasmer_runtime_core::cache::Artifact;
use wasmer_runtime_core::load_cache_with;
use delay_detector::DelayDetector;

pub(crate) fn compile_module(
    code: &[u8],
    config: &VMConfig,
) -> Result<wasmer_runtime::Module, VMError> {
    let prepared_code = prepare::prepare_contract(code, config)?;
    wasmer_runtime::compile(&prepared_code).map_err(|err| err.into_vm_error())
}

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

fn compile_and_serialize_wasmer(
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
    let mut d = DelayDetector::new("in deserialize_wasmer get cache".into());
    let artifact = Artifact::deserialize(serialized_artifact.as_slice())
        .map_err(|_e| CacheError::DeserializationError)?;
    d.snapshot("after deserialize artifact");
    let r = unsafe {
        let compiler = compiler_for_backend(Backend::Singlepass).unwrap();
        match load_cache_with(artifact, compiler.as_ref()) {
            Ok(module) => Ok(Ok(module)),
            Err(_) => Err(CacheError::DeserializationError),
        }
    };
    d.snapshot("after load_cache_with");
    r
}

pub(crate) fn compile_module_cached_wasmer(
    wasm_code_hash: &[u8],
    wasm_code: &[u8],
    config: &VMConfig,
    cache: Option<&dyn CompiledContractCache>,
) -> Result<wasmer_runtime::Module, VMError> {
    /* Consider adding `|| cfg!(feature = "no_cache")` */
    if cache.is_none() {
        return compile_module(wasm_code, config);
    }
    let key = get_key(wasm_code_hash, wasm_code, VMKind::Wasmer, config);
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
