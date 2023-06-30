use crate::errors::ContractPrecompilatonResult;
use crate::logic::errors::{CacheError, CompilationError};
use crate::logic::{CompiledContract, CompiledContractCache, ProtocolVersion, VMConfig};
use crate::vm_kind::VMKind;
use borsh::BorshSerialize;
use near_primitives_core::contract::ContractCode;
use near_primitives_core::hash::CryptoHash;
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
    CryptoHash::hash_borsh(key)
}

#[derive(Default)]
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
}

impl fmt::Debug for MockCompiledContractCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let guard = self.store.lock().unwrap();
        let hm: &HashMap<_, _> = &*guard;
        fmt::Debug::fmt(hm, f)
    }
}

/// Precompiles contract for the current default VM, and stores result to the cache.
/// Returns `Ok(true)` if compiled code was added to the cache, and `Ok(false)` if element
/// is already in the cache, or if cache is `None`.
pub fn precompile_contract(
    code: &ContractCode,
    config: &VMConfig,
    current_protocol_version: ProtocolVersion,
    cache: Option<&dyn CompiledContractCache>,
) -> Result<Result<ContractPrecompilatonResult, CompilationError>, CacheError> {
    let _span = tracing::debug_span!(target: "vm", "precompile_contract").entered();
    let vm_kind = VMKind::for_protocol_version(current_protocol_version);
    let runtime = vm_kind
        .runtime(config.clone())
        .unwrap_or_else(|| panic!("the {vm_kind:?} runtime has not been enabled at compile time"));
    let cache = match cache {
        Some(it) => it,
        None => return Ok(Ok(ContractPrecompilatonResult::CacheNotAvailable)),
    };
    let key = get_contract_cache_key(code, vm_kind, config);
    // Check if we already cached with such a key.
    if cache.has(&key).map_err(CacheError::ReadError)? {
        return Ok(Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
    }
    runtime.precompile(code, cache)
}
