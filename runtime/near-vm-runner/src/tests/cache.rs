//! Tests that `CompiledContractCache` is working correctly. Currently testing only wasmer code, so disabled outside of x86_64
#![cfg(target_arch = "x86_64")]

use super::{create_context, with_vm_variants, LATEST_PROTOCOL_VERSION};
use crate::internal::VMKind;
use crate::runner::VMResult;
use crate::wasmer2_runner::Wasmer2VM;
use crate::{prepare, MockCompiledContractCache};
use assert_matches::assert_matches;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_stable_hasher::StableHasher;
use near_vm_errors::VMError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::VMConfig;
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use wasmer_compiler::{CpuFeature, Target};
use wasmer_engine::Executable;

#[test]
fn test_caches_compilation_error() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            VMKind::Wasmtime => return,
        }
        let cache = MockCompiledContractCache::default();
        let code = [42; 1000];
        let terragas = 1000000000000u64;
        assert_eq!(cache.len(), 0);
        let err1 = make_cached_contract_call_vm(&cache, &code, "method_name1", terragas, vm_kind);
        println!("{:?}", cache);
        assert_eq!(cache.len(), 1);
        let err2 = make_cached_contract_call_vm(&cache, &code, "method_name2", terragas, vm_kind);
        assert_eq!(err1, err2);
    })
}

#[test]
fn test_does_not_cache_io_error() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            VMKind::Wasmtime => return,
        }

        let code = near_test_contracts::trivial_contract();
        let prepaid_gas = 10u64.pow(12);
        let mut cache = FaultingCompiledContractCache::default();

        cache.set_read_fault(true);
        let result = make_cached_contract_call_vm(&cache, &code, "main", prepaid_gas, vm_kind);
        assert!(result.outcome().is_none());
        assert_matches!(
            result.error(),
            Some(&VMError::CacheError(near_vm_errors::CacheError::ReadError))
        );

        cache.set_write_fault(true);
        let result = make_cached_contract_call_vm(&cache, &code, "main", prepaid_gas, vm_kind);
        assert!(result.outcome().is_none());
        assert_matches!(
            result.error(),
            Some(&VMError::CacheError(near_vm_errors::CacheError::WriteError))
        );
    })
}

fn make_cached_contract_call_vm(
    cache: &dyn CompiledContractCache,
    code: &[u8],
    method_name: &str,
    prepaid_gas: u64,
    vm_kind: VMKind,
) -> VMResult {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    let config = VMConfig::test();
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];
    context.prepaid_gas = prepaid_gas;
    let code = ContractCode::new(code.to_vec(), None);
    let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");
    runtime.run(
        &code,
        method_name,
        &mut fake_external,
        context.clone(),
        &fees,
        &promise_results,
        LATEST_PROTOCOL_VERSION,
        Some(cache),
    )
}

#[test]
fn test_wasmer2_artifact_output_stability() {
    // If this test has failed, you want to adjust the necessary constants so that `cache::vm_hash`
    // changes (and only then the hashes here).
    //
    // Note that this test is a best-effort fish net. Some changes that should modify the hash will
    // fall through the cracks here, but hopefully it should catch most of the fish just fine.
    let mut hasher = StableHasher::new();
    let code = near_test_contracts::trivial_contract();
    let config = VMConfig::test();
    let prepared_code = prepare::prepare_contract(code, &config).unwrap();
    let mut features = CpuFeature::set();
    features.insert(CpuFeature::AVX);
    let triple = "x86_64-unknown-linux-gnu".parse().unwrap();
    let target = Target::new(triple, features);
    let vm = Wasmer2VM::new_for_target(config, target);
    let executable = vm.compile_uncached(&prepared_code).unwrap();
    let serialized = executable.serialize().unwrap();
    serialized.hash(&mut hasher);
    assert_eq!(hasher.finish(), 2128441495928970645, "WASMER2_CONFIG needs version change");
}

/// [`CompiledContractCache`] which simulates failures in the underlying
/// database.
#[derive(Default)]
struct FaultingCompiledContractCache {
    read_fault: AtomicBool,
    write_fault: AtomicBool,
    inner: MockCompiledContractCache,
}

impl FaultingCompiledContractCache {
    fn set_read_fault(&mut self, yes: bool) {
        *self.read_fault.get_mut() = yes;
    }

    fn set_write_fault(&mut self, yes: bool) {
        *self.write_fault.get_mut() = yes;
    }
}

impl CompiledContractCache for FaultingCompiledContractCache {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), io::Error> {
        if self.write_fault.swap(false, Ordering::Relaxed) {
            return Err(io::ErrorKind::Other.into());
        }
        self.inner.put(key, value)
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, io::Error> {
        if self.read_fault.swap(false, Ordering::Relaxed) {
            return Err(io::ErrorKind::Other.into());
        }
        self.inner.get(key)
    }
}
