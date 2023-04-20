//! Tests that `CompiledContractCache` is working correctly. Currently testing only wasmer code, so disabled outside of x86_64
#![cfg(target_arch = "x86_64")]

use super::{create_context, with_vm_variants, LATEST_PROTOCOL_VERSION};
use crate::internal::VMKind;
use crate::near_vm_runner::NearVM;
use crate::runner::VMResult;
use crate::wasmer2_runner::Wasmer2VM;
use crate::{prepare, MockCompiledContractCache};
use assert_matches::assert_matches;
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::{CompiledContract, CompiledContractCache};
use near_stable_hasher::StableHasher;
use near_vm_compiler::{CpuFeature, Target};
use near_vm_engine::Executable;
use near_vm_errors::VMRunnerError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::VMConfig;
use std::hash::{Hash, Hasher};
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};

#[test]
fn test_caches_compilation_error() {
    let config = VMConfig::test();
    with_vm_variants(&config, |vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 | VMKind::NearVm => {}
            VMKind::Wasmtime => return,
        }
        let cache = MockCompiledContractCache::default();
        let code = [42; 1000];
        let terragas = 1000000000000u64;
        assert_eq!(cache.len(), 0);
        let outcome1 =
            make_cached_contract_call_vm(&config, &cache, &code, "method_name1", terragas, vm_kind)
                .expect("bad failure");
        println!("{:?}", cache);
        assert_eq!(cache.len(), 1);
        let outcome2 =
            make_cached_contract_call_vm(&config, &cache, &code, "method_name2", terragas, vm_kind)
                .expect("bad failure");
        assert_eq!(outcome1.aborted.as_ref(), outcome2.aborted.as_ref());
    })
}

#[test]
fn test_does_not_cache_io_error() {
    let config = VMConfig::test();
    with_vm_variants(&config, |vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 | VMKind::NearVm => {}
            VMKind::Wasmtime => return,
        }

        let code = near_test_contracts::trivial_contract();
        let prepaid_gas = 10u64.pow(12);
        let mut cache = FaultingCompiledContractCache::default();

        cache.set_read_fault(true);
        let result =
            make_cached_contract_call_vm(&config, &cache, &code, "main", prepaid_gas, vm_kind);
        assert_matches!(
            result.err(),
            Some(VMRunnerError::CacheError(near_vm_errors::CacheError::ReadError(_)))
        );

        cache.set_write_fault(true);
        let result =
            make_cached_contract_call_vm(&config, &cache, &code, "main", prepaid_gas, vm_kind);
        assert_matches!(
            result.err(),
            Some(VMRunnerError::CacheError(near_vm_errors::CacheError::WriteError(_)))
        );
    })
}

fn make_cached_contract_call_vm(
    config: &VMConfig,
    cache: &dyn CompiledContractCache,
    code: &[u8],
    method_name: &str,
    prepaid_gas: u64,
    vm_kind: VMKind,
) -> VMResult {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];
    context.prepaid_gas = prepaid_gas;
    let code = ContractCode::new(code.to_vec(), None);
    let runtime = vm_kind.runtime(config.clone()).expect("runtime has not been compiled");
    runtime.run(
        &code,
        method_name,
        &mut fake_external,
        context,
        &fees,
        &promise_results,
        LATEST_PROTOCOL_VERSION,
        Some(cache),
    )
}

#[test]
fn test_wasmer2_artifact_output_stability() {
    use wasmer_engine::Executable;
    // If this test has failed, you want to adjust the necessary constants so that `cache::vm_hash`
    // changes (and only then the hashes here).
    //
    // Note that this test is a best-effort fish net. Some changes that should modify the hash will
    // fall through the cracks here, but hopefully it should catch most of the fish just fine.
    let seeds = [2, 3, 5, 7, 11, 13, 17];
    let prepared_hashes = [
        11313378614122864359,
        5865541421624917606,
        11731917380556063495,
        8000182875575317016,
        3130574445877428311,
        11574598916196339098,
        10719493536745069553,
    ];
    let mut got_prepared_hashes = Vec::with_capacity(seeds.len());
    let compiled_hashes = [
        5254981150840481178,
        15529260255496677612,
        407257192602619216,
        10913823971520273759,
        17423008210698923502,
        7011050181604188333,
        15514788595649734538,
    ];
    let mut got_compiled_hashes = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let contract = ContractCode::new(near_test_contracts::arbitrary_contract(seed), None);

        let config = VMConfig::test();
        let prepared_code =
            prepare::prepare_contract(contract.code(), &config, VMKind::Wasmer2).unwrap();
        let mut hasher = StableHasher::new();
        (&contract.code(), &prepared_code).hash(&mut hasher);
        got_prepared_hashes.push(hasher.finish());

        let mut features = wasmer_compiler::CpuFeature::set();
        features.insert(wasmer_compiler::CpuFeature::AVX);
        let triple = "x86_64-unknown-linux-gnu".parse().unwrap();
        let target = wasmer_compiler::Target::new(triple, features);
        let vm = Wasmer2VM::new_for_target(config, target);
        let artifact = vm.compile_uncached(&contract).unwrap();
        let serialized = artifact.serialize().unwrap();
        let mut hasher = StableHasher::new();
        serialized.hash(&mut hasher);
        let this_hash = hasher.finish();
        got_compiled_hashes.push(this_hash);

        std::fs::write(format!("/tmp/artifact{}", this_hash), serialized).unwrap();
    }
    // These asserts have failed as a result of some change and the following text describes what
    // the implications of the change.
    //
    // May need a protocol version change, and definitely wants a `WASMER2_CONFIG version update
    // too, as below. Maybe something else too.
    assert!(
        got_prepared_hashes == prepared_hashes,
        "contract preparation hashes have changed to {:#?}",
        got_prepared_hashes
    );
    // In this case you will need to adjust the WASMER2_CONFIG version so that the cached contracts
    // are evicted from the contract cache.
    assert!(
        got_compiled_hashes == compiled_hashes,
        "VM output hashes have changed to {:#?}",
        got_compiled_hashes
    );
    // Once it has been confirmed that these steps have been done, the expected hashes in this test
    // can be adjusted.
}

#[test]
fn test_near_vm_artifact_output_stability() {
    // If this test has failed, you want to adjust the necessary constants so that `cache::vm_hash`
    // changes (and only then the hashes here).
    //
    // Note that this test is a best-effort fish net. Some changes that should modify the hash will
    // fall through the cracks here, but hopefully it should catch most of the fish just fine.
    let seeds = [2, 3, 5, 7, 11, 13, 17];
    let prepared_hashes = [
        15237011375120738807,
        3750594434467176559,
        2196541628148102482,
        1576495094908614397,
        6394387219699970793,
        18132026143745992229,
        4095228008100475322,
    ];
    let mut got_prepared_hashes = Vec::with_capacity(seeds.len());
    let compiled_hashes = [
        10351663297260604629,
        13937571770370186345,
        9438649787181538636,
        17513376043726020657,
        8030854424152836681,
        8449792361847063954,
        2655860096455618118,
    ];
    let mut got_compiled_hashes = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let contract = ContractCode::new(near_test_contracts::arbitrary_contract(seed), None);

        let config = VMConfig::test();
        let prepared_code =
            prepare::prepare_contract(contract.code(), &config, VMKind::NearVm).unwrap();
        let mut hasher = StableHasher::new();
        (&contract.code(), &prepared_code).hash(&mut hasher);
        got_prepared_hashes.push(hasher.finish());

        let mut features = CpuFeature::set();
        features.insert(CpuFeature::AVX);
        let triple = "x86_64-unknown-linux-gnu".parse().unwrap();
        let target = Target::new(triple, features);
        let vm = NearVM::new_for_target(config, target);
        let artifact = vm.compile_uncached(&contract).unwrap();
        let serialized = artifact.serialize().unwrap();
        let mut hasher = StableHasher::new();
        serialized.hash(&mut hasher);
        let this_hash = hasher.finish();
        got_compiled_hashes.push(this_hash);

        std::fs::write(format!("/tmp/artifact{}", this_hash), serialized).unwrap();
    }
    // These asserts have failed as a result of some change and the following text describes what
    // the implications of the change.
    //
    // May need a protocol version change, and definitely wants a `WASMER2_CONFIG version update
    // too, as below. Maybe something else too.
    assert!(
        got_prepared_hashes == prepared_hashes,
        "contract preparation hashes have changed to {:#?}",
        got_prepared_hashes
    );
    // In this case you will need to adjust the WASMER2_CONFIG version so that the cached contracts
    // are evicted from the contract cache.
    assert!(
        got_compiled_hashes == compiled_hashes,
        "VM output hashes have changed to {:#?}",
        got_compiled_hashes
    );
    // Once it has been confirmed that these steps have been done, the expected hashes in this test
    // can be adjusted.
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
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> std::io::Result<()> {
        if self.write_fault.swap(false, Ordering::Relaxed) {
            return Err(io::ErrorKind::Other.into());
        }
        self.inner.put(key, value)
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContract>> {
        if self.read_fault.swap(false, Ordering::Relaxed) {
            return Err(io::ErrorKind::Other.into());
        }
        self.inner.get(key)
    }
}
