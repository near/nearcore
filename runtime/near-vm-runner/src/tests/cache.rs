//! Tests that `ContractRuntimeCache` is working correctly. Currently testing only wasmer code, so disabled outside of x86_64
#![cfg(target_arch = "x86_64")]

use super::{create_context, test_vm_config, with_vm_variants};
use crate::cache::{CompiledContractInfo, ContractRuntimeCache};
use crate::logic::errors::VMRunnerError;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::Config;
use crate::runner::VMKindExt;
use crate::runner::VMResult;
use crate::{ContractCode, MockContractRuntimeCache};
use assert_matches::assert_matches;
use near_parameters::vm::VMKind;
use near_parameters::RuntimeFeesConfig;
use near_primitives_core::hash::CryptoHash;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[test]
fn test_caches_compilation_error() {
    let config = Arc::new(test_vm_config());
    with_vm_variants(&config, |vm_kind: VMKind| {
        // The cache is currently properly implemented only for NearVM
        match vm_kind {
            VMKind::NearVm => {}
            VMKind::Wasmer0 | VMKind::Wasmer2 | VMKind::Wasmtime => return,
        }
        let cache = MockContractRuntimeCache::default();
        let code = [42; 1000];
        let code = ContractCode::new(code.to_vec(), None);
        let code_hash = *code.hash();
        let terragas = 1000000000000u64;
        assert_eq!(cache.len(), 0);
        let outcome1 = make_cached_contract_call_vm(
            Arc::clone(&config),
            &cache,
            code_hash,
            Some(&code),
            "method_name1",
            terragas,
            vm_kind,
        )
        .expect("bad failure");
        println!("{:?}", cache);
        assert_eq!(cache.len(), 1);
        let outcome2 = make_cached_contract_call_vm(
            Arc::clone(&config),
            &cache,
            code_hash,
            None,
            "method_name2",
            terragas,
            vm_kind,
        )
        .expect("bad failure");
        assert_eq!(outcome1.aborted.as_ref(), outcome2.aborted.as_ref());
    })
}

#[test]
fn test_does_not_cache_io_error() {
    let config = Arc::new(test_vm_config());
    with_vm_variants(&config, |vm_kind: VMKind| {
        match vm_kind {
            VMKind::NearVm => {}
            VMKind::Wasmer0 | VMKind::Wasmer2 | VMKind::Wasmtime => return,
        }

        let code = near_test_contracts::trivial_contract();
        let code = ContractCode::new(code.to_vec(), None);
        let code_hash = *code.hash();
        let prepaid_gas = 10u64.pow(12);
        let cache = FaultingContractRuntimeCache::default();

        cache.set_read_fault(true);
        let result = make_cached_contract_call_vm(
            Arc::clone(&config),
            &cache,
            code_hash,
            None,
            "main",
            prepaid_gas,
            vm_kind,
        );
        assert_matches!(
            result.err(),
            Some(VMRunnerError::CacheError(crate::logic::errors::CacheError::ReadError(_)))
        );
        cache.set_read_fault(false);

        cache.set_write_fault(true);
        let result = make_cached_contract_call_vm(
            Arc::clone(&config),
            &cache,
            code_hash,
            Some(&code),
            "main",
            prepaid_gas,
            vm_kind,
        );
        assert_matches!(
            result.err(),
            Some(VMRunnerError::CacheError(crate::logic::errors::CacheError::WriteError(_)))
        );
        cache.set_write_fault(false);
    })
}

fn make_cached_contract_call_vm(
    config: Arc<Config>,
    cache: &dyn ContractRuntimeCache,
    code_hash: CryptoHash,
    code: Option<&ContractCode>,
    method_name: &str,
    prepaid_gas: u64,
    vm_kind: VMKind,
) -> VMResult {
    let mut fake_external = if let Some(code) = code {
        MockedExternal::with_code_and_hash(code_hash, code.clone_for_tests())
    } else {
        MockedExternal::new()
    };
    fake_external.code_hash = code_hash;
    let mut context = create_context(vec![]);
    let fees = Arc::new(RuntimeFeesConfig::test());
    context.prepaid_gas = prepaid_gas;
    let gas_counter = context.make_gas_counter(&config);
    let runtime = vm_kind.runtime(config).expect("runtime has not been compiled");
    runtime.prepare(&fake_external, Some(cache), gas_counter, method_name).run(
        &mut fake_external,
        &context,
        fees,
    )
}

#[test]
#[cfg(feature = "wasmer2_vm")]
fn test_wasmer2_artifact_output_stability() {
    use crate::prepare;
    use crate::wasmer2_runner::Wasmer2VM;
    use wasmer_compiler::{CpuFeature, Target};
    use wasmer_engine::Executable;
    // If this test has failed, you want to adjust the necessary constants so that `cache::vm_hash`
    // changes (and only then the hashes here).
    //
    // Note that this test is a best-effort fish net. Some changes that should modify the hash will
    // fall through the cracks here, but hopefully it should catch most of the fish just fine.
    let seeds = [2, 3, 5, 7, 11, 13, 17];
    let prepared_hashes = [
        7891448467132053449,
        9113804040784454744,
        15987320570619072032,
        7232953852262286521,
        7536220052944640509,
        13392993251733921322,
        4364804287818604246,
    ];
    let mut got_prepared_hashes = Vec::with_capacity(seeds.len());
    let compiled_hashes = [
        7897366507246831520,
        4504708181643041916,
        12437432560773026476,
        3217201805099435580,
        17987183739396987305,
        7265745711428292723,
        4973318757824608216,
    ];
    let mut got_compiled_hashes = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let contract = ContractCode::new(near_test_contracts::arbitrary_contract(seed), None);

        let config = Arc::new(test_vm_config());
        let prepared_code =
            prepare::prepare_contract(contract.code(), &config, VMKind::Wasmer2).unwrap();
        let this_hash = crate::utils::stable_hash((&contract.code(), &prepared_code));
        got_prepared_hashes.push(this_hash);
        if std::env::var_os("NEAR_STABILITY_TEST_WRITE").is_some() {
            std::fs::write(format!("prepared{}", this_hash), prepared_code).unwrap();
        }

        let mut features = CpuFeature::set();
        features.insert(CpuFeature::AVX);
        let triple = "x86_64-unknown-linux-gnu".parse().unwrap();
        let target = Target::new(triple, features);
        let vm = Wasmer2VM::new_for_target(config, target);
        let artifact = vm.compile_uncached(&contract).unwrap();
        let serialized = artifact.serialize().unwrap();
        let this_hash = crate::utils::stable_hash(&serialized);
        got_compiled_hashes.push(this_hash);
        if std::env::var_os("NEAR_STABILITY_TEST_WRITE").is_some() {
            std::fs::write(format!("artifact{}", this_hash), serialized).unwrap();
        }
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
#[cfg(feature = "near_vm")]
fn test_near_vm_artifact_output_stability() {
    use crate::near_vm_runner::NearVM;
    use crate::prepare;
    use near_vm_compiler::{CpuFeature, Target};
    // If this test has failed, you want to adjust the necessary constants so that `cache::vm_hash`
    // changes (and only then the hashes here).
    //
    // Note that this test is a best-effort fish net. Some changes that should modify the hash will
    // fall through the cracks here, but hopefully it should catch most of the fish just fine.
    let seeds = [2, 3, 5, 7, 11, 13, 17];
    let prepared_hashes = [
        // See the above comment if you want to change this
        1949662663451079349,
        2708994788591458264,
        5012156334458820956,
        9920955889305541399,
        18300200182157745360,
        870351225951448146,
        2224723427035408206,
    ];
    let mut got_prepared_hashes = Vec::with_capacity(seeds.len());
    let compiled_hashes = [
        // See the above comment if you want to change this
        14308529260517090966,
        13346695179117473012,
        7039559215467115488,
        2326827595782652597,
        2563919585736953738,
        18320128353038066787,
        4007078811749074507,
    ];
    let mut got_compiled_hashes = Vec::with_capacity(seeds.len());
    for seed in seeds {
        let contract = ContractCode::new(near_test_contracts::arbitrary_contract(seed), None);

        let config = Arc::new(test_vm_config());
        let prepared_code =
            prepare::prepare_contract(contract.code(), &config, VMKind::NearVm).unwrap();
        let this_hash = crate::utils::stable_hash((&contract.code(), &prepared_code));
        got_prepared_hashes.push(this_hash);
        if std::env::var_os("NEAR_STABILITY_TEST_WRITE").is_some() {
            std::fs::write(format!("prepared{}", this_hash), prepared_code).unwrap();
        }

        let mut features = CpuFeature::set();
        features.insert(CpuFeature::AVX);
        let triple = "x86_64-unknown-linux-gnu".parse().unwrap();
        let target = Target::new(triple, features);
        let vm = NearVM::new_for_target(config, target);
        let artifact = vm.compile_uncached(&contract).unwrap();
        let serialized = artifact.serialize().unwrap();
        let this_hash = crate::utils::stable_hash(&serialized);
        got_compiled_hashes.push(this_hash);

        if std::env::var_os("NEAR_STABILITY_TEST_WRITE").is_some() {
            let _ = std::fs::write(format!("artifact{}", this_hash), serialized);
        }
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

/// [`ContractRuntimeCache`] which simulates failures in the underlying
/// database.
#[derive(Default, Clone)]
struct FaultingContractRuntimeCache {
    read_fault: Arc<AtomicBool>,
    write_fault: Arc<AtomicBool>,
    inner: MockContractRuntimeCache,
}

impl FaultingContractRuntimeCache {
    fn set_read_fault(&self, yes: bool) {
        self.read_fault.store(yes, Ordering::SeqCst);
    }

    fn set_write_fault(&self, yes: bool) {
        self.write_fault.store(yes, Ordering::SeqCst);
    }
}

impl ContractRuntimeCache for FaultingContractRuntimeCache {
    fn put(&self, key: &CryptoHash, value: CompiledContractInfo) -> std::io::Result<()> {
        if self.write_fault.swap(false, Ordering::Relaxed) {
            return Err(io::ErrorKind::Other.into());
        }
        self.inner.put(key, value)
    }

    fn get(&self, key: &CryptoHash) -> std::io::Result<Option<CompiledContractInfo>> {
        if self.read_fault.swap(false, Ordering::Relaxed) {
            return Err(io::ErrorKind::Other.into());
        }
        self.inner.get(key)
    }

    fn handle(&self) -> Box<dyn ContractRuntimeCache> {
        Box::new(self.clone())
    }
}
