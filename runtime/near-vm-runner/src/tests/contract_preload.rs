// Currently only testing wasmer code, so disabled outside of x86_64
#![cfg(target_arch = "x86_64")]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::{FunctionCallError, MethodResolveError, VMError};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{ProtocolVersion, VMConfig, VMContext};

use crate::cache::precompile_contract_vm;
use crate::errors::ContractPrecompilatonResult;
use crate::vm_kind::VMKind;
use crate::{ContractCallPrepareRequest, ContractCaller, VMResult};

fn default_vm_context() -> VMContext {
    return VMContext {
        current_account_id: "alice".parse().unwrap(),
        signer_account_id: "bob".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol".parse().unwrap(),
        input: vec![],
        block_index: 1,
        block_timestamp: 1586796191203000000,
        account_balance: 10u128.pow(25),
        account_locked_balance: 0,
        storage_usage: 100,
        attached_deposit: 0,
        prepaid_gas: 10u64.pow(18),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
        epoch_height: 1,
    };
}

#[derive(Default, Clone)]
pub struct MockCompiledContractCache {
    store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>>,
    delay: Duration,
}

impl MockCompiledContractCache {
    pub fn new(delay: i32) -> Self {
        Self {
            store: Arc::new(Mutex::new(HashMap::new())),
            delay: Duration::from_millis(delay as u64),
        }
    }

    pub fn len(&self) -> usize {
        self.store.lock().unwrap().len()
    }
}

impl CompiledContractCache for MockCompiledContractCache {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), std::io::Error> {
        sleep(self.delay);
        self.store.lock().unwrap().insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        sleep(self.delay);
        let res = self.store.lock().unwrap().get(key).cloned();
        Ok(res)
    }
}

#[track_caller]
fn test_result(result: VMResult, expected_gas: u64) -> Result<(), ()> {
    match result {
        VMResult::Ok(outcome) => {
            assert_eq!(outcome.burnt_gas, expected_gas);
            Ok(())
        }
        VMResult::Aborted(
            outcome,
            VMError::FunctionCallError(FunctionCallError::MethodResolveError(
                MethodResolveError::MethodNotFound,
            )),
        ) => {
            assert_eq!(
                outcome.used_gas, expected_gas,
                "Outcome with {expected_gas} gas expected but outcome was: {outcome:?}",
            );
            Err(())
        }
        VMResult::Aborted(_, err) => {
            // This test should only produce `MethodNotFound` errors.
            assert!(false, "Unexpected error: {:?}", err);
            Err(())
        }
    }
}

fn test_vm_runner(preloaded: bool, vm_kind: VMKind, repeat: i32) {
    let code1 = Arc::new(ContractCode::new(near_test_contracts::rs_contract().to_vec(), None));
    let code2 = Arc::new(ContractCode::new(near_test_contracts::ts_contract().to_vec(), None));
    // This method name exists in code1 but not in code2. Using it for both
    // contract calls means the first will succeed and the second will fail.
    let method_name = "log_something";

    let mut fake_external = MockedExternal::new();

    let context = default_vm_context();
    let vm_config = VMConfig::test();
    let cache: Option<Arc<dyn CompiledContractCache>> =
        Some(Arc::new(MockCompiledContractCache::new(0)));
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];

    if preloaded {
        let mut requests = Vec::new();
        let mut caller = ContractCaller::new(4, vm_kind, vm_config);
        for _ in 0..repeat {
            requests.push(ContractCallPrepareRequest {
                code: Arc::clone(&code1),
                cache: cache.clone(),
            });
            requests.push(ContractCallPrepareRequest {
                code: Arc::clone(&code2),
                cache: cache.clone(),
            });
        }
        let calls = caller.preload(requests);
        for (i, prepared) in calls.iter().enumerate() {
            let result = caller.run_preloaded(
                prepared,
                method_name,
                &mut fake_external,
                context.clone(),
                &fees,
                &promise_results,
                ProtocolVersion::MAX,
            );
            let call_to_first_contract = i % 2 == 0;
            if call_to_first_contract {
                test_result(result, 11088051921).expect("Call expected to succeed.");
            } else {
                test_result(result, 0).expect_err("Call expected to fail.");
            }
        }
    } else {
        let runtime = vm_kind.runtime(vm_config).expect("runtime is has not been compiled");
        for _ in 0..repeat {
            let result1 = runtime.run(
                &code1,
                method_name,
                &mut fake_external,
                context.clone(),
                &fees,
                &promise_results,
                ProtocolVersion::MAX,
                cache.as_deref(),
            );
            test_result(result1, 31554569634).expect("Call expected to succeed.");
            let result2 = runtime.run(
                &code2,
                method_name,
                &mut fake_external,
                context.clone(),
                &fees,
                &promise_results,
                ProtocolVersion::MAX,
                cache.as_deref(),
            );
            test_result(result2, 0).expect_err("Call expected to fail.");
        }
    }
}

#[test]
pub fn test_run_sequential() {
    #[cfg(feature = "wasmer0_vm")]
    test_vm_runner(false, VMKind::Wasmer0, 100);
    #[cfg(feature = "wasmer2_vm")]
    test_vm_runner(false, VMKind::Wasmer2, 100);
}

#[test]
pub fn test_run_preloaded() {
    #[cfg(feature = "wasmer0_vm")]
    test_vm_runner(true, VMKind::Wasmer0, 100);
    #[cfg(feature = "wasmer2_vm")]
    test_vm_runner(true, VMKind::Wasmer2, 100);
}

fn test_precompile_vm(vm_kind: VMKind) {
    let mock_cache = MockCompiledContractCache::new(0);
    let cache: Option<&dyn CompiledContractCache> = Some(&mock_cache);
    let vm_config = VMConfig::test();
    let code1 = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let code2 = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);

    let result = precompile_contract_vm(vm_kind, &code1, &vm_config, cache).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractCompiled));
    assert_eq!(mock_cache.len(), 1);
    let result = precompile_contract_vm(vm_kind, &code1, &vm_config, cache).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
    assert_eq!(mock_cache.len(), 1);
    let result = precompile_contract_vm(vm_kind, &code2, &vm_config, None).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::CacheNotAvailable));
    assert_eq!(mock_cache.len(), 1);
    let result = precompile_contract_vm(vm_kind, &code2, &vm_config, cache).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractCompiled));
    assert_eq!(mock_cache.len(), 2);
    let result = precompile_contract_vm(vm_kind, &code2, &vm_config, cache).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
    assert_eq!(mock_cache.len(), 2);
}

#[test]
pub fn test_precompile() {
    #[cfg(feature = "wasmer0_vm")]
    test_precompile_vm(VMKind::Wasmer0);
    #[cfg(feature = "wasmer2_vm")]
    test_precompile_vm(VMKind::Wasmer2);
}
