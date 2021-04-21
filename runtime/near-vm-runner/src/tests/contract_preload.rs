use crate::{run_vm, ContractCallPrepareRequest, ContractCaller, VMError, VMKind};
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_vm_logic::{ProtocolVersion, VMConfig, VMContext, VMOutcome};

use crate::cache::precompile_contract_impl;
use crate::errors::ContractPrecompilatonResult;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::VMError::FunctionCallError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::profile::ProfileData;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

fn default_vm_context() -> VMContext {
    return VMContext {
        current_account_id: "alice".to_string(),
        signer_account_id: "bob".to_string(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol".to_string(),
        input: vec![],
        block_index: 1,
        block_timestamp: 1586796191203000000,
        account_balance: 10u128.pow(25),
        account_locked_balance: 0,
        storage_usage: 100,
        attached_deposit: 0,
        prepaid_gas: 10u64.pow(18),
        random_seed: vec![0, 1, 2],
        is_view: false,
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

fn test_result(result: (Option<VMOutcome>, Option<VMError>), check_gas: bool) -> (i32, i32) {
    let mut oks = 0;
    let mut errs = 0;
    match result.0 {
        Some(outcome) => {
            if check_gas {
                assert_eq!(outcome.burnt_gas, 11088051921);
            }
            oks += 1;
        }
        None => {}
    };
    match result.1 {
        Some(err) => match err {
            FunctionCallError(_) => {
                errs += 1;
            }
            _ => assert!(false, "Unexpected error: {:?}", err),
        },
        None => {}
    }
    (oks, errs)
}

fn test_vm_runner(preloaded: bool, vm_kind: VMKind, repeat: i32) {
    let code1 = Arc::new(ContractCode::new(near_test_contracts::rs_contract().to_vec(), None));
    let code2 = Arc::new(ContractCode::new(near_test_contracts::ts_contract().to_vec(), None));
    let method_name1 = "log_something";

    let mut fake_external = MockedExternal::new();

    let context = default_vm_context();
    let vm_config = VMConfig::default();
    let cache: Option<Arc<dyn CompiledContractCache>> =
        Some(Arc::new(MockCompiledContractCache::new(0)));
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];
    let profile_data = ProfileData::new_disabled();
    let mut oks = 0;
    let mut errs = 0;

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
        for prepared in &calls {
            let result = caller.run_preloaded(
                prepared,
                method_name1,
                &mut fake_external,
                context.clone(),
                &fees,
                &promise_results,
                ProtocolVersion::MAX,
                profile_data.clone(),
            );
            let (ok, err) = test_result(result, true);
            oks += ok;
            errs += err;
        }
    } else {
        for _ in 0..repeat {
            let result1 = run_vm(
                &code1,
                method_name1,
                &mut fake_external,
                context.clone(),
                &vm_config,
                &fees,
                &promise_results,
                vm_kind,
                ProtocolVersion::MAX,
                cache.as_deref(),
                profile_data.clone(),
            );
            let (ok, err) = test_result(result1, false);
            oks += ok;
            errs += err;
            let result2 = run_vm(
                &code2,
                method_name1,
                &mut fake_external,
                context.clone(),
                &vm_config,
                &fees,
                &promise_results,
                vm_kind,
                ProtocolVersion::MAX,
                cache.as_deref(),
                profile_data.clone(),
            );
            let (ok, err) = test_result(result2, false);
            oks += ok;
            errs += err;
        }
    }

    assert_eq!(oks, repeat);
    assert_eq!(errs, repeat);
}

#[test]
pub fn test_run_sequential() {
    #[cfg(feature = "wasmer0_vm")]
    test_vm_runner(false, VMKind::Wasmer0, 100);
    #[cfg(feature = "wasmer1_vm")]
    test_vm_runner(false, VMKind::Wasmer1, 100);
}

#[test]
pub fn test_run_preloaded() {
    #[cfg(feature = "wasmer0_vm")]
    test_vm_runner(true, VMKind::Wasmer0, 100);
    #[cfg(feature = "wasmer1_vm")]
    test_vm_runner(true, VMKind::Wasmer1, 100);
}

fn test_precompile_vm(vm_kind: VMKind) {
    let mock_cache = MockCompiledContractCache::new(0);
    let cache: Option<&dyn CompiledContractCache> = Some(&mock_cache);
    let vm_config = VMConfig::default();
    let code1 = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let code2 = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);

    let result = precompile_contract_impl(vm_kind, &code1, &vm_config, cache);
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractCompiled));
    assert_eq!(mock_cache.len(), 1);
    let result = precompile_contract_impl(vm_kind, &code1, &vm_config, cache);
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
    assert_eq!(mock_cache.len(), 1);
    let result = precompile_contract_impl(vm_kind, &code2, &vm_config, None);
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::CacheNotAvailable));
    assert_eq!(mock_cache.len(), 1);
    let result = precompile_contract_impl(vm_kind, &code2, &vm_config, cache);
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractCompiled));
    assert_eq!(mock_cache.len(), 2);
    let result = precompile_contract_impl(vm_kind, &code2, &vm_config, cache);
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
    assert_eq!(mock_cache.len(), 2);
}

#[test]
pub fn test_precompile() {
    #[cfg(feature = "wasmer0_vm")]
    test_precompile_vm(VMKind::Wasmer0);
    #[cfg(feature = "wasmer1_vm")]
    test_precompile_vm(VMKind::Wasmer1);
}
