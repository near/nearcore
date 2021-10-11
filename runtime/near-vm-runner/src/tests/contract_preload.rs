use crate::{run_vm, ContractCallPrepareRequest, ContractCaller, VMError, VMKind};
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_vm_logic::{ProtocolVersion, VMConfig, VMContext, VMOutcome};

use crate::cache::precompile_contract_vm;
use crate::errors::ContractPrecompilatonResult;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use assert_matches::assert_matches;
use near_primitives::types::CompiledContractCache;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use near_primitives::version::ProtocolFeature;
use near_primitives::version::PROTOCOL_VERSION;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use near_vm_errors::CompilationError::PrepareError;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use near_vm_errors::FunctionCallError::CompilationError;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use near_vm_errors::PrepareError::TooManyFunctions;
use near_vm_errors::VMError::FunctionCallError;
use near_vm_logic::mocks::mock_external::MockedExternal;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use rand::distributions::Alphanumeric;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use rand::{thread_rng, Rng};
use std::collections::HashMap;
#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
use std::fmt::Write;
use std::sync::{Arc, Mutex};
use std::thread::sleep;
use std::time::Duration;

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
    let protocol_version = ProtocolVersion::MAX;
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];
    let mut oks = 0;
    let mut errs = 0;

    if preloaded {
        let mut requests = Vec::new();
        let mut caller = ContractCaller::new(4, vm_kind, vm_config, protocol_version);
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
                protocol_version,
                cache.as_deref(),
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
                protocol_version,
                cache.as_deref(),
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
    let vm_config = VMConfig::default();
    let code1 = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
    let code2 = ContractCode::new(near_test_contracts::ts_contract().to_vec(), None);
    let protocol_version = PROTOCOL_VERSION;

    let result =
        precompile_contract_vm(vm_kind, &code1, &vm_config, cache, protocol_version).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractCompiled));
    assert_eq!(mock_cache.len(), 1);
    let result =
        precompile_contract_vm(vm_kind, &code1, &vm_config, cache, protocol_version).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractAlreadyInCache));
    assert_eq!(mock_cache.len(), 1);
    let result =
        precompile_contract_vm(vm_kind, &code2, &vm_config, None, protocol_version).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::CacheNotAvailable));
    assert_eq!(mock_cache.len(), 1);
    let result =
        precompile_contract_vm(vm_kind, &code2, &vm_config, cache, protocol_version).unwrap();
    assert_eq!(result, Result::Ok(ContractPrecompilatonResult::ContractCompiled));
    assert_eq!(mock_cache.len(), 2);
    let result =
        precompile_contract_vm(vm_kind, &code2, &vm_config, cache, protocol_version).unwrap();
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

#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
fn make_many_methods_contract(method_count: i32) -> ContractCode {
    let mut methods = String::new();
    for i in 0..method_count {
        if i == 0 {
            write!(&mut methods, "(export \"hello{i}\" (func {i}))", i = i).unwrap();
        }
        write!(
            &mut methods,
            "
              (func (;{i};)
                i32.const {i}
                drop
                return
              )
            ",
            i = i
        )
        .unwrap();
    }

    // Add random data section to make sure that VM cache effects do not occur.
    let rand_string: String =
        thread_rng().sample_iter(&Alphanumeric).take(10).map(char::from).collect();
    let code = format!(
        "
        (module
            {}
            (memory 1)           
            (data (i32.const {}) \"{}\")
            )",
        methods, method_count, rand_string
    );
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
pub fn test_max_contract_functions_vm(vm_kind: VMKind) {
    const FUNCTIONS_NUMBER: u64 = 100;
    let method_name = "hello0";

    let mut fake_external = MockedExternal::new();

    let context = default_vm_context();
    let mut vm_config = VMConfig::default();
    vm_config.limit_config.max_functions_number = Some(FUNCTIONS_NUMBER);
    let fees = RuntimeFeesConfig::test();

    let promise_results = vec![];
    let mut runner = |protocol_version: ProtocolVersion,
                      functions_number: u64|
     -> (Option<VMOutcome>, Option<VMError>) {
        let code = Arc::new(make_many_methods_contract(functions_number as i32));
        let cache: Option<Arc<dyn CompiledContractCache>> =
            Some(Arc::new(MockCompiledContractCache::new(0)));
        run_vm(
            &code,
            method_name,
            &mut fake_external,
            context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            vm_kind,
            protocol_version,
            cache.as_deref(),
        )
    };

    let result = runner(
        ProtocolFeature::LimitContractFunctionsNumber.protocol_version() - 1,
        FUNCTIONS_NUMBER + 10,
    );
    assert_matches!(result.1, None);
    let result = runner(
        ProtocolFeature::LimitContractFunctionsNumber.protocol_version(),
        FUNCTIONS_NUMBER - 10,
    );
    assert_matches!(result.1, None);

    let result = runner(
        ProtocolFeature::LimitContractFunctionsNumber.protocol_version(),
        FUNCTIONS_NUMBER + 10,
    );
    assert_matches!(
        result.1,
        Some(FunctionCallError(CompilationError(PrepareError(TooManyFunctions { number: _ }))))
    );
}

#[cfg(feature = "protocol_feature_limit_contract_functions_number")]
#[test]
pub fn test_max_contract_functions() {
    #[cfg(feature = "wasmer0_vm")]
    test_max_contract_functions_vm(VMKind::Wasmer0);
    #[cfg(feature = "wasmer2_vm")]
    test_max_contract_functions_vm(VMKind::Wasmer2);
}
