//! Tests that `CompiledContractCache` is working correctly. 
use super::{create_context, with_vm_variants, LATEST_PROTOCOL_VERSION};
use crate::internal::VMKind;
use crate::MockCompiledContractCache;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_vm_errors::VMError;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMOutcome};

#[test]
fn test_contract_error_caching() {
    with_vm_variants(|vm_kind: VMKind| {
        match vm_kind {
            VMKind::Wasmer0 | VMKind::Wasmer2 => {}
            VMKind::Wasmtime => return,
        }
        let mut cache = MockCompiledContractCache::default();
        let code = [42; 1000];
        let terragas = 1000000000000u64;
        assert_eq!(cache.len(), 0);
        let err1 =
            make_cached_contract_call_vm(&mut cache, &code, "method_name1", terragas, vm_kind);
        println!("{:?}", cache);
        assert_eq!(cache.len(), 1);
        let err2 =
            make_cached_contract_call_vm(&mut cache, &code, "method_name2", terragas, vm_kind);
        assert_eq!(err1, err2);
    })
}

fn make_cached_contract_call_vm(
    cache: &mut dyn CompiledContractCache,
    code: &[u8],
    method_name: &str,
    prepaid_gas: u64,
    vm_kind: VMKind,
) -> (Option<VMOutcome>, Option<VMError>) {
    let mut fake_external = MockedExternal::new();
    let mut context = create_context(vec![]);
    let config = VMConfig::test();
    let fees = RuntimeFeesConfig::test();
    let promise_results = vec![];
    context.prepaid_gas = prepaid_gas;
    let code = ContractCode::new(code.to_vec(), None);
    let runtime = vm_kind.runtime().expect("runtime has not been compiled");

    runtime.run(
        &code,
        method_name,
        &mut fake_external,
        context.clone(),
        &config,
        &fees,
        &promise_results,
        LATEST_PROTOCOL_VERSION,
        Some(cache),
    )
}
