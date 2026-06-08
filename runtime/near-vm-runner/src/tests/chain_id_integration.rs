//! End-to-end integration test for the `chain_id` host function.
//!
//! Runs `test-contract-rs`'s `ext_chain_id` (which imports `chain_id`, reads it
//! into a register, and returns the bytes) through the real WASM VM, covering
//! the imports wiring and register marshalling in a way the logic-only unit
//! tests cannot.

use crate::ContractCode;
use crate::logic::Config;
use crate::logic::External;
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::types::ReturnData;
use crate::runner::VMKindExt;
use crate::tests::{create_context, test_vm_config, with_vm_variants};
use near_parameters::RuntimeFeesConfig;
use near_parameters::vm::VMKind;
use std::cell::RefCell;
use std::sync::Arc;

#[test]
fn test_chain_id_integration_returns_chain_id() {
    let ran = RefCell::new(false);
    with_vm_variants(|vm_kind: VMKind| {
        let config = Arc::new(test_vm_config(Some(vm_kind)));
        let fees = Arc::new(RuntimeFeesConfig::test());
        let code = ContractCode::new(near_test_contracts::rs_contract().to_vec(), None);
        let mut fake_external = MockedExternal::with_code(code);
        let want_chain_id = fake_external.chain_id();
        let context = create_context(vec![]);
        let gas_counter = context.make_gas_counter(&config);
        let runtime = vm_kind.runtime(Arc::<Config>::clone(&config)).expect("no runtime");
        let outcome = runtime
            .prepare(&fake_external, None, gas_counter, "ext_chain_id")
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("execution failed");

        assert!(
            outcome.aborted.is_none(),
            "contract aborted under {vm_kind:?}: {:?}",
            outcome.aborted
        );
        let value = match &outcome.return_data {
            ReturnData::Value(v) => v.clone(),
            other => panic!("unexpected return data for {vm_kind:?}: {other:?}"),
        };
        assert_eq!(value, want_chain_id.as_bytes(), "unexpected chain_id from {vm_kind:?}");

        *ran.borrow_mut() = true;
    });
    assert!(*ran.borrow(), "no VM variants executed this test");
}
