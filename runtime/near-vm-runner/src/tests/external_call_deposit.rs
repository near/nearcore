//! An external contract call has no deposit: `attached_deposit` returns zero,
//! and the VM fails a call that carries a deposit.

use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::{ExecutionMode, VMContext};
use crate::logic::{ReturnData, VMOutcome};
use crate::runner::VMKindExt;
use near_parameters::vm::VMKind;
use near_parameters::{RuntimeConfigStore, RuntimeFeesConfig};
use near_primitives_core::account::AccountContract;
use near_primitives_core::code::ContractCode;
use near_primitives_core::types::{Balance, Gas};
use near_primitives_core::version::PROTOCOL_VERSION;
use std::rc::Rc;
use std::sync::Arc;

/// Run an external call, with the given attached deposit, of a contract method
/// that returns the result of the `attached_deposit` host function.
fn run_external_call(attached_deposit: Balance) -> VMOutcome {
    let code = wat::parse_str(
        r#"(module
            (import "env" "attached_deposit" (func $attached_deposit (param i64)))
            (import "env" "value_return" (func $value_return (param i64 i64)))
            (memory (export "memory") 1)
            (func (export "main")
                (call $attached_deposit (i64.const 0))
                (call $value_return (i64.const 16) (i64.const 0))))"#,
    )
    .unwrap();
    let store = RuntimeConfigStore::new();
    let mut config =
        near_parameters::vm::Config::clone(&store.get_config(PROTOCOL_VERSION).wasm_config);
    config.vm_kind = VMKind::Wasmtime;
    // The budget is zero in configs where external contract calls are not
    // enabled yet.
    config.limit_config.max_gas_burnt_external = Gas::from_teragas(20);
    let config = Arc::new(config);

    let context = VMContext {
        current_account_id: "alice".parse().unwrap(),
        signer_account_id: "alice".parse().unwrap(),
        signer_account_pk: vec![],
        predecessor_account_id: "alice".parse().unwrap(),
        refund_to_account_id: "alice".parse().unwrap(),
        input: Rc::from(Vec::new()),
        promise_results: Vec::new().into(),
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: Balance::from_near(1),
        account_locked_balance: Balance::ZERO,
        storage_usage: 12,
        account_contract: AccountContract::None,
        attached_deposit,
        prepaid_gas: Gas::ZERO,
        random_seed: vec![0, 1, 2],
        execution_mode: ExecutionMode::External {
            gas_price: Balance::from_yoctonear(100_000_000),
            fee_gas: Gas::ZERO,
        },
        output_data_receivers: vec![],
    };

    let gas_counter = context.make_gas_counter(&config);
    let mut ext = MockedExternal::with_code(ContractCode::new(code, None));
    let gas_counter = gas_counter
        .prepare_for_contract(&config, "main", ext.code_len())
        .expect("contract loading charge failed");
    let fees = Arc::new(RuntimeFeesConfig::test());
    let runtime = VMKind::Wasmtime.runtime(Arc::clone(&config)).expect("wasmtime not compiled in");
    runtime
        .prepare(&ext, None, gas_counter, "main")
        .run(&mut ext, &context, fees)
        .expect("execution failed")
}

#[test]
fn external_call_without_deposit_succeeds() {
    let outcome = run_external_call(Balance::ZERO);
    assert_eq!(outcome.aborted, None);
    assert_eq!(outcome.return_data, ReturnData::Value(0u128.to_le_bytes().to_vec()));
}

#[test]
fn external_call_with_deposit_fails() {
    let outcome = run_external_call(Balance::from_yoctonear(1));
    let err = outcome.aborted.expect("an external call with a deposit should fail");
    assert_eq!(err, FunctionCallError::HostError(HostError::DepositInExternalCall));
}
