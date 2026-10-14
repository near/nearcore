//! Test in the same style as `runtime/near-vm-runner/src/tests/view_call_gas_limit.rs`
//! except to ensure that external calls are bounded by `max_gas_burnt_external`.

use crate::logic::VMOutcome;
use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::logic::{ExecutionMode, VMContext};
use crate::runner::VMKindExt;
use near_parameters::vm::VMKind;
use near_parameters::{RuntimeConfigStore, RuntimeFeesConfig};
use near_primitives_core::account::AccountContract;
use near_primitives_core::code::ContractCode;
use near_primitives_core::types::{Balance, Gas};
use near_primitives_core::version::PROTOCOL_VERSION;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

fn run_external_call(budget: Gas, code: &[u8]) -> VMOutcome {
    let store = RuntimeConfigStore::new();
    let mut config =
        near_parameters::vm::Config::clone(&store.get_config(PROTOCOL_VERSION).wasm_config);
    config.vm_kind = VMKind::Wasmtime;
    config.limit_config.max_gas_burnt_external = budget;
    // Make the prepaid gas as large as it can be, so that seeding the in-Wasm
    // gas global from it would never stop the loop.
    config.limit_config.max_total_prepaid_gas = Gas::MAX;
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
        // Enough balance to pay for `Gas::MAX` at the gas price below.
        account_balance: Balance::from_near(1_000_000),
        account_locked_balance: Balance::ZERO,
        storage_usage: 12,
        account_contract: AccountContract::None,
        attached_deposit: Balance::ZERO,
        prepaid_gas: Gas::ZERO,
        random_seed: vec![0, 1, 2],
        execution_mode: ExecutionMode::External { gas_price: Balance::from_yoctonear(1) },
        output_data_receivers: vec![],
    };

    let gas_counter = context.make_gas_counter(&config);
    let mut ext = MockedExternal::with_code(ContractCode::new(code.to_vec(), None));
    let gas_counter = gas_counter
        .prepare_for_contract(&config, "burn", ext.code_len())
        .expect("contract loading charge failed");
    let fees = Arc::new(RuntimeFeesConfig::test());
    let runtime = VMKind::Wasmtime.runtime(Arc::clone(&config)).expect("wasmtime not compiled in");
    runtime
        .prepare(&ext, None, gas_counter, "burn")
        .run(&mut ext, &context, fees)
        .expect("execution failed")
}

#[test]
fn external_call_gas_is_bounded_for_pure_wasm_loop() {
    // Infinite in-Wasm loop, no host imports.
    let code = wat::parse_str(r#"(module (func (export "burn") (loop $l br $l)))"#).unwrap();
    // 1 Tgas ≈ ~1.2M wasm ops at regular_op_cost, i.e. a fraction of a second.
    let budget = Gas::from_teragas(1);

    // Execute on a watchdog thread: if the budget is not enforced the loop
    // never returns, and we want that to fail the test (recv_timeout) rather
    // than hang the test binary forever.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(run_external_call(budget, &code));
    });
    let outcome = rx.recv_timeout(Duration::from_secs(60)).expect(
        "external call did not return within 60s: max_gas_burnt_external is not enforced in-Wasm",
    );

    let err = outcome.aborted.expect("expected the infinite loop to hit the gas limit");
    assert!(
        matches!(err, FunctionCallError::HostError(HostError::GasLimitExceeded)),
        "expected GasLimitExceeded, got {err:?}"
    );
    assert!(
        outcome.burnt_gas <= budget,
        "external call burnt {} gas, exceeding the budget {}",
        outcome.burnt_gas.as_gas(),
        budget.as_gas()
    );
}
