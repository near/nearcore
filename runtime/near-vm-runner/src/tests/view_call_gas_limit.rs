//! Regression test: view calls are bounded by `max_gas_burnt_view`.
//!
//! In view mode `GasCounter::new` used to widen `prepaid_gas` to `Gas::MAX`.
//! The Wasmtime backend seeds the in-Wasm gas global from `remaining_gas()`
//! (`prepaid_gas - used_gas`), and the finite-wasm instrumentation only calls
//! back into the host (where `max_gas_burnt_view` is enforced) when that global
//! underflows a per-block constant. Seeding from `Gas::MAX` therefore let a
//! pure-Wasm loop with no host imports run for ~`u64::MAX / per_block_cost`
//! iterations (minutes-to-hours of CPU) before the cap was ever checked, so a
//! single anonymous `call_function` view query could pin an RPC worker thread.
//!
//! The fix bounds `prepaid_gas` by `max_gas_burnt` in view mode, so the guest
//! gas global reflects the cap and an unbounded loop aborts promptly.

use crate::logic::VMContext;
use crate::logic::VMOutcome;
use crate::logic::errors::{FunctionCallError, HostError};
use crate::logic::mocks::mock_external::MockedExternal;
use crate::runner::VMKindExt;
use near_parameters::vm::VMKind;
use near_parameters::{RuntimeConfigStore, RuntimeFeesConfig};
use near_primitives_core::code::ContractCode;
use near_primitives_core::config::ViewConfig;
use near_primitives_core::types::{Balance, Gas};
use near_primitives_core::version::PROTOCOL_VERSION;
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;

fn run_view_call(cap: Gas, code: &[u8]) -> VMOutcome {
    let store = RuntimeConfigStore::new(None);
    let mut config =
        near_parameters::vm::Config::clone(&store.get_config(PROTOCOL_VERSION).wasm_config);
    config.vm_kind = VMKind::Wasmtime;
    let config = Arc::new(config);

    let context = VMContext {
        current_account_id: "alice".parse().unwrap(),
        signer_account_id: "bob".parse().unwrap(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol".parse().unwrap(),
        refund_to_account_id: "david".parse().unwrap(),
        input: std::rc::Rc::from(Vec::new()),
        promise_results: Vec::new().into(),
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 1,
        account_balance: Balance::from_yoctonear(2),
        account_locked_balance: Balance::ZERO,
        storage_usage: 12,
        account_contract: near_primitives_core::account::AccountContract::None,
        attached_deposit: Balance::from_yoctonear(2),
        prepaid_gas: Gas::from_teragas(100),
        random_seed: vec![0, 1, 2],
        view_config: Some(ViewConfig { max_gas_burnt: cap }),
        output_data_receivers: vec![],
    };

    let gas_counter = context.make_gas_counter(&config);
    let mut ext = MockedExternal::with_code(ContractCode::new(code.to_vec(), None));
    let fees = Arc::new(RuntimeFeesConfig::test());
    let runtime = VMKind::Wasmtime.runtime(Arc::clone(&config)).expect("wasmtime not compiled in");
    runtime
        .prepare(&ext, None, gas_counter, "burn")
        .run(&mut ext, &context, fees)
        .expect("execution failed")
}

#[test]
fn view_call_gas_is_bounded_for_pure_wasm_loop() {
    // Infinite in-Wasm loop, no host imports.
    let code = wat::parse_str(r#"(module (func (export "burn") (loop $l br $l)))"#).unwrap();
    // 1 Tgas ≈ ~1.2M wasm ops at regular_op_cost, i.e. a fraction of a second.
    let cap = Gas::from_teragas(1);

    // Execute on a watchdog thread: if the cap is not enforced the loop never
    // returns, and we want that to fail the test (recv_timeout) rather than
    // hang the test binary forever.
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(run_view_call(cap, &code));
    });
    let outcome = rx
        .recv_timeout(Duration::from_secs(60))
        .expect("view call did not return within 60s: max_gas_burnt_view is not enforced in-Wasm");

    let err = outcome.aborted.expect("expected the infinite loop to hit the gas limit");
    assert!(
        matches!(
            err,
            FunctionCallError::HostError(HostError::GasLimitExceeded | HostError::GasExceeded)
        ),
        "expected a gas-limit error, got {err:?}"
    );
    assert!(
        outcome.burnt_gas <= cap,
        "view call burnt {} gas, exceeding the cap {}",
        outcome.burnt_gas.as_gas(),
        cap.as_gas()
    );
}
