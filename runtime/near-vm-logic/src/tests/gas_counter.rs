use crate::receipt_manager::ReceiptMetadata;
use crate::tests::fixtures::get_context_with_prepaid_gas;
use crate::tests::helpers::*;
use crate::tests::vm_logic_builder::{TestVMLogic, VMLogicBuilder};
use crate::types::Gas;
use crate::VMConfig;
use expect_test::expect;
use near_primitives::config::{ActionCosts, ExtCosts};
use near_primitives::runtime::fees::Fee;
use near_primitives::transaction::{Action, FunctionCallAction};
use near_vm_errors::{HostError, VMLogicError};

#[test]
fn test_dont_burn_gas_when_exceeding_attached_gas_limit() {
    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 2);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit));

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, gas_limit * 2)
        .expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    // Just avoid hard-coding super-precise amount of gas burnt.
    assert!(outcome.burnt_gas < gas_limit / 2);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn test_limit_wasm_gas_after_attaching_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 2);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit));

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, gas_limit / 2)
        .expect("should add action to receipt");
    logic.gas((op_limit / 2) as u32).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.used_gas, gas_limit);
    assert!(gas_limit / 2 < outcome.burnt_gas);
    assert!(outcome.burnt_gas < gas_limit);
}

#[test]
fn test_cant_burn_more_than_max_gas_burnt_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit * 2));

    logic.gas(op_limit * 3).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit * 2);
}

#[test]
fn test_cant_burn_more_than_prepaid_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 2);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit));

    logic.gas(op_limit * 3).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn test_hit_max_gas_burnt_limit() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit * 3));

    promise_create(&mut logic, b"rick.test", 0, gas_limit / 2).expect("should create a promise");
    logic.gas(op_limit * 2).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert!(outcome.used_gas > gas_limit * 2);
}

#[test]
fn test_hit_prepaid_gas_limit() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit * 3);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit));

    promise_create(&mut logic, b"rick.test", 0, gas_limit / 2).expect("should create a promise");
    logic.gas(op_limit * 2).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome_and_distribute_gas();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[track_caller]
fn assert_with_gas(receipt: &ReceiptMetadata, expcted_gas: Gas) {
    match receipt.actions[0] {
        Action::FunctionCall(FunctionCallAction { gas, .. }) => {
            assert_eq!(expcted_gas, gas);
        }
        _ => {
            panic!("expected function call action");
        }
    }
}

#[track_caller]
fn function_call_weight_check(function_calls: &[(Gas, u64, Gas)]) {
    let gas_limit = 10_000_000_000;

    let mut logic_builder = VMLogicBuilder::free().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit));

    let mut ratios = vec![];

    // Schedule all function calls
    for &(static_gas, gas_weight, _) in function_calls {
        let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
        promise_batch_action_function_call_weight(&mut logic, index, 0, static_gas, gas_weight)
            .expect("batch action function call should succeed");
        ratios.push((index, gas_weight));
    }

    // Test static gas assigned before
    let receipts = logic.receipt_manager().action_receipts.iter().map(|(_, rec)| rec);
    for (receipt, &(static_gas, _, _)) in receipts.zip(function_calls) {
        assert_with_gas(receipt, static_gas);
    }

    let outcome = logic.compute_outcome_and_distribute_gas();

    // Test gas is distributed after outcome calculated.
    let receipts = outcome.action_receipts.iter().map(|(_, rec)| rec);

    // Assert lengths are equal for zip
    assert_eq!(receipts.len(), function_calls.len());

    // Assert sufficient amount was given to
    for (receipt, &(_, _, expected)) in receipts.zip(function_calls) {
        assert_with_gas(receipt, expected);
    }

    // Verify that all gas was consumed (assumes at least one ratio is provided)
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn function_call_weight_basic_cases_test() {
    // Following tests input are in the format (static gas, gas weight, expected gas)
    // and the gas limit is `10_000_000_000`

    // Single function call
    function_call_weight_check(&[(0, 1, 10_000_000_000)]);

    // Single function with static gas
    function_call_weight_check(&[(888, 1, 10_000_000_000)]);

    // Large weight
    function_call_weight_check(&[(0, 88888, 10_000_000_000)]);

    // Weight larger than gas limit
    function_call_weight_check(&[(0, 11u64.pow(14), 10_000_000_000)]);

    // Split two
    function_call_weight_check(&[(0, 3, 6_000_000_000), (0, 2, 4_000_000_000)]);

    // Split two with static gas
    function_call_weight_check(&[(1_000_000, 3, 5_998_600_000), (3_000_000, 2, 4_001_400_000)]);

    // Many different gas weights
    function_call_weight_check(&[
        (1_000_000, 3, 2_699_800_000),
        (3_000_000, 2, 1_802_200_000),
        (0, 1, 899_600_000),
        (1_000_000_000, 0, 1_000_000_000),
        (0, 4, 3_598_400_000),
    ]);

    // Weight over u64 bounds
    function_call_weight_check(&[(0, u64::MAX, 9_999_999_999), (0, 1000, 1)]);

    // Weight over gas limit with three function calls
    function_call_weight_check(&[
        (0, 10_000_000_000, 4_999_999_999),
        (0, 1, 0),
        (0, 10_000_000_000, 5_000_000_001),
    ]);

    // Weights with one zero and one non-zero
    function_call_weight_check(&[(0, 0, 0), (0, 1, 10_000_000_000)])
}

#[test]
fn function_call_no_weight_refund() {
    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit));

    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    promise_batch_action_function_call_weight(&mut logic, index, 0, 1000, 0)
        .expect("batch action function call should succeed");

    let outcome = logic.compute_outcome_and_distribute_gas();

    // Verify that unused gas was not allocated to function call
    assert!(outcome.used_gas < gas_limit);
}

#[test]
fn test_overflowing_burn_gas_with_promises_gas() {
    let gas_limit = 3 * 10u64.pow(14);
    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit));

    let account_id = logic.internal_mem_write(b"rick.test");
    let args = logic.internal_mem_write(b"");
    let num_100u128 = logic.internal_mem_write(&100u128.to_le_bytes());
    let num_10u128 = logic.internal_mem_write(&10u128.to_le_bytes());

    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    logic.promise_batch_action_transfer(index, num_100u128.ptr).unwrap();
    let call_id = logic.promise_batch_then(index, account_id.len, account_id.ptr).unwrap();

    let needed_gas_charge = u64::max_value() - logic.gas_counter().used_gas() - 1;
    let function_name_len =
        needed_gas_charge / logic.config().ext_costs.cost(ExtCosts::read_memory_byte);
    let result = logic.promise_batch_action_function_call(
        call_id,
        function_name_len,
        /* function_name_ptr: */ 0,
        args.len,
        args.ptr,
        num_10u128.ptr,
        10000,
    );
    assert!(matches!(
        result,
        Err(near_vm_errors::VMLogicError::HostError(near_vm_errors::HostError::GasLimitExceeded))
    ));
    assert_eq!(logic.gas_counter().used_gas(), gas_limit);
}

#[test]
fn test_overflowing_burn_gas_with_promises_gas_2() {
    let gas_limit = 3 * 10u64.pow(14);
    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_limit / 2));

    let account_id = logic.internal_mem_write(b"rick.test");
    let args = logic.internal_mem_write(b"");
    let num_100u128 = logic.internal_mem_write(&100u128.to_le_bytes());

    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    logic.promise_batch_action_transfer(index, num_100u128.ptr).unwrap();
    logic.promise_batch_then(index, account_id.len, account_id.ptr).unwrap();
    let minimum_prepay = logic.gas_counter().used_gas();
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(minimum_prepay));
    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    logic.promise_batch_action_transfer(index, num_100u128.ptr).unwrap();
    let call_id = logic.promise_batch_then(index, account_id.len, account_id.ptr).unwrap();
    let needed_gas_charge = u64::max_value() - logic.gas_counter().used_gas() - 1;
    let function_name_len =
        needed_gas_charge / logic.config().ext_costs.cost(ExtCosts::read_memory_byte);
    let result = logic.promise_batch_action_function_call(
        call_id,
        function_name_len,
        /* function_name_ptr: */ 0,
        args.len,
        args.ptr,
        10u128.to_le_bytes().as_ptr() as _,
        10000,
    );
    assert!(matches!(
        result,
        Err(near_vm_errors::VMLogicError::HostError(near_vm_errors::HostError::GasExceeded))
    ));
    assert_eq!(logic.gas_counter().used_gas(), minimum_prepay);
}

/// Check consistent result when exceeding gas limit on a specific action gas parameter.
///
/// Increases an action cost to a high value and then watch an execution run out
/// of gas. Then make sure the exact result is still the same. This prevents
/// accidental protocol changes where gas is deducted in different order.
#[track_caller]
fn check_action_gas_exceeds_limit(
    cost: ActionCosts,
    num_action_paid: u64,
    exercise_action: impl FnOnce(&mut TestVMLogic) -> Result<(), VMLogicError>,
) {
    // Create a logic parametrized such that it will fail with out-of-gas when specified action is deducted.
    let gas_limit = 10u64.pow(13);
    let gas_attached = gas_limit;
    let fee = Fee {
        send_sir: gas_limit / num_action_paid + 1,
        send_not_sir: gas_limit / num_action_paid + 10,
        execution: 1, // exec part is `used`, make it small
    };
    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit).gas_fee(cost, fee);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_attached));

    let result = exercise_action(&mut logic);
    assert!(result.is_err(), "expected out-of-gas error for {cost:?} but was ok");
    assert_eq!(result.unwrap_err(), VMLogicError::HostError(HostError::GasLimitExceeded));

    // When gas limit is exceeded, we always set burnt_gas := prepaid and then promise_gas := 0.
    assert_eq!(
        gas_attached,
        logic.gas_counter().burnt_gas(),
        "burnt gas should be all attached gas",
    );
    assert_eq!(
        gas_attached,
        logic.gas_counter().used_gas(),
        "used gas should be no more than burnt gas",
    );
}

/// Check consistent result when exceeding attached gas on a specific action gas parameter.
///
/// Very similar to `check_action_gas_exceeds_limit` but we hit a different
/// limit and return a different error.
///
/// This case is more interesting because the burnt gas can be below used gas,
/// when the prepaid gas was exceeded by burnt burnt + promised gas but not by
/// burnt gas alone.
#[track_caller]
fn check_action_gas_exceeds_attached(
    cost: ActionCosts,
    num_action_paid: u64,
    expected: expect_test::Expect,
    exercise_action: impl FnOnce(&mut TestVMLogic) -> Result<(), VMLogicError>,
) {
    // Create a logic parametrized such that it will fail with out-of-gas when specified action is deducted.
    let gas_limit = 10u64.pow(14);
    let gas_attached = 10u64.pow(13);
    let fee = Fee {
        send_sir: 1,      // make burnt gas small
        send_not_sir: 10, // make it easy to distinguish `sir` / `not_sir`
        execution: gas_attached / num_action_paid + 1,
    };
    let mut logic_builder = VMLogicBuilder::default().max_gas_burnt(gas_limit).gas_fee(cost, fee);
    let mut logic = logic_builder.build(get_context_with_prepaid_gas(gas_attached));

    let result = exercise_action(&mut logic);
    assert!(result.is_err(), "expected out-of-gas error for {cost:?} but was ok");
    assert_eq!(result.unwrap_err(), VMLogicError::HostError(HostError::GasExceeded));

    let actual = format!(
        "{} burnt {} used",
        logic.gas_counter().burnt_gas(),
        logic.gas_counter().used_gas()
    );
    expected.assert_eq(&actual);
}

#[test]
fn out_of_gas_new_action_receipt() {
    // two different ways to create an action receipts, first check exceeding the burnt limit
    check_action_gas_exceeds_limit(ActionCosts::new_action_receipt, 1, create_action_receipt);
    check_action_gas_exceeds_limit(ActionCosts::new_action_receipt, 2, create_promise_dependency);

    // the same again, but for prepaid gas
    check_action_gas_exceeds_attached(
        ActionCosts::new_action_receipt,
        1,
        expect!["8644846690 burnt 10000000000000 used"],
        create_action_receipt,
    );

    check_action_gas_exceeds_attached(
        ActionCosts::new_action_receipt,
        2,
        expect!["9411968532130 burnt 10000000000000 used"],
        create_promise_dependency,
    );
}

#[test]
fn out_of_gas_new_data_receipt() {
    check_action_gas_exceeds_limit(
        ActionCosts::new_data_receipt_base,
        1,
        create_promise_dependency,
    );

    check_action_gas_exceeds_attached(
        ActionCosts::new_data_receipt_base,
        1,
        expect!["10000000000000 burnt 10000000000000 used"],
        create_promise_dependency,
    );
}

fn create_action_receipt(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
    promise_batch_create(logic, "rick.test")?;
    Ok(())
}

fn create_promise_dependency(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
    let account_id = "rick.test";
    let idx = promise_batch_create(logic, account_id)?;
    let account_id = logic.internal_mem_write(account_id.as_bytes());
    logic.promise_batch_then(idx, account_id.len, account_id.ptr)?;
    Ok(())
}

/// Given the limit in gas, compute the corresponding limit in wasm ops for use
/// with [`VMLogic::gas`] function.
fn op_limit(gas_limit: Gas) -> u32 {
    (gas_limit / (VMConfig::test().regular_op_cost as u64)) as u32
}
