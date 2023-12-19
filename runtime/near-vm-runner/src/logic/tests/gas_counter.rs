use crate::logic::tests::helpers::*;
use crate::logic::tests::vm_logic_builder::{TestVMLogic, VMLogicBuilder};
use crate::logic::types::Gas;
use crate::logic::MemSlice;
use crate::logic::{HostError, VMLogicError};
use crate::tests::test_vm_config;
use expect_test::expect;
use near_parameters::{ActionCosts, ExtCosts, Fee};

#[test]
fn test_dont_burn_gas_when_exceeding_attached_gas_limit() {
    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit * 2;
    logic_builder.context.prepaid_gas = gas_limit;
    let mut logic = logic_builder.build();

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, gas_limit * 2)
        .expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome();

    // Just avoid hard-coding super-precise amount of gas burnt.
    assert!(outcome.burnt_gas < gas_limit / 2);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn test_limit_wasm_gas_after_attaching_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit * 2;
    logic_builder.context.prepaid_gas = gas_limit;
    let mut logic = logic_builder.build();

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, gas_limit / 2)
        .expect("should add action to receipt");
    logic.gas_opcodes((op_limit / 2) as u32).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome();

    assert_eq!(outcome.used_gas, gas_limit);
    assert!(gas_limit / 2 < outcome.burnt_gas);
    assert!(outcome.burnt_gas < gas_limit);
}

#[test]
fn test_cant_burn_more_than_max_gas_burnt_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit;
    logic_builder.context.prepaid_gas = gas_limit * 2;
    let mut logic = logic_builder.build();

    logic.gas_opcodes(op_limit * 3).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit * 2);
}

#[test]
fn test_cant_burn_more_than_prepaid_gas() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit * 2;
    logic_builder.context.prepaid_gas = gas_limit;
    let mut logic = logic_builder.build();

    logic.gas_opcodes(op_limit * 3).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn test_hit_max_gas_burnt_limit() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit;
    logic_builder.context.prepaid_gas = gas_limit * 3;
    let mut logic = logic_builder.build();

    promise_create(&mut logic, b"rick.test", 0, gas_limit / 2).expect("should create a promise");
    logic.gas_opcodes(op_limit * 2).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert!(outcome.used_gas > gas_limit * 2);
}

#[test]
fn test_hit_prepaid_gas_limit() {
    let gas_limit = 10u64.pow(14);
    let op_limit = op_limit(gas_limit);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit * 3;
    logic_builder.context.prepaid_gas = gas_limit;
    let mut logic = logic_builder.build();

    promise_create(&mut logic, b"rick.test", 0, gas_limit / 2).expect("should create a promise");
    logic.gas_opcodes(op_limit * 2).expect_err("should fail with gas limit");
    let outcome = logic.compute_outcome();

    assert_eq!(outcome.burnt_gas, gas_limit);
    assert_eq!(outcome.used_gas, gas_limit);
}

#[test]
fn function_call_no_weight_refund() {
    let gas_limit = 10u64.pow(14);

    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit;
    logic_builder.context.prepaid_gas = gas_limit;
    let mut logic = logic_builder.build();

    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    promise_batch_action_function_call_weight(&mut logic, index, 0, 1000, 0)
        .expect("batch action function call should succeed");

    let outcome = logic.compute_outcome();

    // Verify that unused gas was not allocated to function call
    assert!(outcome.used_gas < gas_limit);
}

#[test]
fn test_overflowing_burn_gas_with_promises_gas() {
    let gas_limit = 3 * 10u64.pow(14);
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit;
    logic_builder.context.prepaid_gas = gas_limit;
    let mut logic = logic_builder.build();

    let account_id = logic.internal_mem_write(b"rick.test");
    let args = logic.internal_mem_write(b"");
    let num_100u128 = logic.internal_mem_write(&100u128.to_le_bytes());
    let num_10u128 = logic.internal_mem_write(&10u128.to_le_bytes());

    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    logic.promise_batch_action_transfer(index, num_100u128.ptr).unwrap();
    let call_id = logic.promise_batch_then(index, account_id.len, account_id.ptr).unwrap();

    let needed_gas_charge = u64::max_value() - logic.gas_counter().used_gas() - 1;
    let function_name_len =
        needed_gas_charge / logic.config().ext_costs.gas_cost(ExtCosts::read_memory_byte);
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
        Err(crate::logic::VMLogicError::HostError(crate::logic::HostError::GasLimitExceeded))
    ));
    assert_eq!(logic.gas_counter().used_gas(), gas_limit);
}

#[test]
fn test_overflowing_burn_gas_with_promises_gas_2() {
    let gas_limit = 3 * 10u64.pow(14);
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit;
    logic_builder.context.prepaid_gas = gas_limit / 2;
    let mut logic = logic_builder.build();

    let account_id = logic.internal_mem_write(b"rick.test");
    let args = logic.internal_mem_write(b"");
    let num_100u128 = logic.internal_mem_write(&100u128.to_le_bytes());

    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    logic.promise_batch_action_transfer(index, num_100u128.ptr).unwrap();
    logic.promise_batch_then(index, account_id.len, account_id.ptr).unwrap();
    let minimum_prepay = logic.gas_counter().used_gas();
    logic_builder.context.prepaid_gas = minimum_prepay;
    let mut logic = logic_builder.build();
    let index = promise_batch_create(&mut logic, "rick.test").expect("should create a promise");
    logic.promise_batch_action_transfer(index, num_100u128.ptr).unwrap();
    let call_id = logic.promise_batch_then(index, account_id.len, account_id.ptr).unwrap();
    let needed_gas_charge = u64::max_value() - logic.gas_counter().used_gas() - 1;
    let function_name_len =
        needed_gas_charge / logic.config().ext_costs.gas_cost(ExtCosts::read_memory_byte);
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
        Err(crate::logic::VMLogicError::HostError(crate::logic::HostError::GasExceeded))
    ));
    assert_eq!(logic.gas_counter().used_gas(), minimum_prepay);
}

/// Check consistent result when exceeding gas limit on a specific action gas parameter.
///
/// Increases an action cost to a high value and then watch an execution run out
/// of gas. Then make sure the exact result is still the same. This prevents
/// accidental protocol changes where gas is deducted in different order.
///
/// The `exercise_action` function must be a function or closure that operates
/// on a `VMLogic` and triggers gas costs associated with the action parameter
/// under test.
///
/// `num_action_paid` specifies how often the cost is charged in
/// `exercise_action`. We aim to make it `num_action_paid` = 1 in the typical
/// case but for cots per byte this is usually a higher value.
///
/// `num_action_paid` is required to calculate by how much exactly gas prices
/// must be increased so that it will just trigger the gas limit.
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
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit;
    logic_builder.fees_config.action_fees[cost] = fee;
    logic_builder.context.prepaid_gas = gas_attached;
    logic_builder.context.output_data_receivers = vec!["alice.test".parse().unwrap()];
    let mut logic = logic_builder.build();

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

/// Check consistent result when exceeding attached gas on a specific action gas
/// parameter.
///
/// Very similar to `check_action_gas_exceeds_limit` but we hit a different
/// limit and return a different error. See that comment for an explanation on
/// the arguments.
///
/// This case is more interesting because the burnt gas can be below used gas,
/// when the prepaid gas was exceeded by burnt burnt + promised gas but not by
/// burnt gas alone.
///
/// Consequently, `num_action_paid` here is even more important to calculate
/// exactly what the gas costs should be to trigger the limits.
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
    let mut logic_builder = VMLogicBuilder::default();
    logic_builder.config.limit_config.max_gas_burnt = gas_limit;
    logic_builder.fees_config.action_fees[cost] = fee;
    logic_builder.context.prepaid_gas = gas_attached;
    logic_builder.context.output_data_receivers = vec!["alice.test".parse().unwrap()];
    let mut logic = logic_builder.build();

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

// Below are a bunch of `out_of_gas_*` tests. These test that when we run out of
// gas while charging a specific action gas cost, we burn a consistent amount of
// gas. This is to prevent accidental changes in how we charge gas. It cannot
// cover all cases but it can detect things like a changed order of gas charging
// or splitting pay_gas(A+B) to pay_gas(A), pay_gas(B), which went through to
// master unnoticed before.
//
// The setup for these tests is as follows:
// - 1 test per action cost
// - each test checks for 2 types of out of gas errors, gas limit exceeded and
//   gas attached exceeded
// - common code to create a test VMLogic setup is in checker functions
//   `check_action_gas_exceeds_limit` and `check_action_gas_exceeds_attached`
//   which are called from every test
// - each action cost must be triggered in a different way, so we define a small
//   function that does something which charges the tested action cost, then we
//   give this function to the checker functions
// - if an action cost is charged through different paths, the test defines
//   multiple functions that trigger the cost and the checker functions are
//   called once for each of them
// - these action cost triggering functions are defined in the test's inner
//   scope, unless they are shared between multiple tests

/// see longer comment above for how this test works
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

    /// function to trigger action receipt action cost
    fn create_action_receipt(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        promise_batch_create(logic, "rick.test")?;
        Ok(())
    }
}

/// see longer comment above for how this test works
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

/// see longer comment above for how this test works
#[test]
fn out_of_gas_new_data_receipt_byte() {
    check_action_gas_exceeds_limit(ActionCosts::new_data_receipt_byte, 11, value_return);

    // expect to burn it all because send + exec fees are fully paid upfront
    check_action_gas_exceeds_attached(
        ActionCosts::new_data_receipt_byte,
        11,
        expect!["10000000000000 burnt 10000000000000 used"],
        value_return,
    );

    // value return will pay for the cost of returned data dependency bytes, if there are any.
    fn value_return(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        // 11 characters long string
        let value = logic.internal_mem_write(b"lorem ipsum");
        logic.value_return(11, value.ptr)?;
        Ok(())
    }
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_create_account() {
    check_action_gas_exceeds_limit(ActionCosts::create_account, 1, create_account);

    check_action_gas_exceeds_attached(
        ActionCosts::create_account,
        1,
        expect!["116969114801 burnt 10000000000000 used"],
        create_account,
    );

    fn create_account(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        let account_id = "rick.test";
        let idx = promise_batch_create(logic, account_id)?;
        logic.promise_batch_action_create_account(idx)?;
        Ok(())
    }
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_delete_account() {
    check_action_gas_exceeds_limit(ActionCosts::delete_account, 1, delete_account);

    check_action_gas_exceeds_attached(
        ActionCosts::delete_account,
        1,
        expect!["125349193370 burnt 10000000000000 used"],
        delete_account,
    );

    fn delete_account(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        let beneficiary_account_id = "alice.test";
        let deleted_account_id = "bob.test";
        let idx = promise_batch_create(logic, deleted_account_id)?;
        let beneficiary = logic.internal_mem_write(beneficiary_account_id.as_bytes());
        logic.promise_batch_action_delete_account(idx, beneficiary.len, beneficiary.ptr)?;
        Ok(())
    }
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_deploy_contract_base() {
    check_action_gas_exceeds_limit(ActionCosts::deploy_contract_base, 1, deploy_contract);

    check_action_gas_exceeds_attached(
        ActionCosts::deploy_contract_base,
        1,
        expect!["119677812659 burnt 10000000000000 used"],
        deploy_contract,
    );
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_deploy_contract_byte() {
    check_action_gas_exceeds_limit(ActionCosts::deploy_contract_byte, 26, deploy_contract);

    check_action_gas_exceeds_attached(
        ActionCosts::deploy_contract_byte,
        26,
        expect!["304443562909 burnt 10000000000000 used"],
        deploy_contract,
    );
}

/// function to trigger base + 26 bytes deployment costs (26 is arbitrary)
fn deploy_contract(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
    let account_id = "rick.test";
    let idx = promise_batch_create(logic, account_id)?;
    let code = logic.internal_mem_write(b"lorem ipsum with length 26");
    logic.promise_batch_action_deploy_contract(idx, code.len, code.ptr)?;
    Ok(())
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_function_call_base() {
    check_action_gas_exceeds_limit(ActionCosts::function_call_base, 1, cross_contract_call);
    check_action_gas_exceeds_limit(
        ActionCosts::function_call_base,
        1,
        cross_contract_call_gas_weight,
    );

    check_action_gas_exceeds_attached(
        ActionCosts::function_call_base,
        1,
        expect!["125011579049 burnt 10000000000000 used"],
        cross_contract_call,
    );
    check_action_gas_exceeds_attached(
        ActionCosts::function_call_base,
        1,
        expect!["125011579049 burnt 10000000000000 used"],
        cross_contract_call_gas_weight,
    );
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_function_call_byte() {
    check_action_gas_exceeds_limit(ActionCosts::function_call_byte, 40, cross_contract_call);
    check_action_gas_exceeds_limit(
        ActionCosts::function_call_byte,
        40,
        cross_contract_call_gas_weight,
    );

    check_action_gas_exceeds_attached(
        ActionCosts::function_call_byte,
        40,
        expect!["2444873079439 burnt 10000000000000 used"],
        cross_contract_call,
    );
    check_action_gas_exceeds_attached(
        ActionCosts::function_call_byte,
        40,
        expect!["2444873079439 burnt 10000000000000 used"],
        cross_contract_call_gas_weight,
    );
}

/// function to trigger base + 40 bytes function call action costs (40 is 26 +
/// 14 which are arbitrary)
fn cross_contract_call(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
    let account_id = "rick.test";
    let idx = promise_batch_create(logic, account_id)?;
    let arg = b"lorem ipsum with length 26";
    let name = b"fn_with_len_14";
    let attached_balance = 1u128;
    let gas = 1; // attaching very little gas so it doesn't cause gas exceeded on its own
    promise_batch_action_function_call_ext(logic, idx, name, arg, attached_balance, gas)?;
    Ok(())
}

/// same as `cross_contract_call` but splits gas remainder among outgoing calls
fn cross_contract_call_gas_weight(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
    let account_id = "rick.test";
    let idx = promise_batch_create(logic, account_id)?;
    let arg = b"lorem ipsum with length 26";
    let name = b"fn_with_len_14";
    let attached_balance = 1u128;
    let gas = 1; // attaching very little gas so it doesn't cause gas exceeded on its own
    let gas_weight = 1;
    promise_batch_action_function_call_weight_ext(
        logic,
        idx,
        name,
        arg,
        attached_balance,
        gas,
        gas_weight,
    )?;
    Ok(())
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_transfer() {
    check_action_gas_exceeds_limit(ActionCosts::transfer, 1, promise_transfer);

    check_action_gas_exceeds_attached(
        ActionCosts::transfer,
        1,
        expect!["119935181141 burnt 10000000000000 used"],
        promise_transfer,
    );

    fn promise_transfer(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        let account_id = "alice.test";
        let idx = promise_batch_create(logic, account_id)?;
        let attached_balance = logic.internal_mem_write(&1u128.to_be_bytes());
        logic.promise_batch_action_transfer(idx, attached_balance.ptr)?;
        Ok(())
    }
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_stake() {
    check_action_gas_exceeds_limit(ActionCosts::stake, 1, promise_stake);

    check_action_gas_exceeds_attached(
        ActionCosts::stake,
        1,
        expect!["122375106518 burnt 10000000000000 used"],
        promise_stake,
    );

    fn promise_stake(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        let account_id = "pool.test";
        let idx = promise_batch_create(logic, account_id)?;
        let attached_balance = logic.internal_mem_write(&1u128.to_be_bytes());
        let pk = write_test_pk(logic);
        logic.promise_batch_action_stake(idx, attached_balance.ptr, pk.len, pk.ptr)?;
        Ok(())
    }
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_add_full_access_key() {
    check_action_gas_exceeds_limit(ActionCosts::add_full_access_key, 1, promise_full_access_key);

    check_action_gas_exceeds_attached(
        ActionCosts::add_full_access_key,
        1,
        expect!["119999803802 burnt 10000000000000 used"],
        promise_full_access_key,
    );

    fn promise_full_access_key(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        let account_id = "alice.test";
        let idx = promise_batch_create(logic, account_id)?;
        let pk = test_pk();
        let nonce = 0;
        promise_batch_action_add_key_with_full_access(logic, idx, &pk, nonce)?;
        Ok(())
    }
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_add_function_call_key_base() {
    check_action_gas_exceeds_limit(
        ActionCosts::add_function_call_key_base,
        1,
        promise_function_key,
    );

    check_action_gas_exceeds_attached(
        ActionCosts::add_function_call_key_base,
        1,
        expect!["133982421242 burnt 10000000000000 used"],
        promise_function_key,
    );
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_add_function_call_key_byte() {
    check_action_gas_exceeds_limit(
        ActionCosts::add_function_call_key_byte,
        7,
        promise_function_key,
    );

    check_action_gas_exceeds_attached(
        ActionCosts::add_function_call_key_byte,
        7,
        expect!["236200046312 burnt 10000000000000 used"],
        promise_function_key,
    );
}

/// function to trigger base + 7 bytes action costs for adding a new function
/// call access key to an account (7 is arbitrary)
fn promise_function_key(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
    let account_id = "alice.test";
    let idx = promise_batch_create(logic, account_id)?;
    let allowance = 1u128;
    let pk = test_pk();
    let nonce = 0;
    let methods = b"foo,baz";
    promise_batch_action_add_key_with_function_call(
        logic,
        idx,
        &pk,
        nonce,
        allowance,
        account_id.as_bytes(),
        methods,
    )?;
    Ok(())
}

/// see longer comment above for how this test works
#[test]
fn out_of_gas_delete_key() {
    check_action_gas_exceeds_limit(ActionCosts::delete_key, 1, promise_delete_key);

    check_action_gas_exceeds_attached(
        ActionCosts::delete_key,
        1,
        expect!["119999803802 burnt 10000000000000 used"],
        promise_delete_key,
    );

    fn promise_delete_key(logic: &mut TestVMLogic) -> Result<(), VMLogicError> {
        let account_id = "alice.test";
        let idx = promise_batch_create(logic, account_id)?;
        let pk = write_test_pk(logic);
        logic.promise_batch_action_delete_key(idx, pk.len, pk.ptr)?;
        Ok(())
    }
}

/// function to trigger action + data receipt action costs
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
    (gas_limit / (test_vm_config().regular_op_cost as u64)) as u32
}

fn test_pk() -> Vec<u8> {
    let pk = borsh::to_vec(
        &"ed25519:22W5rKuvbMRphnDoCj6nfrWhRKvh9Xf9SWXfGHaeXGde"
            .parse::<near_crypto::PublicKey>()
            .unwrap(),
    )
    .unwrap();
    pk
}

fn write_test_pk(logic: &mut TestVMLogic) -> MemSlice {
    logic.internal_mem_write(&test_pk())
}
