mod fixtures;
mod helpers;
mod vm_logic_builder;

use fixtures::get_context;
use helpers::*;
use vm_logic_builder::VMLogicBuilder;

#[test]
fn test_dont_burn_gas_when_exceeding_attached_gas_limit() {
    let mut logic_builder = VMLogicBuilder::default();
    let context = get_context(vec![], false);
    let limit = context.prepaid_gas;
    let mut logic = logic_builder.build(context);

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, limit * 2)
        .expect_err("should fail with gas limit");
    let outcome = logic.outcome();

    // Just avoid hard-coding super-precise amount of gas burnt.
    assert!(outcome.burnt_gas < limit / 2);
    assert!(outcome.used_gas == limit);
}

#[test]
fn test_limit_wasm_gas_after_attaching_gas() {
    let mut logic_builder = VMLogicBuilder::default();
    let context = get_context(vec![], false);
    let regular_op_cost = logic_builder.config.regular_op_cost;
    let limit = context.prepaid_gas;
    let op_limit = limit / (regular_op_cost as u64);
    let mut logic = logic_builder.build(context);

    let index = promise_create(&mut logic, b"rick.test", 0, 0).expect("should create a promise");
    promise_batch_action_function_call(&mut logic, index, 0, limit / 2)
        .expect("should add action to receipt");
    logic.gas((op_limit / 2) as u32).expect_err("should fail with gas limit");
    let outcome = logic.outcome();

    assert!(outcome.used_gas == limit);
    assert!(limit / 2 < outcome.burnt_gas && outcome.burnt_gas < limit);
}
