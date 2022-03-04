use near_primitives::contract::ContractCode;

use crate::gas_cost::GasCost;

use crate::estimator_context::EstimatorRuntime;

pub(crate) fn op_loop_cost(
    runner: &EstimatorRuntime,
    repeats: u64,
    warmup_repeats: u64,
    op: &str,
    wasm_type: &str,
    second_operand: &str,
) -> GasCost {
    // this number should be large enough to dwarf loop overhead
    let loop_body_size = 1_000;
    let op_iters = 100_000;

    let local = match wasm_type {
        "i64" => "$a",
        "f64" => "$x",
        _ => panic!("{wasm_type} not implemented in op_loop_cost"),
    };


    let contract = make_op_loop_contract(
        &format!(
            "
    local.get {local}
    {second_operand}
    {op}
    drop
    "
        ),
        op_iters,
        loop_body_size,
    );

    let total = function_call_cost(runner, repeats, warmup_repeats, &contract);
    total / (op_iters * loop_body_size) as u64
}

pub(crate) fn make_op_loop_contract(op: &str, iters: usize, ops_per_iter: usize) -> ContractCode {
    let body = op.repeat(ops_per_iter);
    let code = format!(
        "
        (module
        (export \"op_loop\" (func 0))
            (func (;0;)
                (local $i i32) (local $a i64) (local $x f64)
                (loop $my_loop
                    ;; add one to $i
                    local.get $i
                    i32.const 1
                    i32.add
                    local.set $i
              
                    ;; Execute the operation n times
                    {body}
              
                    ;; branch loop on i < n
                    local.get $i
                    i32.const {iters}
                    i32.lt_s
                    br_if $my_loop
                )
                return
            )
        )
        ",
    );
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

fn function_call_cost(
    runner: &EstimatorRuntime,
    repeats: u64,
    warmup_repeats: u64,
    contract: &ContractCode,
) -> GasCost {
    for _ in 0..warmup_repeats {
        runner.run(contract, "op_loop");
    }

    let mut cost = runner.run(contract, "op_loop");
    for _ in 1..repeats {
        cost += runner.run(contract, "op_loop");
    }
    cost / repeats
}
