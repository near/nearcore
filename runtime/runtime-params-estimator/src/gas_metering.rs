use crate::config::Config;
use crate::gas_cost::{GasCost, LeastSquaresTolerance};
use crate::vm_estimator::create_context;
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::StoreCompiledContractCache;
use near_vm_runner::logic::mocks::mock_external::MockedExternal;
use near_vm_runner::logic::CompiledContractCache;
use std::fmt::Write;

pub(crate) fn gas_metering_cost(config: &Config) -> (GasCost, GasCost) {
    let mut xs1 = vec![];
    let mut ys1 = vec![];
    let mut xs2 = vec![];
    let mut ys2 = vec![];
    for depth in vec![1, 10, 20, 30, 50, 100, 200, 1000] {
        {
            // Here we test gas metering costs for forward branch cases.
            let nested_contract = make_deeply_nested_blocks_contact(depth);
            let cost = compute_gas_metering_cost(config, &nested_contract);
            xs1.push(depth as u64);
            ys1.push(cost);
        }
        {
            let loop_contract = make_simple_loop_contact(depth);
            let cost = compute_gas_metering_cost(config, &loop_contract);
            xs2.push(depth as u64);
            ys2.push(cost);
        }
    }

    let tolerance = LeastSquaresTolerance::default().factor_rel_nn_tolerance(0.001);
    let (cost1_base, cost1_op) =
        GasCost::least_squares_method_gas_cost(&xs1, &ys1, &tolerance, config.debug);
    let (cost2_base, cost2_op) =
        GasCost::least_squares_method_gas_cost(&xs2, &ys2, &tolerance, config.debug);

    let cost_base = std::cmp::max(cost1_base, cost2_base);
    let cost_op = std::cmp::max(cost1_op, cost2_op);
    (cost_base, cost_op)
}

fn make_deeply_nested_blocks_contact(depth: i32) -> ContractCode {
    // Build nested blocks structure.
    let mut blocks = String::new();
    for _ in 0..depth {
        write!(
            &mut blocks,
            "
            block
            "
        )
        .unwrap();
    }
    // Conditional branch forces 1 gas metering injection per block.
    for _ in 0..depth {
        write!(
            &mut blocks,
            "
            local.get 0
            i32.const 2
            i32.gt_s
            br_if 0
            local.get 0
            drop
            end
            "
        )
        .unwrap();
    }

    let code = format!(
        "
        (module
            (export \"hello\" (func 0))
              (func (;0;)
                (local i32)
                i32.const 1
                local.set 0
                {}
                return
              )
            )",
        blocks
    );
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

fn make_simple_loop_contact(depth: i32) -> ContractCode {
    let code = format!(
        "
        (module
            (export \"hello\" (func 0))
              (func (;0;)
                (local i32)
                i32.const {}
                local.set 0
                block
                  loop
                    local.get 0
                    i32.const 1
                    i32.sub
                    local.tee 0
                    i32.const 0
                    i32.gt_s
                    br_if 0
                    br 1
                  end
                end
              )
            )",
        depth
    );
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

/**
 * We compute the cost of gas metering operations in forward and backward branching contracts by
 * running contracts with and without gas metering and comparing the difference induced by gas
 * metering.
 */
pub(crate) fn compute_gas_metering_cost(config: &Config, contract: &ContractCode) -> GasCost {
    let gas_metric = config.metric;
    let repeats = config.iter_per_block as u64;
    let vm_kind = config.vm_kind;
    let warmup_repeats = config.warmup_iters_per_block;

    let store = near_store::test_utils::create_test_store();
    let cache_store = StoreCompiledContractCache::new(&store);
    let cache: Option<&dyn CompiledContractCache> = Some(&cache_store);
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(PROTOCOL_VERSION).as_ref();
    let vm_config_gas = runtime_config.wasm_config.clone();
    let runtime = vm_kind.runtime(vm_config_gas).expect("runtime has not been enabled");
    let runtime_free_gas = vm_kind.runtime(VMConfig::free()).expect("runtime has not been enabled");
    let fees = runtime_config.fees.clone();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let promise_results = vec![];

    // Warmup with gas metering
    for _ in 0..warmup_repeats {
        let result = runtime
            .run(
                contract,
                "hello",
                &mut fake_external,
                fake_context.clone(),
                &fees,
                &promise_results,
                PROTOCOL_VERSION,
                cache,
            )
            .expect("fatal_error");
        if let Some(err) = &result.aborted {
            eprintln!("error: {}", err);
        }
        assert!(result.aborted.is_none());
    }

    // Run with gas metering.
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let result = runtime
            .run(
                contract,
                "hello",
                &mut fake_external,
                fake_context.clone(),
                &fees,
                &promise_results,
                PROTOCOL_VERSION,
                cache,
            )
            .expect("fatal_error");
        assert!(result.aborted.is_none());
    }
    let total_raw_with_gas = start.elapsed();

    // Warmup without gas metering
    for _ in 0..warmup_repeats {
        let result = runtime_free_gas
            .run(
                contract,
                "hello",
                &mut fake_external,
                fake_context.clone(),
                &fees,
                &promise_results,
                PROTOCOL_VERSION,
                cache,
            )
            .expect("fatal_error");
        assert!(result.aborted.is_none());
    }

    // Run without gas metering.
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let result = runtime_free_gas
            .run(
                contract,
                "hello",
                &mut fake_external,
                fake_context.clone(),
                &fees,
                &promise_results,
                PROTOCOL_VERSION,
                cache,
            )
            .expect("fatal_error");
        assert!(result.aborted.is_none());
    }
    let total_raw_no_gas = start.elapsed();

    if total_raw_with_gas < total_raw_no_gas {
        // This might happen due to experimental error, especially when running
        // without warmup or too few iterations.
        let mut null_cost = GasCost::zero();
        null_cost.set_uncertain("NEGATIVE-COST");
        return null_cost;
    }

    (total_raw_with_gas - total_raw_no_gas) / repeats
}
