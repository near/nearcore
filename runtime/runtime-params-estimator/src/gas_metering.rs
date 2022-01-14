use crate::config::GasMetric;
use crate::gas_cost::GasCost;
use crate::vm_estimator::{create_context, least_squares_method};
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::{CompiledContractCache, Gas};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_runner::internal::VMKind;
use nearcore::get_store_path;
use std::fmt::Write;
use std::sync::Arc;

pub(crate) fn gas_metering_cost(metric: GasMetric, vm_kind: VMKind) -> (Gas, Gas) {
    const REPEATS: i32 = 1000;
    let mut xs1 = vec![];
    let mut ys1 = vec![];
    let mut xs2 = vec![];
    let mut ys2 = vec![];
    for depth in vec![1, 10, 20, 30, 50, 100, 200, 1000] {
        if true {
            // Here we test gas metering costs for forward branch cases.
            let nested_contract = make_deeply_nested_blocks_contact(depth);
            let cost = compute_gas_metering_cost(metric, vm_kind, REPEATS, &nested_contract);
            xs1.push(depth as u64);
            ys1.push(cost);
        }
        if true {
            let loop_contract = make_simple_loop_contact(depth);
            let cost = compute_gas_metering_cost(metric, vm_kind, REPEATS, &loop_contract);
            xs2.push(depth as u64);
            ys2.push(cost);
        }
    }

    // Regression analysis only makes sense for additive metrics.
    if metric == GasMetric::Time {
        return (0, 0);
    }

    let (cost1_base, cost1_op, _) = least_squares_method(&xs1, &ys1);
    let (cost2_base, cost2_op, _) = least_squares_method(&xs2, &ys2);

    #[cfg(test)]
    println!("forward branches: {} gas base {} gas per op", cost1_base, cost1_op,);
    #[cfg(test)]
    println!("backward branches: {} gas base {} gas per op", cost2_base, cost2_op,);

    let cost_base = std::cmp::max(cost1_base, cost2_base).round().to_integer() as u64;
    let cost_op = std::cmp::max(cost1_op, cost2_op).round().to_integer() as u64;
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
pub fn compute_gas_metering_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    repeats: i32,
    contract: &ContractCode,
) -> u64 {
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(PROTOCOL_VERSION).as_ref();
    let vm_config_gas = runtime_config.wasm_config.clone();
    let runtime = vm_kind.runtime(vm_config_gas).expect("runtime has not been enabled");
    let fees = runtime_config.transaction_costs.clone();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let promise_results = vec![];

    // Warmup.
    let result = runtime.run(
        contract,
        "hello",
        &mut fake_external,
        fake_context.clone(),
        &fees,
        &promise_results,
        PROTOCOL_VERSION,
        cache,
    );
    if result.1.is_some() {
        let err = result.1.as_ref().unwrap();
        eprintln!("error: {}", err);
    }
    assert!(result.1.is_none());

    // Run with gas metering.
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let result = runtime.run(
            contract,
            "hello",
            &mut fake_external,
            fake_context.clone(),
            &fees,
            &promise_results,
            PROTOCOL_VERSION,
            cache,
        );
        assert!(result.1.is_none());
    }
    let total_raw_with_gas = start.elapsed().to_gas();

    let vm_config_no_gas = VMConfig::free();
    let runtime = vm_kind.runtime(vm_config_no_gas).expect("runtime has not been enabled");
    let result = runtime.run(
        contract,
        "hello",
        &mut fake_external,
        fake_context.clone(),
        &fees,
        &promise_results,
        PROTOCOL_VERSION,
        cache,
    );
    assert!(result.1.is_none());
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let result = runtime.run(
            contract,
            "hello",
            &mut fake_external,
            fake_context.clone(),
            &fees,
            &promise_results,
            PROTOCOL_VERSION,
            cache,
        );
        assert!(result.1.is_none());
    }
    let total_raw_no_gas = start.elapsed().to_gas();

    // TODO: This seems to fail almost always but has some non-determinism to it.
    assert!(
        total_raw_with_gas > total_raw_no_gas,
        "Cost with gas metering should be higher than without. Metric: {:?}. Estimated with gas metering: {}, without: {}",
        gas_metric,
        total_raw_with_gas,
        total_raw_no_gas
    );

    total_raw_with_gas - total_raw_no_gas
}
