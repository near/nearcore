use crate::gas_cost::{ratio_to_gas_signed, GasCost};
use crate::testbed_runners::GasMetric;
use crate::vm_estimator::{create_context, least_squares_method};
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_runner::internal::VMKind;
use nearcore::get_store_path;
use num_traits::ToPrimitive;
use std::fmt::Write;
use std::sync::Arc;

#[allow(dead_code)]
fn test_gas_metering_cost(metric: GasMetric) {
    const REPEATS: i32 = 1000;
    let mut xs1 = vec![];
    let mut ys1 = vec![];
    let mut xs2 = vec![];
    let mut ys2 = vec![];
    for depth in vec![1, 10, 20, 30, 50, 100, 200, 1000] {
        if true {
            // Here we test gas metering costs for forward branch cases.
            let nested_contract = make_deeply_nested_blocks_contact(depth);
            let cost =
                compute_gas_metering_cost(metric, VMKind::Wasmer0, REPEATS, &nested_contract);
            println!("nested {} {}", depth, cost / (REPEATS as u64));
            xs1.push(depth as u64);
            ys1.push(cost);
        }
        if true {
            let loop_contract = make_simple_loop_contact(depth);
            let cost = compute_gas_metering_cost(metric, VMKind::Wasmer0, REPEATS, &loop_contract);
            println!("loop {} {}", depth, cost / (REPEATS as u64));
            xs2.push(depth as u64);
            ys2.push(cost);
        }
    }

    // Regression analysis only makes sense for additive metrics.
    if metric == GasMetric::Time {
        return;
    }

    let (cost1_base, cost1_op, _) = least_squares_method(&xs1, &ys1);
    let (cost2_base, cost2_op, _) = least_squares_method(&xs2, &ys2);

    println!(
        "forward branches: {} gas base {} gas per op",
        ratio_to_gas_signed(metric, cost1_base),
        ratio_to_gas_signed(metric, cost1_op),
    );
    println!(
        "backward branches: {} gas base {} gas per op",
        ratio_to_gas_signed(metric, cost2_base),
        ratio_to_gas_signed(metric, cost2_op),
    );
}

#[test]
fn test_gas_metering_cost_time() {
    // Run with
    // cargo test --release --lib gas_metering::test_gas_metering_cost_time -- --exact --nocapture
    test_gas_metering_cost(GasMetric::Time)
}

#[test]
fn test_gas_metering_cost_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh \
    // cargo test --release --features no_cpu_compatibility_checks \
    // --lib gas_metering::test_gas_metering_cost_icount -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    test_gas_metering_cost(GasMetric::ICount)
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
    let runtime = vm_kind.runtime().expect("runtime has not been enabled");
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(PROTOCOL_VERSION).as_ref();
    let vm_config_gas = runtime_config.wasm_config.clone();
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
        &vm_config_gas,
        &fees,
        &promise_results,
        PROTOCOL_VERSION,
        cache,
    );
    assert!(result.1.is_none());

    // Run with gas metering.
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let result = runtime.run(
            contract,
            "hello",
            &mut fake_external,
            fake_context.clone(),
            &vm_config_gas,
            &fees,
            &promise_results,
            PROTOCOL_VERSION,
            cache,
        );
        assert!(result.1.is_none());
    }
    let total_raw_with_gas = start.elapsed().scalar_cost().to_i128().unwrap();

    let vm_config_no_gas = VMConfig::free();
    let result = runtime.run(
        contract,
        "hello",
        &mut fake_external,
        fake_context.clone(),
        &vm_config_no_gas,
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
            &vm_config_no_gas,
            &fees,
            &promise_results,
            PROTOCOL_VERSION,
            cache,
        );
        assert!(result.1.is_none());
    }
    let total_raw_no_gas = start.elapsed().scalar_cost().to_i128().unwrap();

    // println!("with gas: {}; no gas {}", total_raw_with_gas, total_raw_no_gas);

    (total_raw_with_gas - total_raw_no_gas) as u64
}
