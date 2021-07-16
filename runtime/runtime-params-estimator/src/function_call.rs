use crate::cases::ratio_to_gas_signed;
use crate::testbed_runners::{end_count, start_count, GasMetric};
use crate::vm_estimator::{create_context, least_squares_method};
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::profile::ProfileData;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::{CompiledContractCache, ProtocolVersion};
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_runner::{run_vm, VMKind};
use nearcore::get_store_path;
use std::fmt::Write;
use std::sync::Arc;
use near_test_contracts::aurora_contract;

#[allow(dead_code)]
fn test_function_call(metric: GasMetric, vm_kind: VMKind) {
    let mut xs = vec![];
    let mut ys = vec![];
    const REPEATS: i32 = 50;
    for method_count in vec![5, 20, 30, 50, 100, 200, 1000] {
        let contract = make_many_methods_contact(method_count);
        let cost =
            compute_function_call_cost(metric, vm_kind, REPEATS, &contract);
        println!("{:?} {:?} {} {}", vm_kind, metric, method_count, cost / (REPEATS as u64));
        xs.push(contract.get_code().len() as u64);
        ys.push(cost / (REPEATS as u64));
    }

    // Regression analysis only makes sense for additive metrics.
    if metric == GasMetric::Time {
        return;
    }

    let (cost_base, cost_byte, _) = least_squares_method(&xs, &ys);

    println!(
        "{:?} {:?} function call base {} gas, per byte {} gas",
        vm_kind, metric,
        ratio_to_gas_signed(metric, cost_base),
        ratio_to_gas_signed(metric, cost_byte),
    );
}

#[test]
fn test_function_call_time() {
    // Run with
    // cargo test --release --lib function_call::test_function_call_time -- --exact --nocapture
    test_function_call(GasMetric::Time, VMKind::Wasmer0);
    test_function_call(GasMetric::Time, VMKind::Wasmer1);
    test_function_call(GasMetric::Time, VMKind::Wasmtime);

}

#[test]
fn test_function_call_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh \
    // cargo test --release --features no_cpu_compatibility_checks \
    // --lib function_call::test_function_call_icount -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    test_function_call(GasMetric::ICount, VMKind::Wasmer0);
    test_function_call(GasMetric::ICount, VMKind::Wasmer1);
    test_function_call(GasMetric::ICount, VMKind::Wasmtime);
}

fn make_many_methods_contact(method_count: i32) -> ContractCode {
    let _ = method_count;
    // let mut methods = String::new();
    // for i in 0..method_count {
    //     write!(
    //         &mut methods,
    //         "
    //         (export \"hello{}\" (func {}))
    //           (func (;{};)
    //             i32.const {}
    //             drop
    //             return
    //           )
    //         ", i, i, i, i)
    //         .unwrap();
    // }
    //
    // let code = format!(
    //     "
    //     (module
    //         {}
    //         )",
    //     methods
    // );
    // ContractCode::new(wabt::wat2wasm(code.as_bytes()).unwrap(), None)
    ContractCode::new(aurora_contract().iter().cloned().collect(), None)
}

pub fn compute_function_call_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    repeats: i32,
    contract: &ContractCode,
) -> u64 {
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let vm_config = VMConfig::default();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];

    // Warmup.
    let result = run_vm(
        &contract,
        "state_migration",
        &mut fake_external,
        fake_context.clone(),
        &vm_config,
        &fees,
        &promise_results,
        vm_kind,
        ProtocolVersion::MAX,
        cache,
        ProfileData::new(),
    );
    assert!(result.1.is_none());

    // Run with gas metering.
    let start = start_count(gas_metric);
    for _ in 0..repeats {
        let result = run_vm(
            &contract,
            "state_migration",
            &mut fake_external,
            fake_context.clone(),
            &vm_config,
            &fees,
            &promise_results,
            vm_kind,
            ProtocolVersion::MAX,
            cache,
            ProfileData::new(),
        );
        assert!(result.1.is_none());
    }
    let total_raw = end_count(gas_metric, &start) as i128;

    println!("cost is {}", total_raw);

    total_raw as u64
}
