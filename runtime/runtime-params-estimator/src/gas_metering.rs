use crate::testbed_runners::{end_count, start_count, GasMetric};
use crate::vm_estimator::create_context;
use near_primitives::config::VMConfig;
use near_primitives::contract::ContractCode;
use near_primitives::profile::ProfileData;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::{CompiledContractCache, ProtocolVersion};
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_runner::{run_vm, VMKind};
use nearcore::get_store_path;
use std::sync::Arc;

#[allow(dead_code)]
fn test_gas_metering_cost(metric: GasMetric) {
    let cost = compute_gas_metering_cost(metric, VMKind::Wasmer0, true);
    println!("{:?}", cost);
}

#[test]
fn test_gas_metering_cost_time() {
    test_gas_metering_cost(GasMetric::Time)
}

#[test]
fn test_gas_metering_cost_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh cargo test --release \
    // --lib vm_estimator::test_gas_metering_cost --no-fail-fast -- --exact --nocapture
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=/host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/libcounter.so $@
    test_gas_metering_cost(GasMetric::ICount)
}

fn make_contract(_depth: i32) -> ContractCode {
    let v = wabt::wat2wasm(
        r#"
            (module
              (import "env" "prepaid_gas" (func (;0;) (result i64)))
              (export "hello" (func 1))
              (func (;1;)
                  (drop (call 0))
                  )
            )"#,
    )
    .unwrap();
    ContractCode::new(v, None)
}

pub fn compute_gas_metering_cost(gas_metric: GasMetric, vm_kind: VMKind, _verbose: bool) -> u64 {
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let vm_config = VMConfig::default();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let fees = RuntimeFeesConfig::default();
    let promise_results = vec![];

    let contract = make_contract(100);

    // Warmup.
    let result = run_vm(
        &contract,
        "hello",
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

    let start = start_count(gas_metric);
    let result = run_vm(
        &contract,
        "hello",
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
    let total_raw = end_count(gas_metric, &start) as i128;
    total_raw as u64
}
