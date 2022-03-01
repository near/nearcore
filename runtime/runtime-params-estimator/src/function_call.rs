use crate::config::{Config, GasMetric};
use crate::gas_cost::{GasCost, LeastSquaresTolerance};
use crate::vm_estimator::create_context;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::{CompiledContractCache, ProtocolVersion};
use near_store::StoreCompiledContractCache;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_runner::internal::VMKind;
use std::fmt::Write;
use std::sync::Arc;

/// Estimates linear cost curve for a function call execution cost per byte of
/// total contract code. The contract size is increased by adding more methods
/// to it.
pub(crate) fn function_call_cost_per_code_byte(config: &Config) -> (GasCost, GasCost) {
    let mut xs = vec![];
    let mut ys = vec![];
    let repeats = config.iter_per_block as u64;
    let warmup_repeats = config.warmup_iters_per_block as u64;
    for method_count in vec![5, 20, 30, 50, 100, 200, 1000] {
        let contract = make_many_methods_contract(method_count);
        let cost = compute_function_call_cost(
            config.metric,
            config.vm_kind,
            repeats,
            warmup_repeats,
            &contract,
        );
        xs.push(contract.code().len() as u64);
        ys.push(cost / repeats);
    }

    let tolerance = LeastSquaresTolerance::default();
    GasCost::least_squares_method_gas_cost(&xs, &ys, &tolerance, false)
}

fn make_many_methods_contract(method_count: i32) -> ContractCode {
    let mut methods = String::new();
    for i in 0..method_count {
        write!(
            &mut methods,
            "
            (export \"hello{}\" (func {i}))
              (func (;{i};)
                i32.const {i}
                drop
                return
              )
            ",
            i = i
        )
        .unwrap();
    }

    let code = format!(
        "
        (module
            {}
            )",
        methods
    );
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

fn compute_function_call_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    repeats: u64,
    warmup_repeats: u64,
    contract: &ContractCode,
) -> GasCost {
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = near_store::StoreOpener::with_default_config(workdir.path()).open();
    let cache_store = Arc::new(StoreCompiledContractCache { store });
    let cache: Option<&dyn CompiledContractCache> = Some(cache_store.as_ref());
    let protocol_version = ProtocolVersion::MAX;
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(protocol_version).as_ref();
    let vm_config = runtime_config.wasm_config.clone();
    let runtime = vm_kind.runtime(vm_config).expect("runtime has not been enabled");
    let fees = runtime_config.transaction_costs.clone();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let promise_results = vec![];

    // Warmup.
    for _ in 0..warmup_repeats {
        let result = runtime.run(
            contract,
            "hello0",
            &mut fake_external,
            fake_context.clone(),
            &fees,
            &promise_results,
            protocol_version,
            cache,
        );
        assert!(result.error().is_none());
    }
    // Run with gas metering.
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let result = runtime.run(
            contract,
            "hello0",
            &mut fake_external,
            fake_context.clone(),
            &fees,
            &promise_results,
            protocol_version,
            cache,
        );
        assert!(result.error().is_none());
    }
    start.elapsed()
}
