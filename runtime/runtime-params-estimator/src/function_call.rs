use crate::config::GasMetric;
use crate::gas_cost::GasCost;
use crate::least_squares::least_squares_method;
use crate::vm_estimator::create_context;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::{CompiledContractCache, Gas, ProtocolVersion};
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_runner::internal::VMKind;
use nearcore::get_store_path;
use num_rational::Ratio;
use std::fmt::Write;
use std::sync::Arc;

pub(crate) fn test_function_call(metric: GasMetric, vm_kind: VMKind) -> (Ratio<i128>, Ratio<i128>) {
    let mut xs = vec![];
    let mut ys = vec![];
    const REPEATS: u64 = 50;
    for method_count in vec![5, 20, 30, 50, 100, 200, 1000] {
        let contract = make_many_methods_contract(method_count);
        let cost = compute_function_call_cost(metric, vm_kind, REPEATS, &contract);
        xs.push(contract.code().len() as u64);
        ys.push(cost / REPEATS);
    }

    // Regression analysis only makes sense for additive metrics.
    if metric == GasMetric::Time {
        return (0.into(), 0.into());
    }

    let (cost_base, cost_byte, _) = least_squares_method(&xs, &ys);
    (cost_base, cost_byte)
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

pub fn compute_function_call_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    repeats: u64,
    contract: &ContractCode,
) -> Gas {
    let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
    let store = create_store(&get_store_path(workdir.path()));
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
    if repeats != 1 {
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
    start.elapsed().to_gas()
}
