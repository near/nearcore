use crate::config::{Config, GasMetric};
use crate::gas_cost::{GasCost, LeastSquaresTolerance};
use crate::vm_estimator::create_context;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::ProtocolVersion;
use near_store::StoreCompiledContractCache;
use near_vm_runner::internal::VMKind;
use near_vm_runner::logic::mocks::mock_external::MockedExternal;
use near_vm_runner::logic::CompiledContractCache;
use std::fmt::Write;

/// Estimates linear cost curve for a function call execution cost per byte of
/// total contract code. The contract size is increased by adding more methods
/// to it. This cost is pure VM cost, without the loading from storage.
pub(crate) fn contract_loading_cost(config: &Config) -> (GasCost, GasCost) {
    let mut xs = vec![];
    let mut ys = vec![];
    let repeats = config.iter_per_block as u64;
    let warmup_repeats = config.warmup_iters_per_block as u64;
    for method_count in [5, 20, 30, 50, 100, 200, 1000] {
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
    GasCost::least_squares_method_gas_cost(&xs, &ys, &tolerance, config.debug)
}

fn make_many_methods_contract(method_count: i32) -> ContractCode {
    let mut methods = String::new();
    for i in 0..method_count {
        write!(
            &mut methods,
            r#"
            (export "hello{i}" (func {i}))
              (func
                i32.const {i}
                drop
                return
              )
            "#,
        )
        .unwrap();
    }

    let code = format!("(module {methods})");
    ContractCode::new(wat::parse_str(code).unwrap(), None)
}

fn compute_function_call_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    repeats: u64,
    warmup_repeats: u64,
    contract: &ContractCode,
) -> GasCost {
    let store = near_store::test_utils::create_test_store();
    let cache_store = StoreCompiledContractCache::new(&store);
    let cache: Option<&dyn CompiledContractCache> = Some(&cache_store);
    let protocol_version = ProtocolVersion::MAX;
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(protocol_version).as_ref();
    let vm_config = runtime_config.wasm_config.clone();
    let runtime = vm_kind.runtime(vm_config).expect("runtime has not been enabled");
    let fees = runtime_config.fees.clone();
    let mut fake_external = MockedExternal::new();
    let fake_context = create_context(vec![]);
    let promise_results = vec![];

    // Warmup.
    for _ in 0..warmup_repeats {
        let result = runtime
            .run(
                contract,
                "hello0",
                &mut fake_external,
                fake_context.clone(),
                &fees,
                &promise_results,
                protocol_version,
                cache,
            )
            .expect("fatal error");
        assert!(result.aborted.is_none());
    }
    // Run with gas metering.
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let result = runtime
            .run(
                contract,
                "hello0",
                &mut fake_external,
                fake_context.clone(),
                &fees,
                &promise_results,
                protocol_version,
                cache,
            )
            .expect("fatal_error");
        assert!(result.aborted.is_none());
    }
    start.elapsed()
}
