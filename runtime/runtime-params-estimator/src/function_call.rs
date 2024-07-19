use crate::config::{Config, GasMetric};
use crate::gas_cost::{GasCost, LeastSquaresTolerance};
use crate::vm_estimator::create_context;
use near_parameters::vm::VMKind;
use near_parameters::RuntimeConfigStore;
use near_primitives::types::ProtocolVersion;
use near_vm_runner::internal::VMKindExt;
use near_vm_runner::logic::mocks::mock_external::MockedExternal;
use near_vm_runner::{ContractCode, ContractRuntimeCache, FilesystemContractRuntimeCache};
use std::fmt::Write;
use std::sync::Arc;

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
    let cache_store = FilesystemContractRuntimeCache::test().unwrap();
    let cache: Option<&dyn ContractRuntimeCache> = Some(&cache_store);
    let protocol_version = ProtocolVersion::MAX;
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(protocol_version).as_ref();
    let vm_config = runtime_config.wasm_config.clone();
    let fees = runtime_config.fees.clone();
    let mut fake_external = MockedExternal::with_code(contract.clone_for_tests());
    let fake_context = create_context(vec![]);

    // Warmup.
    for _ in 0..warmup_repeats {
        let gas_counter = fake_context.make_gas_counter(&vm_config);
        let runtime = vm_kind.runtime(vm_config.clone()).expect("runtime has not been enabled");
        let result = runtime
            .prepare(&fake_external, cache, gas_counter, "hello0")
            .run(&mut fake_external, &fake_context, Arc::clone(&fees))
            .expect("fatal error");
        assert!(result.aborted.is_none());
    }
    // Run with gas metering.
    let start = GasCost::measure(gas_metric);
    for _ in 0..repeats {
        let gas_counter = fake_context.make_gas_counter(&vm_config);
        let runtime = vm_kind.runtime(vm_config.clone()).expect("runtime has not been enabled");
        let result = runtime
            .prepare(&fake_external, cache, gas_counter, "hello0")
            .run(&mut fake_external, &fake_context, Arc::clone(&fees))
            .expect("fatal_error");
        assert!(result.aborted.is_none());
    }
    start.elapsed()
}
