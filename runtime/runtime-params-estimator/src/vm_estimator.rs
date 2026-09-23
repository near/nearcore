use crate::config::GasMetric;
use crate::gas_cost::{GasCost, LeastSquaresTolerance};
use crate::{REAL_CONTRACTS_SAMPLE, utils::read_resource};
use near_parameters::vm::VMKind;
use near_parameters::{RuntimeConfigStore, RuntimeFeesConfig};
use near_primitives::types::{Balance, Gas};
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_runner::internal::VMKindExt;
use near_vm_runner::logic::VMContext;
use near_vm_runner::{
    ContractCode, ContractRuntimeCache, FilesystemContractRuntimeCache, MockContractRuntimeCache,
    NoContractRuntimeCache,
};
use std::sync::Arc;

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";
const REFUND_TO_ACCOUNT_ID: &str = "carol";

pub(crate) fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.parse().unwrap(),
        signer_account_id: SIGNER_ACCOUNT_ID.parse().unwrap(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.parse().unwrap(),
        refund_to_account_id: REFUND_TO_ACCOUNT_ID.parse().unwrap(),
        input: std::rc::Rc::from(input),
        promise_results: vec![].into(),
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 0,
        account_balance: Balance::from_yoctonear(2),
        account_locked_balance: Balance::from_yoctonear(1),
        storage_usage: 12,
        account_contract: near_primitives::account::AccountContract::None,
        attached_deposit: Balance::from_yoctonear(2),
        prepaid_gas: Gas::from_teragas(1_000_000),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

fn measure_contract(
    vm_kind: VMKind,
    gas_metric: GasMetric,
    contract: &ContractCode,
    cache: &dyn ContractRuntimeCache,
) -> GasCost {
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(PROTOCOL_VERSION).as_ref();
    let vm_config = runtime_config.wasm_config.clone();
    let start = GasCost::measure(gas_metric);
    let vm = vm_kind.runtime(vm_config).unwrap();
    let result = vm.precompile(contract, cache).unwrap();
    let end = start.elapsed();
    result.unwrap_or_else(|err| panic!("compilation failed, {err}"));
    end
}

/// Returns `(a, b)` - approximation coefficients for formula `a + b * x`
/// where `x` is the contract size in bytes. Practically, we compute upper bound
/// of this approximation, assuming that whole contract consists of code only.
fn precompilation_cost(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    verbose: bool,
) -> (GasCost, GasCost) {
    if cfg!(debug_assertions) {
        eprintln!("WARNING: did you pass --release flag, results do not make sense otherwise")
    }
    let cache_store1 = FilesystemContractRuntimeCache::test().unwrap();
    let cache_store2 = NoContractRuntimeCache;
    let use_store = true;
    let cache: &dyn ContractRuntimeCache = if use_store { &cache_store1 } else { &cache_store2 };
    let mut xs = vec![];
    let mut ys = vec![];

    for (path, _) in REAL_CONTRACTS_SAMPLE {
        let raw_bytes = read_resource(path);
        let contract = ContractCode::new(raw_bytes.to_vec(), None);
        xs.push(raw_bytes.len() as u64);
        ys.push(measure_contract(vm_kind, gas_metric, &contract, cache));
    }

    // Motivation behind these values is the same as in `fn action_deploy_contract_per_byte`.
    let negative_base_tolerance = Gas::from_gas(369_531_500_000u64);
    let rel_factor_tolerance = 0.001;
    let (a, b) = GasCost::least_squares_method_gas_cost(
        &xs,
        &ys,
        &LeastSquaresTolerance::default()
            .base_abs_nn_tolerance(negative_base_tolerance)
            .factor_rel_nn_tolerance(rel_factor_tolerance),
        verbose,
    );

    // We multiply `b` by 5/4 to accommodate for the fact that test contracts are typically 80% code,
    // so in the worst case it could grow to 100% and our costs still give better upper estimation.
    // Safety multiplication with 5/4.
    let safety_numer = 5u64;
    let safety_denom = 4u64;
    let (corrected_a, corrected_b) =
        (a * safety_numer / safety_denom, b * safety_numer / safety_denom);

    // Now validate that estimations obtained earlier provides correct upper estimation
    // for several other contracts.
    // Contracts binaries are taken from near-sdk-rs examples, ae20fc458858144e4a35faf58be778d13c2b0511.
    let validate_contracts = vec![
        // File 139637.
        read_resource("res/status_message.wasm"),
        // File 157010.
        read_resource("res/mission_control.wasm"),
        // File 218444.
        read_resource("res/fungible_token.wasm"),
    ];

    for raw_bytes in validate_contracts {
        let contract = ContractCode::new(raw_bytes.to_vec(), None);
        let x = raw_bytes.len() as u64;
        let y = measure_contract(vm_kind, gas_metric, &contract, cache);
        let expect = corrected_a.to_gas().as_gas() as i128
            + corrected_b.to_gas().as_gas() as i128 * (x as i128);
        let error = expect - (y.to_gas().as_gas() as i128);
        if gas_metric == GasMetric::ICount {
            // Time based metric may lead to unpredictable results.
            assert!(error >= 0);
        }
    }

    (corrected_a, corrected_b)
}

pub(crate) fn compile_single_contract_cost(
    metric: GasMetric,
    vm_kind: VMKind,
    contract_bytes: &[u8],
) -> GasCost {
    let contract = ContractCode::new(contract_bytes.to_vec(), None);
    let cache = FilesystemContractRuntimeCache::test().unwrap();
    measure_contract(vm_kind, metric, &contract, &cache)
}

pub(crate) fn compute_compile_cost_vm(
    metric: GasMetric,
    vm_kind: VMKind,
    verbose: bool,
) -> (GasCost, GasCost) {
    let (a, b) = precompilation_cost(metric, vm_kind, verbose);
    let base = a.to_gas();
    let per_byte = b.to_gas();
    if verbose {
        println!(
            "{:?} using {:?}: in a + b * x: a = {:?}, b = {:?}, base = {} gas, per_byte = {} gas",
            vm_kind, metric, a, b, base, per_byte
        );
    }
    (a, b)
}

pub(crate) fn adversarial_compile_max_blocks(metric: GasMetric, vm_kind: VMKind) -> GasCost {
    let code = near_test_contracts::max_blocks_contract(10, 4_999);
    compile_single_contract_cost(metric, vm_kind, &code)
}

pub(crate) fn adversarial_load_many_globals(metric: GasMetric, vm_kind: VMKind) -> GasCost {
    let code = near_test_contracts::contract_with_num_globals(50_000);
    measure_instantiation_overhead(metric, vm_kind, &code)
}

pub(crate) fn adversarial_load_many_data_segments(metric: GasMetric, vm_kind: VMKind) -> GasCost {
    let code = near_test_contracts::many_data_segments_contract(50_000);
    measure_instantiation_overhead(metric, vm_kind, &code)
}

pub(crate) fn adversarial_load_many_element_segments(
    metric: GasMetric,
    vm_kind: VMKind,
) -> GasCost {
    let code = near_test_contracts::many_element_segments_contract(10_000);
    measure_instantiation_overhead(metric, vm_kind, &code)
}

/// Warm the compile cache, then measure N invocations (instantiation + trivial execution).
/// The function body is a bare `end`, so execution cost is negligible.
fn measure_instantiation_overhead(
    metric: GasMetric,
    vm_kind: VMKind,
    contract_bytes: &[u8],
) -> GasCost {
    let config_store = RuntimeConfigStore::new(None);
    let mut config = config_store.get_config(PROTOCOL_VERSION).wasm_config.as_ref().clone();
    config.vm_kind = vm_kind;
    let config = Arc::new(config);
    let fees = Arc::new(RuntimeFeesConfig::test());
    let code = ContractCode::new(contract_bytes.to_vec(), None);
    let cache = MockContractRuntimeCache::default();
    let mut fake_external = near_vm_runner::logic::mocks::mock_external::MockedExternal::with_code(
        code.clone_for_tests(),
    );

    let mut run_once = || {
        let context = create_context(vec![]);
        let gas_counter = context.make_gas_counter(&config);
        vm_kind
            .runtime(config.clone())
            .unwrap()
            .prepare(&fake_external, Some(&cache), gas_counter, "main")
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("fatal_error")
    };

    // Warm: compiles and caches the module; subsequent calls only instantiate + execute.
    run_once();

    let n = 10_usize;
    let start = GasCost::measure(metric);
    for _ in 0..n {
        run_once();
    }
    start.elapsed() / n as u64
}

pub(crate) fn op_float_nan_canonicalization(metric: GasMetric, vm_kind: VMKind) -> GasCost {
    let code = near_test_contracts::float_nan_loop_contract();
    measure_op_loop(metric, vm_kind, &code)
}

pub(crate) fn op_int_baseline(metric: GasMetric, vm_kind: VMKind) -> GasCost {
    let code = near_test_contracts::int_baseline_loop_contract();
    measure_op_loop(metric, vm_kind, &code)
}

pub(crate) fn op_wide_arithmetic(metric: GasMetric, vm_kind: VMKind) -> GasCost {
    let code = near_test_contracts::wide_arithmetic_loop_contract();
    measure_op_loop(metric, vm_kind, &code)
}

/// Compile + run an infinite loop until gas exhaustion (100 Tgas), return ns
/// per WASM instruction.
fn measure_op_loop(metric: GasMetric, vm_kind: VMKind, contract_bytes: &[u8]) -> GasCost {
    let config_store = RuntimeConfigStore::new(None);
    let mut config = config_store.get_config(PROTOCOL_VERSION).wasm_config.as_ref().clone();
    let gas_limit = Gas::from_teragas(100);
    config.limit_config.max_gas_burnt = gas_limit;
    config.vm_kind = vm_kind;
    let config = Arc::new(config);
    let fees = Arc::new(RuntimeFeesConfig::test());
    let code = ContractCode::new(contract_bytes.to_vec(), None);
    let cache = MockContractRuntimeCache::default();
    let mut fake_external = near_vm_runner::logic::mocks::mock_external::MockedExternal::with_code(
        code.clone_for_tests(),
    );

    let mut run_once = || {
        let mut context = create_context(vec![]);
        context.prepaid_gas = gas_limit;
        let gas_counter = context.make_gas_counter(&config);
        let result = vm_kind
            .runtime(config.clone())
            .unwrap()
            .prepare(&fake_external, Some(&cache), gas_counter, "main")
            .run(&mut fake_external, &context, Arc::clone(&fees))
            .expect("fatal_error");
        assert!(result.aborted.is_some(), "expected gas exhaustion but contract finished cleanly");
        result
    };

    let warmup = run_once();
    let burnt_gas = warmup.burnt_gas.as_gas();
    assert!(
        burnt_gas > 0,
        "loop burnt 0 gas — method not found or gas budget exhausted before first instruction; \
         aborted={:?}",
        warmup.aborted,
    );
    let instructions = burnt_gas / u64::from(config.regular_op_cost);
    assert!(
        instructions > 0,
        "gas budget too small: burnt {} gas but regular_op_cost={} gas/op — \
         increase max_gas_burnt in measure_op_loop",
        burnt_gas,
        config.regular_op_cost,
    );

    let n = 5_usize;
    let start = GasCost::measure(metric);
    for _ in 0..n {
        run_once();
    }
    let result = start.elapsed() / (instructions * n as u64);
    result
}
