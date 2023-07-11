use crate::config::GasMetric;
use crate::gas_cost::{GasCost, LeastSquaresTolerance};
use crate::{utils::read_resource, REAL_CONTRACTS_SAMPLE};
use near_primitives::contract::ContractCode;
use near_primitives::hash::CryptoHash;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::StoreCompiledContractCache;
use near_vm_runner::internal::VMKind;
use near_vm_runner::logic::VMContext;
use near_vm_runner::logic::{CompiledContract, CompiledContractCache};

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

pub(crate) fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.parse().unwrap(),
        signer_account_id: SIGNER_ACCOUNT_ID.parse().unwrap(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.parse().unwrap(),
        input,
        block_height: 10,
        block_timestamp: 42,
        epoch_height: 0,
        account_balance: 2u128,
        account_locked_balance: 1u128,
        storage_usage: 12,
        attached_deposit: 2u128,
        prepaid_gas: 10_u64.pow(18),
        random_seed: vec![0, 1, 2],
        view_config: None,
        output_data_receivers: vec![],
    }
}

fn measure_contract(
    vm_kind: VMKind,
    gas_metric: GasMetric,
    contract: &ContractCode,
    cache: &dyn CompiledContractCache,
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

#[derive(Default, Clone)]
struct MockCompiledContractCache;

impl CompiledContractCache for MockCompiledContractCache {
    fn put(&self, _key: &CryptoHash, _value: CompiledContract) -> std::io::Result<()> {
        Ok(())
    }

    fn get(&self, _key: &CryptoHash) -> std::io::Result<Option<CompiledContract>> {
        Ok(None)
    }
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
    let cache_store1: StoreCompiledContractCache;
    let cache_store2 = MockCompiledContractCache;
    let use_store = true;
    let cache: &dyn CompiledContractCache = if use_store {
        let store = near_store::test_utils::create_test_store();
        cache_store1 = StoreCompiledContractCache::new(&store);
        &cache_store1
    } else {
        &cache_store2
    };
    let mut xs = vec![];
    let mut ys = vec![];

    for (path, _) in REAL_CONTRACTS_SAMPLE {
        let raw_bytes = read_resource(path);
        let contract = ContractCode::new(raw_bytes.to_vec(), None);
        xs.push(raw_bytes.len() as u64);
        ys.push(measure_contract(vm_kind, gas_metric, &contract, cache));
    }

    // Motivation behind these values is the same as in `fn action_deploy_contract_per_byte`.
    let negative_base_tolerance = 369_531_500_000u64;
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
    // Safety muliplication with 5/4.
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
        let expect = corrected_a.to_gas() as i128 + corrected_b.to_gas() as i128 * (x as i128);
        let error = expect - (y.to_gas() as i128);
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

    let store = near_store::test_utils::create_test_store();
    let cache = StoreCompiledContractCache::new(&store);

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
