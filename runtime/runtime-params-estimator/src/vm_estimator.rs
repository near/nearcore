use crate::config::GasMetric;
use crate::gas_cost::GasCost;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::types::{CompiledContractCache, Gas};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::VMContext;
use near_vm_runner::internal::VMKind;
use near_vm_runner::precompile_contract_vm;
use nearcore::get_store_path;
use num_rational::Ratio;
use num_traits::ToPrimitive;
use std::sync::Arc;
use walrus::Result;

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
        block_index: 10,
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
    cache: Option<&dyn CompiledContractCache>,
) -> Gas {
    let config_store = RuntimeConfigStore::new(None);
    let runtime_config = config_store.get_config(PROTOCOL_VERSION).as_ref();
    let vm_config = runtime_config.wasm_config.clone();
    let start = GasCost::measure(gas_metric);
    let result = precompile_contract_vm(vm_kind, contract, &vm_config, cache);
    let end = start.elapsed();
    assert!(result.is_ok(), "Compilation failed");
    end.to_gas()
}

#[derive(Default, Clone)]
struct MockCompiledContractCache {}

impl CompiledContractCache for MockCompiledContractCache {
    fn put(&self, _key: &[u8], _value: &[u8]) -> Result<(), std::io::Error> {
        Ok(())
    }

    fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>, std::io::Error> {
        Ok(None)
    }
}

pub(crate) fn least_squares_method(
    xs: &Vec<u64>,
    ys: &Vec<u64>,
) -> (Ratio<i128>, Ratio<i128>, Vec<i128>) {
    let n = xs.len();
    let n128 = n as i128;

    let mut sum_prod = 0 as i128; // Sum of x * y.
    for i in 0..n {
        sum_prod = sum_prod + (xs[i] as i128) * (ys[i] as i128);
    }
    let mut sum_x = 0 as i128; // Sum of x.
    for i in 0..n {
        sum_x = sum_x + (xs[i] as i128);
    }
    let mut sum_y = 0 as i128; // Sum of y.
    for i in 0..n {
        sum_y = sum_y + (ys[i] as i128);
    }
    let mut sum_x_square = 0 as i128; // Sum of x^2.
    for i in 0..n {
        sum_x_square = sum_x_square + (xs[i] as i128) * (xs[i] as i128);
    }
    let b = Ratio::new(n128 * sum_prod - sum_x * sum_y, n128 * sum_x_square - sum_x * sum_x);
    let a = Ratio::new(sum_y * b.denom() - b.numer() * sum_x, n128 * b.denom());

    // Compute error estimations
    let mut errs = vec![];
    for i in 0..n {
        let expect = (a + b * (xs[i] as i128)).to_integer();
        let diff = expect - (ys[i] as i128);
        errs.push(diff);
    }

    (a, b, errs)
}

/// Returns `(a, b)` - approximation coefficients for formula `a + b * x`
/// where `x` is the contract size in bytes. Practically, we compute upper bound
/// of this approximation, assuming that whole contract consists of code only.
fn precompilation_cost(gas_metric: GasMetric, vm_kind: VMKind) -> (Ratio<i128>, Ratio<i128>) {
    if cfg!(debug_assertions) {
        eprintln!("WARNING: did you pass --release flag, results do not make sense otherwise")
    }
    let cache_store1: Arc<StoreCompiledContractCache>;
    let cache_store2: Arc<MockCompiledContractCache>;
    let cache: Option<&dyn CompiledContractCache>;
    let use_file_store = true;
    if use_file_store {
        let workdir = tempfile::Builder::new().prefix("runtime_testbed").tempdir().unwrap();
        let store = create_store(&get_store_path(workdir.path()));
        cache_store1 = Arc::new(StoreCompiledContractCache { store });
        cache = Some(cache_store1.as_ref());
    } else {
        cache_store2 = Arc::new(MockCompiledContractCache {});
        cache = Some(cache_store2.as_ref());
    }
    let mut xs = vec![];
    let mut ys = vec![];

    // We use core-contracts, e2f60b5b0930a9df2c413e1460e179c65c8876e3.
    let measure_contracts = vec![
        // File 341191, code 279965, data 56627.
        &include_bytes!("../test-contract/res/lockup_contract.wasm")[..],
        // File 257516, code 203545, data 50419.
        &include_bytes!("../test-contract/res/staking_pool.wasm")[..],
        // File 135358, code 113152, data 19520.
        &include_bytes!("../test-contract/res/voting_contract.wasm")[..],
        // File 124250, code 103473, data 18176.
        &include_bytes!("../test-contract/res/whitelist.wasm")[..],
    ];

    for raw_bytes in measure_contracts {
        let contract = ContractCode::new(raw_bytes.to_vec(), None);
        xs.push(raw_bytes.len() as u64);
        ys.push(measure_contract(vm_kind, gas_metric, &contract, cache));
    }

    let (a, b, _) = least_squares_method(&xs, &ys);

    // We multiply `b` by 5/4 to accommodate for the fact that test contracts are typically 80% code,
    // so in the worst case it could grow to 100% and our costs still give better upper estimation.
    let safety = Ratio::new(5i128, 4i128); // 5/4.
    let (corrected_a, corrected_b) = (a * safety, b * safety);

    // Now validate that estimations obtained earlier provides correct upper estimation
    // for several other contracts.
    // Contracts binaries are taken from near-sdk-rs examples, ae20fc458858144e4a35faf58be778d13c2b0511.
    let validate_contracts = vec![
        // File 139637.
        &include_bytes!("../test-contract/res/status_message.wasm")[..],
        // File 157010.
        &include_bytes!("../test-contract/res/mission_control.wasm")[..],
        // File 218444.
        &include_bytes!("../test-contract/res/fungible_token.wasm")[..],
    ];

    for raw_bytes in validate_contracts {
        let contract = ContractCode::new(raw_bytes.to_vec(), None);
        let x = raw_bytes.len() as u64;
        let y = measure_contract(vm_kind, gas_metric, &contract, cache);
        let expect = (corrected_a + corrected_b * (x as i128)).to_integer();
        let error = expect - (y as i128);
        if gas_metric == GasMetric::ICount {
            // Time based metric may lead to unpredictable results.
            assert!(error >= 0);
        }
    }

    (corrected_a, corrected_b)
}

pub(crate) fn compute_compile_cost_vm(
    metric: GasMetric,
    vm_kind: VMKind,
    verbose: bool,
) -> (Gas, Gas) {
    let (a, b) = precompilation_cost(metric, vm_kind);
    let base = a.to_integer();
    let per_byte = b.to_integer();
    if verbose {
        println!(
            "{:?} using {:?}: in a + b * x: a = {} ({}) b = {}({}) base = {} per_byte = {}",
            vm_kind,
            metric,
            a,
            a.to_f64().unwrap(),
            b,
            b.to_f64().unwrap(),
            base,
            per_byte
        );
    }
    match metric {
        GasMetric::ICount => (u64::try_from(base).unwrap(), u64::try_from(per_byte).unwrap()),
        // Time metric can lead to negative coefficients.
        GasMetric::Time => (u64::try_from(base).unwrap_or(0), u64::try_from(per_byte).unwrap_or(0)),
    }
}
