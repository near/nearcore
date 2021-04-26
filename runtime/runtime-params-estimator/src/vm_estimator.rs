use crate::testbed_runners::{end_count, start_count, GasMetric};
use glob::glob;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::types::CompiledContractCache;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::{create_store, StoreCompiledContractCache};
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMContext, VMOutcome};
use near_vm_runner::{compile_module, precompile_contract_vm, prepare, VMError, VMKind};
use neard::get_store_path;
use num_rational::Ratio;
use num_traits::ToPrimitive;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use walrus::{Module, Result};

const CURRENT_ACCOUNT_ID: &str = "alice";
const SIGNER_ACCOUNT_ID: &str = "bob";
const SIGNER_ACCOUNT_PK: [u8; 3] = [0, 1, 2];
const PREDECESSOR_ACCOUNT_ID: &str = "carol";

fn create_context(input: Vec<u8>) -> VMContext {
    VMContext {
        current_account_id: CURRENT_ACCOUNT_ID.to_owned(),
        signer_account_id: SIGNER_ACCOUNT_ID.to_owned(),
        signer_account_pk: Vec::from(&SIGNER_ACCOUNT_PK[..]),
        predecessor_account_id: PREDECESSOR_ACCOUNT_ID.to_owned(),
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
        is_view: false,
        output_data_receivers: vec![],
    }
}

fn call(code: &[u8]) -> (Option<VMOutcome>, Option<VMError>) {
    let mut fake_external = MockedExternal::new();
    let context = create_context(vec![]);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();

    let promise_results = vec![];

    let code = ContractCode::new(code.to_vec(), None);
    near_vm_runner::run(
        &code,
        "cpu_ram_soak_test",
        &mut fake_external,
        context,
        &config,
        &fees,
        &promise_results,
        PROTOCOL_VERSION,
        None,
        &Default::default(),
    )
}

const NUM_ITERATIONS: u64 = 10;

/// Cost of the most CPU demanding operation.
pub fn cost_per_op(gas_metric: GasMetric, code: &[u8]) -> Ratio<u64> {
    // Call once for the warmup.
    let (outcome, _) = call(code);
    let outcome = outcome.unwrap();
    let start = start_count(gas_metric);
    for _ in 0..NUM_ITERATIONS {
        call(code);
    }
    let measured = end_count(gas_metric, &start);
    // We are given by measurement burnt gas
    //   gas_burned(call) = outcome.burnt_gas
    // and raw 'measured' value counting x86 insns.
    // And know that
    //   measured = NUM_ITERATIONS * x86_insns(call)
    // Gas that was burned could be computed in two ways:
    // As number of WASM instructions by cost of a single instruction.
    //   gas_burned(call) = gas_cost_per_wasm_op * wasm_insns(call)
    // and as normalized x86 insns count.
    //   gas_burned(call) = measured * GAS_IN_MEASURE_UNIT / DIVISOR
    // Divisor here is essentially a normalizing factor matching insn count
    // to the notion of 1M gas as nanosecond of computations.
    // So
    //   outcome.burnt_gas = wasm_insns(call) *
    //       VMConfig::default().regular_op_cost
    //   gas_cost_per_wasm_op = (measured * GAS_IN_MEASURE_UNIT *
    //       VMConfig::default().regular_op_cost) /
    //       (DIVISOR * NUM_ITERATIONS * outcome.burnt_gas)
    // Enough to return just
    //    (measured * VMConfig::default().regular_op_cost) /
    //       (outcome.burnt_gas * NUM_ITERATIONS),
    // as remaining can be computed with ratio_to_gas().
    Ratio::new(
        measured * (VMConfig::default().regular_op_cost as u64),
        NUM_ITERATIONS * outcome.burnt_gas,
    )
}

type CompileCost = (u64, Ratio<u64>);

fn compile(code: &[u8], gas_metric: GasMetric, vm_kind: VMKind) -> Option<CompileCost> {
    let start = start_count(gas_metric);
    for _ in 0..NUM_ITERATIONS {
        let prepared_code = prepare::prepare_contract(code, &VMConfig::default()).unwrap();
        if compile_module(vm_kind, &prepared_code) {
            return None;
        }
    }
    let end = end_count(gas_metric, &start);
    Some((code.len() as u64, Ratio::new(end, NUM_ITERATIONS)))
}

pub fn load_and_compile(
    path: &PathBuf,
    gas_metric: GasMetric,
    vm_kind: VMKind,
) -> Option<CompileCost> {
    match fs::read(path) {
        Ok(mut code) => match delete_all_data(&mut code) {
            Ok(code) => compile(&code, gas_metric, vm_kind),
            _ => None,
        },
        _ => None,
    }
}

const USING_LIGHTBEAM: bool = cfg!(feature = "lightbeam");

fn measure_contract(
    vm_kind: VMKind,
    gas_metric: GasMetric,
    contract: &ContractCode,
    cache: Option<&dyn CompiledContractCache>,
) -> u64 {
    match rayon::ThreadPoolBuilder::new().num_threads(1).build_global() {
        Ok(()) => (),
        Err(_err) if rayon::current_num_threads() == 1 => (),
        Err(_err) => panic!("failed to set rayon to use 1 thread"),
    };

    let start = start_count(gas_metric);
    let vm_config = VMConfig::default();
    let result = precompile_contract_vm(vm_kind, &contract, &vm_config, cache);
    assert!(result.is_ok(), "Compilation failed");
    let end = end_count(gas_metric, &start);
    end
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

/// Returns `(a, b)` - approximation coefficients for formula `a + b * x`
/// where `x` is is the contract size in bytes. Practically, we compute upper bound
/// of this approximation, assuming that whole contract consists of code only.
fn precompilation_cost(gas_metric: GasMetric, vm_kind: VMKind) -> (Ratio<i128>, Ratio<i128>) {
    let cache_store1: Arc<StoreCompiledContractCache>;
    let cache_store2: Arc<MockCompiledContractCache>;
    let cache: Option<&dyn CompiledContractCache>;
    let use_file_store = false;
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
    // File 341191, code 279965, data 56627.
    let raw_bytes = include_bytes!("../test-contract/res/lockup_contract.wasm");
    let contract = ContractCode::new(raw_bytes.to_vec(), None);
    xs.push(raw_bytes.len() as u64);
    ys.push(measure_contract(vm_kind, gas_metric, &contract, cache));

    // File 257516, code 203545, data 50419.
    let raw_bytes = include_bytes!("../test-contract/res/staking_pool.wasm");
    let contract = ContractCode::new(raw_bytes.to_vec(), None);
    xs.push(raw_bytes.len() as u64);
    ys.push(measure_contract(vm_kind, gas_metric, &contract, cache));

    // File 135358, code 113152, data 19520.
    let raw_bytes = include_bytes!("../test-contract/res/voting_contract.wasm");
    let contract = ContractCode::new(raw_bytes.to_vec(), None);
    xs.push(raw_bytes.len() as u64);
    ys.push(measure_contract(vm_kind, gas_metric, &contract, cache));

    // File 124250, code 103473, data 18176.
    let raw_bytes = include_bytes!("../test-contract/res/whitelist.wasm");
    let contract = ContractCode::new(raw_bytes.to_vec(), None);
    xs.push(raw_bytes.len() as u64);
    ys.push(measure_contract(vm_kind, gas_metric, &contract, cache));

    // Least squares method.
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

    // Compute error estimation.
    let mut error = 0i128;
    for i in 0..n {
        let expect = (a + b * (xs[i] as i128)).to_integer();
        let diff = expect - (ys[i] as i128);
        error = error + diff * diff;
    }
    println!("Error {}", (error as f64).sqrt() / (n as f64));

    // We multiply `b` by 5/4 to accommodate for the fact that test contracts are typically 80% code,
    // so in the worst case it could grow to 100% and our costs are still properly estimate.
    // (a, b * Ratio::new(5i128, 4i128))
    (a, b)
}

fn test_compile_cost(metric: GasMetric) {
    let (a, b) = precompilation_cost(metric, VMKind::Wasmer0);
    println!(
        "Wasmer0 in a + b * x:  a = {} ({}) b = {}({})",
        a,
        a.to_f64().unwrap(),
        b,
        b.to_f64().unwrap()
    );
    let (a, b) = precompilation_cost(metric, VMKind::Wasmer1);
    println!(
        "Wasmer1 in a + b * x: a = {} ({}) b = {}({})",
        a,
        a.to_f64().unwrap(),
        b,
        b.to_f64().unwrap()
    );
}

#[test]
fn test_compile_cost_time() {
    test_compile_cost(GasMetric::Time)
}

#[test]
fn test_compile_cost_icount() {
    // Use smth like
    // CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER=./runner.sh cargo test --color=always \
    // --lib vm_estimator::test_compile_cost_icount --no-fail-fast -- --exact \
    // -Z unstable-options --show-output
    // Where runner.sh is
    // /host/nearcore/runtime/runtime-params-estimator/emu-cost/counter_plugin/qemu-x86_64 \
    // -cpu Westmere-v1 -plugin file=./emu-cost/counter_plugin/libcounter.so $@
    test_compile_cost(GasMetric::ICount)
}

/// Cost of the compile contract with vm_kind
pub fn cost_to_compile(
    gas_metric: GasMetric,
    vm_kind: VMKind,
    verbose: bool,
) -> (Ratio<u64>, Ratio<u64>) {
    let globbed_files = glob("./**/*.wasm").expect("Failed to read glob pattern for wasm files");
    let paths = globbed_files
        .filter_map(|x| match x {
            Ok(p) => Some(p),
            _ => None,
        })
        .collect::<Vec<PathBuf>>();
    let ratio = Ratio::new(0 as u64, 1);
    let base = Ratio::new(u64::MAX, 1);
    if verbose {
        println!(
            "About to compile {}",
            match vm_kind {
                VMKind::Wasmer0 => "wasmer",
                VMKind::Wasmtime => {
                    if USING_LIGHTBEAM {
                        "wasmtime-lightbeam"
                    } else {
                        "wasmtime"
                    }
                }
                VMKind::Wasmer1 => "wasmer1",
            }
        );
    };
    let measurements = paths
        .iter()
        .filter(|path| fs::metadata(path).is_ok())
        .map(|path| {
            if verbose {
                print!("Testing deploy {}: ", path.display());
            };
            if let Some((size, cost)) = load_and_compile(path, gas_metric, vm_kind) {
                if verbose {
                    println!("({}, {})", size, cost);
                };
                Some((size, cost))
            } else {
                if verbose {
                    println!("FAILED")
                };
                None
            }
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect::<Vec<CompileCost>>();
    let b = measurements.iter().fold(base, |base, (_, cost)| base.min(*cost));
    let m = measurements.iter().fold(ratio, |r, (bytes, cost)| r.max((*cost - b) / bytes));
    if verbose {
        println!("raw data: ({},{})", m, b);
    }
    (m, b)
}

fn delete_all_data(wasm_bin: &mut Vec<u8>) -> Result<&Vec<u8>> {
    let m = &mut Module::from_buffer(wasm_bin)?;
    for id in get_ids(m.data.iter().map(|t| t.id())) {
        m.data.delete(id);
    }
    *wasm_bin = m.emit_wasm();
    Ok(wasm_bin)
}

fn get_ids<T>(all: impl Iterator<Item = T>) -> Vec<T> {
    let mut ids = Vec::new();
    for id in all {
        ids.push(id);
    }
    ids
}
