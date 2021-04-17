use crate::testbed_runners::{end_count, start_count, GasMetric};
use glob::glob;
use near_primitives::contract::ContractCode;
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::version::PROTOCOL_VERSION;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMContext, VMOutcome};
use near_vm_runner::{compile_module, prepare, VMError, VMKind};
use num_rational::Ratio;
use std::fs;
use std::path::PathBuf;
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
