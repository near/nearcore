use crate::testbed_runners::end_count;
use crate::testbed_runners::start_count;
use crate::testbed_runners::GasMetric;
use glob::glob;
use near_evm_runner::run_evm;
use near_primitives::version::PROTOCOL_VERSION;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMContext, VMKind, VMOutcome};
use near_vm_runner::{compile_module, prepare, VMError};
use num_rational::Ratio;
use std::collections::hash_map::DefaultHasher;
use std::fs;
use std::{
    hash::{Hash, Hasher},
    path::PathBuf,
};
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

fn call() -> (Option<VMOutcome>, Option<VMError>) {
    let code = include_bytes!("../test-contract/res/large_contract.wasm");
    let mut fake_external = MockedExternal::new();
    let context = create_context(vec![]);
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();

    let promise_results = vec![];

    let mut hash = DefaultHasher::new();
    code.hash(&mut hash);
    let code_hash = hash.finish().to_le_bytes().to_vec();
    near_vm_runner::run(
        code_hash,
        code,
        b"cpu_ram_soak_test",
        &mut fake_external,
        context,
        &config,
        &fees,
        &promise_results,
        PROTOCOL_VERSION,
    )
}

const NUM_ITERATIONS: u64 = 10;

/// Cost of the most CPU demanding operation.
pub fn cost_per_op(gas_metric: GasMetric) -> Ratio<u64> {
    // Call once for the warmup.
    let (outcome, _) = call();
    let outcome = outcome.unwrap();
    let start = start_count(gas_metric);
    for _ in 0..NUM_ITERATIONS {
        call();
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

fn load_and_compile(path: &PathBuf, gas_metric: GasMetric, vm_kind: VMKind) -> Option<CompileCost> {
    match fs::read(path) {
        Ok(mut code) => match delete_all_data(&mut code) {
            Ok(code) => compile(&code, gas_metric, vm_kind),
            _ => None,
        },
        _ => None,
    }
}

pub struct EvmCost {
    pub evm_gas: u64,
    pub cost: Ratio<u64>,
}

fn deploy_evm_contract(code: &[u8], gas_metric: GasMetric) -> Option<EvmCost> {
    let mut fake_external = MockedExternal::new();
    let config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();

    let start = start_count(gas_metric);
    let mut evm_gas = 0;
    for _ in 0..NUM_ITERATIONS {
        let (_, _, gas_used) = run_evm(
            &mut fake_external,
            &config,
            &fees,
            &"alice".to_string(),
            1000u128,
            0u128,
            0u64,
            "deploy_code".to_string(),
            hex::decode(&code).unwrap(),
            1_000_000_000u64,
            false,
        );
        // All iterations use same amount of (evm) gas, it's safe to use any of them as gas_used.
        // But we loop because we want avg of number of (near) gas_metric
        evm_gas = gas_used.as_u64();
    }
    let end = end_count(gas_metric, &start);
    Some(EvmCost { evm_gas, cost: Ratio::new(end, NUM_ITERATIONS) })
}

fn load_and_deploy_evm_contract(path: &PathBuf, gas_metric: GasMetric) -> Option<EvmCost> {
    match fs::read(path) {
        Ok(code) => deploy_evm_contract(&code, gas_metric),
        _ => None,
    }
}

#[cfg(feature = "lightbeam")]
const USING_LIGHTBEAM: bool = true;
#[cfg(not(feature = "lightbeam"))]
const USING_LIGHTBEAM: bool = false;

type Coef = (Ratio<u64>, Ratio<u64>);

pub struct EvmCostCoef {
    pub deploy_cost: Coef,
    // pub run_cost: Coef,
    // pub ecRecoverCost: Coef,
    // pub sha256Cost: Coef,
    // pub ripemd160Cost: Coef,
    // pub identityCost: Coef,
    // pub modexpImplCost: Coef,
    // pub bn128AddImplCost: Coef,
    // pub bn128MulImplCost: Coef,
    // pub bn128PairingImplCost: Coef,
    // pub blake2FImplCost: Coef,
    // pub lastPrecompileCost: Coef,
}

pub fn measure_evm_deploy(gas_metric: GasMetric, verbose: bool) -> Coef {
    let globbed_files = glob("./**/*.bin").expect("Failed to read glob pattern for bin files");
    let paths = globbed_files
        .filter_map(|x| match x {
            Ok(p) => Some(p),
            _ => None,
        })
        .collect::<Vec<PathBuf>>();
    let ratio = Ratio::new(0 as u64, 1);
    let base = Ratio::new(u64::MAX, 1);

    let measurements = paths
        .iter()
        .filter(|path| fs::metadata(path).is_ok())
        .map(|path| {
            if verbose {
                print!("Testing {}: ", path.display());
            };
            // Evm counted gas already count on size of the contract, therefore we look for cost = m*evm_gas + b.
            if let Some(EvmCost { evm_gas, cost }) = load_and_deploy_evm_contract(path, gas_metric)
            {
                if verbose {
                    println!("({}, {})", evm_gas, cost);
                };
                Some(EvmCost { evm_gas, cost })
            } else {
                if verbose {
                    println!("FAILED")
                };
                None
            }
        })
        .filter(|x| x.is_some())
        .map(|x| x.unwrap())
        .collect::<Vec<EvmCost>>();
    let b = measurements.iter().fold(base, |b, EvmCost { evm_gas: _, cost }| b.min(*cost));
    let m = measurements
        .iter()
        .fold(ratio, |r, EvmCost { evm_gas, cost }| r.max((*cost - b) / evm_gas));
    if verbose {
        println!("raw data: ({},{})", m, b);
    }
    (m, b)
}

pub fn measure_evm_run(gas_metric: GasMetric, verbose: bool) -> Coef {
    let ratio = Ratio::new(0 as u64, 1);
    let base = Ratio::new(u64::MAX, 1);
    //TODO
    (ratio, base)
}

/// Cost of all evm related
pub fn cost_of_evm(gas_metric: GasMetric, verbose: bool) -> EvmCostCoef {
    let evm_cost_config = EvmCostCoef { deploy_cost: measure_evm_deploy(gas_metric, verbose) };
    evm_cost_config
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
            "Abount to compile {}",
            match vm_kind {
                VMKind::Wasmer => "wasmer",
                VMKind::Wasmtime => {
                    if USING_LIGHTBEAM {
                        "wasmtime-lightbeam"
                    } else {
                        "wasmtime"
                    }
                }
            }
        );
    };
    let measurements = paths
        .iter()
        .filter(|path| fs::metadata(path).is_ok())
        .map(|path| {
            if verbose {
                print!("Testing {}: ", path.display());
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
