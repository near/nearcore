use crate::cases::Metric;
use crate::stats::Measurements;
use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::end_count;
use crate::testbed_runners::get_account_id;
use crate::testbed_runners::start_count;
use crate::testbed_runners::Config;
use crate::testbed_runners::GasMetric;
use ethabi_contract::use_contract;
use glob::glob;
use lazy_static_include::lazy_static_include_str;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_evm_runner::utils::encode_call_function_args;
use near_evm_runner::{run_evm, EvmContext};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};
use near_primitives::version::PROTOCOL_VERSION;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::gas_counter::reset_evm_gas_counter;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMContext, VMKind, VMOutcome};
use near_vm_runner::{compile_module, prepare, VMError};
use num_rational::Ratio;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fs;
use std::sync::Mutex;
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

#[derive(Debug)]
pub struct EvmCost {
    pub evm_gas: u64,
    pub cost: Ratio<u64>,
}

fn deploy_evm_contract(code: &[u8], config: &Config) -> Option<EvmCost> {
    let path = PathBuf::from(config.state_dump_path.as_str());
    println!("{:?}. Preparing testbed. Loading state.", config.metric);
    let testbed = Mutex::new(RuntimeTestbed::from_state_dump(&path));
    let allow_failures = false;
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let mut accounts_deployed = HashSet::new();

    let mut f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if accounts_deployed.contains(&x) {
                continue;
            }
            break x;
        };
        accounts_deployed.insert(account_idx);
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);

        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            "evm".to_owned(),
            &signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "deploy_code".to_string(),
                args: hex::decode(code).unwrap(),
                gas: 10u64.pow(18),
                deposit: 0,
            })],
            CryptoHash::default(),
        )
    };

    let mut evm_gas = 0;
    let mut total_cost = 0;
    for block_size in config.block_sizes.clone() {
        // [100]
        for _ in 0..config.iter_per_block {
            // 0..1
            let block: Vec<_> = (0..block_size).map(|_| f()).collect();
            let mut testbed = testbed.lock().unwrap();
            let start = start_count(config.metric);
            testbed.process_block(&block, allow_failures);
            testbed.process_blocks_until_no_receipts(allow_failures);
            let cost = end_count(config.metric, &start);
            total_cost += cost;
        }
    }

    let counts = (config.iter_per_block * config.block_sizes.iter().sum::<usize>()) as u64;
    evm_gas = reset_evm_gas_counter() / counts;

    // evm_gas is  times gas spent, so does cost
    Some(EvmCost { evm_gas, cost: Ratio::new(total_cost, counts) })
}

fn load_and_deploy_evm_contract(path: &PathBuf, config: &Config) -> Option<EvmCost> {
    match fs::read(path) {
        Ok(code) => deploy_evm_contract(&code, config),
        _ => None,
    }
}

#[cfg(feature = "lightbeam")]
const USING_LIGHTBEAM: bool = true;
#[cfg(not(feature = "lightbeam"))]
const USING_LIGHTBEAM: bool = false;

type Coef = (Ratio<u64>, Ratio<u64>);

pub struct EvmPrecompiledFunctionCost {
    pub ecRecoverCost: Ratio<u64>,
    pub sha256Cost: Ratio<u64>,
    pub ripemd160Cost: Ratio<u64>,
    pub identityCost: Ratio<u64>,
    pub modexpImplCost: Ratio<u64>,
    // pub bn128AddImplCost: Ratio<u64>,
    // pub bn128MulImplCost: Ratio<u64>,
    // pub bn128PairingImplCost: Ratio<u64>,
    // pub blake2FImplCost: Ratio<u64>,
    // pub lastPrecompileCost: Ratio<u64>,
}

pub struct EvmCostCoef {
    pub deploy_cost: Coef,
    pub funcall_cost: Coef,
    pub precompiled_function_cost: EvmPrecompiledFunctionCost,
}

pub fn measure_evm_deploy(config: &Config, verbose: bool) -> Coef {
    let globbed_files = glob("./**/*.bin").expect("Failed to read glob pattern for bin files");
    let fees = RuntimeFeesConfig::default();
    let paths = globbed_files
        .filter_map(|x| match x {
            Ok(p) => Some(p),
            _ => None,
        })
        .collect::<Vec<PathBuf>>();

    let measurements = paths
        .iter()
        .filter(|path| fs::metadata(path).is_ok())
        .map(|path| {
            if verbose {
                print!("Testing {}: ", path.display());
            };
            // Evm counted gas already count on size of the contract, therefore we look for cost = m*evm_gas + b.
            if let Some(EvmCost { evm_gas, cost }) = load_and_deploy_evm_contract(path, config) {
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
        .map(|measure| deduct_action_receipt_fee(&fees, &measure))
        .collect::<Vec<EvmCost>>();
    measurements_to_coef(measurements, true)
}

use_contract!(soltest, "../near-evm-runner/tests/build/SolTests.abi");
use_contract!(precompiled_function, "../near-evm-runner/tests/build/PrecompiledFunction.abi");

lazy_static_include_str!(TEST, "../near-evm-runner/tests/build/SolTests.bin");
lazy_static_include_str!(
    PRECOMPILED_TEST,
    "../near-evm-runner/tests/build/PrecompiledFunction.bin"
);

pub fn create_evm_context<'a>(
    external: &'a mut MockedExternal,
    vm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    account_id: String,
    attached_deposit: u128,
) -> EvmContext<'a> {
    EvmContext::new(
        external,
        vm_config,
        fees_config,
        1000,
        account_id.to_string(),
        account_id.to_string(),
        account_id.to_string(),
        attached_deposit,
        0,
        10u64.pow(14),
        false,
        100_000_000.into(),
    )
}

pub fn measure_evm_funcall(config: &Config, verbose: bool) -> Coef {
    let mut fake_external = MockedExternal::new();
    let vm_config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();

    let mut context =
        create_evm_context(&mut fake_external, &vm_config, &fees, "alice".to_string(), 100);
    // TODO: this is wrong, figure out the correct addr from deploy step returned.
    // let sol_test_addr = context.deploy_code(hex::decode(&TEST).unwrap()).unwrap();

    let measurements = vec![
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(sol_test_addr, soltest::functions::deploy_new_guy::call(8).0),
            "deploy_new_guy(8)",
        ),
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(sol_test_addr, soltest::functions::pay_new_guy::call(8).0),
            "pay_new_guy(8)",
        ),
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                sol_test_addr,
                soltest::functions::return_some_funds::call().0,
            ),
            "return_some_funds()",
        ),
        {
            // function not payable must has zero deposit
            context.attached_deposit = 0;
            measure_evm_function(
                config,
                verbose,
                &mut context,
                encode_call_function_args(sol_test_addr, soltest::functions::emit_it::call(8).0),
                "emit_it(8)",
            )
        },
    ]
    .iter()
    .map(|measure| deduct_action_receipt_fee(&fees, measure))
    .collect::<Vec<_>>();
    println!("{:?}", measurements);
    measurements_to_coef(measurements, true)
}

fn deduct_action_receipt_fee(fees: &RuntimeFeesConfig, evm_cost: &EvmCost) -> EvmCost {
    // Because run --only-evm, don't have aggreggated metrics of Metric::noop and Metric::Receipt, so just deduct
    // ReceiptFees::ActionFunctionCallBase from last run without --only-evm
    EvmCost {
        evm_gas: evm_cost.evm_gas,
        cost: evm_cost.cost
            - fees.action_creation_config.function_call_cost.execution
            - fees.action_creation_config.function_call_cost.send_not_sir,
    }
}

pub fn measure_evm_function(
    config: &Config,
    verbose: bool,
    context: &mut EvmContext,
    args: Vec<u8>,
    test_name: &str,
) -> EvmCost {
    let path = PathBuf::from(config.state_dump_path.as_str());
    println!("{:?}. Preparing testbed. Loading state.", config.metric);
    let testbed = Mutex::new(RuntimeTestbed::from_state_dump(&path));
    let allow_failures = false;
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let mut accounts_deployed = HashSet::new();
    let code = hex::decode(&TEST).unwrap();

    let mut f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if accounts_deployed.contains(&x) {
                continue;
            }
            break x;
        };
        accounts_deployed.insert(account_idx);
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);

        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        testbed.lock().unwrap().process_block(
            &vec![SignedTransaction::from_actions(
                nonce as u64,
                account_id.clone(),
                "evm".to_owned(),
                &signer,
                vec![Action::FunctionCall(FunctionCallAction {
                    method_name: "deploy_code".to_string(),
                    args: code.clone(),
                    gas: 10u64.pow(18),
                    deposit: 0,
                })],
                CryptoHash::default(),
            )],
            false,
        );
        testbed.lock().unwrap().process_blocks_until_no_receipts(allow_failures);

        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            "evm".to_owned(),
            &signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "call_function".to_string(),
                args: args.clone(),
                gas: 10u64.pow(18),
                deposit: 0,
            })],
            CryptoHash::default(),
        )
    };

    reset_evm_gas_counter();
    let mut evm_gas = 0;
    let mut total_cost = 0;
    for block_size in config.block_sizes.clone() {
        // [100]
        for _ in 0..config.iter_per_block {
            // 0..1
            let block: Vec<_> = (0..block_size).map(|_| f()).collect();
            let mut testbed = testbed.lock().unwrap();
            let start = start_count(config.metric);
            testbed.process_block(&block, allow_failures);
            testbed.process_blocks_until_no_receipts(allow_failures);
            let cost = end_count(config.metric, &start);
            total_cost += cost;
        }
    }

    let counts = (config.iter_per_block * config.block_sizes.iter().sum::<usize>()) as u64;
    evm_gas = reset_evm_gas_counter() / counts;

    let cost = Ratio::new(total_cost, counts);
    if verbose {
        println!("Testing call {}: ({}, {})", test_name, evm_gas, cost);
    }
    EvmCost { evm_gas, cost }
}

pub fn measure_evm_precompiled(config: &Config, verbose: bool) -> EvmPrecompiledFunctionCost {
    let mut fake_external = MockedExternal::new();
    let vm_config = VMConfig::default();
    let fees = RuntimeFeesConfig::default();

    let mut context =
        create_evm_context(&mut fake_external, &vm_config, &fees, "alice".to_string(), 0);
    let precompiled_function_addr =
        context.deploy_code(hex::decode(&PRECOMPILED_TEST).unwrap()).unwrap();

    let measurements = vec![
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::noop::call().0,
            ),
            "noop()",
        ),
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_ecrecover::call().0,
            ),
            "test_ecrecover()",
        ),
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_sha256::call().0,
            ),
            "test_sha256()",
        ),
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_ripemd160::call().0,
            ),
            "test_ripemd160()",
        ),
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_identity::call().0,
            ),
            "test_identity()",
        ),
        measure_evm_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_mod_exp::call().0,
            ),
            "test_mod_exp()",
        ),
    ];

    EvmPrecompiledFunctionCost {
        ecRecoverCost: measurements[1].cost - measurements[0].cost,
        sha256Cost: measurements[2].cost - measurements[0].cost,
        ripemd160Cost: measurements[3].cost - measurements[0].cost,
        identityCost: measurements[4].cost - measurements[0].cost,
        modexpImplCost: measurements[5].cost - measurements[0].cost,
    }
}

pub fn near_cost_to_evm_gas(funcall_cost: Coef, cost: Ratio<u64>) -> u64 {
    return u64::try_from((cost / funcall_cost.0).to_integer()).unwrap();
}

/// Cost of all evm related
pub fn cost_of_evm(config: &Config, verbose: bool) -> EvmCostCoef {
    let evm_cost_config = EvmCostCoef {
        deploy_cost: measure_evm_deploy(config, verbose),
        funcall_cost: measure_evm_funcall(config, verbose),
        precompiled_function_cost: measure_evm_precompiled(config, verbose),
    };
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
            "About to compile {}",
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

fn measurements_to_coef(measurements: Vec<EvmCost>, verbose: bool) -> Coef {
    let ratio = Ratio::new(0 as u64, 1);
    let base = Ratio::new(u64::MAX, 1);
    let b = measurements.iter().fold(base, |b, EvmCost { evm_gas: _, cost }| b.min(*cost));
    let m = measurements
        .iter()
        .fold(ratio, |r, EvmCost { evm_gas, cost }| r.max((*cost - b) / evm_gas));
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
