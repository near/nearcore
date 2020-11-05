use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::end_count;
use crate::testbed_runners::get_account_id;
use crate::testbed_runners::start_count;
use crate::testbed_runners::total_transactions;
use crate::testbed_runners::Config;
use crate::testbed_runners::GasMetric;
use ethabi_contract::use_contract;
use ethereum_types::H160;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static_include::lazy_static_include_str;
use near_crypto::{InMemorySigner, KeyType};
use near_evm_runner::utils::encode_call_function_args;
use near_evm_runner::EvmContext;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};
use near_primitives::version::PROTOCOL_VERSION;
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::gas_counter::reset_evm_gas_counter;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::{VMConfig, VMContext, VMKind, VMOutcome};
use near_vm_runner::{compile_module, prepare, VMError};
use num_rational::Ratio;
use num_traits::cast::ToPrimitive;
use rand::Rng;
use rocksdb::Env;
use std::collections::hash_map::DefaultHasher;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fs;
use std::sync::{Arc, Mutex, RwLock};
use std::{
    hash::{Hash, Hasher},
    path::Path,
    path::PathBuf,
};
use testlib::node::{Node, RuntimeNode};
use testlib::user::runtime_user::MockClient;
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
        None,
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

#[derive(Debug)]
pub struct EvmCost {
    pub evm_gas: u64,
    pub size: u64,
    pub cost: Ratio<u64>,
}

fn testbed_for_evm(
    state_dump_path: &str,
    accounts: usize,
) -> (Arc<Mutex<RuntimeTestbed>>, Arc<Mutex<HashMap<usize, u64>>>) {
    let path = PathBuf::from(state_dump_path);
    let testbed = Arc::new(Mutex::new(RuntimeTestbed::from_state_dump(&path)));
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let bar = ProgressBar::new(accounts as _);
    println!("Prepare a testbed of {} accounts all having a deployed evm contract", accounts);
    bar.set_style(ProgressStyle::default_bar().template(
        "[elapsed {elapsed_precise} remaining {eta_precise}] Evm contracts {bar} {pos:>7}/{len:7} {msg}",
    ));
    let mut env = Env::default().unwrap();
    env.set_background_threads(4);
    for account_idx in 0..accounts {
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let code = hex::decode(&TEST).unwrap();
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);

        let block: Vec<_> = vec![SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            "evm".to_owned(),
            &signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "deploy_code".to_string(),
                args: code,
                gas: 10u64.pow(18),
                deposit: 0,
            })],
            CryptoHash::default(),
        )];
        let mut testbed = testbed.lock().unwrap();
        testbed.process_block(&block, false);
        testbed.process_blocks_until_no_receipts(false);
        bar.inc(1);
    }
    bar.finish();
    reset_evm_gas_counter();
    env.set_background_threads(0);
    (testbed, Arc::new(Mutex::new(nonces)))
}

fn deploy_evm_contract(
    code: &[u8],
    config: &Config,
    testbed: Arc<Mutex<RuntimeTestbed>>,
    nonces: Arc<Mutex<HashMap<usize, u64>>>,
) -> Option<EvmCost> {
    let path = PathBuf::from(config.state_dump_path.as_str());
    println!("{:?}. Preparing testbed. Loading state.", config.metric);
    let allow_failures = false;
    let mut nonces = nonces.lock().unwrap();
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

    for _ in 0..config.warmup_iters_per_block {
        for block_size in config.block_sizes.clone() {
            let block: Vec<_> = (0..block_size).map(|_| f()).collect();
            let mut testbed = testbed.lock().unwrap();
            testbed.process_block(&block, allow_failures);
            testbed.process_blocks_until_no_receipts(allow_failures);
        }
    }
    reset_evm_gas_counter();
    let mut evm_gas = 0;
    let mut total_cost = 0;
    for _ in 0..config.iter_per_block {
        for block_size in config.block_sizes.clone() {
            let block: Vec<_> = (0..block_size).map(|_| f()).collect();
            let mut testbed = testbed.lock().unwrap();
            testbed.process_block(&block, allow_failures);
            // process_block create action receipt for FunctionCall Action, not count as gas used in evm.
            // In real node, action receipt cost is deducted in validate_tx -> tx_cost so should only count
            // and deduct evm execution cost
            let start = start_count(config.metric);
            testbed.process_blocks_until_no_receipts(allow_failures);
            let cost = end_count(config.metric, &start);
            total_cost += cost;
            evm_gas += reset_evm_gas_counter();
        }
    }

    let counts = total_transactions(config) as u64;
    evm_gas /= counts;

    Some(EvmCost { evm_gas, size: code.len() as u64, cost: Ratio::new(total_cost, counts) })
}

fn load_and_deploy_evm_contract(
    path: &PathBuf,
    config: &Config,
    testbed: Arc<Mutex<RuntimeTestbed>>,
    nonces: Arc<Mutex<HashMap<usize, u64>>>,
) -> Option<EvmCost> {
    match fs::read(path) {
        Ok(code) => deploy_evm_contract(&code, config, testbed, nonces),
        _ => None,
    }
}

#[cfg(feature = "lightbeam")]
const USING_LIGHTBEAM: bool = true;
#[cfg(not(feature = "lightbeam"))]
const USING_LIGHTBEAM: bool = false;

type Coef = (Ratio<u64>, Ratio<u64>);

type Coef2D = (f64, f64, f64);

pub struct EvmPrecompiledFunctionCost {
    pub ec_recover_cost: Ratio<u64>,
    pub sha256_cost: Ratio<u64>,
    pub sha256_cost_per_byte: Ratio<u64>,
    pub ripemd160_cost: Ratio<u64>,
    pub ripemd160_cost_per_byte: Ratio<u64>,
    pub identity_cost: Ratio<u64>,
    pub identity_cost_per_byte: Ratio<u64>,
    pub modexp_impl_cost: Ratio<u64>,
    // pub bn128AddImplCost: Ratio<u64>,
    // pub bn128MulImplCost: Ratio<u64>,
    // pub bn128PairingImplCost: Ratio<u64>,
    // pub blake2FImplCost: Ratio<u64>,
    // pub lastPrecompileCost: Ratio<u64>,
}

pub struct EvmCostCoef {
    pub deploy_cost: Coef2D,
    pub funcall_cost: Coef,
    pub precompiled_function_cost: EvmPrecompiledFunctionCost,
}

pub fn measure_evm_deploy(
    config: &Config,
    verbose: bool,
    testbed: Arc<Mutex<RuntimeTestbed>>,
    nonces: Arc<Mutex<HashMap<usize, u64>>>,
) -> Coef2D {
    let globbed_files = glob("./**/*.bin").expect("Failed to read glob pattern for bin files");
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
            if let Some(EvmCost { evm_gas, size, cost }) =
                load_and_deploy_evm_contract(path, config, testbed.clone(), nonces.clone())
            {
                if verbose {
                    println!("({}, {}, {}),", evm_gas, size, cost);
                };
                Some(EvmCost { evm_gas, size, cost })
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
    measurements_to_coef_2d(measurements, true)
}

use_contract!(soltest, "../near-evm-runner/tests/build/SolTests.abi");
use_contract!(precompiled_function, "../near-evm-runner/tests/build/PrecompiledFunction.abi");

lazy_static_include_str!(TEST, "../near-evm-runner/tests/build/SolTests.bin");
lazy_static_include_str!(
    PRECOMPILED_TEST,
    "../near-evm-runner/tests/build/PrecompiledFunction.bin"
);

const CHAIN_ID: u128 = 0x99;

pub fn create_evm_context<'a>(
    external: &'a mut MockedExternal,
    vm_config: &'a VMConfig,
    fees_config: &'a RuntimeFeesConfig,
    account_id: String,
    attached_deposit: u128,
) -> EvmContext<'a> {
    EvmContext::new(
        external,
        CHAIN_ID,
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

pub fn measure_evm_funcall(
    config: &Config,
    verbose: bool,
    testbed: Arc<Mutex<RuntimeTestbed>>,
    nonces: Arc<Mutex<HashMap<usize, u64>>>,
) -> Coef {
    let measurements = vec![
        measure_evm_function(
            config,
            verbose,
            |sol_test_addr| {
                vec![sol_test_addr, soltest::functions::deploy_new_guy::call(8).0].concat()
            },
            "deploy_new_guy(8)",
            testbed.clone(),
            nonces.clone(),
        ),
        measure_evm_function(
            config,
            verbose,
            |sol_test_addr| {
                vec![sol_test_addr, soltest::functions::pay_new_guy::call(8).0].concat()
            },
            "pay_new_guy(8)",
            testbed.clone(),
            nonces.clone(),
        ),
        measure_evm_function(
            config,
            verbose,
            |sol_test_addr| {
                vec![sol_test_addr, soltest::functions::return_some_funds::call().0].concat()
            },
            "return_some_funds()",
            testbed.clone(),
            nonces.clone(),
        ),
        measure_evm_function(
            config,
            verbose,
            |sol_test_addr| vec![sol_test_addr, soltest::functions::emit_it::call(8).0].concat(),
            "emit_it(8)",
            testbed.clone(),
            nonces.clone(),
        ),
    ];
    println!("{:?}", measurements);
    measurements_to_coef(measurements, true)
}

pub fn measure_evm_precompile_function(
    config: &Config,
    verbose: bool,
    context: &mut EvmContext,
    args: Vec<u8>,
    test_name: &str,
) -> EvmCost {
    // adding action receipt etc is too big compare too evm precompile function itself and cause the error is bigger
    // than evm precompile function cost, so measure this cost in evm context level to be precise
    let gas_metric = config.metric;
    let start = start_count(gas_metric);
    let mut evm_gas = 0;
    for i in 0..NUM_ITERATIONS {
        if i == 0 {
            evm_gas = context.evm_gas_counter.used_gas.as_u64();
        } else if i == 1 {
            evm_gas = context.evm_gas_counter.used_gas.as_u64() - evm_gas;
        }
        let _ = context.call_function(args.clone()).unwrap();
    }
    let end = end_count(gas_metric, &start);
    let cost = Ratio::new(end, NUM_ITERATIONS);
    if verbose {
        println!("Testing call {}: ({}, {})", test_name, evm_gas, cost);
    }
    EvmCost { size: 0, evm_gas, cost }
}

pub fn measure_evm_function<F: FnOnce(Vec<u8>) -> Vec<u8> + Copy>(
    config: &Config,
    verbose: bool,
    args_encoder: F,
    test_name: &str,
    testbed: Arc<Mutex<RuntimeTestbed>>,
    nonces: Arc<Mutex<HashMap<usize, u64>>>,
) -> EvmCost {
    let path = PathBuf::from(config.state_dump_path.as_str());
    println!("{:?}. Preparing testbed. Loading state.", config.metric);
    let allow_failures = false;
    let mut nonces = nonces.lock().unwrap();
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

        let mut testbed = testbed.lock().unwrap();
        let runtime_node = RuntimeNode {
            signer: Arc::new(signer.clone()),
            client: Arc::new(RwLock::new(MockClient {
                runtime: testbed.runtime,
                runtime_config: testbed.genesis.config.runtime_config.clone(),
                tries: testbed.tries.clone(),
                state_root: testbed.root,
                epoch_length: testbed.genesis.config.epoch_length,
            })),
            genesis: testbed.genesis.clone(),
        };
        let node_user = runtime_node.user();
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        let addr = node_user
            .function_call(
                account_id.clone(),
                "evm".to_string(),
                "deploy_code",
                code.clone(),
                10u64.pow(14),
                10,
            )
            .unwrap()
            .status
            .as_success_decoded()
            .unwrap();

        testbed.tries = runtime_node.client.read().unwrap().tries.clone();
        testbed.root = runtime_node.client.read().unwrap().state_root;
        testbed.runtime = runtime_node.client.read().unwrap().runtime;

        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            "evm".to_owned(),
            &signer,
            vec![Action::FunctionCall(FunctionCallAction {
                method_name: "call_function".to_string(),
                args: args_encoder(addr),
                gas: 10u64.pow(18),
                deposit: 0,
            })],
            CryptoHash::default(),
        )
    };

    for _ in 0..config.warmup_iters_per_block {
        for block_size in config.block_sizes.clone() {
            let block: Vec<_> = (0..block_size).map(|_| f()).collect();
            let mut testbed = testbed.lock().unwrap();
            testbed.process_block(&block, allow_failures);
            testbed.process_blocks_until_no_receipts(allow_failures);
        }
    }
    reset_evm_gas_counter();
    let mut evm_gas = 0;
    let mut total_cost = 0;
    for _ in 0..config.iter_per_block {
        for block_size in config.block_sizes.clone() {
            let block: Vec<_> = (0..block_size).map(|_| f()).collect();
            let mut testbed = testbed.lock().unwrap();
            testbed.process_block(&block, allow_failures);
            let start = start_count(config.metric);
            testbed.process_blocks_until_no_receipts(allow_failures);
            let cost = end_count(config.metric, &start);
            total_cost += cost;
            evm_gas += reset_evm_gas_counter();
        }
    }

    let counts = total_transactions(config) as u64;
    evm_gas /= counts;

    let cost = Ratio::new(total_cost, counts);
    if verbose {
        println!("Testing call {}: ({}, {})", test_name, evm_gas, cost);
    }
    EvmCost { evm_gas, size: 0, cost }
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
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::noop::call().0,
            ),
            "noop()",
        ),
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_ecrecover::call().0,
            ),
            "test_ecrecover()",
        ),
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_sha256::call().0,
            ),
            "test_sha256()",
        ),
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_sha256_100bytes::call().0,
            ),
            "test_sha256_100bytes()",
        ),
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_ripemd160::call().0,
            ),
            "test_ripemd160()",
        ),
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_ripemd160_1kb::call().0,
            ),
            "test_ripemd160_1kb()",
        ),
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_identity::call().0,
            ),
            "test_identity()",
        ),
        measure_evm_precompile_function(
            config,
            verbose,
            &mut context,
            encode_call_function_args(
                precompiled_function_addr,
                precompiled_function::functions::test_identity_100bytes::call().0,
            ),
            "test_identity_100bytes()",
        ),
        measure_evm_precompile_function(
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

    println!("measurements: {:?}", measurements);
    let r = EvmPrecompiledFunctionCost {
        ec_recover_cost: measurements[1].cost - measurements[0].cost,
        sha256_cost: measurements[2].cost - measurements[0].cost,
        sha256_cost_per_byte: (measurements[3].cost - measurements[2].cost) / 100,
        ripemd160_cost: measurements[4].cost - measurements[0].cost,
        ripemd160_cost_per_byte: (measurements[5].cost - measurements[4].cost) / 1024,
        identity_cost: measurements[6].cost - measurements[0].cost,
        identity_cost_per_byte: (measurements[7].cost - measurements[6].cost) / 100,
        modexp_impl_cost: measurements[8].cost - measurements[0].cost,
    };
    println!("sha256_cost_per_byte {}", r.sha256_cost_per_byte);
    r
}

pub fn near_cost_to_evm_gas(funcall_cost: Coef, cost: Ratio<u64>) -> u64 {
    return u64::try_from((cost / funcall_cost.0).to_integer()).unwrap();
}

/// Cost of all evm related
pub fn cost_of_evm(config: &Config, verbose: bool) -> EvmCostCoef {
    let (testbed, nonces) = testbed_for_evm(&config.state_dump_path, config.active_accounts);
    let evm_cost_config = EvmCostCoef {
        deploy_cost: measure_evm_deploy(config, verbose, testbed.clone(), nonces.clone()),
        funcall_cost: measure_evm_funcall(config, verbose, testbed.clone(), nonces.clone()),
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

fn dot(v1: &Vec<f64>, v2: &Vec<f64>) -> f64 {
    let mut ret = 0.0;
    for (i, u) in v1.iter().enumerate() {
        ret += u * v2[i];
    }
    ret
}

struct Matrix2x2 {
    a: f64,
    b: f64,
    c: f64,
    d: f64,
}

fn inverse2x2(m: Matrix2x2) -> Matrix2x2 {
    let Matrix2x2 { a, b, c, d } = m;
    let delta = a * d - b * c;
    Matrix2x2 { a: d / delta, b: -b / delta, c: -c / delta, d: a / delta }
}

fn normalize(v: &Vec<f64>) -> (Vec<f64>, f64) {
    let mean = v.iter().sum::<f64>() / (v.len() as f64);
    // default sklearn LinearRegression only normalize mean to 0, but not normalize stddev to 1, and that gives a very good result.

    let v: Vec<_> = v.iter().map(|x| (*x - mean)).collect();
    (v, mean)
}

fn measurements_to_coef_2d(measurements: Vec<EvmCost>, verbose: bool) -> Coef2D {
    let v1: Vec<_> = measurements.iter().map(|m| m.evm_gas as f64).collect();
    let (v1, _) = normalize(&v1);
    let v2: Vec<_> = measurements.iter().map(|m| m.size as f64).collect();
    let (v2, _) = normalize(&v2);
    let a = dot(&v1, &v1);
    let b = dot(&v1, &v2);
    let c = dot(&v2, &v1);
    let d = dot(&v2, &v2);

    let xt_x_inverse = inverse2x2(Matrix2x2 { a, b, c, d });

    let y: Vec<_> = measurements.iter().map(|m| m.cost.to_f64().unwrap()).collect();
    let xt_y1 = dot(&v1, &y);
    let xt_y2 = dot(&v2, &y);

    let beta1 = (xt_x_inverse.a * xt_y1 + xt_x_inverse.b * xt_y2);
    let beta2 = (xt_x_inverse.c * xt_y1 + xt_x_inverse.d * xt_y2);

    let delta: Vec<_> = measurements
        .iter()
        .map(|m| m.cost.to_f64().unwrap() - (m.evm_gas as f64) * beta1 - (m.size as f64) * beta2)
        .collect();
    let r = (beta1, beta2, delta.iter().sum::<f64>() / delta.len() as f64);
    println!("evm calc data {:?}", r);
    println!("delta: {:?}", delta);
    r
}

fn measurements_to_coef(measurements: Vec<EvmCost>, verbose: bool) -> Coef {
    let ratio = Ratio::new(0 as u64, 1);
    let base = Ratio::new(u64::MAX, 1);
    let b = measurements.iter().fold(base, |b, EvmCost { evm_gas: _, size: _, cost }| b.min(*cost));
    let m = measurements
        .iter()
        .fold(ratio, |r, EvmCost { evm_gas, size: _, cost }| r.max((*cost - b) / evm_gas));
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
