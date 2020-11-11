use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::{end_count, get_account_id, start_count, total_transactions, Config};
use ethabi_contract::use_contract;
use glob::glob;
use indicatif::{ProgressBar, ProgressStyle};
use lazy_static_include::lazy_static_include_str;
use near_crypto::{InMemorySigner, KeyType};
use near_evm_runner::EvmContext;
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Action, FunctionCallAction, SignedTransaction};
use near_runtime_fees::RuntimeFeesConfig;
use near_vm_logic::gas_counter::reset_evm_gas_counter;
use near_vm_logic::mocks::mock_external::MockedExternal;
use near_vm_logic::VMConfig;
use num_rational::Ratio;
use num_traits::cast::ToPrimitive;
use rand::Rng;
use rocksdb::Env;
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use testlib::node::{Node, RuntimeNode};
use testlib::user::runtime_user::MockClient;

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

pub struct EvmCostCoef {
    pub deploy_cost: Coef2D,
    pub funcall_cost: Coef,
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

pub fn measure_evm_function<F: FnOnce(Vec<u8>) -> Vec<u8> + Copy>(
    config: &Config,
    verbose: bool,
    args_encoder: F,
    test_name: &str,
    testbed: Arc<Mutex<RuntimeTestbed>>,
    nonces: Arc<Mutex<HashMap<usize, u64>>>,
) -> EvmCost {
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
        let _nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
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

pub fn near_cost_to_evm_gas(funcall_cost: Coef, cost: Ratio<u64>) -> u64 {
    return u64::try_from((cost / funcall_cost.0).to_integer()).unwrap();
}

/// Cost of all evm related
pub fn cost_of_evm(config: &Config, verbose: bool) -> EvmCostCoef {
    let (testbed, nonces) = testbed_for_evm(&config.state_dump_path, config.active_accounts);
    let evm_cost_config = EvmCostCoef {
        deploy_cost: measure_evm_deploy(config, verbose, testbed.clone(), nonces.clone()),
        funcall_cost: measure_evm_funcall(config, verbose, testbed.clone(), nonces.clone()),
    };
    evm_cost_config
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

    let beta1 = xt_x_inverse.a * xt_y1 + xt_x_inverse.b * xt_y2;
    let beta2 = xt_x_inverse.c * xt_y1 + xt_x_inverse.d * xt_y2;

    let delta: Vec<_> = measurements
        .iter()
        .map(|m| m.cost.to_f64().unwrap() - (m.evm_gas as f64) * beta1 - (m.size as f64) * beta2)
        .collect();
    let r = (beta1, beta2, delta.iter().sum::<f64>() / delta.len() as f64);
    if verbose {
        println!("evm calc data {:?}", r);
        println!("delta: {:?}", delta);
    }
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

type Coef = (Ratio<u64>, Ratio<u64>);

type Coef2D = (f64, f64, f64);

struct Matrix2x2 {
    a: f64,
    b: f64,
    c: f64,
    d: f64,
}

fn dot(v1: &Vec<f64>, v2: &Vec<f64>) -> f64 {
    let mut ret = 0.0;
    for (i, u) in v1.iter().enumerate() {
        ret += u * v2[i];
    }
    ret
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
