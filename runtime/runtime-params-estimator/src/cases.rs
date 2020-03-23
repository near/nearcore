use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};

use crate::ext_costs_generator::ExtCostsGenerator;
use crate::runtime_fees_generator::RuntimeFeesGenerator;
use crate::stats::Measurements;
use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::{get_account_id, measure_actions, measure_transactions, Config};
use crate::wasmer_estimator::nanosec_per_op;
use near_runtime_fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee,
    RuntimeFeesConfig,
};
use near_vm_logic::{ExtCosts, ExtCostsConfig, VMConfig, VMLimitConfig};
use node_runtime::config::RuntimeConfig;

/// How much gas there is in a nanosecond worth of computation.
const GAS_IN_NANOS: f64 = 1_000_000f64;

fn measure_function(
    metric: Metric,
    method_name: &'static str,
    measurements: &mut Measurements,
    testbed: RuntimeTestbed,
    accounts_deployed: &[usize],
    nonces: &mut HashMap<usize, u64>,
    config: &Config,
    allow_failures: bool,
    args: Vec<u8>,
) -> RuntimeTestbed {
    // Measure the speed of creating a function fixture with 1MiB input.
    let mut rng = rand_xorshift::XorShiftRng::from_seed([0u8; 16]);
    let mut f = || {
        let account_idx = *accounts_deployed.choose(&mut rng).unwrap();
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        let function_call = Action::FunctionCall(FunctionCallAction {
            method_name: method_name.to_string(),
            args: args.clone(),
            gas: 10u64.pow(18),
            deposit: 0,
        });
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            account_id.clone(),
            &signer,
            vec![function_call],
            CryptoHash::default(),
        )
    };
    measure_transactions(metric, measurements, config, Some(testbed), &mut f, allow_failures)
}

macro_rules! calls_helper(
    { $($el:ident => $method_name:ident),* } => {
    {
        let mut v: Vec<(Metric, &str)> = vec![];
        $(
            v.push((Metric::$el, stringify!($method_name)));
        )*
        v
    }
    };
);

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone, Copy)]
#[allow(non_camel_case_types)]
pub enum Metric {
    Receipt,
    ActionTransfer,
    ActionCreateAccount,
    ActionDeleteAccount,
    ActionAddFullAccessKey,
    ActionAddFunctionAccessKey1Method,
    ActionAddFunctionAccessKey1000Methods,
    ActionDeleteAccessKey,
    ActionStake,
    ActionDeploy10K,
    ActionDeploy100K,
    ActionDeploy1M,

    warmup,
    noop_1MiB,
    noop,
    base_1M,
    read_memory_10b_10k,
    read_memory_1Mib_10k,
    write_memory_10b_10k,
    write_memory_1Mib_10k,
    read_register_10b_10k,
    read_register_1Mib_10k,
    write_register_10b_10k,
    write_register_1Mib_10k,
    utf8_log_10b_10k,
    utf8_log_10kib_10k,
    nul_utf8_log_10b_10k,
    nul_utf8_log_10kib_10k,
    utf16_log_10b_10k,
    utf16_log_10kib_10k,
    nul_utf16_log_10b_10k,
    nul_utf16_log_10kib_10k,
    sha256_10b_10k,
    sha256_10kib_10k,
    keccak256_10b_10k,
    keccak256_10kib_10k,
    keccak512_10b_10k,
    keccak512_10kib_10k,
    storage_write_10b_key_10b_value_1k,
    storage_write_10kib_key_10b_value_1k,
    storage_write_10b_key_10kib_value_1k,
    storage_write_10b_key_10kib_value_1k_evict,
    storage_read_10b_key_10b_value_1k,
    storage_read_10kib_key_10b_value_1k,
    storage_read_10b_key_10kib_value_1k,
    storage_remove_10b_key_10b_value_1k,
    storage_remove_10kib_key_10b_value_1k,
    storage_remove_10b_key_10kib_value_1k,
    storage_has_key_10b_key_10b_value_1k,
    storage_has_key_10kib_key_10b_value_1k,
    storage_has_key_10b_key_10kib_value_1k,
    storage_iter_prefix_10b_1k,
    storage_iter_prefix_10kib_1k,
    storage_iter_range_10b_from_10b_to_1k,
    storage_iter_range_10kib_from_10b_to_1k,
    storage_iter_range_10b_from_10kib_to_1k,

    storage_next_10b_from_10b_to_1k_10b_key_10b_value,
    storage_next_10kib_from_10b_to_1k_10b_key_10b_value,
    storage_next_10b_from_10kib_to_1k_10b_key_10b_value,

    storage_next_10b_from_10b_to_1k_10kib_key_10b_value,
    storage_next_10kib_from_10b_to_1k_10kib_key_10b_value,
    storage_next_10b_from_10kib_to_1k_10kib_key_10b_value,

    storage_next_10b_from_10b_to_1k_10b_key_10kib_value,
    storage_next_10kib_from_10b_to_1k_10b_key_10kib_value,
    storage_next_10b_from_10kib_to_1k_10b_key_10kib_value,

    promise_and_100k,
    promise_and_100k_on_1k_and,
    promise_return_100k,
    data_producer_10b,
    data_producer_100kib,
    data_receipt_10b_1000,
    data_receipt_100kib_1000,
    cpu_ram_soak_test,
}

pub fn run(mut config: Config) -> RuntimeConfig {
    let mut m = Measurements::new();
    config.block_sizes = vec![100];
    // Measure the speed of processing empty receipts.
    measure_actions(Metric::Receipt, &mut m, &config, None, vec![], false, false);

    // Measure the speed of processing simple transfers.
    measure_actions(
        Metric::ActionTransfer,
        &mut m,
        &config,
        None,
        vec![Action::Transfer(TransferAction { deposit: 1 })],
        false,
        false,
    );

    // Measure the speed of creating account.
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let mut f = || {
        let account_idx = rand::thread_rng().gen::<usize>() % config.active_accounts;
        let account_id = get_account_id(account_idx);
        let other_account_id = format!("random_account_{}", rand::thread_rng().gen::<usize>());
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id,
            other_account_id,
            &signer,
            vec![Action::CreateAccount(CreateAccountAction {})],
            CryptoHash::default(),
        )
    };
    measure_transactions(Metric::ActionCreateAccount, &mut m, &config, None, &mut f, false);

    // Measure the speed of deleting an account.
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    let mut deleted_accounts = HashSet::new();
    let mut beneficiaries = HashSet::new();
    let mut f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if !deleted_accounts.contains(&x) & &!beneficiaries.contains(&x) {
                break x;
            }
        };
        let beneficiary_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if !deleted_accounts.contains(&x) && x != account_idx {
                break x;
            }
        };
        deleted_accounts.insert(account_idx);
        beneficiaries.insert(beneficiary_idx);
        let account_id = get_account_id(account_idx);
        let beneficiary_id = get_account_id(beneficiary_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            account_id.clone(),
            &signer,
            vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })],
            CryptoHash::default(),
        )
    };
    measure_transactions(Metric::ActionDeleteAccount, &mut m, &config, None, &mut f, false);

    // Measure the speed of adding a full access key.
    measure_actions(
        Metric::ActionAddFullAccessKey,
        &mut m,
        &config,
        None,
        vec![Action::AddKey(AddKeyAction {
            public_key: serde_json::from_str(
                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
            )
            .unwrap(),
            access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
        })],
        true,
        true,
    );

    // Measure the speed of adding a function call access key.
    measure_actions(
        Metric::ActionAddFunctionAccessKey1Method,
        &mut m,
        &config,
        None,
        vec![Action::AddKey(AddKeyAction {
            public_key: serde_json::from_str(
                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
            )
            .unwrap(),
            access_key: AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(100),
                    receiver_id: get_account_id(0),
                    method_names: vec!["method1".to_string()],
                }),
            },
        })],
        true,
        true,
    );

    // Measure the speed of adding an access key with 1k methods each 10bytes long.
    let many_methods: Vec<_> = (0..1000).map(|i| format!("a123456{:03}", i)).collect();
    measure_actions(
        Metric::ActionAddFunctionAccessKey1000Methods,
        &mut m,
        &config,
        None,
        vec![Action::AddKey(AddKeyAction {
            public_key: serde_json::from_str(
                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
            )
            .unwrap(),
            access_key: AccessKey {
                nonce: 0,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(100),
                    receiver_id: get_account_id(0),
                    method_names: many_methods,
                }),
            },
        })],
        true,
        true,
    );

    // Measure the speed of deleting an access key.
    let mut nonces: HashMap<usize, u64> = HashMap::new();
    // Accounts with deleted access keys.
    let mut deleted_accounts = HashSet::new();
    let mut f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if !deleted_accounts.contains(&x) {
                break x;
            }
        };
        deleted_accounts.insert(account_idx);
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            account_id.clone(),
            &signer,
            vec![Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key.clone() })],
            CryptoHash::default(),
        )
    };
    measure_transactions(Metric::ActionDeleteAccessKey, &mut m, &config, None, &mut f, false);

    // Measure the speed of staking.
    measure_actions(
        Metric::ActionStake,
        &mut m,
        &config,
        None,
        vec![Action::Stake(StakeAction {
            stake: 1,
            public_key: PublicKey::empty(KeyType::ED25519),
        })],
        true,
        true,
    );

    // Measure the speed of deploying some code.
    let code_10k = include_bytes!("../test-contract/res/small_contract.wasm");
    let code_100k = include_bytes!("../test-contract/res/medium_contract.wasm");
    let code_1m = include_bytes!("../test-contract/res/large_contract.wasm");
    let curr_code = RefCell::new(code_10k.to_vec());
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
            account_id.clone(),
            &signer,
            vec![Action::DeployContract(DeployContractAction { code: curr_code.borrow().clone() })],
            CryptoHash::default(),
        )
    };
    let mut testbed =
        measure_transactions(Metric::ActionDeploy10K, &mut m, &config, None, &mut f, false);
    *curr_code.borrow_mut() = code_100k.to_vec();
    testbed = measure_transactions(
        Metric::ActionDeploy100K,
        &mut m,
        &config,
        Some(testbed),
        &mut f,
        false,
    );
    *curr_code.borrow_mut() = code_1m.to_vec();
    testbed =
        measure_transactions(Metric::ActionDeploy1M, &mut m, &config, Some(testbed), &mut f, false);

    let ad: Vec<_> = accounts_deployed.into_iter().collect();

    testbed = measure_function(
        Metric::warmup,
        "noop",
        &mut m,
        testbed,
        &ad,
        &mut nonces,
        &config,
        false,
        vec![],
    );

    testbed = measure_function(
        Metric::noop_1MiB,
        "noop",
        &mut m,
        testbed,
        &ad,
        &mut nonces,
        &config,
        false,
        (&[0u8; 1024 * 1024]).to_vec(),
    );

    testbed = measure_function(
        Metric::noop,
        "noop",
        &mut m,
        testbed,
        &ad,
        &mut nonces,
        &config,
        false,
        vec![],
    );

    config.block_sizes = vec![2];

    let v = calls_helper! {
    cpu_ram_soak_test => cpu_ram_soak_test,
    base_1M => base_1M,
    read_memory_10b_10k => read_memory_10b_10k,
    read_memory_1Mib_10k => read_memory_1Mib_10k,
    write_memory_10b_10k => write_memory_10b_10k,
    write_memory_1Mib_10k => write_memory_1Mib_10k,
    read_register_10b_10k => read_register_10b_10k,
    read_register_1Mib_10k => read_register_1Mib_10k,
    write_register_10b_10k => write_register_10b_10k,
    write_register_1Mib_10k => write_register_1Mib_10k,
    utf8_log_10b_10k => utf8_log_10b_10k,
    utf8_log_10kib_10k => utf8_log_10kib_10k,
    nul_utf8_log_10b_10k => nul_utf8_log_10b_10k,
    nul_utf8_log_10kib_10k => nul_utf8_log_10kib_10k,
    utf16_log_10b_10k => utf16_log_10b_10k,
    utf16_log_10kib_10k => utf16_log_10kib_10k,
    nul_utf16_log_10b_10k => nul_utf16_log_10b_10k,
    nul_utf16_log_10kib_10k => nul_utf16_log_10kib_10k,
    sha256_10b_10k => sha256_10b_10k,
    sha256_10kib_10k => sha256_10kib_10k,
    keccak256_10b_10k => keccak256_10b_10k,
    keccak256_10kib_10k => keccak256_10kib_10k,
    keccak512_10b_10k => keccak512_10b_10k,
    keccak512_10kib_10k => keccak512_10kib_10k,
    storage_write_10b_key_10b_value_1k => storage_write_10b_key_10b_value_1k,
    storage_read_10b_key_10b_value_1k => storage_read_10b_key_10b_value_1k,
    storage_has_key_10b_key_10b_value_1k => storage_has_key_10b_key_10b_value_1k,
    storage_iter_prefix_10b_1k => storage_iter_prefix_10b_1k,
    storage_iter_range_10b_from_10b_to_1k => storage_iter_range_10b_from_10b_to_1k,
    storage_next_10b_from_10b_to_1k_10b_key_10b_value =>   storage_next_10b_from_10b_to_1k,
    storage_next_10kib_from_10b_to_1k_10b_key_10b_value =>   storage_next_10kib_from_10b_to_1k,
    storage_next_10b_from_10kib_to_1k_10b_key_10b_value =>   storage_next_10b_from_10kib_to_1k,
    storage_remove_10b_key_10b_value_1k => storage_remove_10b_key_10b_value_1k,
    storage_write_10kib_key_10b_value_1k => storage_write_10kib_key_10b_value_1k,
    storage_read_10kib_key_10b_value_1k => storage_read_10kib_key_10b_value_1k,
    storage_has_key_10kib_key_10b_value_1k => storage_has_key_10kib_key_10b_value_1k,
    storage_iter_prefix_10kib_1k => storage_iter_prefix_10kib_1k,
    storage_iter_range_10kib_from_10b_to_1k => storage_iter_range_10kib_from_10b_to_1k,
    storage_iter_range_10b_from_10kib_to_1k => storage_iter_range_10b_from_10kib_to_1k,
    storage_next_10b_from_10b_to_1k_10kib_key_10b_value =>   storage_next_10b_from_10b_to_1k ,
    storage_next_10kib_from_10b_to_1k_10kib_key_10b_value =>   storage_next_10kib_from_10b_to_1k,
    storage_next_10b_from_10kib_to_1k_10kib_key_10b_value =>   storage_next_10b_from_10kib_to_1k ,
    storage_remove_10kib_key_10b_value_1k => storage_remove_10kib_key_10b_value_1k,
    storage_write_10b_key_10kib_value_1k => storage_write_10b_key_10kib_value_1k,
    storage_write_10b_key_10kib_value_1k_evict => storage_write_10b_key_10kib_value_1k,
    storage_read_10b_key_10kib_value_1k => storage_read_10b_key_10kib_value_1k,
    storage_has_key_10b_key_10kib_value_1k => storage_has_key_10b_key_10kib_value_1k,
    storage_next_10b_from_10b_to_1k_10b_key_10kib_value =>      storage_next_10b_from_10b_to_1k,
    storage_next_10kib_from_10b_to_1k_10b_key_10kib_value =>   storage_next_10kib_from_10b_to_1k ,
    storage_next_10b_from_10kib_to_1k_10b_key_10kib_value =>   storage_next_10b_from_10kib_to_1k ,
    storage_remove_10b_key_10kib_value_1k =>   storage_remove_10b_key_10kib_value_1k ,
    promise_and_100k => promise_and_100k,
    promise_and_100k_on_1k_and => promise_and_100k_on_1k_and,
    promise_return_100k => promise_return_100k,
    data_producer_10b => data_producer_10b,
    data_producer_100kib => data_producer_100kib,
    data_receipt_10b_1000 => data_receipt_10b_1000,
    data_receipt_100kib_1000 => data_receipt_100kib_1000
        };

    // Measure the speed of all extern function calls.
    for (metric, method_name) in v {
        testbed = measure_function(
            metric,
            method_name,
            &mut m,
            testbed,
            &ad,
            &mut nonces,
            &config,
            false,
            vec![],
        );
    }

    get_runtime_config(&m)

    //    let mut csv_path = PathBuf::from(&config.state_dump_path);
    //    csv_path.push("./metrics.csv");
    //    m.save_to_csv(csv_path.as_path());
    //
    //    m.plot(PathBuf::from(&config.state_dump_path).as_path());
}

/// Converts time of a certain action to a fee, spliting it evenly between send and execution fee.
fn f64_to_fee(value: f64) -> Fee {
    let value = if value >= 0f64 { value } else { 0f64 };
    let value: u64 = (value * GAS_IN_NANOS) as u64;
    Fee { send_sir: value / 2, send_not_sir: value / 2, execution: value / 2 }
}

fn f64_to_gas(value: f64) -> u64 {
    let value = if value >= 0f64 { value } else { 0f64 };
    (value * GAS_IN_NANOS) as u64
}

fn get_runtime_fees_config(measurement: &Measurements) -> RuntimeFeesConfig {
    use crate::runtime_fees_generator::ReceiptFeesFloat::*;
    let generator = RuntimeFeesGenerator::new(measurement);
    let pure = generator.compute();
    RuntimeFeesConfig {
        action_receipt_creation_config: f64_to_fee(pure[&ActionReceiptCreation]),
        data_receipt_creation_config: DataReceiptCreationConfig {
            base_cost: f64_to_fee(pure[&DataReceiptCreationBase]),
            cost_per_byte: f64_to_fee(pure[&DataReceiptCreationPerByte]),
        },
        action_creation_config: ActionCreationConfig {
            create_account_cost: f64_to_fee(pure[&ActionCreateAccount]),
            deploy_contract_cost: f64_to_fee(pure[&ActionDeployContractBase]),
            deploy_contract_cost_per_byte: f64_to_fee(pure[&ActionDeployContractPerByte]),
            function_call_cost: f64_to_fee(pure[&ActionFunctionCallBase]),
            function_call_cost_per_byte: f64_to_fee(pure[&ActionFunctionCallPerByte]),
            transfer_cost: f64_to_fee(pure[&ActionTransfer]),
            stake_cost: f64_to_fee(pure[&ActionStake]),
            add_key_cost: AccessKeyCreationConfig {
                full_access_cost: f64_to_fee(pure[&ActionAddFullAccessKey]),
                function_call_cost: f64_to_fee(pure[&ActionAddFunctionAccessKeyBase]),
                function_call_cost_per_byte: f64_to_fee(pure[&ActionAddFunctionAccessKeyPerByte]),
            },
            delete_key_cost: f64_to_fee(pure[&ActionDeleteKey]),
            delete_account_cost: f64_to_fee(pure[&ActionDeleteAccount]),
        },
        ..Default::default()
    }
}

fn get_ext_costs_config(measurement: &Measurements) -> ExtCostsConfig {
    let mut generator = ExtCostsGenerator::new(measurement);
    let pure = generator.compute();
    use ExtCosts::*;
    ExtCostsConfig {
        base: f64_to_gas(pure[&base]),
        read_memory_base: f64_to_gas(pure[&read_memory_base]),
        read_memory_byte: f64_to_gas(pure[&read_memory_byte]),
        write_memory_base: f64_to_gas(pure[&write_memory_base]),
        write_memory_byte: f64_to_gas(pure[&write_memory_byte]),
        read_register_base: f64_to_gas(pure[&read_register_base]),
        read_register_byte: f64_to_gas(pure[&read_register_byte]),
        write_register_base: f64_to_gas(pure[&write_register_base]),
        write_register_byte: f64_to_gas(pure[&write_register_byte]),
        utf8_decoding_base: f64_to_gas(pure[&utf8_decoding_base]),
        utf8_decoding_byte: f64_to_gas(pure[&utf8_decoding_byte]),
        utf16_decoding_base: f64_to_gas(pure[&utf16_decoding_base]),
        utf16_decoding_byte: f64_to_gas(pure[&utf16_decoding_byte]),
        sha256_base: f64_to_gas(pure[&sha256_base]),
        sha256_byte: f64_to_gas(pure[&sha256_byte]),
        keccak256_base: f64_to_gas(pure[&keccak256_base]),
        keccak256_byte: f64_to_gas(pure[&keccak256_byte]),
        keccak512_base: f64_to_gas(pure[&keccak512_base]),
        keccak512_byte: f64_to_gas(pure[&keccak512_byte]),
        log_base: f64_to_gas(pure[&log_base]),
        log_byte: f64_to_gas(pure[&log_byte]),
        storage_write_base: f64_to_gas(pure[&storage_write_base]),
        storage_write_key_byte: f64_to_gas(pure[&storage_write_key_byte]),
        storage_write_value_byte: f64_to_gas(pure[&storage_write_value_byte]),
        storage_write_evicted_byte: f64_to_gas(pure[&storage_write_evicted_byte]),
        storage_read_base: f64_to_gas(pure[&storage_read_base]),
        storage_read_key_byte: f64_to_gas(pure[&storage_read_key_byte]),
        storage_read_value_byte: f64_to_gas(pure[&storage_read_value_byte]),
        storage_remove_base: f64_to_gas(pure[&storage_remove_base]),
        storage_remove_key_byte: f64_to_gas(pure[&storage_remove_key_byte]),
        storage_remove_ret_value_byte: f64_to_gas(pure[&storage_remove_ret_value_byte]),
        storage_has_key_base: f64_to_gas(pure[&storage_has_key_base]),
        storage_has_key_byte: f64_to_gas(pure[&storage_has_key_byte]),
        storage_iter_create_prefix_base: f64_to_gas(pure[&storage_iter_create_prefix_base]),
        storage_iter_create_prefix_byte: f64_to_gas(pure[&storage_iter_create_prefix_byte]),
        storage_iter_create_range_base: f64_to_gas(pure[&storage_iter_create_range_base]),
        storage_iter_create_from_byte: f64_to_gas(pure[&storage_iter_create_from_byte]),
        storage_iter_create_to_byte: f64_to_gas(pure[&storage_iter_create_to_byte]),
        storage_iter_next_base: f64_to_gas(pure[&storage_iter_next_base]),
        storage_iter_next_key_byte: f64_to_gas(pure[&storage_iter_next_key_byte]),
        storage_iter_next_value_byte: f64_to_gas(pure[&storage_iter_next_value_byte]),
        // TODO: Actually compute it once our storage is complete.
        touching_trie_node: 1,
        promise_and_base: f64_to_gas(pure[&promise_and_base]),
        promise_and_per_promise: f64_to_gas(pure[&promise_and_per_promise]),
        promise_return: f64_to_gas(pure[&promise_return]),
    }
}

fn get_vm_config(measurement: &Measurements) -> VMConfig {
    VMConfig {
        ext_costs: get_ext_costs_config(measurement),
        // TODO: Figure out whether we need this fee at all. If we do what should be the memory
        // growth cost.
        grow_mem_cost: 1,
        regular_op_cost: f64_to_gas(nanosec_per_op()) as u32,
        limit_config: VMLimitConfig::default(),
    }
}

fn get_runtime_config(measurement: &Measurements) -> RuntimeConfig {
    RuntimeConfig {
        transaction_costs: get_runtime_fees_config(measurement),
        wasm_config: get_vm_config(measurement),
        // TODO: Figure out the following values.
        storage_cost_byte_per_block: 5000000,
        poke_threshold: 86400,
        account_length_baseline_cost_per_block: 207909813343189798558,
    }
}
