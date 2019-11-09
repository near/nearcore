use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};

use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, SignedTransaction, StakeAction, TransferAction,
};

use crate::stats::Measurements;
use crate::testbed::RuntimeTestbed;
use crate::testbed_runners::{get_account_id, measure_actions, measure_transactions, Config};

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

#[derive(Debug, PartialEq, Eq, Ord, PartialOrd, Clone)]
pub enum Metric {
    Receipt,
    ActionTransfer,
    ActionCreateAccount,
    ActionDeleteAccount,
    ActionAddFullAccessKey,
    /// Create access key with 1 method.
    ActionAddFunctionAccessKey1Method,
    /// Create access key with 1k methods.
    ActionAddFunctionAccessKey1000Methods,
    ActionDeleteAccessKey,
    ActionStake,
    /// Deploy 10K contract.
    ActionDeploy10K,
    /// Deploy 100K contract.
    ActionDeploy100K,
    /// Deploy 1M contract.
    ActionDeploy1M,
    /// Call noop contract method.
    warmup,
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
    storage_write_10b_key_10b_value_1k,
    storage_write_10kib_key_10b_value_1k,
    storage_write_10b_key_10kib_value_1k,
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

pub fn run(config: Config) {
    let mut m = Measurements::new();
    //    // Measure the speed of processing empty receipts.
    //    measure_actions(Metric::Receipt, &mut m, &config, None, vec![], false, false);
    //
    //    // Measure the speed of processing simple transfers.
    //    measure_actions(
    //        Metric::ActionTransfer,
    //        &mut m,
    //        &config,
    //        None,
    //        vec![Action::Transfer(TransferAction { deposit: 1 })],
    //        false,
    //        false,
    //    );
    //
    //    // Measure the speed of creating account.
    //    let mut nonces: HashMap<usize, u64> = HashMap::new();
    //    let mut f = || {
    //        let account_idx = rand::thread_rng().gen::<usize>() % config.active_accounts;
    //        let account_id = get_account_id(account_idx);
    //        let other_account_id = format!("random_account_{}", rand::thread_rng().gen::<usize>());
    //        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
    //        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
    //        SignedTransaction::from_actions(
    //            nonce as u64,
    //            account_id,
    //            other_account_id,
    //            &signer,
    //            vec![Action::CreateAccount(CreateAccountAction {})],
    //            CryptoHash::default(),
    //        )
    //    };
    //    measure_transactions(Metric::ActionCreateAccount, &mut m, &config, None, &mut f, false);
    //
    //    // Measure the speed of deleting an account.
    //    let mut nonces: HashMap<usize, u64> = HashMap::new();
    //    let mut deleted_accounts = HashSet::new();
    //    let mut beneficiaries = HashSet::new();
    //    let mut f = || {
    //        let account_idx = loop {
    //            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
    //            if !deleted_accounts.contains(&x) & &!beneficiaries.contains(&x) {
    //                break x;
    //            }
    //        };
    //        let beneficiary_idx = loop {
    //            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
    //            if !deleted_accounts.contains(&x) && x != account_idx {
    //                break x;
    //            }
    //        };
    //        deleted_accounts.insert(account_idx);
    //        beneficiaries.insert(beneficiary_idx);
    //        let account_id = get_account_id(account_idx);
    //        let beneficiary_id = get_account_id(beneficiary_idx);
    //        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
    //        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
    //        SignedTransaction::from_actions(
    //            nonce as u64,
    //            account_id.clone(),
    //            account_id.clone(),
    //            &signer,
    //            vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id })],
    //            CryptoHash::default(),
    //        )
    //    };
    //    measure_transactions(Metric::ActionDeleteAccount, &mut m, &config, None, &mut f, false);
    //
    //    // Measure the speed of adding a full access key.
    //    measure_actions(
    //        Metric::ActionAddFullAccessKey,
    //        &mut m,
    //        &config,
    //        None,
    //        vec![Action::AddKey(AddKeyAction {
    //            public_key: serde_json::from_str(
    //                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
    //            )
    //            .unwrap(),
    //            access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
    //        })],
    //        true,
    //        true,
    //    );
    //
    //    // Measure the speed of adding a function call access key.
    //    measure_actions(
    //        Metric::ActionAddFunctionAccessKey1Method,
    //        &mut m,
    //        &config,
    //        None,
    //        vec![Action::AddKey(AddKeyAction {
    //            public_key: serde_json::from_str(
    //                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
    //            )
    //            .unwrap(),
    //            access_key: AccessKey {
    //                nonce: 0,
    //                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
    //                    allowance: Some(100),
    //                    receiver_id: get_account_id(0),
    //                    method_names: vec!["method1".to_string()],
    //                }),
    //            },
    //        })],
    //        true,
    //        true,
    //    );
    //
    //    // Measure the speed of adding an access key with 1k methods each 10bytes long.
    //    let many_methods: Vec<_> = (0..1000).map(|i| format!("a123456{:03}", i)).collect();
    //    measure_actions(
    //        Metric::ActionAddFunctionAccessKey1000Methods,
    //        &mut m,
    //        &config,
    //        None,
    //        vec![Action::AddKey(AddKeyAction {
    //            public_key: serde_json::from_str(
    //                "\"ed25519:DcA2MzgpJbrUATQLLceocVckhhAqrkingax4oJ9kZ847\"",
    //            )
    //            .unwrap(),
    //            access_key: AccessKey {
    //                nonce: 0,
    //                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
    //                    allowance: Some(100),
    //                    receiver_id: get_account_id(0),
    //                    method_names: many_methods,
    //                }),
    //            },
    //        })],
    //        true,
    //        true,
    //    );
    //
    //    // Measure the speed of deleting an access key.
    //    let mut nonces: HashMap<usize, u64> = HashMap::new();
    //    // Accounts with deleted access keys.
    //    let mut deleted_accounts = HashSet::new();
    //    let mut f = || {
    //        let account_idx = loop {
    //            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
    //            if !deleted_accounts.contains(&x) {
    //                break x;
    //            }
    //        };
    //        deleted_accounts.insert(account_idx);
    //        let account_id = get_account_id(account_idx);
    //        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
    //        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
    //        SignedTransaction::from_actions(
    //            nonce as u64,
    //            account_id.clone(),
    //            account_id.clone(),
    //            &signer,
    //            vec![Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key.clone() })],
    //            CryptoHash::default(),
    //        )
    //    };
    //    measure_transactions(Metric::ActionDeleteAccessKey, &mut m, &config, None, &mut f, false);
    //
    //    // Measure the speed of staking.
    //    measure_actions(
    //        Metric::ActionStake,
    //        &mut m,
    //        &config,
    //        None,
    //        vec![Action::Stake(StakeAction {
    //            stake: 1,
    //            public_key: PublicKey::empty(KeyType::ED25519),
    //        })],
    //        true,
    //        true,
    //    );

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

    let v = calls_helper! {
    warmup => noop,
    noop => noop,
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
    data_receipt_100kib_1000 => data_receipt_100kib_1000,
    cpu_ram_soak_test => cpu_ram_soak_test
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
        );
    }

    //    let fees = crate::runtime_fees_generator::RuntimeFeesGenerator::new(&m);
    //    println!("{}", fees);
    //    let ext_costs = crate::ext_costs_generator::ExtCostsGenerator::new(&m);
    //    println!("{}", ext_costs);

    //    let mut csv_path = PathBuf::from(&config.state_dump_path);
    //    csv_path.push("./metrics.csv");
    //    m.save_to_csv(csv_path.as_path());
    //
    //    m.plot(PathBuf::from(&config.state_dump_path).as_path());
}

fn measure_function(
    metric: Metric,
    method_name: &'static str,
    measurements: &mut Measurements,
    testbed: RuntimeTestbed,
    accounts_deployed: &[usize],
    nonces: &mut HashMap<usize, u64>,
    config: &Config,
    allow_failures: bool,
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
            args: vec![],
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
