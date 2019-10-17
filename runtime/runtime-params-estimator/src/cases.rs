use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use rand::distributions::Standard;
use rand::seq::SliceRandom;
use rand::Rng;

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

pub fn run(config: Config) {
    let mut m = Measurements::new();
    // Measure the speed of processing empty receipts.
    measure_actions("receipt only", 1, None, &mut m, &config, None, vec![], false, false);

    // Measure the speed of processing simple transfers.
    measure_actions(
        "transfer",
        1,
        None,
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
    measure_transactions("create_account", 1, None, &mut m, &config, None, &mut f);

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
    measure_transactions("delete_account", 1, None, &mut m, &config, None, &mut f);

    // Measure the speed of adding a full access key.
    measure_actions(
        "add access key full",
        1,
        None,
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
        "add access key full",
        1,
        None,
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
                    method_names: vec![
                        "method1".to_string(),
                        "method2".to_string(),
                        "method3".to_string(),
                    ],
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
    measure_transactions("delete_access_key", 1, None, &mut m, &config, None, &mut f);

    // Measure the speed of staking.
    measure_actions(
        "stake",
        1,
        None,
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
    let small_code = include_bytes!("../test-contract/res/small_contract.wasm");
    let medium_code = include_bytes!("../test-contract/res/medium_contract.wasm");
    let large_code = include_bytes!("../test-contract/res/large_contract.wasm");
    let curr_code = RefCell::new(small_code.to_vec());
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
        measure_transactions("deploy", 1, Some(small_code.len()), &mut m, &config, None, &mut f);
    *curr_code.borrow_mut() = medium_code.to_vec();
    testbed = measure_transactions(
        "deploy",
        1,
        Some(medium_code.len()),
        &mut m,
        &config,
        Some(testbed),
        &mut f,
    );
    *curr_code.borrow_mut() = large_code.to_vec();
    testbed = measure_transactions(
        "deploy",
        1,
        Some(large_code.len()),
        &mut m,
        &config,
        Some(testbed),
        &mut f,
    );

    let ad: Vec<_> = accounts_deployed.into_iter().collect();

    // Measure the speed of processing function calls that do nothing.
    let mut f = || {
        let account_idx = *ad.as_slice().choose(&mut rand::thread_rng()).unwrap();
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        let function_call = Action::FunctionCall(FunctionCallAction {
            method_name: "noop".to_string(),
            args: vec![],
            gas: 10_000_000,
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
    testbed = measure_transactions("call noop", 1, None, &mut m, &config, Some(testbed), &mut f);

    // Measure the speed of all extern function calls.
    testbed = measure_function("call_fixture10", 10, &mut m, testbed, &ad, &mut nonces, &config);
    for (method_name, n) in &[
        ("call_input", 20),
        ("call_input_register_len", 20),
        ("call_input_read_register", 20),
        ("call_current_account_id", 100),
        ("call_signer_account_id", 100),
        ("call_signer_account_pk", 100),
        ("call_predecessor_account_id", 100),
        ("call_block_index", 100),
        ("call_storage_usage", 100),
        ("call_account_balance", 100),
        ("call_attached_deposit", 100),
        ("call_prepaid_gas", 100),
        ("call_used_gas", 100),
        ("call_random_seed", 100),
        ("call_sha256", 20),
        ("call_value_return", 20),
        ("call_log_utf8", 20),
        ("call_log_utf16", 20),
        ("call_promise_batch_create", 100),
        ("call_promise_batch_create_promise_batch_then", 100),
        ("call_promise_batch_create_promise_batch_action_create_account", 20),
        ("call_promise_batch_create_promise_batch_action_create_account_batch_action_deploy_contract", 20),
        ("call_promise_results_count", 100),
        ("call_promise_batch_create_promise_return", 20),
        ("call_storage_write", 20),
        ("call_storage_read", 20),
        ("call_storage_remove", 20),
        ("call_storage_has_key", 20),
        ("call_storage_iter_prefix", 20),
        ("call_storage_iter_range", 20),
        ("call_storage_iter_next", 20),
    ] {
        testbed = measure_function(method_name, *n, &mut m, testbed, &ad, &mut nonces, &config);
    }

    let mut csv_path = PathBuf::from(&config.state_dump_path);
    csv_path.push("./metrics.csv");
    m.save_to_csv(csv_path.as_path());

    m.plot(PathBuf::from(&config.state_dump_path).as_path());
}

fn create_args(n: usize, blob_size: usize) -> Vec<u8> {
    let mut res = vec![];
    res.extend_from_slice(&(n as u64).to_le_bytes());
    let blob: Vec<u8> = rand::thread_rng().sample_iter(Standard).take(blob_size).collect();
    let blob: Vec<u8> = blob.into_iter().map(|x| x % (b'z' - b'a' + 1) + b'a').collect();
    res.extend(blob);
    res
}

fn measure_function(
    method_name: &'static str,
    n: usize,
    measurements: &mut Measurements,
    mut testbed: RuntimeTestbed,
    accounts_deployed: &[usize],
    nonces: &mut HashMap<usize, u64>,
    config: &Config,
) -> RuntimeTestbed {
    for blob_size in &[10, 10000] {
        testbed = measure_function_with_blob_size(
            method_name,
            n,
            *blob_size as _,
            measurements,
            testbed,
            accounts_deployed,
            nonces,
            config,
        );
    }
    testbed
}

fn measure_function_with_blob_size(
    method_name: &'static str,
    n: usize,
    blob_size: usize,
    measurements: &mut Measurements,
    testbed: RuntimeTestbed,
    accounts_deployed: &[usize],
    nonces: &mut HashMap<usize, u64>,
    config: &Config,
) -> RuntimeTestbed {
    // Measure the speed of creating a function fixture with 1MiB input.
    let mut f = || {
        let account_idx = *accounts_deployed.choose(&mut rand::thread_rng()).unwrap();
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        let function_call = Action::FunctionCall(FunctionCallAction {
            method_name: method_name.to_string(),
            args: create_args(n, blob_size),
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
    measure_transactions(
        method_name,
        n,
        Some(blob_size),
        measurements,
        config,
        Some(testbed),
        &mut f,
    )
}
