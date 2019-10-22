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
    CallNoop,
    CallInput1KW10B,
    CallInput1KW10KB,
    CallInputRegisterLen1KW10B,
    CallInputRegisterLen1KW10KB,
    CallInputReadRegister1KW10B,
    CallInputReadRegister1KW1KB,
    CallCurrentAccountId10K,
    CallSignerAccountId10K,
    CallSignerAccountPK10K,
    CallPredecessorAccountId10K,
    CallBlockIndex10K,
    CallStorageUsage10K,
    CallAccountBalance10K,
    CallAttachedDeposit10K,
    CallPrepaidGas10K,
    CallUsedGas10K,
    CallRandomSeed10K,
    CallSHA25610KW10B,
    CallSHA25610KW1KB,
    CallValueReturn10KW10B,
    CallValueReturn10KW1KB,
    CallLogUTF810KW10B,
    CallLogUTF810KW1KB,
    CallLogUTF1610KW10B,
    CallLogUTF1610KW1KB,
    CallPromiseBatchCreate10K,
    CallPromiseBatchCreateThen10K,
    CallPromiseBatchCreateAccount10K,
    CallPromiseBatchCreateDeploy10K,
    CallPromiseResultsCount10K,
    CallPromiseBatchCreateReturn10K,
    CallStorageWrite100W10B,
    CallStorageWrite100W1KB,
    CallStorageRead100W10B,
    CallStorageRead100W1KB,
    CallStorageRemove100W10B,
    CallStorageRemove100W1KB,
    CallStorageHasKey100W10B,
    CallStorageHasKey100W1KB,
    CallStorageIterPrefix100W10B,
    CallStorageIterPrefix100W1KB,
    CallStorageIterRange100W10B,
    CallStorageIterRange100W1KB,
    CallStorageIterNext1KW10B,
    CallStorageIterNext1KW1KB,
    CalCpuRamSoakTest,
}

pub fn run(config: Config) {
    let mut m = Measurements::new();
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
    measure_transactions(Metric::ActionCreateAccount, &mut m, &config, None, &mut f, false);

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
    testbed = measure_transactions(Metric::CallNoop, &mut m, &config, Some(testbed), &mut f, false);

    // Measure the speed of all extern function calls.
    for (allow_failures, metric, method_name, arg_size) in vec![
        (false, Metric::CallInput1KW10B, "input_1k", 10),
        (false, Metric::CallInput1KW10KB, "input_1k", 10 * 1024),
        (false, Metric::CallInputRegisterLen1KW10B, "input_register_len_1k", 10),
        (false, Metric::CallInputRegisterLen1KW10KB, "input_register_len_1k", 10 * 1024),
        (false, Metric::CallInputReadRegister1KW10B, "input_read_register_1k", 10),
        (false, Metric::CallInputReadRegister1KW1KB, "input_read_register_1k", 1024),
        (false, Metric::CallCurrentAccountId10K, "current_account_id_10k", 0),
        (false, Metric::CallSignerAccountId10K, "signer_account_id_10k", 0),
        (false, Metric::CallSignerAccountPK10K, "signer_account_pk_10k", 0),
        (false, Metric::CallPredecessorAccountId10K, "predecessor_account_id_10k", 0),
        (false, Metric::CallBlockIndex10K, "block_index_10k", 0),
        (false, Metric::CallStorageUsage10K, "storage_usage_10k", 0),
        (false, Metric::CallAccountBalance10K, "account_balance_10k", 0),
        (false, Metric::CallAttachedDeposit10K, "attached_deposit_10k", 0),
        (false, Metric::CallPrepaidGas10K, "prepaid_gas_10k", 0),
        (false, Metric::CallUsedGas10K, "used_gas_10k", 0),
        (false, Metric::CallRandomSeed10K, "random_seed_10k", 0),
        (false, Metric::CallSHA25610KW10B, "sha256_10k", 10),
        (false, Metric::CallSHA25610KW1KB, "sha256_10k", 1024),
        (false, Metric::CallValueReturn10KW10B, "value_return_10k", 10),
        (false, Metric::CallValueReturn10KW1KB, "value_return_10k", 1024),
        (false, Metric::CallLogUTF810KW10B, "log_utf8_10k", 10),
        (false, Metric::CallLogUTF810KW1KB, "log_utf8_10k", 1024),
        (false, Metric::CallLogUTF1610KW10B, "log_utf16_10k", 10),
        (false, Metric::CallLogUTF1610KW1KB, "log_utf16_10k", 1024),
        (false, Metric::CallPromiseBatchCreate10K, "promise_batch_create_10k", 10),
        (false, Metric::CallPromiseBatchCreateThen10K, "promise_batch_create_then_10k", 10),
        // Action creation is complex. This measurement does not include measuring the actual action
        // therefore we try creating the same account over and over and fail.
        (true, Metric::CallPromiseBatchCreateAccount10K, "promise_batch_create_account_10k", 10),
        (true, Metric::CallPromiseBatchCreateDeploy10K, "promise_batch_create_deploy_10k", 10),
        (false, Metric::CallPromiseResultsCount10K, "promise_results_count_10k", 10),
        (false, Metric::CallPromiseBatchCreateReturn10K, "promise_batch_create_return_10k", 0),
        (false, Metric::CallStorageWrite100W10B, "storage_write_100", 10),
        (false, Metric::CallStorageHasKey100W10B, "storage_has_key_100", 10),
        (false, Metric::CallStorageRead100W10B, "storage_read_100", 10),
        (false, Metric::CallStorageIterNext1KW10B, "storage_iter_next_1k", 10),
        (false, Metric::CallStorageIterPrefix100W10B, "storage_iter_prefix_100", 10),
        (false, Metric::CallStorageIterRange100W10B, "storage_iter_range_100", 10),
        // Key-value deletion is executed the last, to allow other storage benchmarks to use
        // key-values.
        (false, Metric::CallStorageRemove100W10B, "storage_remove_100", 10),
        // Difficulty of iterating over trie depends on the size of keys. Therefore we run iterator
        // tests after the corresponding write tests.
        (false, Metric::CallStorageWrite100W1KB, "storage_write_100", 1024),
        (false, Metric::CallStorageHasKey100W1KB, "storage_has_key_100", 1024),
        (false, Metric::CallStorageRead100W1KB, "storage_read_100", 1024),
        (false, Metric::CallStorageIterNext1KW1KB, "storage_iter_next_1k", 1024),
        (false, Metric::CallStorageIterPrefix100W1KB, "storage_iter_prefix_100", 1024),
        (false, Metric::CallStorageIterRange100W1KB, "storage_iter_range_100", 1024),
        (false, Metric::CallStorageRemove100W1KB, "storage_remove_100", 1024),
        (false, Metric::CalCpuRamSoakTest, "cpu_ram_soak_test", 10 * 1024),
    ] {
        testbed = measure_function(
            metric,
            method_name,
            arg_size,
            &mut m,
            testbed,
            &ad,
            &mut nonces,
            &config,
            allow_failures,
        );
    }

    let mut csv_path = PathBuf::from(&config.state_dump_path);
    csv_path.push("./metrics.csv");
    m.save_to_csv(csv_path.as_path());

    m.plot(PathBuf::from(&config.state_dump_path).as_path());
}

fn measure_function(
    metric: Metric,
    method_name: &'static str,
    args_size: usize,
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
        let args: Vec<_> = (0..args_size).map(|_| rng.gen_range(b'a', b'z')).collect();
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        let function_call = Action::FunctionCall(FunctionCallAction {
            method_name: method_name.to_string(),
            args,
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
