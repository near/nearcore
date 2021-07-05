use num_rational::Ratio;
use rand::{Rng, SeedableRng};
use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::convert::TryFrom;
use std::process;
use std::sync::{Arc, Mutex};

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
use crate::testbed_runners::GasMetric;
use crate::testbed_runners::{get_account_id, measure_actions, measure_transactions, Config};
use crate::vm_estimator::{compute_compile_cost_vm, cost_per_op, load_and_compile};
use crate::TestContract;

use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::fees::{
    AccessKeyCreationConfig, ActionCreationConfig, DataReceiptCreationConfig, Fee,
    RuntimeFeesConfig,
};
use near_vm_logic::{ExtCosts, ExtCostsConfig, VMConfig, VMLimitConfig};

static SMALLEST_CODE: TestContract = TestContract::new("test-contract/res/smallest_contract.wasm");

cfg_if::cfg_if! {
    if #[cfg(feature = "nightly_protocol_features")] {
        static CODE_10K: TestContract = TestContract::new("test-contract/res/nightly_small_contract.wasm");
        static CODE_100K: TestContract = TestContract::new("test-contract/res/nightly_medium_contract.wasm");
        static CODE_1M: TestContract = TestContract::new("test-contract/res/nightly_large_contract.wasm");
    } else {
        static CODE_10K: TestContract = TestContract::new("test-contract/res/stable_small_contract.wasm");
        static CODE_100K: TestContract = TestContract::new("test-contract/res/stable_medium_contract.wasm");
        static CODE_1M: TestContract = TestContract::new("test-contract/res/stable_large_contract.wasm");
    }
}

/// How much gas there is in a nanosecond worth of computation.
const GAS_IN_MEASURE_UNIT: u128 = 1_000_000u128;

fn measure_function(
    metric: Metric,
    method_name: &'static str,
    measurements: &mut Measurements,
    testbed: Arc<Mutex<RuntimeTestbed>>,
    accounts_deployed: &[usize],
    nonces: &mut HashMap<usize, u64>,
    config: &Config,
    allow_failures: bool,
    args: Vec<u8>,
) -> Arc<Mutex<RuntimeTestbed>> {
    // Measure the speed of creating a function fixture with 1MiB input.
    let mut rng = rand_xorshift::XorShiftRng::from_seed([0u8; 16]);
    let testbed = testbed.clone();
    let mut accounts_deployed = accounts_deployed.to_vec();
    let mut f = || {
        let i = rng.gen::<usize>() % accounts_deployed.len();
        let account_idx = accounts_deployed[i];
        accounts_deployed.remove(i);
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let mut f_write = |account_idx, method_name: &str| {
            let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
            let function_call = Action::FunctionCall(FunctionCallAction {
                method_name: method_name.to_string(),
                args: args.clone(),
                gas: 10u64.pow(18),
                deposit: 0,
            });
            let block = vec![SignedTransaction::from_actions(
                nonce as u64,
                account_id.clone(),
                account_id.clone(),
                &signer,
                vec![function_call],
                CryptoHash::default(),
            )];
            let mut testbed = testbed.lock().unwrap();
            testbed.process_block(&block, allow_failures);
            testbed.process_blocks_until_no_receipts(allow_failures);
        };

        f_write(account_idx, "noop");
        match metric {
            Metric::storage_has_key_10b_key_10b_value_1k
            | Metric::storage_read_10b_key_10b_value_1k
            | Metric::storage_remove_10b_key_10b_value_1k => {
                f_write(account_idx, "storage_write_10b_key_10b_value_1k")
            }
            Metric::storage_has_key_10kib_key_10b_value_1k
            | Metric::storage_read_10kib_key_10b_value_1k
            | Metric::storage_remove_10kib_key_10b_value_1k => {
                f_write(account_idx, "storage_write_10kib_key_10b_value_1k")
            }
            Metric::storage_has_key_10b_key_10kib_value_1k
            | Metric::storage_read_10b_key_10kib_value_1k
            | Metric::storage_remove_10b_key_10kib_value_1k
            | Metric::storage_write_10b_key_10kib_value_1k_evict => {
                f_write(account_idx, "storage_write_10b_key_10kib_value_1k")
            }
            _ => {}
        }

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
            account_id,
            &signer,
            vec![function_call],
            CryptoHash::default(),
        )
    };
    measure_transactions(
        metric,
        measurements,
        config,
        Some(testbed.clone()),
        &mut f,
        allow_failures,
    )
}

macro_rules! calls_helper(
    { $($(#[$feature_name:tt])* $el:ident => $method_name:ident),* } => {
    {
        let mut v: Vec<(Metric, &str)> = vec![];
        $(
            $(#[cfg(feature = $feature_name)])*
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
    SirReceipt,
    ActionTransfer,
    ActionCreateAccount,
    ActionDeleteAccount,
    ActionAddFullAccessKey,
    ActionAddFunctionAccessKey1Method,
    ActionAddFunctionAccessKey1000Methods,
    ActionDeleteAccessKey,
    ActionStake,
    ActionDeploySmallest,
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
    ripemd160_10b_10k,
    ripemd160_10kib_10k,
    ecrecover_10k,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_multiexp_1_1k,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_multiexp_10_1k,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_sum_1_1k,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_g1_sum_10_1k,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_pairing_check_1_1k,
    #[cfg(feature = "protocol_feature_alt_bn128")]
    alt_bn128_pairing_check_10_1k,
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

    promise_and_100k,
    promise_and_100k_on_1k_and,
    promise_return_100k,
    data_producer_10b,
    data_producer_100kib,
    data_receipt_base_10b_1000,
    data_receipt_10b_1000,
    data_receipt_100kib_1000,
    cpu_ram_soak_test,
}

#[allow(unused_variables)]
pub fn run(mut config: Config, only_compile: bool) -> RuntimeConfig {
    let mut m = Measurements::new(config.metric);
    if only_compile {
        let (contract_compile_base_cost, contract_per_byte_cost) =
            compute_compile_cost_vm(config.metric, config.vm_kind, true);
        process::exit(0);
    }
    config.block_sizes = vec![100];
    let mut nonces: HashMap<usize, u64> = HashMap::new();

    // Warmup for receipts
    measure_actions(Metric::warmup, &mut m, &config, None, vec![], false, false, &mut nonces);
    // Measure the speed of processing empty receipts.
    measure_actions(Metric::Receipt, &mut m, &config, None, vec![], false, false, &mut nonces);
    // Measure the speed of processing a sir receipt (where sender is receiver).
    measure_actions(Metric::SirReceipt, &mut m, &config, None, vec![], true, false, &mut nonces);

    // Measure the speed of processing simple transfers.
    measure_actions(
        Metric::ActionTransfer,
        &mut m,
        &config,
        None,
        vec![Action::Transfer(TransferAction { deposit: 1 })],
        false,
        false,
        &mut nonces,
    );

    // Measure the speed of creating account.
    let mut f = || {
        let account_idx = rand::thread_rng().gen::<usize>() % config.active_accounts;
        let account_id = get_account_id(account_idx);
        let other_account_id =
            format!("near_{}_{}", account_idx, rand::thread_rng().gen::<usize>());
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id,
            other_account_id,
            &signer,
            vec![
                Action::CreateAccount(CreateAccountAction {}),
                Action::Transfer(TransferAction { deposit: 10u128.pow(26) }),
            ],
            CryptoHash::default(),
        )
    };
    measure_transactions(Metric::ActionCreateAccount, &mut m, &config, None, &mut f, false);

    // Measure the speed of deleting an account.
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
            account_id,
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
        &mut nonces,
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
        &mut nonces,
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
        &mut nonces,
    );

    // Measure the speed of deleting an access key.
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
            account_id,
            &signer,
            vec![Action::DeleteKey(DeleteKeyAction { public_key: signer.public_key.clone() })],
            CryptoHash::default(),
        )
    };
    measure_transactions(Metric::ActionDeleteAccessKey, &mut m, &config, None, &mut f, false);

    // Measure the speed of staking.
    let public_key: PublicKey = "22skMptHjFWNyuEWY22ftn2AbLPSYpmYwGJRGwpNHbTV".parse().unwrap();
    measure_actions(
        Metric::ActionStake,
        &mut m,
        &config,
        None,
        vec![Action::Stake(StakeAction { stake: 1, public_key })],
        true,
        true,
        &mut nonces,
    );

    // Measure the speed of deploying some code.

    let curr_code = RefCell::new(SMALLEST_CODE.to_vec());
    let mut accounts_deployed = HashSet::new();
    let mut good_code_accounts = HashSet::new();
    let good_account = RefCell::new(false);
    let mut f = || {
        let account_idx = loop {
            let x = rand::thread_rng().gen::<usize>() % config.active_accounts;
            if accounts_deployed.contains(&x) {
                continue;
            }
            break x;
        };
        accounts_deployed.insert(account_idx);
        if *good_account.borrow() {
            good_code_accounts.insert(account_idx);
        }
        let account_id = get_account_id(account_idx);
        let signer = InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id);
        let nonce = *nonces.entry(account_idx).and_modify(|x| *x += 1).or_insert(1);
        SignedTransaction::from_actions(
            nonce as u64,
            account_id.clone(),
            account_id,
            &signer,
            vec![Action::DeployContract(DeployContractAction { code: curr_code.borrow().clone() })],
            CryptoHash::default(),
        )
    };
    let mut testbed =
        measure_transactions(Metric::ActionDeploySmallest, &mut m, &config, None, &mut f, false);

    *good_account.borrow_mut() = true;
    *curr_code.borrow_mut() = CODE_10K.to_vec();

    testbed = measure_transactions(
        Metric::ActionDeploy10K,
        &mut m,
        &config,
        Some(testbed),
        &mut f,
        false,
    );

    // Deploying more small code accounts. It's important that they are the same size to correctly
    // deduct base
    for _ in 0..2 {
        testbed =
            measure_transactions(Metric::warmup, &mut m, &config, Some(testbed), &mut f, false);
    }

    *good_account.borrow_mut() = false;
    *curr_code.borrow_mut() = CODE_100K.to_vec();
    testbed = measure_transactions(
        Metric::ActionDeploy100K,
        &mut m,
        &config,
        Some(testbed),
        &mut f,
        false,
    );
    *curr_code.borrow_mut() = CODE_1M.to_vec();
    testbed =
        measure_transactions(Metric::ActionDeploy1M, &mut m, &config, Some(testbed), &mut f, false);

    let ad: Vec<_> = good_code_accounts.into_iter().collect();

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

    // When adding new functions do not forget to rebuild the test contract by running `test-contract/build.sh`.
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
        ripemd160_10b_10k => ripemd160_10b_10k,
        ripemd160_10kib_10k => ripemd160_10kib_10k,
        ecrecover_10k => ecrecover_10k,
        #["protocol_feature_alt_bn128"] alt_bn128_g1_multiexp_1_1k => alt_bn128_g1_multiexp_1_1k,
        #["protocol_feature_alt_bn128"] alt_bn128_g1_multiexp_10_1k => alt_bn128_g1_multiexp_10_1k,
        #["protocol_feature_alt_bn128"] alt_bn128_g1_sum_1_1k => alt_bn128_g1_sum_1_1k,
        #["protocol_feature_alt_bn128"] alt_bn128_g1_sum_10_1k => alt_bn128_g1_sum_10_1k,
        #["protocol_feature_alt_bn128"] alt_bn128_pairing_check_1_1k => alt_bn128_pairing_check_1_1k,
        #["protocol_feature_alt_bn128"] alt_bn128_pairing_check_10_1k => alt_bn128_pairing_check_10_1k,
        storage_write_10b_key_10b_value_1k => storage_write_10b_key_10b_value_1k,
        storage_read_10b_key_10b_value_1k => storage_read_10b_key_10b_value_1k,
        storage_has_key_10b_key_10b_value_1k => storage_has_key_10b_key_10b_value_1k,
        storage_remove_10b_key_10b_value_1k => storage_remove_10b_key_10b_value_1k,
        storage_write_10kib_key_10b_value_1k => storage_write_10kib_key_10b_value_1k,
        storage_read_10kib_key_10b_value_1k => storage_read_10kib_key_10b_value_1k,
        storage_has_key_10kib_key_10b_value_1k => storage_has_key_10kib_key_10b_value_1k,
        storage_remove_10kib_key_10b_value_1k => storage_remove_10kib_key_10b_value_1k,
        storage_write_10b_key_10kib_value_1k => storage_write_10b_key_10kib_value_1k,
        storage_write_10b_key_10kib_value_1k_evict => storage_write_10b_key_10kib_value_1k,
        storage_read_10b_key_10kib_value_1k => storage_read_10b_key_10kib_value_1k,
        storage_has_key_10b_key_10kib_value_1k => storage_has_key_10b_key_10kib_value_1k,
        storage_remove_10b_key_10kib_value_1k =>   storage_remove_10b_key_10kib_value_1k ,
        promise_and_100k => promise_and_100k,
        promise_and_100k_on_1k_and => promise_and_100k_on_1k_and,
        promise_return_100k => promise_return_100k,
        data_producer_10b => data_producer_10b,
        data_producer_100kib => data_producer_100kib,
        data_receipt_base_10b_1000 => data_receipt_base_10b_1000,
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

    get_runtime_config(&m, &config)

    //    let mut csv_path = PathBuf::from(&config.state_dump_path);
    //    csv_path.push("./metrics.csv");
    //    m.save_to_csv(csv_path.as_path());
    //
    //    m.plot(PathBuf::from(&config.state_dump_path).as_path());
}

fn ratio_to_gas(gas_metric: GasMetric, value: Ratio<u64>) -> u64 {
    let divisor = match gas_metric {
        // We use factor of 8 to approximately match the price of SHA256 operation between
        // time-based and icount-based metric as measured on 3.2Ghz Core i5.
        GasMetric::ICount => 8u128,
        GasMetric::Time => 1u128,
    };
    u64::try_from(
        Ratio::<u128>::new(
            (*value.numer() as u128) * GAS_IN_MEASURE_UNIT,
            (*value.denom() as u128) * divisor,
        )
        .to_integer(),
    )
    .unwrap()
}

pub(crate) fn ratio_to_gas_signed(gas_metric: GasMetric, value: Ratio<i128>) -> i64 {
    let divisor = match gas_metric {
        // We use factor of 8 to approximately match the price of SHA256 operation between
        // time-based and icount-based metric as measured on 3.2Ghz Core i5.
        GasMetric::ICount => 8i128,
        GasMetric::Time => 1i128,
    };
    i64::try_from(
        Ratio::<i128>::new(
            (*value.numer() as i128) * (GAS_IN_MEASURE_UNIT as i128),
            (*value.denom() as i128) * divisor,
        )
        .to_integer(),
    )
    .unwrap()
}

/// Converts cost of a certain action to a fee, spliting it evenly between send and execution fee.
fn measured_to_fee(gas_metric: GasMetric, value: Ratio<u64>) -> Fee {
    let value = ratio_to_gas(gas_metric, value);
    Fee { send_sir: value / 2, send_not_sir: value / 2, execution: value / 2 }
}

fn measured_to_gas(
    gas_metric: GasMetric,
    measured: &BTreeMap<ExtCosts, Ratio<u64>>,
    cost: ExtCosts,
) -> u64 {
    match measured.get(&cost) {
        Some(value) => ratio_to_gas(gas_metric, *value),
        None => panic!("cost {} not found", cost as u32),
    }
}

fn get_runtime_fees_config(measurement: &Measurements) -> RuntimeFeesConfig {
    use crate::runtime_fees_generator::ReceiptFees::*;
    let generator = RuntimeFeesGenerator::new(measurement);
    let measured = generator.compute();
    let metric = measurement.gas_metric;
    let function_call_total_cost = ratio_to_gas(metric, measured[&ActionFunctionCallBase]);
    let function_call_cost = Fee {
        send_sir: function_call_total_cost / 2,
        send_not_sir: function_call_total_cost / 2,
        execution: function_call_total_cost / 2,
    };
    RuntimeFeesConfig {
        action_receipt_creation_config: measured_to_fee(metric, measured[&ActionReceiptCreation]),
        data_receipt_creation_config: DataReceiptCreationConfig {
            base_cost: measured_to_fee(metric, measured[&DataReceiptCreationBase]),
            cost_per_byte: measured_to_fee(metric, measured[&DataReceiptCreationPerByte]),
        },
        action_creation_config: ActionCreationConfig {
            create_account_cost: measured_to_fee(metric, measured[&ActionCreateAccount]),
            deploy_contract_cost: measured_to_fee(metric, measured[&ActionDeployContractBase]),
            deploy_contract_cost_per_byte: measured_to_fee(
                metric,
                measured[&ActionDeployContractPerByte],
            ),
            function_call_cost,
            function_call_cost_per_byte: measured_to_fee(
                metric,
                measured[&ActionFunctionCallPerByte],
            ),
            transfer_cost: measured_to_fee(metric, measured[&ActionTransfer]),
            stake_cost: measured_to_fee(metric, measured[&ActionStake]),
            add_key_cost: AccessKeyCreationConfig {
                full_access_cost: measured_to_fee(metric, measured[&ActionAddFullAccessKey]),
                function_call_cost: measured_to_fee(
                    metric,
                    measured[&ActionAddFunctionAccessKeyBase],
                ),
                function_call_cost_per_byte: measured_to_fee(
                    metric,
                    measured[&ActionAddFunctionAccessKeyPerByte],
                ),
            },
            delete_key_cost: measured_to_fee(metric, measured[&ActionDeleteKey]),
            delete_account_cost: measured_to_fee(metric, measured[&ActionDeleteAccount]),
        },
        ..Default::default()
    }
}

fn get_ext_costs_config(measurement: &Measurements, config: &Config) -> ExtCostsConfig {
    let mut generator = ExtCostsGenerator::new(measurement);
    let measured = generator.compute();
    let metric = measurement.gas_metric;
    use ExtCosts::*;
    let (contract_compile_bytes_, contract_compile_base_) =
        compute_compile_cost_vm(config.metric, config.vm_kind, false);
    ExtCostsConfig {
        base: measured_to_gas(metric, &measured, base),
        contract_compile_base: contract_compile_base_,
        contract_compile_bytes: contract_compile_bytes_,
        read_memory_base: measured_to_gas(metric, &measured, read_memory_base),
        read_memory_byte: measured_to_gas(metric, &measured, read_memory_byte),
        write_memory_base: measured_to_gas(metric, &measured, write_memory_base),
        write_memory_byte: measured_to_gas(metric, &measured, write_memory_byte),
        read_register_base: measured_to_gas(metric, &measured, read_register_base),
        read_register_byte: measured_to_gas(metric, &measured, read_register_byte),
        write_register_base: measured_to_gas(metric, &measured, write_register_base),
        write_register_byte: measured_to_gas(metric, &measured, write_register_byte),
        utf8_decoding_base: measured_to_gas(metric, &measured, utf8_decoding_base),
        utf8_decoding_byte: measured_to_gas(metric, &measured, utf8_decoding_byte),
        utf16_decoding_base: measured_to_gas(metric, &measured, utf16_decoding_base),
        utf16_decoding_byte: measured_to_gas(metric, &measured, utf16_decoding_byte),
        sha256_base: measured_to_gas(metric, &measured, sha256_base),
        sha256_byte: measured_to_gas(metric, &measured, sha256_byte),
        keccak256_base: measured_to_gas(metric, &measured, keccak256_base),
        keccak256_byte: measured_to_gas(metric, &measured, keccak256_byte),
        keccak512_base: measured_to_gas(metric, &measured, keccak512_base),
        keccak512_byte: measured_to_gas(metric, &measured, keccak512_byte),
        ripemd160_base: measured_to_gas(metric, &measured, ripemd160_base),
        ripemd160_block: measured_to_gas(metric, &measured, ripemd160_block),
        ecrecover_base: measured_to_gas(metric, &measured, ecrecover_base),
        log_base: measured_to_gas(metric, &measured, log_base),
        log_byte: measured_to_gas(metric, &measured, log_byte),
        storage_write_base: measured_to_gas(metric, &measured, storage_write_base),
        storage_write_key_byte: measured_to_gas(metric, &measured, storage_write_key_byte),
        storage_write_value_byte: measured_to_gas(metric, &measured, storage_write_value_byte),
        storage_write_evicted_byte: measured_to_gas(metric, &measured, storage_write_evicted_byte),
        storage_read_base: measured_to_gas(metric, &measured, storage_read_base),
        storage_read_key_byte: measured_to_gas(metric, &measured, storage_read_key_byte),
        storage_read_value_byte: measured_to_gas(metric, &measured, storage_read_value_byte),
        storage_remove_base: measured_to_gas(metric, &measured, storage_remove_base),
        storage_remove_key_byte: measured_to_gas(metric, &measured, storage_remove_key_byte),
        storage_remove_ret_value_byte: measured_to_gas(
            metric,
            &measured,
            storage_remove_ret_value_byte,
        ),
        storage_has_key_base: measured_to_gas(metric, &measured, storage_has_key_base),
        storage_has_key_byte: measured_to_gas(metric, &measured, storage_has_key_byte),
        // TODO: storage_iter_* operations below are deprecated, so just hardcode zero price,
        // and remove those operations ASAP.
        storage_iter_create_prefix_base: 0,
        storage_iter_create_prefix_byte: 0,
        storage_iter_create_range_base: 0,
        storage_iter_create_from_byte: 0,
        storage_iter_create_to_byte: 0,
        storage_iter_next_base: 0,
        storage_iter_next_key_byte: 0,
        storage_iter_next_value_byte: 0,
        // TODO: Actually compute it once our storage is complete.
        // TODO: temporary value, as suggested by @nearmax, divisor is log_16(20000) ~ 3.57 ~ 7/2.
        touching_trie_node: measured_to_gas(metric, &measured, storage_read_base) * 2 / 7,
        promise_and_base: measured_to_gas(metric, &measured, promise_and_base),
        promise_and_per_promise: measured_to_gas(metric, &measured, promise_and_per_promise),
        promise_return: measured_to_gas(metric, &measured, promise_return),
        // TODO: accurately price host functions that expose validator information.
        validator_stake_base: 303944908800,
        validator_total_stake_base: 303944908800,
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_sum_base: measured_to_gas(metric, &measured, alt_bn128_g1_sum_base),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_sum_byte: measured_to_gas(metric, &measured, alt_bn128_g1_sum_byte),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_multiexp_base: measured_to_gas(metric, &measured, alt_bn128_g1_multiexp_base),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_multiexp_byte: measured_to_gas(metric, &measured, alt_bn128_g1_multiexp_byte),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_g1_multiexp_sublinear: measured_to_gas(
            metric,
            &measured,
            alt_bn128_g1_multiexp_sublinear,
        ),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_pairing_check_base: measured_to_gas(
            metric,
            &measured,
            alt_bn128_pairing_check_base,
        ),
        #[cfg(feature = "protocol_feature_alt_bn128")]
        alt_bn128_pairing_check_byte: measured_to_gas(
            metric,
            &measured,
            alt_bn128_pairing_check_byte,
        ),
    }
}

fn get_vm_config(measurement: &Measurements, config: &Config) -> VMConfig {
    VMConfig {
        ext_costs: get_ext_costs_config(measurement, config),
        // TODO: Figure out whether we need this fee at all. If we do what should be the memory
        // growth cost.
        grow_mem_cost: 1,
        regular_op_cost: ratio_to_gas(
            measurement.gas_metric,
            cost_per_op(measurement.gas_metric, &CODE_1M),
        ) as u32,
        limit_config: VMLimitConfig::default(),
    }
}

fn get_runtime_config(measurement: &Measurements, config: &Config) -> RuntimeConfig {
    let mut runtime_config = RuntimeConfig::default();
    runtime_config.wasm_config = get_vm_config(measurement, config);

    // Compiling small test contract that was used for `noop` function call estimation.
    load_and_compile(
        &"./test-contract/res/stable_small_contract.wasm".into(),
        config.metric,
        config.vm_kind,
    )
    .unwrap();

    runtime_config.transaction_costs = get_runtime_fees_config(measurement);

    // Shifting compilation costs from function call runtime to the deploy action cost at execution
    // time. Contract used in deploy action testing is very small, so we have to use more complex
    // technique to compute the actual coefficients.
    runtime_config.transaction_costs.action_creation_config.deploy_contract_cost.execution +=
        runtime_config.wasm_config.ext_costs.contract_compile_base;
    runtime_config
        .transaction_costs
        .action_creation_config
        .deploy_contract_cost_per_byte
        .execution += runtime_config.wasm_config.ext_costs.contract_compile_bytes;
    runtime_config.wasm_config.ext_costs.contract_compile_base = 0;
    runtime_config.wasm_config.ext_costs.contract_compile_bytes = 0;

    runtime_config
}
