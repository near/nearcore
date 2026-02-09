//! Integrations tests for deterministic accounts.
//!
//! Deterministic account ids were introduced to enabled sharded contracts, as
//! defined in [NEP-616](https://github.com/near/NEPs/pull/616). This test
//! module aims to test all new features introduced alongside with deterministic
//! account ids.
//!
//! The main feature is the new action `DeterministicStateInit` and the directly
//! related host functions.
//!
//! - `promise_batch_action_state_init`
//! - `promise_batch_action_state_init_by_account_id`
//! - `set_state_init_data_entry`
//!
//! Additionally, there are indirectly related host functions.
//!
//! - `promise_set_refund_to`
//! - `refund_to_account_id`
//! - `promise_result_length`
//! - `current_contract_code`
//! - `storage_config_byte_cost`
//! - `storage_config_num_bytes_account`
//! - `storage_config_num_extra_bytes_record`

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use crate::utils::node::TestLoopNode;
use crate::utils::transactions;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_o11y::testonly::init_test_logger;
use near_parameters::RuntimeConfigStore;
use near_primitives::action::{
    DeterministicStateInitAction, GlobalContractDeployMode, GlobalContractIdentifier,
};
use near_primitives::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives::errors::{
    ActionError, ActionErrorKind, ActionsValidationError, CompilationError, FunctionCallError,
    InvalidTxError, TxExecutionError,
};
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::Action;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::Gas;
use near_primitives::types::{AccountId, Balance};
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::version::ProtocolVersion;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{AccountView, QueryRequest, QueryResponse, QueryResponseKind};
use near_primitives::views::{FinalExecutionOutcomeView, FinalExecutionStatus};
use near_vm_runner::ContractCode;
use std::collections::BTreeMap;

const GAS_PRICE: Balance = Balance::from_yoctonear(1);

#[test]
fn test_deterministic_state_init_by_id_no_data() {
    check_deterministic_state_init(GlobalContractDeployMode::AccountId, empty(), Balance::ZERO);
}

#[test]
fn test_deterministic_state_init_by_hash_no_data() {
    check_deterministic_state_init(GlobalContractDeployMode::CodeHash, empty(), Balance::ZERO);
}

#[test]
fn test_deterministic_state_init_by_id_zba() {
    check_deterministic_state_init(GlobalContractDeployMode::AccountId, small(), Balance::ZERO);
}

#[test]
fn test_deterministic_state_init_by_hash_zba() {
    check_deterministic_state_init(GlobalContractDeployMode::CodeHash, small(), Balance::ZERO);
}

#[test]
fn test_deterministic_state_init_by_id_above_zba() {
    // This is above zero-balance and requires a balance. Using the exact
    // require balance here to ensure no accidental changes. If your change
    // causes this test to fail, this probably means you are changing how much
    // state an empty account uses. Updating the number below to reflect that
    // can be okay. But consider that existing contracts might rely on the
    // current ZBA limit.
    // To update, run with Balance::ZERO and copy the number from the error.
    let balance = Balance::from_yoctonear(1_001_500_000_000_000_000_000_000);
    check_deterministic_state_init(GlobalContractDeployMode::AccountId, big(), balance);
}

#[test]
fn test_deterministic_state_init_by_hash_above_zba() {
    // This is above zero-balance and requires a balance. Using the exact
    // require balance here to ensure no accidental changes. If your change
    // causes this test to fail, this probably means you are changing how much
    // state an empty account uses. Updating the number below to reflect that
    // can be okay. But consider that existing contracts might rely on the
    // current ZBA limit.
    // To update, run with Balance::ZERO and copy the number from the error.
    let balance = Balance::from_yoctonear(1_001_750_000_000_000_000_000_000);
    check_deterministic_state_init(GlobalContractDeployMode::CodeHash, big(), balance);
}

/// Create an account with deterministic ID and call a function on it.
fn check_deterministic_state_init(
    global_deploy_mode: GlobalContractDeployMode,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    balance: Balance,
) {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));

    env.deploy_global_contract(global_deploy_mode.clone());

    let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
        code: env.global_contract_identifier(&global_deploy_mode),
        data,
    });
    let det_account = derive_near_deterministic_account_id(&state_init);
    let user_signer = create_user_test_signer(&env.user_account());
    let create_deterministic_account_tx =
        env.deterministic_account_state_init_tx(state_init, &det_account, user_signer, balance);
    env.run_tx(create_deterministic_account_tx);

    env.assert_test_contract_usable_on_account(det_account);

    env.shutdown();
}

/// Ensure repeating a state initialization does not fail, to allow lazy
/// initialization. The submitted deposit should be refunded.
///
/// This test also checks that the signer is charged the balance correctly.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_repeated_deterministic_state_init() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let data = BTreeMap::from_iter([(b"key".to_vec(), vec![0u8; 100_000])]);
    let (state_init, det_account) = env.new_deterministic_account_with_data(data.clone());

    // send 10 times the required amount
    let required_for_storage = env.balance_for_storage(state_init);
    let attached_balance = required_for_storage.checked_mul(10).unwrap();

    let deposit_before = env.get_account_state(env.user_account()).amount;

    // first init
    let outcome = env.try_deploy_deterministic_account_with_data(data.clone(), attached_balance);
    outcome.expect("should be able to send transaction").assert_success();
    assert_eq!(
        required_for_storage,
        env.get_account_state(det_account.clone()).amount,
        "exactly required balance should be on the created account"
    );
    let deposit_between = env.get_account_state(env.user_account()).amount;
    assert!(
        deposit_between <= deposit_before.checked_sub(required_for_storage).unwrap(),
        "signer should have been charged the deposit cost + gas cost"
    );

    // second init
    let outcome = env.try_deploy_deterministic_account_with_data(data, attached_balance);
    outcome.expect("should be able to send transaction").assert_success();
    assert_eq!(
        required_for_storage,
        env.get_account_state(det_account).amount,
        "exactly attached balance should be on created account, nothing added on the second call"
    );
    let deposit_after = env.get_account_state(env.user_account()).amount;
    assert!(
        deposit_after > deposit_between.checked_sub(required_for_storage).unwrap(),
        "signer should have been refunded the deposit cost and only spend gas cost on the second call"
    );

    env.shutdown();
}

/// Try using non-existing global contract
#[test]
fn test_deterministic_state_init_missing_global_contract() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));

    let data = Default::default();
    let balance = Balance::ZERO;
    let outcome = env
        .try_deploy_deterministic_account_with_data(data, balance)
        .expect("should be able to send transaction");
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::GlobalContractDoesNotExist { .. },
            index: _
        }))
    );

    env.shutdown();
}

/// Try creating an account above ZBA limit without attached balance
#[test]
fn test_deterministic_state_init_above_zba() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let data = big();
    let balance = Balance::ZERO;
    let outcome = env
        .try_deploy_deterministic_account_with_data(data, balance)
        .expect("should be able to send transaction");
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::LackBalanceForState { .. },
            index: _
        }))
    );

    env.shutdown();
}

/// Try creating adding larger-than-allowed KEY to state
#[test]
fn test_deterministic_state_init_key_too_large() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let key = vec![1u8; 2049];
    let data = BTreeMap::from_iter([(key, b"value".to_vec())]);
    let balance = Balance::ZERO;
    let outcome = env.try_deploy_deterministic_account_with_data(data, balance);
    assert_matches!(
        outcome,
        Err(InvalidTxError::ActionsValidation(
            ActionsValidationError::DeterministicStateInitKeyLengthExceeded { .. }
        ))
    );

    env.shutdown();
}

/// Try creating adding larger-than-allowed VALUE to state
#[test]
fn test_deterministic_state_init_value_too_large() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let key = b"key".to_vec();
    let value = vec![1u8; 4_194_305];
    let data = BTreeMap::from_iter([(key, value)]);
    let balance = Balance::from_near(1000);
    let outcome = env.try_deploy_deterministic_account_with_data(data, balance);
    assert_matches!(
        outcome,
        Err(InvalidTxError::ActionsValidation(
            ActionsValidationError::DeterministicStateInitValueLengthExceeded { .. }
        ))
    );

    env.shutdown();
}

/// Try sending the action to an invalid receiver: wrong derived id
#[test]
fn test_deterministic_state_init_invalid_derived_id() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let key = b"key".to_vec();
    let value = vec![1u8; 4_194_305];
    let data = BTreeMap::from_iter([(key, value)]);
    let balance = Balance::from_near(1000);

    let user_signer = create_user_test_signer(&env.user_account());
    let (state_init, _det_account) = env.new_deterministic_account_with_data(data);
    let det_account = "0s1234567890123456789012345678901234567890".parse().unwrap();
    let create_deterministic_account_tx =
        env.deterministic_account_state_init_tx(state_init, &det_account, user_signer, balance);
    let outcome = env.try_execute_tx(create_deterministic_account_tx);

    assert_matches!(
        outcome,
        Err(InvalidTxError::ActionsValidation(
            ActionsValidationError::InvalidDeterministicStateInitReceiver { .. }
        ))
    );

    env.shutdown();
}

/// Try sending the action to an invalid receiver: named account
#[test]
fn test_deterministic_state_init_named_receiver() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let key = b"key".to_vec();
    let value = vec![1u8; 4_194_305];
    let data = BTreeMap::from_iter([(key, value)]);
    let balance = Balance::from_near(1000);

    let user_signer = create_user_test_signer(&env.user_account());
    let (state_init, _det_account) = env.new_deterministic_account_with_data(data);
    let det_account = "named.near".parse().unwrap();
    let create_deterministic_account_tx =
        env.deterministic_account_state_init_tx(state_init, &det_account, user_signer, balance);
    let outcome = env.try_execute_tx(create_deterministic_account_tx);

    assert_matches!(
        outcome,
        Err(InvalidTxError::ActionsValidation(
            ActionsValidationError::InvalidDeterministicStateInitReceiver { .. }
        ))
    );

    env.shutdown();
}

/// Ensure we can pre-pay the balance for a deterministic account.
///
/// It is a required feature that one can send a Transfer to a non-existing
/// deterministic account first and later initialize it without adding balance,
/// even if more storage than the ZBA limit is used.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_deterministic_state_init_prepay_for_storage() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let data = BTreeMap::from_iter([(b"key".to_vec(), vec![0u8; 100_000])]);
    let (state_init, det_account) = env.new_deterministic_account_with_data(data.clone());

    // Try once without pre-paying, must fail.
    let outcome = env
        .try_deploy_deterministic_account_with_data(data.clone(), Balance::ZERO)
        .expect("should be able to send transaction");
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::LackBalanceForState { .. },
            index: _
        }))
    );

    // Prepay
    let required_for_storage = env.balance_for_storage(state_init);
    env.fund_with_near_balance(det_account.clone(), required_for_storage);
    assert_eq!(
        required_for_storage,
        env.get_account_state(det_account.clone()).amount,
        "account should have been created and funded now"
    );

    // Contract can't be called, yet.
    env.assert_test_contract_not_usable_on_account(det_account.clone());

    // Try creating again, with zero balance again. Must succeed this time.
    env.try_deploy_deterministic_account_with_data(data, Balance::ZERO)
        .expect("should be able to send transaction")
        .assert_success();
    env.assert_test_contract_usable_on_account(det_account);

    env.shutdown();
}

/// Test that multi-action receipts fail to create deterministic accounts before
/// `FixDeterministicAccountIdCreation` is enabled.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_deterministic_state_init_multi_action_before_fix() {
    let version_before_fix =
        ProtocolFeature::FixDeterministicAccountIdCreation.protocol_version() - 1;
    assert!(
        ProtocolFeature::DeterministicAccountIds.enabled(version_before_fix),
        "DeterministicAccountIds must be enabled before FixDeterministicAccountIdCreation"
    );

    let mut env = TestEnv::setup_with_protocol_version(Balance::from_near(100), version_before_fix);
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let tx = env.multi_action_deterministic_account_tx(Balance::from_near(5));
    let outcome = env.try_execute_tx(tx).expect("tx should be submitted");

    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::AccountDoesNotExist { .. },
            ..
        }))
    );

    env.shutdown();
}

/// Test that multi-action receipts can create deterministic accounts after
/// `FixDeterministicAccountIdCreation` is enabled.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_deterministic_state_init_multi_action_after_fix() {
    let version_with_fix = ProtocolFeature::FixDeterministicAccountIdCreation.protocol_version();
    let mut env = TestEnv::setup_with_protocol_version(Balance::from_near(100), version_with_fix);
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let balance = Balance::from_near(5);
    let tx = env.multi_action_deterministic_account_tx(balance);
    let det_account = tx.transaction.receiver_id().clone();

    env.try_execute_tx(tx).expect("tx should be submitted").assert_success();

    assert!(env.get_account_state(det_account.clone()).amount >= balance);
    env.assert_test_contract_usable_on_account(det_account);

    env.shutdown();
}

/// Deploy a sharded toy-contract and check it can do a "predecessor is owner"
/// check as intended by NEP-616.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sharded_contract_owner_check() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let data = BTreeMap::from_iter([(b"key".to_vec(), vec![0u8; 100_000])]);
    let (state_init, det_account) = env.new_deterministic_account_with_data(data.clone());

    // Try once without pre-paying, must fail.
    let outcome = env
        .try_deploy_deterministic_account_with_data(data.clone(), Balance::ZERO)
        .expect("should be able to send transaction");
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::LackBalanceForState { .. },
            index: _
        }))
    );

    // Prepay
    let required_for_storage = env.balance_for_storage(state_init);
    env.fund_with_near_balance(det_account.clone(), required_for_storage);
    assert_eq!(
        required_for_storage,
        env.get_account_state(det_account.clone()).amount,
        "account should have been created and funded now"
    );

    // Contract can't be called, yet.
    env.assert_test_contract_not_usable_on_account(det_account.clone());

    // Try creating again, with zero balance again. Must succeed this time.
    env.try_deploy_deterministic_account_with_data(data, Balance::ZERO)
        .expect("should be able to send transaction")
        .assert_success();
    env.assert_test_contract_usable_on_account(det_account);
    let user_account = env.user_account();
    let sharded_account = env.setup_sharded_account(user_account.clone());

    env.call_sharded_owner_only(&user_account, &sharded_account).assert_success();

    env.shutdown();
}

/// Deploy a sharded toy-contract and check it can do a "predecessor is owner"
/// check as intended by NEP-616.
#[test]
fn test_sharded_contract_owner_check_fails() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    let user_account = env.user_account();
    let sharded_account = env.setup_sharded_account(user_account);

    let foreign_caller_outcome =
        env.call_sharded_owner_only(&env.independent_account(), &sharded_account);
    assert_eq!(
        foreign_caller_outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
                "Smart contract panicked: not root: account0 != account2".to_owned()
            )),
            index: Some(0)
        }))
    );
    env.shutdown();
}

/// Deploy a sharded toy-contract and check it can do a "predecessor has same
/// code as myself" check as intended by NEP-616.
#[test]
fn test_sharded_contract_peer_check() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    let user_account1 = env.user_account();
    let user_account2 = env.independent_account();
    let sharded_account1 = env.setup_sharded_account(user_account1.clone());
    let sharded_account2 = env.setup_sharded_account(user_account2);

    // check calling `peer_only` indirectly through `ping` works
    let outcome = env.call_sharded_contract(
        user_account1,
        sharded_account1,
        "ping",
        sharded_account2.as_bytes().to_vec(),
        Balance::ZERO,
    );
    outcome.assert_success();

    assert_eq!(3, outcome.receipts_outcome.len());
    let ping_call_result = &outcome.receipts_outcome[1];
    assert_eq!(sharded_account2, ping_call_result.outcome.executor_id);
    assert_eq!(vec!["peer ok".to_owned()], ping_call_result.outcome.logs);

    env.shutdown();
}

/// Deploy a sharded toy-contract and check it can do a "predecessor has same
/// code as myself" check as intended by NEP-616.
#[test]
fn test_sharded_contract_peer_check_fails() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    let user_account = env.user_account();
    let sharded_account = env.setup_sharded_account(user_account.clone());

    // check calling `peer_only` directly fails, as the predecessor it not a peer
    let outcome = env.call_sharded_contract(
        user_account,
        sharded_account,
        "peer_only",
        vec![],
        Balance::ZERO,
    );
    assert_eq!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
                "Smart contract panicked: not peer: 0s956d38ada09c708ad55824ba9cb162e4669a63eb != account0".to_owned()
            )),
            index: Some(0)
        }))
    );

    env.shutdown();
}

/// Deploy a sharded toy-contract and check it can spread itself to another account.
#[test]
fn test_sharded_contract_spread() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    let user_account = env.user_account();
    let sharded_account = env.setup_sharded_account(user_account.clone());
    let user_to_onboard = env.independent_account();

    // check the user does't have a sharded contract, yet
    let outcome = env.call_sharded_owner_only(&user_to_onboard, &sharded_account);
    assert_eq!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
                "Smart contract panicked: not root: account0 != account2".to_owned()
            )),
            index: Some(0)
        }))
    );

    // now onboard the user using "spread" on the existing sharded contract instance
    let target_account_id =
        env.new_deterministic_account_with_data(sharded_contract_data(user_to_onboard.clone())).1;

    let outcome = env.call_sharded_contract(
        user_account,
        sharded_account.clone(),
        "spread",
        // pass in id as argument but don't sign anything with that user
        user_to_onboard.as_bytes().to_vec(),
        Balance::ZERO,
    );
    outcome.assert_success();

    // check the onboarded account can use their new sharded contract instance
    env.call_sharded_owner_only(&user_to_onboard, &target_account_id).assert_success();

    env.shutdown();
}

/// Deploy a sharded toy-contract and check it can spread itself to another
/// account and provide funding on the initial call.
#[test]
fn test_sharded_contract_spread_funded() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    let user_account = env.user_account();
    let sharded_account = env.setup_sharded_account(user_account.clone());
    let user_to_onboard = env.independent_account();

    // check the user does't have a sharded contract, yet
    let outcome = env.call_sharded_owner_only(&user_to_onboard, &sharded_account);
    assert_eq!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
                "Smart contract panicked: not root: account0 != account2".to_owned()
            )),
            index: Some(0)
        }))
    );

    // now onboard the user using "spread" on the existing sharded contract instance
    let target_account_id =
        env.new_deterministic_account_with_data(sharded_contract_data(user_to_onboard.clone())).1;

    let provided_balance = Balance::from_near(5);
    let outcome = env.call_sharded_contract(
        user_account,
        sharded_account.clone(),
        "spread",
        // pass in id as argument but don't sign anything with that user
        user_to_onboard.as_bytes().to_vec(),
        // provide funding here, which will use the attached deposit to fund the new account
        provided_balance,
    );
    outcome.assert_success();

    // check the onboarded account can use their new sharded contract instance
    env.call_sharded_owner_only(&user_to_onboard, &target_account_id).assert_success();

    env.shutdown();
}

struct TestEnv {
    env: TestLoopEnv,
    runtime_config_store: RuntimeConfigStore,
    contract: ContractCode,
    global_contract_account: AccountId,
    user_account: AccountId,
    independent_account: AccountId,
    nonce: u64,
}

impl TestEnv {
    fn setup(initial_balance: Balance) -> Self {
        Self::setup_with_protocol_version(initial_balance, PROTOCOL_VERSION)
    }

    fn setup_with_protocol_version(
        initial_balance: Balance,
        protocol_version: ProtocolVersion,
    ) -> Self {
        init_test_logger();

        let [user_account, independent_account, global_contract_account] =
            create_account_ids(["account0", "account2", "account"]);

        let boundary_accounts = create_account_ids(["account1"]).to_vec();
        let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
        let validators_spec = create_validators_spec(2, 2);
        let clients = validators_spec_clients_with_rpc(&validators_spec);

        let genesis = TestLoopBuilder::new_genesis_builder()
            .protocol_version(protocol_version)
            .validators_spec(validators_spec)
            .shard_layout(shard_layout)
            .add_user_accounts_simple(
                &[
                    user_account.clone(),
                    independent_account.clone(),
                    global_contract_account.clone(),
                ],
                initial_balance,
            )
            .gas_prices(GAS_PRICE, GAS_PRICE)
            .build();

        let runtime_config_store = RuntimeConfigStore::new(None);
        let env = TestLoopBuilder::new()
            .genesis(genesis)
            .epoch_config_store_from_genesis()
            .clients(clients)
            .runtime_config_store(runtime_config_store.clone())
            .build()
            .warmup();

        Self {
            env,
            runtime_config_store,
            user_account,
            independent_account,
            global_contract_account,
            contract: ContractCode::new(near_test_contracts::rs_contract().to_vec(), None),
            nonce: 1,
        }
    }

    fn shutdown(self) {
        self.env.shutdown_and_drain_remaining_events(Duration::seconds(10));
    }

    fn global_contract_account(&self) -> AccountId {
        self.global_contract_account.clone()
    }

    fn user_account(&self) -> AccountId {
        self.user_account.clone()
    }

    fn independent_account(&self) -> AccountId {
        self.independent_account.clone()
    }

    fn deploy_global_contract_custom_tx(
        &mut self,
        deploy_mode: GlobalContractDeployMode,
        contract_code: Vec<u8>,
    ) -> SignedTransaction {
        SignedTransaction::deploy_global_contract(
            self.next_nonce(),
            self.global_contract_account.clone(),
            contract_code,
            &create_user_test_signer(&self.global_contract_account),
            self.get_tx_block_hash(),
            deploy_mode,
        )
    }

    fn deploy_global_contract_tx(
        &mut self,
        deploy_mode: GlobalContractDeployMode,
    ) -> SignedTransaction {
        self.deploy_global_contract_custom_tx(deploy_mode, self.contract.code().to_vec())
    }

    fn deploy_global_contract(&mut self, deploy_mode: GlobalContractDeployMode) {
        let tx = self.deploy_global_contract_tx(deploy_mode);
        self.run_tx(tx);
    }

    /// Assumes to use global_contract_account by account id as code.
    fn new_deterministic_account_with_data(
        &self,
        data: BTreeMap<Vec<u8>, Vec<u8>>,
    ) -> (DeterministicAccountStateInit, AccountId) {
        let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
            code: GlobalContractIdentifier::AccountId(self.global_contract_account()),
            data,
        });
        let det_account = derive_near_deterministic_account_id(&state_init);
        (state_init, det_account)
    }

    fn deterministic_account_state_init_tx(
        &mut self,
        state_init: DeterministicAccountStateInit,
        det_account: &AccountId,
        user_signer: near_crypto::Signer,
        balance: Balance,
    ) -> SignedTransaction {
        let create_deterministic_account_tx = SignedTransaction::deterministic_state_init(
            self.next_nonce(),
            user_signer.get_account_id(),
            det_account.clone(),
            &user_signer,
            self.get_tx_block_hash(),
            state_init,
            balance,
        );
        create_deterministic_account_tx
    }

    fn try_deploy_deterministic_account_with_data(
        &mut self,
        data: BTreeMap<Vec<u8>, Vec<u8>>,
        balance: Balance,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        let user_signer = create_user_test_signer(&self.user_account());
        let (state_init, det_account) = self.new_deterministic_account_with_data(data);
        let create_deterministic_account_tx = self.deterministic_account_state_init_tx(
            state_init,
            &det_account,
            user_signer,
            balance,
        );
        self.try_execute_tx(create_deterministic_account_tx)
    }

    /// Creates a multi-action transaction: Transfer + DeterministicStateInit + FunctionCall.
    fn multi_action_deterministic_account_tx(&mut self, balance: Balance) -> SignedTransaction {
        let (state_init, det_account) = self.new_deterministic_account_with_data(small());
        let signer = create_user_test_signer(&self.user_account());
        let actions = vec![
            Action::Transfer(near_primitives::transaction::TransferAction { deposit: balance }),
            Action::DeterministicStateInit(Box::new(DeterministicStateInitAction {
                state_init,
                deposit: Balance::ZERO,
            })),
            Action::FunctionCall(Box::new(near_primitives::action::FunctionCallAction {
                method_name: "log_something".to_owned(),
                args: vec![],
                gas: Gas::from_teragas(50),
                deposit: Balance::ZERO,
            })),
        ];
        SignedTransaction::from_actions(
            self.next_nonce(),
            self.user_account(),
            det_account,
            &signer,
            actions,
            self.get_tx_block_hash(),
        )
    }

    /// Creates, on-chain, a deterministic account id owned by `user`.
    fn setup_sharded_account(&mut self, user: AccountId) -> AccountId {
        let tx = self.deploy_global_contract_custom_tx(
            GlobalContractDeployMode::AccountId,
            near_test_contracts::sharded_contract_test_contract().to_vec(),
        );
        self.run_tx(tx);

        let initial_balance = Balance::from_near(5);
        self.create_sharded_contract_user(user, initial_balance)
    }

    fn create_sharded_contract_user(&mut self, owner: AccountId, balance: Balance) -> AccountId {
        let signer = create_user_test_signer(&owner);
        let data = sharded_contract_data(owner);
        let (state_init, det_account) = self.new_deterministic_account_with_data(data);

        let tx = SignedTransaction::deterministic_state_init(
            self.next_nonce(),
            signer.get_account_id(),
            det_account.clone(),
            &signer,
            self.get_tx_block_hash(),
            state_init,
            balance,
        );
        self.run_tx(tx);
        det_account
    }

    fn call_sharded_owner_only(
        &mut self,
        user: &AccountId,
        sharded_account: &AccountId,
    ) -> FinalExecutionOutcomeView {
        self.call_sharded_contract(
            user.clone(),
            sharded_account.clone(),
            "owner_only",
            vec![],
            Balance::ZERO,
        )
    }

    fn call_test_contract_tx(
        &mut self,
        signer_id: AccountId,
        receiver_id: AccountId,
    ) -> SignedTransaction {
        let signer = create_user_test_signer(&signer_id);
        SignedTransaction::call(
            self.next_nonce(),
            signer_id,
            receiver_id,
            &signer,
            Balance::ZERO,
            "log_something".to_owned(),
            vec![],
            Gas::from_teragas(300),
            self.get_tx_block_hash(),
        )
    }

    fn fund_with_near_balance(&mut self, receiver_id: AccountId, amount: Balance) {
        let signer_id = self.independent_account();
        let signer = create_user_test_signer(&signer_id);
        let tx = SignedTransaction::send_money(
            self.next_nonce(),
            signer_id,
            receiver_id,
            &signer,
            amount,
            self.get_tx_block_hash(),
        );
        self.execute_tx(tx).assert_success();
    }

    fn call_sharded_contract(
        &mut self,
        signer_id: AccountId,
        receiver_id: AccountId,
        method: &str,
        arg: Vec<u8>,
        balance: Balance,
    ) -> FinalExecutionOutcomeView {
        let signer = create_user_test_signer(&signer_id);
        let tx = SignedTransaction::call(
            self.next_nonce(),
            signer_id,
            receiver_id,
            &signer,
            balance,
            method.to_owned(),
            arg,
            Gas::from_teragas(300),
            self.get_tx_block_hash(),
        );
        self.try_execute_tx(tx).unwrap()
    }

    fn assert_test_contract_usable_on_account(&mut self, account_with_contract: AccountId) {
        let tx = self.call_test_contract_tx(self.independent_account(), account_with_contract);
        self.run_tx(tx);
    }

    fn assert_test_contract_not_usable_on_account(&mut self, account_with_contract: AccountId) {
        let tx = self.call_test_contract_tx(self.independent_account(), account_with_contract);
        let outcome = self.execute_tx(tx);

        assert_matches!(
            outcome.status,
            FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::CompilationError(
                    CompilationError::CodeDoesNotExist { .. }
                )),
                index: _
            }))
        );
    }

    fn try_execute_tx(
        &mut self,
        tx: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        TestLoopNode::rpc(&self.env.node_datas).execute_tx(
            &mut self.env.test_loop,
            tx,
            Duration::seconds(5),
        )
    }

    fn balance_for_storage(&self, state_init: DeterministicAccountStateInit) -> Balance {
        let runtime_config = self.runtime_config_store.get_config(PROTOCOL_VERSION);
        let storage_config = &runtime_config.fees.storage_usage_config;
        let num_records = state_init.data().len() as u64;

        let num_bytes = state_init.len_bytes() as u64
            + storage_config.num_bytes_account
            + num_records * storage_config.num_extra_bytes_record
            + state_init.code().len() as u64;

        storage_config.storage_amount_per_byte.checked_mul(num_bytes as u128).unwrap()
    }

    fn next_nonce(&mut self) -> u64 {
        let ret = self.nonce;
        self.nonce += 1;
        ret
    }

    fn get_tx_block_hash(&self) -> CryptoHash {
        transactions::get_shared_block_hash(&self.env.node_datas, &self.env.test_loop.data)
    }

    #[track_caller]
    fn run_tx(&mut self, tx: SignedTransaction) {
        TestLoopNode::rpc(&self.env.node_datas).run_tx(
            &mut self.env.test_loop,
            tx,
            Duration::seconds(5),
        );
    }

    #[track_caller]
    fn execute_tx(&mut self, tx: SignedTransaction) -> FinalExecutionOutcomeView {
        TestLoopNode::rpc(&self.env.node_datas)
            .execute_tx(&mut self.env.test_loop, tx, Duration::seconds(5))
            .unwrap()
    }

    fn global_contract_identifier(
        &self,
        deploy_mode: &GlobalContractDeployMode,
    ) -> GlobalContractIdentifier {
        match deploy_mode {
            GlobalContractDeployMode::CodeHash => {
                GlobalContractIdentifier::CodeHash(*self.contract.hash())
            }
            GlobalContractDeployMode::AccountId => {
                GlobalContractIdentifier::AccountId(self.global_contract_account.clone())
            }
        }
    }

    fn runtime_query(&self, query: QueryRequest) -> QueryResponse {
        TestLoopNode::rpc(&self.env.node_datas).runtime_query(self.env.test_loop_data(), query)
    }

    fn get_account_state(&mut self, account: AccountId) -> AccountView {
        // Need to wait a bit for RPC node to catch up with the results
        // of previously submitted txs
        self.env.test_loop.run_for(Duration::seconds(2));
        self.view_account(&account)
    }

    fn view_account(&self, account: &AccountId) -> AccountView {
        let response =
            self.runtime_query(QueryRequest::ViewAccount { account_id: account.clone() });
        let QueryResponseKind::ViewAccount(account_view) = response.kind else { unreachable!() };
        account_view
    }
}

fn empty() -> BTreeMap<Vec<u8>, Vec<u8>> {
    BTreeMap::new()
}

fn small() -> BTreeMap<Vec<u8>, Vec<u8>> {
    BTreeMap::from_iter([(b"key".to_vec(), b"value".to_vec())])
}

fn big() -> BTreeMap<Vec<u8>, Vec<u8>> {
    BTreeMap::from_iter([(b"key".to_vec(), vec![0u8; 100_000])])
}

fn sharded_contract_data(owner: AccountId) -> BTreeMap<Vec<u8>, Vec<u8>> {
    BTreeMap::from_iter([(b"root".to_vec(), owner.as_bytes().to_vec())])
}
