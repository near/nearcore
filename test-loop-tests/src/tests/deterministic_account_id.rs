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
//! => TODO(jakmeier): Add tests covering these
//!
//! - `promise_set_refund_to`
//! - `refund_to_account_id`
//! - `promise_result_length`
//! - `current_contract_code`
//! - `storage_config_byte_cost`
//! - `storage_config_num_bytes_account`
//! - `storage_config_num_extra_bytes_record`

use crate::tests::global_contracts::GlobalContractsTestEnv;
use crate::utils::account::rpc_account_id;
use crate::utils::node::TestLoopNode;
use assert_matches::assert_matches;
use near_async::time::Duration;
use near_primitives::action::{GlobalContractDeployMode, GlobalContractIdentifier};
use near_primitives::deterministic_account_id::{
    DeterministicAccountStateInit, DeterministicAccountStateInitV1,
};
use near_primitives::errors::{
    ActionError, ActionErrorKind, ActionsValidationError, InvalidTxError, TxExecutionError,
};
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::Gas;
use near_primitives::types::{AccountId, Balance};
use near_primitives::utils::derive_near_deterministic_account_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_primitives::views::{FinalExecutionOutcomeView, FinalExecutionStatus};
use std::collections::BTreeMap;

#[test]
fn test_deterministic_state_init() {
    let empty = || BTreeMap::new();
    let small = || BTreeMap::from_iter([(b"key".to_vec(), b"value".to_vec())]);
    let big = || BTreeMap::from_iter([(b"key".to_vec(), vec![0u8; 100_000])]);

    // These are zero-balance accounts and don't need a balance
    check_deterministic_state_init(GlobalContractDeployMode::AccountId, empty(), Balance::ZERO);
    check_deterministic_state_init(GlobalContractDeployMode::CodeHash, empty(), Balance::ZERO);
    check_deterministic_state_init(GlobalContractDeployMode::AccountId, small(), Balance::ZERO);
    check_deterministic_state_init(GlobalContractDeployMode::CodeHash, small(), Balance::ZERO);

    // These are above zero-balance and require a balance. Using the exact
    // require balance here to ensure no accidental changes. If your change
    // causes this test to fail, this probably means you are changing how much
    // state an empty account uses. Updating the numbers below to reflect that
    // can be okay. But consider that existing contracts might rely on the
    // current ZBA limit.
    // To update, run with Balance::ZERO and copy the number from the error.
    let balance = Balance::from_yoctonear(1_001_500_000_000_000_000_000_000);
    check_deterministic_state_init(GlobalContractDeployMode::AccountId, big(), balance);
    let balance = Balance::from_yoctonear(1_001_750_000_000_000_000_000_000);
    check_deterministic_state_init(GlobalContractDeployMode::CodeHash, big(), balance);
}

/// Create an account with deterministic ID and call a function on it.
#[track_caller]
fn check_deterministic_state_init(
    global_deploy_mode: GlobalContractDeployMode,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    balance: Balance,
) {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));

    env.parent.deploy_global_contract(global_deploy_mode.clone());

    let state_init = DeterministicAccountStateInit::V1(DeterministicAccountStateInitV1 {
        code: env.parent.global_contract_identifier(&global_deploy_mode),
        data,
    });
    let det_account = derive_near_deterministic_account_id(&state_init);
    let user_signer = create_user_test_signer(&env.user_account());
    let create_deterministic_account_tx =
        env.deterministic_account_state_init_tx(state_init, &det_account, user_signer, balance);
    env.parent.run_tx(create_deterministic_account_tx);

    env.assert_test_contract_usable_on_account(det_account);

    env.shutdown();
}

/// Ensure repeating a state initialization does not fail, to allow lazy
/// initialization. The submitted deposit should be refunded.
///
/// This test also checks that the signer is charged the balance correctly.
#[test]
fn test_repeated_deterministic_state_init() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));
    env.parent.deploy_global_contract(GlobalContractDeployMode::AccountId);

    let data = BTreeMap::from_iter([(b"key".to_vec(), vec![0u8; 100_000])]);
    let (state_init, det_account) = env.new_deterministic_account_with_data(data.clone());

    // send 10 times the required amount
    let required_for_storage = env.balance_for_storage(state_init);
    let attached_balance = required_for_storage.checked_mul(10).unwrap();

    let deposit_before = env.parent.get_account_state(env.user_account()).amount;

    // first init
    let outcome = env.try_deploy_deterministic_account_with_data(data.clone(), attached_balance);
    outcome.expect("should be able to send transaction").assert_success();
    assert_eq!(
        required_for_storage,
        env.parent.get_account_state(det_account.clone()).amount,
        "exactly required balance should be on the created account"
    );
    let deposit_between = env.parent.get_account_state(env.user_account()).amount;
    assert!(
        deposit_between <= deposit_before.checked_sub(required_for_storage).unwrap(),
        "signer should have been charged the deposit cost + gas cost"
    );

    // second init
    let outcome = env.try_deploy_deterministic_account_with_data(data, attached_balance);
    outcome.expect("should be able to send transaction").assert_success();
    assert_eq!(
        required_for_storage,
        env.parent.get_account_state(det_account.clone()).amount,
        "exactly attached balance should be on created account, nothing added on the second call"
    );
    let deposit_after = env.parent.get_account_state(env.user_account()).amount;
    assert!(
        deposit_after > deposit_between.checked_sub(required_for_storage).unwrap(),
        "signer should have been refunded the deposit cost and only spend gas cost on the second call"
    );

    env.shutdown();
}

/// Ensure all possible errors in state initialization are returned in their
/// respective failure conditions.
#[test]
fn test_deterministic_state_init_negative_cases() {
    if !ProtocolFeature::DeterministicAccountIds.enabled(PROTOCOL_VERSION) {
        return;
    }
    let mut env = TestEnv::setup(Balance::from_near(100));

    // Try using non-existing global contract
    {
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
    }

    // Now deploy the necessary global contract
    env.parent.deploy_global_contract(GlobalContractDeployMode::AccountId);

    // Try creating an account above ZBA limit without attached balance
    {
        let data = BTreeMap::from_iter([(b"key".to_vec(), vec![0u8; 100_000])]);
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
    }

    // Try creating adding larger-than-allowed KEY to state
    {
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
    }

    // Try creating adding larger-than-allowed VALUE to state
    {
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
    }

    // Try sending the action to an invalid receiver: wrong derived id
    {
        let key = b"key".to_vec();
        let value = vec![1u8; 4_194_305];
        let data = BTreeMap::from_iter([(key, value)]);
        let balance = Balance::from_near(1000);

        let user_signer = create_user_test_signer(&env.user_account());
        let (state_init, _det_account) = env.new_deterministic_account_with_data(data);
        let det_account = "0s1234567890123456789012345678901234567890".parse().unwrap();
        let create_deterministic_account_tx = env.deterministic_account_state_init_tx(
            state_init,
            &det_account,
            user_signer.clone(),
            balance,
        );
        let outcome = env.try_execute_tx(create_deterministic_account_tx);

        assert_matches!(
            outcome,
            Err(InvalidTxError::ActionsValidation(
                ActionsValidationError::InvalidDeterministicStateInitReceiver { .. }
            ))
        );
    }

    // Try sending the action to an invalid receiver: named account
    {
        let key = b"key".to_vec();
        let value = vec![1u8; 4_194_305];
        let data = BTreeMap::from_iter([(key, value)]);
        let balance = Balance::from_near(1000);

        let user_signer = create_user_test_signer(&env.user_account());
        let (state_init, _det_account) = env.new_deterministic_account_with_data(data);
        let det_account = "named.near".parse().unwrap();
        let create_deterministic_account_tx = env.deterministic_account_state_init_tx(
            state_init,
            &det_account,
            user_signer.clone(),
            balance,
        );
        let outcome = env.try_execute_tx(create_deterministic_account_tx);

        assert_matches!(
            outcome,
            Err(InvalidTxError::ActionsValidation(
                ActionsValidationError::InvalidDeterministicStateInitReceiver { .. }
            ))
        );
    }

    env.shutdown();
}

struct TestEnv {
    /// Contains basic test setup with utilities for global contracts.
    parent: GlobalContractsTestEnv,
}

impl TestEnv {
    fn setup(initial_balance: Balance) -> Self {
        let parent = GlobalContractsTestEnv::setup(initial_balance);
        Self { parent }
    }

    fn shutdown(self) {
        self.parent.shutdown();
    }

    fn global_contract_account(&self) -> AccountId {
        self.parent.deploy_account.clone()
    }

    fn user_account(&self) -> AccountId {
        self.parent.account_shard_0.clone()
    }

    fn independent_account(&self) -> AccountId {
        self.parent.account_shard_1.clone()
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
            self.parent.next_nonce(),
            user_signer.get_account_id(),
            det_account.clone(),
            &user_signer,
            self.parent.get_tx_block_hash(),
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
            user_signer.clone(),
            balance,
        );
        self.try_execute_tx(create_deterministic_account_tx)
    }

    fn call_test_contract_tx(
        &mut self,
        signer_id: AccountId,
        receiver_id: AccountId,
    ) -> SignedTransaction {
        let signer = create_user_test_signer(&signer_id);
        SignedTransaction::call(
            self.parent.next_nonce(),
            signer_id,
            receiver_id,
            &signer,
            Balance::ZERO,
            "log_something".to_owned(),
            vec![],
            Gas::from_teragas(300),
            self.parent.get_tx_block_hash(),
        )
    }

    fn assert_test_contract_usable_on_account(&mut self, account_with_contract: AccountId) {
        let tx =
            self.call_test_contract_tx(self.independent_account().clone(), account_with_contract);
        self.parent.run_tx(tx);
    }

    fn try_execute_tx(
        &mut self,
        tx: SignedTransaction,
    ) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
        TestLoopNode::for_account(&self.parent.env.node_datas, &rpc_account_id()).execute_tx(
            &mut self.parent.env.test_loop,
            tx,
            Duration::seconds(5),
        )
    }

    fn balance_for_storage(&self, state_init: DeterministicAccountStateInit) -> Balance {
        let runtime_config = self.parent.runtime_config_store.get_config(PROTOCOL_VERSION);
        let storage_config = &runtime_config.fees.storage_usage_config;
        let num_records = state_init.data().len() as u64;

        let num_bytes = state_init.len_bytes() as u64
            + storage_config.num_bytes_account
            + num_records * storage_config.num_extra_bytes_record
            + state_init.code().len() as u64;

        storage_config.storage_amount_per_byte.checked_mul(num_bytes as u128).unwrap()
    }
}
