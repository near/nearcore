//! Non-refundable transfers during account creation allow to sponsor an
//! accounts storage staking balance without that someone being able to run off
//! with the money.
//!
//! This feature introduces the NonrefundableStorageTransfer action.
//!
//! NEP: https://github.com/near/NEPs/pull/491

use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::errors::{
    ActionError, ActionErrorKind, ActionsValidationError, InvalidTxError, TxExecutionError,
};
use near_primitives::test_utils::{eth_implicit_test_account, near_implicit_test_account};
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeployContractAction,
    NonrefundableStorageTransferAction, SignedTransaction, TransferAction,
};
use near_primitives::types::{AccountId, Balance};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::{
    ExecutionStatusView, FinalExecutionOutcomeView, QueryRequest, QueryResponseKind,
};
use near_primitives_core::account::{AccessKey, AccessKeyPermission};
use nearcore::config::GenesisExt;
use nearcore::test_utils::TestEnvNightshadeSetupExt;
use nearcore::NEAR_BASE;

/// Default sender to use in tests of this module.
fn sender() -> AccountId {
    "test0".parse().unwrap()
}

/// Default receiver to use in tests of this module.
fn receiver() -> AccountId {
    "test1".parse().unwrap()
}

/// Default signer (corresponding to the default sender) to use in tests of this module.
fn signer() -> InMemorySigner {
    InMemorySigner::from_seed(sender(), KeyType::ED25519, "test0")
}

/// Creates a test environment using given protocol version (if some).
fn setup_env_with_protocol_version(protocol_version: Option<ProtocolVersion>) -> TestEnv {
    let mut genesis = Genesis::test(vec![sender(), receiver()], 1);
    if let Some(protocol_version) = protocol_version {
        genesis.config.protocol_version = protocol_version;
    }
    TestEnv::builder(ChainGenesis::new(&genesis))
        .real_epoch_managers(&genesis.config)
        .nightshade_runtimes(&genesis)
        .build()
}

/// Creates a test environment using default protocol version.
fn setup_env() -> TestEnv {
    setup_env_with_protocol_version(None)
}

fn get_nonce(env: &mut TestEnv, signer: &InMemorySigner) -> u64 {
    let request = QueryRequest::ViewAccessKey {
        account_id: signer.account_id.clone(),
        public_key: signer.public_key.clone(),
    };
    match env.query_view(request).unwrap().kind {
        QueryResponseKind::AccessKey(view) => view.nonce,
        _ => panic!("wrong query response"),
    }
}

fn account_exists(env: &mut TestEnv, account_id: AccountId) -> bool {
    let request = QueryRequest::ViewAccount { account_id };
    env.query_view(request).is_ok()
}

fn execute_transaction_from_actions(
    env: &mut TestEnv,
    actions: Vec<Action>,
    signer: &InMemorySigner,
    receiver: AccountId,
) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
    let tip = env.clients[0].chain.head().unwrap();
    let nonce = get_nonce(env, signer);
    let tx = SignedTransaction::from_actions(
        nonce + 1,
        signer.account_id.clone(),
        receiver,
        signer,
        actions,
        tip.last_block_hash,
    );
    let tx_result = env.execute_tx(tx);
    let height = env.clients[0].chain.head().unwrap().height;
    for i in 0..2 {
        env.produce_block(0, height + 1 + i);
    }
    tx_result
}

/// Submits a transfer (either regular or non-refundable).
///
/// This methods checks that the balance is subtracted from the sender and added
/// to the receiver, if the status was ok. No checks are done on an error.
fn exec_transfer(
    env: &mut TestEnv,
    signer: InMemorySigner,
    receiver: AccountId,
    deposit: Balance,
    nonrefundable: bool,
    account_creation: bool,
    implicit_account_creation: bool,
    deploy_contract: bool,
) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
    let sender_pre_balance = env.query_balance(sender());
    let (receiver_before_amount, receiver_before_nonrefundable) = if account_creation {
        (0, 0)
    } else {
        let receiver_before = env.query_account(receiver.clone());
        (receiver_before.amount, receiver_before.nonrefundable)
    };

    let mut actions = vec![];

    if account_creation && !implicit_account_creation {
        actions.push(Action::CreateAccount(CreateAccountAction {}));
        actions.push(Action::AddKey(Box::new(AddKeyAction {
            public_key: PublicKey::from_seed(KeyType::ED25519, receiver.as_str()),
            access_key: AccessKey { nonce: 0, permission: AccessKeyPermission::FullAccess },
        })));
    }

    if nonrefundable {
        actions.push(Action::NonrefundableStorageTransfer(NonrefundableStorageTransferAction {
            deposit,
        }));
    } else {
        actions.push(Action::Transfer(TransferAction { deposit }));
    }

    if deploy_contract {
        let contract = near_test_contracts::sized_contract(1500 as usize);
        actions.push(Action::DeployContract(DeployContractAction { code: contract.to_vec() }))
    }

    let tx_result = execute_transaction_from_actions(env, actions, &signer, receiver.clone());

    let outcome = match &tx_result {
        Ok(outcome) => outcome,
        _ => {
            return tx_result;
        }
    };

    if !matches!(outcome.status, near_primitives::views::FinalExecutionStatus::SuccessValue(_)) {
        return tx_result;
    }

    let gas_cost = outcome.gas_cost();
    assert_eq!(sender_pre_balance - deposit - gas_cost, env.query_balance(sender()));

    let receiver_after = env.query_account(receiver);
    let (receiver_expected_amount_after, receiver_expected_non_refundable_after) = if nonrefundable
    {
        (receiver_before_amount, receiver_before_nonrefundable + deposit)
    } else {
        (receiver_before_amount + deposit, receiver_before_nonrefundable)
    };

    assert_eq!(receiver_after.amount, receiver_expected_amount_after);
    assert_eq!(receiver_after.nonrefundable, receiver_expected_non_refundable_after);

    tx_result
}

fn delete_account(
    env: &mut TestEnv,
    signer: &InMemorySigner,
) -> Result<FinalExecutionOutcomeView, InvalidTxError> {
    let actions = vec![Action::DeleteAccount(DeleteAccountAction { beneficiary_id: receiver() })];
    execute_transaction_from_actions(env, actions, &signer, signer.account_id.clone())
}

/// Can delete account with non-refundable storage.
#[test]
fn deleting_account_with_non_refundable_storage() {
    let mut env = setup_env();
    let new_account_id: AccountId = "subaccount.test0".parse().unwrap();
    let new_account = InMemorySigner::from_seed(
        new_account_id.clone(),
        KeyType::ED25519,
        new_account_id.as_str(),
    );
    // Create account with non-refundable storage.
    // Deploy a contract that does not fit within Zero-balance account limit.
    let create_account_tx_result = exec_transfer(
        &mut env,
        signer(),
        new_account_id.clone(),
        NEAR_BASE,
        true,
        true,
        false,
        true,
    );
    create_account_tx_result.unwrap().assert_success();

    // Send some NEAR (refundable) so that the new account is able to pay the gas for its deletion in the next transaction.
    let send_money_tx_result = exec_transfer(
        &mut env,
        signer(),
        new_account_id.clone(),
        10u128.pow(20),
        false,
        false,
        false,
        false,
    );
    send_money_tx_result.unwrap().assert_success();

    // Delete the new account (that has 1 NEAR of non-refundable balance).
    let delete_account_tx_result = delete_account(&mut env, &new_account);
    delete_account_tx_result.unwrap().assert_success();
    assert!(!account_exists(&mut env, new_account_id));
}

/// Non-refundable balance cannot be transferred.
#[test]
fn non_refundable_balance_cannot_be_transferred() {
    let mut env = setup_env();
    let new_account_id: AccountId = "subaccount.test0".parse().unwrap();
    let new_account = InMemorySigner::from_seed(
        new_account_id.clone(),
        KeyType::ED25519,
        new_account_id.as_str(),
    );
    // The `new_account` is created with 1 NEAR non-refundable balance.
    let create_account_tx_result = exec_transfer(
        &mut env,
        signer(),
        new_account_id.clone(),
        NEAR_BASE,
        true,
        true,
        false,
        false,
    );
    create_account_tx_result.unwrap().assert_success();

    // Although `new_account` has 1 NEAR non-refundable balance, it cannot make neither refundable nor non-refundable transfer of 1 yoctoNEAR.
    for nonrefundable in [false, true] {
        let transfer_tx_result = exec_transfer(
            &mut env,
            new_account.clone(),
            receiver(),
            1,
            nonrefundable,
            false,
            false,
            false,
        );
        match transfer_tx_result {
            Err(InvalidTxError::NotEnoughBalance { signer_id, balance, .. }) => {
                assert_eq!(signer_id, new_account_id);
                assert_eq!(balance, 0);
            }
            _ => panic!("Expected NotEnoughBalance error"),
        }
    }
}

/// Non-refundable balance allows to have account with zero balance and more than 1kB of state.
#[test]
fn non_refundable_balance_allows_1kb_state_with_zero_balance() {
    let mut env = setup_env();
    let new_account_id: AccountId = "subaccount.test0".parse().unwrap();
    let tx_result =
        exec_transfer(&mut env, signer(), new_account_id, NEAR_BASE / 5, true, true, false, true);
    tx_result.unwrap().assert_success();
}

/// Non-refundable transfer successfully adds non-refundable balance when creating named account.
#[test]
fn non_refundable_transfer_create_named_account() {
    let new_account_id: AccountId = "subaccount.test0".parse().unwrap();
    let tx_result =
        exec_transfer(&mut setup_env(), signer(), new_account_id, 1, true, true, false, false);
    tx_result.unwrap().assert_success();
}

/// Non-refundable transfer successfully adds non-refundable balance when creating NEAR-implicit account.
#[test]
fn non_refundable_transfer_create_near_implicit_account() {
    let new_account_id = near_implicit_test_account();
    let tx_result =
        exec_transfer(&mut setup_env(), signer(), new_account_id, 1, true, true, true, false);
    tx_result.unwrap().assert_success();
}

/// Non-refundable transfer successfully adds non-refundable balance when creating ETH-implicit account.
#[test]
fn non_refundable_transfer_create_eth_implicit_account() {
    let new_account_id = eth_implicit_test_account();
    let tx_result =
        exec_transfer(&mut setup_env(), signer(), new_account_id, 1, true, true, true, false);
    tx_result.unwrap().assert_success();
}

/// Non-refundable transfer is rejected on existing account.
#[test]
fn reject_non_refundable_transfer_existing_account() {
    let tx_result =
        exec_transfer(&mut setup_env(), signer(), receiver(), 1, true, false, false, false);
    let status = &tx_result.unwrap().receipts_outcome[0].outcome.status;
    assert!(matches!(
        status,
        ExecutionStatusView::Failure(TxExecutionError::ActionError(
            ActionError { kind: ActionErrorKind::NonRefundableBalanceToExistingAccount { account_id }, .. }
        )) if *account_id == receiver(),
    ));
}

/// During the protocol upgrade phase, before the voting completes, we must not
/// include non-refundable transfer actions on the chain.
///
/// The correct way to handle it is to reject transaction before they even get
/// into the transaction pool. Hence, we check that an `InvalidTxError` error is
/// returned for older protocol versions.
#[test]
fn reject_non_refundable_transfer_in_older_versions() {
    let mut env = setup_env_with_protocol_version(Some(
        ProtocolFeature::NonRefundableBalance.protocol_version() - 1,
    ));
    let tx_result = exec_transfer(&mut env, signer(), receiver(), 1, true, false, false, false);
    assert_eq!(
        tx_result,
        Err(InvalidTxError::ActionsValidation(
            ActionsValidationError::UnsupportedProtocolFeature {
                protocol_feature: "NonRefundableBalance".to_string(),
                version: ProtocolFeature::NonRefundableBalance.protocol_version()
            }
        ))
    );
}
