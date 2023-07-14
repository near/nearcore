use std::sync::Arc;

mod rpc;
mod runtime;

use assert_matches::assert_matches;
use near_crypto::{InMemorySigner, KeyType};
use near_jsonrpc_primitives::errors::ServerError;
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::config::{ActionCosts, ExtCosts};
use near_primitives::errors::{
    ActionError, ActionErrorKind, FunctionCallError, InvalidAccessKeyError, InvalidTxError,
    MethodResolveError, TxExecutionError,
};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{AccountId, Balance, TrieNodesCount};
use near_primitives::views::{
    AccessKeyView, AccountView, ExecutionMetadataView, FinalExecutionOutcomeView,
    FinalExecutionStatus,
};
use nearcore::config::{NEAR_BASE, TESTING_INIT_BALANCE, TESTING_INIT_STAKE};

use crate::node::Node;
use crate::user::User;
use near_primitives::receipt::{ActionReceipt, Receipt, ReceiptEnum};
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::test_utils;
use near_primitives::transaction::{Action, DeployContractAction, FunctionCallAction};
use testlib::fees_utils::FeeHelper;
use testlib::runtime_utils::{
    alice_account, bob_account, eve_dot_alice_account, x_dot_y_dot_alice_account,
};

/// The amount to send with function call.
const FUNCTION_CALL_AMOUNT: Balance = TESTING_INIT_BALANCE / 10;

pub(crate) fn fee_helper(node: &impl Node) -> FeeHelper {
    FeeHelper::new(RuntimeConfig::test().fees, node.genesis().config.min_gas_price)
}

/// Adds given access key to the given account_id using signer2.
fn add_access_key(
    node: &impl Node,
    node_user: &dyn User,
    access_key: &AccessKey,
    signer2: &InMemorySigner,
) -> FinalExecutionOutcomeView {
    let root = node_user.get_state_root();
    let account_id = &node.account_id().unwrap();
    let transaction_result = node_user
        .add_key(account_id.clone(), signer2.public_key.clone(), access_key.clone())
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    transaction_result
}

pub fn test_smart_contract_simple(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(alice_account(), bob_account(), "run_test", vec![], 10u64.pow(14), 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::SuccessValue(10i32.to_le_bytes().to_vec())
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_panic(node: impl Node) {
    let node_user = node.user();
    let transaction_result = node_user
        .function_call(
            alice_account(),
            alice_account(),
            "panic_with_message",
            vec![],
            10u64.pow(14),
            0,
        )
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::ExecutionError(
                    "Smart contract panicked: WAT?".to_string()
                ))
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
}

pub fn test_smart_contract_self_call(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(account_id.clone(), account_id.clone(), "run_test", vec![], 10u64.pow(14), 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::SuccessValue(10i32.to_le_bytes().to_vec())
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_bad_method_name(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), "_run_test", vec![], 10u64.pow(14), 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodNotFound
                ))
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_empty_method_name_with_no_tokens(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), "", vec![], 10u64.pow(14), 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName
                ))
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_empty_method_name_with_tokens(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), "", vec![], 10u64.pow(14), 10)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::FunctionCallError(FunctionCallError::MethodResolveError(
                    MethodResolveError::MethodEmptyName
                ))
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_with_args(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(
            account_id.clone(),
            bob_account(),
            "sum_with_input",
            (2u64..4).flat_map(|x| x.to_le_bytes().to_vec()).collect(),
            10u64.pow(14),
            0,
        )
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::SuccessValue(5u64.to_le_bytes().to_vec())
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_async_call_with_logs(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), "log_something", vec![], 10u64.pow(14), 0)
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(transaction_result.receipts_outcome[0].outcome.logs[0], "hello".to_string());
}

pub fn test_nonce_update_when_deploying_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let wasm_binary = b"test_binary";
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.deploy_contract(account_id.clone(), wasm_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_nonce_updated_when_tx_failed(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    node_user.send_money(account_id.clone(), bob_account(), TESTING_INIT_BALANCE + 1).unwrap_err();
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 0);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

pub fn test_upload_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            eve_dot_alice_account(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE / 2,
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);

    node_user.view_contract_code(&eve_dot_alice_account()).expect_err(
        "RpcError { code: -32000, message: \"Server error\", data: Some(String(\"contract code of account eve.alice.near does not exist while viewing\")) }");

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let wasm_binary = b"test_binary";
    let transaction_result =
        node_user.deploy_contract(eve_dot_alice_account(), wasm_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(&eve_dot_alice_account()).unwrap();
    assert_eq!(account.code_hash, hash(wasm_binary));

    let code = node_user.view_contract_code(&eve_dot_alice_account()).unwrap();
    assert_eq!(code.code, wasm_binary.to_vec());
}

pub fn test_redeploy_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let test_binary = b"test_binary";
    let transaction_result =
        node_user.deploy_contract(account_id.clone(), test_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.code_hash, hash(test_binary));
}

pub fn test_send_money(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10000;
    let fee_helper = fee_helper(&node);
    let transfer_cost = fee_helper.transfer_cost();
    let transaction_result =
        node_user.send_money(account_id.clone(), bob_account(), money_used).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let AccountView { amount, locked, .. } = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (amount, locked),
        (
            TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE - transfer_cost,
            TESTING_INIT_STAKE
        )
    );
    let AccountView { amount, locked, .. } = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(
        (amount, locked),
        (TESTING_INIT_BALANCE + money_used - TESTING_INIT_STAKE, TESTING_INIT_STAKE,)
    );
}

pub fn transfer_tokens_implicit_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let tokens_used = 10u128.pow(25);
    let fee_helper = fee_helper(&node);
    let transfer_cost = fee_helper.transfer_cost_64len_hex();
    let public_key = node_user.signer().public_key();
    let raw_public_key = public_key.unwrap_as_ed25519().0.to_vec();
    let receiver_id = AccountId::try_from(hex::encode(&raw_public_key)).unwrap();
    let transaction_result =
        node_user.send_money(account_id.clone(), receiver_id.clone(), tokens_used).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let AccountView { amount, locked, .. } = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (amount, locked),
        (
            TESTING_INIT_BALANCE - tokens_used - TESTING_INIT_STAKE - transfer_cost,
            TESTING_INIT_STAKE
        )
    );

    let AccountView { amount, locked, .. } = node_user.view_account(&receiver_id).unwrap();
    assert_eq!((amount, locked), (tokens_used, 0));

    let view_access_key = node_user.get_access_key(&receiver_id, &public_key).unwrap();
    assert_eq!(view_access_key, AccessKey::full_access().into());

    let transaction_result =
        node_user.send_money(account_id.clone(), receiver_id.clone(), tokens_used).unwrap();

    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 2);

    let AccountView { amount, locked, .. } = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (amount, locked),
        (
            TESTING_INIT_BALANCE - 2 * tokens_used - TESTING_INIT_STAKE - 2 * transfer_cost,
            TESTING_INIT_STAKE
        )
    );

    let AccountView { amount, locked, .. } = node_user.view_account(&receiver_id).unwrap();
    assert_eq!((amount, locked), (tokens_used * 2, 0));
}

pub fn trying_to_create_implicit_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let tokens_used = 10u128.pow(25);
    let fee_helper = fee_helper(&node);

    let public_key = node_user.signer().public_key();
    let raw_public_key = public_key.unwrap_as_ed25519().0.to_vec();
    let receiver_id = AccountId::try_from(hex::encode(&raw_public_key)).unwrap();

    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            receiver_id.clone(),
            node.signer().public_key(),
            tokens_used,
        )
        .unwrap();

    let cost = fee_helper.create_account_transfer_full_key_cost_fail_on_create_account()
        + fee_helper.gas_to_balance(
            fee_helper.cfg.fee(ActionCosts::create_account).send_fee(false)
                + fee_helper
                    .cfg
                    .fee(near_primitives::config::ActionCosts::add_full_access_key)
                    .send_fee(false),
        );

    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::OnlyImplicitAccountCreationAllowed {
                    account_id: receiver_id.clone(),
                }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.locked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE - cost, TESTING_INIT_STAKE)
    );

    let result2 = node_user.view_account(&receiver_id);
    assert!(result2.is_err());
}

pub fn test_smart_contract_reward(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let bob = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(bob.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    let transaction_result = node_user
        .function_call(alice_account(), bob_account(), "run_test", vec![], 10u64.pow(14), 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::SuccessValue(10i32.to_le_bytes().to_vec())
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let fee_helper = fee_helper(&node);
    let bob = node_user.view_account(&bob_account()).unwrap();
    let gas_burnt_for_function_call = transaction_result.receipts_outcome[0].outcome.gas_burnt
        - fee_helper.function_call_exec_gas(b"run_test".len() as u64);
    let reward = fee_helper.gas_burnt_to_reward(gas_burnt_for_function_call);
    assert_eq!(bob.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE + reward);
}

pub fn test_send_money_over_balance(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = TESTING_INIT_BALANCE + 1;
    node_user.send_money(account_id.clone(), bob_account(), money_used).unwrap_err();
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.locked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
    );
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 0);

    let result2 = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(
        (result2.amount, result2.locked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
    );
}

pub fn test_refund_on_send_money_to_non_existent_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    // Successful atomic transfer has the same cost as failed atomic transfer.
    let fee_helper = fee_helper(&node);
    let transfer_cost = fee_helper.transfer_cost();
    let transaction_result =
        node_user.send_money(account_id.clone(), eve_dot_alice_account(), money_used).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::AccountDoesNotExist { account_id: eve_dot_alice_account() }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.locked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE - transfer_cost, TESTING_INIT_STAKE)
    );
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);
    let result2 = node_user.view_account(&eve_dot_alice_account());
    assert!(result2.is_err());
}

pub fn test_create_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = NEAR_BASE;
    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            eve_dot_alice_account(),
            node.signer().public_key(),
            money_used,
        )
        .unwrap();

    let fee_helper = fee_helper(&node);
    let create_account_cost = fee_helper.create_account_transfer_full_key_cost();

    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.locked),
        (
            TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE - create_account_cost,
            TESTING_INIT_STAKE
        )
    );

    let result2 = node_user.view_account(&eve_dot_alice_account()).unwrap();
    assert_eq!((result2.amount, result2.locked), (money_used, 0));
}

pub fn test_create_account_again(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = NEAR_BASE;
    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            eve_dot_alice_account(),
            node.signer().public_key(),
            money_used,
        )
        .unwrap();

    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let fee_helper = fee_helper(&node);
    let create_account_cost = fee_helper.create_account_transfer_full_key_cost();

    let result1 = node_user.view_account(account_id).unwrap();
    let new_expected_balance =
        TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE - create_account_cost;
    assert_eq!((result1.amount, result1.locked), (new_expected_balance, TESTING_INIT_STAKE));
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result2 = node_user.view_account(&eve_dot_alice_account()).unwrap();
    assert_eq!((result2.amount, result2.locked), (money_used, 0));

    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            eve_dot_alice_account(),
            node.signer().public_key(),
            money_used,
        )
        .unwrap();

    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::AccountAlreadyExists { account_id: eve_dot_alice_account() }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 2);

    // Additional cost for trying to create an account with repeated name. Will fail after
    // the first action.
    let additional_cost = fee_helper.create_account_transfer_full_key_cost_fail_on_create_account();

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.locked),
        (new_expected_balance - additional_cost, TESTING_INIT_STAKE)
    );
}

pub fn test_create_account_failure_no_funds(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let transaction_result = node_user
        .create_account(account_id.clone(), eve_dot_alice_account(), node.signer().public_key(), 0)
        .unwrap();
    assert_matches!(transaction_result.status, FinalExecutionStatus::SuccessValue(_));
}

pub fn test_create_account_failure_already_exists(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 1000;

    let transaction_result = node_user
        .create_account(account_id.clone(), bob_account(), node.signer().public_key(), money_used)
        .unwrap();
    let fee_helper = fee_helper(&node);
    let create_account_cost =
        fee_helper.create_account_transfer_full_key_cost_fail_on_create_account();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::AccountAlreadyExists { account_id: bob_account() }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.locked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE - create_account_cost, TESTING_INIT_STAKE)
    );

    let result2 = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(
        (result2.amount, result2.locked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
    );
}

pub fn test_swap_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = TESTING_INIT_BALANCE / 2;
    node_user
        .create_account(
            account_id.clone(),
            eve_dot_alice_account(),
            node.signer().public_key(),
            money_used,
        )
        .unwrap();
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let transaction_result = node_user
        .swap_key(
            eve_dot_alice_account(),
            node.signer().public_key(),
            signer2.public_key.clone(),
            AccessKey::full_access(),
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root1 = node_user.get_state_root();
    assert_ne!(new_root, new_root1);

    assert!(node_user
        .get_access_key(&eve_dot_alice_account(), &node.signer().public_key())
        .is_err());
    assert!(node_user.get_access_key(&eve_dot_alice_account(), &signer2.public_key).is_ok());
}

pub fn test_add_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    let node_user = node.user();

    add_access_key(&node, node_user.as_ref(), &AccessKey::full_access(), &signer2);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_ok());
}

pub fn test_add_existing_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .add_key(account_id.clone(), node.signer().public_key(), AccessKey::full_access())
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::AddKeyAlreadyExists {
                    account_id: account_id.clone(),
                    public_key: node.signer().public_key()
                }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
}

pub fn test_delete_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    let node_user = node.user();
    add_access_key(&node, node_user.as_ref(), &AccessKey::full_access(), &signer2);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_ok());

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.delete_key(account_id.clone(), node.signer().public_key()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_err());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_ok());
}

pub fn test_delete_key_not_owned(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    let node_user = node.user();

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_err());

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.delete_key(account_id.clone(), signer2.public_key.clone()).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::DeleteKeyDoesNotExist {
                    account_id: account_id.clone(),
                    public_key: signer2.public_key.clone()
                }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_err());
}

pub fn test_delete_key_last(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    let transaction_result = node_user.delete_key(account_id.clone(), node.signer().public_key());

    match transaction_result {
        Ok(transaction_result) => {
            assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
            assert_eq!(transaction_result.receipts_outcome.len(), 1);
        }
        Err(err) => {
            // TODO(#6724): This is a wrong error, the transaction actually
            // succeeds. We get an error here when we retry the tx and the second
            // time around it fails. Normally, retries are handled by nonces, but we
            // forget the nonce when we delete a key!
            assert_eq!(
                err,
                ServerError::TxExecutionError(TxExecutionError::InvalidTxError(
                    InvalidTxError::InvalidAccessKeyError(
                        InvalidAccessKeyError::AccessKeyNotFound {
                            account_id: account_id.clone(),
                            public_key: node.signer().public_key(),
                        },
                    )
                ))
            )
        }
    }

    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_err());
}

#[track_caller]
fn assert_access_key(
    access_key: &AccessKey,
    access_key_view: AccessKeyView,
    result: &FinalExecutionOutcomeView,
    user: &dyn User,
) {
    let mut key = access_key.clone();
    let block = user.get_block(result.transaction_outcome.block_hash);
    if let Some(b) = block {
        key.nonce = (b.header.height - 1) * AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER;
    }
    assert_eq!(access_key_view, key.into());
}

pub fn test_add_access_key_function_call(node: impl Node) {
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: None,
            receiver_id: account_id.to_string(),
            method_names: vec![],
        }),
    };
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    let result = add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_access_key(&access_key, view_access_key, &result, node_user.as_ref());
}

pub fn test_delete_access_key(node: impl Node) {
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: None,
            receiver_id: account_id.to_string(),
            method_names: vec![],
        }),
    };
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_ok());

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.delete_key(account_id.clone(), signer2.public_key.clone()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_err());
}

pub fn test_add_access_key_with_allowance(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(10),
            receiver_id: account_id.to_string(),
            method_names: vec![],
        }),
    };
    let node_user = node.user();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    let fee_helper = fee_helper(&node);
    let add_access_key_cost = fee_helper.add_key_cost(0);
    let result = add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, initial_balance - add_access_key_cost);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_access_key(&access_key, view_access_key, &result, node_user.as_ref());
}

pub fn test_delete_access_key_with_allowance(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(10),
            receiver_id: account_id.to_string(),
            method_names: vec![],
        }),
    };
    let node_user = node.user();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    let fee_helper = fee_helper(&node);
    let add_access_key_cost = fee_helper.add_key_cost(0);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_ok());

    let root = node_user.get_state_root();
    let delete_access_key_cost = fee_helper.delete_key_cost();
    let transaction_result =
        node_user.delete_key(account_id.clone(), signer2.public_key.clone()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, initial_balance - add_access_key_cost - delete_access_key_cost);

    assert!(node_user.get_access_key(account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(account_id, &signer2.public_key).is_err());
}

pub fn test_access_key_smart_contract(node: impl Node) {
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(FUNCTION_CALL_AMOUNT),
            receiver_id: bob_account().into(),
            method_names: vec![],
        }),
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = Arc::new(InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519));
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(signer2.clone());

    let method_name = "run_test";
    let prepaid_gas = 10u64.pow(14);
    let fee_helper = fee_helper(&node);
    let function_call_cost =
        fee_helper.function_call_cost(method_name.as_bytes().len() as u64, prepaid_gas);
    let exec_gas = fee_helper.function_call_exec_gas(method_name.as_bytes().len() as u64);
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), method_name, vec![], prepaid_gas, 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::SuccessValue(10i32.to_le_bytes().to_vec())
    );
    let gas_refund = fee_helper.gas_to_balance(
        prepaid_gas + exec_gas - transaction_result.receipts_outcome[0].outcome.gas_burnt,
    );

    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(
        view_access_key,
        AccessKey {
            nonce: view_access_key.nonce,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(FUNCTION_CALL_AMOUNT - function_call_cost + gas_refund),
                receiver_id: bob_account().into(),
                method_names: vec![],
            }),
        }
        .into()
    );
}

pub fn test_access_key_smart_contract_reject_method_name(node: impl Node) {
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(FUNCTION_CALL_AMOUNT),
            receiver_id: bob_account().into(),
            method_names: vec!["log_something".to_string()],
        }),
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(Arc::new(signer2));

    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), "run_test", vec![], 10u64.pow(14), 0)
        .unwrap_err();
    assert_eq!(
        transaction_result,
        ServerError::TxExecutionError(TxExecutionError::InvalidTxError(
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::MethodNameMismatch {
                method_name: "run_test".to_string()
            })
        ))
    );
}

pub fn test_access_key_smart_contract_reject_contract_id(node: impl Node) {
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(FUNCTION_CALL_AMOUNT),
            receiver_id: bob_account().into(),
            method_names: vec![],
        }),
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(Arc::new(signer2));

    let transaction_result = node_user
        .function_call(
            account_id.clone(),
            eve_dot_alice_account(),
            "run_test",
            vec![],
            10u64.pow(14),
            0,
        )
        .unwrap_err();
    assert_eq!(
        transaction_result,
        ServerError::TxExecutionError(TxExecutionError::InvalidTxError(
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::ReceiverMismatch {
                tx_receiver: eve_dot_alice_account(),
                ak_receiver: bob_account().into()
            })
        ))
    );
}

pub fn test_access_key_reject_non_function_call(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(FUNCTION_CALL_AMOUNT),
            receiver_id: account_id.to_string(),
            method_names: vec![],
        }),
    };
    let mut node_user = node.user();
    let signer2 = InMemorySigner::from_random("test".parse().unwrap(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(Arc::new(signer2));

    let transaction_result =
        node_user.delete_key(account_id.clone(), node.signer().public_key()).unwrap_err();
    assert_eq!(
        transaction_result,
        ServerError::TxExecutionError(TxExecutionError::InvalidTxError(
            InvalidTxError::InvalidAccessKeyError(InvalidAccessKeyError::RequiresFullAccess)
        ))
    );
}

pub fn test_increase_stake(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let account_id = &node.account_id().unwrap();
    let amount_staked = TESTING_INIT_STAKE + 1;
    let fee_helper = fee_helper(&node);
    let stake_cost = fee_helper.stake_cost();
    let transaction_result = node_user
        .stake(account_id.clone(), node.block_signer().public_key(), amount_staked)
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let node_user = node.user();
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1 - stake_cost);
    assert_eq!(account.locked, amount_staked)
}

pub fn test_decrease_stake(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let amount_staked = 10;
    let account_id = &node.account_id().unwrap();
    let transaction_result = node_user
        .stake(account_id.clone(), node.block_signer().public_key(), amount_staked)
        .unwrap();
    let fee_helper = fee_helper(&node);
    let stake_cost = fee_helper.stake_cost();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - stake_cost);
    assert_eq!(account.locked, TESTING_INIT_STAKE);
}

pub fn test_unstake_while_not_staked(node: impl Node) {
    let node_user = node.user();
    let transaction_result = node_user
        .create_account(
            alice_account(),
            eve_dot_alice_account(),
            node.signer().public_key(),
            TESTING_INIT_BALANCE / 2,
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    let transaction_result =
        node_user.stake(eve_dot_alice_account(), node.block_signer().public_key(), 0).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::TriesToUnstake { account_id: eve_dot_alice_account() }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
}

/// Account must have enough balance to cover storage of the account.
pub fn test_fail_not_enough_balance_for_storage(node: impl Node) {
    let mut node_user = node.user();
    let account_id = bob_account();
    let signer = Arc::new(InMemorySigner::from_seed(
        account_id.clone(),
        KeyType::ED25519,
        account_id.as_ref(),
    ));
    node_user.set_signer(signer);
    node_user.send_money(account_id, alice_account(), 10).unwrap_err();
}

pub fn test_delete_account_ok(node: impl Node) {
    let money_used = TESTING_INIT_BALANCE / 2;
    let node_user = node.user();
    let _ = node_user.create_account(
        alice_account(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        money_used,
    );
    let transaction_result =
        node_user.delete_account(eve_dot_alice_account(), eve_dot_alice_account()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert!(node.user().view_account(&eve_dot_alice_account()).is_err());
}

pub fn test_creating_invalid_subaccount_fail(node: impl Node) {
    let money_used = TESTING_INIT_BALANCE / 2;
    let node_user = node.user();
    let result = node_user
        .create_account(
            alice_account(),
            x_dot_y_dot_alice_account(),
            node.signer().public_key(),
            money_used,
        )
        .unwrap();
    assert_eq!(
        result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::CreateAccountNotAllowed {
                    account_id: x_dot_y_dot_alice_account(),
                    predecessor_id: alice_account()
                }
            }
            .into()
        )
    );
}

pub fn test_delete_account_fail(node: impl Node) {
    let money_used = TESTING_INIT_BALANCE / 2;
    let node_user = node.user();
    let _ = node_user.create_account(
        alice_account(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        money_used,
    );
    let initial_amount = node_user.view_account(&node.account_id().unwrap()).unwrap().amount;
    let fee_helper = fee_helper(&node);
    let delete_account_cost = fee_helper.prepaid_delete_account_cost();

    let transaction_result =
        node_user.delete_account(alice_account(), eve_dot_alice_account()).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::ActorNoPermission {
                    account_id: eve_dot_alice_account(),
                    actor_id: alice_account()
                }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
    assert!(node.user().view_account(&bob_account()).is_ok());
    assert_eq!(
        node.user().view_account(&node.account_id().unwrap()).unwrap().amount,
        initial_amount - delete_account_cost
    );
}

pub fn test_delete_account_no_account(node: impl Node) {
    let node_user = node.user();
    let transaction_result =
        node_user.delete_account(alice_account(), eve_dot_alice_account()).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::AccountDoesNotExist { account_id: eve_dot_alice_account() }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 2);
}

pub fn test_delete_account_while_staking(node: impl Node) {
    let money_used = TESTING_INIT_BALANCE / 2;
    let node_user = node.user();
    let _ = node_user.create_account(
        alice_account(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        money_used,
    );
    let fee_helper = fee_helper(&node);
    let stake_fee = fee_helper.stake_cost();
    let delete_account_fee = fee_helper.prepaid_delete_account_cost();
    let transaction_result = node_user
        .stake(
            eve_dot_alice_account(),
            node.block_signer().public_key(),
            money_used - stake_fee - delete_account_fee,
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(Vec::new()));
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    let transaction_result =
        node_user.delete_account(eve_dot_alice_account(), eve_dot_alice_account()).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError {
                index: Some(0),
                kind: ActionErrorKind::DeleteAccountStaking { account_id: eve_dot_alice_account() }
            }
            .into()
        )
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 1);
    assert!(node.user().view_account(&eve_dot_alice_account()).is_ok());
}

pub fn test_smart_contract_free(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(alice_account(), bob_account(), "run_test", vec![], 10u64.pow(14), 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::SuccessValue(10i32.to_le_bytes().to_vec())
    );
    assert_eq!(transaction_result.receipts_outcome.len(), 1);

    let total_gas_burnt = transaction_result.transaction_outcome.outcome.gas_burnt
        + transaction_result.receipts_outcome.iter().map(|t| t.outcome.gas_burnt).sum::<u64>();
    assert_eq!(total_gas_burnt, 0);

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

/// Get number of charged trie node accesses from the execution metadata.
fn get_trie_nodes_count(
    metadata: &ExecutionMetadataView,
    runtime_config: &RuntimeConfig,
) -> TrieNodesCount {
    let mut count = TrieNodesCount { db_reads: 0, mem_reads: 0 };
    for cost in metadata.gas_profile.clone().unwrap_or_default().iter() {
        match cost.cost.as_str() {
            "TOUCHING_TRIE_NODE" => {
                count.db_reads += cost.gas_used
                    / runtime_config.wasm_config.ext_costs.gas_cost(ExtCosts::touching_trie_node);
            }
            "READ_CACHED_TRIE_NODE" => {
                count.mem_reads += cost.gas_used
                    / runtime_config
                        .wasm_config
                        .ext_costs
                        .gas_cost(ExtCosts::read_cached_trie_node);
            }
            _ => {}
        };
    }
    count
}

/// Checks correctness of touching trie node cost for writing value into contract storage.
/// First call should touch 2 nodes (Extension and Branch), because before it contract storage is empty.
/// The second call should touch 4 nodes, because the first call adds Leaf and Value nodes to trie.
pub fn test_contract_write_key_value_cost(node: impl Node) {
    let node_user = node.user();
    let results: Vec<_> = vec![
        TrieNodesCount { db_reads: 2, mem_reads: 0 },
        TrieNodesCount { db_reads: 4, mem_reads: 0 },
    ];
    for i in 0..2 {
        let transaction_result = node_user
            .function_call(
                alice_account(),
                bob_account(),
                "write_key_value",
                test_utils::encode(&[10u64, 20u64]),
                10u64.pow(14),
                0,
            )
            .unwrap();
        assert_matches!(transaction_result.status, FinalExecutionStatus::SuccessValue(_));
        assert_eq!(transaction_result.receipts_outcome.len(), 2);

        let trie_nodes_count = get_trie_nodes_count(
            &transaction_result.receipts_outcome[0].outcome.metadata,
            &RuntimeConfig::test(),
        );
        assert_eq!(trie_nodes_count, results[i]);
    }
}

fn make_write_key_value_action(key: Vec<u64>, value: Vec<u64>) -> Action {
    let args: Vec<u64> = key.into_iter().chain(value.into_iter()).collect();
    FunctionCallAction {
        method_name: "write_key_value".to_string(),
        args: test_utils::encode(&args),
        gas: 10u64.pow(14),
        deposit: 0,
    }
    .into()
}

fn make_receipt(node: &impl Node, actions: Vec<Action>, receiver_id: AccountId) -> Receipt {
    let receipt_enum = ReceiptEnum::Action(ActionReceipt {
        signer_id: alice_account(),
        signer_public_key: node.signer().as_ref().public_key(),
        gas_price: 0,
        output_data_receivers: vec![],
        input_data_ids: vec![],
        actions,
    });
    Receipt {
        predecessor_id: alice_account(),
        receiver_id,
        receipt_id: CryptoHash::hash_borsh(&receipt_enum),
        receipt: receipt_enum,
    }
}

/// Check that numbers of charged trie node accesses during execution of the given receipts matches the provided
/// results.
/// Runs the list of receipts 2 times. 1st run establishes the state structure, 2nd run is used to get node counts.
fn check_trie_nodes_count(
    node: &impl Node,
    runtime_config: &RuntimeConfig,
    receipts: Vec<Receipt>,
    results: Vec<TrieNodesCount>,
    use_flat_storage: bool,
) {
    let node_user = node.user();
    let mut node_touches: Vec<_> = vec![];
    let receipt_hashes: Vec<CryptoHash> =
        receipts.iter().map(|receipt| receipt.receipt_id).collect();

    for i in 0..2 {
        node_user.add_receipts(receipts.clone(), use_flat_storage).unwrap();

        if i == 1 {
            node_touches = receipt_hashes
                .iter()
                .map(|receipt_hash| {
                    let result = node_user.get_transaction_result(receipt_hash);
                    get_trie_nodes_count(&result.metadata, runtime_config)
                })
                .collect();
        }
    }

    assert_eq!(node_touches, results);
}

/// Check correctness of charging for trie node accesses with enabled chunk nodes cache.
/// We run the same set of receipts 2 times and compare resulting trie node counts. Each receipt writes some key-value
/// pair to the contract storage.
/// 1st run establishes the trie structure. For our needs, the structure is:
///
///                                                    --> (Leaf) -> (Value 1)
/// (Extension) -> (Branch) -> (Extension) -> (Branch) |-> (Leaf) -> (Value 2)
///                                                    --> (Leaf) -> (Value 3)
///
/// 1st receipt should count 6 db reads.
/// 2nd and 3rd receipts should count 2 db and 4 memory reads, because for them first 4 nodes were already put into the
/// chunk cache.
pub fn test_chunk_nodes_cache_common_parent(node: impl Node, runtime_config: RuntimeConfig) {
    let receipts: Vec<Receipt> = (0..3)
        .map(|i| {
            make_receipt(
                &node,
                vec![make_write_key_value_action(vec![i], vec![10u64 + i])],
                bob_account(),
            )
        })
        .collect();

    let results = vec![
        TrieNodesCount { db_reads: 6, mem_reads: 0 },
        TrieNodesCount { db_reads: 2, mem_reads: 4 },
        TrieNodesCount { db_reads: 2, mem_reads: 4 },
    ];
    check_trie_nodes_count(&node, &runtime_config, receipts, results, true);
}

/// This test is similar to `test_chunk_nodes_cache_common_parent` but checks another trie structure:
///
///                                                    --> (Value 1)
/// (Extension) -> (Branch) -> (Extension) -> (Branch) |-> (Leaf) -> (Value 2)
///
/// 1st receipt should count 5 db reads.
/// 2nd receipt should count 2 db and 4 memory reads.
pub fn test_chunk_nodes_cache_branch_value(node: impl Node, runtime_config: RuntimeConfig) {
    let receipts: Vec<Receipt> = (0..2)
        .map(|i| {
            make_receipt(
                &node,
                vec![make_write_key_value_action(vec![1; i + 1], vec![10u64 + i as u64])],
                bob_account(),
            )
        })
        .collect();

    let results = vec![
        TrieNodesCount { db_reads: 5, mem_reads: 0 },
        TrieNodesCount { db_reads: 2, mem_reads: 4 },
    ];
    check_trie_nodes_count(&node, &runtime_config, receipts, results, true);
}

/// This test is similar to `test_chunk_nodes_cache_common_parent` but checks another trie structure:
///
///                                                     --> (Leaf) -> (Value 1)
/// (Extension) -> (Branch) --> (Extension) -> (Branch) |-> (Leaf) -> (Value 2)
///                         |-> (Leaf) -> (Value 2)
///
/// Here we check that chunk cache is enabled *only during function calls execution*.
/// 1st receipt writes `Value 1` and should count 6 db reads.
/// 2nd receipt deploys a new contract which *code* is the same as `Value 2`. But this value shouldn't be put into the
/// chunk cache.
/// 3rd receipt writes `Value 2` and should count 2 db and 4 memory reads.
///
/// We have checked manually that if chunk cache mode is not disabled, then the following scenario happens:
/// - 1st receipt enables chunk cache mode but doesn't disable it
/// - 2nd receipt triggers insertion of `Value 2` into the chunk cache
/// - 3rd receipt reads it from the chunk cache, so it incorrectly charges user for 1 db and 5 memory reads.
pub fn test_chunk_nodes_cache_mode(node: impl Node, runtime_config: RuntimeConfig) {
    let receipts: Vec<Receipt> = vec![
        make_receipt(&node, vec![make_write_key_value_action(vec![1], vec![1])], bob_account()),
        make_receipt(
            &node,
            vec![DeployContractAction { code: test_utils::encode(&[2]) }.into()],
            alice_account(),
        ),
        make_receipt(&node, vec![make_write_key_value_action(vec![2], vec![2])], bob_account()),
    ];

    let results = vec![
        TrieNodesCount { db_reads: 6, mem_reads: 0 },
        TrieNodesCount { db_reads: 0, mem_reads: 0 },
        TrieNodesCount { db_reads: 2, mem_reads: 4 },
    ];
    check_trie_nodes_count(&node, &runtime_config, receipts, results, true);
}

/// Checks costs for subsequent `storage_read` and `storage_write` for the same key.
/// Depth for the storage key is 4.
/// Without flat storage, cost for reading Value is skipped due to a bug, so we count
/// 3 DB reads during read. During write, all nodes get cached, so we count 4 mem reads.
/// With flat storage, neither DB nor memory reads should be counted, as we skip Trie
/// node reads and don't charge for Value read due to the same bug. For write we go
/// to Trie and count 3 DB node reads and 1 mem read for Value.
pub fn test_storage_read_write_costs(node: impl Node, runtime_config: RuntimeConfig) {
    let receipts: Vec<Receipt> = vec![
        make_receipt(
            &node,
            vec![FunctionCallAction {
                args: test_utils::encode(&[1]),
                method_name: "read_value".to_string(),
                gas: 10u64.pow(14),
                deposit: 0,
            }
            .into()],
            bob_account(),
        ),
        make_receipt(&node, vec![make_write_key_value_action(vec![1], vec![20])], bob_account()),
    ];

    let results = vec![
        TrieNodesCount { db_reads: 3, mem_reads: 0 },
        TrieNodesCount { db_reads: 0, mem_reads: 4 },
    ];
    check_trie_nodes_count(&node, &runtime_config, receipts.clone(), results, false);

    let results = vec![
        TrieNodesCount { db_reads: 0, mem_reads: 0 },
        TrieNodesCount { db_reads: 3, mem_reads: 1 },
    ];
    check_trie_nodes_count(&node, &runtime_config, receipts, results, true);
}
