use std::sync::Arc;

use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::errors::{ActionError, InvalidAccessKeyError, InvalidTxError};
use near_primitives::hash::hash;
use near_primitives::serialize::to_base64;
use near_primitives::types::Balance;
use near_primitives::views::FinalExecutionStatus;
use near_primitives::views::{AccountView, FinalExecutionOutcomeView};

use crate::fees_utils::FeeHelper;
use crate::node::Node;
use crate::runtime_utils::{alice_account, bob_account, eve_dot_alice_account};
use crate::user::User;

use assert_matches::assert_matches;

/// The amount to send with function call.
const FUNCTION_CALL_AMOUNT: Balance = TESTING_INIT_BALANCE / 10;

fn fee_helper(node: &impl Node) -> FeeHelper {
    FeeHelper::new(
        node.genesis_config().runtime_config.transaction_costs.clone(),
        node.genesis_config().min_gas_price,
    )
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
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
        FinalExecutionStatus::SuccessValue(to_base64(&10i32.to_le_bytes()))
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
            ActionError::FunctionCallError("Smart contract panicked: WAT?".to_string()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
        FinalExecutionStatus::SuccessValue(to_base64(&10i32.to_le_bytes()))
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
            ActionError::FunctionCallError("MethodNotFound".to_string()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
            ActionError::FunctionCallError("MethodEmptyName".to_string()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
            ActionError::FunctionCallError("MethodEmptyName".to_string()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
        FinalExecutionStatus::SuccessValue(to_base64(&5u64.to_le_bytes()))
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(transaction_result.receipts[0].outcome.logs[0], "hello".to_string());
}

pub fn test_nonce_update_when_deploying_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let wasm_binary = b"test_binary";
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.deploy_contract(account_id.clone(), wasm_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let wasm_binary = b"test_binary";
    let transaction_result =
        node_user.deploy_contract(eve_dot_alice_account(), wasm_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(&eve_dot_alice_account()).unwrap();
    assert_eq!(account.code_hash, hash(wasm_binary).into());
}

pub fn test_redeploy_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let test_binary = b"test_binary";
    let transaction_result =
        node_user.deploy_contract(account_id.clone(), test_binary.to_vec()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.code_hash, hash(test_binary).into());
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
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
        FinalExecutionStatus::SuccessValue(to_base64(&10i32.to_le_bytes()))
    );
    assert_eq!(transaction_result.receipts.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let fee_helper = fee_helper(&node);
    let bob = node_user.view_account(&bob_account()).unwrap();
    let gas_burnt_for_function_call = transaction_result.receipts[0].outcome.gas_burnt
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
            ActionError::AccountDoesNotExist("Transfer".to_string(), eve_dot_alice_account())
                .into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
    let money_used = 1000;
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

    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
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
    let money_used = 1000;
    let transaction_result = node_user
        .create_account(
            account_id.clone(),
            eve_dot_alice_account(),
            node.signer().public_key(),
            money_used,
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
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
            ActionError::AccountAlreadyExists(eve_dot_alice_account()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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

pub fn test_create_account_failure_invalid_name(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let money_used = 10;
    for invalid_account_name in &[
        "e",                                                                 // too short
        "Alice.near",                                                        // capital letter
        "alice(near)",                                                       // brackets are invalid
        "qq@qq*qq",                                                          // * is invalid
        "01234567890123456789012345678901234567890123456789012345678901234", // too long
    ] {
        let transaction_result = node_user
            .create_account(
                account_id.clone(),
                invalid_account_name.to_string(),
                node.signer().public_key(),
                money_used,
            )
            .unwrap_err();
        assert_eq!(
            transaction_result,
            format!("{}", InvalidTxError::InvalidReceiver(invalid_account_name.to_string()))
        );
    }
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
        FinalExecutionStatus::Failure(ActionError::AccountAlreadyExists(bob_account()).into())
    );
    assert_eq!(transaction_result.receipts.len(), 2);
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
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root1 = node_user.get_state_root();
    assert_ne!(new_root, new_root1);

    assert!(node_user
        .get_access_key(&eve_dot_alice_account(), &node.signer().public_key())
        .is_err());
    assert!(node_user.get_access_key(&eve_dot_alice_account(), &signer2.public_key).is_ok());
}

pub fn test_add_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let node_user = node.user();

    add_access_key(&node, node_user.as_ref(), &AccessKey::full_access(), &signer2);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_ok());
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
            ActionError::AddKeyAlreadyExists(node.signer().public_key()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
}

pub fn test_delete_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let node_user = node.user();
    add_access_key(&node, node_user.as_ref(), &AccessKey::full_access(), &signer2);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_ok());

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.delete_key(account_id.clone(), node.signer().public_key()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_err());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_ok());
}

pub fn test_delete_key_not_owned(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let node_user = node.user();

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_err());

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.delete_key(account_id.clone(), signer2.public_key.clone()).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError::DeleteKeyDoesNotExist(account_id.clone()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_err());
}

pub fn test_delete_key_last(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    let transaction_result =
        node_user.delete_key(account_id.clone(), node.signer().public_key()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_err());
}

pub fn test_add_access_key_function_call(node: impl Node) {
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: None,
            receiver_id: account_id.clone(),
            method_names: vec![],
        }),
    };
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, access_key.into());
}

pub fn test_delete_access_key(node: impl Node) {
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: None,
            receiver_id: account_id.clone(),
            method_names: vec![],
        }),
    };
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_ok());

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.delete_key(account_id.clone(), signer2.public_key.clone()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_err());
}

pub fn test_add_access_key_with_allowance(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(10),
            receiver_id: account_id.clone(),
            method_names: vec![],
        }),
    };
    let node_user = node.user();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    let fee_helper = fee_helper(&node);
    let add_access_key_cost = fee_helper.add_key_cost(0);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, initial_balance - add_access_key_cost);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, access_key.into());
}

pub fn test_delete_access_key_with_allowance(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(10),
            receiver_id: account_id.clone(),
            method_names: vec![],
        }),
    };
    let node_user = node.user();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    let fee_helper = fee_helper(&node);
    let add_access_key_cost = fee_helper.add_key_cost(0);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_ok());

    let root = node_user.get_state_root();
    let delete_access_key_cost = fee_helper.delete_key_cost();
    let transaction_result =
        node_user.delete_key(account_id.clone(), signer2.public_key.clone()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, initial_balance - add_access_key_cost - delete_access_key_cost);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).is_ok());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).is_err());
}

pub fn test_access_key_smart_contract(node: impl Node) {
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(FUNCTION_CALL_AMOUNT),
            receiver_id: bob_account(),
            method_names: vec![],
        }),
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = Arc::new(InMemorySigner::from_random("".to_string(), KeyType::ED25519));
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(signer2.clone());

    let method_name = "run_test";
    let gas = 10u64.pow(14);
    let fee_helper = fee_helper(&node);
    let function_call_cost =
        fee_helper.function_call_cost(method_name.as_bytes().len() as u64, gas);
    let root = node_user.get_state_root();
    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), method_name, vec![], gas, 0)
        .unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::SuccessValue(to_base64(&10i32.to_le_bytes()))
    );
    assert_eq!(transaction_result.receipts.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(
        view_access_key,
        AccessKey {
            nonce: 1,
            permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                allowance: Some(FUNCTION_CALL_AMOUNT - function_call_cost),
                receiver_id: bob_account(),
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
            receiver_id: bob_account(),
            method_names: vec!["log_something".to_string()],
        }),
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(Arc::new(signer2));

    let transaction_result = node_user
        .function_call(account_id.clone(), bob_account(), "run_test", vec![], 10u64.pow(14), 0)
        .unwrap_err();
    assert_eq!(
        transaction_result,
        format!(
            "{}",
            InvalidTxError::InvalidAccessKey(InvalidAccessKeyError::MethodNameMismatch(
                "run_test".to_string()
            ))
        )
    );
}

pub fn test_access_key_smart_contract_reject_contract_id(node: impl Node) {
    let access_key = AccessKey {
        nonce: 0,
        permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
            allowance: Some(FUNCTION_CALL_AMOUNT),
            receiver_id: bob_account(),
            method_names: vec![],
        }),
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
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
        format!(
            "{}",
            InvalidTxError::InvalidAccessKey(InvalidAccessKeyError::ReceiverMismatch(
                eve_dot_alice_account(),
                bob_account(),
            ))
        )
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
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(Arc::new(signer2));

    let transaction_result =
        node_user.delete_key(account_id.clone(), node.signer().public_key()).unwrap_err();
    assert_eq!(
        transaction_result,
        format!("{}", InvalidTxError::InvalidAccessKey(InvalidAccessKeyError::ActionError))
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
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
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result =
        node_user.stake(eve_dot_alice_account(), node.block_signer().public_key(), 0).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(ActionError::TriesToUnstake(eve_dot_alice_account()).into())
    );
    assert_eq!(transaction_result.receipts.len(), 1);
}

/// Account must have enough rent to pay for next `poke_threshold` blocks.
/// `bob.near` is not wealthy enough.
pub fn test_fail_not_enough_rent(node: impl Node) {
    let mut node_user = node.user();
    let account_id = bob_account();
    let signer = Arc::new(InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id));
    node_user.set_signer(signer);
    node_user.send_money(account_id, alice_account(), 10).unwrap_err();
}

/// Account must have enough rent to pay for next 4 x `epoch_length` blocks (otherwise can not stake).
fn test_stake_fail_not_enough_rent_with_balance(node: impl Node, initial_balance: Balance) {
    let node_user = node.user();
    let new_account_id = "b0b_near".to_string();
    let transaction_result = node_user
        .create_account(
            alice_account(),
            new_account_id.clone(),
            node.signer().public_key(),
            initial_balance,
        )
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result =
        node_user.stake(new_account_id.clone(), node.block_signer().public_key(), 5).unwrap();
    assert_matches!(
        &transaction_result.status,
        FinalExecutionStatus::Failure(e) if e.error_type == "ActionError::RentUnpaid"
    );
    assert_eq!(transaction_result.receipts.len(), 1);
}

pub fn test_stake_fail_not_enough_rent_for_storage(node: impl Node) {
    test_stake_fail_not_enough_rent_with_balance(node, TESTING_INIT_BALANCE / 10);
}

pub fn test_stake_fail_not_enough_rent_for_account_id(node: impl Node) {
    test_stake_fail_not_enough_rent_with_balance(node, TESTING_INIT_BALANCE * 2);
}

pub fn test_delete_account_low_balance(node: impl Node) {
    let node_user = node.user();
    // There is some data attached to the account.
    assert!(node_user.view_state(&bob_account(), b"").unwrap().values.len() > 0);
    let initial_amount = node_user.view_account(&node.account_id().unwrap()).unwrap().amount;
    let bobs_amount = node_user.view_account(&bob_account()).unwrap().amount;
    let fee_helper = fee_helper(&node);
    let delete_account_cost = fee_helper.delete_account_cost();
    let transaction_result = node_user.delete_account(alice_account(), bob_account()).unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 2);
    assert!(node_user.view_account(&bob_account()).is_err());
    // No data left.
    assert_eq!(node_user.view_state(&bob_account(), b"").unwrap().values.len(), 0);
    // Receive back reward the balance of the bob's account.
    assert_eq!(
        node_user.view_account(&node.account_id().unwrap()).unwrap().amount,
        initial_amount + bobs_amount - delete_account_cost
    );
}

pub fn test_delete_account_fail(node: impl Node) {
    let node_user = node.user();
    let initial_amount = node_user.view_account(&node.account_id().unwrap()).unwrap().amount;
    let fee_helper = fee_helper(&node);
    let delete_account_cost = fee_helper.delete_account_cost();
    let transaction_result = node_user.delete_account(alice_account(), bob_account()).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(ActionError::DeleteAccountStaking(bob_account()).into())
    );
    assert_eq!(transaction_result.receipts.len(), 1);
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
            ActionError::AccountDoesNotExist("DeleteAccount".to_string(), eve_dot_alice_account())
                .into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 1);
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
    let transaction_result = node_user
        .stake(eve_dot_alice_account(), node.block_signer().public_key(), money_used - stake_fee)
        .unwrap();
    assert_eq!(transaction_result.status, FinalExecutionStatus::SuccessValue(to_base64(&[])));
    assert_eq!(transaction_result.receipts.len(), 1);
    let transaction_result =
        node_user.delete_account(alice_account(), eve_dot_alice_account()).unwrap();
    assert_eq!(
        transaction_result.status,
        FinalExecutionStatus::Failure(
            ActionError::DeleteAccountStaking(eve_dot_alice_account()).into()
        )
    );
    assert_eq!(transaction_result.receipts.len(), 1);
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
        FinalExecutionStatus::SuccessValue(to_base64(&10i32.to_le_bytes()))
    );
    assert_eq!(transaction_result.receipts.len(), 2);

    let total_gas_burnt = transaction_result.transaction.outcome.gas_burnt
        + transaction_result.receipts.iter().map(|t| t.outcome.gas_burnt).sum::<u64>();
    assert_eq!(total_gas_burnt, 0);

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}
