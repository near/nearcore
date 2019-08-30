use std::sync::Arc;

use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_crypto::{InMemorySigner, KeyType};
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::hash::hash;
use near_primitives::types::Balance;
use near_primitives::views::AccountView;
use near_primitives::views::FinalTransactionStatus;

use crate::fees_utils::*;
use crate::node::Node;
use crate::runtime_utils::{
    alice_account, bob_account, default_code_hash, encode_int, eve_dot_alice_account,
};
use crate::user::User;

/// The amount to send with function call.
const FUNCTION_CALL_AMOUNT: Balance = 1_000_000_000_000;

/// Adds given access key to the given account_id using signer2.
fn add_access_key(
    node: &impl Node,
    node_user: &dyn User,
    access_key: &AccessKey,
    signer2: &InMemorySigner,
) {
    let root = node_user.get_state_root();
    let account_id = &node.account_id().unwrap();
    let transaction_result =
        node_user.add_key(account_id.clone(), signer2.public_key.clone(), access_key.clone());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_simple(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(alice_account(), bob_account(), "run_test", vec![], 1000000, 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_self_call(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user.function_call(
        account_id.clone(),
        account_id.clone(),
        "run_test",
        vec![],
        1000000,
        0,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_bad_method_name(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(account_id.clone(), bob_account(), "_run_test", vec![], 1000000, 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_empty_method_name_with_no_tokens(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(account_id.clone(), bob_account(), "", vec![], 1000000, 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_empty_method_name_with_tokens(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(account_id.clone(), bob_account(), "", vec![], 1000000, 10);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_smart_contract_with_args(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user.function_call(
        account_id.clone(),
        bob_account(),
        "run_test",
        (2..4).flat_map(|x| encode_int(x).to_vec()).collect(),
        1000000,
        0,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_async_call_with_logs(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user.function_call(
        account_id.clone(),
        bob_account(),
        "log_something",
        vec![],
        1000000,
        0,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(transaction_result.transactions[1].result.logs[0], "LOG: hello".to_string());
}

pub fn test_nonce_update_when_deploying_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let wasm_binary = b"test_binary";
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user.deploy_contract(account_id.clone(), wasm_binary.to_vec());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_nonce_updated_when_tx_failed(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.send_money(account_id.clone(), bob_account(), TESTING_INIT_BALANCE + 1);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 0);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

pub fn test_upload_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user.create_account(
        account_id.clone(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        10000,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let wasm_binary = b"test_binary";
    let transaction_result =
        node_user.deploy_contract(eve_dot_alice_account(), wasm_binary.to_vec());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
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
    let transaction_result = node_user.deploy_contract(account_id.clone(), test_binary.to_vec());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.code_hash, hash(test_binary).into());
}

pub fn test_send_money(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transfer_cost = transfer_cost();
    let transaction_result = node_user.send_money(account_id.clone(), bob_account(), money_used);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountView {
            amount: TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE - transfer_cost,
            staked: TESTING_INIT_STAKE,
            code_hash: default_code_hash().into(),
            storage_paid_at: 0,
            storage_usage: 254500,
        }
    );
    let result2 = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(
        result2,
        AccountView {
            amount: TESTING_INIT_BALANCE + money_used - TESTING_INIT_STAKE,
            staked: TESTING_INIT_STAKE,
            code_hash: default_code_hash().into(),
            storage_paid_at: 0,
            storage_usage: 254500,
        }
    );
}

pub fn test_send_money_over_balance(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = TESTING_INIT_BALANCE + 1;
    let transaction_result = node_user.send_money(account_id.clone(), bob_account(), money_used);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.staked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
    );
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 0);

    let result2 = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(
        (result2.amount, result2.staked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
    );
}

pub fn test_refund_on_send_money_to_non_existent_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    // Successful atomic transfer has the same cost as failed atomic transfer.
    let transfer_cost = transfer_cost();
    let transaction_result =
        node_user.send_money(account_id.clone(), eve_dot_alice_account(), money_used);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.staked),
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
    let money_used = 10;
    let transaction_result = node_user.create_account(
        account_id.clone(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        money_used,
    );

    let create_account_cost = create_account_transfer_full_key_cost();

    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.staked),
        (
            TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE - create_account_cost,
            TESTING_INIT_STAKE
        )
    );

    let result2 = node_user.view_account(&eve_dot_alice_account()).unwrap();
    assert_eq!((result2.amount, result2.staked), (money_used, 0));
}

pub fn test_create_account_again(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    node_user.create_account(
        account_id.clone(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        money_used,
    );
    let create_account_cost = create_account_transfer_full_key_cost();

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.staked),
        (
            TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE - create_account_cost,
            TESTING_INIT_STAKE
        )
    );
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result2 = node_user.view_account(&eve_dot_alice_account()).unwrap();
    assert_eq!((result2.amount, result2.staked), (money_used, 0));

    let transaction_result = node_user.create_account(
        account_id.clone(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        money_used,
    );

    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 2);

    // Additional cost for trying to create an account with repeated name. Will fail after
    // the first action.
    let additional_cost = create_account_transfer_full_key_cost_fail_on_create_account();

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.staked),
        (
            TESTING_INIT_BALANCE
                - money_used
                - TESTING_INIT_STAKE
                - create_account_cost
                - additional_cost,
            TESTING_INIT_STAKE
        )
    );
}

pub fn test_create_account_failure_invalid_name(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    for invalid_account_name in [
        "e",                                                                 // too short
        "Alice.near",                                                        // capital letter
        "alice(near)",                                                       // brackets are invalid
        "qq@qq*qq",                                                          // * is invalid
        "01234567890123456789012345678901234567890123456789012345678901234", // too long
    ]
    .iter()
    {
        let transaction_result = node_user.create_account(
            account_id.clone(),
            invalid_account_name.to_string(),
            node.signer().public_key(),
            money_used,
        );

        assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
        assert_eq!(transaction_result.transactions.len(), 1);
        assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 0);
        let new_root = node_user.get_state_root();
        assert_eq!(root, new_root);
        let result1 = node_user.view_account(account_id).unwrap();
        assert_eq!(
            (result1.amount, result1.staked),
            (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
        );
    }
}

pub fn test_create_account_failure_already_exists(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;

    let transaction_result = node_user.create_account(
        account_id.clone(),
        bob_account(),
        node.signer().public_key(),
        money_used,
    );
    let create_account_cost = create_account_transfer_full_key_cost_fail_on_create_account();
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    assert_eq!(node_user.get_access_key_nonce_for_signer(account_id).unwrap(), 1);

    let result1 = node_user.view_account(account_id).unwrap();
    assert_eq!(
        (result1.amount, result1.staked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE - create_account_cost, TESTING_INIT_STAKE)
    );

    let result2 = node_user.view_account(&bob_account()).unwrap();
    assert_eq!(
        (result2.amount, result2.staked),
        (TESTING_INIT_BALANCE - TESTING_INIT_STAKE, TESTING_INIT_STAKE)
    );
}

pub fn test_swap_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    node_user.create_account(
        account_id.clone(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        money_used,
    );
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let transaction_result = node_user.swap_key(
        eve_dot_alice_account(),
        node.signer().public_key(),
        signer2.public_key.clone(),
        AccessKey::full_access(),
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root1 = node_user.get_state_root();
    assert_ne!(new_root, new_root1);

    assert!(node_user
        .get_access_key(&eve_dot_alice_account(), &node.signer().public_key())
        .unwrap()
        .is_none());
    assert!(node_user
        .get_access_key(&eve_dot_alice_account(), &signer2.public_key)
        .unwrap()
        .is_some());
}

pub fn test_add_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let node_user = node.user();

    add_access_key(&node, node_user.as_ref(), &AccessKey::full_access(), &signer2);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_some());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).unwrap().is_some());
}

pub fn test_add_existing_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.add_key(account_id.clone(), node.signer().public_key(), AccessKey::full_access());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_some());
}

pub fn test_delete_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let node_user = node.user();
    add_access_key(&node, node_user.as_ref(), &AccessKey::full_access(), &signer2);
    let root = node_user.get_state_root();
    let transaction_result = node_user.delete_key(account_id.clone(), node.signer().public_key());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_none());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).unwrap().is_some());
}

pub fn test_delete_key_not_owned(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random("".to_string(), KeyType::ED25519);
    let node_user = node.user();
    let root = node_user.get_state_root();

    let transaction_result = node_user.delete_key(account_id.clone(), signer2.public_key.clone());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_some());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).unwrap().is_none());
}

pub fn test_delete_key_last(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();

    let transaction_result = node_user.delete_key(account_id.clone(), node.signer().public_key());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_none());
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

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_some());

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, Some(access_key.into()));
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

    let root = node_user.get_state_root();
    let transaction_result = node_user.delete_key(account_id.clone(), signer2.public_key.clone());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_some());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).unwrap().is_none());
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
    let add_access_key_cost = add_key_cost(0);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, initial_balance - add_access_key_cost);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_some());
    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, Some(access_key.into()));
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
    let add_access_key_cost = add_key_cost(0);
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let root = node_user.get_state_root();
    let delete_access_key_cost = delete_key_cost();
    let transaction_result = node_user.delete_key(account_id.clone(), signer2.public_key.clone());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, initial_balance - add_access_key_cost - delete_access_key_cost);

    assert!(node_user.get_access_key(&account_id, &node.signer().public_key()).unwrap().is_some());
    assert!(node_user.get_access_key(&account_id, &signer2.public_key).unwrap().is_none());
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
    let function_call_cost = function_call_cost(method_name.as_bytes().len() as u64);
    let gas = 1000000;
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(account_id.clone(), bob_account(), method_name, vec![], gas, 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(
        view_access_key,
        Some(
            AccessKey {
                nonce: 1,
                permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                    allowance: Some(FUNCTION_CALL_AMOUNT - gas as Balance - function_call_cost),
                    receiver_id: bob_account(),
                    method_names: vec![],
                }),
            }
            .into()
        )
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

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(account_id.clone(), bob_account(), "run_test", vec![], 1000000, 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
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

    let root = node_user.get_state_root();
    let transaction_result = node_user.function_call(
        account_id.clone(),
        eve_dot_alice_account(),
        "run_test",
        vec![],
        1000000,
        0,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
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

    let root = node_user.get_state_root();
    let transaction_result = node_user.delete_key(account_id.clone(), node.signer().public_key());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

pub fn test_increase_stake(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let account_id = &node.account_id().unwrap();
    let amount_staked = TESTING_INIT_STAKE + 1;
    let stake_cost = stake_cost();
    let transaction_result =
        node_user.stake(account_id.clone(), node.signer().public_key(), amount_staked);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let node_user = node.user();
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1 - stake_cost);
    assert_eq!(account.staked, amount_staked)
}

pub fn test_decrease_stake(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let amount_staked = 10;
    let account_id = &node.account_id().unwrap();
    let transaction_result =
        node_user.stake(account_id.clone(), node.signer().public_key(), amount_staked);
    let stake_cost = stake_cost();
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - stake_cost);
    assert_eq!(account.staked, TESTING_INIT_STAKE);
}

pub fn test_unstake_while_not_staked(node: impl Node) {
    let node_user = node.user();
    let transaction_result = node_user.create_account(
        alice_account(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        10,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let transaction_result =
        node_user.stake(eve_dot_alice_account(), node.signer().public_key(), 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
}

/// Account must have enough rent to pay for next `poke_threshold` blocks.
/// `bob.near` is not wealthy enough.
pub fn test_fail_not_enough_rent(node: impl Node) {
    let mut node_user = node.user();
    let account_id = bob_account();
    let signer = Arc::new(InMemorySigner::from_seed(&account_id, KeyType::ED25519, &account_id));
    node_user.set_signer(signer);
    let transaction_result = node_user.send_money(account_id, alice_account(), 10);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
}

/// Account must have enough rent to pay for next 4 x `epoch_length` blocks (otherwise can not stake).
fn test_stake_fail_not_enough_rent_with_balance(node: impl Node, initial_balance: Balance) {
    let node_user = node.user();
    let new_account_id = "b0b_near".to_string();
    let transaction_result = node_user.create_account(
        alice_account(),
        new_account_id.clone(),
        node.signer().public_key(),
        initial_balance,
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let transaction_result = node_user.stake(new_account_id.clone(), node.signer().public_key(), 5);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
}

pub fn test_stake_fail_not_enough_rent_for_storage(node: impl Node) {
    test_stake_fail_not_enough_rent_with_balance(node, 1_000_000_000_000_000_010);
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
    let delete_account_cost = delete_account_cost();
    let transaction_result = node_user.delete_account(alice_account(), bob_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
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
    let delete_account_cost = delete_account_cost();
    let transaction_result = node_user.delete_account(alice_account(), bob_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    assert!(node.user().view_account(&bob_account()).is_ok());
    assert_eq!(
        node.user().view_account(&node.account_id().unwrap()).unwrap().amount,
        initial_amount - delete_account_cost
    );
}

pub fn test_delete_account_no_account(node: impl Node) {
    let node_user = node.user();
    let transaction_result = node_user.delete_account(alice_account(), eve_dot_alice_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
}

pub fn test_delete_account_while_staking(node: impl Node) {
    let node_user = node.user();
    let _ = node_user.create_account(
        alice_account(),
        eve_dot_alice_account(),
        node.signer().public_key(),
        10,
    );
    let _ = node_user.stake(eve_dot_alice_account(), node.signer().public_key(), 10);
    let transaction_result = node_user.delete_account(alice_account(), eve_dot_alice_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    assert!(node.user().view_account(&eve_dot_alice_account()).is_ok());
}
