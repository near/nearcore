use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near_primitives::account::AccessKey;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::rpc::AccountViewCallResult;
use near_primitives::transaction::FinalTransactionStatus;
use near_primitives::types::Balance;

use crate::node::Node;
use crate::runtime_utils::{
    alice_account, bob_account, default_code_hash, encode_int, eve_account,
};
use crate::user::User;
use std::sync::Arc;

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
    let wasm_binary = include_bytes!("../../../runtime/wasm/runtest/res/wasm_with_mem.wasm");
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user.deploy_contract(account_id.clone(), wasm_binary.to_vec());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    assert_eq!(node_user.get_account_nonce(account_id).unwrap(), 1);
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
    assert_eq!(node_user.get_account_nonce(account_id).unwrap(), 0);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

pub fn test_upload_contract(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result =
        node_user.create_account(account_id.clone(), eve_account(), node.signer().public_key(), 10);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);

    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let wasm_binary = include_bytes!("../../../runtime/wasm/runtest/res/wasm_with_mem.wasm");
    let transaction_result = node_user.deploy_contract(eve_account(), wasm_binary.to_vec());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let account = node_user.view_account(&eve_account()).unwrap();
    assert_eq!(account.code_hash, hash(wasm_binary));
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
    assert_eq!(account.code_hash, hash(test_binary));
}

pub fn test_send_money(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction_result = node_user.send_money(account_id.clone(), bob_account(), money_used);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
    let result2 = node_user.view_account(&bob_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: bob_account(),
            public_keys,
            amount: TESTING_INIT_BALANCE + money_used - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
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
    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 0,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
    let result2 = node_user.view_account(&bob_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: bob_account(),
            public_keys,
            amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_refund_on_send_money_to_non_existent_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction_result = node_user.send_money(account_id.clone(), eve_account(), money_used);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
    let result2 = node_user.view_account(&eve_account());
    assert!(result2.is_err());
}

pub fn test_create_account(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    let transaction_result = node_user.create_account(
        account_id.clone(),
        eve_account(),
        node.signer().public_key(),
        money_used,
    );

    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );

    let result2 = node_user.view_account(&eve_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: eve_account(),
            public_keys,
            amount: money_used,
            stake: 0,
            code_hash: CryptoHash::default(),
        }
    );
}

pub fn test_create_account_again(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    node_user.create_account(
        account_id.clone(),
        eve_account(),
        node.signer().public_key(),
        money_used,
    );

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );

    let result2 = node_user.view_account(&eve_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: eve_account(),
            public_keys,
            amount: money_used,
            stake: 0,
            code_hash: CryptoHash::default(),
        }
    );

    let transaction_result = node_user.create_account(
        account_id.clone(),
        eve_account(),
        node.signer().public_key(),
        money_used,
    );

    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 2,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - money_used - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_create_account_failure_invalid_name(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    for invalid_account_name in [
        "eve",                               // too short
        "Alice.near",                        // capital letter
        "alice(near)",                       // brackets are invalid
        "long_of_the_name_for_real_is_hard", // too long
        "qq@qq*qq",                          // * is invalid
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
        let new_root = node_user.get_state_root();
        assert_eq!(root, new_root);
        let account = node_user.view_account(account_id).unwrap();
        assert_eq!(
            account,
            AccountViewCallResult {
                nonce: 0,
                account_id: account_id.clone(),
                public_keys: vec![node.signer().public_key()],
                amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                stake: TESTING_INIT_STAKE,
                code_hash: default_code_hash(),
            }
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
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let result1 = node_user.view_account(account_id);
    assert_eq!(
        result1.unwrap(),
        AccountViewCallResult {
            nonce: 1,
            account_id: account_id.clone(),
            public_keys: vec![node.signer().public_key()],
            amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );

    let result2 = node_user.view_account(&bob_account()).unwrap();
    let public_keys = result2.public_keys.clone();
    assert_eq!(
        result2,
        AccountViewCallResult {
            nonce: 0,
            account_id: bob_account(),
            public_keys,
            amount: TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
            stake: TESTING_INIT_STAKE,
            code_hash: default_code_hash(),
        }
    );
}

pub fn test_swap_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let money_used = 10;
    node_user.create_account(
        account_id.clone(),
        eve_account(),
        node.signer().public_key(),
        money_used,
    );
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let transaction_result = node_user.swap_key(
        eve_account(),
        node.signer().public_key(),
        signer2.public_key.clone(),
        AccessKey { amount: 0, balance_owner: None, contract_id: None, method_name: None },
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root1 = node_user.get_state_root();
    assert_ne!(new_root, new_root1);

    let account = node_user.view_account(&eve_account()).unwrap();
    assert_eq!(account.public_keys, vec![signer2.public_key]);
}

pub fn test_add_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();

    add_access_key(
        &node,
        node_user.as_ref(),
        &AccessKey { amount: 0, balance_owner: None, contract_id: None, method_name: None },
        &signer2,
    );

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 2);
    assert_eq!(account.public_keys[1], signer2.public_key);
}

pub fn test_add_existing_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let node_user = node.user();
    let root = node_user.get_state_root();
    let transaction_result = node_user.add_key(
        account_id.clone(),
        node.signer().public_key(),
        AccessKey { amount: 0, balance_owner: None, contract_id: None, method_name: None },
    );
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
}

pub fn test_delete_key(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    add_access_key(
        &node,
        node_user.as_ref(),
        &AccessKey { amount: 0, balance_owner: None, contract_id: None, method_name: None },
        &signer2,
    );
    let root = node_user.get_state_root();
    let transaction_result = node_user.delete_key(account_id.clone(), node.signer().public_key());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.public_keys[0], signer2.public_key);
}

pub fn test_delete_key_not_owned(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    let node_user = node.user();
    let root = node_user.get_state_root();

    let transaction_result = node_user.delete_key(account_id.clone(), signer2.public_key.clone());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
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

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 0);
}

pub fn test_add_access_key(node: impl Node) {
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        amount: 0,
        balance_owner: None,
        contract_id: Some(account_id.clone()),
        method_name: None,
    };
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, Some(access_key));
}

pub fn test_delete_access_key(node: impl Node) {
    let node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        amount: 0,
        balance_owner: None,
        contract_id: Some(account_id.clone()),
        method_name: None,
    };
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let root = node_user.get_state_root();
    let transaction_result = node_user.delete_key(account_id.clone(), signer2.public_key.clone());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.public_keys[0], node.signer().public_key());

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, None);
}

pub fn test_add_access_key_with_funding(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        amount: 10,
        balance_owner: None,
        contract_id: Some(account_id.clone()),
        method_name: None,
    };
    let node_user = node.user();
    let signer2 = InMemorySigner::from_random();
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.amount, initial_balance - 10);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, Some(access_key));
}

pub fn test_delete_access_key_with_owner_refund(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        amount: 10,
        balance_owner: None,
        contract_id: Some(account_id.clone()),
        method_name: None,
    };
    let node_user = node.user();
    let signer2 = InMemorySigner::from_random();
    let account = node_user.view_account(account_id).unwrap();
    let initial_balance = account.amount;
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);

    let root = node_user.get_state_root();
    let transaction_result = node_user.delete_key(account_id.clone(), signer2.public_key.clone());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(new_root, root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.public_keys.len(), 1);
    assert_eq!(account.public_keys[0], node.signer().public_key());
    // Shit Happens. AccessKey should be switched to allowance and currently don't give refunds.
    assert_eq!(account.amount, initial_balance - 10);

    let view_access_key = node_user.get_access_key(account_id, &signer2.public_key).unwrap();
    assert_eq!(view_access_key, None);
}

pub fn test_access_key_smart_contract(node: impl Node) {
    let access_key = AccessKey {
        amount: FUNCTION_CALL_AMOUNT,
        balance_owner: None,
        contract_id: Some(bob_account()),
        method_name: None,
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(Arc::new(signer2));

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(account_id.clone(), bob_account(), "run_test", vec![], 1000000, 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);
}

pub fn test_access_key_smart_contract_reject_method_name(node: impl Node) {
    let access_key = AccessKey {
        amount: FUNCTION_CALL_AMOUNT,
        balance_owner: None,
        contract_id: Some(bob_account()),
        method_name: Some(b"log_something".to_vec()),
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
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
        amount: FUNCTION_CALL_AMOUNT,
        balance_owner: None,
        contract_id: Some(bob_account()),
        method_name: None,
    };
    let mut node_user = node.user();
    let account_id = &node.account_id().unwrap();
    let signer2 = InMemorySigner::from_random();
    add_access_key(&node, node_user.as_ref(), &access_key, &signer2);
    node_user.set_signer(Arc::new(signer2));

    let root = node_user.get_state_root();
    let transaction_result =
        node_user.function_call(account_id.clone(), eve_account(), "run_test", vec![], 1000000, 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
    let new_root = node_user.get_state_root();
    assert_eq!(root, new_root);
}

pub fn test_access_key_reject_non_function_call(node: impl Node) {
    let account_id = &node.account_id().unwrap();
    let access_key = AccessKey {
        amount: 0,
        balance_owner: None,
        contract_id: Some(account_id.clone()),
        method_name: None,
    };
    let mut node_user = node.user();
    let signer2 = InMemorySigner::from_random();
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
    let transaction_result =
        node_user.stake(account_id.clone(), node.signer().public_key(), amount_staked);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let node_user = node.user();
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE - 1);
    assert_eq!(account.stake, amount_staked)
}

pub fn test_decrease_stake(node: impl Node) {
    let node_user = node.user();
    let root = node_user.get_state_root();
    let amount_staked = 10;
    let account_id = &node.account_id().unwrap();
    let transaction_result =
        node_user.stake(account_id.clone(), node.signer().public_key(), amount_staked);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let new_root = node_user.get_state_root();
    assert_ne!(root, new_root);

    let account = node_user.view_account(account_id).unwrap();
    assert_eq!(account.amount, TESTING_INIT_BALANCE - TESTING_INIT_STAKE);
    assert_eq!(account.stake, TESTING_INIT_STAKE);
}

pub fn test_unstake_while_not_staked(node: impl Node) {
    let node_user = node.user();
    let transaction_result =
        node_user.create_account(alice_account(), eve_account(), node.signer().public_key(), 10);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 2);
    let transaction_result = node_user.stake(eve_account(), node.signer().public_key(), 0);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
}

/// Account must have enough rent to pay for next `poke_threshold` blocks.
pub fn test_fail_not_enough_rent(node: impl Node) {
    let node_user = node.user();
    let _ =
        node_user.create_account(alice_account(), eve_account(), node.signer().public_key(), 10);
    let transaction_result = node_user.send_money(eve_account(), alice_account(), 10);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 1);
}

/// Account must have enough rent to pay for next 4 x `epoch_length` blocks (otherwise can not stake).
pub fn test_stake_fail_not_enough_rent(node: impl Node) {
    let node_user = node.user();
    let _ = node_user.create_account(
        alice_account(),
        eve_account(),
        node.signer().public_key(),
        119_000_000_000_000_010,
    );
    let transaction_result = node_user.stake(eve_account(), node.signer().public_key(), 5);
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
}

pub fn test_delete_account(node: impl Node) {
    let node_user = node.user();
    // There is some data attached to the account.
    assert!(node_user.view_state(&bob_account()).unwrap().values.len() > 0);
    let initial_amount = node_user.view_account(&node.account_id().unwrap()).unwrap().amount;
    let bobs_amount = node_user.view_account(&bob_account()).unwrap().amount;
    let transaction_result = node_user.delete_account(alice_account(), bob_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Completed);
    assert_eq!(transaction_result.transactions.len(), 3);
    assert!(node_user.view_account(&bob_account()).is_err());
    // No data left.
    assert_eq!(node_user.view_state(&bob_account()).unwrap().values.len(), 0);
    // Receive back reward the balance of the bob's account.
    assert_eq!(
        node_user.view_account(&node.account_id().unwrap()).unwrap().amount,
        initial_amount + bobs_amount
    );
}

pub fn test_delete_account_fail(node: impl Node) {
    let node_user = node.user();
    let initial_amount = node_user.view_account(&node.account_id().unwrap()).unwrap().amount;
    let transaction_result = node_user.delete_account(alice_account(), bob_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    assert!(node.user().view_account(&bob_account()).is_ok());
    assert_eq!(
        node.user().view_account(&node.account_id().unwrap()).unwrap().amount,
        initial_amount
    );
}

pub fn test_delete_account_no_account(node: impl Node) {
    let node_user = node.user();
    let transaction_result = node_user.delete_account(alice_account(), eve_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
}

pub fn test_delete_account_while_staking(node: impl Node) {
    let node_user = node.user();
    let _ =
        node_user.create_account(alice_account(), eve_account(), node.signer().public_key(), 10);
    let _ = node_user.stake(eve_account(), node.signer().public_key(), 10);
    let transaction_result = node_user.delete_account(alice_account(), eve_account());
    assert_eq!(transaction_result.status, FinalTransactionStatus::Failed);
    assert_eq!(transaction_result.transactions.len(), 2);
    assert!(node.user().view_account(&eve_account()).is_ok());
}
