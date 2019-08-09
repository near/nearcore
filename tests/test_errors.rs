use near::config::TESTING_INIT_BALANCE;
use near::{load_test_config, GenesisConfig};
use near_network::test_utils::open_port;
use near_primitives::account::AccessKey;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, SignedTransaction, TransferAction,
};
use std::sync::Arc;
use testlib::node::{Node, ThreadNode};

fn start_node() -> ThreadNode {
    init_integration_logger();
    let genesis_config = GenesisConfig::legacy_test(vec!["alice.near", "bob.near"], 1);
    let mut near_config = load_test_config("alice.near", open_port(), &genesis_config);
    near_config.client_config.skip_sync_wait = true;

    let mut node = ThreadNode::new(near_config);
    node.start();
    node
}

#[test]
fn test_check_tx_error_log() {
    let node = start_node();
    let signer = Arc::new(InMemorySigner::from_seed("alice.near", "alice.near"));
    let tx = SignedTransaction::from_actions(
        1,
        "bob.near".to_string(),
        "test.near".to_string(),
        signer.clone(),
        vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::Transfer(TransferAction { deposit: 1_000 }),
            Action::AddKey(AddKeyAction {
                public_key: signer.public_key.clone(),
                access_key: AccessKey {
                    amount: 0,
                    balance_owner: None,
                    contract_id: None,
                    method_name: None,
                },
            }),
        ],
    );

    let tx_result = node.user().commit_transaction(tx);
    assert_eq!(
        tx_result,
        Err("RpcError { code: -32000, message: \"Server error\", data: Some(String(\"Transaction is not signed with a public key of the signer \\\"bob.near\\\"\")) }".to_string())
    );
}

#[test]
fn test_deliver_tx_error_log() {
    let node = start_node();
    let signer = Arc::new(InMemorySigner::from_seed("alice.near", "alice.near"));
    let tx = SignedTransaction::from_actions(
        1,
        "alice.near".to_string(),
        "test.near".to_string(),
        signer.clone(),
        vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::Transfer(TransferAction { deposit: TESTING_INIT_BALANCE + 1 }),
            Action::AddKey(AddKeyAction {
                public_key: signer.public_key.clone(),
                access_key: AccessKey {
                    amount: 0,
                    balance_owner: None,
                    contract_id: None,
                    method_name: None,
                },
            }),
        ],
    );

    let tx_result = node.user().commit_transaction(tx).unwrap();
    assert_eq!(
        tx_result.transactions[0].result.logs[0],
        "Runtime error: Sender does not have enough balance 999999950000000 for operation costing 1000000000000001"
    );
}
