use near::config::TESTING_INIT_BALANCE;
use near::{load_test_config, GenesisConfig};
use near_network::test_utils::open_port;
use near_primitives::crypto::signer::InMemorySigner;
use near_primitives::test_utils::init_integration_logger;
use near_primitives::transaction::{CreateAccountTransaction, TransactionBody};
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
    let signer = InMemorySigner::from_seed("alice.near", "alice.near");
    let tx = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: 1,
        originator_id: "bob.near".to_string(),
        new_account_id: "test.near".to_string(),
        amount: 1_000,
        public_key: signer.public_key.0[..].to_vec(),
    })
    .sign(&signer);

    let tx_result = node.user().commit_transaction(tx);
    assert_eq!(
        tx_result,
        Err("RpcError { code: -32000, message: \"Server error\", data: Some(String(\"Transaction is not signed with a public key of the originator \\\"bob.near\\\"\")) }".to_string())
    );
}

#[test]
fn test_deliver_tx_error_log() {
    let node = start_node();
    let signer = InMemorySigner::from_seed("alice.near", "alice.near");
    let tx = TransactionBody::CreateAccount(CreateAccountTransaction {
        nonce: 1,
        originator_id: "alice.near".to_string(),
        new_account_id: "test.near".to_string(),
        amount: TESTING_INIT_BALANCE + 1,
        public_key: signer.public_key.0[..].to_vec(),
    })
    .sign(&signer);

    let tx_result = node.user().commit_transaction(tx).unwrap();
    assert_eq!(
        tx_result.logs[0].lines[0],
        "Runtime error: Account alice.near tries to create new account with 1000000000000001, but only has 1000000000000000"
    );
}
