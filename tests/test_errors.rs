use std::sync::Arc;

use near::config::{TESTING_INIT_BALANCE, TESTING_INIT_STAKE};
use near::{load_test_config, GenesisConfig};
use near_crypto::{InMemorySigner, KeyType};
use near_network::test_utils::open_port;
use near_primitives::account::AccessKey;
use near_primitives::errors::{InvalidAccessKeyError, InvalidTxError};
use near_primitives::test_utils::init_integration_logger;
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, SignedTransaction, TransferAction,
};
use testlib::node::{Node, ThreadNode};

fn start_node() -> ThreadNode {
    init_integration_logger();
    let genesis_config = GenesisConfig::test(vec!["alice.near", "bob.near"], 1);
    let mut near_config = load_test_config("alice.near", open_port(), &genesis_config);
    near_config.client_config.skip_sync_wait = true;

    let mut node = ThreadNode::new(near_config);
    node.start();
    node
}

#[test]
fn test_check_tx_error_log() {
    let node = start_node();
    let signer = Arc::new(InMemorySigner::from_seed("alice.near", KeyType::ED25519, "alice.near"));
    let block_hash = node.user().get_best_block_hash().unwrap();
    let tx = SignedTransaction::from_actions(
        1,
        "bob.near".to_string(),
        "test.near".to_string(),
        &*signer,
        vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::Transfer(TransferAction { deposit: 1_000 }),
            Action::AddKey(AddKeyAction {
                public_key: signer.public_key.clone(),
                access_key: AccessKey::full_access(),
            }),
        ],
        block_hash,
    );

    let tx_result = node.user().commit_transaction(tx).unwrap_err();
    assert_eq!(
        tx_result,
        format!(
            "{}",
            InvalidTxError::InvalidAccessKey(InvalidAccessKeyError::AccessKeyNotFound(
                "bob.near".to_string(),
                signer.public_key.clone()
            ))
        ),
    );
}

#[test]
fn test_deliver_tx_error_log() {
    let node = start_node();
    let fee_helper = testlib::fees_utils::FeeHelper::new(
        node.genesis_config().runtime_config.transaction_costs.clone(),
        node.genesis_config().gas_price,
    );
    let signer = Arc::new(InMemorySigner::from_seed("alice.near", KeyType::ED25519, "alice.near"));
    let block_hash = node.user().get_best_block_hash().unwrap();
    let cost = fee_helper.create_account_transfer_full_key_cost_no_reward();
    let tx = SignedTransaction::from_actions(
        1,
        "alice.near".to_string(),
        "test.near".to_string(),
        &*signer,
        vec![
            Action::CreateAccount(CreateAccountAction {}),
            Action::Transfer(TransferAction { deposit: TESTING_INIT_BALANCE + 1 }),
            Action::AddKey(AddKeyAction {
                public_key: signer.public_key.clone(),
                access_key: AccessKey::full_access(),
            }),
        ],
        block_hash,
    );

    let tx_result = node.user().commit_transaction(tx).unwrap_err();
    assert_eq!(
        tx_result,
        format!(
            "{}",
            InvalidTxError::NotEnoughBalance(
                "alice.near".to_string(),
                TESTING_INIT_BALANCE - TESTING_INIT_STAKE,
                TESTING_INIT_BALANCE + 1 + cost
            )
        ),
    );
}
