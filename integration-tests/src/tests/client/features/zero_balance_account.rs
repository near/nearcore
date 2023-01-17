use assert_matches::assert_matches;

use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::adapter::ProcessTxResponse;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::account::id::AccountId;
use near_primitives::errors::{ActionError, ActionErrorKind, InvalidTxError, TxExecutionError};
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::{AddKeyAction, SignedTransaction};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::{FinalExecutionStatus, QueryRequest, QueryResponseKind};
use nearcore::config::GenesisExt;

use crate::tests::client::runtimes::create_nightshade_runtimes;
use near_primitives::account::AccessKey;
use near_primitives::config::ActionCosts;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::transaction::Action::AddKey;
use near_primitives::types::Balance;

/// Test 2 things: 1) a valid zero balance account can be created and 2) a nonzero balance account
/// (one with a contract deployed) cannot be created without maintaining an initial balance
#[cfg(feature = "nightly_protocol")]
#[test]
fn test_zero_balance_account_creation() {
    let epoch_length = 1000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = ProtocolFeature::ZeroBalanceAccount.protocol_version();
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let new_account_id: AccountId = "hello.test0".parse().unwrap();
    let signer0 = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let new_signer =
        InMemorySigner::from_seed(new_account_id.clone(), KeyType::ED25519, "hello.test0");

    // create a valid zero balance account. Transaction should succeed
    let create_account_tx = SignedTransaction::create_account(
        1,
        signer0.account_id.clone(),
        new_account_id.clone(),
        0,
        new_signer.public_key.clone(),
        &signer0,
        *genesis_block.hash(),
    );
    let res = env.clients[0].process_tx(create_account_tx, false, false);
    assert_matches!(res, ProcessTxResponse::ValidTx);
    for i in 1..5 {
        env.produce_block(0, i);
    }
    // new account should have been created
    let head = env.clients[0].chain.head().unwrap();
    let head_block = env.clients[0].chain.get_block(&head.last_block_hash).unwrap();
    let response = env.clients[0]
        .runtime_adapter
        .query(
            ShardUId::single_shard(),
            &head_block.chunks()[0].prev_state_root(),
            head.height,
            0,
            &head.prev_block_hash,
            &head.last_block_hash,
            head_block.header().epoch_id(),
            &QueryRequest::ViewAccount { account_id: new_account_id.clone() },
        )
        .unwrap();
    match response.kind {
        QueryResponseKind::ViewAccount(view) => {
            assert_eq!(view.amount, 0);
        }
        _ => panic!("wrong query response"),
    }

    // create a zero balance account with contract deployed. The transaction should fail
    let new_account_id: AccountId = "hell.test0".parse().unwrap();
    let create_account_tx = SignedTransaction::create_contract(
        2,
        signer0.account_id.clone(),
        new_account_id.clone(),
        vec![1, 2, 3],
        0,
        new_signer.public_key.clone(),
        &signer0,
        *genesis_block.hash(),
    );
    let tx_hash = create_account_tx.get_hash();
    let res = env.clients[0].process_tx(create_account_tx, false, false);
    assert_matches!(res, ProcessTxResponse::ValidTx);
    for i in 5..10 {
        env.produce_block(0, i);
    }
    let outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::LackBalanceForState { .. },
            ..
        }))
    );
}

/// Test that if a zero balance account becomes a regular account (through adding more keys),
/// it has to pay for storage cost of the account structure and the keys that
/// it didn't have to pay while it was a zero balance account.
#[cfg(feature = "nightly_protocol")]
#[test]
fn test_zero_balance_account_add_key() {
    let epoch_length = 1000;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = ProtocolFeature::ZeroBalanceAccount.protocol_version();
    let min_gas_price = genesis.config.min_gas_price;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let new_account_id: AccountId = "hello.test0".parse().unwrap();
    let signer0 = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let new_signer =
        InMemorySigner::from_seed(new_account_id.clone(), KeyType::ED25519, "hello.test0");

    let amount = 10u128.pow(24);
    let create_account_tx = SignedTransaction::create_account(
        1,
        signer0.account_id.clone(),
        new_account_id.clone(),
        amount,
        new_signer.public_key.clone(),
        &signer0,
        *genesis_block.hash(),
    );
    let res = env.clients[0].process_tx(create_account_tx, false, false);
    assert_matches!(res, ProcessTxResponse::ValidTx);
    for i in 1..5 {
        env.produce_block(0, i);
    }

    // add two more keys so that the account is no longer a zero balance account
    let new_key1 = PublicKey::from_seed(KeyType::ED25519, "random1");
    let new_key2 = PublicKey::from_seed(KeyType::ED25519, "random2");

    let head = env.clients[0].chain.head().unwrap();
    let add_key_tx = SignedTransaction::from_actions(
        head.height * AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER + 1,
        new_account_id.clone(),
        new_account_id.clone(),
        &new_signer,
        vec![
            AddKey(AddKeyAction { public_key: new_key1, access_key: AccessKey::full_access() }),
            AddKey(AddKeyAction { public_key: new_key2, access_key: AccessKey::full_access() }),
        ],
        *genesis_block.hash(),
    );
    let res = env.clients[0].process_tx(add_key_tx, false, false);
    assert_matches!(res, ProcessTxResponse::ValidTx);
    for i in 5..10 {
        env.produce_block(0, i);
    }

    // since the account is no longer zero balance account, it cannot transfer all its tokens out
    // and must keep some amount for storage staking
    let transaction_costs = RuntimeConfig::test().fees;
    let send_money_total_gas = transaction_costs.fee(ActionCosts::transfer).send_fee(false)
        + transaction_costs.fee(ActionCosts::new_action_receipt).send_fee(false)
        + transaction_costs.fee(ActionCosts::transfer).exec_fee()
        + transaction_costs.fee(ActionCosts::new_action_receipt).exec_fee();
    let total_cost = send_money_total_gas as Balance * min_gas_price;

    let head = env.clients[0].chain.head().unwrap();
    let send_money_tx = SignedTransaction::send_money(
        head.height * AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER + 1,
        new_account_id.clone(),
        signer0.account_id.clone(),
        &new_signer,
        amount - total_cost,
        *genesis_block.hash(),
    );
    let res = env.clients[0].process_tx(send_money_tx, false, false);
    assert_matches!(res, ProcessTxResponse::InvalidTx(InvalidTxError::LackBalanceForState { .. }));
}

/// Test that zero balance accounts cannot be created before the upgrade but can succeed after
/// the protocol upgrade
#[cfg(feature = "nightly_protocol")]
#[test]
fn test_zero_balance_account_upgrade() {
    let epoch_length = 5;
    let mut genesis = Genesis::test(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], 1);
    genesis.config.epoch_length = epoch_length;
    genesis.config.protocol_version = ProtocolFeature::ZeroBalanceAccount.protocol_version() - 1;
    let mut env = TestEnv::builder(ChainGenesis::test())
        .runtime_adapters(create_nightshade_runtimes(&genesis, 1))
        .build();
    let genesis_block = env.clients[0].chain.get_block_by_height(0).unwrap();

    let new_account_id: AccountId = "hello.test0".parse().unwrap();
    let signer0 = InMemorySigner::from_seed("test0".parse().unwrap(), KeyType::ED25519, "test0");
    let new_signer =
        InMemorySigner::from_seed(new_account_id.clone(), KeyType::ED25519, "hello.test0");

    // before protocol upgrade, should not be possible to create a zero balance account
    let create_account_tx = SignedTransaction::create_account(
        1,
        signer0.account_id.clone(),
        new_account_id.clone(),
        0,
        new_signer.public_key.clone(),
        &signer0,
        *genesis_block.hash(),
    );
    let tx_hash = create_account_tx.get_hash();
    let res = env.clients[0].process_tx(create_account_tx, false, false);
    assert_matches!(res, ProcessTxResponse::ValidTx);
    for i in 1..12 {
        env.produce_block(0, i);
    }
    let outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash).unwrap();
    assert_matches!(
        outcome.status,
        FinalExecutionStatus::Failure(TxExecutionError::ActionError(ActionError {
            kind: ActionErrorKind::LackBalanceForState { .. },
            ..
        }))
    );
    let create_account_tx2 = SignedTransaction::create_account(
        2,
        signer0.account_id.clone(),
        new_account_id.clone(),
        0,
        new_signer.public_key.clone(),
        &signer0,
        *genesis_block.hash(),
    );
    let tx_hash2 = create_account_tx2.get_hash();
    let res = env.clients[0].process_tx(create_account_tx2, false, false);
    assert_matches!(res, ProcessTxResponse::ValidTx);
    for i in 12..20 {
        env.produce_block(0, i);
    }
    let outcome = env.clients[0].chain.get_final_transaction_result(&tx_hash2).unwrap();
    assert_matches!(outcome.status, FinalExecutionStatus::SuccessValue(_));
}
