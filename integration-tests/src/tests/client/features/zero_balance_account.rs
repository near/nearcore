use std::path::Path;
use std::sync::Arc;

use assert_matches::assert_matches;

use near_chain::ChainGenesis;
use near_chain_configs::Genesis;
use near_client::adapter::ProcessTxResponse;
use near_client::test_utils::TestEnv;
use near_crypto::{InMemorySigner, KeyType, PublicKey};
use near_primitives::account::id::AccountId;
use near_primitives::account::AccessKey;
use near_primitives::config::ExtCostsConfig;
use near_primitives::errors::{ActionError, ActionErrorKind, InvalidTxError, TxExecutionError};
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::fees::StorageUsageConfig;
use near_primitives::shard_layout::ShardUId;
use near_primitives::transaction::Action::AddKey;
use near_primitives::transaction::{Action, AddKeyAction, DeleteKeyAction, SignedTransaction};
use near_primitives::version::ProtocolFeature;
use near_primitives::views::{FinalExecutionStatus, QueryRequest, QueryResponseKind};
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use nearcore::{NightshadeRuntime, TrackedConfig};

use crate::tests::client::runtimes::create_nightshade_runtimes;

/// Assert that an account exists and has zero balance
fn assert_zero_balance_account(env: &mut TestEnv, account_id: &AccountId) {
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
            &QueryRequest::ViewAccount { account_id: account_id.clone() },
        )
        .unwrap();
    match response.kind {
        QueryResponseKind::ViewAccount(view) => {
            assert_eq!(view.amount, 0);
        }
        _ => panic!("wrong query response"),
    }
}

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
    assert_zero_balance_account(&mut env, &new_account_id);

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
    // create free runtime config for transaction costs to make it easier to assert
    // the exact amount of tokens on accounts
    let mut runtime_config = RuntimeConfig::free();
    runtime_config.fees.storage_usage_config = StorageUsageConfig {
        storage_amount_per_byte: 10u128.pow(19),
        num_bytes_account: 100,
        num_extra_bytes_record: 40,
    };
    runtime_config.wasm_config.ext_costs = ExtCostsConfig::test();
    let runtime_config_store = RuntimeConfigStore::with_one_config(runtime_config);
    let nightshade_runtime = Arc::new(NightshadeRuntime::test_with_runtime_config_store(
        Path::new("."),
        create_test_store(),
        &genesis,
        TrackedConfig::new_empty(),
        runtime_config_store,
    ));
    let mut env =
        TestEnv::builder(ChainGenesis::test()).runtime_adapters(vec![nightshade_runtime]).build();
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
    let nonce = head.height * AccessKey::ACCESS_KEY_NONCE_RANGE_MULTIPLIER + 1;
    let add_key_tx = SignedTransaction::from_actions(
        nonce,
        new_account_id.clone(),
        new_account_id.clone(),
        &new_signer,
        vec![
            AddKey(AddKeyAction { public_key: new_key1, access_key: AccessKey::full_access() }),
            AddKey(AddKeyAction {
                public_key: new_key2.clone(),
                access_key: AccessKey::full_access(),
            }),
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
    let send_money_tx = SignedTransaction::send_money(
        nonce + 10,
        new_account_id.clone(),
        signer0.account_id.clone(),
        &new_signer,
        amount,
        *genesis_block.hash(),
    );
    let res = env.clients[0].process_tx(send_money_tx.clone(), false, false);
    assert_matches!(res, ProcessTxResponse::InvalidTx(InvalidTxError::LackBalanceForState { .. }));

    let delete_key_tx = SignedTransaction::from_actions(
        nonce + 1,
        new_account_id.clone(),
        new_account_id.clone(),
        &new_signer,
        vec![Action::DeleteKey(DeleteKeyAction { public_key: new_key2 })],
        *genesis_block.hash(),
    );
    env.clients[0].process_tx(delete_key_tx, false, false);
    for i in 10..15 {
        env.produce_block(0, i);
    }
    let res = env.clients[0].process_tx(send_money_tx, false, false);
    assert_matches!(res, ProcessTxResponse::ValidTx);
    for i in 15..20 {
        env.produce_block(0, i);
    }
    assert_zero_balance_account(&mut env, &new_account_id);
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
