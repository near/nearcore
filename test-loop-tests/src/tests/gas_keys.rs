use near_async::time::Duration;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::action::{AddKeyAction, TransferToGasKeyAction};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::{Action, SignedTransaction, TransactionNonce, TransferAction};
use near_primitives::types::{AccountId, Balance, Nonce, NonceIndex};
use near_primitives::views::{QueryRequest, QueryResponseKind};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_account_ids, create_validators_spec, validators_spec_clients_with_rpc,
};
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::get_shared_block_hash;

/// Get the nonce for a gas key with specific nonce_index.
fn get_gas_key_nonce(
    env: &TestLoopEnv,
    node: &TestLoopNode<'_>,
    account_id: &AccountId,
    public_key: &PublicKey,
    nonce_index: NonceIndex,
) -> Nonce {
    let response = node.runtime_query(
        env.test_loop_data(),
        account_id,
        QueryRequest::ViewGasKeyNonces {
            account_id: account_id.clone(),
            public_key: public_key.clone(),
        },
    );
    let QueryResponseKind::GasKeyNonces(nonces) = response.kind else {
        panic!("Expected GasKeyNonces response");
    };
    nonces[nonce_index as usize]
}

#[test]
// TODO(gas-keys): Remove "nightly" once stable supports gas keys.
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(any(not(feature = "nightly"), feature = "protocol_feature_spice"), ignore)]
fn test_gas_key_transaction() {
    init_test_logger();

    let epoch_length = 10;
    let shard_layout = ShardLayout::single_shard();
    let user_accounts = create_account_ids(["account0", "account1", "account2", "account3"]);
    let initial_balance = Balance::from_near(1_000_000);
    let gas_price = Balance::from_yoctonear(1);
    let validators_spec = create_validators_spec(shard_layout.num_shards() as usize, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .gas_prices(gas_price, gas_price)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let sender = &user_accounts[0];
    let receiver = &user_accounts[1];

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, "gas_key").into();
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let num_nonces = 3; // Arbitrary number of nonces for testing
    let add_key_tx = SignedTransaction::from_actions(
        1, // nonce
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(num_nonces),
        }))],
        block_hash,
    );
    let rpc_node = TestLoopNode::rpc(&env.node_datas);
    rpc_node.run_tx(&mut env.test_loop, add_key_tx, Duration::seconds(5));
    // Run for 1 more block for the access key to be reflected in chunks prev state root.
    rpc_node.run_for_number_of_blocks(&mut env.test_loop, 1);

    // Fund the gas key
    let gas_key_fund_amount = Balance::from_near(100);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let fund_tx = SignedTransaction::from_actions(
        2, // nonce
        sender.clone(),
        sender.clone(),
        &create_user_test_signer(sender),
        vec![Action::TransferToGasKey(Box::new(TransferToGasKeyAction {
            public_key: gas_key_signer.public_key(),
            deposit: gas_key_fund_amount,
        }))],
        block_hash,
    );
    rpc_node.run_tx(&mut env.test_loop, fund_tx, Duration::seconds(5));
    rpc_node.run_for_number_of_blocks(&mut env.test_loop, 1);

    // Record balances before the gas key transaction
    let sender_balance_before = rpc_node.view_account_query(env.test_loop_data(), sender).amount;
    let gas_key_info_before = rpc_node.runtime_query(
        env.test_loop_data(),
        sender,
        QueryRequest::ViewAccessKey {
            account_id: sender.clone(),
            public_key: gas_key_signer.public_key(),
        },
    );
    let QueryResponseKind::AccessKey(access_key_view_before) = gas_key_info_before.kind else {
        panic!("expected AccessKey response");
    };

    // Send a transfer using the gas key
    let nonce_index = 0;
    let gas_key_nonce =
        get_gas_key_nonce(&env, &rpc_node, sender, &gas_key_signer.public_key(), nonce_index);
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let transfer_amount = Balance::from_near(10);
    let gas_key_tx = SignedTransaction::from_actions_v1(
        TransactionNonce::from_nonce_and_index(gas_key_nonce + 1, nonce_index),
        sender.clone(),
        receiver.clone(),
        &gas_key_signer,
        vec![Action::Transfer(TransferAction { deposit: transfer_amount })],
        block_hash,
    );
    let outcome =
        rpc_node.execute_tx(&mut env.test_loop, gas_key_tx, Duration::seconds(5)).unwrap();
    // Run for 1 more block for the transfer to be reflected in chunks prev state root.
    rpc_node.run_for_number_of_blocks(&mut env.test_loop, 1);

    // Check that the nonce for the gas key has been incremented
    let updated_gas_key_nonce =
        get_gas_key_nonce(&env, &rpc_node, sender, &gas_key_signer.public_key(), nonce_index);
    assert_eq!(updated_gas_key_nonce, gas_key_nonce + 1);

    // Verify account balance pays for deposit, gas key balance pays for gas.
    let sender_balance_after = rpc_node.view_account_query(env.test_loop_data(), sender).amount;
    assert_eq!(sender_balance_after, sender_balance_before.checked_sub(transfer_amount).unwrap());
    let gas_key_info_after = rpc_node.runtime_query(
        env.test_loop_data(),
        sender,
        QueryRequest::ViewAccessKey {
            account_id: sender.clone(),
            public_key: gas_key_signer.public_key(),
        },
    );
    let QueryResponseKind::AccessKey(access_key_view_after) = gas_key_info_after.kind else {
        panic!("expected AccessKey response");
    };
    let gas_cost = outcome.total_tokens_burnt();
    assert!(!gas_cost.is_zero());
    let gas_key_balance_before = access_key_view_before.permission.gas_key_balance().unwrap();
    let gas_key_balance_after = access_key_view_after.permission.gas_key_balance().unwrap();
    assert_eq!(gas_key_balance_after, gas_key_balance_before.checked_sub(gas_cost).unwrap());

    // Verify receiver got the transfer
    let receiver_balance = rpc_node.view_account_query(env.test_loop_data(), receiver).amount;
    assert_eq!(receiver_balance, initial_balance.checked_add(transfer_amount).unwrap());

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
