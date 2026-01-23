use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_o11y::testonly::init_test_logger;
use near_primitives::account::AccessKey;
use near_primitives::action::AddKeyAction;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::transaction::{Action, SignedTransaction, TransactionNonce, TransferAction};
use near_primitives::types::{AccountId, Balance, NonceIndex};
use near_primitives::views::{QueryRequest, QueryResponseKind};

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::{
    create_validators_spec, rpc_account_id, validators_spec_clients_with_rpc,
};
use crate::utils::client_queries::ClientQueries;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{check_txs, get_shared_block_hash};

/// Get the nonce for a gas key with specific nonce_index.
fn get_gas_key_nonce(
    env: &TestLoopEnv,
    account_id: &AccountId,
    public_key: &PublicKey,
    nonce_index: NonceIndex,
) -> u64 {
    let clients = env
        .node_datas
        .iter()
        .map(|data| &env.test_loop.data.get(&data.client_sender.actor_handle()).client)
        .collect_vec();
    let response = clients.runtime_query(
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
    let accounts: Vec<AccountId> =
        (0..4).map(|i| format!("account{}", i).parse().unwrap()).collect();

    let shard_layout = ShardLayout::single_shard();
    let validators_spec = create_validators_spec(shard_layout.num_shards() as usize, 0);
    let clients = validators_spec_clients_with_rpc(&validators_spec);
    let initial_balance = Balance::from_near(1_000_000);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, initial_balance)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .track_all_shards()
        .build()
        .warmup();

    let sender = &accounts[0];
    let receiver = &accounts[1];

    let gas_key_signer: Signer =
        InMemorySigner::from_seed(sender.clone(), KeyType::ED25519, "gas_key").into();
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    let num_nonces = 3; // Arbitrary number of nonces for testing
    let add_key_tx = SignedTransaction::from_actions(
        1, // nonce
        sender.clone(),
        sender.clone(),
        &near_primitives::test_utils::create_user_test_signer(sender),
        vec![Action::AddKey(Box::new(AddKeyAction {
            public_key: gas_key_signer.public_key(),
            access_key: AccessKey::gas_key_full_access(num_nonces),
        }))],
        block_hash,
        0,
    );
    let add_key_hash = add_key_tx.get_hash();
    let rpc_node = TestLoopNode::rpc(&env.node_datas);
    rpc_node.submit_tx(add_key_tx);
    env.test_loop.run_for(Duration::seconds(5));
    check_txs(&env.test_loop.data, &env.node_datas, &rpc_account_id(), &[add_key_hash]);

    // Send a transfer using the gas key
    let nonce_index = 0;
    let gas_key_nonce = get_gas_key_nonce(&env, sender, &gas_key_signer.public_key(), nonce_index);
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
    rpc_node.run_tx(&mut env.test_loop, gas_key_tx, Duration::seconds(5));
    // Run for 1 more block for the transfer to be reflected in chunks prev state root.
    rpc_node.run_for_number_of_blocks(&mut env.test_loop, 1);

    // Check that the nonce for the gas key has been incremented
    let updated_gas_key_nonce =
        get_gas_key_nonce(&env, sender, &gas_key_signer.public_key(), nonce_index);
    assert_eq!(updated_gas_key_nonce, gas_key_nonce + 1);

    // Check the balances of sender and receiver
    let sender_balance = rpc_node.view_account_query(env.test_loop_data(), sender).amount;
    assert_eq!(sender_balance, initial_balance.checked_sub(transfer_amount).unwrap());
    let receiver_balance = rpc_node.view_account_query(env.test_loop_data(), receiver).amount;
    assert_eq!(receiver_balance, initial_balance.checked_add(transfer_amount).unwrap());

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
