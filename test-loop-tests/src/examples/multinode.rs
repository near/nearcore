use near_async::time::Duration;
use near_chain_configs::test_genesis::TestEpochConfigBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{
    create_account_ids, create_validator_spec, rpc_account_id, validator_spec_clients_with_rpc,
};
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::{get_shared_block_hash, run_tx};
use crate::utils::{ONE_NEAR, retrieve_all_clients, run_for_number_of_blocks};

// Example on how to send tokens between accounts
#[test]
fn test_cross_shard_token_transfer() {
    init_test_logger();

    let boundary_accounts = create_account_ids(["account01"]).to_vec();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let user_accounts = create_account_ids(["account0", "account1"]);
    let initial_balance = 1_000_000 * ONE_NEAR;
    let validators_spec = create_validator_spec(shard_layout.num_shards() as usize, 0);
    let clients = validator_spec_clients_with_rpc(&validators_spec);
    let rpc = rpc_account_id();

    let genesis = TestLoopBuilder::new_genesis_builder()
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&user_accounts, initial_balance)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut test_loop_env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let sender_account = &user_accounts[0];
    let receiver_account = &user_accounts[1];
    let transfer_amount = 42 * ONE_NEAR;

    let block_hash =
        get_shared_block_hash(&test_loop_env.node_datas, &test_loop_env.test_loop.data);
    let nonce = 1;
    let tx = SignedTransaction::send_money(
        nonce,
        sender_account.clone(),
        receiver_account.clone(),
        &create_user_test_signer(&sender_account),
        transfer_amount,
        block_hash,
    );
    run_tx(&mut test_loop_env.test_loop, &rpc, tx, &test_loop_env.node_datas, Duration::seconds(5));
    // Run for 1 more block for the transfer to be reflected in chunks prev state root.
    run_for_number_of_blocks(&mut test_loop_env, &rpc, 1);

    let clients = retrieve_all_clients(&test_loop_env.node_datas, &test_loop_env.test_loop.data);
    assert_eq!(clients.query_balance(sender_account), initial_balance - transfer_amount);
    assert_eq!(clients.query_balance(receiver_account), initial_balance + transfer_amount);

    // Give the test a chance to finish off remaining events in the event loop, which can
    // be important for properly shutting down the nodes.
    test_loop_env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
