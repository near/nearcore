#![cfg(feature = "test_features")] // required for adversarial behaviors

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::{get_anchor_hash, transfer_money};
use near_async::messaging::CanSend as _;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::client_actor::{AdvProduceChunksMode, NetworkAdversarialMessage};
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::types::AccountId;
use tracing::info;

/// Test that verifies partial block processing allows transactions in unaffected shards
/// to continue processing even when one shard has unavailable chunks.
///
/// This test simulates a scenario where:
/// 1. A malicious chunk producer withholds chunk parts for one shard
/// 2. Transactions in other shards should still be processed
/// 3. The blockchain continues to make progress
#[test]
fn test_partial_processing_with_transactions() {
    // Create accounts across multiple shards
    let accounts =
        (0..6).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    
    // Set up validators with the first account as the malicious chunk producer
    let chunk_producer = accounts[0].as_str();
    let validators: Vec<_> = accounts[1..4].iter().map(|a| a.as_str()).collect();
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &validators);
    
    // Create a multi-shard test environment
    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(10)
        // Use v1 shard layout with 4 shards to test cross-shard transactions
        .shard_layout(ShardLayout::v1_test())
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .build();
    
    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let mut test_loop_env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(accounts.clone())
        .build()
        .warmup();
    
    let TestLoopEnv { test_loop, node_datas, .. } = &mut test_loop_env;

    // Make the chunk producer malicious - it will withhold chunks for shard 0
    let chunk_producer = &node_datas[0];
    let data_clone = node_datas.clone();
    test_loop.send_adhoc_event("set malicious chunk production".into(), move |_| {
        data_clone[0].client_sender.send(NetworkAdversarialMessage::AdvProduceChunks(
            AdvProduceChunksMode::WithholdChunksForShard(0),
        ));
    });

    // Wait for a few blocks to ensure the malicious behavior is active
    test_loop.run_until(
        |test_loop_data| {
            let c = &test_loop_data.get(&node_datas[1].client_sender.actor_handle()).client;
            c.chain.head().unwrap().height > 10010
        },
        Duration::seconds(10),
    );

    // Send transactions between accounts in different shards (not shard 0)
    // These should continue to process despite the unavailable chunks in shard 0
    let sender = &accounts[4]; // User account in a different shard
    let receiver = &accounts[5]; // Another user account in a different shard
    
    // Record initial balances
    let clients = vec![&test_loop.data.get(&node_datas[1].client_sender.actor_handle()).client];
    let sender_initial_balance = clients.query_balance(sender);
    let receiver_initial_balance = clients.query_balance(receiver);
    
    info!(
        target: "test",
        "Initial balances - sender: {}, receiver: {}",
        sender_initial_balance, receiver_initial_balance
    );

    // Send a transaction between accounts in unaffected shards
    let signer = create_user_test_signer(sender);
    let anchor_hash = get_anchor_hash(&clients);
    let amount = 100 * ONE_NEAR;
    
    transfer_money(
        &clients,
        sender,
        receiver,
        &signer,
        amount,
        anchor_hash,
    );

    // Run the test for a while to allow transactions to be processed
    test_loop.run_until(
        |test_loop_data| {
            let c = &test_loop_data.get(&node_datas[1].client_sender.actor_handle()).client;
            c.chain.head().unwrap().height > 10020
        },
        Duration::seconds(20),
    );

    // Verify that transactions in unaffected shards were processed
    let sender_final_balance = clients.query_balance(sender);
    let receiver_final_balance = clients.query_balance(receiver);
    
    info!(
        target: "test",
        "Final balances - sender: {}, receiver: {}",
        sender_final_balance, receiver_final_balance
    );

    // Assert that the transaction was processed despite the unavailable chunks in shard 0
    assert!(
        sender_final_balance < sender_initial_balance,
        "Sender balance should have decreased"
    );
    assert!(
        receiver_final_balance > receiver_initial_balance,
        "Receiver balance should have increased"
    );
    assert_eq!(
        receiver_final_balance - receiver_initial_balance,
        amount,
        "Receiver should have received the exact amount"
    );

    // Verify that the chain made progress despite missing chunks
    for node in &node_datas[1..] {
        let client = &test_loop.data.get(&node.client_sender.actor_handle()).client;
        let head = client.chain.head().unwrap();
        assert!(head.height > 10020, "Chain should make progress despite missing chunks");
    }

    test_loop_env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
