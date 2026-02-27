use std::cell::RefCell;
use std::rc::Rc;

use near_async::time::Duration;
use near_epoch_manager::epoch_sync::derive_epoch_sync_proof_from_last_block;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::types::AccountId;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::adapter::StoreAdapter;
use near_store::adapter::epoch_store::EpochStoreAdapter;

use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::{create_validators_spec, validators_spec_clients};

// Test that epoch sync proof is correctly updated after each epoch.
// Validate the updated proof against derive_epoch_sync_proof_from_last_block.
#[test]
fn test_epoch_sync_proof_update() {
    // This test is only relevant when ContinuousEpochSync is enabled.
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let epoch_length = 10;
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    let epoch_store = env.validator().client().chain.chain_store.epoch_store();

    // Run for 5 epochs
    for _ in 0..5 {
        env.validator_runner().run_until_new_epoch();
    }

    // Run for another 5 epochs
    for _ in 0..5 {
        env.validator_runner().run_until_new_epoch();
        let head_hash = env.validator().head().last_block_hash;
        validate_epoch_sync_proof(&epoch_store, &head_hash);
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Test that epoch sync proof is correctly updated even in presence of forks.
#[cfg(feature = "test_features")]
#[test]
fn test_epoch_sync_proof_update_with_forks() {
    use near_client::client_actor::AdvProduceBlockHeightSelection;

    // This test is only relevant when ContinuousEpochSync is enabled.
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let epoch_length = 10;
    let validators_spec = create_validators_spec(1, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    // Run for 5 epochs
    for _ in 0..5 {
        env.validator_runner().run_until_new_epoch();
    }

    // verify if this is the first block of an epoch
    {
        let node = env.validator();
        let chain_store = node.client().chain.chain_store.chain_store();
        let head = chain_store.head().unwrap();
        let prev_block_header = chain_store.get_block_header(&head.prev_block_hash).unwrap();
        assert_ne!(&head.epoch_id, prev_block_header.epoch_id());
    }

    // create a fork
    let head = env.validator().head();
    let height_selection = AdvProduceBlockHeightSelection::NextHeightOnSelectedBlock {
        base_block_height: head.height - 1,
    };
    env.validator_mut().client_actor().adv_produce_blocks_on(3, true, height_selection);

    // verify proof after processing fork
    {
        let node = env.validator();
        let chain_store = node.client().chain.chain_store.chain_store();
        validate_epoch_sync_proof(&chain_store.epoch_store(), &head.last_block_hash);
    }

    // run for another epoch
    env.validator_runner().run_until_new_epoch();

    // verify proof
    {
        let node = env.validator();
        let chain_store = node.client().chain.chain_store.chain_store();
        let head = chain_store.head().unwrap();
        validate_epoch_sync_proof(&chain_store.epoch_store(), &head.last_block_hash);
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Test that a stale node (one that has data beyond genesis but is far behind the
// network) triggers a data reset shutdown signal when it detects it needs epoch sync.
#[test]
fn test_epoch_sync_stale_node_triggers_reset() {
    // This test is only relevant when ContinuousEpochSync is enabled.
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let epoch_length = 10;
    // Use 4 validators so 3 remaining can continue after node 0 is killed.
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    // Run all nodes to height 30 (3 epochs), then kill node 0.
    let kill_height = 3 * epoch_length;
    env.node_runner(0).run_until_head_height(kill_height);
    let node0_identifier = env.node_datas[0].identifier.clone();
    let killed_state = env.kill_node(&node0_identifier);

    // Advance the remaining nodes well past the epoch sync horizon (4 epochs * 10 = 40 blocks).
    // Node 0 is at height 30, so we need the network to be at least 30 + 40 + 1 = 71.
    let target_height = kill_height + 5 * epoch_length;
    env.node_runner(1).run_until_head_height(target_height);

    // Restart node 0 with its stale data (at height 30, network at ~80).
    let restart_id = format!("{}-restart", node0_identifier);
    env.restart_node(&restart_id, killed_state);

    // Run node 1 a bit more to give node 0 time to detect it's stale and trigger shutdown.
    // Once node 0's ClientActor detects it's stale, it sends EpochSyncDataReset on the
    // shutdown channel, which the test-loop picks up and denylists the node.
    env.node_runner(1).run_for_number_of_blocks(5);

    // The restarted node should have been denylisted (shutdown signal fired).
    // Its head should be at or near the kill height (it didn't catch up).
    let restart_node_idx = env.node_datas.len() - 1;
    let node0_head = env.node(restart_node_idx).head().height;
    assert!(
        node0_head <= kill_height + 2,
        "stale node head ({node0_head}) should be near kill height ({kill_height}), \
         indicating it was shut down for data reset rather than catching up"
    );

    tracing::info!(node0_head, "stale node data reset test passed");
    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Test that a fresh node (genesis-only) can bootstrap via epoch sync using the
// ContinuousEpochSync proof path.
#[test]
fn test_epoch_sync_bootstrap_fresh_node() {
    // This test is only relevant when ContinuousEpochSync is enabled.
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let epoch_length = 10;
    let validators_spec = create_validators_spec(4, 0);
    let clients = validators_spec_clients(&validators_spec);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .validators_spec(validators_spec)
        .build();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store_from_genesis()
        .clients(clients)
        .build()
        .warmup();

    // Run for 8 epochs (80 blocks), well past the default 4-epoch horizon (40 blocks).
    let target_height = 8 * epoch_length;
    env.node_runner(0).run_until_head_height(target_height);

    // Create a fresh node with genesis-only store.
    let identifier = "fresh_node";
    let account_id: AccountId = "fresh_node".parse().unwrap();
    let node_state = env
        .node_state_builder()
        .account_id(account_id)
        .config_modifier(|config| {
            config.block_header_fetch_horizon = 8;
            config.block_fetch_horizon = 3;
        })
        .build();
    env.add_node(identifier, node_state);

    // Track sync status transitions on the fresh node.
    let fresh_node_idx = env.node_datas.len() - 1;
    let new_node = env.node_datas[fresh_node_idx].client_sender.actor_handle();
    let sync_status_history = Rc::new(RefCell::new(Vec::new()));
    {
        let sync_status_history = sync_status_history.clone();
        env.test_loop.set_every_event_callback(move |test_loop_data| {
            let client = &test_loop_data.get(&new_node).client;
            let sync_status = client.sync_handler.sync_status.as_variant_name();
            let mut history = sync_status_history.borrow_mut();
            if history.last().map(|s| s as &str) != Some(sync_status) {
                history.push(sync_status.to_string());
            }
        });
    }

    // Run until the fresh node catches up to node 0's head height.
    env.node_runner(fresh_node_idx).run_until_head_height(target_height);

    // Run 2 more epochs to verify continued normal operation.
    for _ in 0..2 {
        env.node_runner(fresh_node_idx).run_until_new_epoch();
    }

    assert_eq!(
        sync_status_history.borrow().as_slice(),
        &[
            "AwaitingPeers",
            "NoSync",
            "EpochSync",
            "EpochSyncDone",
            "HeaderSync",
            "StateSync",
            "StateSyncDone",
            "BlockSync",
            "NoSync",
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Validate that the epoch sync proof stored in epoch_store matches the one derived
// from derive_epoch_sync_proof_from_last_block.
//
// We pass the first_block_hash of the current epoch T.
// The proof stored in epoch_store is for epoch T-2.
fn validate_epoch_sync_proof(epoch_store: &EpochStoreAdapter, first_block_hash: &CryptoHash) {
    // Get the proof stored in epoch_store, compare this against the expected proof generated
    // from derive_epoch_sync_proof_from_last_block.
    let proof = epoch_store.get_epoch_sync_proof().unwrap().expect("Proof should exist");

    // For derive_epoch_sync_proof_from_last_block, we need to provide the last final block of epoch T-2,
    let first_block_info = epoch_store.get_block_info(first_block_hash).unwrap();
    let last_block_info_in_prev_epoch =
        epoch_store.get_block_info(first_block_info.prev_hash()).unwrap();
    let first_block_hash_in_prev_epoch = last_block_info_in_prev_epoch.epoch_first_block();
    let first_block_info_in_prev_epoch =
        epoch_store.get_block_info(&first_block_hash_in_prev_epoch).unwrap();
    let last_block_hash_in_prev_prev_epoch = first_block_info_in_prev_epoch.prev_hash();

    let expected_proof = derive_epoch_sync_proof_from_last_block(
        &epoch_store,
        &last_block_hash_in_prev_prev_epoch,
        false,
    )
    .unwrap();

    assert_eq!(proof, expected_proof.into_v1());
}
