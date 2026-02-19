use std::cell::RefCell;
use std::rc::Rc;

use near_async::time::Duration;
use near_epoch_manager::epoch_sync::derive_epoch_sync_proof_from_last_block;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::EPOCH_SYNC_RESET_MARKER;
use near_store::adapter::StoreAdapter;
use near_store::adapter::epoch_store::EpochStoreAdapter;

use crate::setup::builder::{NodeStateBuilder, TestLoopBuilder};
use crate::setup::state::NodeExecutionData;
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

// Test that a brand-new node joining after the network has advanced catches up via epoch sync.
// The new node starts from genesis and goes through the full sync status progression:
// AwaitingPeers → NoSync → EpochSync → EpochSyncDone → HeaderSync → StateSync →
// StateSyncDone → BlockSync → NoSync
#[test]
fn test_epoch_sync_catchup_fresh_node() {
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

    // Run the network to target_height = 8 * epoch_length = 80.
    // epoch_sync_horizon_num_epochs defaults to 4, so epoch sync triggers when the node is
    // 4 * epoch_length = 40 blocks behind the highest height.
    let target_height = 8 * epoch_length;
    env.node_runner(0).run_until_head_height(target_height);

    // Add a brand-new node from genesis state.
    let genesis = env.shared_state.genesis.clone();
    let tempdir_path = env.shared_state.tempdir.path().to_path_buf();
    let fresh_identifier = format!("new-account{}", env.node_datas.len());
    let fresh_account_id = fresh_identifier.parse().unwrap();
    let node_state =
        NodeStateBuilder::new(genesis, tempdir_path).account_id(fresh_account_id).build();
    env.add_node(&fresh_identifier, node_state);

    let fresh_node_handle = env.node_datas.last().unwrap().client_sender.actor_handle();
    let stable_node_handle = env.node_datas[0].client_sender.actor_handle();

    // Track sync status transitions on the fresh node.
    let sync_status_history = Rc::new(RefCell::new(Vec::new()));
    {
        let sync_status_history = sync_status_history.clone();
        let fresh_handle = fresh_node_handle.clone();
        env.test_loop.set_every_event_callback(move |test_loop_data| {
            let client = &test_loop_data.get(&fresh_handle).client;
            let header_head_height = client.chain.header_head().unwrap().height;
            let head_height = client.chain.head().unwrap().height;
            tracing::info!(
                ?client.sync_handler.sync_status,
                ?header_head_height,
                ?head_height,
                "fresh node sync status"
            );
            let sync_status = client.sync_handler.sync_status.as_variant_name();
            let mut history = sync_status_history.borrow_mut();
            if history.last().map(|s| s as &str) != Some(sync_status) {
                history.push(sync_status.to_string());
            }
        });
    }

    // Run until the fresh node catches up to the stable node.
    env.test_loop.run_until(
        |test_loop_data| {
            let fresh_height =
                test_loop_data.get(&fresh_node_handle).client.chain.head().unwrap().height;
            let stable_height =
                test_loop_data.get(&stable_node_handle).client.chain.head().unwrap().height;
            fresh_height == stable_height
        },
        Duration::seconds(10),
    );

    // Verify the sync status transitions.
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

// Test that an existing node killed and restarted writes an epoch sync reset marker and shuts down.
// When a node with tip != genesis enters epoch sync, it detects stale data, writes a marker file,
// and fires the shutdown signal.
#[test]
fn test_epoch_sync_marker_and_shutdown() {
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

    // Run all nodes for 3 epochs so node 0 has data (tip >> genesis).
    let kill_height = 3 * epoch_length;
    env.node_runner(0).run_until_head_height(kill_height);

    // Kill node 0.
    let restart_identifier = env.node_datas[0].identifier.clone();
    let killed_node_state = env.kill_node(&restart_identifier);

    // Advance the remaining nodes well past the epoch_sync_horizon (4 epochs = 40 blocks).
    // Target: kill_height + 8 * epoch_length = 30 + 80 = 110.
    let advance_target = kill_height + 8 * epoch_length;
    env.node_runner(1).run_until_head_height(advance_target);

    // Restart node 0 with its killed state (stale data, tip != genesis).
    let new_identifier = format!("{}-restart", restart_identifier);
    env.restart_node(&new_identifier, killed_node_state);

    let restarted_node_handle = env.node_datas.last().unwrap().client_sender.actor_handle();
    let restarted_head_before =
        env.test_loop.data.get(&restarted_node_handle).client.chain.head().unwrap().height;

    // Run briefly — epoch_sync.run() sees tip != genesis, writes marker, fires shutdown.
    env.test_loop.run_for(Duration::seconds(10));

    // Assert: marker file exists at the restarted node's home directory.
    let homedir = NodeExecutionData::homedir(&env.shared_state.tempdir, &new_identifier);
    let marker_path = homedir.join("data").join(EPOCH_SYNC_RESET_MARKER);
    assert!(marker_path.exists(), "epoch sync reset marker should exist at {marker_path:?}");

    // Assert: restarted node stopped advancing (shutdown denylisted it).
    let restarted_head_after =
        env.test_loop.data.get(&restarted_node_handle).client.chain.head().unwrap().height;
    tracing::info!(
        restarted_head_before,
        restarted_head_after,
        "restarted node head heights (should not have advanced)"
    );
    assert_eq!(
        restarted_head_before, restarted_head_after,
        "restarted node should not have advanced past its kill height"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
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
