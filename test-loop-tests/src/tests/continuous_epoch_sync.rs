use std::cell::RefCell;
use std::rc::Rc;

use near_async::time::Duration;
use near_epoch_manager::epoch_sync::derive_epoch_sync_proof_from_last_block;
use near_o11y::testonly::init_test_logger;
use near_primitives::hash::CryptoHash;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_store::adapter::StoreAdapter;
use near_store::adapter::epoch_store::EpochStoreAdapter;

use crate::setup::builder::{NodeStateBuilder, TestLoopBuilder};
use crate::utils::account::{create_validators_spec, validators_spec_clients};
use crate::utils::node::TestLoopNode;

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
    let validator_node = TestLoopNode::from(&env.node_datas[0]);

    let client = validator_node.client(env.test_loop_data());
    let epoch_store = client.chain.chain_store.epoch_store();

    // Run for 5 epochs
    for _ in 0..5 {
        validator_node.run_until_new_epoch(&mut env.test_loop);
    }

    // Run for another 5 epochs
    for _ in 0..5 {
        validator_node.run_until_new_epoch(&mut env.test_loop);
        let head_hash = validator_node.head(&env.test_loop_data()).last_block_hash;
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
    let validator_node = TestLoopNode::from(&env.node_datas[0]);

    // Run for 5 epochs
    for _ in 0..5 {
        validator_node.run_until_new_epoch(&mut env.test_loop);
    }

    // verify if this is the first block of an epoch
    let chain_store = validator_node.client(env.test_loop_data()).chain.chain_store.chain_store();
    let head = chain_store.head().unwrap();
    let prev_block_header = chain_store.get_block_header(&head.prev_block_hash).unwrap();
    assert_ne!(&head.epoch_id, prev_block_header.epoch_id());

    // create a fork
    let height_selection = AdvProduceBlockHeightSelection::NextHeightOnSelectedBlock {
        base_block_height: head.height - 1,
    };
    let client_actor = validator_node.client_actor(&mut env.test_loop.data);
    client_actor.adv_produce_blocks_on(3, true, height_selection);

    // verify proof after processing fork
    validate_epoch_sync_proof(&chain_store.epoch_store(), &head.last_block_hash);

    // run for another epoch
    validator_node.run_until_new_epoch(&mut env.test_loop);

    // verify proof
    let head = chain_store.head().unwrap();
    validate_epoch_sync_proof(&chain_store.epoch_store(), &head.last_block_hash);

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

/// Test that a node killed and restarted after peers have advanced beyond GC
/// can catch up via epoch sync when ContinuousEpochSync is enabled.
///
/// Verifies the node walks through the complete sync pipeline:
/// AwaitingPeers → NoSync → EpochSync → EpochSyncDone → HeaderSync → StateSync → StateSyncDone → BlockSync → NoSync
#[test]
fn test_epoch_sync_catchup_restart_node() {
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();

    let epoch_length = 10;
    let gc_num_epochs_to_keep = 3;

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
        .gc_num_epochs_to_keep(gc_num_epochs_to_keep)
        .config_modifier(|config, _idx| {
            config.epoch_sync.epoch_sync_horizon = 30;
        })
        .build()
        .warmup();

    let restart_node = TestLoopNode::from(env.node_datas[0].clone());
    let stable_node = TestLoopNode::from(env.node_datas[1].clone());

    // Progress all nodes for 3 epochs, then kill node 0.
    let kill_height = 3 * epoch_length;
    restart_node.run_until_head_height(&mut env.test_loop, kill_height);
    let killed_node_state = env.kill_node(&restart_node.data().identifier);

    // Progress remaining nodes beyond the GC period.
    let target_height = kill_height + (gc_num_epochs_to_keep + 5) * epoch_length + 7;
    stable_node.run_until_head_height(&mut env.test_loop, target_height);

    // Restart the killed node.
    let restart_account_id = restart_node.data().account_id.clone();
    let restart_identifier = format!("{}-restart", restart_node.data().identifier);
    env.restart_node(&restart_identifier, killed_node_state);

    let restart_node = TestLoopNode::for_account(&env.node_datas, &restart_account_id);

    // Track sync status transitions on the restarted node.
    let restart_handle = restart_node.data().client_sender.actor_handle();
    let stable_handle = stable_node.data().client_sender.actor_handle();
    let sync_status_history = Rc::new(RefCell::new(Vec::<String>::new()));
    {
        let sync_status_history = sync_status_history.clone();
        env.test_loop.set_every_event_callback(move |test_loop_data| {
            let client = &test_loop_data.get(&restart_handle).client;
            let sync_status = client.sync_handler.sync_status.as_variant_name();
            let mut history = sync_status_history.borrow_mut();
            if history.last().map(|s| s as &str) != Some(sync_status) {
                history.push(sync_status.to_string());
            }
        });
    }

    // Run until the restarted node catches up to the stable node.
    let restart_handle = restart_node.data().client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            let stable_height =
                test_loop_data.get(&stable_handle).client.chain.head().unwrap().height;
            let restart_height =
                test_loop_data.get(&restart_handle).client.chain.head().unwrap().height;
            restart_height == stable_height
        },
        Duration::seconds(60),
    );

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

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Test that a brand-new node joining after the network has advanced beyond GC
/// can catch up via epoch sync when ContinuousEpochSync is enabled.
///
/// Verifies the node walks through the complete sync pipeline:
/// AwaitingPeers → NoSync → EpochSync → EpochSyncDone → HeaderSync → StateSync → StateSyncDone → BlockSync → NoSync
#[test]
fn test_epoch_sync_catchup_fresh_node() {
    if !ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();

    let epoch_length = 10;
    let gc_num_epochs_to_keep = 3;

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
        .gc_num_epochs_to_keep(gc_num_epochs_to_keep)
        .config_modifier(|config, _idx| {
            config.epoch_sync.epoch_sync_horizon = 30;
        })
        .build()
        .warmup();

    let stable_node = TestLoopNode::from(env.node_datas[0].clone());

    // Progress all validators beyond the GC period.
    let target_height = (gc_num_epochs_to_keep + 8) * epoch_length;
    stable_node.run_until_head_height(&mut env.test_loop, target_height);

    // Add a brand-new node with only genesis state.
    let fresh_identifier = "fresh_node0";
    let fresh_account_id: near_primitives::types::AccountId = fresh_identifier.parse().unwrap();
    let node_state = NodeStateBuilder::new(
        env.shared_state.genesis.clone(),
        env.shared_state.tempdir.path().to_path_buf(),
    )
    .account_id(fresh_account_id.clone())
    .config_modifier(|config| {
        config.epoch_sync.epoch_sync_horizon = 30;
    })
    .build();
    env.add_node(fresh_identifier, node_state);

    let fresh_node = TestLoopNode::for_account(&env.node_datas, &fresh_account_id);

    // Track sync status transitions on the fresh node.
    let fresh_handle = fresh_node.data().client_sender.actor_handle();
    let stable_handle = stable_node.data().client_sender.actor_handle();
    let sync_status_history = Rc::new(RefCell::new(Vec::<String>::new()));
    {
        let sync_status_history = sync_status_history.clone();
        env.test_loop.set_every_event_callback(move |test_loop_data| {
            let client = &test_loop_data.get(&fresh_handle).client;
            let sync_status = client.sync_handler.sync_status.as_variant_name();
            let mut history = sync_status_history.borrow_mut();
            if history.last().map(|s| s as &str) != Some(sync_status) {
                history.push(sync_status.to_string());
            }
        });
    }

    // Run until the fresh node catches up to the stable node.
    let fresh_handle = fresh_node.data().client_sender.actor_handle();
    env.test_loop.run_until(
        |test_loop_data| {
            let stable_height =
                test_loop_data.get(&stable_handle).client.chain.head().unwrap().height;
            let fresh_height =
                test_loop_data.get(&fresh_handle).client.chain.head().unwrap().height;
            fresh_height == stable_height
        },
        Duration::seconds(60),
    );

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

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
}
