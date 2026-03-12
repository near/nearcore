//! Near-horizon sync tests.
//!
//! These test the V2 pipeline for nodes that are only a few epochs behind:
//!   BlockSync → NoSync (no epoch sync, no state sync)
//!
//! Near-horizon means the node is within `epoch_sync_horizon` of the network
//! tip, so it enters BlockSync directly. Header sync and block sync run
//! together; the node never does epoch sync or state sync.

use super::util::{
    TEST_EPOCH_SYNC_HORIZON, assert_near_horizon_sync_sequence, run_until_synced,
    track_sync_status, verify_balances_on_synced_node,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::SyncStatus;
use near_client::sync::SYNC_V2_ENABLED;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance};

// Scenario: A fresh node starts with only genesis data while the network is
// ~1.5 epochs ahead. The node is within epoch_sync_horizon distance,
// so it enters BlockSync directly without epoch sync or state sync.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, gc_num_epochs_to_keep=20
//   - Network runs ~1.5 epochs (within horizon)
//   - Add a fresh node with explicit horizon config
//
// Assertions:
//   - New node catches up to network tip
//   - Sync status sequence: AwaitingPeers → NoSync → BlockSync → NoSync
//   - No EpochSync or StateSync in the status history
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_block_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(20)
        .build();

    // Run within the horizon (1.5 epochs < TEST_EPOCH_SYNC_HORIZON * epoch_length).
    env.node_runner(0).run_until_head_height(epoch_length + epoch_length / 2);

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);

    assert_near_horizon_sync_sequence(&sync_history.borrow());
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: A node that is exactly at the edge of `epoch_sync_horizon` should
// enter BlockSync (not epoch sync) and still succeed because all needed blocks
// are within the GC window on peers.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, gc_num_epochs_to_keep=3
//   - Network runs exactly TEST_EPOCH_SYNC_HORIZON epochs
//   - Add a fresh node at genesis with explicit horizon config
//
// Assertions:
//   - Node enters BlockSync (not EpochSync)
//   - Block sync succeeds (all needed blocks within GC window)
//   - Node catches up to network tip
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_epoch_sync_boundary() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(3)
        .build();

    // Run exactly at the horizon boundary.
    env.node_runner(0).run_until_head_height(TEST_EPOCH_SYNC_HORIZON * epoch_length);

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);

    assert_near_horizon_sync_sequence(&sync_history.borrow());
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: One validator is killed while the other 3 continue producing
// blocks with cross-shard transactions. The killed validator falls behind,
// then is restarted and must catch up via near-horizon BlockSync.
//
// This is a key operational scenario: validators occasionally restart
// (upgrades, crashes) and must recover quickly without full state sync.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, shard shuffling
//   - Run all nodes ~2 epochs with cross-shard money transfers
//   - Kill validator 0, remaining 3 advance ~15 blocks
//   - Restart validator 0
//   - Repeat kill/restart cycle once more
//
// Assertions:
//   - Restarted validator catches up to same height as others
//   - Near-horizon sync status sequence (BlockSync, no EpochSync)
//   - Validator remains in the producer set after restart
//   - Cross-shard transactions succeed after restart
//   - Balance consistency across all validators
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_validator_restart() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    const NUM_CLIENTS: usize = 4;
    let accounts = make_accounts(20);
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(ShardLayout::multi_shard(4, 3))
        .add_user_accounts_simple(&accounts, Balance::from_near(1_000_000))
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .gc_num_epochs_to_keep(10)
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(20);

    let kill_account: AccountId = "account0".parse().unwrap();
    let live_account: AccountId = "account1".parse().unwrap();

    // Two cycles: first tests restart after normal operation, second tests
    // restart after a restart (validator 0 both times).
    for i in 0..2 {
        let kill_data_idx = env.account_data_idx(&kill_account);
        let kill_identifier = env.node_datas[kill_data_idx].identifier.clone();
        let killed_state = env.kill_node(&kill_identifier);

        env.runner_for_account(&live_account).run_for_number_of_blocks(15);

        let restart_id = format!("{}-restart-{}", kill_identifier, i);
        env.restart_node(&restart_id, killed_state);
        let restarted_idx = env.node_datas.len() - 1;

        let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, restarted_idx);

        let live_data_idx = env.account_data_idx(&live_account);
        run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, live_data_idx);
        assert_near_horizon_sync_sequence(&sync_history.borrow());

        // Verify the restarted validator is still in the next epoch's producer set.
        let restarted_handle = env.node_datas[restarted_idx].client_sender.actor_handle();
        let restarted_client = &env.test_loop.data.get(&restarted_handle).client;
        let head = restarted_client.chain.head().unwrap();
        let next_epoch_id =
            restarted_client.epoch_manager.get_next_epoch_id(&head.last_block_hash).unwrap();
        let producers = restarted_client
            .epoch_manager
            .get_epoch_block_producers_ordered(&next_epoch_id)
            .unwrap();
        assert!(
            producers.iter().any(|p| p.account_id() == &kill_account),
            "validator {kill_account} not in next epoch's producer set after restart cycle {i}"
        );

        // Run another epoch to verify the validator is still producing.
        env.runner_for_account(&live_account).run_for_number_of_blocks(10);

        // Send cross-shard transfers from the restarted validator.
        for (j, sender) in accounts.iter().enumerate().take(20) {
            let receiver = &accounts[(j + 1) % accounts.len()];
            let tx = env.node(restarted_idx).tx_send_money(sender, receiver, Balance::from_near(1));
            env.node(restarted_idx).submit_tx(tx);
        }
        env.runner_for_account(&live_account).run_for_number_of_blocks(5);
    }

    let ref_idx = env.account_data_idx(&kill_account);
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, ref_idx, &accounts);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: A fresh node doing near-horizon block sync is killed mid-sync
// and restarted. On restart, the node has partial block data in the DB.
// It should resume block sync and catch up.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, gc_num_epochs_to_keep=20
//   - Network runs ~1.5 epochs (within horizon)
//   - Add a fresh node, wait until mid-BlockSync, kill, restart
//
// Assertions:
//   - Restarted node catches up to network tip
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_restart_during_block_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(10)
        .gc_num_epochs_to_keep(20)
        .build();

    env.node_runner(0).run_until_head_height(15);

    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    // Wait until new node is in the MIDDLE of BlockSync (current_height > start_height).
    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let status = &data.get(&new_node_handle).client.sync_handler.sync_status;
            matches!(status, SyncStatus::BlockSync { start_height, current_height, .. }
                if *current_height > *start_height)
        },
        Duration::seconds(20),
    );

    let new_node_identifier = env.node_datas.last().unwrap().identifier.clone();
    let killed_state = env.kill_node(&new_node_identifier);

    env.restart_node("restart_block_sync", killed_state);
    let restarted_idx = env.node_datas.len() - 1;

    run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, 0);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: A fresh node has epoch_sync_horizon=10 but gc=3. The network is
// ~8 epochs ahead, which is beyond the node's GC window (3 epochs = 30 blocks)
// but within its large horizon (10 epochs = 100 blocks). The node enters
// near-horizon BlockSync and catches up ~80 blocks — more than its own GC
// window would normally allow.
//
// Setup:
//   - 4 validators, epoch_length=10, gc=10 (peers keep all blocks)
//   - Network runs 8 epochs (80 blocks)
//   - Add a fresh node with gc=3, epoch_sync_horizon=10
//
// Assertions:
//   - Node enters BlockSync (near horizon, not EpochSync)
//   - Node catches up to network tip despite its tight GC
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_sync_beyond_gc_window() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let custom_horizon: u64 = 10;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(10)
        .build();

    // Run 8 epochs (80 blocks) — within the custom horizon (100) but beyond gc (30).
    env.node_runner(0).run_until_head_height(8 * epoch_length);

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(move |config| {
            config.gc.gc_num_epochs_to_keep = 3;
            config.epoch_sync.epoch_sync_horizon_num_epochs = custom_horizon;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);

    assert_near_horizon_sync_sequence(&sync_history.borrow());
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
