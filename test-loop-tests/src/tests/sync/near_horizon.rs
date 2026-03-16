//! Near-horizon sync tests.
//!
//! These test the V2 pipeline for nodes that are only a few epochs behind:
//!   BlockSync → NoSync (no epoch sync, no state sync)
//!
//! Near-horizon means the node is within `epoch_sync_horizon` of the network
//! tip, so it enters BlockSync directly. Header sync and block sync run
//! together; the node never does epoch sync or state sync.

use super::util::{
    TEST_EPOCH_SYNC_HORIZON, assert_near_horizon_sync_sequence, run_until_synced, track_sync_status,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use near_async::time::Duration;
use near_client::SyncStatus;
use near_client::sync::SYNC_V2_ENABLED;
use near_o11y::testonly::init_test_logger;

// Scenario: A fresh node starts with only genesis data while the network is
// 1 block below the epoch sync horizon. The node is within horizon distance,
// so it enters BlockSync directly without epoch sync or state sync.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs to height 19 (horizon = 20, just below boundary)
//   - Add a fresh node with explicit horizon config
//
// Assertions:
//   - New node catches up to network tip
//   - Sync status sequence: AwaitingPeers → NoSync → BlockSync → NoSync
//   - No EpochSync or StateSync in the status history
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_block_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env =
        TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(epoch_length).build();

    // Run 1 block below the horizon boundary (horizon = TEST_EPOCH_SYNC_HORIZON
    // * epoch_length = 20). At height 19, the fresh node sees
    // tip_height(0) + horizon(20) >= highest_height(19) and skips epoch sync.
    env.node_runner(0).run_until_head_height(TEST_EPOCH_SYNC_HORIZON * epoch_length - 1);

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
}

// Scenario: A node that is exactly at the edge of `epoch_sync_horizon` should
// enter BlockSync (not epoch sync) and still succeed because all needed blocks
// are within the GC window on peers.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs exactly TEST_EPOCH_SYNC_HORIZON epochs
//   - Add a fresh node at genesis with explicit horizon config
//
// Assertions:
//   - Node enters BlockSync (not EpochSync)
//   - Block sync succeeds (all needed blocks within GC window)
//   - Node catches up to network tip
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_epoch_sync_boundary() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env =
        TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(epoch_length).build();

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
}

// Scenario: A fresh node doing near-horizon block sync is killed mid-sync
// and restarted. On restart, the node has partial block data in the DB.
// It should resume block sync and catch up.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs to height 19 (1 block below horizon boundary)
//   - Add a fresh node, wait until mid-BlockSync, kill, restart
//
// Assertions:
//   - Restarted node catches up to network tip
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_restart_during_block_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env =
        TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(epoch_length).build();

    // Run within near-horizon range (1 block below the horizon boundary).
    env.node_runner(0).run_until_head_height(TEST_EPOCH_SYNC_HORIZON * epoch_length - 1);

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

    let killed_state = env.kill_node("new_node");

    env.restart_node("restart_block_sync", killed_state);
    let restarted_idx = env.node_datas.len() - 1;

    run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, 0);
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
//   - Node catches up to network tip despite its tight `gc_num_epochs_to_keep`
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
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
}
