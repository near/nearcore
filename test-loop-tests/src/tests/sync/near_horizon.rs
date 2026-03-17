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
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_client::SyncStatus;
use near_client::sync::SYNC_V2_ENABLED;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{Balance, ShardId};

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

// Scenario: Kill a validator that tracks all shards, reduce its tracked shards
// config to a subset, and restart. The node block-syncs to catch up, then runs
// past epoch boundaries where the schedule rotates to different shards.
//
// Mirrors the pytest `block_sync_flat_storage.py`: the node starts with AllShards
// (so it has state for every shard), then the config is changed to a schedule
// that tracks only shard [0] now and [1, 2, 3] next epoch. On restart the node
// must correctly handle chunks for the shards it will care about next epoch.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, all tracking AllShards
//   - 100 user accounts with cross-shard money transfers
//   - Run past 1 epoch boundary, kill node 0, change config to
//     Schedule: [[0], [0], [1, 2, 3]], advance 1 epoch, restart
//
// Assertions:
//   - Node catches up via block sync after the gap
//   - Node survives 2 more epoch boundaries (schedule rotates to [1, 2, 3])
//   - No IncorrectStateRoot or FlatStorage panics
//
// This is a regression test for PR #9368 / PR #10820: flat storage and memtrie
// loading must handle tracked shards changing between restarts.
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_change_tracked_shards_on_restart() {
    init_test_logger();
    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .track_all_shards()
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    // Run past 1 epoch boundary so all nodes have state for all shards.
    env.node_runner(0).run_until_new_epoch();

    // Kill node 0 mid-epoch.
    env.node_runner(0).run_for_number_of_blocks(5);
    let node0_identifier = env.node_datas[0].identifier.clone();
    let mut killed_state = env.kill_node(&node0_identifier);

    // Change tracked shards: track only shard [0] now, [1, 2, 3] next epoch.
    // Same schedule as the pytest: [[0], [0], [1, 2, 3]].
    killed_state.client_config.tracked_shards_config = TrackedShardsConfig::Schedule(vec![
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        vec![ShardId::new(1), ShardId::new(2), ShardId::new(3)],
    ]);

    // Advance remaining validators ~1 epoch so node 0 falls behind (needs block sync).
    env.node_runner(1).run_until_new_epoch();

    // Restart node 0 — it block-syncs to catch up.
    env.restart_node("node0_restart", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, restarted_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, 1);
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    // Run past 2 more epoch boundaries so the schedule rotates through all entries,
    // including the [1, 2, 3] entry that exercises the re-track path.
    env.node_runner(restarted_idx).run_until_new_epoch();
    env.node_runner(restarted_idx).run_until_new_epoch();

    // Verify node 0 kept up with the network. The restarted node may be 1 block
    // ahead of node 1 since run_until_new_epoch is driven from the restarted node.
    let restarted_height = env.node(restarted_idx).head().height;
    let reference_height = env.node(1).head().height;
    assert!(
        restarted_height >= reference_height,
        "restarted node fell behind: {restarted_height} < {reference_height}"
    );
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
