//! Sync followed by shard catchup.
//!
//! Tests the scenario where a fresh node with a rotating shard schedule syncs
//! and then experiences shard rotation at epoch boundaries — triggering the
//! catchup path for newly tracked shards.
//!
//! Two variants:
//! - Far-horizon: node joins many epochs behind, syncs via the full V2 pipeline
//! - Near-horizon: node joins within epoch_sync_horizon, syncs via BlockSync only

use super::util::{
    TEST_EPOCH_SYNC_HORIZON, assert_far_horizon_sync_sequence, assert_near_horizon_sync_sequence,
    run_until_synced, track_sync_status,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_chain_configs::TrackedShardsConfig;
use near_client::sync::SYNC_V2_ENABLED;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{Balance, ShardId};

// Scenario: A fresh non-validator node with a rotating shard schedule joins
// the network far behind. It goes through the full V2 far-horizon pipeline,
// then continues running. When a new epoch starts and the shard schedule
// rotates, the node must catch up state for the newly tracked shard via the
// catchup path.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - 100 user accounts with cross-shard money transfers
//   - Network runs past the epoch sync horizon
//   - Add a fresh non-validator with rotating shard schedule:
//     epoch 0-1: [0], epoch 2: [1], epoch 3: [2], epoch 4: [3],
//     epoch 5: [1], epoch 6: [2], epoch 7: [3]
//   - Node syncs, then continues running past 3 more epoch boundaries
//
// Assertions:
//   - Full far-horizon sync status sequence
//   - Node survives shard schedule rotation (catchup path fires)
//   - No panics during partial encoded chunk creation after catchup
#[test]
// TODO(spice-test): Assess if this test is relevant for spice and if yes fix it.
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sync_then_shard_catchup() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height((TEST_EPOCH_SYNC_HORIZON + 3) * epoch_length);

    // Add a fresh non-validator with a rotating shard schedule.
    // Schedule: [[0],[0],[1],[2],[3],[1],[2],[3]].
    // After sync, when the node enters a new epoch, its tracked shard changes,
    // triggering the catchup path for the newly tracked shard.
    let schedule = vec![
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        vec![ShardId::new(1)],
        vec![ShardId::new(2)],
        vec![ShardId::new(3)],
        vec![ShardId::new(1)],
        vec![ShardId::new(2)],
        vec![ShardId::new(3)],
    ];
    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(move |config| {
            config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    // No single-peer restriction — the node tracks only 1 shard at a time, so
    // it needs to fetch chunks from multiple validators.
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    assert_far_horizon_sync_sequence(&sync_history.borrow());

    // Continue running past 3 more epoch boundaries to trigger shard schedule
    // rotation and catchup multiple times.
    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);
}

// Scenario: Same as above but the node joins within the epoch_sync_horizon,
// syncing via near-horizon BlockSync (no epoch sync, no state sync). After
// catch-up, shard rotation at epoch boundaries triggers the catchup path.
//
// This tests that catchup works correctly after block-sync-only catch-up,
// where the node has recent enough state to apply blocks directly but still
// needs to catch up new shards at epoch boundaries.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs 1 block below the epoch sync horizon (height 19)
//   - Add a fresh non-validator with the same rotating shard schedule
//   - Node syncs via near-horizon (BlockSync), then runs 3 more epochs
//
// Assertions:
//   - Near-horizon sync status sequence (no EpochSync or StateSync)
//   - Node advanced 3+ full epochs after sync (shard rotation happened)
//   - No panics during catchup for newly tracked shards
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_sync_then_shard_catchup() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env =
        TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(epoch_length).build();

    // Run 1 block below the horizon boundary so the fresh node enters near-horizon.
    env.node_runner(0).run_until_head_height(TEST_EPOCH_SYNC_HORIZON * epoch_length - 1);

    // Same rotating shard schedule as the far-horizon variant.
    let schedule = vec![
        vec![ShardId::new(0)],
        vec![ShardId::new(0)],
        vec![ShardId::new(1)],
        vec![ShardId::new(2)],
        vec![ShardId::new(3)],
        vec![ShardId::new(1)],
        vec![ShardId::new(2)],
        vec![ShardId::new(3)],
    ];
    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(move |config| {
            config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    // Continue past 3 epoch boundaries to trigger shard schedule rotation and catchup.
    // If catchup fails for a newly tracked shard, the node would panic with
    // IncorrectStateRoot or stall.
    let height_after_sync = env.node(new_node_idx).head().height;
    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);
    let height_after_catchup = env.node(new_node_idx).head().height;

    // Verify the node advanced through 3 full epochs (shard rotation happened 3 times).
    assert!(
        height_after_catchup >= height_after_sync + 3 * epoch_length,
        "node should have advanced 3+ epochs after sync for shard rotation, \
         but only went from {height_after_sync} to {height_after_catchup}"
    );
}
