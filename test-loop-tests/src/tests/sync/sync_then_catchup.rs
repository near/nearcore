//! State sync followed by shard catchup.
//!
//! Tests the scenario where a fresh node with a rotating shard schedule joins
//! far behind, syncs via the V2 pipeline, and then experiences shard rotation
//! at an epoch boundary — triggering the catchup path for newly tracked shards.
//!
//! TODO: add a near-horizon variant where the node is within epoch_sync_horizon
//! and catches up via BlockSync, then shard rotation triggers catchup.

use super::util::{
    TEST_EPOCH_SYNC_HORIZON, assert_far_horizon_sync_sequence, run_until_synced, track_sync_status,
    verify_balances_on_synced_node,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::time::Duration;
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
//   - Node catches up to network tip via far-horizon sync
//   - Node survives shard schedule rotation (catchup path fires)
//   - No panics during partial encoded chunk creation after catchup
//   - Full far-horizon sync status sequence
//   - Account balances match source validator
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
    // The schedule mirrors the pytest's [[0],[0],[1],[2],[3],[1],[2],[3]].
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

    // Continue running past 3 more epoch boundaries to trigger shard schedule
    // rotation and catchup multiple times.
    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

    assert_far_horizon_sync_sequence(&sync_history.borrow());
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node_idx, &accounts);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
