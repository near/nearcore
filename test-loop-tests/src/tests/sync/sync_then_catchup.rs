//! State sync followed by shard catchup.
//!
//! Tests the scenario where a fresh node with a rotating shard schedule joins
//! far behind, syncs via the V2 pipeline, and then experiences shard rotation
//! at an epoch boundary — triggering the catchup path for newly tracked shards.
//!
//! Migrated from: pytest/tests/sanity/state_sync_then_catchup.py

use super::util::{assert_far_horizon_sync_sequence, track_sync_status};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_async::time::Duration;
use near_chain_configs::TrackedShardsConfig;
use near_client::sync::SYNC_V2_ENABLED;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::{Balance, ShardId};

// ---------------------------------------------------------------------------
// State sync then shard catchup
// ---------------------------------------------------------------------------
//
// Scenario: A fresh non-validator node with a rotating shard schedule joins
// the network far behind. It goes through the full V2 far-horizon pipeline
// (EpochSync → HeaderSync → StateSync → BlockSync → NoSync), then continues
// running. When a new epoch starts and the shard schedule rotates, the node
// must catch up state for the newly tracked shard via the catchup path.
//
// The pytest uses contract deployments + function calls to generate cross-shard
// receipts. We use money transfers which also produce cross-shard receipts and
// exercise the same catchup code paths.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, shard shuffling
//   - 100 user accounts with cross-shard money transfers
//   - Network runs past 5 epochs
//   - Add a fresh non-validator with shard schedule:
//     epoch 0: [0], epoch 1: [0], epoch 2: [1], epoch 3: [2],
//     epoch 4: [3], epoch 5: [1], epoch 6: [2], epoch 7: [3]
//   - Node syncs, then continues running past epoch boundaries
//
// Assertions:
//   - Node catches up to network tip via far-horizon sync
//   - Node survives shard schedule rotation (catchup path fires)
//   - No panics during partial encoded chunk creation after catchup
//   - Sync status includes StateSync (far-horizon)
//
// Migrated from: state_sync_then_catchup.py
// Covers: Combined far-horizon sync → shard catchup under cross-shard load
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_sync_then_shard_catchup() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut builder =
        TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(epoch_length);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    // Run network for 5+ epochs with cross-shard money transfers.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(5 * epoch_length);

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
    let new_account = create_account_id("catchup_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(move |config| {
            config.tracked_shards_config = TrackedShardsConfig::Schedule(schedule.clone());
        })
        .build();
    env.add_node("catchup_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    // Wait for the new node to catch up to the network tip.
    // No single-peer restriction — the node tracks only 1 shard at a time, so
    // it needs to fetch chunks from multiple validators.
    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_node_handle).client.chain.head().unwrap().height;
            let node0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == node0_h
        },
        Duration::seconds(60),
    );

    // Continue running past 3 more epoch boundaries to trigger shard schedule
    // rotation and catchup multiple times. The schedule changes tracked shard
    // each epoch, so each boundary triggers catchup for a different shard.
    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

    assert_far_horizon_sync_sequence(&sync_history.borrow());

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
