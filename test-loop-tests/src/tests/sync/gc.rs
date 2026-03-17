//! GC boundary verification tests after sync.
//!
//! These tests verify that garbage collection works correctly after sync — specifically,
//! that old blocks are properly GC'd while recent blocks are retained.
//!
//! Historical context: the regression these guard against (#2980) was state sync clearing
//! block infos needed to compute `gc_stop_height`, causing GC failures after state sync.
//!
//! How GC boundaries work:
//! - `get_gc_stop_height_impl` walks back `gc_num_epochs_to_keep - 1` epochs from the
//!   current epoch's first block. With gc=3, gc_stop is the start of the epoch that is
//!   2 epochs before the current one.
//! - `clear_old_blocks_data` clears blocks in range `(tail, gc_stop)` exclusive, so after
//!   a full GC pass: `tail == gc_stop - 1`.
//! - `head - gc_stop` varies from `(gc-1)*EPOCH_LENGTH` to `gc*EPOCH_LENGTH - 1` depending
//!   on where head sits within the current epoch. gc_stop is an epoch boundary, not a fixed
//!   offset from head.
//!
//! Migrated from pytests: `gc_after_sync.py`, `gc_after_sync1.py`, `gc_sync_after_sync.py`.

use super::util::{
    TEST_EPOCH_SYNC_HORIZON, assert_far_horizon_sync_sequence, assert_near_horizon_sync_sequence,
    far_horizon_height, run_until_synced, track_sync_status,
};
use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, make_accounts};
use near_chain_configs::TrackedShardsConfig;
use near_client::sync::SYNC_V2_ENABLED;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::Balance;

const EPOCH_LENGTH: u64 = 10;
const GC_EPOCHS: u64 = 3;

/// Verify GC boundaries on a node.
///
/// Asserts:
/// - `tail == gc_stop - 1`: GC clears blocks in `(tail, gc_stop)` exclusive, so after
///   a full pass the tail sits one below gc_stop.
/// - `head - gc_stop` is in `[(gc-1)*EPOCH_LENGTH, gc*EPOCH_LENGTH - 1]`: gc_stop is an
///   epoch boundary, so the exact offset depends on where head sits in the current epoch.
/// - Genesis block (height 1) is accessible (genesis_height defaults to 1 in tests).
/// - Blocks in `(genesis_height, tail)` are not accessible (GC'd or never downloaded).
/// - Blocks from tail to head are accessible.
fn assert_gc_boundaries(env: &TestLoopEnv, node_idx: usize) {
    let client = env.node(node_idx).client();
    let head = client.chain.head().unwrap();
    let genesis_height = client.chain.genesis().height();
    let tail = client.chain.tail();
    let gc_stop = client.runtime_adapter.get_gc_stop_height(&head.last_block_hash);

    assert_eq!(
        tail,
        gc_stop - 1,
        "tail should be gc_stop - 1: tail={tail}, gc_stop={gc_stop}, head={}",
        head.height
    );

    let diff = head.height - gc_stop;
    let min_diff = (GC_EPOCHS - 1) * EPOCH_LENGTH;
    let max_diff = GC_EPOCHS * EPOCH_LENGTH - 1;
    assert!(
        diff >= min_diff && diff <= max_diff,
        "head - gc_stop = {diff}, expected in [{min_diff}, {max_diff}] \
         (head={}, gc_stop={gc_stop})",
        head.height
    );

    // Genesis block should always be accessible.
    assert!(
        client.chain.get_block_by_height(genesis_height).is_ok(),
        "genesis block at height {genesis_height} should be accessible"
    );

    // Blocks between genesis and tail should be GC'd or never downloaded.
    for h in (genesis_height + 1)..tail {
        assert!(
            client.chain.get_block_by_height(h).is_err(),
            "block at height {h} should not be accessible (below tail={tail})"
        );
    }

    for h in tail..=head.height {
        assert!(
            client.chain.get_block_by_height(h).is_ok(),
            "block at height {h} should be present (tail={tail}, head={})",
            head.height
        );
    }
}

/// Shared setup for `test_gc_boundary_after_sync` and `test_gc_incremental`.
///
/// Adds a fresh node that syncs via far-horizon, then runs enough additional epochs
/// for GC to clean up sync-era data. The fresh node uses tight gc (`GC_EPOCHS=3`).
fn run_gc_after_far_horizon_sync(gc_blocks_limit: Option<u64>) {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(EPOCH_LENGTH)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    // Money transfers build non-trivial state needed for state sync to work.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(EPOCH_LENGTH));

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(move |config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
            config.gc.gc_num_epochs_to_keep = GC_EPOCHS;
            if let Some(limit) = gc_blocks_limit {
                config.gc.gc_blocks_limit = limit;
            }
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    assert_far_horizon_sync_sequence(&sync_history.borrow());

    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * EPOCH_LENGTH as usize);

    assert_gc_boundaries(&env, new_node_idx);
}

// Scenario: A fresh node syncs via far-horizon (EpochSync → HeaderSync → StateSync
// → BlockSync), then runs for several more epochs. Verify that GC correctly cleans
// up old blocks while retaining recent ones.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, 100 user accounts
//   - Network runs past the epoch sync horizon with cross-shard money transfers
//   - Add a fresh node with gc_num_epochs_to_keep=3, track all shards
//   - Node syncs via far-horizon, then runs 3 more epochs
//
// Assertions:
//   - Node went through far-horizon sync (status sequence check)
//   - tail == gc_stop - 1
//   - head - gc_stop in [20, 29] (gc=3, epoch_length=10)
//   - Blocks below tail are not accessible
//   - Blocks from tail to head are accessible
//
// Covers the regression from #2980 where state sync cleared block infos needed for
// `gc_stop_height` computation.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_gc_boundary_after_sync() {
    run_gc_after_far_horizon_sync(None);
}

// Same as `test_gc_boundary_after_sync` but with `gc_blocks_limit=2` — the production
// default (test default is 100, which processes all blocks in a single pass). With
// gc_blocks_limit=2, GC needs many passes to clean up, exercising the incremental
// code path where `gc_stop_height` computation runs across multiple small batches —
// the specific path that failed in #2980.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_gc_incremental() {
    run_gc_after_far_horizon_sync(Some(2));
}

// Scenario: A fresh node syncs via near-horizon (BlockSync only, no epoch sync or
// state sync), then runs for several more epochs. Verify that GC correctly cleans
// up old blocks while retaining recent ones.
//
// This complements `test_gc_boundary_after_sync` (far-horizon) by testing GC after
// a simpler sync path. The near-horizon node has blocks from genesis to tip (all
// downloaded via block sync), unlike the far-horizon node which only has blocks from
// the sync hash onward.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs 1 block below the epoch sync horizon (height 19)
//   - Add a fresh node with gc_num_epochs_to_keep=3, epoch_sync_horizon=2
//   - Node syncs via near-horizon (BlockSync), then runs 3 more epochs
//
// Assertions:
//   - Node went through near-horizon sync (status sequence check)
//   - tail == gc_stop - 1
//   - head - gc_stop in [20, 29] (gc=3, epoch_length=10)
//   - Blocks below tail are not accessible
//   - Blocks from tail to head are accessible
//
// Replaces `gc_sync_after_sync.py`: the original pytest tested GC after multiple V1
// state syncs, but under V2 stale nodes get their DB wiped (EpochSyncDataReset),
// making the multi-sync scenario redundant. This near-horizon variant is more
// meaningful under V2.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_gc_boundary_after_near_horizon_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let mut env =
        TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(EPOCH_LENGTH).build();

    env.node_runner(0).run_until_head_height(TEST_EPOCH_SYNC_HORIZON * EPOCH_LENGTH - 1);

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(move |config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
            config.gc.gc_num_epochs_to_keep = GC_EPOCHS;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * EPOCH_LENGTH as usize);

    assert_gc_boundaries(&env, new_node_idx);
}
