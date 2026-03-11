//! Far-horizon sync tests.
//!
//! These test the full V2 pipeline for nodes that are many epochs behind:
//!   EpochSync → HeaderSync → StateSync → BlockSync → NoSync
//!
//! The node must do epoch sync to bootstrap, then headers to learn the chain,
//! then state sync to get shard state, then block sync to catch up.

use super::util::{restrict_to_single_peer, track_sync_status};
use crate::setup::builder::TestLoopBuilder;
use crate::tests::sync::near_horizon::assert_near_horizon_sync_sequence;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{BalanceMismatchError, execute_money_transfers, make_accounts};
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::TrackedShardsConfig;
use near_client::SyncStatus;
use near_client::sync::SYNC_V2_ENABLED;
use near_client_primitives::types::ShardSyncStatus;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::Balance;

/// Expected V2 far-horizon sync status sequence.
const FAR_HORIZON_SYNC_SEQUENCE: &[&str] =
    &["AwaitingPeers", "NoSync", "EpochSync", "HeaderSync", "StateSync", "BlockSync", "NoSync"];

fn assert_far_horizon_sync_sequence(history: &[String]) {
    let expected: Vec<String> =
        FAR_HORIZON_SYNC_SEQUENCE.iter().map(|s| (*s).to_string()).collect();
    assert_eq!(history, expected.as_slice(), "unexpected sync status history");
}

// ---------------------------------------------------------------------------
// T1: Far-horizon full pipeline
// ---------------------------------------------------------------------------
//
// Scenario: A fresh node starts with only genesis data while the network is
// 5+ epochs ahead. The node must go through the complete far-horizon sync
// pipeline to catch up.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs past 5 epochs with cross-shard money transfers
//   - Add a fresh node
//   - Restrict new node to communicate with a single source peer
//
// Assertions:
//   - New node catches up to the network tip
//   - Sync status sequence matches V2 expectations:
//     AwaitingPeers → NoSync → EpochSync → HeaderSync → StateSync → BlockSync → NoSync
//   - New node runs for 2+ additional epochs after catch-up without issues
//
// Derived from: epoch_sync.rs::slow_test_epoch_sync_from_genesis
// Covers: T1, T10 (correct start_height after epoch sync), T11 (header→state transition)
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_full_pipeline() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    // 4 validators, 4 shards, epoch_length=10, 100 user accounts for money transfers.
    let accounts = make_accounts(100);
    let mut builder = TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(10);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    // Run network for 5+ epochs with cross-shard money transfers.
    // Default epoch_sync_horizon_num_epochs=2, so a fresh node at genesis will be >2 epochs
    // behind and enter far-horizon sync.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(50);

    // Add a fresh node, restrict it to talk only with validator 0.
    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    // Track sync status transitions on the new node.
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    // Run until the new node catches up to validator 0.
    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_node_handle).client.chain.head().unwrap().height;
            let node0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == node0_h
        },
        Duration::seconds(30),
    );

    // Run 2+ more epochs to verify continued operation after sync.
    env.node_runner(new_node_idx).run_for_number_of_blocks(30);

    assert_far_horizon_sync_sequence(&sync_history.borrow());

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// ---------------------------------------------------------------------------
// T1b: Far-horizon with short transaction validity period
// ---------------------------------------------------------------------------
//
// Scenario: Same as T1 but with transaction_validity_period=10 (1 epoch).
// This exercises the edge case where the sync hash must be in a recent epoch
// because older epochs' transactions have expired.
//
// Setup:
//   - Same as T1 but transaction_validity_period=10
//   - Some money transfers will expire, which is expected
//
// Assertions:
//   - Same sync status sequence as T1
//   - Node catches up despite expired transactions
//
// Derived from: epoch_sync.rs::slow_test_epoch_sync_transaction_validity_period_one_epoch
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_short_tx_validity() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let accounts = make_accounts(100);
    let mut builder = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(10)
        .transaction_validity_period(10);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    // With validity_period=10 (1 epoch), some transfers may expire.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(50);

    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_node_handle).client.chain.head().unwrap().height;
            let node0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == node0_h
        },
        Duration::seconds(30),
    );

    env.node_runner(new_node_idx).run_for_number_of_blocks(30);

    assert_far_horizon_sync_sequence(&sync_history.borrow());

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// ---------------------------------------------------------------------------
// T1c: Far-horizon with expired transactions
// ---------------------------------------------------------------------------
//
// Scenario: Same as T1 but with transaction_validity_period=1. Transactions
// expire almost immediately, testing that epoch sync + state sync handle
// expired transaction data correctly.
//
// Setup:
//   - Same as T1 but transaction_validity_period=1
//   - Money transfers are expected to fail (balance mismatch due to expired txs)
//
// Assertions:
//   - Same sync status sequence as T1
//   - Node catches up despite nearly all transactions being expired
//
// Derived from: epoch_sync.rs::slow_test_epoch_sync_with_expired_transactions
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_expired_transactions() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let accounts = make_accounts(100);
    let mut builder = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(10)
        .transaction_validity_period(1);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    // With validity_period=1, nearly all transactions expire. Expect balance mismatch.
    match execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts) {
        Ok(()) => panic!("expected money transfers to fail due to expired transactions"),
        Err(BalanceMismatchError { .. }) => {}
    }
    env.node_runner(0).run_until_head_height(50);

    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_node_handle).client.chain.head().unwrap().height;
            let node0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == node0_h
        },
        Duration::seconds(30),
    );

    env.node_runner(new_node_idx).run_for_number_of_blocks(30);

    assert_far_horizon_sync_sequence(&sync_history.borrow());

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// ---------------------------------------------------------------------------
// T19: Chained epoch sync (epoch sync from an epoch-synced node)
// ---------------------------------------------------------------------------
//
// Scenario: After a fresh node catches up via far-horizon sync, a second
// fresh node bootstraps from the first epoch-synced node (not from the
// original validators). Validates that epoch-synced nodes can serve as
// sync sources.
//
// Setup:
//   - Same as T1: network runs, fresh new_node0 bootstraps via epoch sync from validator 0
//   - Then fresh new_node1 bootstraps via epoch sync from new_node0
//
// Assertions:
//   - Both nodes catch up to network tip
//   - Both go through the full V2 status sequence
//   - new_node1's sync works even though new_node0 lacks old headers (it was epoch-synced)
//
// Derived from: epoch_sync.rs::slow_test_epoch_sync_from_another_epoch_synced_node
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_chained_epoch_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let accounts = make_accounts(100);
    let mut builder = TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(10);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(50);

    // --- Sync new_node0 from validator 0 ---
    let new_node0_account = create_account_id("new_node0");
    let node_state = env.node_state_builder().account_id(&new_node0_account).build();
    env.add_node("new_node0", node_state);
    let new_node0_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node0_idx, 0);

    let history_0 = track_sync_status(&mut env.test_loop, &env.node_datas, new_node0_idx);

    let new_node0_handle = env.node_datas[new_node0_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let n0_h = data.get(&new_node0_handle).client.chain.head().unwrap().height;
            let v0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            n0_h == v0_h
        },
        Duration::seconds(30),
    );

    assert_far_horizon_sync_sequence(&history_0.borrow());

    // --- Sync new_node1 from new_node0 (not from validators) ---
    let new_node1_account = create_account_id("new_node1");
    let node_state = env.node_state_builder().account_id(&new_node1_account).build();
    env.add_node("new_node1", node_state);
    let new_node1_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node1_idx, new_node0_idx);

    let history_1 = track_sync_status(&mut env.test_loop, &env.node_datas, new_node1_idx);

    let new_node1_handle = env.node_datas[new_node1_idx].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let n1_h = data.get(&new_node1_handle).client.chain.head().unwrap().height;
            let v0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            n1_h == v0_h
        },
        Duration::seconds(30),
    );

    assert_far_horizon_sync_sequence(&history_1.borrow());

    // Run 2+ more epochs to verify continued operation.
    env.node_runner(new_node1_idx).run_for_number_of_blocks(30);

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// ---------------------------------------------------------------------------
// T4: Stale node detection and shutdown
// ---------------------------------------------------------------------------
//
// Scenario: A non-genesis node (one that has progressed past genesis) falls
// far behind the network. When it restarts, it should detect that it's stale
// and trigger an EpochSyncDataReset shutdown signal instead of trying to sync.
//
// This is the stale-node protection: nodes with stale data must wipe their DB
// and restart fresh rather than attempt an incremental sync that would fail.
//
// Setup:
//   - 4 validators, epoch_length=10
//   - All nodes run to height ~30 (3 epochs)
//   - Kill node 0
//   - Remaining 3 nodes advance to height ~80 (5 more epochs, past epoch_sync_horizon)
//   - Restart node 0 with its stale data (at height 30, network at ~80)
//
// Assertions:
//   - Node 0 is denylisted (shutdown signal fired) rather than catching up
//   - Node 0's head remains near the kill height (did not sync)
//
// Derived from: continuous_epoch_sync.rs::test_epoch_sync_stale_node_triggers_reset
// Covers: T4 (stale node detection under SyncV2 gate)
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_stale_node_shutdown() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env = TestLoopBuilder::new().validators(4, 0).epoch_length(epoch_length).build();

    // Run all nodes to height 30 (3 epochs).
    let kill_height = 3 * epoch_length;
    env.node_runner(0).run_until_head_height(kill_height);

    // Kill node 0.
    let node0_identifier = env.node_datas[0].identifier.clone();
    let killed_state = env.kill_node(&node0_identifier);

    // Advance remaining nodes well past epoch sync horizon (default=2 epochs=20 blocks).
    // Node 0 is at height 30. Network needs to be >30+20=50 to trigger staleness.
    let target_height = kill_height + 5 * epoch_length; // height 80
    env.node_runner(1).run_until_head_height(target_height);

    // Restart node 0 with stale data.
    let restart_id = format!("{}-restart", node0_identifier);
    env.restart_node(&restart_id, killed_state);

    // Give node 0 time to detect staleness.
    env.node_runner(1).run_for_number_of_blocks(5);

    // Assert: node 0 was denylisted (shutdown signal fired for epoch sync data reset).
    assert!(
        env.test_loop.is_denylisted(&restart_id),
        "stale node should have been denylisted via EpochSyncDataReset shutdown signal"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// ---------------------------------------------------------------------------
// T8: Archival node skips epoch sync
// ---------------------------------------------------------------------------
//
// Scenario: An archival node that falls behind the network should NOT use
// epoch sync. Archival nodes must process all blocks to maintain a complete
// history, so they should enter block sync (potentially header sync first)
// but never epoch sync.
//
// Setup:
//   - 4 validators, epoch_length=10
//   - Network runs past 5 epochs
//   - Add a fresh archival node (cold_storage=true sets archive=true)
//
// Assertions:
//   - Archival node catches up to network tip
//   - Sync status sequence does NOT include "EpochSync"
//
// Covers: T8 (archival epoch sync skip)
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_archival_skips_epoch_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(20) // generous GC so blocks aren't deleted
        .build();

    // Run past 5 epochs (beyond default horizon of 2).
    env.node_runner(0).run_until_head_height(60);

    // Add fresh archival node with cold storage (sets archive=true).
    // Archival nodes must track all shards for cold storage to work.
    let archival_account = create_account_id("archival_node");
    let node_state = env
        .node_state_builder()
        .account_id(&archival_account)
        .cold_storage(true)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
        })
        .build();
    env.add_node("archival_node", node_state);
    let archival_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, archival_idx);

    // Run until caught up.
    let archival_handle = env.node_datas[archival_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let arch_h = data.get(&archival_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            arch_h == n0_h
        },
        Duration::seconds(60),
    );

    // Archival node should follow near-horizon path (no epoch sync).
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    // Verify archival node has old blocks (block at genesis+2, well before GC window).
    let archival_client = &env.test_loop.data.get(&archival_handle).client;
    let early_height = 3; // near genesis, would be GC'd on non-archival nodes
    assert!(
        archival_client.chain.chain_store().get_block_hash_by_height(early_height).is_ok(),
        "archival node should have block at height {early_height}"
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// ---------------------------------------------------------------------------
// T3: NoPeers during state sync (far horizon)
// ---------------------------------------------------------------------------
//
// Scenario: A node in the middle of state sync (far-horizon path) temporarily
// loses all peers for one or two ticks, then regains them. Sync should resume
// from where it left off — the node must NOT reset to NoSync.
//
// This is the CRITICAL fix from design-review-2: in V1, losing peers during
// sync resets the sync status, discarding all progress. V2 preserves state.
//
// Setup:
//   - Same as T1: 4 validators, fresh node doing far-horizon sync
//   - When the new node enters StateSync, temporarily block all peer
//     connections (highest_height_peers returns empty → NoPeers condition)
//   - Restore peers after 1-2 ticks
//
// Assertions:
//   - Node does NOT reset to NoSync or AwaitingPeers during the NoPeers window
//   - StateSyncStatus (sync_hash, per-shard progress) is preserved
//   - Sync resumes and completes normally after peers return
//   - Final status sequence does not contain a second NoSync→EpochSync cycle
//
// Tooling needed: peer blocking in mock peer manager (tooling gap 4.1)
// Covers: T3 (NoPeers protection during active sync)
#[test]
#[ignore] // requires peer manipulation tooling
fn test_far_horizon_no_peers_during_state_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    todo!("T3: implement NoPeers during state sync test")
}

// ---------------------------------------------------------------------------
// T15: NoPeers during header sync (far horizon)
// ---------------------------------------------------------------------------
//
// Scenario: A node in the middle of header sync (far-horizon path, after
// epoch sync completes) temporarily loses all peers. Sync should preserve
// header sync progress and resume when peers return.
//
// Setup:
//   - Same as T1: 4 validators, fresh node doing far-horizon sync
//   - When the new node enters HeaderSync, block all peer connections
//   - Restore peers after 1-2 ticks
//
// Assertions:
//   - Node remains in HeaderSync during NoPeers window
//   - Header sync progress (current_height) is preserved
//   - Sync resumes and completes normally after peers return
//
// Tooling needed: peer blocking in mock peer manager (tooling gap 4.1)
// Covers: T15 (NoPeers during header sync)
#[test]
#[ignore] // requires peer manipulation tooling
fn test_far_horizon_no_peers_during_header_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    todo!("T15: implement NoPeers during header sync test")
}

// ===========================================================================
// Restart-during-sync tests (far horizon)
// ===========================================================================
//
// These tests verify that a node killed at various points during far-horizon
// sync can restart with its persisted DB and recover. Each test:
//   1. Sets up a 4-validator network at height 50+
//   2. Adds a fresh node doing far-horizon sync (restricted to single peer)
//   3. Waits until the node enters the target sync phase
//   4. Kills the node
//   5. Restarts with same store
//   6. Asserts the restarted node catches up to the network tip
//
// Currently, restart during header sync and state sync are known failures —
// the restarted node does not correctly resume the interrupted phase. These
// are marked #[ignore] and will be enabled as the V2 handler gains restart
// recovery logic.

// ---------------------------------------------------------------------------
// Restart during header sync (far horizon)
// ---------------------------------------------------------------------------
//
// On restart, the node has the epoch sync proof and partial headers. It
// should detect it still needs headers and resume HeaderSync.
//
// KNOWN FAILURE: node does not resume header sync correctly after restart.
#[test]
#[ignore] // restart recovery during header sync not yet implemented
fn test_far_horizon_restart_during_header_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let accounts = make_accounts(100);
    let mut builder = TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(10);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(50);

    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    // Run until new node is in the MIDDLE of HeaderSync (current_height > start_height).
    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let status = &data.get(&new_node_handle).client.sync_handler.sync_status;
            matches!(status, SyncStatus::HeaderSync { start_height, current_height, .. }
                if *current_height > *start_height)
        },
        Duration::seconds(20),
    );

    // Kill mid-header-sync.
    let new_node_identifier = env.node_datas.last().unwrap().identifier.clone();
    let killed_state = env.kill_node(&new_node_identifier);

    // Restart with same store (has epoch sync proof + partial headers).
    env.restart_node("restart_header_sync", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, restarted_idx, 0);

    // Should catch up to network tip.
    let restarted_handle = env.node_datas[restarted_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&restarted_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == n0_h
        },
        Duration::seconds(60),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// ---------------------------------------------------------------------------
// Restart during state sync (far horizon)
// ---------------------------------------------------------------------------
//
// On restart, the node has epoch sync proof + full headers but no shard
// state. It should detect the missing state and re-enter StateSync.
//
// KNOWN FAILURE: node sees headers are caught up and enters BlockSync
// without state, causing failures. Will be fixed by restart recovery logic.
#[test]
#[ignore] // restart recovery during state sync not yet implemented
fn test_far_horizon_restart_during_state_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let accounts = make_accounts(100);
    let mut builder = TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(10);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(50);

    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    // Run until new node is in the MIDDLE of StateSync (at least one shard downloading parts).
    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let status = &data.get(&new_node_handle).client.sync_handler.sync_status;
            match status {
                SyncStatus::StateSync(state_sync_status) => {
                    // At least one shard has progressed past the initial header download.
                    state_sync_status.sync_status.values().any(|shard_status| {
                        !matches!(shard_status, ShardSyncStatus::StateDownloadHeader)
                    })
                }
                _ => false,
            }
        },
        Duration::seconds(20),
    );

    // Kill mid-state-sync.
    let new_node_identifier = env.node_datas.last().unwrap().identifier.clone();
    let killed_state = env.kill_node(&new_node_identifier);

    // Restart with same store (has epoch sync proof + headers, but no state).
    env.restart_node("restart_state_sync", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, restarted_idx, 0);

    // Should catch up to network tip.
    let restarted_handle = env.node_datas[restarted_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&restarted_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == n0_h
        },
        Duration::seconds(60),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// ---------------------------------------------------------------------------
// Restart during block sync (far horizon)
// ---------------------------------------------------------------------------
//
// On restart, the node has epoch sync proof + headers + shard state but
// only partial blocks. It should re-enter BlockSync and catch up.
// This is the easiest restart case — the node has all the state it needs,
// it just needs to download remaining blocks.
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_restart_during_block_sync() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let accounts = make_accounts(100);
    let mut builder = TestLoopBuilder::new().validators(4, 0).num_shards(4).epoch_length(10);
    for acc in &accounts {
        builder = builder.add_user_account(acc, Balance::from_near(1_000_000));
    }
    let mut env = builder.build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(50);

    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    // Run until new node is in the MIDDLE of BlockSync (current_height > start_height).
    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let status = &data.get(&new_node_handle).client.sync_handler.sync_status;
            matches!(status, SyncStatus::BlockSync { start_height, current_height, .. }
                if *current_height > *start_height)
        },
        Duration::seconds(30),
    );

    // Kill mid-block-sync.
    let new_node_identifier = env.node_datas.last().unwrap().identifier.clone();
    let killed_state = env.kill_node(&new_node_identifier);

    // Restart with same store (has epoch sync proof + headers + state, partial blocks).
    env.restart_node("restart_block_sync", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, restarted_idx, 0);

    // Should catch up to network tip.
    let restarted_handle = env.node_datas[restarted_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&restarted_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == n0_h
        },
        Duration::seconds(60),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
