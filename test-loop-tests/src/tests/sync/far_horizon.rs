//! Far-horizon sync tests.
//!
//! These test the full V2 pipeline for nodes that are many epochs behind:
//!   EpochSync → HeaderSync → StateSync → BlockSync → NoSync
//!
//! The node must do epoch sync to bootstrap, then headers to learn the chain,
//! then state sync to get shard state, then block sync to catch up.
//!
//! Note: shard shuffling is intentionally NOT enabled in these tests. Far-horizon
//! tests bootstrap a fresh observer node via epoch sync — the node is not a
//! validator and does not participate in shard assignment. Shard shuffling is
//! only relevant for the catchup path (Layer 1 tests in `state_sync.rs`), not
//! the sync handler entry path tested here.

use super::util::{
    assert_far_horizon_sync_sequence, assert_near_horizon_sync_sequence, restrict_to_single_peer,
    track_sync_status, verify_balances_on_synced_node,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{
    BalanceMismatchError, execute_money_transfers, get_next_nonce, get_shared_block_hash,
    make_accounts,
};
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::TrackedShardsConfig;
use near_client::SyncStatus;
use near_client::sync::SYNC_V2_ENABLED;
use near_client_primitives::types::ShardSyncStatus;
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};

// Scenario: A fresh node starts with only genesis data while the network is
// 5+ epochs ahead. The node must go through the complete far-horizon sync
// pipeline to catch up.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs past 5 epochs with cross-shard money transfers
//   - Add a fresh node restricted to a single source peer
//
// Assertions:
//   - New node catches up to the network tip
//   - Sync status sequence: AwaitingPeers → NoSync → EpochSync → HeaderSync
//     → StateSync → BlockSync → NoSync
//   - New node runs for 2+ additional epochs after catch-up
//   - Account balances match source validator
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

    // Verify balance consistency between synced node and source validator.
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node_idx, &accounts);

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: Same as full pipeline but with transaction_validity_period=10
// (1 epoch). Some transfers expire due to the short validity window.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, transaction_validity_period=10
//   - Network runs 5+ epochs with cross-shard money transfers (some expire)
//   - Fresh node syncs via far-horizon pipeline
//
// Assertions:
//   - Full far-horizon sync status sequence
//   - Node catches up despite expired transactions
//   - Account balances match source validator (both reflect expirations)
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

    // Verify balance consistency: some txs may have expired, but source and
    // synced node should agree on the final balances.
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node_idx, &accounts);

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: Same as full pipeline but with transaction_validity_period=1.
// Nearly all transactions expire immediately.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, transaction_validity_period=1
//   - Network runs 5+ epochs; money transfers fail (expired)
//   - Fresh node syncs via far-horizon pipeline
//
// Assertions:
//   - Full far-horizon sync status sequence
//   - Node catches up despite nearly all txs being expired
//   - Account balances match source validator (both reflect expirations)
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

    // Verify balance consistency: nearly all txs expired, but source and synced
    // node should agree on the final balances (both reflect the expired txs).
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node_idx, &accounts);

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: After a fresh node catches up via far-horizon sync, a second
// fresh node bootstraps from the first epoch-synced node (not from the
// original validators). Validates that epoch-synced nodes can serve as
// sync sources.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs 5+ epochs with cross-shard money transfers
//   - new_node0 bootstraps via epoch sync from validator 0
//   - new_node1 bootstraps via epoch sync from new_node0
//
// Assertions:
//   - Both nodes catch up to network tip
//   - Both go through the full V2 status sequence
//   - new_node1 syncs despite new_node0 lacking old headers (epoch-synced)
//   - Account balances match source validator on both nodes
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

    // Verify balance consistency on the chained node (synced via another synced node).
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node1_idx, &accounts);

    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: A non-genesis node (one that has progressed past genesis) falls
// far behind the network. When it restarts, it should detect that it's stale
// and trigger an EpochSyncDataReset shutdown signal instead of trying to sync.
//
// This is the stale-node protection: nodes with stale data must wipe their DB
// and restart fresh rather than attempt an incremental sync that would fail.
//
// Setup:
//   - 4 validators, epoch_length=10
//   - All nodes run to height 30 (3 epochs)
//   - Kill node 0, remaining 3 advance to height 80 (past epoch_sync_horizon)
//   - Restart node 0 with stale data (at height 30, network at ~80)
//
// Assertions:
//   - Node 0 is denylisted (shutdown signal fired)
//   - Node 0's head remains near the kill height (did not sync)
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

// Scenario: An archival node that falls behind the network should NOT use
// epoch sync. Archival nodes must process all blocks to maintain a complete
// history, so they enter block sync (potentially header sync first) but
// never epoch sync.
//
// Setup:
//   - 4 validators, epoch_length=10, gc_num_epochs_to_keep=20
//   - Network runs past 5 epochs (beyond default horizon of 2)
//   - Add a fresh archival node (cold_storage=true, track all shards)
//
// Assertions:
//   - Archival node catches up to network tip
//   - Sync status sequence does NOT include "EpochSync"
//   - Near-horizon status sequence (BlockSync only)
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

// Scenario: A fresh node doing far-horizon sync is killed mid-header-sync
// and restarted. On restart, the node has the epoch sync proof and partial
// headers. It should detect it still needs headers and resume HeaderSync.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs 5+ epochs with cross-shard money transfers
//   - Fresh node restricted to single peer, killed mid-HeaderSync, restarted
//
// Assertions:
//   - Restarted node catches up to network tip
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

// Scenario: A fresh node doing far-horizon sync is killed mid-state-sync
// and restarted. On restart, the node has epoch sync proof + full headers
// but no shard state. It should detect the missing state and re-enter
// StateSync.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs 5+ epochs with cross-shard money transfers
//   - Fresh node restricted to single peer, killed mid-StateSync, restarted
//
// Assertions:
//   - Restarted node catches up to network tip
//
// KNOWN FAILURE: node enters BlockSync without state.
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

// Scenario: A fresh node doing far-horizon sync is killed mid-block-sync
// and restarted. On restart, the node has epoch sync proof + headers +
// shard state but only partial blocks. It re-enters BlockSync and catches
// up. This is the easiest restart case — all state is present.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs 5+ epochs with cross-shard money transfers
//   - Fresh node restricted to single peer, killed mid-BlockSync, restarted
//
// Assertions:
//   - Restarted node catches up to network tip
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

// Scenario: A non-validator node is killed near an epoch boundary, the
// network advances past the boundary by many epochs, and the node is
// restarted. The node must sync via the far-horizon pipeline and handle
// the epoch boundary transition without panicking.
//
// Setup:
//   - 4 validators + 1 non-validator, epoch_length=10, 4 shards
//   - Run money transfers, advance to near epoch boundary (height ~8)
//   - Kill non-validator, advance validators past 5 more epochs
//   - Restart non-validator (triggers far-horizon sync)
//
// Assertions:
//   - Restarted node catches up without panics
//   - Sync includes StateSync (far-horizon pipeline)
#[test]
#[ignore] // restart recovery not yet debugged — will revisit with other restart tests
fn test_far_horizon_restart_near_epoch_boundary() {
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

    // Add a non-validator node.
    let observer_account = create_account_id("observer_node");
    let node_state = env.node_state_builder().account_id(&observer_account).build();
    env.add_node("observer_node", node_state);
    let observer_idx = env.node_datas.len() - 1;

    // Run to near epoch boundary (height 8, just before epoch 1 ends at 10).
    env.node_runner(0).run_until_head_height(epoch_length - 2);

    // Kill the non-validator near the epoch boundary.
    let observer_identifier = env.node_datas[observer_idx].identifier.clone();
    let killed_state = env.kill_node(&observer_identifier);

    // Advance validators past 5 epochs with money transfers, far beyond the
    // killed node's position at height ~8.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(5 * epoch_length);

    // Restart the killed node — it should trigger far-horizon sync.
    env.restart_node("observer-restart", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, restarted_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, restarted_idx);

    let restarted_handle = env.node_datas[restarted_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let r_h = data.get(&restarted_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            r_h == n0_h
        },
        Duration::seconds(60),
    );

    env.node_runner(restarted_idx).run_for_number_of_blocks(20);

    // Verify the node went through the far-horizon pipeline (since it was
    // killed at height 8 and network advanced to 50+, that's well past horizon).
    let borrowed_history = sync_history.borrow();
    assert!(
        borrowed_history.iter().any(|s| s == "StateSync"),
        "restarted node should have done state sync (far-horizon), got: {:?}",
        *borrowed_history,
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Scenario: Before a fresh node joins, some accounts submit staking
// transactions. The synced node must correctly retrieve the staking-modified
// trie state without "trie node missing" panics.
//
// This is a regression test: staking modifies the account's `locked` balance
// in the trie. If state sync misses trie nodes for staking-related state
// parts, the node will crash when querying those accounts.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Run initial blocks with money transfers
//   - 10 accounts submit staking transactions
//   - Run past epoch boundary so staking takes effect
//   - Fresh node syncs via far-horizon pipeline
//
// Assertions:
//   - Fresh node catches up without panics
//   - Staking accounts have non-zero locked balance on synced node
//   - Full far-horizon sync status sequence
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_staking_state() {
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

    // Run initial blocks with money transfers.
    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(10);

    // Submit staking transactions from accounts 10-19.
    // Use validator 0 to look up nonces and submit txs (user accounts aren't
    // node accounts — node_for_account only finds validators).
    let staking_accounts: Vec<AccountId> = accounts[10..20].to_vec();
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    for account in &staking_accounts {
        let node = env.node(0);
        let nonce = node.get_next_nonce(account);
        let tx = SignedTransaction::stake(
            nonce,
            account.clone(),
            &create_user_test_signer(account),
            Balance::from_near(100),
            create_test_signer(account.as_str()).public_key(),
            block_hash,
        );
        node.submit_tx(tx);
    }

    // Run past epoch boundary so staking takes effect, then continue to 5+ epochs.
    env.node_runner(0).run_until_head_height(5 * epoch_length);

    // Add a fresh node via far-horizon sync.
    let new_account = create_account_id("staking_sync_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("staking_sync_node", node_state);
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

    env.node_runner(new_node_idx).run_for_number_of_blocks(20);
    assert_far_horizon_sync_sequence(&sync_history.borrow());

    // Verify staking state on the synced node.
    let synced_node =
        TestLoopNode { data: &env.test_loop.data, node_data: &env.node_datas[new_node_idx] };
    for account in &staking_accounts {
        match synced_node.view_account_query(account) {
            Ok(view) => {
                assert!(
                    view.locked > Balance::ZERO,
                    "staking balance should be non-zero for {account}, got locked={:?}",
                    view.locked
                );
            }
            Err(near_client::QueryError::UnavailableShard { .. }) => continue,
            Err(err) => panic!("unexpected query error for {account}: {err:?}"),
        }
    }

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

// Scenario: While a fresh node is actively syncing through the far-horizon
// pipeline, payment transactions are periodically sent to the node. The node
// should not crash and should complete sync normally.
//
// This tests crash resilience: a syncing node may receive RPCs from users
// before it's fully caught up. Transactions should be gracefully rejected
// or queued without corrupting sync state.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs 5+ epochs with money transfers
//   - Fresh node added, tx injection via periodic run_for intervals
//
// Assertions:
//   - Node catches up without crashing
//   - Sync status sequence is correct
//   - At least some txs were injected (counter > 0)
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_tx_during_sync() {
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

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(5 * epoch_length);

    // Add a fresh node.
    let new_account = create_account_id("tx_sync_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("tx_sync_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    // Track sync status on the new node.
    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    // Periodically inject txs while the node is syncing. Each iteration:
    //   1. Run the test loop for step_time
    //   2. If node is not syncing, skip tx injection
    //   3. If syncing, inject a batch of txs to the syncing node
    //   4. Repeat until caught up or total_time is exhausted
    let total_time = Duration::seconds(60);
    let step_time = Duration::milliseconds(300);
    let num_steps = total_time.whole_milliseconds() / step_time.whole_milliseconds();

    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    let tx_accounts: Vec<AccountId> = accounts[50..60].to_vec();
    let rpc_sender = env.node_datas[new_node_idx].rpc_handler_sender.clone();
    let mut tx_counter: u64 = 0;

    for _ in 0..num_steps {
        env.test_loop.run_for(step_time);

        let new_h = env.test_loop.data.get(&new_node_handle).client.chain.head().unwrap().height;
        let node0_h = env.test_loop.data.get(&node0_handle).client.chain.head().unwrap().height;
        if new_h == node0_h {
            break;
        }

        if !env.test_loop.data.get(&new_node_handle).client.sync_handler.sync_status.is_syncing() {
            continue;
        }

        // Inject a batch of txs to the syncing node. Use a valid block hash
        // from a source validator and per-account nonces so the transactions
        // are not silently rejected.
        let block_hash = get_shared_block_hash(&env.node_datas[..4], &env.test_loop.data);
        for _ in 0..5 {
            let idx = (tx_counter as usize) % tx_accounts.len();
            let sender = &tx_accounts[idx];
            let receiver = &tx_accounts[(idx + 1) % tx_accounts.len()];
            let nonce =
                get_next_nonce(&env.test_loop.data, &env.node_datas[..4], sender) + tx_counter;
            let tx = SignedTransaction::send_money(
                nonce,
                sender.clone(),
                receiver.clone(),
                &create_user_test_signer(sender),
                Balance::from_near(1),
                block_hash,
            );
            rpc_sender.send(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
            tx_counter += 1;
        }
    }

    // Verify the node caught up.
    let new_h = env.test_loop.data.get(&new_node_handle).client.chain.head().unwrap().height;
    let node0_h = env.test_loop.data.get(&node0_handle).client.chain.head().unwrap().height;
    assert_eq!(new_h, node0_h, "new node failed to catch up within timeout");

    env.node_runner(new_node_idx).run_for_number_of_blocks(20);

    assert_far_horizon_sync_sequence(&sync_history.borrow());

    // Verify that txs were actually injected during sync.
    assert!(
        tx_counter > 0,
        "expected at least some txs to be injected during sync, got {tx_counter}"
    );
    tracing::info!("injected {tx_counter} txs during sync without crash");

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
