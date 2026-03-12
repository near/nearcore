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
    TEST_EPOCH_SYNC_HORIZON, assert_far_horizon_sync_sequence, assert_near_horizon_sync_sequence,
    restrict_to_single_peer, run_until_synced, track_sync_status, verify_balances_on_synced_node,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::node::TestLoopNode;
use crate::utils::transactions::{
    BalanceMismatchError, execute_money_transfers, get_shared_block_hash, make_accounts,
};
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::TrackedShardsConfig;
use near_client::SyncStatus;
use near_client::sync::SYNC_V2_ENABLED;
use near_o11y::testonly::init_test_logger;
use near_primitives::test_utils::{create_test_signer, create_user_test_signer};
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};

/// Height past the epoch sync horizon for far-horizon tests.
fn far_horizon_height(epoch_length: u64) -> u64 {
    (TEST_EPOCH_SYNC_HORIZON + 3) * epoch_length
}

// Scenario: A fresh node starts with only genesis data while the network is
// 5+ epochs ahead. The node must go through the complete far-horizon sync
// pipeline to catch up.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs past the epoch sync horizon with cross-shard money transfers
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

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

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
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

    assert_far_horizon_sync_sequence(&sync_history.borrow());
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node_idx, &accounts);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: Same as full pipeline but with transaction_validity_period=10
// (1 epoch). Some cross-epoch transfers may expire due to the short validity
// window, but `execute_money_transfers` still returns Ok because it verifies
// balances after execution and the on-chain state is consistent.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, transaction_validity_period=10
//   - Network runs past the horizon with cross-shard money transfers
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

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .transaction_validity_period(10)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

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
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

    assert_far_horizon_sync_sequence(&sync_history.borrow());
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, new_node_idx, &accounts);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: Same as full pipeline but with transaction_validity_period=1.
// Nearly all transactions expire immediately.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, transaction_validity_period=1
//   - Network runs past the horizon; money transfers fail (expired)
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

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .transaction_validity_period(1)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    // With validity_period=1, nearly all transactions expire before being
    // included in a block. `execute_money_transfers` detects the resulting
    // balance mismatch and returns `BalanceMismatchError`.
    match execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts) {
        Ok(()) => panic!("expected money transfers to fail due to expired transactions"),
        Err(BalanceMismatchError { .. }) => {}
    }
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

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
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    env.node_runner(new_node_idx).run_for_number_of_blocks(3 * epoch_length as usize);

    assert_far_horizon_sync_sequence(&sync_history.borrow());
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
//   - Network runs past the horizon with cross-shard money transfers
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

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

    // Sync new_node0 from validator 0.
    let new_node0_account = create_account_id("new_node0");
    let node_state = env
        .node_state_builder()
        .account_id(&new_node0_account)
        .config_modifier(|config| {
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node0", node_state);
    let new_node0_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node0_idx, 0);

    let history_0 = track_sync_status(&mut env.test_loop, &env.node_datas, new_node0_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node0_idx, 0);
    assert_far_horizon_sync_sequence(&history_0.borrow());

    // Sync new_node1 from new_node0 (not from validators).
    let new_node1_account = create_account_id("new_node1");
    let node_state = env
        .node_state_builder()
        .account_id(&new_node1_account)
        .config_modifier(|config| {
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node1", node_state);
    let new_node1_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node1_idx, new_node0_idx);

    let history_1 = track_sync_status(&mut env.test_loop, &env.node_datas, new_node1_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node1_idx, 0);
    assert_far_horizon_sync_sequence(&history_1.borrow());

    env.node_runner(new_node1_idx).run_for_number_of_blocks(3 * epoch_length as usize);
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
//   - Kill node 0, remaining 3 advance past the epoch sync horizon
//   - Restart node 0 with stale data
//
// Assertions:
//   - Node 0 detects it is stale, triggers EpochSyncDataReset, and gets denylisted
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

    let kill_height = 3 * epoch_length;
    env.node_runner(0).run_until_head_height(kill_height);

    // Node 0 shuts down (simulated kill).
    let node0_identifier = env.node_datas[0].identifier.clone();
    let killed_state = env.kill_node(&node0_identifier);

    // Advance remaining nodes well past epoch sync horizon.
    let target_height = kill_height + (TEST_EPOCH_SYNC_HORIZON + 3) * epoch_length;
    env.node_runner(1).run_until_head_height(target_height);

    // Restart node 0 — it detects staleness and gets denylisted.
    let restart_id = format!("{}-restart", node0_identifier);
    env.restart_node(&restart_id, killed_state);
    env.node_runner(1).run_for_number_of_blocks(5);

    assert!(
        env.test_loop.is_denylisted(&restart_id),
        "stale node should have been denylisted via EpochSyncDataReset shutdown signal"
    );
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: An archival node that falls behind the network should NOT use
// epoch sync. Archival nodes must process all blocks to maintain a complete
// history, so they enter block sync (potentially header sync first) but
// never epoch sync.
//
// Note: We use `node_state_builder().cold_storage(true)` to add an archival
// node after the initial build, because `enable_archival_node()` on
// TestLoopBuilder creates a node at build time with full history from genesis,
// which would defeat the purpose of testing sync.
//
// Setup:
//   - 4 validators, epoch_length=10, gc_num_epochs_to_keep=20
//   - Network runs past the horizon
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
        .gc_num_epochs_to_keep(20)
        .build();

    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .cold_storage(true)
        .config_modifier(|config| {
            config.tracked_shards_config = TrackedShardsConfig::AllShards;
            config.epoch_sync.epoch_sync_horizon_num_epochs = TEST_EPOCH_SYNC_HORIZON;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);

    assert_near_horizon_sync_sequence(&sync_history.borrow());

    // Verify archival node has old blocks (near genesis, would be GC'd on non-archival nodes).
    let archival_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let archival_client = &env.test_loop.data.get(&archival_handle).client;
    let early_height = 3;
    assert!(
        archival_client.chain.chain_store().get_block_hash_by_height(early_height).is_ok(),
        "archival node should have block at height {early_height}"
    );
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: A fresh node doing far-horizon sync is killed mid-header-sync
// and restarted. On restart, the node has the epoch sync proof and partial
// headers. It should detect it still needs headers and resume HeaderSync.
//
// KNOWN FAILURE: node does not resume header sync correctly after restart.
#[test]
#[ignore] // restart recovery during header sync not yet implemented
fn test_far_horizon_restart_during_header_sync() {
    // TODO: implement once restart recovery is supported
}

// Scenario: A fresh node doing far-horizon sync is killed mid-state-sync
// and restarted. On restart, the node has epoch sync proof + full headers
// but no shard state. It should detect the missing state and re-enter
// StateSync.
//
// KNOWN FAILURE: node enters BlockSync without state.
#[test]
#[ignore] // restart recovery during state sync not yet implemented
fn test_far_horizon_restart_during_state_sync() {
    // TODO: implement once restart recovery is supported
}

// Scenario: A fresh node doing far-horizon sync is killed mid-block-sync
// and restarted. On restart, the node has epoch sync proof + headers +
// shard state but only partial blocks. It re-enters BlockSync and catches
// up. This is the easiest restart case — all state is present.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards
//   - Network runs past the horizon with cross-shard money transfers
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

    let epoch_length = 10;
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

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

    let new_node_identifier = env.node_datas.last().unwrap().identifier.clone();
    let killed_state = env.kill_node(&new_node_identifier);

    env.restart_node("restart_block_sync", killed_state);
    let restarted_idx = env.node_datas.len() - 1;
    restrict_to_single_peer(&env.shared_state, &env.node_datas, restarted_idx, 0);

    run_until_synced(&mut env.test_loop, &env.node_datas, restarted_idx, 0);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: A non-validator node is killed near an epoch boundary, the
// network advances past the boundary by many epochs, and the node is
// restarted. The node must sync via the far-horizon pipeline and handle
// the epoch boundary transition without panicking.
//
// KNOWN FAILURE: restart recovery not yet debugged.
#[test]
#[ignore] // restart recovery not yet debugged — will revisit with other restart tests
fn test_far_horizon_restart_near_epoch_boundary() {
    // TODO: implement once restart recovery is supported
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
//   - 10 accounts submit staking transactions (100 NEAR each)
//   - Run past epoch boundary so staking takes effect
//   - Fresh node syncs via far-horizon pipeline
//
// Assertions:
//   - Fresh node catches up without panics
//   - Staking accounts have locked balance == 100 NEAR on synced node
//   - Full far-horizon sync status sequence
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_far_horizon_staking_state() {
    if !SYNC_V2_ENABLED {
        return;
    }
    init_test_logger();

    let epoch_length = 10;
    let stake_amount = Balance::from_near(100);
    let accounts = make_accounts(100);
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(epoch_length as usize);

    // Submit staking transactions from accounts 10-19.
    let staking_accounts: Vec<AccountId> = accounts[10..20].to_vec();
    let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
    for account in &staking_accounts {
        let node = env.node(0);
        let nonce = node.get_next_nonce(account);
        let tx = SignedTransaction::stake(
            nonce,
            account.clone(),
            &create_user_test_signer(account),
            stake_amount,
            create_test_signer(account.as_str()).public_key(),
            block_hash,
        );
        node.submit_tx(tx);
    }

    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

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
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);
    run_until_synced(&mut env.test_loop, &env.node_datas, new_node_idx, 0);
    env.node_runner(new_node_idx).run_for_number_of_blocks(2 * epoch_length as usize);
    assert_far_horizon_sync_sequence(&sync_history.borrow());

    let synced_node =
        TestLoopNode { data: &env.test_loop.data, node_data: &env.node_datas[new_node_idx] };
    for account in &staking_accounts {
        match synced_node.view_account_query(account) {
            Ok(view) => {
                assert_eq!(
                    view.locked, stake_amount,
                    "staking balance should be {stake_amount} for {account}, got locked={:?}",
                    view.locked
                );
            }
            Err(near_client::QueryError::UnavailableShard { .. }) => continue,
            Err(err) => panic!("unexpected query error for {account}: {err:?}"),
        }
    }
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
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
//   - Network runs past the horizon with money transfers
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
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(epoch_length)
        .add_user_accounts(&accounts, Balance::from_near(1_000_000))
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_until_head_height(far_horizon_height(epoch_length));

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
    restrict_to_single_peer(&env.shared_state, &env.node_datas, new_node_idx, 0);

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    // Periodically inject txs while the node is syncing. Each iteration:
    //   1. Run the test loop for step_time
    //   2. If caught up, stop
    //   3. If syncing, inject a batch of txs to the syncing node
    let total_time = Duration::seconds(60);
    let step_time = Duration::milliseconds(300);
    let num_steps = total_time.whole_milliseconds() / step_time.whole_milliseconds();

    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    let tx_accounts: Vec<AccountId> = accounts[50..60].to_vec();
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

        // Build txs using validator 0 (for nonce tracking and block hash) and
        // submit them to the syncing node.
        for _ in 0..5 {
            let idx = (tx_counter as usize) % tx_accounts.len();
            let sender = &tx_accounts[idx];
            let receiver = &tx_accounts[(idx + 1) % tx_accounts.len()];
            let tx = env.node(0).tx_send_money(sender, receiver, Balance::from_near(1));
            env.node(new_node_idx).submit_tx(tx);
            tx_counter += 1;
        }
    }

    let new_h = env.test_loop.data.get(&new_node_handle).client.chain.head().unwrap().height;
    let node0_h = env.test_loop.data.get(&node0_handle).client.chain.head().unwrap().height;
    assert_eq!(new_h, node0_h, "new node failed to catch up within timeout");

    env.node_runner(new_node_idx).run_for_number_of_blocks(2 * epoch_length as usize);
    assert_far_horizon_sync_sequence(&sync_history.borrow());
    assert!(tx_counter > 0, "expected at least some txs to be injected during sync");
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
