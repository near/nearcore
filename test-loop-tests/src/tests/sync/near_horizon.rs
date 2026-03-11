//! Near-horizon sync tests.
//!
//! These test the V2 pipeline for nodes that are only a few epochs behind:
//!   BlockSync → NoSync (no epoch sync, no state sync)
//!
//! Near-horizon means the node is within `epoch_sync_horizon` of the network
//! tip, so it enters BlockSync directly. Header sync and block sync run
//! together; the node never does epoch sync or state sync.

use super::util::{
    assert_near_horizon_sync_sequence, track_sync_status, verify_balances_on_synced_node,
};
use crate::setup::builder::TestLoopBuilder;
use crate::utils::account::create_account_id;
use crate::utils::transactions::{execute_money_transfers, get_next_nonce, get_shared_block_hash};
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::SyncStatus;
use near_client::sync::SYNC_V2_ENABLED;
use near_network::client::ProcessTxRequest;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::test_utils::create_user_test_signer;
use near_primitives::transaction::SignedTransaction;
use near_primitives::types::{AccountId, Balance};

// Scenario: A fresh node starts with only genesis data while the network is
// ~1.5 epochs ahead. The node is within epoch_sync_horizon distance (default=2),
// so it enters BlockSync directly without epoch sync or state sync.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, gc_num_epochs_to_keep=20
//   - Network runs ~1.5 epochs (15 blocks)
//   - Add a fresh node with default config
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

    // No user accounts needed — just need a short chain within the horizon.
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .num_shards(4)
        .epoch_length(10)
        .gc_num_epochs_to_keep(20) // generous GC so blocks aren't deleted
        .build();

    // Run ~1.5 epochs (within default horizon of 2 epochs = 20 blocks).
    env.node_runner(0).run_until_head_height(15);

    // Add fresh node.
    let new_account = create_account_id("new_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("new_node", node_state);
    let new_node_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_node_idx);

    // Run until caught up.
    let new_node_handle = env.node_datas[new_node_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_node_handle).client.chain.head().unwrap().height;
            let node0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == node0_h
        },
        Duration::seconds(20),
    );

    // Near-horizon: no EpochSync, no StateSync.
    assert_near_horizon_sync_sequence(&sync_history.borrow());
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Scenario: A node that is exactly at the edge of `epoch_sync_horizon` should
// enter BlockSync (not epoch sync) and still succeed because all needed blocks
// are within the GC window on peers.
//
// With default epoch_sync_horizon_num_epochs=2 and epoch_length=10, the horizon
// is 20 blocks. Running the chain to exactly height 20 puts the fresh node at
// exactly the boundary.
//
// Setup:
//   - 4 validators, epoch_length=10, 4 shards, gc_num_epochs_to_keep=3
//   - Network runs exactly 2 epochs (height 20)
//   - Add a fresh node at genesis
//
// Assertions:
//   - Node enters BlockSync (not EpochSync)
//   - Block sync succeeds (all needed blocks within GC window)
//   - Node catches up to network tip
#[test]
#[cfg_attr(feature = "protocol_feature_spice", ignore)]
fn test_near_horizon_gc_boundary() {
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

    // Run exactly 2 epochs (= horizon boundary with default horizon=2).
    env.node_runner(0).run_until_head_height(2 * epoch_length);

    // Add fresh node.
    let new_account = create_account_id("boundary_node");
    let node_state = env.node_state_builder().account_id(&new_account).build();
    env.add_node("boundary_node", node_state);
    let new_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_idx);

    let new_handle = env.node_datas[new_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == n0_h
        },
        Duration::seconds(30),
    );

    // Assert: full near-horizon sequence — BlockSync, NOT EpochSync.
    assert_near_horizon_sync_sequence(&sync_history.borrow());
    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
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
//   - Validator remains in next epoch's producer set
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
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
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
        .gc_num_epochs_to_keep(20)
        .build();

    execute_money_transfers(&mut env.test_loop, &env.node_datas, &accounts).unwrap();
    env.node_runner(0).run_for_number_of_blocks(20);

    // Two cycles: first tests restart after normal operation, second tests
    // restart after a restart (validator 0 both times).
    for i in 0..2 {
        // Kill validator 0.
        let kill_account: AccountId = "account0".parse().unwrap();
        let kill_data_idx = env.account_data_idx(&kill_account);
        let kill_identifier = env.node_datas[kill_data_idx].identifier.clone();
        let killed_state = env.kill_node(&kill_identifier);

        // Remaining 3 validators advance with cross-shard transfers.
        let live_account: AccountId = "account1".parse().unwrap();
        env.runner_for_account(&live_account).run_for_number_of_blocks(15);

        // Restart validator 0 — it should catch up via near-horizon BlockSync.
        let restart_id = format!("{}-restart-{}", kill_identifier, i);
        env.restart_node(&restart_id, killed_state);
        let restarted_idx = env.node_datas.len() - 1;

        let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, restarted_idx);

        // Run until restarted validator catches up to the live ones.
        let restarted_handle = env.node_datas[restarted_idx].client_sender.actor_handle();
        let live_data_idx = env.account_data_idx(&live_account);
        let live_handle = env.node_datas[live_data_idx].client_sender.actor_handle();
        env.test_loop.run_until(
            |data| {
                let restarted_h = data.get(&restarted_handle).client.chain.head().unwrap().height;
                let live_h = data.get(&live_handle).client.chain.head().unwrap().height;
                restarted_h == live_h
            },
            Duration::seconds(30),
        );

        // Restarted validator should use near-horizon BlockSync, not EpochSync.
        assert_near_horizon_sync_sequence(&sync_history.borrow());

        // Verify the restarted validator is still in the next epoch's producer set.
        let restarted_client = &env.test_loop.data.get(&restarted_handle).client;
        let head = restarted_client.chain.head().unwrap();
        let next_epoch_id =
            restarted_client.epoch_manager.get_next_epoch_id(&head.last_block_hash).unwrap();
        let producers = restarted_client
            .epoch_manager
            .get_epoch_block_producers_ordered(&next_epoch_id)
            .unwrap();
        let producer_accounts: Vec<_> = producers.iter().map(|p| p.account_id().clone()).collect();
        assert!(
            producer_accounts.contains(&kill_account),
            "validator {kill_account} not in next epoch's producer set after restart cycle {i}"
        );

        // Send cross-shard transfers from the restarted validator to verify it's
        // fully operational.
        let block_hash = get_shared_block_hash(&env.node_datas, &env.test_loop.data);
        for (j, sender) in accounts.iter().enumerate().take(20) {
            let receiver = &accounts[(j + 1) % accounts.len()];
            let nonce = get_next_nonce(&env.test_loop.data, &env.node_datas, sender);
            let tx = SignedTransaction::send_money(
                nonce,
                sender.clone(),
                receiver.clone(),
                &create_user_test_signer(sender),
                Balance::from_near(1),
                block_hash,
            );
            env.node_datas[restarted_idx].rpc_handler_sender.send(ProcessTxRequest {
                transaction: tx,
                is_forwarded: false,
                check_only: false,
            });
        }

        // Let txs propagate.
        env.runner_for_account(&live_account).run_for_number_of_blocks(5);
    }

    // Verify balance consistency across all validators.
    let reference_account: AccountId = "account0".parse().unwrap();
    let ref_idx = env.account_data_idx(&reference_account);
    verify_balances_on_synced_node(&env.test_loop.data, &env.node_datas, ref_idx, &accounts);

    env.shutdown_and_drain_remaining_events(Duration::seconds(20));
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

    // Add fresh node.
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

    // Kill mid-block-sync.
    let new_node_identifier = env.node_datas.last().unwrap().identifier.clone();
    let killed_state = env.kill_node(&new_node_identifier);

    // Restart with same store.
    env.restart_node("restart_block_sync", killed_state);
    let restarted_idx = env.node_datas.len() - 1;

    // Should catch up to network tip.
    let restarted_handle = env.node_datas[restarted_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&restarted_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == n0_h
        },
        Duration::seconds(30),
    );

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
    let mut env = TestLoopBuilder::new()
        .validators(4, 0)
        .epoch_length(epoch_length)
        .gc_num_epochs_to_keep(10) // peers keep all blocks
        .build();

    // Run 8 epochs (80 blocks).
    env.node_runner(0).run_until_head_height(8 * epoch_length);

    // Add a fresh node with tight GC (3 epochs) but large horizon (10 epochs).
    // 80 blocks is within horizon (100) so the node enters BlockSync.
    // But 80 blocks > gc window (30), testing the mismatch.
    let new_account = create_account_id("new_node");
    let node_state = env
        .node_state_builder()
        .account_id(&new_account)
        .config_modifier(|config| {
            config.gc.gc_num_epochs_to_keep = 3;
            config.epoch_sync.epoch_sync_horizon_num_epochs = 10;
        })
        .build();
    env.add_node("new_node", node_state);
    let new_idx = env.node_datas.len() - 1;

    let sync_history = track_sync_status(&mut env.test_loop, &env.node_datas, new_idx);

    let new_handle = env.node_datas[new_idx].client_sender.actor_handle();
    let node0_handle = env.node_datas[0].client_sender.actor_handle();
    env.test_loop.run_until(
        |data| {
            let new_h = data.get(&new_handle).client.chain.head().unwrap().height;
            let n0_h = data.get(&node0_handle).client.chain.head().unwrap().height;
            new_h == n0_h
        },
        Duration::seconds(60),
    );

    // Within the large horizon: should use near-horizon BlockSync, not EpochSync.
    assert_near_horizon_sync_sequence(&sync_history.borrow());

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}
