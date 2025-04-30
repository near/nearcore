use std::cell::RefCell;
use std::rc::Rc;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::GenesisConfig;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::sync::epoch::EpochSync;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_sync::EpochSyncProof;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, BlockHeightDelta};
use near_primitives::utils::compression::CompressedData;
use near_store::adapter::StoreAdapter;

use crate::setup::builder::{NodeStateBuilder, TestLoopBuilder};
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::client_queries::ClientQueries;
use crate::utils::transactions::{BalanceMismatchError, execute_money_transfers};

const NUM_CLIENTS: usize = 4;

fn setup_initial_blockchain(transaction_validity_period: BlockHeightDelta) -> TestLoopEnv {
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(NUM_CLIENTS).cloned().collect_vec();

    let epoch_length = 10;
    let shard_layout = ShardLayout::simple_v1(&["account3", "account5", "account7"]);
    let validators_spec =
        ValidatorsSpec::desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[]);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&accounts, 1_000_000 * ONE_NEAR)
        .genesis_height(10000)
        .transaction_validity_period(transaction_validity_period)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = TestLoopBuilder::new()
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .build()
        .warmup();

    let first_epoch_tracked_shards = {
        let clients = node_datas
            .iter()
            .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
            .collect_vec();
        clients.tracked_shards_for_each_client()
    };
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    if transaction_validity_period <= 1 {
        // If we're testing handling expired transactions, the money transfers should fail at the end when checking
        // account balances, since some transactions expired.
        match execute_money_transfers(&mut test_loop, &node_datas, &accounts) {
            Ok(()) => panic!("Expected money transfers to fail due to expired transactions"),
            Err(BalanceMismatchError { .. }) => {}
        }
    } else {
        execute_money_transfers(&mut test_loop, &node_datas, &accounts).unwrap();
    }

    // Make sure the chain progressed for several epochs.
    let client_actor = test_loop.data.get(&node_datas[0].client_sender.actor_handle());
    assert!(client_actor.client.chain.head().unwrap().height > 10050);

    TestLoopEnv { test_loop, node_datas, shared_state }
}

fn bootstrap_node_via_epoch_sync(mut env: TestLoopEnv, source_node: usize) -> TestLoopEnv {
    let genesis = env.shared_state.genesis.clone();
    let tempdir_path = env.shared_state.tempdir.path().to_path_buf();
    let identifier = format!("account{}", env.node_datas.len());
    let account_id = identifier.parse().unwrap();
    let node_state = NodeStateBuilder::new(genesis, tempdir_path)
        .account_id(account_id)
        .config_modifier(|config| {
            // Enable epoch sync, and make the horizon small enough to trigger it.
            config.epoch_sync.epoch_sync_horizon = 30;
            // Make header sync horizon small enough to trigger it.
            config.block_header_fetch_horizon = 8;
            // Make block sync horizon small enough to trigger it.
            config.block_fetch_horizon = 3;
        })
        .build();
    env.add_node(&identifier, node_state);

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = env;

    // Allow talking only with the source node.
    let new_node_peer_id = node_datas.last().unwrap().peer_id.clone();
    shared_state.network_shared_state.allow_all_requests();
    for (index, data) in node_datas[..node_datas.len() - 1].iter().enumerate() {
        if index != source_node {
            shared_state
                .network_shared_state
                .disallow_requests(data.peer_id.clone(), new_node_peer_id.clone());
            shared_state
                .network_shared_state
                .disallow_requests(new_node_peer_id.clone(), data.peer_id.clone());
        }
    }

    // Check that the new node will reach a high height as well.
    let client_sender = &node_datas.last().unwrap().client_sender;
    let new_node = client_sender.actor_handle();
    let sync_status_history = Rc::new(RefCell::new(Vec::new()));
    {
        let sync_status_history = sync_status_history.clone();
        test_loop.set_every_event_callback(move |test_loop_data| {
            let client = &test_loop_data.get(&new_node).client;
            let header_head_height = client.chain.header_head().unwrap().height;
            let head_height = client.chain.head().unwrap().height;
            tracing::info!(
                "New node sync status: {:?}, header head height: {:?}, head height: {:?}",
                client.sync_handler.sync_status,
                header_head_height,
                head_height
            );
            let sync_status = client.sync_handler.sync_status.as_variant_name();
            let mut history = sync_status_history.borrow_mut();
            if history.last().map(|s| s as &str) != Some(sync_status) {
                history.push(sync_status.to_string());
            }
        });
    }
    let new_node = client_sender.actor_handle();
    let node0 = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            let new_node_height = test_loop_data.get(&new_node).client.chain.head().unwrap().height;
            let node0_height = test_loop_data.get(&node0).client.chain.head().unwrap().height;
            new_node_height == node0_height
        },
        Duration::seconds(20),
    );

    let current_height = test_loop.data.get(&node0).client.chain.head().unwrap().height;
    // Run for at least two more epochs to make sure everything continues to be fine.
    test_loop.run_until(
        |test_loop_data| {
            let new_node_height = test_loop_data.get(&new_node).client.chain.head().unwrap().height;
            new_node_height >= current_height + 30
        },
        Duration::seconds(30),
    );
    assert_eq!(
        sync_status_history.borrow().as_slice(),
        &[
            // Initial state.
            "AwaitingPeers",
            // State after having enough peers.
            "NoSync",
            // EpochSync should be entered first.
            "EpochSync",
            // EpochSync should succeed.
            "EpochSyncDone",
            // Header sync happens next to bring forward HEADER_HEAD.
            "HeaderSync",
            // State sync downloads the state from state dumps.
            "StateSync",
            // State sync is done.
            "StateSyncDone",
            // Block sync picks up from where StateSync left off, and finishes the sync.
            "BlockSync",
            // NoSync means we're up to date.
            "NoSync"
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
    );

    TestLoopEnv { test_loop, node_datas, shared_state }
}

// Test that a new node that only has genesis can use Epoch Sync to bring itself
// up to date.
#[test]
fn slow_test_epoch_sync_from_genesis() {
    init_test_logger();
    let env = setup_initial_blockchain(20);
    let env = bootstrap_node_via_epoch_sync(env, 0);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

// Tests that after epoch syncing, we can use the new node to bootstrap another
// node via epoch sync.
#[test]
fn slow_test_epoch_sync_from_another_epoch_synced_node() {
    init_test_logger();
    let env = setup_initial_blockchain(20);
    let env = bootstrap_node_via_epoch_sync(env, 0);
    let env = bootstrap_node_via_epoch_sync(env, 4);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

#[test]
fn slow_test_epoch_sync_transaction_validity_period_one_epoch() {
    init_test_logger();
    let env = setup_initial_blockchain(10);
    let env = bootstrap_node_via_epoch_sync(env, 0);
    let env = bootstrap_node_via_epoch_sync(env, 4);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

#[test]
fn slow_test_epoch_sync_with_expired_transactions() {
    init_test_logger();
    let env = setup_initial_blockchain(1);
    let env = bootstrap_node_via_epoch_sync(env, 0);
    let env = bootstrap_node_via_epoch_sync(env, 4);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

impl TestLoopEnv {
    fn derive_epoch_sync_proof(&self, node_index: usize) -> EpochSyncProof {
        let client_handle = self.node_datas[node_index].client_sender.actor_handle();
        let store = self.test_loop.data.get(&client_handle).client.chain.chain_store.store();
        EpochSync::derive_epoch_sync_proof(
            store,
            self.shared_state.genesis.config.transaction_validity_period,
            Default::default(),
        )
        .unwrap()
        .decode()
        .unwrap()
        .0
    }

    fn chain_final_head_height(&self, node_index: usize) -> u64 {
        let client_handle = self.node_datas[node_index].client_sender.actor_handle();
        let chain = &self.test_loop.data.get(&client_handle).client.chain;
        chain.chain_store.final_head().unwrap().height
    }

    fn assert_epoch_sync_proof_existence_on_disk(&self, node_index: usize, exists: bool) {
        let client_handle = self.node_datas[node_index].client_sender.actor_handle();
        let store = self.test_loop.data.get(&client_handle).client.chain.chain_store.store();
        let proof = store.epoch_store().get_epoch_sync_proof().unwrap();
        assert_eq!(proof.is_some(), exists);
    }

    fn assert_header_existence(&self, node_index: usize, height: u64, exists: bool) {
        let client_handle = self.node_datas[node_index].client_sender.actor_handle();
        let store = self.test_loop.data.get(&client_handle).client.chain.chain_store.store();
        let header = store.chain_store().get_block_hash_by_height(height);
        assert_eq!(header.is_ok(), exists);
    }
}

/// Performs some basic checks for the epoch sync proof; does not check the proof's correctness.
fn sanity_check_epoch_sync_proof(
    proof: &EpochSyncProof,
    final_head_height: u64,
    genesis_config: &GenesisConfig,
    // e.g. if this is 2, it means that we're expecting the proof's "current epoch" to be two
    // epochs ago, i.e. the current epoch's previous previous epoch.
    expected_epochs_ago: u64,
) {
    let proof = proof.as_v1();
    let epoch_height_of_final_block =
        (final_head_height - genesis_config.genesis_height - 1) / genesis_config.epoch_length + 1;
    let expected_current_epoch_height = epoch_height_of_final_block - expected_epochs_ago;
    assert_eq!(
        proof.current_epoch.first_block_header_in_epoch.height(),
        genesis_config.genesis_height
            + (expected_current_epoch_height - 1) * genesis_config.epoch_length
            + 1
    );
    assert_eq!(
        proof.last_epoch.first_block_in_epoch.height(),
        genesis_config.genesis_height
            + (expected_current_epoch_height - 2) * genesis_config.epoch_length
            + 1
    );

    // EpochSyncProof starts with epoch height 2 because the first height is proven by
    // genesis.
    let mut epoch_height = 2;
    for past_epoch in &proof.all_epochs {
        assert_eq!(
            past_epoch.last_final_block_header.height(),
            genesis_config.genesis_height + epoch_height * genesis_config.epoch_length - 2
        );
        epoch_height += 1;
    }
    assert_eq!(epoch_height, expected_current_epoch_height + 1);
}

#[test]
fn slow_test_initial_epoch_sync_proof_sanity() {
    init_test_logger();
    let env = setup_initial_blockchain(20);
    let proof = env.derive_epoch_sync_proof(0);
    let final_head_height = env.chain_final_head_height(0);
    sanity_check_epoch_sync_proof(&proof, final_head_height, &env.shared_state.genesis.config, 2);
    // Requesting the proof should not have persisted the proof on disk. This is intentional;
    // it is to reduce the stateful-ness of the system so that we may modify the way the proof
    // is presented in the future (for e.g. bug fixes) without a DB migration.
    env.assert_epoch_sync_proof_existence_on_disk(0, false);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

#[test]
fn slow_test_epoch_sync_proof_sanity_from_epoch_synced_node() {
    init_test_logger();
    let env = setup_initial_blockchain(20);
    let env = bootstrap_node_via_epoch_sync(env, 0);
    let old_proof = env.derive_epoch_sync_proof(0);
    let new_proof = env.derive_epoch_sync_proof(4);
    let final_head_height_old = env.chain_final_head_height(0);
    let final_head_height_new = env.chain_final_head_height(4);
    sanity_check_epoch_sync_proof(
        &new_proof,
        final_head_height_new,
        &env.shared_state.genesis.config,
        2,
    );
    // Test loop shutdown mechanism should not have left any new block messages unhandled,
    // so the nodes should be at the same height in the end.
    assert_eq!(final_head_height_old, final_head_height_new);
    assert_eq!(old_proof, new_proof);

    // On the original node we should have no proof but all headers.
    env.assert_epoch_sync_proof_existence_on_disk(0, false);
    env.assert_header_existence(0, env.shared_state.genesis.config.genesis_height + 1, true);

    // On the new node we should have a proof but missing headers for the old epochs.
    env.assert_epoch_sync_proof_existence_on_disk(4, true);
    env.assert_header_existence(4, env.shared_state.genesis.config.genesis_height + 1, false);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

#[test]
fn slow_test_epoch_sync_proof_sanity_shorter_transaction_validity_period() {
    init_test_logger();
    let env = setup_initial_blockchain(10);
    let proof = env.derive_epoch_sync_proof(0);
    let final_head_height = env.chain_final_head_height(0);
    sanity_check_epoch_sync_proof(&proof, final_head_height, &env.shared_state.genesis.config, 1);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}

#[test]
fn slow_test_epoch_sync_proof_sanity_zero_transaction_validity_period() {
    init_test_logger();
    let env = setup_initial_blockchain(0);
    let proof = env.derive_epoch_sync_proof(0);
    let final_head_height = env.chain_final_head_height(0);
    // The proof should still be for the previous epoch, for state sync purposes.
    sanity_check_epoch_sync_proof(&proof, final_head_height, &env.shared_state.genesis.config, 1);
    env.shutdown_and_drain_remaining_events(Duration::seconds(5));
}
