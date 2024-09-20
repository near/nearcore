use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_chain_configs::{Genesis, GenesisConfig};
use near_client::test_utils::test_loop::ClientQueries;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;
use near_store::{DBCol, Store};
use tempfile::TempDir;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::transactions::execute_money_transfers;
use crate::test_loop::utils::ONE_NEAR;
use near_async::messaging::CanSend;
use near_chain::{ChainStore, ChainStoreAccess};
use near_client::sync::epoch::EpochSync;
use near_client::SetNetworkInfo;
use near_network::types::{HighestHeightPeerInfo, NetworkInfo, PeerInfo};
use near_primitives::block::GenesisId;
use near_primitives::epoch_sync::EpochSyncProof;
use near_primitives::hash::CryptoHash;
use near_store::test_utils::create_test_store;
use std::cell::RefCell;
use std::rc::Rc;

struct TestNetworkSetup {
    tempdir: TempDir,
    genesis: Genesis,
    accounts: Vec<AccountId>,
    stores: Vec<Store>,
}

fn setup_initial_blockchain(num_clients: usize) -> TestNetworkSetup {
    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts =
        (0..100).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().take(num_clients).cloned().collect_vec();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .genesis_height(10000)
        .gas_prices_free()
        .gas_limit_one_petagas()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .transaction_validity_period(1000)
        .epoch_length(10)
        .validators_desired_roles(&clients.iter().map(|t| t.as_str()).collect_vec(), &[])
        .shuffle_shard_assignment_for_chunk_producers(true);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let genesis = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } =
        builder.genesis(genesis.clone()).clients(clients).build();

    let first_epoch_tracked_shards = {
        let clients = node_datas
            .iter()
            .map(|data| &test_loop.data.get(&data.client_sender.actor_handle()).client)
            .collect_vec();
        clients.tracked_shards_for_each_client()
    };
    tracing::info!("First epoch tracked shards: {:?}", first_epoch_tracked_shards);

    execute_money_transfers(&mut test_loop, &node_datas, &accounts);

    // Make sure the chain progressed for several epochs.
    assert!(
        test_loop
            .data
            .get(&node_datas[0].client_sender.actor_handle())
            .client
            .chain
            .head()
            .unwrap()
            .height
            > 10050
    );

    // Make a new TestLoopEnv, adding a new node to the network, and check that it can properly sync.
    let mut stores = Vec::new();
    for data in &node_datas {
        stores.push(
            test_loop
                .data
                .get(&data.client_sender.actor_handle())
                .client
                .chain
                .chain_store
                .store()
                .clone(),
        );
    }

    // Properly shut down the previous TestLoopEnv.
    // We must preserve the tempdir, since state dumps are stored there,
    // and are necessary for state sync to work on the new node.
    let tempdir = TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(5));

    TestNetworkSetup { tempdir, genesis, accounts, stores }
}

fn bootstrap_node_via_epoch_sync(setup: TestNetworkSetup, source_node: usize) -> TestNetworkSetup {
    tracing::info!("Starting new TestLoopEnv with new node");
    let TestNetworkSetup { genesis, accounts, mut stores, tempdir } = setup;
    let num_existing_clients = stores.len();
    let clients = accounts.iter().take(num_existing_clients + 1).cloned().collect_vec();
    stores.push(create_test_store()); // new node starts empty.

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = TestLoopBuilder::new()
        .genesis(genesis.clone())
        .clients(clients)
        .stores_override_hot_only(stores)
        .test_loop_data_dir(tempdir)
        .config_modifier(|config, _| {
            // Enable epoch sync, and make the horizon small enough to trigger it.
            config.epoch_sync.enabled = true;
            config.epoch_sync.epoch_sync_horizon = 30;
            config.epoch_sync.epoch_sync_accept_proof_max_horizon = 20;
            // Make header sync horizon small enough to trigger it.
            config.block_header_fetch_horizon = 8;
            // Make block sync horizon small enough to trigger it.
            config.block_fetch_horizon = 3;
        })
        .skip_warmup()
        .build();

    // Note: TestLoopEnv does not currently propagate the network info to other peers. This is because
    // the networking layer is completely mocked out. So in order to allow the new node to sync, we
    // need to manually propagate the network info to the new node. In this case we'll tell the new
    // node that node 0 is available to sync from.
    let source_chain =
        &test_loop.data.get(&node_datas[source_node].client_sender.actor_handle()).client.chain;
    let peer_info = HighestHeightPeerInfo {
        archival: false,
        genesis_id: GenesisId {
            chain_id: genesis.config.chain_id.clone(),
            hash: *source_chain.genesis().hash(),
        },
        highest_block_hash: source_chain.head().unwrap().last_block_hash,
        highest_block_height: source_chain.head().unwrap().height,
        tracked_shards: vec![],
        peer_info: PeerInfo {
            account_id: Some(accounts[source_node].clone()),
            addr: None,
            id: node_datas[source_node].peer_id.clone(),
        },
    };
    node_datas[num_existing_clients].client_sender.send(SetNetworkInfo(NetworkInfo {
        connected_peers: Vec::new(),
        highest_height_peers: vec![peer_info], // only this field matters.
        known_producers: vec![],
        num_connected_peers: 0,
        peer_max_count: 0,
        received_bytes_per_sec: 0,
        sent_bytes_per_sec: 0,
        tier1_accounts_data: Vec::new(),
        tier1_accounts_keys: Vec::new(),
        tier1_connections: Vec::new(),
    }));

    // Check that the new node will reach a high height as well.
    let new_node = node_datas.last().unwrap().client_sender.actor_handle();
    let sync_status_history = Rc::new(RefCell::new(Vec::new()));
    {
        let sync_status_history = sync_status_history.clone();
        test_loop.set_every_event_callback(move |test_loop_data| {
            let client = &test_loop_data.get(&new_node).client;
            let header_head_height = client.chain.header_head().unwrap().height;
            let head_height = client.chain.head().unwrap().height;
            tracing::info!(
                "New node sync status: {:?}, header head height: {:?}, head height: {:?}",
                client.sync_status,
                header_head_height,
                head_height
            );
            let sync_status = client.sync_status.as_variant_name();
            let mut history = sync_status_history.borrow_mut();
            if history.last().map(|s| s as &str) != Some(sync_status) {
                history.push(sync_status.to_string());
            }
        });
    }
    let new_node = node_datas.last().unwrap().client_sender.actor_handle();
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
            // Block sync picks up from where StateSync left off, and finishes the sync.
            "BlockSync",
            // NoSync means we're up to date.
            "NoSync"
        ]
        .into_iter()
        .map(|s| s.to_string())
        .collect::<Vec<_>>()
    );

    let mut stores = Vec::new();
    for data in &node_datas {
        stores.push(
            test_loop
                .data
                .get(&data.client_sender.actor_handle())
                .client
                .chain
                .chain_store
                .store()
                .clone(),
        );
    }

    let tempdir = TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(5));

    TestNetworkSetup { tempdir, genesis, accounts, stores }
}

// Test that a new node that only has genesis can use Epoch Sync to bring itself
// up to date.
#[test]
fn test_epoch_sync_from_genesis() {
    init_test_logger();
    let setup = setup_initial_blockchain(4);
    bootstrap_node_via_epoch_sync(setup, 0);
}

// Tests that after epoch syncing, we can use the new node to bootstrap another
// node via epoch sync.
#[test]
fn test_epoch_sync_from_another_epoch_synced_node() {
    init_test_logger();
    let setup = setup_initial_blockchain(4);
    let setup = bootstrap_node_via_epoch_sync(setup, 0);
    bootstrap_node_via_epoch_sync(setup, 4);
}

impl TestNetworkSetup {
    fn derive_epoch_sync_proof(&self, node_index: usize) -> EpochSyncProof {
        let store = self.stores[node_index].clone();
        EpochSync::derive_epoch_sync_proof(store).unwrap()
    }

    fn chain_final_head_height(&self, node_index: usize) -> u64 {
        let store = self.stores[node_index].clone();
        let chain_store = ChainStore::new(store, self.genesis.config.genesis_height, false);
        chain_store.final_head().unwrap().height
    }

    fn assert_epoch_sync_proof_existence_on_disk(&self, node_index: usize, exists: bool) {
        let store = self.stores[node_index].clone();
        let proof = store.get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[]).unwrap();
        assert_eq!(proof.is_some(), exists);
    }

    fn assert_header_existence(&self, node_index: usize, height: u64, exists: bool) {
        let store = self.stores[node_index].clone();
        let header =
            store.get_ser::<CryptoHash>(DBCol::BlockHeight, &height.to_le_bytes()).unwrap();
        assert_eq!(header.is_some(), exists);
    }
}

/// Performs some basic checks for the epoch sync proof; does not check the proof's correctness.
fn sanity_check_epoch_sync_proof(
    proof: &EpochSyncProof,
    final_head_height: u64,
    genesis_config: &GenesisConfig,
) {
    let epoch_height_of_final_block =
        (final_head_height - genesis_config.genesis_height - 1) / genesis_config.epoch_length + 1;
    let expected_current_epoch_height = epoch_height_of_final_block - 1;
    assert_eq!(
        proof.current_epoch.first_block_info_in_epoch.height(),
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
    for past_epoch in &proof.past_epochs {
        assert_eq!(
            past_epoch.last_final_block_header.height(),
            genesis_config.genesis_height + epoch_height * genesis_config.epoch_length - 2
        );
        epoch_height += 1;
    }
    assert_eq!(epoch_height, expected_current_epoch_height);
}

#[test]
fn test_initial_epoch_sync_proof_sanity() {
    init_test_logger();
    let setup = setup_initial_blockchain(4);
    let proof = setup.derive_epoch_sync_proof(0);
    let final_head_height = setup.chain_final_head_height(0);
    sanity_check_epoch_sync_proof(&proof, final_head_height, &setup.genesis.config);
    // Requesting the proof should not have persisted the proof on disk. This is intentional;
    // it is to reduce the statefulness of the system so that we may modify the way the proof
    // is presented in the future (for e.g. bug fixes) without a DB migration.
    setup.assert_epoch_sync_proof_existence_on_disk(0, false);
}

#[test]
fn test_epoch_sync_proof_sanity_from_epoch_synced_node() {
    init_test_logger();
    let setup = setup_initial_blockchain(4);
    let setup = bootstrap_node_via_epoch_sync(setup, 0);
    let old_proof = setup.derive_epoch_sync_proof(0);
    let new_proof = setup.derive_epoch_sync_proof(4);
    let final_head_height_old = setup.chain_final_head_height(0);
    let final_head_height_new = setup.chain_final_head_height(4);
    sanity_check_epoch_sync_proof(&new_proof, final_head_height_new, &setup.genesis.config);
    // Test loop shutdown mechanism should not have left any new block messages unhandled,
    // so the nodes should be at the same height in the end.
    assert_eq!(final_head_height_old, final_head_height_new);
    assert_eq!(old_proof, new_proof);

    // On the original node we should have no proof but all headers.
    setup.assert_epoch_sync_proof_existence_on_disk(0, false);
    setup.assert_header_existence(0, setup.genesis.config.genesis_height + 1, true);

    // On the new node we should have a proof but missing headers for the old epochs.
    setup.assert_epoch_sync_proof_existence_on_disk(4, true);
    setup.assert_header_existence(4, setup.genesis.config.genesis_height + 1, false);
}
