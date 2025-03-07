use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;
use crate::utils::transactions::execute_money_transfers;
use itertools::Itertools;
use near_async::messaging::CanSend;
use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::SetNetworkInfo;
use near_client::test_utils::test_loop::ClientQueries;
use near_network::types::{HighestHeightPeerInfo, NetworkInfo, PeerInfo};
use near_o11y::testonly::init_test_logger;
use near_primitives::block::GenesisId;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_store::test_utils::create_test_store;

const NUM_CLIENTS: usize = 4;

// Test that a new node that only has genesis can use whatever method available
// to sync up to the current state of the network.
#[test]
fn slow_test_sync_from_genesis() {
    init_test_logger();
    let builder = TestLoopBuilder::new();

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
        .transaction_validity_period(1000)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .shuffle_shard_assignment_for_chunk_producers(true)
        .build_store_for_genesis_protocol_version();
    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis.clone())
        .epoch_config_store(epoch_config_store.clone())
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

    execute_money_transfers(&mut test_loop, &node_datas, &accounts).unwrap();

    // Make sure the chain progresses for several epochs.
    let client_handle = node_datas[0].client_sender.actor_handle();
    test_loop.run_until(
        |test_loop_data| {
            test_loop_data.get(&client_handle).client.chain.head().unwrap().height > 10050
        },
        Duration::seconds(50),
    );

    // Make a new TestLoopEnv, adding a new node to the network, and check that it can properly sync.
    let mut stores = Vec::new();
    for data in &node_datas {
        stores.push((
            test_loop
                .data
                .get(&data.client_sender.actor_handle())
                .client
                .chain
                .chain_store
                .store()
                .clone(),
            None,
        ));
    }
    stores.push((create_test_store(), None)); // new node starts empty.

    // Properly shut down the previous TestLoopEnv.
    // We must preserve the tempdir, since state dumps are stored there,
    // and are necessary for state sync to work on the new node.
    let tempdir = TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));

    tracing::info!("Starting new TestLoopEnv with new node");

    let clients = accounts.iter().take(NUM_CLIENTS + 1).cloned().collect_vec();

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = TestLoopBuilder::new()
        .genesis(genesis.clone())
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .stores_override(stores)
        .test_loop_data_dir(tempdir)
        .skip_warmup()
        .build();

    // Note: TestLoopEnv does not currently propagate the network info to other peers. This is because
    // the networking layer is completely mocked out. So in order to allow the new node to sync, we
    // need to manually propagate the network info to the new node. In this case we'll tell the new
    // node that node 0 is available to sync from.
    let chain0 = &test_loop.data.get(&node_datas[0].client_sender.actor_handle()).client.chain;
    let peer_info = HighestHeightPeerInfo {
        archival: false,
        genesis_id: GenesisId { chain_id: genesis.config.chain_id, hash: *chain0.genesis().hash() },
        highest_block_hash: chain0.head().unwrap().last_block_hash,
        highest_block_height: chain0.head().unwrap().height,
        tracked_shards: vec![],
        peer_info: PeerInfo {
            account_id: Some(accounts[0].clone()),
            addr: None,
            id: node_datas[0].peer_id.clone(),
        },
    };
    node_datas[NUM_CLIENTS].client_sender.send(SetNetworkInfo(NetworkInfo {
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
    test_loop.run_until(
        |test_loop_data| test_loop_data.get(&new_node).client.chain.head().unwrap().height > 10050,
        Duration::seconds(20),
    );
    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
