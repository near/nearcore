use crate::setup::builder::TestLoopBuilder;
use near_async::time::Duration;
use near_chain::genesis::get_genesis_congestion_infos;
use near_chain::{Chain, ChainGenesis, ChainStoreAccess};
use near_chain_configs::Genesis;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::Client;
use near_epoch_manager::{EpochManager, EpochManagerHandle};
use near_o11y::testonly::init_test_logger;
use near_primitives::congestion_info::CongestionInfo;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, Balance, ShardId};
use near_primitives::version::PROTOCOL_VERSION;
use near_store::Store;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::path::Path;
use std::sync::Arc;

const NUM_SHARDS: usize = 4;

/// This test checks that the genesis congestion control info is saved into DB and not cleaned during GC,
/// so that client can use it to bootstrap the genesis congestion control info after restarting.
/// Restarting is the node is not checked here but in python/nayduck tests.
#[test]
fn test_congestion_control_genesis_bootstrap() {
    init_test_logger();

    let builder = TestLoopBuilder::new();

    let accounts = ["test0", "test1"];
    let clients: Vec<AccountId> = accounts.iter().map(|account| account.parse().unwrap()).collect();

    let epoch_length = 100;
    let boundary_accounts =
        ["account3", "account5", "account7"].iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let validators_spec = ValidatorsSpec::desired_roles(&accounts[0..1], &accounts[1..2]);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .epoch_length(epoch_length)
        .shard_layout(shard_layout)
        .validators_spec(validators_spec)
        .add_user_accounts_simple(&clients, Balance::from_near(1_000_000))
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(1)
        .build_store_for_genesis_protocol_version();

    let mut env = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build();

    env.test_loop.run_for(Duration::seconds(5));

    for i in 0..clients.len() {
        check_genesis_congestion_info_in_store(&mut env.node_mut(i).client_actor().client);
    }
}

/// Tests that getting the genesis congestion infos fails when they are not in
/// the database and the genesis state is missing.
///
/// Setup:
/// 1. A network using high enough protocol version (>29)
/// 2. Genesis congestion infos is missing (DB was deleted)
/// 3. Genesis state roots point to non-existent trie data (DB was deleted)
#[test]
fn test_missing_genesis_congestion_infos_is_an_error() {
    init_test_logger();

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION)
        .validators_spec(ValidatorsSpec::desired_roles(&["test0"], &[]))
        .add_user_accounts_simple(&["test0".parse().unwrap()], Balance::from_near(1_000_000))
        .build();

    // Step 1: Initialize genesis to get state roots
    let store1 = create_test_store();
    initialize_genesis_state(store1.clone(), &genesis, None);
    let genesis_state_roots = near_store::get_genesis_state_roots(&store1).unwrap();

    // Step 2: Create a new fresh store (equivalent to a deleted data folder)
    let store2 = create_test_store();
    let epoch_manager = EpochManager::new_arc_handle(store2.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(Path::new("."), store2, &genesis.config, epoch_manager.clone());

    let result = get_genesis_congestion_infos(
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &genesis_state_roots,
    );

    let err = result.expect_err("missing genesis congestion infos should be an error");
    let err = err.to_string();
    assert!(err.contains("cannot build the genesis block"), "unexpected error: {err}");
}

/// Tests that the genesis block can be built without the genesis state when
/// the genesis is given by state roots and has the congestion infos.
///
/// Setup:
/// 1. A network with multiple shards, so that the congestion infos are not the
///    default ones (`allowed_shard` is the id of the shard)
/// 2. A node that has the genesis state, it builds the real genesis block
/// 3. A genesis file with the state roots and the congestion infos
/// 4. A node with an empty DB and that genesis file, without the genesis state
#[test]
fn test_genesis_congestion_infos_from_genesis_file() {
    init_test_logger();

    let boundary_accounts =
        ["account3", "account5", "account7"].iter().map(|a| a.parse().unwrap()).collect();
    let shard_layout = ShardLayout::multi_shard_custom(boundary_accounts, 1);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION)
        .shard_layout(shard_layout.clone())
        .validators_spec(ValidatorsSpec::desired_roles(&["test0"], &[]))
        .add_user_accounts_simple(&["test0".parse().unwrap()], Balance::from_near(1_000_000))
        .build();
    let chain_genesis = ChainGenesis::new(&genesis.config);

    // Step 1: The node that has the genesis state.
    let store1 = create_test_store();
    initialize_genesis_state(store1.clone(), &genesis, None);
    let genesis_state_roots = near_store::get_genesis_state_roots(&store1).unwrap();
    let epoch_manager1 = new_epoch_manager(store1.clone(), &genesis);
    let runtime1 =
        NightshadeRuntime::test(Path::new("."), store1, &genesis.config, epoch_manager1.clone());
    let (real_genesis_block, _) = Chain::make_genesis_block(
        epoch_manager1.as_ref(),
        runtime1.as_ref(),
        &chain_genesis,
        genesis_state_roots.clone(),
    )
    .unwrap();
    let real_congestion_infos = get_genesis_congestion_infos(
        epoch_manager1.as_ref(),
        runtime1.as_ref(),
        &genesis_state_roots,
    )
    .unwrap();

    // The real congestion infos are different from the default ones.
    assert_eq!(real_congestion_infos.len(), NUM_SHARDS);
    let shard_ids = shard_layout.shard_ids().collect::<Vec<_>>();
    for (shard_id, congestion_info) in shard_ids.iter().zip(&real_congestion_infos) {
        assert_eq!(ShardId::from(congestion_info.allowed_shard()), *shard_id);
    }
    let default_congestion_infos = vec![CongestionInfo::default(); NUM_SHARDS];
    assert_ne!(real_congestion_infos, default_congestion_infos);

    // Step 2: The genesis with the state roots and the congestion infos,
    // serialized and deserialized as when it goes through a genesis file.
    let state_roots_genesis = Genesis::new_from_state_roots(
        genesis.config,
        genesis_state_roots.clone(),
        Some(real_congestion_infos.clone()),
    );
    let state_roots_genesis = serde_json::to_string(&state_roots_genesis).unwrap();
    let state_roots_genesis: Genesis = serde_json::from_str(&state_roots_genesis).unwrap();

    // Step 3: The node with a fresh store (equivalent to a deleted data folder)
    let store2 = create_test_store();
    initialize_genesis_state(store2.clone(), &state_roots_genesis, None);
    let epoch_manager2 = new_epoch_manager(store2.clone(), &state_roots_genesis);
    let runtime2 = NightshadeRuntime::test(
        Path::new("."),
        store2,
        &state_roots_genesis.config,
        epoch_manager2.clone(),
    );
    let (genesis_block, _) = Chain::make_genesis_block(
        epoch_manager2.as_ref(),
        runtime2.as_ref(),
        &chain_genesis,
        genesis_state_roots.clone(),
    )
    .unwrap();
    let congestion_infos = get_genesis_congestion_infos(
        epoch_manager2.as_ref(),
        runtime2.as_ref(),
        &genesis_state_roots,
    )
    .unwrap();

    assert_eq!(congestion_infos, real_congestion_infos);
    assert_eq!(genesis_block.hash(), real_genesis_block.hash());
}

fn new_epoch_manager(store: Store, genesis: &Genesis) -> Arc<EpochManagerHandle> {
    let epoch_config_store =
        TestEpochConfigBuilder::from_genesis(genesis).build_store_for_genesis_protocol_version();
    EpochManager::new_arc_handle_from_epoch_config_store(store, &genesis.config, epoch_config_store)
}

fn check_genesis_congestion_info_in_store(client: &mut Client) {
    client.chain.clear_data(&client.config.gc).unwrap();

    let infos =
        near_store::get_genesis_congestion_infos(&client.chain.chain_store().store()).unwrap();
    assert_eq!(infos.len(), NUM_SHARDS);
    for i in 0..NUM_SHARDS {
        assert_eq!(infos[i].buffered_receipts_gas(), 0);
        assert_eq!(infos[i].delayed_receipts_gas(), 0);
        assert_eq!(infos[i].receipt_bytes(), 0);
    }
}
