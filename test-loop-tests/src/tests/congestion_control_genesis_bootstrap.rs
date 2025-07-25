use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain::genesis::get_genesis_congestion_infos;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::Client;
use near_epoch_manager::EpochManager;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::path::Path;

use crate::setup::builder::TestLoopBuilder;
use crate::setup::env::TestLoopEnv;
use crate::utils::ONE_NEAR;

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
        .add_user_accounts_simple(&clients, 1_000_000 * ONE_NEAR)
        .build();
    let epoch_config_store = TestEpochConfigBuilder::from_genesis(&genesis)
        .minimum_validators_per_shard(1)
        .build_store_for_genesis_protocol_version();

    let TestLoopEnv { mut test_loop, node_datas, shared_state } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients.clone())
        .build()
        .warmup();

    test_loop.run_for(Duration::seconds(5));

    for i in 0..clients.len() {
        check_genesis_congestion_info_in_store(
            &mut test_loop.data.get_mut(&node_datas[i].client_sender.actor_handle()).client,
        );
    }

    TestLoopEnv { test_loop, node_datas, shared_state }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

/// Tests that genesis congestion infos computation succeeds even when genesis state is missing.
///
/// Setup:
/// 1. A network using high enough protocol version (>29)
/// 2. Genesis congestion infos is missing (DB was deleted)
/// 3. Genesis state roots point to non-existent trie data (DB was deleted)
#[test]
fn test_missing_genesis_congestion_infos_bootstrap() {
    init_test_logger();

    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION)
        .validators_spec(ValidatorsSpec::desired_roles(&["test0"], &[]))
        .add_user_accounts_simple(&["test0".parse().unwrap()], 1_000_000 * ONE_NEAR)
        .build();

    // Step 1: Initialize genesis to get state roots
    let store1 = create_test_store();
    initialize_genesis_state(store1.clone(), &genesis, None);
    let genesis_state_roots = near_store::get_genesis_state_roots(&store1).unwrap().unwrap();

    // Step 2: Create a new fresh store (equivalent to a deleted data folder)
    let store2 = create_test_store();
    let epoch_manager = EpochManager::new_arc_handle(store2.clone(), &genesis.config, None);

    // Note: with broken genesis congestion info, this call below will fail when passing store2,
    // but succeeds when passing store1
    let runtime =
        NightshadeRuntime::test(Path::new("."), store2, &genesis.config, epoch_manager.clone());

    let result = get_genesis_congestion_infos(
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &genesis_state_roots,
    );

    // Should succeed when the node is epoch synched from an empty data dir
    let computed_congestion_infos =
        result.expect("Getting genesis congestion info should not fail");

    // Single shard, so len must be 1
    assert_eq!(computed_congestion_infos.len(), 1);

    // Validate that the computed congestion info matches the original one
    let runtime =
        NightshadeRuntime::test(Path::new("."), store1, &genesis.config, epoch_manager.clone());
    let real_congestion_infos = get_genesis_congestion_infos(
        epoch_manager.as_ref(),
        runtime.as_ref(),
        &genesis_state_roots,
    )
    .unwrap();
    assert_eq!(computed_congestion_infos, real_congestion_infos);
}

fn check_genesis_congestion_info_in_store(client: &mut Client) {
    client.chain.clear_data(&client.config.gc).unwrap();

    let infos = near_store::get_genesis_congestion_infos(&client.chain.chain_store().store())
        .unwrap()
        .unwrap();
    assert_eq!(infos.len(), NUM_SHARDS);
    for i in 0..NUM_SHARDS {
        assert_eq!(infos[i].buffered_receipts_gas(), 0);
        assert_eq!(infos[i].delayed_receipts_gas(), 0);
        assert_eq!(infos[i].receipt_bytes(), 0);
    }
}
