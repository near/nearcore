use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::types::AccountId;
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;

const NUM_SHARDS: usize = 4;

/// This test checks that the genesis congestion control info is saved into DB and not cleaned during GC,
/// so that client can use it to bootstrap the genesis congestion control info after restarting.
/// Restarting is the node is not checked here but in python/nayduck tests.
#[test]
fn test_congestion_control_genesis_bootstrap() {
    if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();

    let builder = TestLoopBuilder::new();

    let initial_balance = 10000 * ONE_NEAR;
    let accounts = ["test0", "test1"];
    let clients: Vec<AccountId> = accounts.iter().map(|account| account.parse().unwrap()).collect();

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .protocol_version_latest()
        .shard_layout_simple_v1(&["account3", "account5", "account7"])
        .validators_desired_roles(&accounts[0..1], &accounts[1..2])
        .minimum_validators_per_shard(1);

    for i in 0..clients.len() {
        genesis_builder.add_user_account_simple(clients[i].clone(), initial_balance);
    }

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis_and_epoch_config_store(genesis_builder.build())
        .clients(clients.clone())
        .build();

    test_loop.run_for(Duration::seconds(5));

    for i in 0..clients.len() {
        check_genesis_congestion_info_in_store(
            &mut test_loop.data.get_mut(&node_datas[i].client_sender.actor_handle()).client,
        );
    }

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}

fn check_genesis_congestion_info_in_store(client: &mut Client) {
    let gc_config = client.config.gc.clone();
    client.chain.clear_data(&gc_config).unwrap();

    let infos = near_store::get_genesis_congestion_infos(client.chain.chain_store().store())
        .unwrap()
        .unwrap();
    assert_eq!(infos.len(), NUM_SHARDS);
    for i in 0..NUM_SHARDS {
        assert_eq!(infos[i].buffered_receipts_gas(), 0);
        assert_eq!(infos[i].delayed_receipts_gas(), 0);
        assert_eq!(infos[i].receipt_bytes(), 0);
    }
}
