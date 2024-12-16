use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::{
    build_genesis_and_epoch_config_store, GenesisAndEpochConfigParams, ValidatorsSpec,
};
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;

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

    let accounts = ["test0", "test1"];
    let clients: Vec<AccountId> = accounts.iter().map(|account| account.parse().unwrap()).collect();

    let epoch_length = 100;
    let shard_layout = ShardLayout::simple_v1(&["account3", "account5", "account7"]);
    let validators_spec = ValidatorsSpec::desired_roles(&accounts[0..1], &accounts[1..2]);

    let (genesis, epoch_config_store) = build_genesis_and_epoch_config_store(
        GenesisAndEpochConfigParams {
            epoch_length,
            protocol_version: PROTOCOL_VERSION,
            shard_layout,
            validators_spec,
            accounts: &clients,
        },
        |genesis_builder| genesis_builder,
        |epoch_config_builder| epoch_config_builder.minimum_validators_per_shard(1),
    );

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
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
