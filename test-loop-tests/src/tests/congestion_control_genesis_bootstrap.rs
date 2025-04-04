use near_async::time::Duration;
use near_chain::ChainStoreAccess;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_client::Client;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::AccountId;

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
    let shard_layout = ShardLayout::simple_v1(&["account3", "account5", "account7"]);
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

fn check_genesis_congestion_info_in_store(client: &mut Client) {
    let gc_config = client.config.gc.clone();
    let signer = client.validator_signer.get();
    let me = signer.as_ref().map(|signer| signer.validator_id());
    client.chain.clear_data(&gc_config, me).unwrap();

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
