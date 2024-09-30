use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::{ShardLayout, ShardLayoutV1};
use near_primitives::types::{AccountId, ShardId};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;

/// Stub for checking Resharding V3.
/// After uncommenting panics with
/// StorageInconsistentState("Failed to find root node ... in memtrie")
#[test]
#[ignore]
fn test_resharding_v3() {
    if !ProtocolFeature::SimpleNightshadeV4.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 1_000_000 * ONE_NEAR;
    let epoch_length = 10;
    let accounts =
        (0..8).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = accounts.iter().cloned().collect_vec();
    let block_and_chunk_producers = (0..8).map(|idx| accounts[idx].as_str()).collect_vec();

    // Prepare shard split configuration.
    let base_epoch_config_store = EpochConfigStore::for_chain_id("mainnet").unwrap();
    let base_protocol_version = ProtocolFeature::SimpleNightshadeV4.protocol_version() - 1;
    let mut base_epoch_config =
        base_epoch_config_store.get_config(base_protocol_version).as_ref().clone();
    base_epoch_config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers =
        false;
    let base_shard_layout = base_epoch_config.shard_layout.clone();
    let base_num_shards = base_shard_layout.shard_ids().count() as ShardId;
    let mut epoch_config = base_epoch_config.clone();
    let ShardLayout::V1(ShardLayoutV1 { mut boundary_accounts, version, .. }) =
        base_shard_layout.clone()
    else {
        panic!("Expected shard layout v1");
    };
    let mut shards_split_map: Vec<Vec<ShardId>> =
        (0..base_num_shards - 1).map(|i| vec![i]).collect();
    shards_split_map.push(vec![base_num_shards - 1, base_num_shards]);
    boundary_accounts.push(AccountId::try_from("x.near".to_string()).unwrap());
    epoch_config.shard_layout = ShardLayout::v1(boundary_accounts, Some(shards_split_map), version);
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(vec![
        (base_protocol_version, Arc::new(base_epoch_config)),
        (base_protocol_version + 1, Arc::new(epoch_config)),
    ]));

    let mut genesis_builder = TestGenesisBuilder::new();
    genesis_builder
        .genesis_time_from_clock(&builder.clock())
        .shard_layout(base_shard_layout)
        .protocol_version(base_protocol_version)
        .epoch_length(epoch_length)
        .validators_desired_roles(&block_and_chunk_producers, &[]);
    for account in &accounts {
        genesis_builder.add_user_account_simple(account.clone(), initial_balance);
    }
    let (genesis, _) = genesis_builder.build();

    let TestLoopEnv { mut test_loop, datas: node_datas, tempdir } = builder
        .genesis_and_epoch_config_store((genesis, epoch_config_store))
        .clients(clients)
        .build();

    let client_handle = node_datas[0].client_sender.actor_handle();
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();
        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
        assert!(epoch_height < 5);
        let epoch_config = client.epoch_manager.get_epoch_config(&tip.epoch_id).unwrap();
        return epoch_config.shard_layout.shard_ids().count() == base_num_shards as usize + 1;
    };

    test_loop.run_until(
        success_condition,
        // Timeout at producing 5 epochs, approximately.
        Duration::seconds((5 * epoch_length) as i64),
    );

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
