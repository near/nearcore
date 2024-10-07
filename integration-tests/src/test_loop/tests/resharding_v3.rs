use itertools::Itertools;
use near_async::test_loop::data::TestLoopData;
use near_async::time::Duration;
use near_chain_configs::test_genesis::TestGenesisBuilder;
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, ShardId};
use near_primitives::version::{ProtocolFeature, PROTOCOL_VERSION};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::test_loop::builder::TestLoopBuilder;
use crate::test_loop::env::TestLoopEnv;
use crate::test_loop::utils::ONE_NEAR;

/// Stub for checking Resharding V3.
/// TODO(#11881): add the following scenarios:
/// - Shard ids should not be contiguous. For now we reuse existing shard id
/// which is incorrect!!!
/// - Nodes must not track all shards. State sync must succeed.
/// - Set up chunk validator-only nodes. State witness must pass validation.
/// - Tx load must be consistent. Txs and receipts must cross resharding
/// boundary. All txs must succeed.
/// - Shard layout can be taken from mainnet.
#[test]
fn test_resharding_v3() {
    if !ProtocolFeature::SimpleNightshadeV4.enabled(PROTOCOL_VERSION) {
        return;
    }

    init_test_logger();
    let builder = TestLoopBuilder::new();

    let initial_balance = 1_000_000 * ONE_NEAR;
    let epoch_length = 6;
    let accounts =
        (0..8).map(|i| format!("account{}", i).parse().unwrap()).collect::<Vec<AccountId>>();
    let clients = vec![accounts[0].clone(), accounts[3].clone(), accounts[6].clone()];
    let block_and_chunk_producers =
        clients.iter().map(|account: &AccountId| account.as_str()).collect_vec();

    // Prepare shard split configuration.
    let base_epoch_config_store = EpochConfigStore::for_chain_id("mainnet").unwrap();
    let base_protocol_version = ProtocolFeature::SimpleNightshadeV4.protocol_version() - 1;
    let mut base_epoch_config =
        base_epoch_config_store.get_config(base_protocol_version).as_ref().clone();
    base_epoch_config.validator_selection_config.shuffle_shard_assignment_for_chunk_producers =
        false;
    base_epoch_config.block_producer_kickout_threshold = 0;
    base_epoch_config.chunk_producer_kickout_threshold = 0;
    base_epoch_config.chunk_validator_only_kickout_threshold = 0;
    base_epoch_config.shard_layout = ShardLayout::v1(vec!["account3".parse().unwrap()], None, 3);
    let base_shard_layout = base_epoch_config.shard_layout.clone();
    let mut epoch_config = base_epoch_config.clone();
    let mut boundary_accounts = base_shard_layout.boundary_accounts().clone();
    let mut shard_ids: Vec<_> = base_shard_layout.shard_ids().collect();
    let max_shard_id = *shard_ids.iter().max().unwrap();
    let last_shard_id = shard_ids.pop().unwrap();
    let mut shards_split_map: BTreeMap<ShardId, Vec<ShardId>> =
        shard_ids.iter().map(|shard_id| (*shard_id, vec![*shard_id])).collect();
    // Keep this way until non-contiguous shard ids are supported.
    // let new_shards = vec![max_shard_id + 1, max_shard_id + 2];
    let new_shards = vec![max_shard_id, max_shard_id + 1];
    shard_ids.extend(new_shards.clone());
    shards_split_map.insert(last_shard_id, new_shards);
    boundary_accounts.push(AccountId::try_from("xyz.near".to_string()).unwrap());
    epoch_config.shard_layout =
        ShardLayout::v2(boundary_accounts, shard_ids, Some(shards_split_map));
    let expected_num_shards = epoch_config.shard_layout.shard_ids().count();
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
        .genesis(genesis)
        .epoch_config_store(epoch_config_store)
        .clients(clients)
        .track_all_shards()
        .build();

    let client_handle = node_datas[0].client_sender.actor_handle();
    let success_condition = |test_loop_data: &mut TestLoopData| -> bool {
        let client = &test_loop_data.get(&client_handle).client;
        let tip = client.chain.head().unwrap();
        let epoch_height =
            client.epoch_manager.get_epoch_height_from_prev_block(&tip.prev_block_hash).unwrap();
        assert!(epoch_height < 5);
        let epoch_config = client.epoch_manager.get_epoch_config(&tip.epoch_id).unwrap();
        return epoch_config.shard_layout.shard_ids().count() == expected_num_shards;
    };

    test_loop.run_until(
        success_condition,
        // Give enough time to produce ~6 epochs.
        Duration::seconds((6 * epoch_length) as i64),
    );

    TestLoopEnv { test_loop, datas: node_datas, tempdir }
        .shutdown_and_drain_remaining_events(Duration::seconds(20));
}
