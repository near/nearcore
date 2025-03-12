use std::collections::BTreeMap;
use std::sync::Arc;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, ShardId};
use near_primitives::utils::get_block_shard_id_rev;
use near_primitives::version::ProtocolFeature;
use near_store::adapter::StoreAdapter as _;
use near_store::{DBCol, Store};

use crate::builder::TestLoopBuilder;
use crate::utils::retrieve_client_actor;
use crate::utils::setups::derive_new_epoch_config_from_boundary;

#[test]
fn test_state_transition_data_gc() {
    let chunk_producer = "cp0";
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &[]);

    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(ShardLayout::single_shard())
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let client: AccountId = chunk_producer.parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(vec![client.clone()])
        .epoch_config_store(epoch_config_store)
        .build();

    let store: Store = retrieve_client_actor(&env.datas, &mut env.test_loop.data, &client)
        .client
        .chain
        .chain_store
        .store()
        .clone();

    env.test_loop.run_for(Duration::seconds(100));
    // We should clean everyhting before final block so final block with additional 2 non-final
    // blocks in front.
    assert_eq!(store.store().iter(DBCol::StateTransitionData).count(), 3);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn reasharding() {
    let mut builder = TestLoopBuilder::new();
    // FIXME: check if required.
    // builder = builder.config_modifier(move |config, client_index| {
    //     // Adjust the resharding configuration to make the tests faster.
    //     let mut resharding_config = config.resharding_config.get();
    //     resharding_config.batch_delay = Duration::milliseconds(1);
    //     config.resharding_config.update(resharding_config);
    // });

    let base_epoch_config_store = EpochConfigStore::for_chain_id("mainnet", None).unwrap();
    let base_protocol_version = ProtocolFeature::SimpleNightshadeV4.protocol_version() - 1;
    let mut base_epoch_config =
        base_epoch_config_store.get_config(base_protocol_version).as_ref().clone();

    let num_producers = 3;
    let num_validators = 2;
    base_epoch_config.num_block_producer_seats = num_producers;
    base_epoch_config.num_chunk_producer_seats = num_producers;
    base_epoch_config.num_chunk_validator_seats = num_producers + num_validators;

    const DEFAULT_SHARD_LAYOUT_VERSION: u64 = 2;
    let base_shard_layout = get_base_shard_layout(DEFAULT_SHARD_LAYOUT_VERSION);
    base_epoch_config.shard_layout = base_shard_layout.clone();
    let new_boundary_account: AccountId = "account6".parse().unwrap();
    let epoch_config =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &new_boundary_account);

    println!(
        "Base epoch layout: {:?}",
        base_epoch_config.shard_layout.shard_ids().collect::<Vec<_>>()
    );
    println!("Epoch layout: {:?}", epoch_config.shard_layout.shard_ids().collect::<Vec<_>>());

    let mut epoch_configs = vec![
        (base_protocol_version, Arc::new(base_epoch_config.clone())),
        (base_protocol_version + 1, Arc::new(epoch_config.clone())),
    ];
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let num_epochs_to_wait = DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT;
    let epoch_length = DEFAULT_EPOCH_LENGTH;

    let chunk_producer = "cp0";
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(base_protocol_version)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout)
        .epoch_length(epoch_length)
        .build();

    let client: AccountId = chunk_producer.parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(vec![client.clone()])
        .epoch_config_store(epoch_config_store)
        .gc_num_epochs_to_keep(5)
        .build();

    const DEFAULT_EPOCH_LENGTH: u64 = 7;
    const DEFAULT_TESTLOOP_NUM_EPOCHS_TO_WAIT: u64 = 8;

    let chain_store = retrieve_client_actor(&env.datas, &mut env.test_loop.data, &client)
        .client
        .chain
        .chain_store
        .clone();

    let store: Store = chain_store.store();

    let debug = || {
        println!("------");
        for res in store.store().iter(DBCol::StateTransitionData) {
            let (block_hash, shard_id) = get_block_shard_id_rev(&res.unwrap().0).unwrap();
            let block_height = chain_store.get_block_height(&block_hash).unwrap();
            println!("block_height: {block_height} block_hash: {block_hash} shard_id {shard_id}");
        }
    };

    debug();

    assert_eq!(store.store().iter(DBCol::StateTransitionData).count(), 9);

    env.test_loop.run_for(Duration::seconds(((4) * epoch_length) as i64));

    debug();
    env.test_loop.run_for(Duration::seconds(((1) * epoch_length) as i64));

    debug();

    assert_eq!(store.store().iter(DBCol::StateTransitionData).count(), 12);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

fn get_base_shard_layout(version: u64) -> ShardLayout {
    let boundary_accounts = vec!["account1".parse().unwrap(), "account3".parse().unwrap()];
    match version {
        1 => {
            let shards_split_map = vec![vec![ShardId::new(0), ShardId::new(1), ShardId::new(2)]];
            #[allow(deprecated)]
            ShardLayout::v1(boundary_accounts, Some(shards_split_map), 3)
        }
        2 => {
            let shard_ids = vec![ShardId::new(5), ShardId::new(3), ShardId::new(6)];
            let shards_split_map = [(ShardId::new(0), shard_ids.clone())].into_iter().collect();
            let shards_split_map = Some(shards_split_map);
            ShardLayout::v2(boundary_accounts, shard_ids, shards_split_map)
        }
        _ => panic!("Unsupported shard layout version {}", version),
    }
}
