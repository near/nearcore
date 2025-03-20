use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::types::{AccountId, ShardId};
use near_primitives::utils::{get_block_shard_id, get_block_shard_id_rev};
use near_primitives::version::ProtocolFeature;
use near_store::DBCol;
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;

use crate::setup;
use crate::setup::builder::TestLoopBuilder;
use crate::utils::retrieve_client_actor;
use crate::utils::setups::derive_new_epoch_config_from_boundary;

// We set small gc_step_period in tests to help make sure gc runs at least as often as blocks are
// produced.
const GC_STEP_PERIOD: Duration = Duration::milliseconds(setup::builder::MIN_BLOCK_PROD_TIME as i64);

#[test]
fn test_state_transition_data_gc_simple() {
    init_test_logger();

    let chunk_producer = "cp0";
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &[]);

    let shard_layout = ShardLayout::single_shard();
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(shard_layout.clone())
        .build();

    let epoch_config_store = TestEpochConfigBuilder::build_store_from_genesis(&genesis);
    let client: AccountId = chunk_producer.parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(vec![client.clone()])
        .epoch_config_store(epoch_config_store)
        .config_modifier(move |config, _client_index| {
            config.gc.gc_step_period = GC_STEP_PERIOD;
        })
        .build()
        .warmup();

    env.test_loop.run_for(Duration::seconds(20));

    assert_state_transition_data_is_cleared(
        &retrieve_client_actor(&env.node_datas, &mut env.test_loop.data, &client)
            .client
            .chain
            .chain_store,
        &shard_layout.shard_ids().collect(),
    );

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

#[test]
fn test_state_transition_data_gc_when_resharding() {
    init_test_logger();

    let base_shard_layout = ShardLayout::multi_shard(3, 3);
    let epoch_length = 5;
    let chunk_producer = "cp0";
    let validators_spec = ValidatorsSpec::desired_roles(&[chunk_producer], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(ProtocolFeature::SimpleNightshadeV4.protocol_version())
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .epoch_length(epoch_length)
        .build();

    let base_epoch_config = TestEpochConfigBuilder::from_genesis(&genesis).build();

    let new_epoch_config = {
        let boundary_account: AccountId = "account6".parse().unwrap();
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account)
    };
    let new_shard_layout = new_epoch_config.shard_layout.clone();

    let epoch_configs = vec![
        (genesis.config.protocol_version, Arc::new(base_epoch_config)),
        (genesis.config.protocol_version + 1, Arc::new(new_epoch_config)),
    ];
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let client: AccountId = chunk_producer.parse().unwrap();
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(vec![client])
        .epoch_config_store(epoch_config_store)
        .gc_num_epochs_to_keep(5)
        .config_modifier(move |config, _client_index| {
            config.gc.gc_step_period = GC_STEP_PERIOD;
        })
        .build()
        .warmup();

    let client_handle = env.node_datas[0].client_sender.actor_handle();
    let chain_store = env.test_loop.data.get(&client_handle).client.chain.chain_store.clone();
    let epoch_manager = env.test_loop.data.get(&client_handle).client.epoch_manager.clone();

    assert_state_transition_data_is_cleared(&chain_store, &base_shard_layout.shard_ids().collect());

    env.test_loop.run_until(
        |_| {
            let prev_hash = chain_store.final_head().unwrap().prev_block_hash;
            let prev_block = chain_store.get_block(&prev_hash).unwrap();
            let epoch_id = prev_block.header().epoch_id();

            // Because GC is async we need to wait for one extra block after epoch changes to make
            // sure it runs after shard layout change.
            epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout == new_shard_layout
        },
        Duration::seconds((3 * epoch_length) as i64),
    );

    assert_state_transition_data_is_cleared(&chain_store, &new_shard_layout.shard_ids().collect());

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

fn assert_state_transition_data_is_cleared(
    chain_store: &ChainStoreAdapter,
    expected_shard_ids: &HashSet<ShardId>,
) {
    let final_block_height = chain_store.final_head().unwrap().height;
    let store = chain_store.store().store();
    for res in store.iter(DBCol::StateTransitionData) {
        let (block_hash, shard_id) = get_block_shard_id_rev(&res.unwrap().0).unwrap();
        let block_height = chain_store.get_block_height(&block_hash).unwrap();
        assert!(
            expected_shard_ids.contains(&shard_id),
            "StateTransitionData gc didn't clear data for shard {shard_id}"
        );
        // Ideally data for all blocks with height < final_block_height should be gc-ed, however gc
        // is run async and may not run right after a new block is finalized. Because of this we're using
        // weaker check here.
        assert!(
            block_height >= final_block_height - 1,
            "StateTransitionData data contains too old block at height {block_height} while final_block_height is {final_block_height}"
        );
    }

    let head_height = chain_store.head().unwrap().height;
    for height in final_block_height..head_height {
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        for shard_id in expected_shard_ids {
            assert!(
                store
                    .get(DBCol::StateTransitionData, &get_block_shard_id(&block_hash, *shard_id))
                    .unwrap()
                    .is_some(),
                "StateTransitionData missing for shard id {shard_id} and height {height}",
            );
        }
    }
}
