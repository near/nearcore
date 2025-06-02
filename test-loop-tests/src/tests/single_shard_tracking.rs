use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use itertools::Itertools;
use near_async::time::Duration;
use near_chain_configs::test_genesis::{TestEpochConfigBuilder, ValidatorsSpec};
use near_chain_configs::{MIN_GC_NUM_EPOCHS_TO_KEEP, TrackedShardsConfig};
use near_o11y::testonly::init_test_logger;
use near_primitives::epoch_manager::EpochConfigStore;
use near_primitives::shard_layout::{ShardLayout, get_block_shard_uid, get_block_shard_uid_rev};
use near_primitives::types::AccountId;
use near_primitives::version::{PROD_GENESIS_PROTOCOL_VERSION, PROTOCOL_VERSION};
use near_store::adapter::StoreAdapter as _;
use near_store::adapter::chain_store::ChainStoreAdapter;
use near_store::{DBCol, ShardUId};

use crate::setup;
use crate::setup::builder::TestLoopBuilder;
use crate::utils::retrieve_client_actor;
use crate::utils::setups::derive_new_epoch_config_from_boundary;

// We set small gc_step_period in tests to help make sure gc runs at least as often as blocks are
// produced.
const GC_STEP_PERIOD: Duration = Duration::milliseconds(setup::builder::MIN_BLOCK_PROD_TIME as i64);
const EPOCH_LENGTH: u64 = 10;
const GC_NUM_EPOCHS_TO_KEEP: u64 = MIN_GC_NUM_EPOCHS_TO_KEEP;

/// Run an RPC node that tracks all shards except one.
/// Ensure that shard data is stored only for the shards it is tracking.
/// Verify that old data is garbage collected for all shards.
#[test]
fn test_rpc_single_shard_tracking() {
    init_test_logger();
    let validator = "cp0";
    let validator_client: AccountId = validator.parse().unwrap();
    let validators_spec = ValidatorsSpec::desired_roles(&[validator], &[]);
    let shard_layout = ShardLayout::multi_shard(3, 3);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .validators_spec(validators_spec)
        .shard_layout(shard_layout.clone())
        .epoch_length(EPOCH_LENGTH)
        .build();
    let shard_uids: Vec<ShardUId> = shard_layout.shard_uids().collect();
    let (_untracked_shard, tracked_shards) =
        shard_uids.split_last().map(|(l, r)| (*l, r.to_vec())).unwrap();
    let tracked_shards_set = tracked_shards.iter().cloned().collect();
    let epoch_config = Arc::new(TestEpochConfigBuilder::from_genesis(&genesis).build());
    let epoch_configs = (PROD_GENESIS_PROTOCOL_VERSION..=genesis.config.protocol_version)
        .map(|protocol_version| (protocol_version, epoch_config.clone()))
        .collect_vec();
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let rpc_client: AccountId = "rpc".parse().unwrap();
    let clients = vec![rpc_client.clone(), validator_client];
    let rpc_client_index = 0;
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .config_modifier(move |config, client_index| {
            if client_index != rpc_client_index {
                return;
            }
            config.gc.gc_step_period = GC_STEP_PERIOD;
            config.gc.gc_num_epochs_to_keep = GC_NUM_EPOCHS_TO_KEEP;
            config.tracked_shards_config = TrackedShardsConfig::Shards(tracked_shards.clone());
        })
        .build()
        .warmup();

    let num_blocks_to_wait = EPOCH_LENGTH * (GC_NUM_EPOCHS_TO_KEEP + 1);
    env.test_loop.run_for(Duration::seconds(num_blocks_to_wait as i64));
    let chain_store = &retrieve_client_actor(&env.node_datas, &mut env.test_loop.data, &rpc_client)
        .client
        .chain
        .chain_store;

    assert_old_chunks_are_cleared(chain_store, &tracked_shards_set);
    assert_new_chunks_exist(chain_store, &tracked_shards_set);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

/// Tests that an archival node tracking a parent shard correctly tracks its children after resharding.
/// Verifies that GC works as expected and only relevant chunk data is kept.
#[test]
fn test_archival_single_shard_tracking_when_resharding() {
    init_test_logger();

    let base_shard_layout = ShardLayout::multi_shard(3, 3);
    let validator = "cp0";
    let validator_client: AccountId = validator.parse().unwrap();
    let validators_spec = ValidatorsSpec::desired_roles(&[validator], &[]);
    let genesis = TestLoopBuilder::new_genesis_builder()
        .protocol_version(PROTOCOL_VERSION - 1)
        .validators_spec(validators_spec)
        .shard_layout(base_shard_layout.clone())
        .epoch_length(EPOCH_LENGTH)
        .build();

    let base_epoch_config = Arc::new(TestEpochConfigBuilder::from_genesis(&genesis).build());
    let boundary_account: AccountId = "account6".parse().unwrap();
    let parent_shard_id = base_shard_layout.account_id_to_shard_id(&boundary_account);
    let new_epoch_config =
        derive_new_epoch_config_from_boundary(&base_epoch_config, &boundary_account);
    let new_shard_layout = new_epoch_config.shard_layout.clone();
    let initial_shards: Vec<ShardUId> = base_shard_layout.shard_uids().collect();
    let (untracked_shard, initial_tracked_shards) =
        initial_shards.split_last().map(|(l, r)| (*l, r.to_vec())).unwrap();
    // We want to test an archival node that is tracking the shard being resharded.
    assert_ne!(untracked_shard.shard_id(), parent_shard_id);
    let mut shards_tracked_after_resharding: HashSet<_> = initial_tracked_shards
        .iter()
        .filter(|shard_uid| shard_uid.shard_id() != parent_shard_id)
        .cloned()
        .collect();

    for child_shard_uid in new_shard_layout.get_children_shards_uids(parent_shard_id).unwrap() {
        shards_tracked_after_resharding.insert(child_shard_uid);
    }

    let mut epoch_configs = (PROD_GENESIS_PROTOCOL_VERSION..=genesis.config.protocol_version)
        .map(|protocol_version| (protocol_version, base_epoch_config.clone()))
        .collect_vec();
    epoch_configs.push((genesis.config.protocol_version + 1, Arc::new(new_epoch_config)));
    let epoch_config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));

    let archival_client: AccountId = "archival".parse().unwrap();
    let clients = vec![archival_client, validator_client];
    let archival_client_index = 0;
    let mut env = TestLoopBuilder::new()
        .genesis(genesis)
        .clients(clients)
        .epoch_config_store(epoch_config_store)
        .gc_num_epochs_to_keep(GC_NUM_EPOCHS_TO_KEEP)
        .config_modifier(move |config, client_index| {
            if client_index != archival_client_index {
                return;
            }
            config.tracked_shards_config =
                TrackedShardsConfig::Shards(initial_tracked_shards.clone());
        })
        .build()
        .warmup();

    let client_handle = env.node_datas[archival_client_index].client_sender.actor_handle();
    let chain_store = env.test_loop.data.get(&client_handle).client.chain.chain_store.clone();
    let epoch_manager = env.test_loop.data.get(&client_handle).client.epoch_manager.clone();

    // Wait for GC to kick in for the first time. This should clean up genesis data from the hot store.
    let num_blocks_to_wait = EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP;
    env.test_loop.run_until(
        |_| {
            let prev_hash = chain_store.final_head().unwrap().prev_block_hash;
            let prev_block = chain_store.get_block(&prev_hash).unwrap();
            prev_block.header().height() > num_blocks_to_wait
        },
        Duration::seconds(num_blocks_to_wait as i64),
    );

    let prev_hash = chain_store.final_head().unwrap().prev_block_hash;
    let prev_block = chain_store.get_block(&prev_hash).unwrap();
    let epoch_id = prev_block.header().epoch_id();
    // By this point, resharding is expected to have occurred.
    assert_eq!(epoch_manager.get_epoch_config(&epoch_id).unwrap().shard_layout, new_shard_layout);

    // Shortly after resharding, both parent and child chunk data should exist in storage.
    let mut expected_stored_shard_uids = shards_tracked_after_resharding.clone();
    expected_stored_shard_uids
        .insert(ShardUId::from_shard_id_and_layout(parent_shard_id, &base_shard_layout));
    assert_old_chunks_are_cleared(&chain_store, &expected_stored_shard_uids);
    // New chunks should no longer be produced for the parent shard.
    assert_new_chunks_exist(&chain_store, &shards_tracked_after_resharding);

    env.shutdown_and_drain_remaining_events(Duration::seconds(10));
}

fn assert_old_chunks_are_cleared(
    chain_store: &ChainStoreAdapter,
    tracked_shards: &HashSet<ShardUId>,
) {
    let final_block_height = chain_store.final_head().unwrap().height;
    let store = chain_store.store().store();
    let mut stored_shards = HashSet::<ShardUId>::default();
    for res in store.iter(DBCol::ChunkExtra) {
        let (block_hash, shard_uid) = get_block_shard_uid_rev(&res.unwrap().0).unwrap();
        let block_height = chain_store.get_block_height(&block_hash).unwrap();
        stored_shards.insert(shard_uid);
        assert!(
            block_height >= final_block_height.saturating_sub(EPOCH_LENGTH * GC_NUM_EPOCHS_TO_KEEP),
            "ChunkExtra data contains too old block at height {block_height} while final_block_height is {final_block_height}",
        );
    }
    assert_eq!(
        &stored_shards, tracked_shards,
        "ChunkExtra contains data for shards {stored_shards:?} while the node is expected to track shards {tracked_shards:?}",
    );
}

fn assert_new_chunks_exist(chain_store: &ChainStoreAdapter, tracked_shards: &HashSet<ShardUId>) {
    let final_block_height = chain_store.final_head().unwrap().height;
    let store = chain_store.store().store();
    let head_height = chain_store.head().unwrap().height;
    for height in final_block_height..head_height {
        let block_hash = chain_store.get_block_hash_by_height(height).unwrap();
        for shard_uid in tracked_shards {
            assert!(
                store
                    .get(DBCol::ChunkExtra, &get_block_shard_uid(&block_hash, shard_uid))
                    .unwrap()
                    .is_some(),
                "ChunkExtra missing for ShardUId {shard_uid} and height {height}",
            );
        }
    }
}
