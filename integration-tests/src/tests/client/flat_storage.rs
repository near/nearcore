/// Tests which check correctness of background flat storage creation.
use assert_matches::assert_matches;
use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::AccountId;
use near_primitives_core::types::{BlockHeight, NumShards};
use near_store::flat_state::{
    store_helper, FetchingStateStatus, FlatStorageStateStatus, NUM_PARTS_IN_ONE_STEP,
};
use near_store::test_utils::create_test_store;
#[cfg(feature = "protocol_feature_flat_state")]
use near_store::DBCol;
use near_store::TrieTraversalItem;
use nearcore::config::GenesisExt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Number of steps which should be enough to create flat storage.
const CREATION_TIMEOUT: BlockHeight = 30;

/// Waits for flat storage creation on shard 0 for `CREATION_TIMEOUT` blocks.
/// We have a pause after processing each block because state data is being fetched in rayon threads,
/// but we expect it to finish in <30s because state is small and there is only one state part.
/// Returns next block height available to produce.
fn wait_for_flat_storage_creation(env: &mut TestEnv, start_height: BlockHeight) -> BlockHeight {
    let store = env.clients[0].runtime_adapter.store().clone();
    let mut next_height = start_height;
    let mut prev_status = store_helper::get_flat_storage_state_status(&store, 0);
    while next_height < start_height + CREATION_TIMEOUT {
        env.produce_block(0, next_height);
        env.clients[0].run_flat_storage_creation_step().unwrap();

        let status = store_helper::get_flat_storage_state_status(&store, 0);
        let index_prev_status: i64 = (&prev_status).into();
        let index_status: i64 = (&status).into();
        assert!(index_prev_status <= index_status, "Inconsistency in flat storage creation: moved from {prev_status:?} to {status:?} for height {next_height}");
        prev_status = status;
        next_height += 1;

        thread::sleep(Duration::from_secs(1));
    }
    let status = store_helper::get_flat_storage_state_status(&store, 0);
    assert_eq!(
        status,
        FlatStorageStateStatus::Ready,
        "Client couldn't create flat storage until block {next_height}, status: {status:?}"
    );
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_some());
    next_height
}

/// Check correctness of flat storage creation.
#[test]
fn test_flat_storage_creation() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let chain_genesis = ChainGenesis::new(&genesis);
    let store = create_test_store();

    // Process some blocks with flat storage. Then remove flat storage data from disk.
    {
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(
            nearcore::NightshadeRuntime::test(Path::new("../../../.."), store.clone(), &genesis),
        )];
        let mut env =
            TestEnv::builder(chain_genesis.clone()).runtime_adapters(runtimes.clone()).build();
        for i in 1..4 {
            env.produce_block(0, i);
        }

        if cfg!(feature = "protocol_feature_flat_state") {
            // If chain was initialized from scratch, flat storage state should be created. During block processing, flat
            // storage head should be moved to block 1.
            assert_eq!(
                store_helper::get_flat_storage_state_status(&store, 0),
                FlatStorageStateStatus::Ready
            );
            let expected_flat_storage_head =
                env.clients[0].chain.get_block_hash_by_height(1).unwrap();
            assert_eq!(store_helper::get_flat_head(&store, 0), Some(expected_flat_storage_head));

            // Deltas for blocks 0 and 1 should not exist.
            for i in 0..2 {
                let block_hash = env.clients[0].chain.get_block_hash_by_height(i).unwrap();
                assert_eq!(store_helper::get_delta(&store, 0, block_hash), Ok(None));
            }
            // Deltas for blocks 2 and 3 should still exist, because they come after flat storage head.
            for i in 2..4 {
                let block_hash = env.clients[0].chain.get_block_hash_by_height(i).unwrap();
                assert_matches!(store_helper::get_delta(&store, 0, block_hash), Ok(Some(_)));
            }
        } else {
            assert_eq!(
                store_helper::get_flat_storage_state_status(&store, 0),
                FlatStorageStateStatus::DontCreate
            );
            assert_eq!(store_helper::get_flat_head(&store, 0), None);
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(3).unwrap();
        let epoch_id = env.clients[0].chain.runtime_adapter.get_epoch_id(&block_hash).unwrap();
        env.clients[0]
            .chain
            .runtime_adapter
            .remove_flat_storage_state_for_shard(0, &epoch_id)
            .unwrap();
    }

    // Create new chain and runtime using the same store. It should produce next blocks normally, but now it should
    // think that flat storage does not exist and background creation should be initiated.
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(nearcore::NightshadeRuntime::test(
        Path::new("../../../.."),
        store.clone(),
        &genesis,
    ))];
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes.clone()).build();
    for i in 4..6 {
        env.produce_block(0, i);
    }
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_none());

    if !cfg!(feature = "protocol_feature_flat_state") {
        assert_eq!(
            store_helper::get_flat_storage_state_status(&store, 0),
            FlatStorageStateStatus::DontCreate
        );
        assert_eq!(store_helper::get_flat_head(&store, 0), None);
        // Stop the test here.
        return;
    }

    // At first, flat storage state should start saving deltas. Deltas for all newly processed blocks should be saved to
    // disk.
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 0),
        FlatStorageStateStatus::SavingDeltas
    );
    for i in 4..6 {
        let block_hash = env.clients[0].chain.get_block_hash_by_height(i).unwrap();
        assert_matches!(store_helper::get_delta(&store, 0, block_hash), Ok(Some(_)));
    }

    // Produce new block and run flat storage creation step.
    // We started the node from height 3, and now final head should move to height 4.
    // Because final head height became greater than height on which node started,
    // we must start fetching the state.
    env.produce_block(0, 6);
    assert!(!env.clients[0].run_flat_storage_creation_step().unwrap());
    let final_block_hash = env.clients[0].chain.get_block_hash_by_height(4).unwrap();
    assert_eq!(store_helper::get_flat_head(&store, 0), Some(final_block_hash));
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 0),
        FlatStorageStateStatus::FetchingState(FetchingStateStatus {
            part_id: 0,
            num_parts_in_step: NUM_PARTS_IN_ONE_STEP,
            num_parts: 1,
        })
    );

    wait_for_flat_storage_creation(&mut env, 8);
}

/// Check that client can create flat storage on some shard while it already exists on another shard.
#[test]
fn test_flat_storage_creation_two_shards() {
    init_test_logger();
    let num_shards: NumShards = 2;
    let genesis = Genesis::test_sharded_new_version(
        vec!["test0".parse().unwrap()],
        1,
        vec![1; num_shards as usize],
    );
    let chain_genesis = ChainGenesis::new(&genesis);
    let store = create_test_store();

    // Process some blocks with flat storages for two shards. Then remove flat storage data from disk for shard 0.
    {
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(
            nearcore::NightshadeRuntime::test(Path::new("../../../.."), store.clone(), &genesis),
        )];
        let mut env =
            TestEnv::builder(chain_genesis.clone()).runtime_adapters(runtimes.clone()).build();
        for i in 1..4 {
            env.produce_block(0, i);
        }

        for shard_id in 0..num_shards {
            if cfg!(feature = "protocol_feature_flat_state") {
                assert_eq!(
                    store_helper::get_flat_storage_state_status(&store, shard_id),
                    FlatStorageStateStatus::Ready
                );
            } else {
                assert_eq!(
                    store_helper::get_flat_storage_state_status(&store, shard_id),
                    FlatStorageStateStatus::DontCreate
                );
            }
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(3).unwrap();
        let epoch_id = env.clients[0].chain.runtime_adapter.get_epoch_id(&block_hash).unwrap();
        env.clients[0]
            .chain
            .runtime_adapter
            .remove_flat_storage_state_for_shard(0, &epoch_id)
            .unwrap();
    }

    if !cfg!(feature = "protocol_feature_flat_state") {
        return;
    }

    // Check that flat storage is not ready for shard 0 but ready for shard 1.
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(nearcore::NightshadeRuntime::test(
        Path::new("../../../.."),
        store.clone(),
        &genesis,
    ))];
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes.clone()).build();
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_none());
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 0),
        FlatStorageStateStatus::SavingDeltas
    );
    assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(1).is_some());
    assert_eq!(
        store_helper::get_flat_storage_state_status(&store, 1),
        FlatStorageStateStatus::Ready
    );

    wait_for_flat_storage_creation(&mut env, 4);
}

/// Check that flat storage creation can be started from intermediate state where one
/// of state parts is already fetched.
#[test]
fn test_flat_storage_creation_start_from_state_part() {
    init_test_logger();
    // Create several accounts to ensure that state is non-trivial.
    let accounts =
        (0..4).map(|i| AccountId::from_str(&format!("test{}", i)).unwrap()).collect::<Vec<_>>();
    let genesis = Genesis::test(accounts, 1);
    let chain_genesis = ChainGenesis::new(&genesis);
    let store = create_test_store();
    let shard_layout = ShardLayout::v0_single_shard();

    // Process some blocks with flat storage.
    // Split state into two parts and return trie keys corresponding to each part.
    const NUM_PARTS: u64 = 2;
    let trie_keys: Vec<_> = {
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(
            nearcore::NightshadeRuntime::test(Path::new("../../../.."), store.clone(), &genesis),
        )];
        let mut env =
            TestEnv::builder(chain_genesis.clone()).runtime_adapters(runtimes.clone()).build();
        for i in 1..4 {
            env.produce_block(0, i);
        }

        if cfg!(feature = "protocol_feature_flat_state") {
            assert_eq!(
                store_helper::get_flat_storage_state_status(&store, 0),
                FlatStorageStateStatus::Ready
            );
        } else {
            assert_eq!(
                store_helper::get_flat_storage_state_status(&store, 0),
                FlatStorageStateStatus::DontCreate
            );
            return;
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(3).unwrap();
        let state_root = *env.clients[0]
            .chain
            .get_chunk_extra(&block_hash, &ShardUId::from_shard_id_and_layout(0, &shard_layout))
            .unwrap()
            .state_root();
        let trie = env.clients[0]
            .chain
            .runtime_adapter
            .get_trie_for_shard(0, &block_hash, state_root, true)
            .unwrap();
        (0..NUM_PARTS)
            .map(|part_id| {
                let path_begin = trie.find_path_for_part_boundary(part_id, NUM_PARTS).unwrap();
                let path_end = trie.find_path_for_part_boundary(part_id + 1, NUM_PARTS).unwrap();
                let mut trie_iter = trie.iter().unwrap();
                let mut keys = vec![];
                for item in trie_iter.visit_nodes_interval(&path_begin, &path_end).unwrap() {
                    if let TrieTraversalItem { key: Some(trie_key), .. } = item {
                        keys.push(trie_key);
                    }
                }
                keys
            })
            .collect()
    };
    assert!(!trie_keys[0].is_empty());
    assert!(!trie_keys[1].is_empty());

    #[cfg(feature = "protocol_feature_flat_state")]
    {
        // Remove keys of part 1 from the flat state.
        // Manually set flat storage creation status to the step when it should start from fetching part 1.
        let mut store_update = store.store_update();
        for key in trie_keys[1].iter() {
            store_update.delete(DBCol::FlatState, key);
        }
        store_helper::set_fetching_state_status(
            &mut store_update,
            0,
            FetchingStateStatus { part_id: 1, num_parts_in_step: 1, num_parts: NUM_PARTS },
        );
        store_update.commit().unwrap();

        // Re-create runtime, check that flat storage is not created yet.
        let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(
            nearcore::NightshadeRuntime::test(Path::new("../../../.."), store.clone(), &genesis),
        )];
        let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes.clone()).build();
        assert!(env.clients[0].runtime_adapter.get_flat_storage_state_for_shard(0).is_none());

        // Run chain for a couple of blocks and check that flat storage for shard 0 is eventually created.
        let next_height = wait_for_flat_storage_creation(&mut env, 4);

        // Check that all the keys are present in flat storage.
        let block_hash = env.clients[0].chain.get_block_hash_by_height(next_height - 1).unwrap();
        let state_root = *env.clients[0]
            .chain
            .get_chunk_extra(&block_hash, &ShardUId::from_shard_id_and_layout(0, &shard_layout))
            .unwrap()
            .state_root();
        let trie = env.clients[0]
            .chain
            .runtime_adapter
            .get_trie_for_shard(0, &block_hash, state_root, true)
            .unwrap();
        let flat_state = trie.flat_state.unwrap();
        for part_trie_keys in trie_keys.iter() {
            for trie_key in part_trie_keys.iter() {
                assert_matches!(flat_state.get_ref(trie_key), Ok(Some(_)));
            }
        }
    }
}
