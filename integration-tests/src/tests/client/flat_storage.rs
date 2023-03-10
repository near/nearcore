/// Tests which check correctness of background flat storage creation.
use assert_matches::assert_matches;
use near_chain::{ChainGenesis, RuntimeWithEpochManagerAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_o11y::testonly::init_test_logger;
use near_primitives::shard_layout::{ShardLayout, ShardUId};
use near_primitives::types::AccountId;
use near_primitives_core::types::{BlockHeight, NumShards};
use near_store::flat::{
    store_helper, FetchingStateStatus, FlatStorageCreationStatus, NUM_PARTS_IN_ONE_STEP,
};
use near_store::test_utils::create_test_store;
use near_store::{Store, TrieTraversalItem};
use nearcore::config::GenesisExt;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Height on which we start flat storage background creation.
const START_HEIGHT: BlockHeight = 4;

/// Number of steps which should be enough to create flat storage.
const CREATION_TIMEOUT: BlockHeight = 30;

/// Setup environment with one Near client for testing.
fn setup_env(genesis: &Genesis, store: Store) -> TestEnv {
    let chain_genesis = ChainGenesis::new(genesis);
    let runtimes: Vec<Arc<dyn RuntimeWithEpochManagerAdapter>> =
        vec![Arc::new(nearcore::NightshadeRuntime::test(Path::new("../../../.."), store, genesis))];
    TestEnv::builder(chain_genesis).runtime_adapters(runtimes).build()
}

/// Waits for flat storage creation on shard 0 for `CREATION_TIMEOUT` blocks.
/// We have a pause after processing each block because state data is being fetched in rayon threads,
/// but we expect it to finish in <30s because state is small and there is only one state part.
/// Returns next block height available to produce.
fn wait_for_flat_storage_creation(
    env: &mut TestEnv,
    start_height: BlockHeight,
    produce_blocks: bool,
) -> BlockHeight {
    let store = env.clients[0].runtime_adapter.store().clone();
    let mut next_height = start_height;
    let mut prev_status = store_helper::get_flat_storage_creation_status(&store, 0);
    while next_height < start_height + CREATION_TIMEOUT {
        if produce_blocks {
            env.produce_block(0, next_height);
        }
        env.clients[0].run_flat_storage_creation_step().unwrap();

        let status = store_helper::get_flat_storage_creation_status(&store, 0);
        // Check validity of state transition for flat storage creation.
        match &prev_status {
            FlatStorageCreationStatus::SavingDeltas => assert_matches!(
                status,
                FlatStorageCreationStatus::SavingDeltas
                    | FlatStorageCreationStatus::FetchingState(_)
            ),
            FlatStorageCreationStatus::FetchingState(_) => assert_matches!(
                status,
                FlatStorageCreationStatus::FetchingState(_)
                    | FlatStorageCreationStatus::CatchingUp(_)
            ),
            FlatStorageCreationStatus::CatchingUp(_) => assert_matches!(
                status,
                FlatStorageCreationStatus::CatchingUp(_) | FlatStorageCreationStatus::Ready
            ),
            _ => {
                panic!("Invalid status {prev_status:?} observed during flat storage creation for height {next_height}");
            }
        }
        tracing::info!("Flat Creation status: {:?}", status);

        prev_status = status;
        next_height += 1;
        if prev_status == FlatStorageCreationStatus::Ready {
            break;
        }

        thread::sleep(Duration::from_secs(1));
    }
    let status = store_helper::get_flat_storage_creation_status(&store, 0);
    assert_eq!(
        status,
        FlatStorageCreationStatus::Ready,
        "Client couldn't create flat storage until block {next_height}, status: {status:?}"
    );
    assert!(env.clients[0].runtime_adapter.get_flat_storage_for_shard(0).is_some());
    next_height
}

/// Check correctness of flat storage creation.
#[test]
fn test_flat_storage_creation() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let shard_uid = ShardLayout::v0_single_shard().get_shard_uids()[0];
    let store = create_test_store();

    // Process some blocks with flat storage. Then remove flat storage data from disk.
    {
        let mut env = setup_env(&genesis, store.clone());
        for height in 1..START_HEIGHT {
            env.produce_block(0, height);
        }

        if cfg!(feature = "protocol_feature_flat_state") {
            // If chain was initialized from scratch, flat storage state should be created. During block processing, flat
            // storage head should be moved to block `START_HEIGHT - 3`.
            assert_eq!(
                store_helper::get_flat_storage_creation_status(&store, 0),
                FlatStorageCreationStatus::Ready
            );
            let expected_flat_storage_head =
                env.clients[0].chain.get_block_hash_by_height(START_HEIGHT - 3).unwrap();
            assert_eq!(store_helper::get_flat_head(&store, 0), Some(expected_flat_storage_head));

            // Deltas for blocks until `START_HEIGHT - 2` should not exist.
            for height in 0..START_HEIGHT - 2 {
                let block_hash = env.clients[0].chain.get_block_hash_by_height(height).unwrap();
                assert_eq!(
                    store_helper::get_delta_changes(&store, shard_uid, block_hash),
                    Ok(None)
                );
            }
            // Deltas for blocks until `START_HEIGHT` should still exist,
            // because they come after flat storage head.
            for height in START_HEIGHT - 2..START_HEIGHT {
                let block_hash = env.clients[0].chain.get_block_hash_by_height(height).unwrap();
                assert_matches!(
                    store_helper::get_delta_changes(&store, shard_uid, block_hash),
                    Ok(Some(_))
                );
            }
        } else {
            assert_eq!(
                store_helper::get_flat_storage_creation_status(&store, 0),
                FlatStorageCreationStatus::DontCreate
            );
            assert_eq!(store_helper::get_flat_head(&store, 0), None);
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(START_HEIGHT - 1).unwrap();
        let epoch_id = env.clients[0].chain.runtime_adapter.get_epoch_id(&block_hash).unwrap();
        env.clients[0].chain.runtime_adapter.remove_flat_storage_for_shard(0, &epoch_id).unwrap();
    }

    // Create new chain and runtime using the same store. It should produce next blocks normally, but now it should
    // think that flat storage does not exist and background creation should be initiated.
    let mut env = setup_env(&genesis, store.clone());
    for height in START_HEIGHT..START_HEIGHT + 2 {
        env.produce_block(0, height);
    }
    assert!(env.clients[0].runtime_adapter.get_flat_storage_for_shard(0).is_none());

    if !cfg!(feature = "protocol_feature_flat_state") {
        assert_eq!(
            store_helper::get_flat_storage_creation_status(&store, 0),
            FlatStorageCreationStatus::DontCreate
        );
        assert_eq!(store_helper::get_flat_head(&store, 0), None);
        // Stop the test here.
        return;
    }

    // At first, flat storage state should start saving deltas. Deltas for all newly processed blocks should be saved to
    // disk.
    assert_eq!(
        store_helper::get_flat_storage_creation_status(&store, 0),
        FlatStorageCreationStatus::SavingDeltas
    );
    for height in START_HEIGHT..START_HEIGHT + 2 {
        let block_hash = env.clients[0].chain.get_block_hash_by_height(height).unwrap();
        assert_matches!(
            store_helper::get_delta_changes(&store, shard_uid, block_hash),
            Ok(Some(_))
        );
    }

    // Produce new block and run flat storage creation step.
    // We started the node from height `START_HEIGHT - 1`, and now final head should move to height `START_HEIGHT`.
    // Because final head height became greater than height on which node started,
    // we must start fetching the state.
    env.produce_block(0, START_HEIGHT + 2);
    assert!(!env.clients[0].run_flat_storage_creation_step().unwrap());
    let final_block_hash = env.clients[0].chain.get_block_hash_by_height(START_HEIGHT).unwrap();
    assert_eq!(store_helper::get_flat_head(&store, 0), None);
    assert_eq!(
        store_helper::get_flat_storage_creation_status(&store, 0),
        FlatStorageCreationStatus::FetchingState(FetchingStateStatus {
            block_hash: final_block_hash,
            part_id: 0,
            num_parts_in_step: NUM_PARTS_IN_ONE_STEP,
            num_parts: 1,
        })
    );

    wait_for_flat_storage_creation(&mut env, START_HEIGHT + 3, true);
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
    let store = create_test_store();

    // Process some blocks with flat storages for two shards. Then remove flat storage data from disk for shard 0.
    {
        let mut env = setup_env(&genesis, store.clone());
        for height in 1..START_HEIGHT {
            env.produce_block(0, height);
        }

        for shard_id in 0..num_shards {
            if cfg!(feature = "protocol_feature_flat_state") {
                assert_eq!(
                    store_helper::get_flat_storage_creation_status(&store, shard_id),
                    FlatStorageCreationStatus::Ready
                );
            } else {
                assert_eq!(
                    store_helper::get_flat_storage_creation_status(&store, shard_id),
                    FlatStorageCreationStatus::DontCreate
                );
            }
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(START_HEIGHT - 1).unwrap();
        let epoch_id = env.clients[0].chain.runtime_adapter.get_epoch_id(&block_hash).unwrap();
        env.clients[0].chain.runtime_adapter.remove_flat_storage_for_shard(0, &epoch_id).unwrap();
    }

    if !cfg!(feature = "protocol_feature_flat_state") {
        return;
    }

    // Check that flat storage is not ready for shard 0 but ready for shard 1.
    let mut env = setup_env(&genesis, store.clone());
    assert!(env.clients[0].runtime_adapter.get_flat_storage_for_shard(0).is_none());
    assert_eq!(
        store_helper::get_flat_storage_creation_status(&store, 0),
        FlatStorageCreationStatus::SavingDeltas
    );
    assert!(env.clients[0].runtime_adapter.get_flat_storage_for_shard(1).is_some());
    assert_eq!(
        store_helper::get_flat_storage_creation_status(&store, 1),
        FlatStorageCreationStatus::Ready
    );

    wait_for_flat_storage_creation(&mut env, START_HEIGHT, true);
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
    let store = create_test_store();
    let shard_layout = ShardLayout::v0_single_shard();
    let shard_uid = shard_layout.get_shard_uids()[0];

    // Process some blocks with flat storage.
    // Split state into two parts and return trie keys corresponding to each part.
    const NUM_PARTS: u64 = 2;
    let trie_keys: Vec<_> = {
        let mut env = setup_env(&genesis, store.clone());
        for height in 1..START_HEIGHT {
            env.produce_block(0, height);
        }

        if cfg!(feature = "protocol_feature_flat_state") {
            assert_eq!(
                store_helper::get_flat_storage_creation_status(&store, 0),
                FlatStorageCreationStatus::Ready
            );
        } else {
            assert_eq!(
                store_helper::get_flat_storage_creation_status(&store, 0),
                FlatStorageCreationStatus::DontCreate
            );
            return;
        }

        let block_hash = env.clients[0].chain.get_block_hash_by_height(START_HEIGHT - 1).unwrap();
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

    if cfg!(feature = "protocol_feature_flat_state") {
        // Remove keys of part 1 from the flat state.
        // Manually set flat storage creation status to the step when it should start from fetching part 1.
        let flat_head = store_helper::get_flat_head(&store, 0).unwrap();
        let mut store_update = store.store_update();
        for key in trie_keys[1].iter() {
            store_helper::set_ref(&mut store_update, shard_uid, key.clone(), None).unwrap();
        }
        store_helper::remove_flat_head(&mut store_update, 0);
        store_helper::set_flat_storage_creation_status(
            &mut store_update,
            0,
            FlatStorageCreationStatus::FetchingState(FetchingStateStatus {
                block_hash: flat_head,
                part_id: 1,
                num_parts_in_step: 1,
                num_parts: NUM_PARTS,
            }),
        );
        store_update.commit().unwrap();

        // Re-create runtime, check that flat storage is not created yet.
        let mut env = setup_env(&genesis, store.clone());
        assert!(env.clients[0].runtime_adapter.get_flat_storage_for_shard(0).is_none());

        // Run chain for a couple of blocks and check that flat storage for shard 0 is eventually created.
        let next_height = wait_for_flat_storage_creation(&mut env, START_HEIGHT, true);

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
        let chunk_view = trie.flat_storage_chunk_view.unwrap();
        for part_trie_keys in trie_keys.iter() {
            for trie_key in part_trie_keys.iter() {
                assert_matches!(chunk_view.get_ref(trie_key), Ok(Some(_)));
            }
        }
    }
}

/// Tests the scenario where we start flat storage migration, and get just a few new blocks.
/// (in this test we still generate 3 blocks in order to generate deltas).
#[cfg(feature = "protocol_feature_flat_state")]
#[test]
fn test_cachup_succeeds_even_if_no_new_blocks() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let store = create_test_store();

    // Process some blocks with flat storage. Then remove flat storage data from disk.
    {
        let mut env = setup_env(&genesis, store.clone());
        for height in 1..START_HEIGHT {
            env.produce_block(0, height);
        }
        // Remove flat storage.
        let block_hash = env.clients[0].chain.get_block_hash_by_height(START_HEIGHT - 1).unwrap();
        let epoch_id = env.clients[0].chain.runtime_adapter.get_epoch_id(&block_hash).unwrap();
        env.clients[0].chain.runtime_adapter.remove_flat_storage_for_shard(0, &epoch_id).unwrap();
    }
    let mut env = setup_env(&genesis, store.clone());
    assert!(env.clients[0].runtime_adapter.get_flat_storage_for_shard(0).is_none());
    assert_eq!(
        store_helper::get_flat_storage_creation_status(&store, 0),
        FlatStorageCreationStatus::SavingDeltas
    );
    // Create 3 more blocks (so that the deltas are generated) - and assume that no new blocks are received.
    // In the future, we should also support the scenario where no new blocks are created.

    for block_height in START_HEIGHT + 1..=START_HEIGHT + 3 {
        env.produce_block(0, block_height);
    }

    assert!(!env.clients[0].run_flat_storage_creation_step().unwrap());
    wait_for_flat_storage_creation(&mut env, START_HEIGHT + 3, false);
}

/// Tests the flat storage iterator. Running on a chain with 3 shards, and couple blocks produced.
#[cfg(feature = "protocol_feature_flat_state")]
#[test]
fn test_flat_storage_iter() {
    init_test_logger();
    let num_shards: NumShards = 3;

    let shard_layout =
        ShardLayout::v1(vec!["test0".parse().unwrap(), "test1".parse().unwrap()], vec![], None, 0);

    let genesis = Genesis::test_with_seeds(
        vec!["test0".parse().unwrap()],
        1,
        vec![1; num_shards as usize],
        shard_layout.clone(),
    );

    let store = create_test_store();

    let mut env = setup_env(&genesis, store.clone());
    for height in 1..START_HEIGHT {
        env.produce_block(0, height);
    }

    for shard_id in 0..3 {
        let items: Vec<_> = store_helper::iter_flat_state_entries(
            shard_layout.clone(),
            shard_id,
            &store,
            None,
            None,
        )
        .collect();

        match shard_id {
            0 => {
                // Two entries - one for account, the other for contract.
                assert_eq!(2, items.len());
                assert_eq!(
                    near_primitives::trie_key::TrieKey::Account {
                        account_id: "test0".parse().unwrap()
                    }
                    .to_vec(),
                    items.get(0).unwrap().0
                );
            }
            1 => {
                // Test1 account was not created yet - so no entries.
                assert_eq!(0, items.len());
            }
            2 => {
                assert_eq!(2, items.len());
                // Two entries - one for 'near' system account, the other for the contract.
                assert_eq!(
                    near_primitives::trie_key::TrieKey::Account {
                        account_id: "near".parse().unwrap()
                    }
                    .to_vec(),
                    items.get(0).unwrap().0
                );
            }
            _ => {
                panic!("Unexpected shard_id");
            }
        }
    }
}
