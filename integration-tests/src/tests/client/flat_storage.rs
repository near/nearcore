use assert_matches::assert_matches;
use near_chain::{ChainGenesis, RuntimeAdapter};
use near_chain_configs::Genesis;
use near_client::test_utils::TestEnv;
use near_o11y::testonly::init_test_logger;
use near_store::flat_state::{store_helper, FlatStorageStateStatus};
use near_store::test_utils::create_test_store;
use nearcore::config::GenesisExt;
use std::path::Path;
use std::sync::Arc;

/// Check correctness of flat storage creation.
#[test]
fn test_flat_storage_creation() {
    init_test_logger();
    let genesis = Genesis::test(vec!["test0".parse().unwrap()], 1);
    let chain_genesis = ChainGenesis::new(&genesis);
    let store = create_test_store();
    {
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(nearcore::NightshadeRuntime::test(
        Path::new("../../../.."),
        store.clone(),
        &genesis,
    ))];
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
        let expected_flat_storage_head = env.clients[0].chain.get_block_hash_by_height(1).unwrap();
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
    }

    // Remove flat storage head using low-level disk operation. Flat storage is implemented in such way that its
    // existence is determined by existence of flat storage head.
    #[cfg(feature = "protocol_feature_flat_state")]
    {
        let mut store_update = store.store_update();
        store_helper::remove_flat_head(&mut store_update, 0);
        store_update.commit().unwrap();
    }

    // Create new chain and runtime using the same store. It should produce next blocks normally, but now it should
    // think that flat storage does not exist and background creation should be initiated.
    let runtimes: Vec<Arc<dyn RuntimeAdapter>> = vec![Arc::new(nearcore::NightshadeRuntime::test(
        Path::new("../../../.."),
        store.clone(),
        &genesis,
    ))];
    let mut env = TestEnv::builder(chain_genesis).runtime_adapters(runtimes.clone()).build();
    for i in 4..7 {
        env.produce_block(0, i);
    }

    if cfg!(feature = "protocol_feature_flat_state") {
        // At first, flat storage state should start saving deltas. Deltas for all newly processed blocks should be saved to
        // disk.
        assert_eq!(
            store_helper::get_flat_storage_state_status(&store, 0),
            FlatStorageStateStatus::SavingDeltas
        );
        for i in 4..7 {
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

    // TODO: support next statuses once their logic is implemented.
}
