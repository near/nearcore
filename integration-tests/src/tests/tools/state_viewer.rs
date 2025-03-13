use crate::env::test_env::TestEnv;
use near_chain_configs::{Genesis, MutableConfigValue};
use near_crypto::{InMemorySigner, KeyFile};
use near_epoch_manager::EpochManager;
use near_primitives::shard_layout::ShardUId;
use near_primitives::types::AccountId;
use near_primitives::types::chunk_extra::ChunkExtra;
use near_state_viewer::util::load_trie;
use near_store::genesis::initialize_genesis_state;
use nearcore::config::Config;
use nearcore::{NearConfig, NightshadeRuntime};
use std::sync::Arc;

#[test]
/// Tests that getting the latest trie state actually gets the latest state.
/// Adds a transaction and waits for it to be included in a block.
/// Checks that the change of state caused by that transaction is visible to `load_trie()`.
fn test_latest_trie_state() {
    near_o11y::testonly::init_test_logger();
    let validators = vec!["test0".parse::<AccountId>().unwrap()];
    let genesis = Genesis::test_sharded_new_version(validators, 1, vec![1]);

    let tmp_dir = tempfile::tempdir().unwrap();
    let home_dir = tmp_dir.path();

    let store = near_store::test_utils::create_test_store();
    initialize_genesis_state(store.clone(), &genesis, Some(home_dir));
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config, None);
    let runtime =
        NightshadeRuntime::test(home_dir, store.clone(), &genesis.config, epoch_manager.clone());

    let stores = vec![store.clone()];
    let epoch_managers = vec![epoch_manager];
    let runtimes = vec![runtime];

    let mut env = TestEnv::builder(&genesis.config)
        .stores(stores)
        .epoch_managers(epoch_managers)
        .runtimes(runtimes)
        .build();

    let signer = InMemorySigner::test_signer(&"test0".parse().unwrap());
    assert_eq!(env.send_money(0), near_client::ProcessTxResponse::ValidTx);

    // It takes 2 blocks to record a transaction on chain and apply the receipts.
    env.produce_block(0, 1);
    env.produce_block(0, 2);

    let chunk_extras: Vec<Arc<ChunkExtra>> = (1..=2)
        .map(|height| {
            let block = env.clients[0].chain.get_block_by_height(height).unwrap();
            let hash = *block.hash();
            let chunk_extra = env.clients[0]
                .chain
                .get_chunk_extra(&hash, &ShardUId { version: 1, shard_id: 0 })
                .unwrap();
            chunk_extra
        })
        .collect();

    // Check that `send_money()` actually changed state.
    assert_ne!(chunk_extras[0].state_root(), chunk_extras[1].state_root());

    let near_config = NearConfig::new(
        Config::default(),
        genesis,
        KeyFile::from(signer),
        MutableConfigValue::new(None, "validator_signer"),
    )
    .unwrap();
    let (_epoch_manager, _runtime, state_roots, block_header) =
        load_trie(store, home_dir, &near_config);
    assert_eq!(&state_roots[0], chunk_extras[1].state_root());
    assert_eq!(block_header.height(), 2);
}
