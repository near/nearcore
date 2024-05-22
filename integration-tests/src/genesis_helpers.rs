use near_async::time::Clock;
use near_chain::rayon_spawner::RayonAsyncComputationSpawner;
use near_chain::types::ChainConfig;
use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::Genesis;
use near_epoch_manager::shard_tracker::ShardTracker;
use near_epoch_manager::EpochManager;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_store::genesis::initialize_genesis_state;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;
use std::sync::Arc;
use tempfile::tempdir;

/// Compute genesis hash from genesis.
pub fn genesis_hash(genesis: &Genesis) -> CryptoHash {
    *genesis_header(genesis).hash()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_header(genesis: &Genesis) -> BlockHeader {
    let dir = tempdir().unwrap();
    let store = create_test_store();
    initialize_genesis_state(store.clone(), genesis, None);
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime =
        NightshadeRuntime::test(dir.path(), store, &genesis.config, epoch_manager.clone());
    let chain = Chain::new(
        Clock::real(),
        epoch_manager,
        shard_tracker,
        runtime,
        &chain_genesis,
        DoomslugThresholdMode::TwoThirds,
        ChainConfig::test(),
        None,
        Arc::new(RayonAsyncComputationSpawner),
        None,
    )
    .unwrap();
    chain.genesis().clone()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_block(genesis: &Genesis) -> Block {
    let dir = tempdir().unwrap();
    let store = create_test_store();
    initialize_genesis_state(store.clone(), genesis, None);
    let chain_genesis = ChainGenesis::new(&genesis.config);
    let epoch_manager = EpochManager::new_arc_handle(store.clone(), &genesis.config);
    let shard_tracker = ShardTracker::new_empty(epoch_manager.clone());
    let runtime =
        NightshadeRuntime::test(dir.path(), store, &genesis.config, epoch_manager.clone());
    let chain = Chain::new(
        Clock::real(),
        epoch_manager,
        shard_tracker,
        runtime,
        &chain_genesis,
        DoomslugThresholdMode::TwoThirds,
        ChainConfig::test(),
        None,
        Arc::new(RayonAsyncComputationSpawner),
        None,
    )
    .unwrap();
    chain.get_block(&chain.genesis().hash().clone()).unwrap()
}
