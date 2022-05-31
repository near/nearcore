use std::sync::Arc;

use tempfile::tempdir;

use near_chain::{Chain, ChainGenesis, DoomslugThresholdMode};
use near_chain_configs::Genesis;
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_store::test_utils::create_test_store;
use nearcore::NightshadeRuntime;

/// Compute genesis hash from genesis.
pub fn genesis_hash(genesis: &Genesis) -> CryptoHash {
    *genesis_header(genesis).hash()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_header(genesis: &Genesis) -> BlockHeader {
    let dir = tempdir().unwrap();
    let store = create_test_store();
    let chain_genesis = ChainGenesis::from(genesis);
    let runtime = Arc::new(NightshadeRuntime::test(dir.path(), store, genesis));
    let chain =
        Chain::new(runtime, &chain_genesis, DoomslugThresholdMode::TwoThirds, true).unwrap();
    chain.genesis().clone()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_block(genesis: &Genesis) -> Block {
    let dir = tempdir().unwrap();
    let store = create_test_store();
    let chain_genesis = ChainGenesis::from(genesis);
    let runtime = Arc::new(NightshadeRuntime::test(dir.path(), store, genesis));
    let chain =
        Chain::new(runtime, &chain_genesis, DoomslugThresholdMode::TwoThirds, true).unwrap();
    chain.get_block(&chain.genesis().hash().clone()).unwrap()
}
