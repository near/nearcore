use near::{GenesisConfig, NightshadeRuntime};
use near_chain::{Chain, ChainGenesis};
use near_primitives::block::{Block, BlockHeader};
use near_primitives::hash::CryptoHash;
use near_store::test_utils::create_test_store;
use std::sync::Arc;
use tempdir::TempDir;

pub mod actix_utils;
pub mod fees_utils;
pub mod node;
pub mod runtime_utils;
pub mod standard_test_cases;
pub mod test_helpers;
pub mod user;

/// Compute genesis hash from genesis config.
pub fn genesis_hash(genesis_config: &GenesisConfig) -> CryptoHash {
    genesis_header(&genesis_config).hash
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_header(genesis_config: &GenesisConfig) -> BlockHeader {
    let dir = TempDir::new("unused").unwrap();
    let store = create_test_store();
    let genesis_time = genesis_config.genesis_time.clone();
    let runtime = Arc::new(NightshadeRuntime::new(
        dir.path(),
        store.clone(),
        genesis_config.clone(),
        vec![],
        vec![],
    ));
    let chain_genesis = ChainGenesis::new(
        genesis_time,
        genesis_config.gas_limit,
        genesis_config.gas_price,
        genesis_config.total_supply,
        genesis_config.max_inflation_rate,
        genesis_config.gas_price_adjustment_rate,
        genesis_config.transaction_validity_period,
    );
    let chain = Chain::new(store, runtime, &chain_genesis).unwrap();
    chain.genesis().clone()
}

/// Utility to generate genesis header from config for testing purposes.
pub fn genesis_block(genesis_config: GenesisConfig) -> Block {
    let dir = TempDir::new("unused").unwrap();
    let store = create_test_store();
    let runtime = Arc::new(NightshadeRuntime::new(
        dir.path(),
        store.clone(),
        genesis_config.clone(),
        vec![],
        vec![],
    ));
    let mut chain = Chain::new(store, runtime, &genesis_config.into()).unwrap();
    chain.get_block(&chain.genesis().hash()).unwrap().clone()
}
