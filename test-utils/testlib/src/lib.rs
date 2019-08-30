use near::{GenesisConfig, NightshadeRuntime};
use near_chain::Chain;
use near_primitives::block::BlockHeader;
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
    let runtime =
        Arc::new(NightshadeRuntime::new(dir.path(), store.clone(), genesis_config.clone()));
    let chain =
        Chain::new(store, runtime, genesis_time, genesis_config.transaction_validity_period)
            .unwrap();
    chain.genesis().clone()
}
