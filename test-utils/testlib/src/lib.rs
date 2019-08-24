use near::{get_store_path, GenesisConfig, NightshadeRuntime};
use near_chain::RuntimeAdapter;
use near_primitives::block::Block;
use near_primitives::hash::CryptoHash;
use near_store::create_store;
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
    let tmp_dir = TempDir::new("tmp").unwrap();
    let store = create_store(&get_store_path(tmp_dir.path()));
    let runtime = NightshadeRuntime::new(tmp_dir.path(), store.clone(), genesis_config.clone());
    let (_, state_roots) = runtime.genesis_state();
    Block::genesis(state_roots[0], genesis_config.genesis_time).hash()
}
