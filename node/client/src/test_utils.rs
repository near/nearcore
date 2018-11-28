use std::sync::Arc;

use chain_spec::ChainSpec;
use primitives::signer::InMemorySigner;
use storage::test_utils::create_memory_db;
use Client;

pub fn generate_test_client() -> Client {
    let storage = Arc::new(create_memory_db());
    let genesis_wasm = include_bytes!(
        "../../../core/wasm/runtest/res/wasm_with_mem.wasm"
    ).to_vec();
    let chain_spec = ChainSpec {
        balances: vec![],
        initial_authorities: vec![],
        genesis_wasm,
    };
    let signer = Arc::new(InMemorySigner::default());
    Client::new(&chain_spec, storage, signer)
}
