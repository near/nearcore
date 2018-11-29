use std::sync::Arc;

use chain_spec::ChainSpec;
use primitives::signature::get_keypair;
use primitives::signer::InMemorySigner;
use storage::test_utils::create_memory_db;
use Client;

pub fn generate_test_client() -> Client {
    let storage = Arc::new(create_memory_db());
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let (public_key, _) = get_keypair();
    let chain_spec = ChainSpec {
        accounts: vec![("alice".to_string(), public_key.to_string(), 100)],
        initial_authorities: vec![],
        genesis_wasm,
    };
    let signer = Arc::new(InMemorySigner::default());
    Client::new(&chain_spec, storage, signer)
}
