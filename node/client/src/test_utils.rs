use std::sync::Arc;

use chain_spec::ChainSpec;
use Client;
use primitives::signer::InMemorySigner;
use storage::MemoryStorage;

pub fn generate_test_client() -> Client {
    let storage = Arc::new(MemoryStorage::new());
    let chain_spec = ChainSpec {
        balances: vec!(),
        initial_authorities: vec!(),
    };
    let signer = InMemorySigner::default();
    Client::new(&chain_spec, storage, signer)
}
