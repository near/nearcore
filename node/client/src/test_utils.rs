use chain_spec::ChainSpec;
use Client;
use std::sync::Arc;
use storage::MemoryStorage;

pub fn generate_test_client() -> Client {
    let storage = Arc::new(MemoryStorage::new());
    let chain_spec = ChainSpec {
        balances: vec!(),
        initial_authorities: vec!(),
    };
    Client::new(storage, &chain_spec)
}
