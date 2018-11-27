use std::sync::Arc;

use chain_spec::ChainSpec;
use primitives::signer::InMemorySigner;
use storage::test_utils::create_memory_db;
use Client;

pub fn generate_test_client() -> Client {
    let storage = Arc::new(create_memory_db());
    let chain_spec = ChainSpec { balances: vec![], initial_authorities: vec![] };
    let signer = Arc::new(InMemorySigner::default());
    Client::new(&chain_spec, storage, signer)
}
