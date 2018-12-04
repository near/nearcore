use Client;
use node_runtime::test_utils::generate_test_chain_spec;
use primitives::signer::InMemorySigner;
use std::sync::Arc;
use storage::test_utils::create_memory_db;

pub fn generate_test_client() -> Client {
    let storage = Arc::new(create_memory_db());
    let chain_spec = generate_test_chain_spec();
    let signer = Arc::new(InMemorySigner::default());
    Client::new(&chain_spec, storage, signer)
}

impl Client {
    pub fn num_transactions(&self) -> usize {
        self.tx_pool.read().len()
    }

    pub fn num_blocks_in_queue(&self) -> usize {
        self.import_queue.read().len()
    }
}
