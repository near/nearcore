use chain_spec::ChainSpec;
use Client;
use primitives::signer::InMemorySigner;
use primitives::types::AccountAlias;
use std::sync::Arc;
use storage::test_utils::create_memory_db;

fn generate_test_chain_spec() -> ChainSpec {
    let genesis_wasm = include_bytes!(
        "../../../core/wasm/runtest/res/wasm_with_mem.wasm"
    ).to_vec();
    ChainSpec {
        balances: vec![("alice".into(), 100), ("bob".into(), 100)],
        initial_authorities: vec![AccountAlias::from("alice")],
        genesis_wasm,
    }
}

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
