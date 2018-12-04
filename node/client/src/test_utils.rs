use chain_spec::ChainSpec;
use primitives::signature::get_keypair;
use primitives::signer::InMemorySigner;
use std::sync::Arc;
use storage::test_utils::create_memory_db;
use Client;

fn generate_test_chain_spec() -> ChainSpec {
    let genesis_wasm = include_bytes!("../../../core/wasm/runtest/res/wasm_with_mem.wasm").to_vec();
    let (public_key, _) = get_keypair();
    ChainSpec {
        accounts: vec![("alice".to_string(), public_key.to_string(), 100)],
        initial_authorities: vec![(public_key.to_string(), 50)],
        genesis_wasm,
        beacon_chain_epoch_length: 2,
        beacon_chain_num_seats_per_slot: 10,
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
