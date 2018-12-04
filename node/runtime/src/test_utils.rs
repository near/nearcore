use StateDbViewer;
use chain_spec::ChainSpec;
use primitives::signature::get_keypair;
use std::sync::Arc;
use storage::test_utils::create_memory_db;
use Runtime;
use storage::StateDb;
use beacon::types::BeaconBlock;
use primitives::hash::CryptoHash;
use chain::BlockChain;

pub fn generate_test_chain_spec() -> ChainSpec {
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

pub fn get_test_state_db_viewer() -> StateDbViewer {
    let chain_spec = generate_test_chain_spec();
    let storage = Arc::new(create_memory_db());
    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = Runtime::new(state_db);
    let genesis_root =
        runtime.apply_genesis_state(&chain_spec.accounts, &chain_spec.genesis_wasm);

    let genesis = BeaconBlock::new(0, CryptoHash::default(), genesis_root, vec![]);
    let beacon_chain = BlockChain::new(genesis, storage);

    StateDbViewer {
        beacon_chain,
        runtime,
    }
}
