use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::Client;
use beacon::authority::Authority;
use beacon::types::{BeaconBlockChain, SignedBeaconBlock};
use chain::SignedBlock;
use configs::authority::get_authority_config;
use configs::ChainSpec;
use node_runtime::state_viewer::StateDbViewer;
use node_runtime::test_utils::generate_test_chain_spec;
use node_runtime::Runtime;
use primitives::signer::InMemorySigner;
use shard::{ShardBlockChain, SignedShardBlock};
use storage::test_utils::create_memory_db;
use storage::StateDb;

/// Implements dummy client for testing. The differences with the real client:
/// * It does not do the correct signing;
/// * It has in-memory storage.
pub fn get_client_from_cfg(chain_spec: &ChainSpec, signer: InMemorySigner) -> Client {
    let storage = Arc::new(create_memory_db());
    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = RwLock::new(Runtime::new(state_db.clone()));
    let genesis_root = runtime.write().apply_genesis_state(
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities,
    );

    let shard_genesis = SignedShardBlock::genesis(genesis_root);
    let genesis = SignedBeaconBlock::genesis(shard_genesis.block_hash());
    let shard_chain = Arc::new(ShardBlockChain::new(shard_genesis, storage.clone()));
    let beacon_chain = BeaconBlockChain::new(genesis, storage.clone());

    let authority_config = get_authority_config(&chain_spec);
    let authority = RwLock::new(Authority::new(authority_config, &beacon_chain));

    let statedb_viewer = StateDbViewer::new(shard_chain.clone(), state_db.clone());
    Client {
        account_id: signer.account_id.clone(),
        state_db,
        authority,
        runtime,
        shard_chain,
        beacon_chain,
        signer,
        statedb_viewer,
        pending_beacon_blocks: RwLock::new(HashMap::new()),
        pending_shard_blocks: RwLock::new(HashMap::new()),
    }
}

pub fn get_client() -> Client {
    let (chain_spec, signer) = generate_test_chain_spec();
    get_client_from_cfg(&chain_spec, signer)
}
