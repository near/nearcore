use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::chain_spec::get_authority_config;
use crate::Client;
use beacon::authority::Authority;
use beacon::types::{BeaconBlockChain, SignedBeaconBlock};
use chain::SignedBlock;
use node_runtime::chain_spec::ChainSpec;
use node_runtime::Runtime;
use primitives::signer::InMemorySigner;
use shard::{ShardBlockChain, SignedShardBlock};
use storage::test_utils::create_memory_db;
use storage::StateDb;
use node_runtime::test_utils::generate_test_chain_spec;

/// Implements dummy client for testing. The differences with the real client:
/// * It does not do the correct signing;
/// * It has in-memory storage.
pub fn get_client_from_cfg(chain_spec: &ChainSpec, signer: InMemorySigner) -> Client {
    let storage = Arc::new(create_memory_db());
    let state_db = Arc::new(StateDb::new(storage.clone()));
    let runtime = Arc::new(RwLock::new(Runtime::new(state_db.clone())));
    let genesis_root = runtime.write().apply_genesis_state(
        &chain_spec.accounts,
        &chain_spec.genesis_wasm,
        &chain_spec.initial_authorities,
    );

    let shard_genesis = SignedShardBlock::genesis(genesis_root);
    let genesis = SignedBeaconBlock::genesis(shard_genesis.block_hash());
    let shard_chain = Arc::new(ShardBlockChain::new(shard_genesis, storage.clone()));
    let beacon_chain = Arc::new(BeaconBlockChain::new(genesis, storage.clone()));

    let authority_config = get_authority_config(&chain_spec);
    let authority = Arc::new(RwLock::new(Authority::new(authority_config, &beacon_chain)));

    Client {
        account_id: signer.account_id.clone(),
        state_db,
        authority,
        runtime,
        shard_chain,
        beacon_chain,
        signer: Arc::new(signer),
        pending_beacon_blocks: RwLock::new(HashMap::new()),
        pending_shard_blocks: RwLock::new(HashMap::new()),
    }
}

pub fn get_client() -> Client {
    let (chain_spec, signer) = generate_test_chain_spec();
    get_client_from_cfg(&chain_spec, signer)
}
