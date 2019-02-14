use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

use crate::Client;
use beacon::beacon_chain::BeaconBlockChain;
use configs::ChainSpec;
use node_runtime::test_utils::generate_test_chain_spec;
use primitives::signer::InMemorySigner;
use shard::ShardBlockChain;
use storage::test_utils::create_memory_db;
use primitives::beacon::SignedBeaconBlock;

/// Implements dummy client for testing. The differences with the real client:
/// * It does not do the correct signing;
/// * It has in-memory storage.
pub fn get_client_from_cfg(chain_spec: &ChainSpec, signer: InMemorySigner) -> Client {
    let storage = Arc::new(create_memory_db());
    let shard_chain = ShardBlockChain::new(chain_spec, storage.clone());
    let genesis = SignedBeaconBlock::genesis(shard_chain.chain.genesis_hash);
    let beacon_chain = BeaconBlockChain::new(genesis, chain_spec, storage.clone());
    Client {
        account_id: signer.account_id.clone(),
        signer,
        shard_chain,
        beacon_chain,
        pending_beacon_blocks: RwLock::new(HashMap::new()),
        pending_shard_blocks: RwLock::new(HashMap::new()),
    }
}

pub fn get_client() -> Client {
    let (chain_spec, signer, _) = generate_test_chain_spec();
    get_client_from_cfg(&chain_spec, signer)
}
