use parking_lot::RwLock;
use std::collections::HashMap;

use crate::Client;
use beacon::beacon_chain::BeaconBlockChain;
use configs::ChainSpec;
use node_runtime::test_utils::generate_test_chain_spec;
use primitives::signer::InMemorySigner;
use shard::ShardBlockChain;
use primitives::beacon::SignedBeaconBlock;
use storage::test_utils::create_beacon_shard_storages;

/// Implements dummy client for testing. The differences with the real client:
/// * It does not do the correct signing;
/// * It has in-memory storage.
pub fn get_client_from_cfg(chain_spec: &ChainSpec, signer: InMemorySigner) -> Client {
    let (beacon_storage, shard_storage) = create_beacon_shard_storages();
    let shard_chain = ShardBlockChain::new(chain_spec, shard_storage);
    let genesis = SignedBeaconBlock::genesis(shard_chain.genesis_hash());
    let beacon_chain = BeaconBlockChain::new(genesis, chain_spec, beacon_storage);
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
