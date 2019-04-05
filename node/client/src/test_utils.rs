use std::collections::HashSet;
use std::sync::Arc;
use std::sync::RwLock;

use beacon::beacon_chain::BeaconClient;
use configs::ChainSpec;
use primitives::beacon::SignedBeaconBlock;
use primitives::crypto::signer::InMemorySigner;
use shard::ShardClient;
use storage::test_utils::create_beacon_shard_storages;

use crate::Client;

/// Creates a test client, that uses in memory storage.
pub fn get_client_from_cfg(chain_spec: &ChainSpec, signer: Arc<InMemorySigner>) -> Client {
    let (beacon_storage, shard_storage) = create_beacon_shard_storages();
    let shard_client = ShardClient::new(signer.clone(), chain_spec, shard_storage);
    let genesis = SignedBeaconBlock::genesis(shard_client.genesis_hash());
    let beacon_client = BeaconClient::new(genesis, chain_spec, beacon_storage);
    Client {
        account_id: signer.account_id.clone(),
        signer,
        shard_client,
        beacon_client,
        pending_beacon_blocks: RwLock::new(HashSet::new()),
        pending_shard_blocks: RwLock::new(HashSet::new()),
    }
}
