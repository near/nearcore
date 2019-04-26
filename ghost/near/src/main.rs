use actix::{Actor, System};
use std::sync::Arc;
use kvdb::KeyValueDB;

use near_client::ClientActor;
use near_chain::{ChainAdapter, RuntimeAdapter, Block, BlockStatus, Chain, BlockHeader, Store};
use near_network::NetworkActor;

struct ChainToPoolAndNetworkAdapter {}

impl ChainAdapter for ChainToPoolAndNetworkAdapter {
    fn block_accepted(&self, block: &Block, status: BlockStatus) {}
}

struct SampleRuntime {
    storage: Arc<KeyValueDB>,
}

impl SampleRuntime {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        SampleRuntime {
            storage
        }
    }
}

impl RuntimeAdapter for SampleRuntime {}


fn main() {
    let _ = env_logger::init();

    let system = System::new("NEAR");

    // TODO: Replace with rocksdb.
    let storage = Arc::new(kvdb_memorydb::create(5));

    let runtime = Arc::new(SampleRuntime::new(storage.clone()));
    let store = Arc::new(Store::new(storage));
    let chain_adapter = Arc::new(ChainToPoolAndNetworkAdapter {});
    let genesis = BlockHeader::default();
    let chain = Chain::new(store, chain_adapter, runtime, genesis).unwrap();

    let network_actor = NetworkActor{}.start();
    let client_actor = ClientActor::new(chain, network_actor.recipient()).unwrap();
    let addr = client_actor.start();

    system.run().unwrap();
}
