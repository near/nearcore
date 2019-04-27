use actix::{Actor, System};
use std::sync::{Arc, RwLock};
use kvdb::KeyValueDB;

use primitives::transaction::SignedTransaction;

use near_client::ClientActor;
use near_chain::{ChainAdapter, RuntimeAdapter, Block, BlockStatus, Chain, BlockHeader, Store, Provenance};
use near_network::NetworkActor;
use near_pool::TransactionPool;

mod adapters;
use adapters::{PoolToChainAdapter, ChainToPoolAndNetworkAdapter};

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
    let genesis = BlockHeader::default();
    let pool_adapter = Arc::new(PoolToChainAdapter::new());

    let tx_pool = Arc::new(RwLock::new(TransactionPool::new(pool_adapter.clone())));

    let chain_adapter = Arc::new(ChainToPoolAndNetworkAdapter::new(tx_pool.clone()));
    let chain = Arc::new(RwLock::new(Chain::new(store, chain_adapter, runtime, genesis).unwrap()));
    pool_adapter.set_chain(chain.clone());

    let network_actor = NetworkActor{}.start();
    let client_actor = ClientActor::new(chain, network_actor.recipient()).unwrap();
    let addr = client_actor.start();

    system.run().unwrap();
}
