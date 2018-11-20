extern crate beacon;
extern crate network;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;

use beacon::chain::BeaconChain;
use beacon::types::{BeaconBlock, BeaconBlockHeader};
use node_runtime::Runtime;
use parking_lot::RwLock;
use primitives::hash::CryptoHash;
use primitives::types::{BlockId, MerkleHash, SignedTransaction, ViewCall, ViewCallResult};
use storage::{StateDb, Storage};

#[allow(dead_code)]
pub struct Client {
    state_db: RwLock<StateDb>,
    runtime: Runtime,
    last_root: RwLock<MerkleHash>,
    beacon_chain: RwLock<BeaconChain>,
}

impl Client {
    pub fn new(storage: Storage) -> Self {
        let state_db = StateDb::new(storage);
        let state_view = state_db.get_state_view();
        let genesis_hash = CryptoHash::default();
        Client {
            runtime: Runtime::default(),
            state_db: RwLock::new(state_db),
            last_root: RwLock::new(state_view),
            beacon_chain: RwLock::new(BeaconChain::new(genesis_hash)),
        }
    }

    pub fn receive_transaction(&self, t: SignedTransaction) {
        println!("{:?}", t);
        // TODO: Put into the non-existent pool or TxFlow?
        let mut state_db = self.state_db.write();
        let (_, new_root) = self
            .runtime
            .apply(&mut state_db, &self.last_root.read(), vec![t]);
        *self.last_root.write() = new_root;
    }

    pub fn view_call(&self, view_call: &ViewCall) -> ViewCallResult {
        let mut state_db = self.state_db.write();
        self.runtime
            .view(&mut state_db, &self.last_root.read(), view_call)
    }
}

impl network::client::Client<BeaconBlock> for Client {
    fn get_block(&self, id: &BlockId) -> Option<BeaconBlock> {
        self.beacon_chain.read().get_block(id).cloned()
    }
    fn get_header(&self, id: &BlockId) -> Option<BeaconBlockHeader> {
        self.beacon_chain.read().get_header(id).cloned()
    }
    fn best_hash(&self) -> CryptoHash {
        self.beacon_chain.read().best_hash
    }
    fn best_number(&self) -> u64 {
        self.beacon_chain.read().best_number
    }
    fn genesis_hash(&self) -> CryptoHash {
        self.beacon_chain.read().genesis_hash
    }
    fn import_blocks(&self, blocks: Vec<BeaconBlock>) {
        let mut beacon_chain = self.beacon_chain.write();
        for block in blocks {
            beacon_chain.add_block(block);
        }
    }
}
