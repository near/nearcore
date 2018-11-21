extern crate beacon;
extern crate network;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;

use beacon::chain::BeaconChain;
use beacon::types::{BeaconBlock, BeaconBlockHeader};
use network::protocol::Transaction;
use node_runtime::Runtime;
use parking_lot::RwLock;
use primitives::hash::CryptoHash;
use primitives::traits::{Block, GenericResult};
use primitives::types::{
    BLSSignature, BlockId, MerkleHash, SignedTransaction, ViewCall, ViewCallResult,
};
use std::sync::Arc;
use storage::{StateDb, Storage};

#[allow(dead_code)]
pub struct Client {
    state_db: RwLock<StateDb>,
    runtime: Runtime,
    last_root: RwLock<MerkleHash>,
    beacon_chain: RwLock<BeaconChain>,
}

impl Client {
    pub fn new(storage: &Arc<Storage>) -> Self {
        let state_db = StateDb::new(&storage.clone());
        let state_view = state_db.get_state_view();
        let genesis = BeaconBlock::new(0, CryptoHash::default(), BLSSignature::default(), vec![]);
        Client {
            runtime: Runtime::default(),
            state_db: RwLock::new(state_db),
            last_root: RwLock::new(state_view),
            beacon_chain: RwLock::new(BeaconChain::new(genesis, storage.clone())),
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

    pub fn handle_signed_transaction<T: Transaction>(&self, t: &T) -> GenericResult {
        println!("{:?}", t);
        Ok(())
    }
}

impl network::client::Client<BeaconBlock> for Client {
    fn get_block(&self, id: &BlockId) -> Option<BeaconBlock> {
        self.beacon_chain.read().get_block(id)
    }
    fn get_header(&self, id: &BlockId) -> Option<BeaconBlockHeader> {
        self.beacon_chain.read().get_header(id)
    }
    fn best_hash(&self) -> CryptoHash {
        let best_block = self.beacon_chain.read().best_block();
        best_block.hash()
    }
    fn best_number(&self) -> u64 {
        let best_block = self.beacon_chain.read().best_block();
        best_block.header().index
    }
    fn genesis_hash(&self) -> CryptoHash {
        self.beacon_chain.read().genesis_hash
    }
    fn import_blocks(&self, blocks: Vec<BeaconBlock>) {
        let mut beacon_chain = self.beacon_chain.write();
        for block in blocks {
            beacon_chain.insert_block(block);
        }
    }
}
