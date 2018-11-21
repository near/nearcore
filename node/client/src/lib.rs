extern crate beacon;
extern crate network;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;
#[macro_use]
extern crate log;

use beacon::chain::{Blockchain, ChainConfig};
use beacon::types::{BeaconBlock, BeaconBlockHeader};
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
    beacon_chain: RwLock<Blockchain<BeaconBlock>>,
    // transaction pool (put here temporarily)
    tx_pool: RwLock<Vec<SignedTransaction>>,
}

impl Client {
    pub fn new(storage: Arc<Storage>) -> Self {
        let state_db = StateDb::new(storage.clone());
        let state_view = state_db.get_state_view();
        let chain_config = ChainConfig {
            extra_col: storage::COL_BEACON_EXTRA,
            header_col: storage::COL_BEACON_HEADERS,
            block_col: storage::COL_BEACON_BLOCKS,
            index_col: storage::COL_BEACON_INDEX,
        };
        let genesis = BeaconBlock::new(0, CryptoHash::default(), BLSSignature::default(), vec![]);
        Client {
            runtime: Runtime::default(),
            state_db: RwLock::new(state_db),
            last_root: RwLock::new(state_view),
            beacon_chain: RwLock::new(Blockchain::new(chain_config, genesis, storage)),
            tx_pool: RwLock::new(vec![]),
        }
    }

    pub fn receive_transaction(&self, t: SignedTransaction) {
        debug!(target: "client", "receive transaction {:?}", t);
        self.tx_pool.write().push(t);
    }

    pub fn view_call(&self, view_call: &ViewCall) -> ViewCallResult {
        let mut state_db = self.state_db.write();
        self.runtime
            .view(&mut state_db, &self.last_root.read(), view_call)
    }

    pub fn handle_signed_transaction(&self, t: SignedTransaction) -> GenericResult {
        debug!(target: "client", "handle transaction {:?}", t);
        self.tx_pool.write().push(t);
        Ok(())
    }
}

impl network::client::Client<BeaconBlock, SignedTransaction> for Client {
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
    fn best_index(&self) -> u64 {
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
    fn prod_block(&self) -> BeaconBlock {
        // TODO: compute actual merkle root and state, as well as signature, and
        // use some reasonable fork-choice rule
        let transactions = std::mem::replace(&mut *self.tx_pool.write(), vec![]);
        let header = BeaconBlockHeader {
            prev_hash: self.beacon_chain.read().best_hash,
            merkle_root_tx: CryptoHash::default(),
            merkle_root_state: CryptoHash::default(),
            signature: BLSSignature::default(),
            index: self.beacon_chain.read().best_number,
        };
        BeaconBlock::new(header, transactions)
    }
}
