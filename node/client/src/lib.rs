extern crate beacon;
extern crate network;
extern crate node_runtime;
extern crate parking_lot;
extern crate primitives;
extern crate storage;
#[macro_use]
extern crate log;

use beacon::chain::{BlockChain, ChainConfig};
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
    beacon_chain: BlockChain<BeaconBlock>,
    // transaction pool (put here temporarily)
    tx_pool: RwLock<Vec<SignedTransaction>>,
    // import queue for receiving blocks
    import_queue: RwLock<Vec<BeaconBlock>>,
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
            beacon_chain: BlockChain::new(chain_config, genesis, storage),
            tx_pool: RwLock::new(vec![]),
            import_queue: RwLock::new(vec![]),
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

    #[allow(unused)]
    fn validate_signature(&self, block: &BeaconBlock) -> bool {
        // TODO: validate multisig
        true
    }

    /// import a block. Returns true if it is successfully imported to the chain
    fn import_block(&self, block: BeaconBlock) -> bool {
        let parent_hash = block.header.parent_hash;
        let is_known_block = self.beacon_chain.is_known(&block.hash());
        if !is_known_block
            && self.beacon_chain.is_known(&parent_hash)
            && self.validate_signature(&block)
        {
            let mut state_db = self.state_db.write();
            let (header, transactions) = block.deconstruct();
            let last_root = self.last_root.read();
            let (filtered_transactions, new_root) =
                self.runtime.apply(&mut state_db, &last_root, transactions);
            if new_root != header.merkle_root_tx {
                // TODO: something bad happened, what should we do
                unimplemented!();
            }
            // Do we want all transactions to be in the block?
            let block = Block::new(header, filtered_transactions);
            self.beacon_chain.insert_block(block);
            true
        } else if !is_known_block {
            self.import_queue.write().push(block);
            false
        } else {
            // the block is already known, discard it?
            false
        }
    }
}

impl network::client::Client<BeaconBlock> for Client {
    fn get_block(&self, id: &BlockId) -> Option<BeaconBlock> {
        self.beacon_chain.get_block(id)
    }
    fn get_header(&self, id: &BlockId) -> Option<BeaconBlockHeader> {
        self.beacon_chain.get_header(id)
    }
    fn best_hash(&self) -> CryptoHash {
        let best_block = self.beacon_chain.best_block();
        best_block.hash()
    }
    fn best_index(&self) -> u64 {
        let best_block = self.beacon_chain.best_block();
        best_block.header().index
    }
    fn genesis_hash(&self) -> CryptoHash {
        self.beacon_chain.genesis_hash
    }
    fn import_blocks(&self, blocks: Vec<BeaconBlock>) {
        for block in blocks {
            // might not be the most efficient way to do this
            // also related to fork choice rule
            if self.import_block(block) {
                let old_queue = std::mem::replace(&mut *self.import_queue.write(), vec![]);
                for b in old_queue {
                    self.import_block(b);
                }
            }
        }
    }
    fn prod_block(&self) -> BeaconBlock {
        // TODO: compute actual merkle root and state, as well as signature, and
        // use some reasonable fork-choice rule
        let transactions = std::mem::replace(&mut *self.tx_pool.write(), vec![]);
        let parent_hash = self.best_hash();
        let index = self.best_index();
        BeaconBlock::new(
            index + 1,
            parent_hash,
            BLSSignature::default(),
            transactions,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use primitives::hash::hash_struct;
    use storage::MemoryStorage;

    #[test]
    fn test_import_queue_empty() {
        let client = Client::new(Arc::new(MemoryStorage::new()));
        let parent_hash = client.beacon_chain.genesis_hash;
        let block1 = BeaconBlock::new(1, parent_hash, 0, vec![SignedTransaction::default()]);
        client.import_block(block1);
        assert_eq!(client.import_queue.read().len(), 0);
    }

    #[test]
    fn test_import_queue_non_empty() {
        let client = Client::new(Arc::new(MemoryStorage::new()));
        let parent_hash = client.beacon_chain.genesis_hash;
        let block1 = BeaconBlock::new(1, hash_struct(&1), 0, vec![]);
        client.import_block(block1);
        assert_eq!(client.import_queue.read().len(), 1);
        let block2 = BeaconBlock::new(1, parent_hash, 0, vec![]);
        client.import_block(block2);
        assert_eq!(client.import_queue.read().len(), 1);
    }

    #[test]
    fn test_duplicate_import() {
        let client = Client::new(Arc::new(MemoryStorage::new()));
        let parent_hash = client.beacon_chain.genesis_hash;
        let block0 = BeaconBlock::new(0, parent_hash, 0, vec![]);
        client.import_block(block0);
        assert_eq!(client.import_queue.read().len(), 0);
    }

    #[test]
    fn test_import_blocks() {
        let client = Client::new(Arc::new(MemoryStorage::new()));
        let parent_hash = client.beacon_chain.genesis_hash;
        let block1 = BeaconBlock::new(1, parent_hash, 0, vec![SignedTransaction::default()]);
        let block2 = BeaconBlock::new(2, block1.hash(), 0, vec![SignedTransaction::default()]);
        network::client::Client::import_blocks(&client, vec![block1, block2]);
        assert_eq!(client.import_queue.read().len(), 0);
    }

}
