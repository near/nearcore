use super::Client;
use beacon::types::{BeaconBlock, BeaconBlockHeader};
use node_runtime::ApplyState;
use primitives::hash::CryptoHash;
use primitives::traits::{Block, Header};
use primitives::types::BlockId;

/// Chain Backend trait
/// an abstraction that communicates chain info to network
pub trait Chain<B: Block> {
    // get block from id
    fn get_block(&self, id: &BlockId) -> Option<B>;
    // get block header from id
    fn get_header(&self, id: &BlockId) -> Option<B::Header>;
    // hash of latest block
    fn best_hash(&self) -> CryptoHash;
    // index of latest block
    fn best_index(&self) -> u64;
    // genesis hash
    fn genesis_hash(&self) -> CryptoHash;
    // import blocks
    fn import_blocks(&self, blocks: Vec<B>);
    // produce block
    fn prod_block(&self) -> B;
}

impl Chain<BeaconBlock> for Client {
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
        best_block.header().index()
    }

    fn genesis_hash(&self) -> CryptoHash {
        self.beacon_chain.genesis_hash
    }

    fn import_blocks(&self, blocks: Vec<BeaconBlock>) {
        for block in blocks {
            let mut hash = block.hash();
            let mut b = block;
            while self.import_block(b) {
                match self.import_queue.write().remove(&hash) {
                    Some(next_block) => {
                        b = next_block;
                        hash = b.hash();
                    }
                    None => {
                        break;
                    }
                };
            }
        }
    }

    fn prod_block(&self) -> BeaconBlock {
        // TODO: compute actual merkle root and state, as well as signature, and
        // use some reasonable fork-choice rule
        let last_block = self.beacon_chain.best_block();
        let (transactions, mut receipts) = self.tx_pool.write().drain();
        let apply_state = ApplyState {
            root: last_block.header().body.merkle_root_state,
            parent_block_hash: last_block.hash(),
            block_index: last_block.header().index() + 1,
        };
        let (filtered_transactions, filtered_receipts, mut apply_result) =
            self.runtime.borrow_mut().apply(&apply_state, transactions, &mut receipts);
        self.state_db.commit(&mut apply_result.transaction).ok();
        let mut block = BeaconBlock::new(
            last_block.header().index() + 1,
            last_block.hash(),
            apply_result.root,
            filtered_transactions,
            filtered_receipts,
        );
        block.sign(&self.signer);
        self.beacon_chain.insert_block(block.clone());
        block
    }
}
