use std::rc::Rc;

use near_chain::{Block, ChainStore, ChainStoreAccess};
use near_primitives::hash::CryptoHash;

/// Iterate over the last N blocks in the blockchain
pub struct LastNBlocksIterator {
    chain_store: Rc<ChainStore>,
    blocks_left: u64,
    /// Hash of the block that will be returned when next() is called
    current_block_hash: Option<CryptoHash>,
}

impl LastNBlocksIterator {
    pub fn new(blocks_num: u64, chain_store: Rc<ChainStore>) -> LastNBlocksIterator {
        let current_block_hash = Some(chain_store.head().unwrap().last_block_hash);
        LastNBlocksIterator { chain_store, blocks_left: blocks_num, current_block_hash }
    }
}

impl Iterator for LastNBlocksIterator {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        // Decrease the amount of blocks left to produce
        match self.blocks_left.checked_sub(1) {
            Some(new_blocks_left) => self.blocks_left = new_blocks_left,
            None => return None,
        };

        if let Some(current_block_hash) = self.current_block_hash.take() {
            let current_block = self.chain_store.get_block(&current_block_hash).unwrap();

            // Set the previous block as "current" one, as long as the current one isn't the genesis block
            if current_block.header().height() != self.chain_store.get_genesis_height() {
                self.current_block_hash = Some(*current_block.header().prev_hash());
            }
            return Some(current_block);
        }

        None
    }
}
