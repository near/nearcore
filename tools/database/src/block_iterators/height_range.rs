use std::rc::Rc;

use near_chain::{Block, ChainStore, ChainStoreAccess, Error};
use near_primitives::{hash::CryptoHash, types::BlockHeight};

/// Iterate over blocks between two block heights.
/// `from_height` and `to_height` are inclusive
pub struct BlockHeightRangeIterator {
    chain_store: Rc<ChainStore>,
    from_block_height: BlockHeight,
    /// Hash of the block that will be returned when next() is called
    current_block_hash: Option<CryptoHash>,
}

impl BlockHeightRangeIterator {
    /// Create an iterator which iterates over blocks between from_height and to_height.
    /// `from_height` and `to_height` are inclusive.
    /// Both arguments are optional, passing `None`` means that the limit is +- infinity.
    pub fn new(
        from_height_opt: Option<BlockHeight>,
        to_height_opt: Option<BlockHeight>,
        chain_store: Rc<ChainStore>,
    ) -> BlockHeightRangeIterator {
        if let (Some(from), Some(to)) = (&from_height_opt, &to_height_opt) {
            if *from > *to {
                // Empty iterator
                return BlockHeightRangeIterator {
                    chain_store,
                    from_block_height: 0,
                    current_block_hash: None,
                };
            }
        }

        let min_height = chain_store.get_genesis_height();
        let max_height = chain_store.head().unwrap().height;

        let from_height = from_height_opt.unwrap_or(min_height);
        let mut to_height = to_height_opt.unwrap_or(max_height);

        // There is no point in going over nonexisting blocks past the highest height
        if to_height > max_height {
            to_height = max_height;
        }

        // A block with height `to_height` might not exist.
        // Go over the range in reverse and find the highest block that exists.
        let mut current_block_hash: Option<CryptoHash> = None;
        for height in (from_height..=to_height).rev() {
            match chain_store.get_block_hash_by_height(height) {
                Ok(hash) => {
                    current_block_hash = Some(hash);
                    break;
                }
                Err(Error::DBNotFoundErr(_)) => continue,
                err => err.unwrap(),
            };
        }

        BlockHeightRangeIterator { chain_store, from_block_height: from_height, current_block_hash }
    }
}

impl Iterator for BlockHeightRangeIterator {
    type Item = Block;

    fn next(&mut self) -> Option<Block> {
        let current_block_hash = match self.current_block_hash.take() {
            Some(hash) => hash,
            None => return None,
        };
        let current_block = self.chain_store.get_block(&current_block_hash).unwrap();

        // Make sure that the block is within the from..=to range, stop iterating if it isn't
        if current_block.header().height() >= self.from_block_height {
            // Set the previous block as "current" one, as long as the current one isn't the genesis block
            if current_block.header().height() != self.chain_store.get_genesis_height() {
                self.current_block_hash = Some(*current_block.header().prev_hash());
            }

            return Some(current_block);
        }

        None
    }
}
