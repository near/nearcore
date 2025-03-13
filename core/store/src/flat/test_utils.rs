use super::BlockInfo;
use near_primitives::hash::{CryptoHash, hash};
use near_primitives::types::BlockHeight;
use std::collections::HashMap;

pub struct MockChain {
    height_to_hashes: HashMap<BlockHeight, CryptoHash>,
    blocks: HashMap<CryptoHash, BlockInfo>,
    head_height: BlockHeight,
}

impl MockChain {
    pub fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo {
        *self.blocks.get(block_hash).unwrap()
    }

    pub fn block_hash(height: BlockHeight) -> CryptoHash {
        hash(&borsh::to_vec(&height).unwrap())
    }

    /// Build a chain with given set of heights and a function mapping block heights to heights of their parents.
    pub fn build(
        heights: Vec<BlockHeight>,
        get_parent: fn(BlockHeight) -> Option<BlockHeight>,
    ) -> MockChain {
        let height_to_hashes: HashMap<_, _> =
            heights.iter().cloned().map(|height| (height, MockChain::block_hash(height))).collect();
        let blocks = heights
            .iter()
            .cloned()
            .map(|height| {
                let hash = *height_to_hashes.get(&height).unwrap();
                let prev_hash = match get_parent(height) {
                    None => CryptoHash::default(),
                    Some(parent_height) => *height_to_hashes.get(&parent_height).unwrap(),
                };
                (hash, BlockInfo { hash, height, prev_hash })
            })
            .collect();
        MockChain { height_to_hashes, blocks, head_height: *heights.last().unwrap() }
    }

    // Create a chain with no forks with length n.
    pub fn linear_chain(n: usize) -> MockChain {
        Self::build((0..n as BlockHeight).collect(), |i| if i == 0 { None } else { Some(i - 1) })
    }

    // Create a linear chain of length n where blocks with odd numbers are skipped:
    // 0 -> 2 -> 4 -> ...
    pub fn linear_chain_with_skips(n: usize) -> MockChain {
        Self::build((0..n as BlockHeight).map(|i| i * 2).collect(), |i| {
            if i == 0 { None } else { Some(i - 2) }
        })
    }

    // Create a chain with two forks, where blocks 1 and 2 have a parent block 0, and each next block H
    // has a parent block H-2:
    // 0 |-> 1 -> 3 -> 5 -> ...
    //   --> 2 -> 4 -> 6 -> ...
    pub fn chain_with_two_forks(n: usize) -> MockChain {
        Self::build(
            (0..n as BlockHeight).collect(),
            |i| {
                if i == 0 { None } else { Some(i.max(2) - 2) }
            },
        )
    }

    pub fn get_block_hash(&self, height: BlockHeight) -> CryptoHash {
        *self.height_to_hashes.get(&height).unwrap()
    }

    pub fn get_block(&self, height: BlockHeight) -> BlockInfo {
        self.blocks[&self.height_to_hashes[&height]]
    }

    /// create a new block on top the current chain head, return the new block hash
    pub fn create_block(&mut self) -> CryptoHash {
        let hash = MockChain::block_hash(self.head_height + 1);
        self.height_to_hashes.insert(self.head_height + 1, hash);
        self.blocks.insert(
            hash,
            BlockInfo {
                hash,
                height: self.head_height + 1,
                prev_hash: self.get_block_hash(self.head_height),
            },
        );
        self.head_height += 1;
        hash
    }
}
