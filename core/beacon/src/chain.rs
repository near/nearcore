use primitives::hash::CryptoHash;
use primitives::traits::{Block, Header};
use std::collections::HashMap;
use types::{BeaconBlock, BeaconBlockHeader};

#[derive(Debug)]
pub struct BeaconChain {
    /// genesis hash
    genesis_hash: CryptoHash,
    /// hash of tip
    best_hash: CryptoHash,
    /// index of tip
    best_number: u64,
    /// headers indexed by hash
    headers: HashMap<CryptoHash, BeaconBlockHeader>,
    /// blocks indexed by hash
    hash_to_blocks: HashMap<CryptoHash, BeaconBlock>,
    // TODO: state?
}

impl BeaconChain {
    pub fn new(genesis_hash: CryptoHash) -> Self {
        BeaconChain {
            genesis_hash,
            best_hash: genesis_hash,
            best_number: 0,
            headers: HashMap::new(),
            hash_to_blocks: HashMap::new(),
        }
    }

    pub fn add_block(&mut self, block: BeaconBlock) {
        let block_hash = block.hash();
        if self.hash_to_blocks.contains_key(&block_hash) {
            return;
        }
        let header = block.header().clone();
        self.best_hash = block_hash;
        self.best_number = header.number();
        self.headers.insert(block_hash, header);
        self.hash_to_blocks.insert(block_hash, block);
    }
}
