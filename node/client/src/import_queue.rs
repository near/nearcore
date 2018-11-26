use beacon::types::BeaconBlock;
use primitives::hash::CryptoHash;
use primitives::traits::Header;
use std::collections::HashMap;

pub struct ImportQueue {
    queue: HashMap<CryptoHash, BeaconBlock>,
}

impl ImportQueue {
    pub fn new() -> Self {
        ImportQueue {
            queue: HashMap::new(),
        }
    }

    pub fn insert(&mut self, block: BeaconBlock) {
        let parent_hash = block.header.parent_hash();
        self.queue.insert(parent_hash, block);
    }

    #[allow(dead_code)]
    pub fn contains_block(&self, block: &BeaconBlock) -> bool {
        let parent_hash = block.header.parent_hash();
        self.queue.contains_key(&parent_hash)
    }

    #[allow(dead_code)]
    pub fn contains_hash(&self, hash: &CryptoHash) -> bool {
        self.queue.contains_key(hash)
    }

    pub fn remove(&mut self, hash: &CryptoHash) -> Option<BeaconBlock> {
        self.queue.remove(hash)
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.queue.len()
    }
}
