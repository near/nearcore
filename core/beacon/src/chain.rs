use byteorder::{LittleEndian, WriteBytesExt};
use parking_lot::RwLock;
use primitives::hash::CryptoHash;
use primitives::traits::{Block, Decode, Encode};
use primitives::types::BlockId;
use std::collections::HashMap;
use std::sync::Arc;
use storage::Storage;
use types::{BeaconBlock, BeaconBlockHeader};

const BEACON_CHAIN_BEST_BLOCK: &[u8] = b"beacon_best";

pub struct BeaconChain {
    /// Storage backend.
    storage: Arc<Storage>,
    /// Genesis hash
    pub genesis_hash: CryptoHash,
    /// Tip of the known chain.
    best_block: RwLock<BeaconBlock>,
    /// Headers indexed by hash
    headers: RwLock<HashMap<CryptoHash, BeaconBlockHeader>>,
    /// Blocks indexed by hash
    hash_to_blocks: RwLock<HashMap<CryptoHash, BeaconBlock>>,
    /// Block index to hash map.
    index_to_hash: RwLock<HashMap<u64, CryptoHash>>,
    // TODO: state?
}

fn index_to_bytes(index: u64) -> Vec<u8> {
    let mut bytes = vec![];
    bytes
        .write_u64::<LittleEndian>(index)
        .expect("writing to bytes failed");
    bytes
}

impl BeaconChain {
    pub fn new(genesis: BeaconBlock, storage: Arc<Storage>) -> Self {
        let genesis_hash = genesis.hash();
        let mut bc = BeaconChain {
            storage,
            genesis_hash,
            best_block: RwLock::new(genesis.clone()),
            headers: RwLock::new(HashMap::new()),
            hash_to_blocks: RwLock::new(HashMap::new()),
            index_to_hash: RwLock::new(HashMap::new()),
        };

        // Load best block hash from storage.
        let best_block_hash = match bc.storage.get(BEACON_CHAIN_BEST_BLOCK) {
            Some(best_hash) => CryptoHash::new(&best_hash),
            _ => {
                // Insert genesis block into cache.
                bc.insert_block(genesis);
                genesis_hash
            }
        };

        {
            let mut best_block = bc.best_block.write();
            *best_block = bc
                .get_block(&BlockId::Hash(best_block_hash))
                .expect("Not found best block in the chain");
        }

        // Load best block into cache.
        bc
    }

    pub fn best_block(&self) -> BeaconBlock {
        self.best_block.read().clone()
    }

    /// Check if block already is known.
    pub fn is_known(&self, hash: &CryptoHash) -> bool {
        self.hash_to_blocks.read().contains_key(hash) || self.storage.exists(hash.as_ref())
    }

    /// Inserts a verified block.
    /// Returns true if block is disconnected.
    pub fn insert_block(&mut self, block: BeaconBlock) -> bool {
        let block_hash = block.hash();
        if self.is_known(&block_hash) {
            return false;
        }

        // Store block in db.
        let block_bytes = Encode::encode(&block).expect("Error serializing block");
        //        let block_header_bytes =
        //            Encode::encode(&block.header()).expect("Error serializing block header");
        self.storage.set(block_hash.as_ref(), &block_bytes);
        // TODO: use different column.
        //        self.storage.set(block_hash.as_ref(), &block_header_bytes);
        self.storage
            .set(&index_to_bytes(block.header.index), block_hash.as_ref());

        // Insert into cache.
        self.hash_to_blocks
            .write()
            .insert(block_hash, block.clone());
        self.headers
            .write()
            .insert(block_hash, block.header.clone());
        self.index_to_hash
            .write()
            .insert(block.header.index, block_hash);

        let maybe_parent = self.get_header(&BlockId::Hash(block.header.prev_hash));
        if let Some(_parent_details) = maybe_parent {
            // TODO: rewind parents if they were not processed somehow?
            if block.header.index > self.best_block.read().header.index {
                let mut best_block = self.best_block.write();
                *best_block = block;
                self.storage
                    .set(BEACON_CHAIN_BEST_BLOCK, block_hash.as_ref());
            }
            false
        } else {
            true
        }
    }

    fn get_block_hash_by_index(&self, index: u64) -> Option<CryptoHash> {
        match self.index_to_hash.read().get(&index) {
            Some(value) => Some(*value),
            None => match self.storage.get(&index_to_bytes(index)) {
                Some(value) => Some(CryptoHash::new(&value)),
                _ => None,
            },
        }
    }

    fn get_block_by_hash(&self, block_hash: &CryptoHash) -> Option<BeaconBlock> {
        {
            let hash_to_blocks = self.hash_to_blocks.read();
            if let Some(value) = hash_to_blocks.get(block_hash) {
                return Some(value.clone());
            }
        }

        match self.storage.get(block_hash.as_ref()) {
            Some(value) => {
                let r: Option<BeaconBlock> = Decode::decode(&value);
                match r {
                    Some(block) => {
                        let result = Some(block.clone());
                        self.hash_to_blocks.write().insert(*block_hash, block);
                        result
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }

    pub fn get_block(&self, id: &BlockId) -> Option<BeaconBlock> {
        match id {
            BlockId::Number(num) => match self.get_block_hash_by_index(*num) {
                Some(hash) => self.get_block_by_hash(&hash),
                _ => None,
            },
            BlockId::Hash(hash) => self.get_block_by_hash(hash),
        }
    }

    pub fn get_header(&self, id: &BlockId) -> Option<BeaconBlockHeader> {
        let headers = self.headers.read();
        match id {
            BlockId::Number(_num) => None,
            BlockId::Hash(hash) => match headers.get(hash) {
                Some(value) => Some(value.clone()),
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use primitives::types::BLSSignature;
    use std::sync::Arc;
    use storage::MemoryStorage;

    #[test]
    fn test_genesis() {
        let storage = Arc::new(MemoryStorage::default());
        let genesis = BeaconBlock::new(0, CryptoHash::default(), BLSSignature::default(), vec![]);
        let bc = BeaconChain::new(genesis.clone(), storage);
        assert_eq!(
            bc.get_block(&BlockId::Hash(genesis.hash())).unwrap(),
            genesis
        );
        assert_eq!(bc.get_block(&BlockId::Number(0)).unwrap(), genesis);
    }

    #[test]
    fn test_restart_chain() {
        let storage = Arc::new(MemoryStorage::default());
        let genesis = BeaconBlock::new(0, CryptoHash::default(), BLSSignature::default(), vec![]);
        let mut bc = BeaconChain::new(genesis.clone(), storage.clone());
        let block1 = BeaconBlock::new(1, genesis.hash(), BLSSignature::default(), vec![]);
        assert_eq!(bc.insert_block(block1.clone()), false);
        assert_eq!(bc.best_block().hash(), block1.hash());
        assert_eq!(bc.best_block().header.index, 1);
        // Create new blockchain that reads from the same storage.
        let other_bc = BeaconChain::new(genesis.clone(), storage.clone());
        assert_eq!(other_bc.best_block().hash(), block1.hash());
        assert_eq!(other_bc.best_block().header.index, 1);
        assert_eq!(
            other_bc.get_block(&BlockId::Hash(block1.hash())).unwrap(),
            block1
        );
    }
}
