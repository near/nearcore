use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use primitives::hash::CryptoHash;
use primitives::types::BlockId;
use primitives::utils::index_to_bytes;
use storage::{read_with_cache, write_with_cache, Storage};
use serde_derive::{Serialize, Deserialize};
use log::info;

use primitives::block_traits::{SignedHeader, SignedBlock};

const BLOCKCHAIN_BEST_BLOCK: &[u8] = b"best";

/// A block plus its "virtual" fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockIndex<B> {
    pub block: B,
    // Note: zero weight indicates orphan-chain (ancestry cannot be traced to genesis)
    pub cumulative_weight: u128,
}

/// General BlockChain container.
pub struct BlockChain<B: SignedBlock> {
    /// Storage backend.
    storage: Arc<Storage>,
    /// Genesis hash
    pub genesis_hash: CryptoHash,
    /// Best block key of current blockchain. Key length is CryptoHash length + "best" bytes.
    /// This uses the genesis hash in order to support multiple chains.
    best_block_key: [u8; 36],
    /// Tip of the known chain.
    best_block_index: RwLock<BlockIndex<B>>,
    /// Headers indexed by hash
    headers: RwLock<HashMap<Vec<u8>, B::SignedHeader>>,
    /// Blocks indexed by hash
    blocks: RwLock<HashMap<Vec<u8>, BlockIndex<B>>>,
    /// Maps block index to hash.
    // TODO: This doesn't handle forks at all and needs to be rewritten
    index_to_hash: RwLock<HashMap<Vec<u8>, CryptoHash>>,
    // TODO: state?
}

impl<B: SignedBlock> BlockChain<B> {
    pub fn new(genesis: B, storage: Arc<Storage>) -> Self {
        let genesis_hash = genesis.block_hash();
        let mut best_block_key = [0; 36];
        best_block_key[..32].copy_from_slice(genesis_hash.as_ref());
        best_block_key[32..].copy_from_slice(BLOCKCHAIN_BEST_BLOCK);
        let genesis_index = BlockIndex {
            block: genesis,
            cumulative_weight: 1,
        };
        let bc = BlockChain {
            storage,
            genesis_hash,
            best_block_key,
            best_block_index: RwLock::new(genesis_index.clone()),
            headers: RwLock::new(HashMap::new()),
            blocks: RwLock::new(HashMap::new()),
            index_to_hash: RwLock::new(HashMap::new()),
        };

        // Load best block hash from storage.
        let best_block_hash = match bc.storage.get(storage::COL_EXTRA, &bc.best_block_key) {
            Ok(Some(best_hash)) => CryptoHash::new(best_hash.as_ref()),
            _ => {
                // Insert genesis block into cache.
                bc.insert_block_index(&genesis_index);
                genesis_hash
            }
        };

        {
            let mut best_block = bc.best_block_index.write();
            *best_block = bc
                .get_block_index(&BlockId::Hash(best_block_hash))
                .expect("Not found best block in the chain");
        }

        // Load best block into cache.
        bc
    }

    pub fn best_block_index(&self) -> BlockIndex<B> {
        self.best_block_index.read().clone()
    }

    pub fn best_block(&self) -> B {
        self.best_block_index().block
    }

    #[inline]
    pub fn best_index(&self) -> u64 {
        self.best_block().header().index()
    }

    #[inline]
    pub fn best_hash(&self) -> CryptoHash {
        self.best_block().block_hash()
    }

    /// Check if block already is known.
    pub fn is_known(&self, hash: &CryptoHash) -> bool {
        if self.headers.read().contains_key(hash.as_ref()) {
            return true;
        }
        match self.storage.get(storage::COL_HEADERS, hash.as_ref()) {
            Ok(Some(_)) => true,
            _ => false,
        }
    }

    fn update_best_block(&self, block_index: BlockIndex<B>) {
        let block_hash = block_index.block.block_hash();
        let mut best_block_index = self.best_block_index.write();
        *best_block_index = block_index;
        let mut db_transaction = self.storage.transaction();
        db_transaction.put(storage::COL_EXTRA, &self.best_block_key, block_hash.as_ref());
        self.storage.write(db_transaction).expect("Database write failed");
    }

    /// Inserts a verified block.
    /// Returns true if block is disconnected.
    pub fn insert_block(&self, block: B) -> bool {
        let block_hash = block.block_hash();
        if self.is_known(&block_hash) {
            // TODO: known header but not known block.
            return false;
        }

        let mut cumulative_weight = 0;

        let maybe_parent = self.get_block_index(&BlockId::Hash(block.header().parent_hash()));
        let mut result = true;
        if let Some(parent_details) = maybe_parent {
            if parent_details.cumulative_weight > 0 {
                cumulative_weight = block.weight() + parent_details.cumulative_weight;
            }
            result = false
        }
        let block_index = BlockIndex { block, cumulative_weight };
        self.insert_block_index(&block_index);
        if block_index.cumulative_weight > self.best_block_index.read().cumulative_weight {
            self.update_best_block(block_index);
        } else {
            info!(target: "chain",
                  "Block #{:?} {:?} was declined via fork choice rule (best block weight = {} vs new block weight = {}).",
                  block_index.block.header().index(), block_hash,
                  self.best_block_index.read().cumulative_weight, block_index.cumulative_weight);
        }

        result
    }

    fn insert_block_index(&self, block_index: &BlockIndex<B>) {
        let block = &block_index.block;
        let block_hash = block.block_hash();

        // Store block in db.
        write_with_cache(
            &self.storage,
            storage::COL_BLOCKS,
            &self.blocks,
            block_hash.as_ref(),
            &block_index,
        );
        write_with_cache(
            &self.storage,
            storage::COL_HEADERS,
            &self.headers,
            block_hash.as_ref(),
            &block.header(),
        );
        write_with_cache(
            &self.storage,
            storage::COL_BLOCK_INDEX,
            &self.index_to_hash,
            &index_to_bytes(block.header().index()),
            &block_hash,
        );
    }

    fn get_block_hash_by_index(&self, index: u64) -> Option<CryptoHash> {
        read_with_cache(
            &self.storage,
            storage::COL_BLOCK_INDEX,
            &self.index_to_hash,
            &index_to_bytes(index),
        )
    }

    fn get_block_index_by_hash(&self, block_hash: &CryptoHash) -> Option<BlockIndex<B>> {
        read_with_cache(&self.storage, storage::COL_BLOCKS, &self.blocks, block_hash.as_ref())
    }

    pub fn get_block_index(&self, id: &BlockId) -> Option<BlockIndex<B>> {
        match id {
            BlockId::Number(num) => self.get_block_index_by_hash(&self.get_block_hash_by_index(*num)?),
            BlockId::Hash(hash) => self.get_block_index_by_hash(hash),
        }
    }

    pub fn get_block(&self, id: &BlockId) -> Option<B> {
        let block_index = self.get_block_index(id)?;
        Some(block_index.block)
    }

    fn get_block_header_by_hash(&self, block_hash: &CryptoHash) -> Option<B::SignedHeader> {
        read_with_cache(&self.storage, storage::COL_HEADERS, &self.headers, block_hash.as_ref())
    }

    pub fn get_header(&self, id: &BlockId) -> Option<B::SignedHeader> {
        match id {
            BlockId::Number(num) => {
                self.get_block_header_by_hash(&self.get_block_hash_by_index(*num)?)
            }
            BlockId::Hash(hash) => self.get_block_header_by_hash(hash),
        }
    }

    pub fn get_blocks_by_index(&self, start: u64, limit: u64) -> Result<Vec<B>, String> {
        let mut blocks = vec![];
        for i in start..=(start + limit) {
            if let Some(block) = self.get_block(&BlockId::Number(i)) {
                blocks.push(block)
            }
        };
        Ok(blocks)
    }
}
