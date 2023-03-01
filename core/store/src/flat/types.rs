use std::collections::HashSet;

use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;

pub type BlockHash = CryptoHash;

pub struct BlockInfo {
    pub hash: BlockHash,
    pub height: BlockHeight,
    pub prev_hash: BlockHash,
}

#[derive(Debug)]
pub enum FlatStorageError {
    /// This means we can't find a path from `flat_head` to the block.
    /// Includes `flat_head` hash and block hash respectively.
    BlockNotSupported((CryptoHash, CryptoHash)),
    StorageInternalError,
}

pub trait ChainAccessForFlatStorage {
    fn get_block_info(&self, block_hash: &CryptoHash) -> BlockInfo;
    fn get_block_hashes_at_height(&self, block_height: BlockHeight) -> HashSet<CryptoHash>;
}