use crate::db::DBCol::ColBlockHeader;
use crate::Store;
use log::error;
use near_primitives::block::BlockHeader;
use near_primitives::hash::CryptoHash;
use near_primitives::types::BlockHeight;
use std::io;
use std::sync::Arc;

pub struct ForksManager {
    pub(crate) store: Arc<Store>,
}

impl ForksManager {
    pub fn get_block_height(&self, block_hash: CryptoHash) -> Option<BlockHeight> {
        let block_header = self.store.get_ser::<BlockHeader>(ColBlockHeader, &(block_hash.0).0[..]);
        match block_header {
            Ok(Some(header)) => Some(header.inner_lite.height),
            Ok(None) => {
                error!(target: "client", "ForksManager: orphan block?! {:?}", block_hash);
                None
            }
            Err(e) => {
                error!(target: "client", "ForksManager: error getting block parent {:?}", e);
                panic!("error getting block parent");
            }
        }
    }

    pub fn get_block_parent(&self, block_hash: CryptoHash) -> Option<CryptoHash> {
        let block_header = self.store.get_ser::<BlockHeader>(ColBlockHeader, &(block_hash.0).0[..]);
        match block_header {
            Ok(Some(header)) => Some(header.prev_hash),
            Ok(None) => {
                error!(target: "client", "ForksManager: orphan block?! {:?}", block_hash);
                None
            }
            Err(e) => {
                error!(target: "client", "ForksManager: error getting block parent {:?}", e);
                panic!("error getting block parent");
            }
        }
    }

    /// Checks if two blocks are on the same chain and one is preceded by another.
    pub fn is_same_chain(
        &self,
        height1: BlockHeight,
        hash1: CryptoHash,
        mut height2: BlockHeight,
        mut hash2: CryptoHash,
    ) -> bool {
        if height1 > height2 {
            return self.is_same_chain(height2, hash2, height1, hash1);
        }
        if height1 == 0 {
            return true;
        }
        while height1 < height2 {
            hash2 = match self.get_block_parent(hash2) {
                Some(val) => val,
                None => {
                    debug_assert!(false, "Found an orphan ?! {:?} {:?}", height2, hash2);
                    return false;
                }
            };
            height2 = self.get_block_height(hash2).expect("Orphan ?!");
        }
        height1 == height2 && hash1 == hash2
    }
}
