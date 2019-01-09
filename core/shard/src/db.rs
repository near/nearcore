use std::sync::Arc;

use primitives::hash::CryptoHash;
use primitives::traits::Decode;
use SignedShardBlock;
use storage;

type H264 = [u8; 33];

pub struct ShardChainDb {
    db: Arc<storage::Storage>,
}

/// Represents index of extra data in database
#[derive(Copy, Debug, Hash, Eq, PartialEq, Clone)]
pub enum ExtrasIndex {
    /// Transaction address index
    TransactionAddress = 0,
}

fn with_index(hash: &CryptoHash, i: ExtrasIndex) -> H264 {
    let mut result = [0; 33];
    result[0] = i as u8;
    result[1..].clone_from_slice(hash.as_ref());
    result
}

/// Represents address of certain transaction within block
#[derive(Debug, PartialEq, Clone, Deserialize)]
pub struct TransactionAddress {
    /// Block hash
    pub block_hash: CryptoHash,
    /// Transaction index within the block
    pub index: usize
}

impl ShardChainDb {
    pub fn new(db: Arc<storage::Storage>) -> Self {
        Self {
            db,
        }
    }

    /// Get the address of transaction with given hash.
    pub fn get_transaction_address(&self, hash: &CryptoHash) -> Option<TransactionAddress> {
        let index = with_index(&hash, ExtrasIndex::TransactionAddress);
        self.db.get(storage::COL_EXTRA, &index).expect("fuck").map(|v| {
            Decode::decode(&v.into_vec()).unwrap()
        })
    }

    pub fn update_for_inserted_block(&self, _block: &SignedShardBlock) {
        println!("here");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::test_utils::create_memory_db;

    #[test]
    fn test_get_transaction_address() {
        let db = ShardChainDb {
            db: Arc::new(create_memory_db()),
        };
        let hash = CryptoHash::default();
        let address = db.get_transaction_address(&hash);
        assert_eq!(None, address);
    }
}
