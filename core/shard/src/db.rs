use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use primitives::hash::CryptoHash;
use primitives::types::Transaction;
use SignedShardBlock;
use storage::{self, extend_with_cache};
use storage::read_with_cache;

type H264 = [u8; 33];

pub struct ShardChainDb {
    storage: Arc<storage::Storage>,
    transaction_addresses: RwLock<HashMap<Vec<u8>, TransactionAddress>>,
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
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct TransactionAddress {
    /// Block hash
    pub block_hash: CryptoHash,
    /// Transaction index within the block
    pub index: usize
}

impl ShardChainDb {
    pub fn new(storage: Arc<storage::Storage>) -> Self {
        Self {
            storage,
            transaction_addresses: RwLock::new(HashMap::new()),
        }
    }

    /// Get the address of transaction with given hash.
    pub fn get_transaction_address(&self, hash: &CryptoHash) -> Option<TransactionAddress> {
        let key = with_index(&hash, ExtrasIndex::TransactionAddress);
        read_with_cache(
            &self.storage.clone(),
            storage::COL_EXTRA,
            &self.transaction_addresses,
            &key,
        )
    }

    pub fn update_for_inserted_block(&self, block: &SignedShardBlock) {
        let updates: HashMap<Vec<u8>, TransactionAddress> = block.body.transactions.iter()
            .enumerate()
            .filter_map(|(i, transaction)| {
                match transaction {
                    Transaction::SignedTransaction(t) => {
                        let key = with_index(
                            &t.transaction_hash(),
                            ExtrasIndex::TransactionAddress,
                        );
                        Some((key.to_vec(), TransactionAddress {
                            block_hash: block.hash,
                            index: i,
                        }))
                    },
                    Transaction::Receipt(_) => None,
                }
            })
            .collect();
        extend_with_cache(
            &self.storage.clone(),
            storage::COL_EXTRA,
            &self.transaction_addresses,
            updates,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use storage::test_utils::create_memory_db;
    use primitives::types::SignedTransaction;

    #[test]
    fn test_get_transaction_address() {
        let db = ShardChainDb::new(Arc::new(create_memory_db()));
        let t = SignedTransaction::empty();
        let transaction = Transaction::SignedTransaction(SignedTransaction::empty());
        let block = SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![],
        );
        db.update_for_inserted_block(&block);
        let address = db.get_transaction_address(&t.transaction_hash());
        let expected = TransactionAddress {
            block_hash: block.hash,
            index: 0
        };
        assert_eq!(address, Some(expected.clone()));

        let cache_key = with_index(
            &t.transaction_hash(),
            ExtrasIndex::TransactionAddress,
        );
        let read = db.transaction_addresses.read();
        let v = read.get(&cache_key.to_vec());
        assert_eq!(v.unwrap(), &expected.clone());
    }
}
