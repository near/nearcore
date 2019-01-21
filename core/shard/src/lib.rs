extern crate chain;
extern crate parking_lot;
extern crate primitives;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate storage;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use chain::{SignedBlock, SignedHeader};
use primitives::hash::CryptoHash;
use primitives::types::BlockId;
use storage::{extend_with_cache, read_with_cache};
use transaction::{SignedTransaction, Transaction};

pub use crate::types::{ShardBlock, ShardBlockHeader, SignedShardBlock};

pub mod types;

type H264 = [u8; 33];

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

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum TransactionStatus {
    Unknown,
    Started,
    Completed,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SignedTransactionInfo {
    pub transaction: SignedTransaction,
    pub block_index: u64,
    pub status: TransactionStatus,
}

pub struct ShardBlockChain {
    pub chain: chain::BlockChain<SignedShardBlock>,
    storage: Arc<storage::Storage>,
    transaction_addresses: RwLock<HashMap<Vec<u8>, TransactionAddress>>,
}

impl ShardBlockChain {
    pub fn new(genesis: SignedShardBlock, storage: Arc<storage::Storage>) -> Self {
        let chain = chain::BlockChain::<SignedShardBlock>::new(genesis, storage.clone());
        Self {
            chain,
            storage,
            transaction_addresses: RwLock::new(HashMap::new()),
        }
    }

    pub fn insert_block(&self, block: &SignedShardBlock) {
        self.chain.insert_block(block.clone());
        self.update_for_inserted_block(&block.clone());
    }

    fn is_transaction_complete(
        &self,
        hash: &CryptoHash,
        last_block: &SignedShardBlock,
    ) -> TransactionStatus {
        for receipt in last_block.body.new_receipts.iter() {
            match receipt {
                Transaction::Receipt(r) => {
                    if r.nonce == hash.as_ref() {
                        let next_index = last_block.body.header.index + 1;
                        match self.chain.get_block(&BlockId::Number(next_index)) {
                            Some(b) => return self.is_transaction_complete(hash, &b),
                            None => return TransactionStatus::Started,
                        };
                    }
                }
                _ => panic!("non receipt in SignedShardBlock.new_receipts")
            }
        };
        TransactionStatus::Completed
    }

    fn get_transaction_status_from_address(
        &self,
        hash: &CryptoHash,
        address: &TransactionAddress,
    ) -> TransactionStatus {
        let block = self.chain.get_block(&BlockId::Hash(address.block_hash))
            .expect("transaction address points to non-existent block");
        self.is_transaction_complete(&hash, &block)
    }

    fn get_transaction_address(&self, hash: &CryptoHash) -> Option<TransactionAddress> {
        let key = with_index(&hash, ExtrasIndex::TransactionAddress);
        read_with_cache(
            &self.storage.clone(),
            storage::COL_EXTRA,
            &self.transaction_addresses,
            &key,
        )
    }

    /// Get the address of transaction with given hash.
    pub fn get_transaction_status(&self, hash: &CryptoHash) -> TransactionStatus {
        match self.get_transaction_address(&hash) {
            Some(a) => self.get_transaction_status_from_address(hash, &a),
            None => TransactionStatus::Unknown,
        }
    }

    pub fn get_transaction_info(
        &self,
        hash: &CryptoHash,
    ) -> Option<SignedTransactionInfo> {
        match self.get_transaction_address(&hash) {
            Some(address) => {
                let block_id = BlockId::Hash(address.block_hash);
                let block = self.chain.get_block(&block_id)
                    .expect("transaction address points to non-existent block");
                let transaction = block.body.transactions.get(address.index)
                    .expect("transaction address points to invalid index inside block");
                match transaction {
                    Transaction::SignedTransaction(transaction) => {
                        let status = self.is_transaction_complete(&hash, &block);
                        Some(SignedTransactionInfo {
                            transaction: transaction.clone(),
                            block_index: block.header().index(),
                            status,
                        })
                    }
                    Transaction::Receipt(_) => {
                        unreachable!("receipts should not have transaction addresses")
                    }
                }
            },
            None => None,
        }
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
                    }
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
    use primitives::types::AccountId;
    use storage::test_utils::create_memory_db;
    use transaction::{ReceiptTransaction, SignedTransaction, ReceiptBody};

    use super::*;

    fn get_chain() -> ShardBlockChain {
        let genesis = SignedShardBlock::genesis(CryptoHash::default());
        ShardBlockChain::new(genesis, Arc::new(create_memory_db()))
    }

    #[test]
    fn test_get_transaction_status_unknown() {
        let chain = get_chain();
        let status = chain.get_transaction_status(&CryptoHash::default());
        assert_eq!(status, TransactionStatus::Unknown);
    }

    #[test]
    fn test_get_transaction_status_complete_no_receipts() {
        let chain = get_chain();
        let t = SignedTransaction::empty();
        let transaction = Transaction::SignedTransaction(SignedTransaction::empty());
        let block = SignedShardBlock::new(
            0,
            1,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![],
        );
        chain.insert_block(&block);

        let status = chain.get_transaction_status(&t.transaction_hash());
        assert_eq!(status, TransactionStatus::Completed);
    }

    #[test]
    fn test_get_transaction_status_complete_with_receipts() {
        let chain = get_chain();
        let t = SignedTransaction::empty();
        let transaction = Transaction::SignedTransaction(SignedTransaction::empty());
        let receipt0 = Transaction::Receipt(ReceiptTransaction::new(
            AccountId::default(),
            AccountId::default(),
            t.transaction_hash().into(),
            ReceiptBody::Refund(0),
        ));
        let block1 = SignedShardBlock::new(
            0,
            1,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![receipt0],
        );
        chain.insert_block(&block1);

        let status = chain.get_transaction_status(&t.transaction_hash());
        assert_eq!(status, TransactionStatus::Started);

        let receipt1 = Transaction::Receipt(ReceiptTransaction::new(
            AccountId::default(),
            AccountId::default(),
            t.transaction_hash().into(),
            ReceiptBody::Refund(0),
        ));
        let block2 = SignedShardBlock::new(
            0,
            2,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
            vec![receipt1],
        );
        chain.insert_block(&block2);

        let status = chain.get_transaction_status(&t.transaction_hash());
        assert_eq!(status, TransactionStatus::Started);

        let block3 = SignedShardBlock::new(
            0,
            3,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![],
            vec![],
        );
        chain.insert_block(&block3);

        let status = chain.get_transaction_status(&t.transaction_hash());
        assert_eq!(status, TransactionStatus::Completed);
    }

    #[test]
    fn test_get_transaction_address() {
        let chain = get_chain();
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
        chain.insert_block(&block);
        let address = chain.get_transaction_address(&t.transaction_hash());
        let expected = TransactionAddress {
            block_hash: block.hash,
            index: 0,
        };
        assert_eq!(address, Some(expected.clone()));

        let cache_key = with_index(
            &t.transaction_hash(),
            ExtrasIndex::TransactionAddress,
        );
        let read = chain.transaction_addresses.read();
        let v = read.get(&cache_key.to_vec());
        assert_eq!(v.unwrap(), &expected.clone());
    }
}
