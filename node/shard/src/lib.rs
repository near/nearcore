#[macro_use]
extern crate log;
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
use configs::chain_spec::ChainSpec;
use node_runtime::{ApplyState, Runtime};
use node_runtime::state_viewer::StateDbViewer;
use primitives::hash::CryptoHash;
use primitives::types::{BlockId, AuthorityStake};
use storage::{extend_with_cache, read_with_cache, StateDb};
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
    pub state_db: Arc<StateDb>,
    pub runtime: RwLock<Runtime>,
    pub statedb_viewer: StateDbViewer,
}

impl ShardBlockChain {
    pub fn new(chain_spec: &ChainSpec, storage: Arc<storage::Storage>) -> Self {
        let state_db = Arc::new(StateDb::new(storage.clone()));
        let runtime = RwLock::new(Runtime::new(state_db.clone()));
        let genesis_root = runtime.write().apply_genesis_state(
            &chain_spec.accounts,
            &chain_spec.genesis_wasm,
            &chain_spec.initial_authorities,
        );
        let genesis = SignedShardBlock::genesis(genesis_root);

        let chain = chain::BlockChain::<SignedShardBlock>::new(genesis, storage.clone());
        let statedb_viewer = StateDbViewer::new(state_db.clone());
        Self {
            chain,
            storage,
            transaction_addresses: RwLock::new(HashMap::new()),
            state_db,
            runtime,
            statedb_viewer
        }
    }

    pub fn insert_block(&self, block: &SignedShardBlock, db_transaction: storage::DBChanges) {
        self.state_db.commit(db_transaction).ok();
        self.chain.insert_block(block.clone());
        self.update_for_inserted_block(&block.clone());
    }

    pub fn prepare_new_block(&self, last_block_hash: CryptoHash, transactions: Vec<Transaction>)
        -> (SignedShardBlock, storage::DBChanges, Vec<AuthorityStake>) {
        let last_block = self
            .chain
            .get_block(&BlockId::Hash(last_block_hash))
            .expect("At the moment we should have given shard block present");
        let apply_state = ApplyState {
            root: last_block.body.header.merkle_root_state,
            parent_block_hash: last_block_hash,
            block_index: last_block.body.header.index + 1,
            shard_id: last_block.body.header.shard_id,
        };
        let apply_result = self.runtime.write().apply(
            &apply_state,
            &[],
            transactions,
        );
        let shard_block = SignedShardBlock::new(
            last_block.body.header.shard_id,
            last_block.body.header.index + 1,
            last_block.block_hash(),
            apply_result.root,
            apply_result.filtered_transactions,
            apply_result.new_receipts,
        );
        (shard_block, apply_result.transaction, apply_result.authority_proposals)
    }

    pub fn apply_block(&self, block: &SignedShardBlock) -> bool {
        let parent_hash = block.body.header.parent_hash;
        let prev_block = self
            .chain
            .get_block(&BlockId::Hash(parent_hash))
            .expect("At this moment previous shard chain block should be present");
        let prev_header = prev_block.header();
        let apply_state = ApplyState {
            root: prev_header.body.merkle_root_state,
            block_index: prev_header.body.index + 1,
            parent_block_hash: parent_hash,
            shard_id: block.body.header.shard_id,
        };
        let apply_result = self.runtime.write().check(
            &apply_state,
            &[],
            &block.body.transactions,
        );
        match apply_result {
            Some((db_transaction, root)) => {
                if root != block.header().body.merkle_root_state {
                    info!(
                        "Merkle root {} is not equal to received {} after applying the transactions from {:?}",
                        block.header().body.merkle_root_state,
                        root,
                        block
                    );
                    false
                } else {
                    self.insert_block(&block, db_transaction);
                    true
                }
            }
            None => {
                info!("Found incorrect transaction in block {:?}", block);
                false
            }
        }
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
    use transaction::{ReceiptBody, ReceiptTransaction, SignedTransaction};

    use super::*;

    fn get_chain() -> ShardBlockChain {
        let chain_spec = ChainSpec {
            accounts: vec![], genesis_wasm: vec![], initial_authorities: vec![], beacon_chain_epoch_length: 1, beacon_chain_num_seats_per_slot: 1, boot_nodes: vec![]
        };
        ShardBlockChain::new(&chain_spec, Arc::new(create_memory_db()))
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
        chain.insert_block(&block, HashMap::default());

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
        let db_changes = HashMap::default();
        chain.insert_block(&block1, db_changes.clone());

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
        chain.insert_block(&block2, db_changes.clone());

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
        chain.insert_block(&block3, db_changes);

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
        let db_changes = HashMap::default();
        chain.insert_block(&block, db_changes);
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

    // TODO(472): Add extensive testing for ShardBlockChain.
}
