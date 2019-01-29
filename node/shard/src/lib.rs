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
use primitives::types::{AuthorityStake, BlockId};
use storage::{extend_with_cache, read_with_cache, StateDb};
use transaction::{FinalTransactionResult, FinalTransactionStatus, SignedTransaction,
                  Transaction, TransactionLogs, TransactionResult, TransactionStatus};

pub use crate::types::{ShardBlock, ShardBlockHeader, SignedShardBlock};

pub mod types;

type H264 = [u8; 33];

/// Represents index of extra data in database
#[derive(Copy, Debug, Hash, Eq, PartialEq, Clone)]
pub enum ExtrasIndex {
    /// Transaction address index
    TransactionAddress = 0,
    /// Transaction result index
    TransactionResult = 1,
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

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SignedTransactionInfo {
    pub transaction: SignedTransaction,
    pub block_index: u64,
    pub result: TransactionResult,
}

pub struct ShardBlockChain {
    pub chain: chain::BlockChain<SignedShardBlock>,
    storage: Arc<storage::Storage>,
    transaction_addresses: RwLock<HashMap<Vec<u8>, TransactionAddress>>,
    transaction_results: RwLock<HashMap<Vec<u8>, TransactionResult>>,
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
            transaction_results: RwLock::new(HashMap::new()),
            state_db,
            runtime,
            statedb_viewer
        }
    }

    #[inline]
    pub fn genesis_hash(&self) -> CryptoHash {
        self.chain.genesis_hash
    }

    pub fn insert_block(&self, block: &SignedShardBlock, db_transaction: storage::DBChanges, tx_result: Vec<TransactionResult>) {
        self.state_db.commit(db_transaction).ok();
        self.chain.insert_block(block.clone());
        self.update_for_inserted_block(&block.clone(), tx_result);
    }

    pub fn prepare_new_block(&self, last_block_hash: CryptoHash, transactions: Vec<Transaction>)
        -> (SignedShardBlock, storage::DBChanges, Vec<AuthorityStake>, Vec<TransactionResult>) {
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
            &transactions,
        );
        let shard_block = SignedShardBlock::new(
            last_block.body.header.shard_id,
            last_block.body.header.index + 1,
            last_block.block_hash(),
            apply_result.root,
            transactions,
            apply_result.new_receipts,
        );
        (shard_block, apply_result.db_changes, apply_result.authority_proposals, apply_result.tx_result)
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
            Some((db_transaction, root, tx_result)) => {
                if root != block.header().body.merkle_root_state {
                    info!(
                        "Merkle root {} is not equal to received {} after applying the transactions from {:?}",
                        block.header().body.merkle_root_state,
                        root,
                        block
                    );
                    false
                } else {
                    self.insert_block(&block, db_transaction, tx_result);
                    true
                }
            }
            None => {
                info!("Found incorrect transaction in block {:?}", block);
                false
            }
        }
    }

    pub fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        let key = with_index(&hash, ExtrasIndex::TransactionResult);
        match read_with_cache(
            &self.storage.clone(),
            storage::COL_EXTRA,
            &self.transaction_results,
            &key,
        ) {
            Some(result) => result,
            None => TransactionResult::default()
        }
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
                        let result = self.get_transaction_result(&hash);
                        Some(SignedTransactionInfo {
                            transaction: transaction.clone(),
                            block_index: block.header().index(),
                            result,
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

    pub fn update_for_inserted_block(&self, block: &SignedShardBlock, tx_result: Vec<TransactionResult>) {
        let updates: HashMap<Vec<u8>, TransactionAddress> = block.body.transactions.iter()
            .enumerate()
            .map(|(i, transaction)| {
                let key = match transaction {
                    Transaction::SignedTransaction(t) => with_index(
                            &t.get_hash(),
                            ExtrasIndex::TransactionAddress,
                    ),
                    Transaction::Receipt(r) => with_index(
                            &r.nonce,
                            ExtrasIndex::TransactionAddress,
                    ),
                };
                (key.to_vec(), TransactionAddress {
                    block_hash: block.hash,
                    index: i,
                })
            })
            .collect();
        extend_with_cache(
            &self.storage.clone(),
            storage::COL_EXTRA,
            &self.transaction_addresses,
            updates,
        );
        let updates: HashMap<Vec<u8>, TransactionResult> = block.body.transactions.iter()
            .enumerate()
            .map(|(i, transaction)| {
                let key = match transaction {
                    Transaction::SignedTransaction(t) => with_index(
                        &t.get_hash(),
                        ExtrasIndex::TransactionResult,
                    ),
                    Transaction::Receipt(r) => with_index(
                        &r.nonce,
                        ExtrasIndex::TransactionResult
                    ),
                };
                (key.to_vec(), tx_result[i].clone())
            })
            .collect();
        extend_with_cache(
            &self.storage.clone(),
            storage::COL_EXTRA,
            &self.transaction_results,
            updates,
        );
    }

    fn collect_transaction_final_result(&self, transaction_result: &TransactionResult, logs: &mut Vec<TransactionLogs>) -> FinalTransactionStatus {
        match transaction_result.status {
            TransactionStatus::Unknown => FinalTransactionStatus::Unknown,
            TransactionStatus::Failed => FinalTransactionStatus::Failed,
            TransactionStatus::Completed => {
                for r in transaction_result.receipts.iter() {
                    let receipt_result = self.get_transaction_result(&r);
                    logs.push(TransactionLogs{ hash: *r, lines: receipt_result.logs.clone(), receipts: receipt_result.receipts.clone() });
                    match self.collect_transaction_final_result(&receipt_result, logs) {
                        FinalTransactionStatus::Failed => return FinalTransactionStatus::Failed,
                        FinalTransactionStatus::Completed => {},
                        _ => return FinalTransactionStatus::Started,
                    };
                }
                FinalTransactionStatus::Completed
            }
        }
    }

    pub fn get_transaction_final_result(&self, hash: &CryptoHash) -> FinalTransactionResult {
        let transaction_result = self.get_transaction_result(hash);
        let mut result = FinalTransactionResult {
            status: FinalTransactionStatus::Unknown,
            logs: vec![TransactionLogs{ hash: *hash, lines: transaction_result.logs.clone(), receipts: transaction_result.receipts.clone() }] };
        result.status = self.collect_transaction_final_result(&transaction_result, &mut result.logs);
        result
    }
}

#[cfg(test)]
mod tests {
    use node_runtime::test_utils::generate_test_chain_spec;
    use primitives::signature::DEFAULT_SIGNATURE;
    use primitives::types::Balance;
    use storage::test_utils::create_memory_db;
    use transaction::{SendMoneyTransaction, SignedTransaction, TransactionBody, TransactionStatus};

    use super::*;

    fn get_test_chain() -> ShardBlockChain {
        let (chain_spec, _signer) = generate_test_chain_spec();
        ShardBlockChain::new(&chain_spec, Arc::new(create_memory_db()))
    }

    fn send_money_tx(originator: &str, receiver: &str, amount: Balance) -> SignedTransaction {
        SignedTransaction::new(
            DEFAULT_SIGNATURE,
            TransactionBody::SendMoney(SendMoneyTransaction {
                nonce: 1, originator: originator.to_string(), receiver: receiver.to_string(), amount
            }), )
    }

    #[test]
    fn test_get_transaction_status_unknown() {
        let chain = get_test_chain();
        let result = chain.get_transaction_result(&CryptoHash::default());
        assert_eq!(result.status, TransactionStatus::Unknown);
    }

    #[test]
    fn test_transaction_failed() {
        let chain = get_test_chain();
        let tx = send_money_tx("xyz.near", "bob.near", 100);
        let (block, db_changes, _, tx_status) = chain.prepare_new_block(chain.genesis_hash(), vec![Transaction::SignedTransaction(tx.clone())]);
        chain.insert_block(&block, db_changes, tx_status);

        let result = chain.get_transaction_result(&tx.get_hash());
        assert_eq!(result.status, TransactionStatus::Failed);
    }

    #[test]
    fn test_get_transaction_status_complete() {
        let chain = get_test_chain();
        let tx = send_money_tx("alice.near", "bob.near", 10);
        let (block, db_changes, _, tx_status) = chain.prepare_new_block(chain.genesis_hash(), vec![Transaction::SignedTransaction(tx.clone())]);
        chain.insert_block(&block, db_changes, tx_status);

        let result = chain.get_transaction_result(&tx.get_hash());
        assert_eq!(result.status, TransactionStatus::Completed);
        assert_eq!(result.receipts.len(), 1);
        assert_ne!(result.receipts[0], tx.get_hash());
        let final_result = chain.get_transaction_final_result(&tx.get_hash());
        assert_eq!(final_result.status, FinalTransactionStatus::Started);
        assert_eq!(final_result.logs.len(), 2);
        assert_eq!(final_result.logs[0].hash, tx.get_hash());
        assert_eq!(final_result.logs[0].lines.len(), 0);
        assert_eq!(final_result.logs[0].receipts.len(), 1);

        let (block2, db_changes2, _, tx_status2) = chain.prepare_new_block(block.hash, block.body.new_receipts);
        chain.insert_block(&block2, db_changes2, tx_status2);

        let result2 = chain.get_transaction_result(&result.receipts[0]);
        assert_eq!(result2.status, TransactionStatus::Completed);
        let final_result2 = chain.get_transaction_final_result(&tx.get_hash());
        assert_eq!(final_result2.status, FinalTransactionStatus::Completed);
        assert_eq!(final_result2.logs.len(), 2);
        assert_eq!(final_result2.logs[0].hash, tx.get_hash());
        assert_eq!(final_result2.logs[1].hash, result.receipts[0]);
        assert_eq!(final_result2.logs[1].receipts.len(), 0);
    }

    #[test]
    fn test_get_transaction_address() {
        let chain = get_test_chain();
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
        chain.insert_block(&block, db_changes, vec![TransactionResult::default()]);
        let address = chain.get_transaction_address(&t.get_hash());
        let expected = TransactionAddress {
            block_hash: block.hash,
            index: 0,
        };
        assert_eq!(address, Some(expected.clone()));

        let cache_key = with_index(
            &t.get_hash(),
            ExtrasIndex::TransactionAddress,
        );
        let read = chain.transaction_addresses.read();
        let v = read.get(&cache_key.to_vec());
        assert_eq!(v.unwrap(), &expected.clone());
    }

    // TODO(472): Add extensive testing for ShardBlockChain.
}
