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

use chain::{SignedBlock, SignedHeader, SignedShardBlock, ReceiptBlock};
use configs::chain_spec::ChainSpec;
use node_runtime::{ApplyState, Runtime};
use node_runtime::state_viewer::StateDbViewer;
use primitives::hash::CryptoHash;
use primitives::types::{AuthorityStake, BlockId, ShardId, BlockIndex};
use primitives::merkle::{merklize, MerklePath};
use storage::{extend_with_cache, read_with_cache, StateDb};
use transaction::{
    FinalTransactionResult, FinalTransactionStatus, SignedTransaction,
    TransactionLogs, TransactionResult, TransactionStatus,
    ReceiptTransaction
};

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

type ShardBlockExtraInfo = (
    storage::DBChanges,
    Vec<AuthorityStake>,
    Vec<TransactionResult>,
    HashMap<ShardId, ReceiptBlock>
);

pub struct ShardBlockChain {
    pub chain: chain::BlockChain<SignedShardBlock>,
    storage: Arc<storage::Storage>,
    transaction_addresses: RwLock<HashMap<Vec<u8>, TransactionAddress>>,
    transaction_results: RwLock<HashMap<Vec<u8>, TransactionResult>>,
    pub receipts: RwLock<HashMap<BlockIndex, HashMap<ShardId, ReceiptBlock>>>,
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
            receipts: RwLock::new(HashMap::new()),
            state_db,
            runtime,
            statedb_viewer
        }
    }

    #[inline]
    pub fn genesis_hash(&self) -> CryptoHash {
        self.chain.genesis_hash
    }

    pub fn insert_block(
        &self,
        block: &SignedShardBlock,
        db_transaction: storage::DBChanges,
        tx_result: Vec<TransactionResult>,
        new_receipts: HashMap<ShardId, ReceiptBlock>
    ) {
        self.state_db.commit(db_transaction).ok();
        self.chain.insert_block(block.clone());
        self.update_for_inserted_block(&block.clone(), tx_result);
        let index = block.index();
        self.receipts.write().insert(index, new_receipts);
    }

    fn compute_receipt_blocks(
        shard_ids: Vec<ShardId>,
        receipts: Vec<Vec<ReceiptTransaction>>,
        receipt_merkle_paths: Vec<MerklePath>,
        block: &SignedShardBlock
    ) -> HashMap<ShardId, ReceiptBlock> {
        shard_ids
            .into_iter()
            .zip(
                receipts.into_iter()
                .zip(receipt_merkle_paths.into_iter())
                .map(|(receipts, path)| ReceiptBlock {
                    header: block.header(),
                    receipts,
                    path
                })
            )
            .collect()
    }

    pub fn prepare_new_block(
        &self,
        last_block_hash: CryptoHash,
        prev_receipts: Vec<ReceiptBlock>,
        transactions: Vec<SignedTransaction>
    ) -> (SignedShardBlock, ShardBlockExtraInfo) {
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
            &prev_receipts,
            &transactions,
        );
        let (shard_ids, new_receipts): (Vec<_>, Vec<_>) = apply_result.new_receipts
                .into_iter()
                .unzip();
        let (receipt_merkle_root, receipt_merkle_paths) = merklize(&new_receipts);
        let shard_block = SignedShardBlock::new(
            last_block.body.header.shard_id,
            last_block.body.header.index + 1,
            last_block.block_hash(),
            apply_result.root,
            transactions,
            prev_receipts,
            receipt_merkle_root,
        );
        let receipt_map = Self::compute_receipt_blocks(
            shard_ids,
            new_receipts,
            receipt_merkle_paths,
            &shard_block
        );
        let shard_block_extra = (
            apply_result.db_changes,
            apply_result.authority_proposals,
            apply_result.tx_result,
            receipt_map,
        );
        (shard_block, shard_block_extra)
    }

    pub fn apply_block(&self, block: SignedShardBlock) -> bool {
        let state_merkle_root = block.body.header.merkle_root_state;
        let receipt_merkle_root = block.body.header.receipt_merkle_root;
        let (shard_block, (db_changes, _, tx_result, receipt_map)) = self.prepare_new_block(
            block.body.header.parent_hash,
            block.body.receipts,
            block.body.transactions
        );
        if shard_block.body.header.merkle_root_state == state_merkle_root
        && shard_block.body.header.receipt_merkle_root == receipt_merkle_root {
            self.insert_block(&shard_block, db_changes, tx_result, receipt_map);
            true
        } else {
            error!("Received Invalid block. It's a scam");
            false
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
        self.get_transaction_address(&hash).map(|address| {
            let block_id = BlockId::Hash(address.block_hash);
            let block = self.chain.get_block(&block_id)
                .expect("transaction address points to non-existent block");
            let transaction = block.body.transactions.get(address.index)
                .expect("transaction address points to invalid index inside block");
            let result = self.get_transaction_result(&hash);
            SignedTransactionInfo {
                transaction: transaction.clone(),
                block_index: block.header().index(),
                result,
            }
        })
    }

    pub fn update_for_inserted_block(&self, block: &SignedShardBlock, tx_result: Vec<TransactionResult>) {
        let updates: HashMap<Vec<u8>, TransactionAddress> = block.body.receipts.iter()
            .flat_map(|b| b.receipts.iter()
                .map(|r| with_index(&r.nonce, ExtrasIndex::TransactionAddress))
            )
            .chain(
                block.body.transactions.iter()
                .map(|t| with_index(&t.get_hash(), ExtrasIndex::TransactionAddress))
            )
            .enumerate()
            .map(|(i, key)| (key.to_vec(), TransactionAddress {
                    block_hash: block.hash,
                    index: i,
                }))
            .collect();
        extend_with_cache(
            &self.storage.clone(),
            storage::COL_EXTRA,
            &self.transaction_addresses,
            updates,
        );

        let updates: HashMap<Vec<u8>, TransactionResult> = block.body.receipts.iter()
            .flat_map(|b| b.receipts.iter()
                .map(|r| with_index(&r.nonce, ExtrasIndex::TransactionResult))
            )
            .chain(
                block.body.transactions.iter()
                .map(|t| with_index(&t.get_hash(), ExtrasIndex::TransactionResult))
            )
            .enumerate()
            .map(|(i, key)| (key.to_vec(), tx_result[i].clone()))
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

    pub fn get_receipt_block(&self, block_index: BlockIndex, shard_id: ShardId) -> Option<ReceiptBlock> {
        self.receipts
            .read()
            .get(&block_index)
            .and_then(|m| m.get(&shard_id))
            .cloned()
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
        let (block, (db_changes, _, tx_status, receipts)) = chain.prepare_new_block(
            chain.genesis_hash(), 
            vec![],
            vec![tx.clone()]
        );
        chain.insert_block(&block, db_changes, tx_status, receipts);

        let result = chain.get_transaction_result(&tx.get_hash());
        assert_eq!(result.status, TransactionStatus::Failed);
    }

    #[test]
    fn test_get_transaction_status_complete() {
        let chain = get_test_chain();
        let tx = send_money_tx("alice.near", "bob.near", 10);
        let (block, (db_changes, _, tx_status, new_receipts)) = chain.prepare_new_block(
            chain.genesis_hash(),
            vec![],
            vec![tx.clone()]
        );
        chain.insert_block(&block, db_changes, tx_status, new_receipts);

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

        let receipt_block = chain.get_receipt_block(block.index(), block.shard_id()).unwrap();
        let (block2, (db_changes2, _, tx_status2, receipts)) = chain.prepare_new_block(
            block.hash, vec![receipt_block], vec![]
        );
        chain.insert_block(&block2, db_changes2, tx_status2, receipts);

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
        let transaction = SignedTransaction::empty();
        let block = SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![],
            CryptoHash::default(),
        );
        let db_changes = HashMap::default();
        chain.insert_block(&block, db_changes, vec![TransactionResult::default()], HashMap::new());
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
