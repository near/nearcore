#[macro_use]
extern crate log;
extern crate parking_lot;
extern crate primitives;
extern crate rand;
#[macro_use]
extern crate serde_derive;
extern crate storage;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use configs::chain_spec::ChainSpec;
use mempool::Pool;
use node_runtime::{ApplyState, Runtime};
use node_runtime::state_viewer::TrieViewer;
use primitives::block_traits::{SignedBlock, SignedHeader};
use primitives::chain::{ReceiptBlock, SignedShardBlock, SignedShardBlockHeader};
use primitives::hash::CryptoHash;
use primitives::merkle::{MerklePath, merklize};
use primitives::signer::BlockSigner;
use primitives::transaction::{
    FinalTransactionResult, FinalTransactionStatus, ReceiptTransaction, SignedTransaction,
    TransactionAddress, TransactionLogs, TransactionResult, TransactionStatus
};
use primitives::types::{AuthorityStake, BlockId, BlockIndex, MerkleHash, ShardId};
use storage::{Trie, TrieUpdate};
use storage::ShardChainStorage;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

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
    HashMap<ShardId, ReceiptBlock>,
);

pub fn get_all_receipts<'a, I>(receipt_blocks_iter: I) -> Vec<&'a ReceiptTransaction>
where
    I: Iterator<Item = &'a ReceiptBlock>
{
    receipt_blocks_iter.flat_map(|rb| &rb.receipts).collect()
}

#[allow(unused)]
pub struct ShardClient {
    pub chain: Arc<chain::BlockChain<SignedShardBlockHeader, SignedShardBlock, ShardChainStorage>>,
    pub trie: Arc<Trie>,
    storage: Arc<RwLock<ShardChainStorage>>,
    pub runtime: Runtime,
    pub trie_viewer: TrieViewer,
    pub pool: Arc<Pool>,
}

impl ShardClient {
    pub fn new(signer: Arc<BlockSigner>, chain_spec: &ChainSpec, storage: Arc<RwLock<ShardChainStorage>>) -> Self {
        let trie = Arc::new(Trie::new(storage.clone()));
        let runtime = Runtime {};
        let state_update = TrieUpdate::new(trie.clone(), MerkleHash::default());
        let (genesis_root, db_changes) = runtime.apply_genesis_state(
            state_update,
            &chain_spec.accounts,
            &chain_spec.genesis_wasm,
            &chain_spec.initial_authorities,
        );
        trie.apply_changes(db_changes).expect("Failed to commit genesis state");
        let genesis = SignedShardBlock::genesis(genesis_root);

        let chain = Arc::new(chain::BlockChain::new(genesis, storage.clone()));
        let trie_viewer = TrieViewer {};
        let pool = Arc::new(Pool::new(signer, storage.clone(), trie.clone()));
        Self { 
            chain,
            trie,
            storage,
            runtime,
            trie_viewer,
            pool
        }
    }

    pub fn get_state_update(&self) -> TrieUpdate {
        let root = self.chain.best_block().merkle_root_state();
        TrieUpdate::new(self.trie.clone(), root)
    }

    #[inline]
    pub fn genesis_hash(&self) -> CryptoHash {
        self.chain.genesis_hash()
    }

    pub fn insert_block(
        &self,
        block: &SignedShardBlock,
        db_transaction: storage::DBChanges,
        tx_result: Vec<TransactionResult>,
        new_receipts: HashMap<ShardId, ReceiptBlock>,
    ) {
        self.trie.apply_changes(db_transaction).ok();
        self.chain.insert_block(block.clone());
        self.pool.import_block(&block);
        self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .extend_transaction_results_addresses(block, tx_result)
            .unwrap();
        
        let index = block.index();
        self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .extend_receipts(index, new_receipts)
            .unwrap();
    }

    fn compute_receipt_blocks(
        shard_ids: Vec<ShardId>,
        receipts: Vec<Vec<ReceiptTransaction>>,
        receipt_merkle_paths: Vec<MerklePath>,
        block: &SignedShardBlock,
    ) -> HashMap<ShardId, ReceiptBlock> {
        shard_ids
            .into_iter()
            .zip(
                receipts.into_iter().zip(receipt_merkle_paths.into_iter()).map(
                    |(receipts, path)| ReceiptBlock { header: block.header(), receipts, path },
                ),
            )
            .collect()
    }

    pub fn prepare_new_block(
        &self,
        last_block_hash: CryptoHash,
        prev_receipts: Vec<ReceiptBlock>,
        transactions: Vec<SignedTransaction>,
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
        let state_update = TrieUpdate::new(self.trie.clone(), apply_state.root);
        let apply_result =
            self.runtime.apply(state_update, &apply_state, &prev_receipts, &transactions);
        let (shard_ids, new_receipts): (Vec<_>, Vec<_>) =
            apply_result.new_receipts.into_iter().unzip();
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
            &shard_block,
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
            block.body.transactions,
        );
        if shard_block.body.header.merkle_root_state == state_merkle_root
            && shard_block.body.header.receipt_merkle_root == receipt_merkle_root
        {
            self.insert_block(&shard_block, db_changes, tx_result, receipt_map);
            true
        } else {
            error!("Received Invalid block. It's a scam");
            false
        }
    }

    pub fn get_transaction_result(&self, hash: &CryptoHash) -> TransactionResult {
        self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .transaction_result(hash)
            .unwrap()
            .cloned()
            .unwrap_or_default()
    }

    pub fn get_transaction_address(&self, hash: &CryptoHash) -> Option<TransactionAddress> {
        self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .transaction_address(hash)
            .unwrap()
            .cloned()
    }

    pub fn get_transaction_info(&self, hash: &CryptoHash) -> Option<SignedTransactionInfo> {
        self.get_transaction_address(hash).map(|address| {
            let block_id = BlockId::Hash(address.block_hash);
            let block = self
                .chain
                .get_block(&block_id)
                .expect("transaction address points to non-existent block");
            let transaction = block
                .body
                .transactions
                .get(address.index)
                .expect("transaction address points to invalid index inside block");
            let result = self.get_transaction_result(&hash);
            SignedTransactionInfo {
                transaction: transaction.clone(),
                block_index: block.header().index(),
                result,
            }
        })
    }

    fn collect_transaction_final_result(
        &self,
        transaction_result: &TransactionResult,
        logs: &mut Vec<TransactionLogs>,
    ) -> FinalTransactionStatus {
        match transaction_result.status {
            TransactionStatus::Unknown => FinalTransactionStatus::Unknown,
            TransactionStatus::Failed => FinalTransactionStatus::Failed,
            TransactionStatus::Completed => {
                for r in transaction_result.receipts.iter() {
                    let receipt_result = self.get_transaction_result(&r);
                    logs.push(TransactionLogs {
                        hash: *r,
                        lines: receipt_result.logs.clone(),
                        receipts: receipt_result.receipts.clone(),
                    });
                    match self.collect_transaction_final_result(&receipt_result, logs) {
                        FinalTransactionStatus::Failed => return FinalTransactionStatus::Failed,
                        FinalTransactionStatus::Completed => {}
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
            logs: vec![TransactionLogs {
                hash: *hash,
                lines: transaction_result.logs.clone(),
                receipts: transaction_result.receipts.clone(),
            }],
        };
        result.status =
            self.collect_transaction_final_result(&transaction_result, &mut result.logs);
        result
    }

    pub fn get_receipt_block(
        &self,
        block_index: BlockIndex,
        shard_id: ShardId,
    ) -> Option<ReceiptBlock> {
        self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .receipt_block(block_index, shard_id)
            .unwrap()
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use node_runtime::test_utils::generate_test_chain_spec;
    use primitives::signer::InMemorySigner;
    use primitives::transaction::{
        FinalTransactionStatus, SignedTransaction, TransactionAddress,
        TransactionBody, TransactionStatus
    };
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;

    fn get_test_client() -> (ShardClient, Arc<InMemorySigner>) {
        let (chain_spec, signer) = generate_test_chain_spec();
        let shard_storage = create_beacon_shard_storages().1;
        let shard_client = ShardClient::new(signer.clone(), &chain_spec, shard_storage);
        (shard_client, signer)
    }

    #[test]
    fn test_get_transaction_status_unknown() {
        let (client, _) = get_test_client();
        let result = client.get_transaction_result(&CryptoHash::default());
        assert_eq!(result.status, TransactionStatus::Unknown);
    }

    #[test]
    fn test_transaction_failed() {
        let (client, signer) = get_test_client();
        let tx = TransactionBody::send_money(1, "xyz.near", "bob.near", 100).sign(signer);
        let (block, (db_changes, _, tx_status, receipts)) =
            client.prepare_new_block(client.genesis_hash(), vec![], vec![tx.clone()]);
        client.insert_block(&block, db_changes, tx_status, receipts);

        let result = client.get_transaction_result(&tx.get_hash());
        assert_eq!(result.status, TransactionStatus::Failed);
    }

    #[test]
    fn test_get_transaction_status_complete() {
        let (client, signer) = get_test_client();
        let tx = TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(signer);
        let (block, (db_changes, _, tx_status, new_receipts)) =
            client.prepare_new_block(client.genesis_hash(), vec![], vec![tx.clone()]);
        client.insert_block(&block, db_changes, tx_status, new_receipts);

        let result = client.get_transaction_result(&tx.get_hash());
        assert_eq!(result.status, TransactionStatus::Completed);
        assert_eq!(result.receipts.len(), 1);
        assert_ne!(result.receipts[0], tx.get_hash());
        let final_result = client.get_transaction_final_result(&tx.get_hash());
        assert_eq!(final_result.status, FinalTransactionStatus::Started);
        assert_eq!(final_result.logs.len(), 2);
        assert_eq!(final_result.logs[0].hash, tx.get_hash());
        assert_eq!(final_result.logs[0].lines.len(), 0);
        assert_eq!(final_result.logs[0].receipts.len(), 1);

        let receipt_block = client.get_receipt_block(block.index(), block.shard_id()).unwrap();
        let (block2, (db_changes2, _, tx_status2, receipts)) =
            client.prepare_new_block(block.hash, vec![receipt_block], vec![]);
        client.insert_block(&block2, db_changes2, tx_status2, receipts);

        let result2 = client.get_transaction_result(&result.receipts[0]);
        assert_eq!(result2.status, TransactionStatus::Completed);
        let final_result2 = client.get_transaction_final_result(&tx.get_hash());
        assert_eq!(final_result2.status, FinalTransactionStatus::Completed);
        assert_eq!(final_result2.logs.len(), 2);
        assert_eq!(final_result2.logs[0].hash, tx.get_hash());
        assert_eq!(final_result2.logs[1].hash, result.receipts[0]);
        assert_eq!(final_result2.logs[1].receipts.len(), 0);
    }

    #[test]
    fn test_get_transaction_address() {
        let (client, _) = get_test_client();
        let transaction = SignedTransaction::empty();
        let hash = transaction.get_hash();
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
        client.insert_block(&block, db_changes, vec![TransactionResult::default()], HashMap::new());
        let address = client.get_transaction_address(&hash);
        let expected = TransactionAddress { block_hash: block.hash, index: 0 };
        assert_eq!(address, Some(expected.clone()));
    }

    // TODO(472): Add extensive testing for ShardBlockChain.
}
