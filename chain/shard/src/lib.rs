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

use mempool::Pool;
use node_runtime::state_viewer::TrieViewer;
use node_runtime::chain_spec::ChainSpec;
use node_runtime::{ApplyState, Runtime};
use primitives::block_traits::{SignedBlock, SignedHeader};
use primitives::chain::{ReceiptBlock, SignedShardBlock, SignedShardBlockHeader};
use primitives::crypto::signer::EDSigner;
use primitives::hash::CryptoHash;
use primitives::merkle::{merklize, MerklePath};
use primitives::transaction::{
    FinalTransactionResult, FinalTransactionStatus, ReceiptTransaction, SignedTransaction,
    TransactionAddress, TransactionLogs, TransactionResult, TransactionStatus,
};
use primitives::types::{AccountId, AuthorityStake, BlockId, BlockIndex, MerkleHash, ShardId};
use storage::{ShardChainStorage, Trie, TrieUpdate};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct SignedTransactionInfo {
    pub transaction: SignedTransaction,
    pub block_index: u64,
    pub result: TransactionResult,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct ReceiptInfo {
    pub receipt: ReceiptTransaction,
    pub block_index: u64,
    pub result: TransactionResult,
}

pub struct ShardBlockExtraInfo {
    pub db_changes: storage::DBChanges,
    pub authority_proposals: Vec<AuthorityStake>,
    pub tx_results: Vec<TransactionResult>,
    pub largest_tx_nonce: HashMap<AccountId, u64>,
    pub new_receipts: HashMap<ShardId, ReceiptBlock>,
}

pub fn get_all_receipts<'a, I>(receipt_blocks_iter: I) -> Vec<&'a ReceiptTransaction>
where
    I: Iterator<Item = &'a ReceiptBlock>,
{
    receipt_blocks_iter.flat_map(|rb| &rb.receipts).collect()
}

pub struct ShardClient {
    pub chain: Arc<chain::BlockChain<SignedShardBlockHeader, SignedShardBlock, ShardChainStorage>>,
    pub trie: Arc<Trie>,
    pub storage: Arc<RwLock<ShardChainStorage>>,
    pub runtime: Runtime,
    pub trie_viewer: TrieViewer,
    pub pool: Option<Arc<RwLock<Pool>>>,
}

impl ShardClient {
    pub fn new<T: EDSigner + 'static>(
        signer: Option<Arc<T>>,
        chain_spec: &ChainSpec,
        storage: Arc<RwLock<ShardChainStorage>>,
        max_block_size: u32,
    ) -> Self {
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
        let pool = match signer {
            Some(signer) => Some(Arc::new(RwLock::new(Pool::new(
                signer,
                storage.clone(),
                trie.clone(),
                max_block_size,
            )))),
            None => None,
        };
        Self { chain, trie, storage, runtime, trie_viewer, pool }
    }

    pub fn get_state_update(&self) -> TrieUpdate {
        let root = self.chain.best_header().body.merkle_root_state;
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
        tx_results: Vec<TransactionResult>,
        largest_tx_nonce: HashMap<AccountId, u64>,
        new_receipts: HashMap<ShardId, ReceiptBlock>,
    ) {
        self.trie.apply_changes(db_transaction).ok();
        self.chain.insert_block(block.clone());
        if let Some(pool) = &self.pool {
            pool.write().expect(POISONED_LOCK_ERR).import_block(&block);
        }
        let index = block.index();

        let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
        guard.extend_transaction_results_addresses(block, tx_results).unwrap();
        guard.extend_receipts(index, new_receipts).unwrap();
        guard.extend_tx_nonce(largest_tx_nonce).unwrap();
    }

    fn compute_receipt_blocks(
        shard_ids: Vec<ShardId>,
        receipts: Vec<Vec<ReceiptTransaction>>,
        receipt_merkle_paths: Vec<MerklePath>,
        block: &SignedShardBlock,
    ) -> HashMap<ShardId, ReceiptBlock> {
        shard_ids
            .into_iter()
            .zip(receipts.into_iter().zip(receipt_merkle_paths.into_iter()))
            .map(|(shard_id, (receipts, path))| {
                (shard_id, ReceiptBlock::new(block.header(), path, receipts, shard_id))
            })
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
        let shard_block_extra = ShardBlockExtraInfo {
            db_changes: apply_result.db_changes,
            authority_proposals: apply_result.authority_proposals,
            tx_results: apply_result.tx_result,
            largest_tx_nonce: apply_result.largest_tx_nonce,
            new_receipts: receipt_map,
        };
        (shard_block, shard_block_extra)
    }

    pub fn apply_block(&self, block: SignedShardBlock) -> bool {
        let state_merkle_root = block.body.header.merkle_root_state;
        let receipt_merkle_root = block.body.header.receipt_merkle_root;
        let (shard_block, shard_block_extra) = self.prepare_new_block(
            block.body.header.parent_hash,
            block.body.receipts.clone(),
            block.body.transactions.clone(),
        );
        if shard_block.body.header.merkle_root_state == state_merkle_root
            && shard_block.body.header.receipt_merkle_root == receipt_merkle_root
        {
            self.insert_block(
                &block,
                shard_block_extra.db_changes,
                shard_block_extra.tx_results,
                shard_block_extra.largest_tx_nonce,
                shard_block_extra.new_receipts,
            );
            true
        } else {
            error!("Received Invalid block.");
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
        self.storage.write().expect(POISONED_LOCK_ERR).transaction_address(hash).unwrap().cloned()
    }

    pub fn get_transaction_info(&self, hash: &CryptoHash) -> Option<SignedTransactionInfo> {
        self.get_transaction_address(hash).and_then(|address| {
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
            Some(SignedTransactionInfo {
                transaction: transaction.clone(),
                block_index: block.header().index(),
                result,
            })
        })
    }

    pub fn get_receipt_info(&self, hash: &CryptoHash) -> Option<ReceiptInfo> {
        self.get_transaction_address(hash).and_then(|address| {
            let block_id = BlockId::Hash(address.block_hash);
            let header = self
                .chain
                .get_header(&block_id)
                .expect("transaction address points to non-existent block");
            let shard_id = address.shard_id?;
            let block_index = header.index();
            let lookup_index = if block_index >= 1 {
                block_index - 1
            } else {
                return None;
            };
            let receipt = self
                .storage
                .write()
                .expect(POISONED_LOCK_ERR)
                .receipt_block(lookup_index, shard_id)
                .unwrap()
                .map(|b| b.receipts[address.index].clone())?;
            let result = self.get_transaction_result(&hash);
            Some(ReceiptInfo { receipt, block_index, result })
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
                        result: receipt_result.result.clone(),
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
                result: transaction_result.result.clone(),
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

    /// get the largest transaction nonce for account from last block
    pub fn get_account_nonce(&self, account_id: AccountId) -> Option<u64> {
        self.storage.write().expect(POISONED_LOCK_ERR).tx_nonce(account_id).unwrap().cloned()
    }
}

#[cfg(test)]
mod tests {
    use node_runtime::chain_spec::{AuthorityRotation, DefaultIdType};
    use primitives::crypto::signer::InMemorySigner;
    use primitives::transaction::{
        FinalTransactionStatus, SignedTransaction, TransactionAddress, TransactionBody,
        TransactionStatus,
    };
    use storage::test_utils::create_beacon_shard_storages;
    use storage::GenericStorage;
    use testlib::node::TEST_BLOCK_MAX_SIZE;

    use super::*;

    fn get_test_client() -> (ShardClient, Vec<Arc<InMemorySigner>>) {
        let (chain_spec, signers) = ChainSpec::testing_spec(
            DefaultIdType::Named,
            2,
            1,
            AuthorityRotation::ProofOfAuthority,
        );
        let shard_storage = create_beacon_shard_storages().1;
        let shard_client = ShardClient::new(
            Some(signers[0].clone()),
            &chain_spec,
            shard_storage,
            TEST_BLOCK_MAX_SIZE,
        );
        (shard_client, signers)
    }

    impl ShardClient {
        // add a number of test blocks. Each block contains one transaction that sends money
        // from sender to receiver. Sender and receiver are assumed to exist.
        fn add_blocks(
            &mut self,
            sender: &str,
            receiver: &str,
            sender_signer: Arc<InMemorySigner>,
            num_blocks: u32,
        ) -> (u64, CryptoHash) {
            let mut prev_hash = *self
                .storage
                .write()
                .expect(POISONED_LOCK_ERR)
                .blockchain_storage_mut()
                .best_block_hash()
                .unwrap()
                .unwrap();
            let mut nonce = self.get_account_nonce(sender.to_string()).unwrap_or_else(|| 0) + 1;
            for _ in 0..num_blocks {
                let tx =
                    TransactionBody::send_money(nonce, sender, receiver, 1).sign(&*sender_signer);
                let (block, block_extra) =
                    self.prepare_new_block(prev_hash, vec![], vec![tx.clone()]);
                prev_hash = block.hash;
                nonce += 1;
                self.insert_block(
                    &block,
                    block_extra.db_changes,
                    block_extra.tx_results,
                    block_extra.largest_tx_nonce,
                    block_extra.new_receipts,
                );
            }
            (nonce, prev_hash)
        }
    }

    #[test]
    fn test_get_transaction_status_unknown() {
        let (client, _) = get_test_client();
        let result = client.get_transaction_result(&CryptoHash::default());
        assert_eq!(result.status, TransactionStatus::Unknown);
    }

    #[test]
    fn test_transaction_failed() {
        let (client, signers) = get_test_client();
        let tx = TransactionBody::send_money(1, "xyz.near", "bob.near", 100).sign(&*signers[0]);
        let (block, block_extra) =
            client.prepare_new_block(client.genesis_hash(), vec![], vec![tx.clone()]);
        client.insert_block(
            &block,
            block_extra.db_changes,
            block_extra.tx_results,
            block_extra.largest_tx_nonce,
            block_extra.new_receipts,
        );

        let result = client.get_transaction_result(&tx.get_hash());
        assert_eq!(result.status, TransactionStatus::Failed);
    }

    #[test]
    fn test_get_transaction_status_complete() {
        let (client, signers) = get_test_client();
        let tx = TransactionBody::send_money(1, "alice.near", "bob.near", 10).sign(&*signers[0]);
        let (block, block_extra) =
            client.prepare_new_block(client.genesis_hash(), vec![], vec![tx.clone()]);
        client.insert_block(
            &block,
            block_extra.db_changes,
            block_extra.tx_results,
            block_extra.largest_tx_nonce,
            block_extra.new_receipts,
        );

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
        let (block2, block_extra2) =
            client.prepare_new_block(block.hash, vec![receipt_block], vec![]);
        client.insert_block(
            &block2,
            block_extra2.db_changes,
            block_extra2.tx_results,
            block_extra2.largest_tx_nonce,
            block_extra2.new_receipts,
        );

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
        client.insert_block(
            &block,
            db_changes,
            vec![TransactionResult::default()],
            HashMap::new(),
            HashMap::new(),
        );
        let address = client.get_transaction_address(&hash);
        let expected = TransactionAddress { block_hash: block.hash, index: 0, shard_id: None };
        assert_eq!(address, Some(expected.clone()));
    }

    // TODO(472): Add extensive testing for ShardBlockChain.

    #[test]
    fn test_tx_nonce() {
        let (mut client, signers) = get_test_client();
        let (nonce, _) = client.add_blocks("alice.near", "bob.near", signers[0].clone(), 5);
        let tx_nonce = client
            .storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .tx_nonce("alice.near".to_string())
            .unwrap()
            .unwrap()
            .clone();
        assert_eq!(nonce - 1, tx_nonce);
    }

    #[test]
    /// Create chain with two accounts, each having different nonce. Check that
    /// transactions with old nonce cannot make it into the mempool and transactions
    /// with valid nonce can.
    fn test_mempool_add_tx() {
        let (mut client, signers) = get_test_client();
        let create_transaction = |sender, receiver, signer, nonce| {
            TransactionBody::send_money(nonce, sender, receiver, 1).sign(signer)
        };
        client.add_blocks("alice.near", "bob.near", signers[0].clone(), 5);
        client.add_blocks("bob.near", "alice.near", signers[1].clone(), 1);
        let pool_arc = client.pool.clone().unwrap();
        let mut pool = pool_arc.write().expect(POISONED_LOCK_ERR);
        let tx = create_transaction("alice.near", "bob.near", &*signers[0], 2);
        pool.add_transaction(tx).unwrap();
        assert!(pool.is_empty());
        let tx = create_transaction("alice.near", "bob.near", &*signers[0], 6);
        pool.add_transaction(tx).unwrap();
        assert_eq!(pool.len(), 1);
        let tx = create_transaction("bob.near", "alice.near", &*signers[1], 1);
        pool.add_transaction(tx).unwrap();
        assert_eq!(pool.len(), 1);
        let tx = create_transaction("bob.near", "alice.near", &*signers[1], 2);
        pool.add_transaction(tx).unwrap();
        assert_eq!(pool.len(), 2);
    }
}
