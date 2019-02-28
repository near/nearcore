use std::collections::{HashSet, HashMap};
use std::sync::{Arc, RwLock};

use log::info;

use node_runtime::state_viewer::TrieViewer;
use primitives::consensus::Payload;
use primitives::types::AuthorityId;
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};
use primitives::hash::{CryptoHash, hash_struct};
use primitives::merkle::verify_path;
use primitives::transaction::{SignedTransaction, verify_transaction_signature};
use storage::{GenericStorage, ShardChainStorage, Trie, TrieUpdate};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub mod pool_task;

/// mempool that stores transactions and receipts for a chain
pub struct Pool {
    transactions: RwLock<HashSet<SignedTransaction>>,
    receipts: RwLock<HashSet<ReceiptBlock>>,
    storage: Arc<RwLock<ShardChainStorage>>,
    trie: Arc<Trie>,
    state_viewer: TrieViewer,
    snapshots: RwLock<HashMap<CryptoHash, ChainPayload>>,
}

impl Pool {
    pub fn new(storage: Arc<RwLock<ShardChainStorage>>, trie: Arc<Trie>) -> Self {
        Pool { 
            transactions: RwLock::new(HashSet::new()),
            receipts: RwLock::new(HashSet::new()),
            storage,
            trie,
            state_viewer: TrieViewer {},
            snapshots: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_state_update(&self) -> TrieUpdate {
        let root = self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .blockchain_storage_mut()
            .best_block()
            .unwrap()
            .unwrap()
            .merkle_root_state();
        TrieUpdate::new(self.trie.clone(), root)
    }

    pub fn add_transaction(&self, transaction: SignedTransaction) -> Result<(), String> {
        if let Ok(Some(_)) = self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .transaction_address(&transaction.get_hash()) {
            return Ok(());
        }
        let mut state_update = self.get_state_update();
        let originator = transaction.body.get_originator();
        let public_keys =
            self.state_viewer.get_public_keys_for_account(&mut state_update, &originator)?;
        if !verify_transaction_signature(&transaction, &public_keys) {
            return Err(format!(
                "transaction not signed with a public key of originator {:?}",
                originator
            ));
        }
        self.transactions.write().expect(POISONED_LOCK_ERR).insert(transaction);
        Ok(())
    }

    pub fn add_receipt(&self, receipt: ReceiptBlock) -> Result<(), String> {
        // TODO: cache hash of receipt
        if let Ok(Some(_)) = self.storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .transaction_address(&hash_struct(&receipt)) {
            return Ok(());
        }
        if !verify_path(receipt.header.body.receipt_merkle_root, &receipt.path, &receipt.receipts) {
            return Err("Invalid receipt block".to_string());
        }
        self.receipts.write().expect(POISONED_LOCK_ERR).insert(receipt);
        Ok(())
    }

    pub fn add_payload(&self, payload: ChainPayload) -> Result<(), String> {
        for transaction in payload.transactions {
            self.add_transaction(transaction)?;
        }
        for receipt in payload.receipts {
            self.add_receipt(receipt)?;
        }
        Ok(())
    }

    pub fn snapshot_payload(&self) -> CryptoHash {
        let transactions: Vec<_> = self.transactions
            .write()
            .expect(POISONED_LOCK_ERR)
            .drain()
            .collect();
        let receipts: Vec<_> = self.receipts
            .write()
            .expect(POISONED_LOCK_ERR)
            .drain()
            .collect();
        let snapshot = ChainPayload { transactions, receipts };
        if snapshot.is_empty() {
            return CryptoHash::default();
        }
        let h = hash_struct(&snapshot);
        info!("Snapshot: {:?}, hash: {:?}", snapshot, h);
        self.snapshots.write().expect(POISONED_LOCK_ERR).insert(h, snapshot);
        h
    }

    pub fn contains_payload_snapshot(&self, hash: &CryptoHash) -> bool {
        if hash == &CryptoHash::default() {
            return true;
        }
        self.snapshots.write().expect(POISONED_LOCK_ERR).contains_key(hash)
    }

    pub fn pop_payload_snapshot(&self, hash: &CryptoHash) -> Option<ChainPayload> {
        if hash == &CryptoHash::default() {
            return Some(ChainPayload::default());
        }
        self.snapshots.write().expect(POISONED_LOCK_ERR).remove(hash)
    }

    pub fn clear_snapshots(&self) {
        self.snapshots.write().expect(POISONED_LOCK_ERR).clear();
    }

    /// Request payload diff for given authority.
    pub fn snapshot_request(&self, _authority_id: AuthorityId, _hash: CryptoHash) -> Result<ChainPayload, String> {
        Err("Not implemented".to_string())
    }

    /// Prepares payload to gossip to peer authority.
    pub fn prepare_payload_announce(&self) -> Option<(AuthorityId, ChainPayload)> {
        None
    }

    pub fn import_block(&self, block: &SignedShardBlock) {
        for transaction in block.body.transactions.iter() {
            self.transactions.write().expect(POISONED_LOCK_ERR).remove(transaction);
        }
        for receipt in block.body.receipts.iter() {
            self.receipts.write().expect(POISONED_LOCK_ERR).remove(receipt);
        }
    }
}

#[cfg(test)]
mod tests {
    use node_runtime::{Runtime, test_utils::generate_test_chain_spec};
    use primitives::hash::CryptoHash;
    use primitives::signature::{SecretKey, sign};
    use primitives::transaction::{SendMoneyTransaction, TransactionBody};
    use primitives::types::MerkleHash;
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;

    fn get_test_chain() -> (
        Arc<RwLock<ShardChainStorage>>,
        Arc<Trie>,
        SecretKey,
    ) {
        let (chain_spec, _, secret_key) = generate_test_chain_spec();
        let shard_storage = create_beacon_shard_storages().1;
        let trie = Arc::new(Trie::new(shard_storage.clone()));
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
        let _ = Arc::new(chain::BlockChain::new(genesis, shard_storage.clone()));
        (shard_storage, trie, secret_key)
    }

    #[test]
    fn test_import_block() {
        let (storage, trie, secret_key) = get_test_chain();
        let pool = Pool::new(storage, trie);
        let tx_body = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 0,
            originator: "alice.near".to_string(),
            receiver: "bob.near".to_string(),
            amount: 1,
        });
        let hash = tx_body.get_hash();
        let signature = sign(hash.as_ref(), &secret_key);
        let transaction = SignedTransaction::new(signature, tx_body);
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.read().expect(POISONED_LOCK_ERR).len(), 1);
        let block = SignedShardBlock::new(
            0,
            0,
            CryptoHash::default(),
            CryptoHash::default(),
            vec![transaction],
            vec![],
            CryptoHash::default(),
        );
        pool.import_block(&block);
        assert_eq!(pool.transactions.read().expect(POISONED_LOCK_ERR).len(), 0);
    }
}
