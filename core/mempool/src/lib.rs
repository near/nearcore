use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use log::info;

use node_runtime::state_viewer::TrieViewer;
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};
use primitives::consensus::Payload;
use primitives::hash::{hash_struct, CryptoHash};
use primitives::merkle::verify_path;
use primitives::transaction::{verify_transaction_signature, SignedTransaction};
use primitives::types::AuthorityId;
use storage::{GenericStorage, ShardChainStorage, Trie, TrieUpdate};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub mod pool_task;

use crate::pool_task::MemPoolControl;

/// mempool that stores transactions and receipts for a chain
pub struct Pool {
    transactions: RwLock<HashSet<SignedTransaction>>,
    receipts: RwLock<HashSet<ReceiptBlock>>,
    storage: Arc<RwLock<ShardChainStorage>>,
    trie: Arc<Trie>,
    state_viewer: TrieViewer,
    snapshots: RwLock<HashMap<CryptoHash, ChainPayload>>,
    /// Given MemPool's authority id.
    pub authority_id: RwLock<Option<AuthorityId>>,
    /// Number of authorities currently.
    num_authorities: RwLock<Option<usize>>,
    /// Map from hash of tx/receipt to hashset of authorities it is known.
    known_to: RwLock<HashMap<CryptoHash, HashSet<AuthorityId>>>,
    /// List of requested snapshots that can't be fetched yet.
    pending_snapshots: RwLock<Vec<(AuthorityId, CryptoHash)>>,
    /// List of requested snapshots that are unblocked and can be confirmed.
    ready_snapshots: RwLock<Vec<(AuthorityId, CryptoHash)>>,
}

impl Pool {
    pub fn new(storage: Arc<RwLock<ShardChainStorage>>, trie: Arc<Trie>) -> Self {
        Pool {
            transactions: RwLock::new(HashSet::new()),
            receipts: RwLock::new(HashSet::new()),
            storage,
            trie,
            state_viewer: TrieViewer {},
            snapshots: Default::default(),
            authority_id: Default::default(),
            num_authorities: Default::default(),
            known_to: Default::default(),
            pending_snapshots: Default::default(),
            ready_snapshots: Default::default(),
        }
    }

    /// Reset MemPool: clear snapshots, switch to new authorities and own authority id.
    pub fn reset(&self, control: MemPoolControl) {
        match control {
            MemPoolControl::Reset { authority_id, num_authorities, owner_uid, block_index, .. } => {
                info!(target: "mempool", "MemPool reset for authority_id={}, owner_uid={}, block_index={}",
                      authority_id, owner_uid, block_index);
                *self.authority_id.write().expect(POISONED_LOCK_ERR) = Some(authority_id);
                *self.num_authorities.write().expect(POISONED_LOCK_ERR) = Some(num_authorities);
            }
            MemPoolControl::Stop => {
                info!(target: "mempool", "MemPool stopped");
                *self.authority_id.write().expect(POISONED_LOCK_ERR) = None;
                *self.num_authorities.write().expect(POISONED_LOCK_ERR) = None;
            }
        }
        self.snapshots.write().expect(POISONED_LOCK_ERR).clear();
        self.known_to.write().expect(POISONED_LOCK_ERR).clear();
    }

    pub fn get_state_update(&self) -> TrieUpdate {
        let root = self
            .storage
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
        if let Ok(Some(_)) = self
            .storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .transaction_address(&transaction.get_hash())
        {
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
        if let Ok(Some(_)) = self
            .storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .transaction_address(&hash_struct(&receipt))
        {
            return Ok(());
        }
        if !verify_path(receipt.header.body.receipt_merkle_root, &receipt.path, &receipt.receipts) {
            return Err("Invalid receipt block".to_string());
        }
        self.receipts.write().expect(POISONED_LOCK_ERR).insert(receipt);
        Ok(())
    }

    // TODO: Check snapshots on `pending_snapshots` which get unblocked and move them to `ready_snapshots`
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
        let transactions: Vec<_> =
            self.transactions.write().expect(POISONED_LOCK_ERR).drain().collect();
        let receipts: Vec<_> = self.receipts.write().expect(POISONED_LOCK_ERR).drain().collect();
        let snapshot = ChainPayload { transactions, receipts };
        if snapshot.is_empty() {
            return CryptoHash::default();
        }
        let h = hash_struct(&snapshot);
        self.snapshots.write().expect(POISONED_LOCK_ERR).insert(h, snapshot);
        h
    }

    pub fn contains_payload_snapshot(&self, hash: &CryptoHash) -> bool {
        if hash == &CryptoHash::default() {
            return true;
        }
        self.snapshots.read().expect(POISONED_LOCK_ERR).contains_key(hash)
    }

    pub fn pop_payload_snapshot(&self, hash: &CryptoHash) -> Option<ChainPayload> {
        if hash == &CryptoHash::default() {
            return Some(ChainPayload::default());
        }
        self.snapshots.write().expect(POISONED_LOCK_ERR).remove(hash)
    }

    /// Request payload diff for given authority.
    pub fn snapshot_request(
        &self,
        _authority_id: AuthorityId,
        hash: CryptoHash,
    ) -> Result<ChainPayload, String> {
        if let Some(value) = self.snapshots.read().expect(POISONED_LOCK_ERR).get(&hash) {
            Ok(value.clone())
        } else {
            Err(format!(
                "[{:?}] No such payload with hash {}",
                self.authority_id.read().expect(POISONED_LOCK_ERR),
                hash
            ))
        }
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

    pub fn add_pending(&self, authority_id: AuthorityId, hash: CryptoHash) {
        self.pending_snapshots.write().expect(POISONED_LOCK_ERR).push((authority_id, hash))
    }

    pub fn add_payload_snapshot(
        &self,
        authority_id: AuthorityId,
        payload: ChainPayload,
    ) -> Result<(), String> {
        // TODO: payload should be diff and then calculate the actual payload, but for now it is full.
        if payload.is_empty() {
            return Ok(());
        }
        let h = hash_struct(&payload);
        self.snapshots.write().expect(POISONED_LOCK_ERR).insert(h, payload);
        self.ready_snapshots.write().expect(POISONED_LOCK_ERR).push((authority_id, h));
        Ok(())
    }

    pub fn ready_snapshots(&self) -> Vec<(AuthorityId, CryptoHash)> {
        self.ready_snapshots.write().expect(POISONED_LOCK_ERR).drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use node_runtime::{test_utils::generate_test_chain_spec, Runtime};
    use primitives::hash::CryptoHash;
    use primitives::signature::{sign, SecretKey};
    use primitives::transaction::{SendMoneyTransaction, TransactionBody};
    use primitives::types::MerkleHash;
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;

    fn get_test_chain() -> (Arc<RwLock<ShardChainStorage>>, Arc<Trie>, SecretKey) {
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
