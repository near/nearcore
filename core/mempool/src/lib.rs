#[macro_use]
extern crate serde_derive;

use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::{Arc, RwLock};

use log::info;

use node_runtime::state_viewer::TrieViewer;
use primitives::chain::{ChainPayload, ReceiptBlock, SignedShardBlock};
use primitives::consensus::Payload;
use primitives::hash::CryptoHash;
use primitives::merkle::verify_path;
use primitives::signer::BlockSigner;
use primitives::transaction::{SignedTransaction, verify_transaction_signature};
use primitives::types::{AuthorityId, AccountId};
use storage::{GenericStorage, ShardChainStorage, Trie, TrieUpdate};

pub mod payload_gossip;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Mempool that stores transactions and receipts for a chain
pub struct Pool {
    signer: Arc<BlockSigner>,
    // transactions need to be grouped by account, and within each account,
    // ordered by nonce so that runtime will not ignore some of the transactions
    transactions: RwLock<HashMap<AccountId, BTreeMap<u64, SignedTransaction>>>,
    receipts: RwLock<HashSet<ReceiptBlock>>,
    storage: Arc<RwLock<ShardChainStorage>>,
    trie: Arc<Trie>,
    state_viewer: TrieViewer,
    snapshots: RwLock<HashSet<ChainPayload>>,
    /// List of requested snapshots that can't be fetched yet.
    pending_snapshots: RwLock<Vec<(AuthorityId, CryptoHash)>>,
    /// List of requested snapshots that are unblocked and can be confirmed.
    ready_snapshots: RwLock<Vec<(AuthorityId, CryptoHash)>>,
    /// Given MemPool's authority id.
    pub authority_id: RwLock<Option<AuthorityId>>,
    /// Number of authorities currently.
    num_authorities: RwLock<Option<usize>>,
    /// Map from hash of tx/receipt to hashset of authorities it is known.
    known_to: RwLock<HashMap<CryptoHash, HashSet<AuthorityId>>>,
}

impl Pool {
    pub fn new(signer: Arc<BlockSigner>, storage: Arc<RwLock<ShardChainStorage>>, trie: Arc<Trie>) -> Self {
        Pool {
            signer,
            transactions: Default::default(),
            receipts: Default::default(),
            storage,
            trie,
            state_viewer: TrieViewer {},
            snapshots: Default::default(),
            pending_snapshots: Default::default(),
            ready_snapshots: Default::default(),
            authority_id: Default::default(),
            num_authorities: Default::default(),
            known_to: Default::default(),
        }
    }

    pub fn reset(&self, authority_id: Option<AuthorityId>, num_authorities: Option<usize>) {
        *self.authority_id.write().expect(POISONED_LOCK_ERR) = authority_id;
        *self.num_authorities.write().expect(POISONED_LOCK_ERR) = num_authorities;
        self.snapshots.write().expect(POISONED_LOCK_ERR).clear();
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.read().expect(POISONED_LOCK_ERR).is_empty() 
        && self.receipts.read().expect(POISONED_LOCK_ERR).is_empty()
    }

    pub fn len(&self) -> usize {
        let mut length = 0;
        let guard = self.transactions.read().expect(POISONED_LOCK_ERR);
        length += guard.values().map(BTreeMap::len).sum::<usize>();
        let guard = self.receipts.read().expect(POISONED_LOCK_ERR);
        length += guard.len();
        length
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
        {
            // validate transaction
            let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);
            let sender = transaction.body.get_originator();
            // get the largest nonce from sender before this block
            let old_tx_nonce = *guard.tx_nonce(sender).unwrap().unwrap_or(&0);
            if transaction.body.get_nonce() <= old_tx_nonce {
                return Ok(());
            }
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
        self.known_to.write().expect(POISONED_LOCK_ERR).insert(transaction.get_hash(), HashSet::new());
        self.transactions
            .write()
            .expect(POISONED_LOCK_ERR)
            .entry(transaction.body.get_originator())
            .or_insert_with(BTreeMap::new)
            .insert(transaction.body.get_nonce(), transaction);
        Ok(())
    }

    pub fn add_receipt(&self, receipt: ReceiptBlock) -> Result<(), String> {
        if let Ok(Some(_)) = self
            .storage
            .write()
            .expect(POISONED_LOCK_ERR)
            .transaction_address(&receipt.hash)
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

    pub fn add_payload_with_author(&self, payload: ChainPayload, author: AuthorityId) -> Result<(), String> {
        for transaction in payload.transactions {
            let hash = transaction.get_hash();
            self.add_transaction(transaction)?;
            self.known_to
                .write()
                .expect(POISONED_LOCK_ERR)
                .entry(hash)
                .or_insert_with(HashSet::new)
                .insert(author);
        }
        for receipt in payload.receipts {
            self.add_receipt(receipt)?;
        }
        Ok(())
    }

    pub fn snapshot_payload(&self) -> CryptoHash {
        // Put tx and receipts into an snapshot without erasing them.
        let transactions: Vec<_> =
            self.transactions
                .write()
                .expect(POISONED_LOCK_ERR)
                .values()
                .flat_map::<Vec<_>, _>(|v| v.values().collect())
                .cloned()
                .collect();
        let receipts: Vec<_> = self.receipts.write().expect(POISONED_LOCK_ERR).iter().cloned().collect();
        let snapshot = ChainPayload::new(transactions, receipts);
        if snapshot.is_empty() {
            return CryptoHash::default();
        }
        let h = snapshot.get_hash();
        info!(target: "mempool", "Snapshotting payload, #tx={}, #r={}, hash={:?}",
            snapshot.transactions.len(),
            snapshot.receipts.len(),
            h,
        );
        self.snapshots.write().expect(POISONED_LOCK_ERR).insert(snapshot);
        h
    }

    pub fn contains_payload_snapshot(&self, hash: &CryptoHash) -> bool {
        if hash == &CryptoHash::default() {
            return true;
        }
        self.snapshots.read().expect(POISONED_LOCK_ERR).contains(hash)
    }

    pub fn pop_payload_snapshot(&self, hash: &CryptoHash) -> Option<ChainPayload> {
        if hash == &CryptoHash::default() {
            return Some(ChainPayload::default());
        }
        let payload = self.snapshots.write().expect(POISONED_LOCK_ERR).take(hash);
        if let Some(ref p) = payload {
            info!(target: "mempool", "Popping snapshot, authority={:?}, #tx={}, #r={}, hash={:?}",
                  self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap(),
                  p.transactions.len(),
                  p.receipts.len(),
                  hash,
            );
        } else {
            info!(target: "mempool", "Failed to pop snapshot, authority={:?}, hash={:?}",
                  self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap(),
                  hash,
            );
        }
        payload
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
                "No such payload with hash {}",
                hash
            ))
        }
    }

    /// Prepares payload to gossip to peer authority.
    pub fn prepare_payload_gossip(&self) -> Vec<crate::payload_gossip::PayloadGossip> {
        if self.authority_id.read().expect(POISONED_LOCK_ERR).is_none() {
            return vec![];
        }
        let mut result = vec![];
        let authority_id = self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap();
        for their_authority_id in 0..self.num_authorities.read().expect(POISONED_LOCK_ERR).unwrap_or(0) {
            if their_authority_id == authority_id {
                continue;
            }
            let mut to_send = vec![];
            for tx in self.transactions
                .read()
                .expect(POISONED_LOCK_ERR)
                .values()
                .flat_map(BTreeMap::values) {
                let mut locked_known_to = self.known_to.write().expect(POISONED_LOCK_ERR);
                match locked_known_to.get_mut(&tx.get_hash()) {
                    Some(known_to) => {
                        if !known_to.contains(&their_authority_id) {
                            to_send.push(tx.clone());
                            known_to.insert(their_authority_id);
                        }
                    }
                    None => {
                        to_send.push(tx.clone());
                        let mut known_to = HashSet::new();
                        known_to.insert(their_authority_id);
                        locked_known_to.insert(tx.get_hash(), known_to);
                    }
                }
            }
            if to_send.is_empty() {
                continue;
            }
            let payload = ChainPayload::new(to_send, vec![]);
            result.push(crate::payload_gossip::PayloadGossip::new(
                authority_id,
                their_authority_id,
                payload,
                self.signer.clone(),
            ));
        }
        result
    }

    pub fn import_block(&self, block: &SignedShardBlock) {
        for transaction in block.body.transactions.iter() {
            self.known_to.write().expect(POISONED_LOCK_ERR).remove(&transaction.get_hash());
            let mut guard = self.transactions.write().expect(POISONED_LOCK_ERR);
            let mut remove_map = false;
            let sender = transaction.body.get_originator();
            if let Some(map) = guard.get_mut(&sender) {
                map.remove(&transaction.body.get_nonce());
                remove_map = map.is_empty();
            }
            if remove_map {
                guard.remove(&sender);
            }
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
        let h = payload.get_hash();
        info!(target: "mempool", "Adding payload snapshot, authority={:?}, #tx={}, #r={}, hash={:?} received from {:?}",
            self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap(),
            payload.transactions.len(),
            payload.receipts.len(),
            h,
            authority_id,
        );
        self.snapshots.write().expect(POISONED_LOCK_ERR).insert(payload);
        self.ready_snapshots.write().expect(POISONED_LOCK_ERR).push((authority_id, h));
        Ok(())
    }

    pub fn ready_snapshots(&self) -> Vec<(AuthorityId, CryptoHash)> {
        self.ready_snapshots.write().expect(POISONED_LOCK_ERR).drain(..).collect()
    }
}

#[cfg(test)]
mod tests {
    use node_runtime::{Runtime, test_utils::generate_test_chain_spec};
    use primitives::hash::CryptoHash;
    use primitives::signer::InMemorySigner;
    use primitives::transaction::{SendMoneyTransaction, TransactionBody};
    use primitives::types::MerkleHash;
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;

    fn get_test_chain() -> (
        Arc<RwLock<ShardChainStorage>>,
        Arc<Trie>,
        Vec<Arc<InMemorySigner>>
    ) {
        let (chain_spec, signers) = generate_test_chain_spec();
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
        (shard_storage, trie, signers)
    }

    #[test]
    fn test_import_block() {
        let (storage, trie, signers) = get_test_chain();
        let pool = Pool::new(signers[0].clone(), storage, trie);
        let transaction = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            originator: "alice.near".to_string(),
            receiver: "bob.near".to_string(),
            amount: 1,
        })
        .sign(signers[0].clone());
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.read().expect(POISONED_LOCK_ERR).len(), 1);
        assert_eq!(pool.known_to.read().expect(POISONED_LOCK_ERR).len(), 1);
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
        assert_eq!(pool.known_to.read().expect(POISONED_LOCK_ERR).len(), 0);
    }
}
