#[macro_use]
extern crate serde_derive;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use log::info;

use node_runtime::state_viewer::TrieViewer;
use primitives::chain::{
    ChainPayload, MissingPayloadRequest, MissingPayloadResponse, ReceiptBlock, SignedShardBlock,
    Snapshot,
};
use primitives::hash::CryptoHash;
use primitives::merkle::verify_path;
use primitives::signer::BlockSigner;
use primitives::transaction::{verify_transaction_signature, SignedTransaction};
use primitives::types::{AccountId, AuthorityId, BlockIndex};
use storage::{GenericStorage, ShardChainStorage, Trie, TrieUpdate};

pub mod payload_gossip;
use payload_gossip::PayloadGossip;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Mempool that stores transactions and receipts for a chain
pub struct Pool {
    signer: Arc<BlockSigner>,
    // transactions need to be grouped by account, and within each account,
    // ordered by nonce so that runtime will not ignore some of the transactions
    transactions: RwLock<HashMap<AccountId, BTreeMap<u64, SignedTransaction>>>,
    // store the map (transaction hash -> (account_id, nonce))
    // that allows us to lookup transaction by hash
    transaction_info: RwLock<HashMap<CryptoHash, (AccountId, u64)>>,
    receipts: RwLock<HashSet<ReceiptBlock>>,
    storage: Arc<RwLock<ShardChainStorage>>,
    trie: Arc<Trie>,
    state_viewer: TrieViewer,
    /// List of requested snapshots that are unblocked and can be confirmed.
    snapshots: RwLock<HashSet<Snapshot>>,
    /// List of requested snapshots that can't be fetched yet.
    pending_snapshots: RwLock<HashSet<Snapshot>>,
    /// Given MemPool's authority id.
    pub authority_id: RwLock<Option<AuthorityId>>,
    /// Number of authorities currently.
    num_authorities: RwLock<Option<usize>>,
    /// Map from hash of tx/receipt to hashset of authorities it is known.
    known_to: RwLock<HashMap<CryptoHash, HashSet<AuthorityId>>>,
}

impl Pool {
    pub fn new(
        signer: Arc<BlockSigner>,
        storage: Arc<RwLock<ShardChainStorage>>,
        trie: Arc<Trie>,
    ) -> Self {
        Pool {
            signer,
            transactions: Default::default(),
            transaction_info: Default::default(),
            receipts: Default::default(),
            storage,
            trie,
            state_viewer: TrieViewer {},
            snapshots: Default::default(),
            pending_snapshots: Default::default(),
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
        let sender = transaction.body.get_originator();
        let nonce = transaction.body.get_nonce();
        {
            // validate transaction
            let mut guard = self.storage.write().expect(POISONED_LOCK_ERR);

            // get the largest nonce from sender before this block
            let old_tx_nonce = *guard.tx_nonce(sender.clone()).unwrap().unwrap_or(&0);
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
        self.known_to
            .write()
            .expect(POISONED_LOCK_ERR)
            .entry(transaction.get_hash())
            .or_insert_with(HashSet::new);
        let mut transaction_guard = self.transactions.write().expect(POISONED_LOCK_ERR);
        let mut transaction_info_guard = self.transaction_info.write().expect(POISONED_LOCK_ERR);
        transaction_info_guard.insert(transaction.get_hash(), (sender.clone(), nonce));
        transaction_guard.entry(sender).or_insert_with(BTreeMap::new).insert(nonce, transaction);
        Ok(())
    }

    pub fn add_receipt(&self, receipt: ReceiptBlock) -> Result<(), String> {
        if let Ok(Some(_)) =
            self.storage.write().expect(POISONED_LOCK_ERR).transaction_address(&receipt.get_hash())
        {
            return Ok(());
        }
        if !verify_path(receipt.header.body.receipt_merkle_root, &receipt.path, &receipt.receipts) {
            return Err("Invalid receipt block".to_string());
        }
        self.receipts.write().expect(POISONED_LOCK_ERR).insert(receipt);
        Ok(())
    }

    /// Add requested transactions and receipts from the peer who sends the missing payload response.
    /// Update `known_to` to include the include the authority id of the peer.
    /// Move snapshot from `pending_snapshots` to `snapshots`.
    pub fn add_missing_payload(
        &self,
        author: AuthorityId,
        response: MissingPayloadResponse,
    ) -> Result<CryptoHash, String> {
        let snapshot =
            self.pending_snapshots.write().expect(POISONED_LOCK_ERR).take(&response.snapshot_hash);
        match snapshot {
            Some(snapshot) => {
                for transaction in response.transactions {
                    let hash = transaction.get_hash();
                    self.add_transaction(transaction)?;
                    self.known_to
                        .write()
                        .expect(POISONED_LOCK_ERR)
                        .entry(hash)
                        .or_insert_with(HashSet::new)
                        .insert(author);
                }
                for receipt in response.receipts {
                    let hash = receipt.get_hash();
                    self.add_receipt(receipt)?;
                    self.known_to
                        .write()
                        .expect(POISONED_LOCK_ERR)
                        .entry(hash)
                        .or_insert_with(HashSet::new)
                        .insert(author);
                }
                let hash = snapshot.get_hash();
                self.snapshots.write().expect(POISONED_LOCK_ERR).insert(snapshot);
                Ok(hash)
            }
            None => Err("snapshot does not exist".to_string()),
        }
    }

    pub fn add_payload_with_author(
        &self,
        payload: ChainPayload,
        author: AuthorityId,
    ) -> Result<(), String> {
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
            let hash = receipt.get_hash();
            self.add_receipt(receipt)?;
            self.known_to
                .write()
                .expect(POISONED_LOCK_ERR)
                .entry(hash)
                .or_insert_with(HashSet::new)
                .insert(author);
        }
        Ok(())
    }

    pub fn snapshot_payload(&self) -> CryptoHash {
        // Put tx and receipts into an snapshot without erasing them.
        let transactions: Vec<_> = self
            .transactions
            .read()
            .expect(POISONED_LOCK_ERR)
            .values()
            .flat_map::<Vec<_>, _>(|v| v.values().map(SignedTransaction::get_hash).collect())
            .collect();
        let receipts: Vec<_> = self
            .receipts
            .read()
            .expect(POISONED_LOCK_ERR)
            .iter()
            .map(ReceiptBlock::get_hash)
            .clone()
            .collect();
        let snapshot = Snapshot::new(transactions, receipts);
        if snapshot.is_empty() {
            return CryptoHash::default();
        }
        let h = snapshot.get_hash();
        info!(
            target: "mempool", "Snapshotting payload, #tx={}, #r={}, hash={:?}",
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

    fn snapshot_to_payload(&self, snapshot: Snapshot) -> ChainPayload {
        let guard = self.transactions.read().expect(POISONED_LOCK_ERR);
        let info_guard = self.transaction_info.read().expect(POISONED_LOCK_ERR);
        let transactions = snapshot
            .transactions
            .into_iter()
            .filter_map(|hash| {
                info_guard
                    .get(&hash)
                    .and_then(|(id, nonce)| guard.get(id).and_then(|map| map.get(&nonce)).cloned())
            })
            .collect();
        let guard = self.receipts.read().expect(POISONED_LOCK_ERR);
        let receipts =
            snapshot.receipts.into_iter().filter_map(|hash| guard.get(&hash).cloned()).collect();
        ChainPayload::new(transactions, receipts)
    }

    pub fn pop_payload_snapshot(&self, hash: &CryptoHash) -> Option<ChainPayload> {
        if hash == &CryptoHash::default() {
            return Some(ChainPayload::default());
        }
        let snapshot = self.snapshots.write().expect(POISONED_LOCK_ERR).take(hash);
        if let Some(ref p) = snapshot {
            info!(
                target: "mempool", "Popping snapshot, authority={:?}, #tx={}, #r={}, hash={:?}",
                self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap(),
                p.transactions.len(),
                p.receipts.len(),
                hash,
            );
        } else {
            info!(
                target: "mempool", "Failed to pop snapshot, authority={:?}, hash={:?}",
                self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap(),
                hash,
            );
        }
        Some(self.snapshot_to_payload(snapshot?))
    }

    /// respond to snapshot request
    pub fn on_snapshot_request(
        &self,
        _authority_id: AuthorityId,
        hash: CryptoHash,
    ) -> Result<Snapshot, String> {
        if let Some(snapshot) = self.snapshots.read().expect(POISONED_LOCK_ERR).get(&hash) {
            Ok(snapshot.clone())
        } else {
            Err(format!("No such payload with hash {}", hash))
        }
    }

    /// Prepares payload to gossip to peer authority.
    pub fn prepare_payload_gossip(&self, block_index: BlockIndex) -> Vec<PayloadGossip> {
        if self.authority_id.read().expect(POISONED_LOCK_ERR).is_none() {
            return vec![];
        }
        let mut result = vec![];
        let authority_id = self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap();
        for their_authority_id in
            0..self.num_authorities.read().expect(POISONED_LOCK_ERR).unwrap_or(0)
        {
            if their_authority_id == authority_id {
                continue;
            }
            let mut tx_to_send = vec![];
            for tx in self
                .transactions
                .read()
                .expect(POISONED_LOCK_ERR)
                .values()
                .flat_map(BTreeMap::values)
            {
                let mut locked_known_to = self.known_to.write().expect(POISONED_LOCK_ERR);
                match locked_known_to.get_mut(&tx.get_hash()) {
                    Some(known_to) => {
                        if !known_to.contains(&their_authority_id) {
                            tx_to_send.push(tx.clone());
                            known_to.insert(their_authority_id);
                        }
                    }
                    None => {
                        tx_to_send.push(tx.clone());
                        let mut known_to = HashSet::new();
                        known_to.insert(their_authority_id);
                        locked_known_to.insert(tx.get_hash(), known_to);
                    }
                }
            }
            let mut receipt_to_send = vec![];
            for receipt in self.receipts.read().expect(POISONED_LOCK_ERR).iter() {
                let mut locked_known_to = self.known_to.write().expect(POISONED_LOCK_ERR);
                match locked_known_to.get_mut(&receipt.get_hash()) {
                    Some(known_to) => {
                        if !known_to.contains(&their_authority_id) {
                            receipt_to_send.push(receipt.clone());
                            known_to.insert(their_authority_id);
                        }
                    }
                    None => {
                        receipt_to_send.push(receipt.clone());
                        let mut known_to = HashSet::new();
                        known_to.insert(their_authority_id);
                        locked_known_to.insert(receipt.get_hash(), known_to);
                    }
                }
            }
            if tx_to_send.is_empty() && receipt_to_send.is_empty() {
                continue;
            }
            let payload = ChainPayload::new(tx_to_send, receipt_to_send);
            result.push(PayloadGossip::new(
                block_index,
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
            let mut info_guard = self.transaction_info.write().expect(POISONED_LOCK_ERR);
            let mut remove_map = false;
            let sender = transaction.body.get_originator();
            if let Some(map) = guard.get_mut(&sender) {
                map.remove(&transaction.body.get_nonce());
                remove_map = map.is_empty();
            }
            if remove_map {
                guard.remove(&sender);
            }
            info_guard.remove(&transaction.get_hash());
        }
        for receipt in block.body.receipts.iter() {
            self.receipts.write().expect(POISONED_LOCK_ERR).remove(receipt);
        }
    }

    pub fn fetch_payload(&self, request: MissingPayloadRequest) -> MissingPayloadResponse {
        let transactions: Vec<_> = {
            let guard = self.transactions.read().expect(POISONED_LOCK_ERR);
            let info_guard = self.transaction_info.read().expect(POISONED_LOCK_ERR);
            request
                .transactions
                .into_iter()
                .filter_map(|hash| {
                    info_guard
                        .get(&hash)
                        .and_then(|(id, nonce)| guard.get(id).and_then(|map| map.get(nonce)))
                        .cloned()
                })
                .collect()
        };
        let receipts: Vec<_> = {
            let guard = self.receipts.read().expect(POISONED_LOCK_ERR);
            request.receipts.into_iter().filter_map(|hash| guard.get(&hash).cloned()).collect()
        };

        MissingPayloadResponse { transactions, receipts, snapshot_hash: request.snapshot_hash }
    }

    /// Add snapshot sent by peer to `pending_snapshots` if there are missing transactions or receipts.
    /// In this case, also request the missing transactions/receipts from the peer who sends the
    /// snapshot. Otherwise add the snapshot to `snapshots`.
    pub fn add_payload_snapshot(
        &self,
        authority_id: AuthorityId,
        snapshot: Snapshot,
    ) -> Option<MissingPayloadRequest> {
        if snapshot.is_empty() {
            return None;
        }
        let h = snapshot.get_hash();
        info!(
            target: "mempool",
            "Adding payload snapshot, authority={:?}, #tx={}, #r={}, hash={:?} received from {:?}",
            self.authority_id.read().expect(POISONED_LOCK_ERR).unwrap(),
            snapshot.transactions.len(),
            snapshot.receipts.len(),
            h,
            authority_id,
        );
        let request_transactions: Vec<_> = {
            let guard = self.transactions.read().expect(POISONED_LOCK_ERR);
            let info_guard = self.transaction_info.read().expect(POISONED_LOCK_ERR);
            snapshot
                .transactions
                .clone()
                .into_iter()
                .filter(|hash| {
                    info_guard
                        .get(hash)
                        .and_then(|(id, nonce)| guard.get(id).and_then(|map| map.get(&nonce)))
                        .is_none()
                })
                .collect()
        };
        let request_receipts: Vec<_> = {
            let guard = self.receipts.read().expect(POISONED_LOCK_ERR);
            snapshot.receipts.clone().into_iter().filter(|hash| !guard.contains(hash)).collect()
        };

        // if nothing to request, which is possible as pool may receive new
        // transactions while waiting for the snapshot response, return
        if request_transactions.is_empty() && request_receipts.is_empty() {
            self.snapshots.write().expect(POISONED_LOCK_ERR).insert(snapshot);
            return None;
        }
        self.pending_snapshots.write().expect(POISONED_LOCK_ERR).insert(snapshot);
        let request = MissingPayloadRequest {
            transactions: request_transactions,
            receipts: request_receipts,
            snapshot_hash: h,
        };
        Some(request)
    }
}

#[cfg(test)]
mod tests {
    use node_runtime::{test_utils::generate_test_chain_spec, Runtime};
    use primitives::hash::CryptoHash;
    use primitives::signer::InMemorySigner;
    use primitives::transaction::{SendMoneyTransaction, TransactionBody};
    use primitives::types::MerkleHash;
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;

    fn get_test_chain() -> (Arc<RwLock<ShardChainStorage>>, Arc<Trie>, Vec<Arc<InMemorySigner>>) {
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
        assert_eq!(pool.transaction_info.read().expect(POISONED_LOCK_ERR).len(), 1);
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
        assert_eq!(pool.transaction_info.read().expect(POISONED_LOCK_ERR).len(), 0);
        assert_eq!(pool.known_to.read().expect(POISONED_LOCK_ERR).len(), 0);
    }

    #[test]
    // adding the same transaction twice should not change known_to
    fn test_known_to() {
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
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.read().expect(POISONED_LOCK_ERR).len(), 1);
        assert_eq!(pool.known_to.read().expect(POISONED_LOCK_ERR).len(), 1);
    }

    #[test]
    // two mempools with different set of transactions. When one sends a snapshot
    // to the other, it should get a request for missing payload and respond
    // with the missing payload
    fn test_two_mempool_sync() {
        let (storage, trie, signers) = get_test_chain();
        let transactions: Vec<_> = (1..5)
            .map(|i| {
                TransactionBody::SendMoney(SendMoneyTransaction {
                    nonce: i,
                    originator: "alice.near".to_string(),
                    receiver: "bob.near".to_string(),
                    amount: i,
                })
                .sign(signers[0].clone())
            })
            .collect();
        let pool1 = Pool::new(signers[0].clone(), storage.clone(), trie.clone());
        for i in 0..3 {
            pool1.add_transaction(transactions[i].clone()).unwrap();
        }
        let pool2 = Pool::new(signers[0].clone(), storage, trie);
        for i in 1..4 {
            pool2.add_transaction(transactions[i].clone()).unwrap();
        }
        let snapshot_hash = pool1.snapshot_payload();
        let snapshot =
            pool1.snapshots.read().expect(POISONED_LOCK_ERR).get(&snapshot_hash).cloned().unwrap();
        let missing_payload_request = pool2.add_payload_snapshot(0, snapshot).unwrap();
        assert_eq!(
            missing_payload_request,
            MissingPayloadRequest {
                transactions: vec![transactions[0].get_hash()],
                receipts: vec![],
                snapshot_hash,
            }
        );

        let missing_payload_response = pool1.fetch_payload(missing_payload_request);
        assert_eq!(
            missing_payload_response,
            MissingPayloadResponse {
                transactions: vec![transactions[0].clone()],
                receipts: vec![],
                snapshot_hash,
            }
        );

        let hash = pool2.add_missing_payload(0, missing_payload_response).unwrap();
        assert_eq!(hash, snapshot_hash);
        assert!(pool2.snapshots.read().expect(POISONED_LOCK_ERR).contains(&snapshot_hash));
    }
}
