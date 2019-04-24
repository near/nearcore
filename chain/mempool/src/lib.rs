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
use primitives::crypto::signer::EDSigner;
use primitives::hash::CryptoHash;
use primitives::merkle::verify_path;
use primitives::transaction::{verify_transaction_signature, SignedTransaction};
use primitives::types::{AccountId, AuthorityId, BlockIndex};
use storage::{GenericStorage, ShardChainStorage, Trie, TrieUpdate};

pub mod payload_gossip;
use payload_gossip::PayloadGossip;

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Mempool that stores transactions and receipts for a chain
pub struct Pool {
    signer: Arc<EDSigner>,
    // transactions need to be grouped by account, and within each account,
    // ordered by nonce so that runtime will not ignore some of the transactions
    transactions: HashMap<AccountId, BTreeMap<u64, SignedTransaction>>,
    // store the map (transaction hash -> (account_id, nonce))
    // that allows us to lookup transaction by hash
    transaction_info: HashMap<CryptoHash, (AccountId, u64)>,
    receipts: HashSet<ReceiptBlock>,
    storage: Arc<RwLock<ShardChainStorage>>,
    trie: Arc<Trie>,
    state_viewer: TrieViewer,
    /// List of requested snapshots that are unblocked and can be confirmed.
    snapshots: HashSet<Snapshot>,
    /// List of requested snapshots that can't be fetched yet.
    pending_snapshots: HashSet<Snapshot>,
    /// Given MemPool's authority id.
    pub authority_id: Option<AuthorityId>,
    /// Number of authorities currently.
    num_authorities: Option<usize>,
    /// Map from hash of tx/receipt to hashset of authorities it is known.
    known_to: HashMap<CryptoHash, HashSet<AuthorityId>>,
    /// Maximum block size
    max_block_size: u32,
}

impl Pool {
    pub fn new(
        signer: Arc<EDSigner>,
        storage: Arc<RwLock<ShardChainStorage>>,
        trie: Arc<Trie>,
        max_block_size: u32,
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
            max_block_size,
        }
    }

    pub fn reset(&mut self, authority_id: Option<AuthorityId>, num_authorities: Option<usize>) {
        self.authority_id = authority_id;
        self.num_authorities = num_authorities;
        self.snapshots.clear();
        self.pending_snapshots.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty() && self.receipts.is_empty()
    }

    pub fn len(&self) -> usize {
        self.transactions.values().map(BTreeMap::len).sum::<usize>() + self.receipts.len()
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

    pub fn add_transaction(&mut self, transaction: SignedTransaction) -> Result<(), String> {
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

        let state_update = self.get_state_update();
        let originator = transaction.body.get_originator();
        let public_keys =
            self.state_viewer.get_public_keys_for_account(&state_update, &originator)?;
        if !verify_transaction_signature(&transaction, &public_keys) {
            return Err(format!(
                "transaction not signed with a public key of originator {:?}",
                originator
            ));
        }
        self.known_to.entry(transaction.get_hash()).or_insert_with(HashSet::new);
        self.transaction_info.insert(transaction.get_hash(), (sender.clone(), nonce));
        self.transactions.entry(sender).or_insert_with(BTreeMap::new).insert(nonce, transaction);
        Ok(())
    }

    pub fn add_receipt(&mut self, receipt: ReceiptBlock) -> Result<(), String> {
        if let Ok(Some(_)) =
            self.storage.write().expect(POISONED_LOCK_ERR).transaction_address(&receipt.get_hash())
        {
            return Ok(());
        }
        if !verify_path(receipt.header.body.receipt_merkle_root, &receipt.path, &receipt.receipts) {
            return Err("Invalid receipt block".to_string());
        }
        self.receipts.insert(receipt);
        Ok(())
    }

    /// Add requested transactions and receipts from the peer who sends the missing payload response.
    /// Update `known_to` to include the include the authority id of the peer.
    /// Move snapshot from `pending_snapshots` to `snapshots`.
    pub fn add_missing_payload(
        &mut self,
        author: AuthorityId,
        response: MissingPayloadResponse,
    ) -> Result<CryptoHash, String> {
        let snapshot = self.pending_snapshots.take(&response.snapshot_hash);
        match snapshot {
            Some(snapshot) => {
                for transaction in response.transactions {
                    let hash = transaction.get_hash();
                    self.add_transaction(transaction)?;
                    self.known_to.entry(hash).or_insert_with(HashSet::new).insert(author);
                }
                for receipt in response.receipts {
                    let hash = receipt.get_hash();
                    self.add_receipt(receipt)?;
                    self.known_to.entry(hash).or_insert_with(HashSet::new).insert(author);
                }
                for transaction in snapshot.transactions.iter() {
                    if !self.transaction_info.contains_key(transaction) {
                        return Err("Some transaction is missing from the response".to_string());
                    }
                }
                for receipt in snapshot.receipts.iter() {
                    if !self.receipts.contains(receipt) {
                        return Err("Some receipt is missing from the response".to_string());
                    }
                }
                let hash = snapshot.get_hash();
                self.snapshots.insert(snapshot);
                Ok(hash)
            }
            None => Err("snapshot does not exist".to_string()),
        }
    }

    /// When we receive a payload gossip from some peer, we add transactions and
    /// receipts to the pool and update `known_to` to include the peer.
    pub fn add_payload_with_author(
        &mut self,
        payload: ChainPayload,
        author: AuthorityId,
    ) -> Result<(), String> {
        for transaction in payload.transactions {
            let hash = transaction.get_hash();
            self.add_transaction(transaction)?;
            self.known_to.entry(hash).or_insert_with(HashSet::new).insert(author);
        }
        for receipt in payload.receipts {
            let hash = receipt.get_hash();
            self.add_receipt(receipt)?;
            self.known_to.entry(hash).or_insert_with(HashSet::new).insert(author);
        }
        Ok(())
    }

    fn get_transaction(&self, hash: CryptoHash) -> Option<SignedTransaction> {
        self.transaction_info.get(&hash).and_then(|(id, nonce)| {
            self.transactions.get(id).and_then(|map| map.get(&nonce)).cloned()
        })
    }

    pub fn snapshot_payload(&mut self) -> CryptoHash {
        // strategy for snapshot: take all receipts and limit number of transactions
        let receipt_hashes: Vec<_> = self.receipts.iter().map(ReceiptBlock::get_hash).collect();
        let transaction_hashes: Vec<_> = self
            .transactions
            .values()
            .flat_map(BTreeMap::values)
            .take(self.max_block_size as usize)
            .map(SignedTransaction::get_hash)
            .collect();
        let snapshot = Snapshot::new(transaction_hashes, receipt_hashes);
        if snapshot.is_empty() {
            return CryptoHash::default();
        }
        let h = snapshot.get_hash();
        info!(
            target: "mempool", "Snapshotting payload, authority={:?}, #tx={}, #r={}, hash={:?}",
            self.authority_id,
            snapshot.transactions.len(),
            snapshot.receipts.len(),
            h,
        );
        self.snapshots.insert(snapshot);
        h
    }

    pub fn contains_payload_snapshot(&self, hash: &CryptoHash) -> bool {
        // if the payload is empty, return true
        if hash == &CryptoHash::default() {
            return true;
        }
        self.snapshots.contains(hash)
    }

    fn snapshot_to_payload(&self, snapshot: Snapshot) -> ChainPayload {
        let transactions = snapshot
            .transactions
            .into_iter()
            .filter_map(|hash| self.get_transaction(hash))
            .collect();
        let receipts = snapshot
            .receipts
            .into_iter()
            .filter_map(|hash| self.receipts.get(&hash).cloned())
            .collect();
        ChainPayload::new(transactions, receipts)
    }

    pub fn pop_payload_snapshot(&mut self, hash: &CryptoHash) -> Option<ChainPayload> {
        if hash == &CryptoHash::default() {
            return Some(ChainPayload::default());
        }
        let snapshot = self.snapshots.take(hash);
        if let Some(ref p) = snapshot {
            info!(
                target: "mempool", "Popping snapshot, authority={:?}, #tx={}, #r={}, hash={:?}",
                self.authority_id,
                p.transactions.len(),
                p.receipts.len(),
                hash,
            );
        } else {
            info!(
                target: "mempool", "Failed to pop snapshot, authority={:?}, hash={:?}",
                self.authority_id.unwrap(),
                hash,
            );
        }
        Some(self.snapshot_to_payload(snapshot?))
    }

    /// respond to snapshot request
    pub fn on_snapshot_request(&self, hash: CryptoHash) -> Result<Snapshot, String> {
        if let Some(snapshot) = self.snapshots.get(&hash) {
            Ok(snapshot.clone())
        } else {
            Err(format!("No such payload with hash {}", hash))
        }
    }

    /// Prepares payload to gossip to peer authority.
    pub fn prepare_payload_gossip(&mut self, block_index: BlockIndex) -> Vec<PayloadGossip> {
        if self.authority_id.is_none() {
            return vec![];
        }
        let mut result = vec![];
        let authority_id = self.authority_id.unwrap();
        for their_authority_id in 0..self.num_authorities.unwrap_or(0) {
            if their_authority_id == authority_id {
                continue;
            }
            let mut tx_to_send = vec![];
            for tx in self.transactions.values().flat_map(BTreeMap::values) {
                match self.known_to.get_mut(&tx.get_hash()) {
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
                        self.known_to.insert(tx.get_hash(), known_to);
                    }
                }
            }
            let mut receipt_to_send = vec![];
            for receipt in self.receipts.iter() {
                match self.known_to.get_mut(&receipt.get_hash()) {
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
                        self.known_to.insert(receipt.get_hash(), known_to);
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

    pub fn import_block(&mut self, block: &SignedShardBlock) {
        for transaction in block.body.transactions.iter() {
            self.known_to.remove(&transaction.get_hash());
            let mut remove_map = false;
            let sender = transaction.body.get_originator();
            if let Some(map) = self.transactions.get_mut(&sender) {
                map.remove(&transaction.body.get_nonce());
                remove_map = map.is_empty();
            }
            if remove_map {
                self.transactions.remove(&sender);
            }
            self.transaction_info.remove(&transaction.get_hash());
        }
        for receipt in block.body.receipts.iter() {
            self.receipts.remove(receipt);
        }
    }

    /// Tries to fetch the missing payload, but might not be able to due to new blocks being imported.
    /// If that's the case, return None.
    pub fn fetch_payload(&self, request: MissingPayloadRequest) -> Option<MissingPayloadResponse> {
        let transactions = request
            .transactions
            .into_iter()
            .map(|hash| self.get_transaction(hash))
            .collect::<Option<Vec<_>>>()?;
        let receipts = request
            .receipts
            .into_iter()
            .map(|hash| self.receipts.get(&hash).cloned())
            .collect::<Option<Vec<_>>>()?;
        Some(MissingPayloadResponse {
            transactions,
            receipts,
            snapshot_hash: request.snapshot_hash,
        })
    }

    /// Add snapshot sent by peer to `pending_snapshots` if there are missing transactions or receipts.
    /// In this case, also request the missing transactions/receipts from the peer who sends the
    /// snapshot. Otherwise add the snapshot to `snapshots`.
    pub fn add_payload_snapshot(
        &mut self,
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
            self.authority_id.unwrap(),
            snapshot.transactions.len(),
            snapshot.receipts.len(),
            h,
            authority_id,
        );
        let request_transactions: Vec<_> = snapshot
            .transactions
            .clone()
            .into_iter()
            .filter(|hash| {
                self.transaction_info
                    .get(hash)
                    .and_then(|(id, nonce)| {
                        self.transactions.get(id).and_then(|map| map.get(&nonce))
                    })
                    .is_none()
            })
            .collect();
        let request_receipts: Vec<_> = snapshot
            .receipts
            .clone()
            .into_iter()
            .filter(|hash| !self.receipts.contains(hash))
            .collect();

        // if nothing to request, which is possible as pool may receive new
        // transactions while waiting for the snapshot response, return
        if request_transactions.is_empty() && request_receipts.is_empty() {
            self.snapshots.insert(snapshot);
            return None;
        }
        self.pending_snapshots.insert(snapshot);
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
    use node_runtime::chain_spec::{AuthorityRotation, ChainSpec, DefaultIdType};
    use node_runtime::Runtime;
    use primitives::crypto::signer::InMemorySigner;
    use primitives::hash::CryptoHash;
    use primitives::transaction::{SendMoneyTransaction, TransactionBody};
    use primitives::types::MerkleHash;
    use rand::prelude::SliceRandom;
    use rand::thread_rng;
    use storage::test_utils::create_beacon_shard_storages;

    use super::*;
    const BLOCK_SIZE: u32 = 100;

    fn get_test_chain() -> (Arc<RwLock<ShardChainStorage>>, Arc<Trie>, Vec<Arc<InMemorySigner>>) {
        let (chain_spec, signers) = ChainSpec::testing_spec(
            DefaultIdType::Named,
            3,
            1,
            AuthorityRotation::ThresholdedProofOfStake { epoch_length: 2, num_seats_per_slot: 1 },
        );
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
    /// Importing blocks to mempool should clear the relevant containers.
    fn test_import_block() {
        let (storage, trie, signers) = get_test_chain();
        let mut pool = Pool::new(signers[0].clone(), storage, trie, BLOCK_SIZE);
        let transaction = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            originator: "alice.near".to_string(),
            receiver: "bob.near".to_string(),
            amount: 1,
        })
        .sign(&*signers[0]);
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.len(), 1);
        assert_eq!(pool.transaction_info.len(), 1);
        assert_eq!(pool.known_to.len(), 1);
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
        assert_eq!(pool.transactions.len(), 0);
        assert_eq!(pool.transaction_info.len(), 0);
        assert_eq!(pool.known_to.len(), 0);
    }

    #[test]
    // adding the same transaction twice should not change known_to
    fn test_known_to() {
        let (storage, trie, signers) = get_test_chain();
        let mut pool = Pool::new(signers[0].clone(), storage, trie, BLOCK_SIZE);
        let transaction = TransactionBody::SendMoney(SendMoneyTransaction {
            nonce: 1,
            originator: "alice.near".to_string(),
            receiver: "bob.near".to_string(),
            amount: 1,
        })
        .sign(&*signers[0]);
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.len(), 1);
        assert_eq!(pool.known_to.len(), 1);
        pool.add_transaction(transaction.clone()).unwrap();
        assert_eq!(pool.transactions.len(), 1);
        assert_eq!(pool.known_to.len(), 1);
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
                .sign(&*signers[0])
            })
            .collect();
        let mut pool1 = Pool::new(signers[0].clone(), storage.clone(), trie.clone(), BLOCK_SIZE);
        for i in 0..3 {
            pool1.add_transaction(transactions[i].clone()).unwrap();
        }
        let mut pool2 = Pool::new(signers[0].clone(), storage, trie, BLOCK_SIZE);
        for i in 1..4 {
            pool2.add_transaction(transactions[i].clone()).unwrap();
        }
        let snapshot_hash = pool1.snapshot_payload();
        let snapshot = pool1.snapshots.get(&snapshot_hash).cloned().unwrap();
        let missing_payload_request = pool2.add_payload_snapshot(0, snapshot).unwrap();
        assert_eq!(
            missing_payload_request,
            MissingPayloadRequest {
                transactions: vec![transactions[0].get_hash()],
                receipts: vec![],
                snapshot_hash,
            }
        );

        let missing_payload_response = pool1.fetch_payload(missing_payload_request).unwrap();
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
        assert!(pool2.snapshots.contains(&snapshot_hash));
        assert!(pool2.pending_snapshots.is_empty())
    }

    #[test]
    /// When one pool requests payload from another and the other pool
    /// happens to import a block, there is no response.
    fn test_missing_payload() {
        let (storage, trie, signers) = get_test_chain();
        let transactions: Vec<_> = (1..5)
            .map(|i| {
                TransactionBody::SendMoney(SendMoneyTransaction {
                    nonce: i,
                    originator: "alice.near".to_string(),
                    receiver: "bob.near".to_string(),
                    amount: i,
                })
                .sign(&*signers[0])
            })
            .collect();
        let mut pool1 = Pool::new(signers[0].clone(), storage.clone(), trie.clone(), BLOCK_SIZE);
        for i in 0..3 {
            pool1.add_transaction(transactions[i].clone()).unwrap();
        }
        let mut pool2 = Pool::new(signers[0].clone(), storage, trie, BLOCK_SIZE);
        for i in 1..4 {
            pool2.add_transaction(transactions[i].clone()).unwrap();
        }
        let snapshot_hash = pool1.snapshot_payload();
        let snapshot = pool1.snapshots.get(&snapshot_hash).cloned().unwrap();
        let missing_payload_request = pool2.add_payload_snapshot(0, snapshot).unwrap();
        assert_eq!(
            missing_payload_request,
            MissingPayloadRequest {
                transactions: vec![transactions[0].get_hash()],
                receipts: vec![],
                snapshot_hash,
            }
        );

        // pool1 imports a block
        let block = SignedShardBlock::new(
            0,
            1,
            CryptoHash::default(),
            CryptoHash::default(),
            transactions.clone(),
            vec![],
            CryptoHash::default(),
        );
        pool1.import_block(&block);

        let missing_payload_response = pool1.fetch_payload(missing_payload_request);
        assert!(missing_payload_response.is_none());
    }

    #[test]
    /// Add transactions of nonce from 1..10 in random order. Check that mempool
    /// orders them correctly.
    fn test_order_nonce() {
        let (storage, trie, signers) = get_test_chain();
        let mut transactions: Vec<_> = (1..10)
            .map(|i| {
                TransactionBody::SendMoney(SendMoneyTransaction {
                    nonce: i,
                    originator: "alice.near".to_string(),
                    receiver: "bob.near".to_string(),
                    amount: i,
                })
                .sign(&*signers[0])
            })
            .collect();
        let mut pool = Pool::new(signers[0].clone(), storage.clone(), trie.clone(), BLOCK_SIZE);
        let mut rng = thread_rng();
        transactions.shuffle(&mut rng);
        for tx in transactions {
            pool.add_transaction(tx).unwrap();
        }
        let snapshot_hash = pool.snapshot_payload();
        let payload = pool.pop_payload_snapshot(&snapshot_hash).unwrap();
        let nonces: Vec<u64> = payload.transactions.iter().map(|tx| tx.body.get_nonce()).collect();
        assert_eq!(nonces, (1..10).collect::<Vec<u64>>())
    }

    #[test]
    /// Gossip payload from one mempool to another. Check that the payload is received and
    /// `known_to` is updated correctly in both mempools.
    fn test_payload_gossip() {
        let (storage, trie, signers) = get_test_chain();
        let transactions: Vec<_> = (1..5)
            .map(|i| {
                TransactionBody::SendMoney(SendMoneyTransaction {
                    nonce: i,
                    originator: "alice.near".to_string(),
                    receiver: "bob.near".to_string(),
                    amount: i,
                })
                .sign(&*signers[0])
            })
            .collect();
        let mut pool1 = Pool::new(signers[0].clone(), storage.clone(), trie.clone(), BLOCK_SIZE);
        pool1.num_authorities = Some(2);
        pool1.authority_id = Some(0);
        for tx in transactions.iter() {
            pool1.add_transaction(tx.clone()).unwrap();
        }
        let mut pool2 = Pool::new(signers[1].clone(), storage, trie, BLOCK_SIZE);
        pool2.num_authorities = Some(2);
        pool2.authority_id = Some(1);
        let mut payload_gossip = pool1.prepare_payload_gossip(1);

        assert_eq!(payload_gossip.len(), 1);
        for tx in transactions.iter() {
            let known_to = pool1.known_to.get(&tx.get_hash()).unwrap();
            assert_eq!(known_to.len(), 1);
            assert!(known_to.contains(&1));
        }

        let payload_gossip = payload_gossip.pop().unwrap();
        pool2.add_payload_with_author(payload_gossip.payload, 0).unwrap();
        assert_eq!(pool2.len(), 4);

        for tx in transactions.iter() {
            let known_to = pool2.known_to.get(&tx.get_hash()).unwrap();
            assert_eq!(known_to.len(), 1);
            assert!(known_to.contains(&0));
        }
    }

    #[test]
    /// Add BLOCK_SIZE + 1 transactions into mempool and check that
    /// the produced chain payload has size BLOCK_SIZE
    fn test_over_block_size() {
        let (storage, trie, signers) = get_test_chain();
        let transactions: Vec<_> = (1..BLOCK_SIZE as usize + 2)
            .map(|i| {
                TransactionBody::SendMoney(SendMoneyTransaction {
                    nonce: i as u64,
                    originator: "alice.near".to_string(),
                    receiver: "bob.near".to_string(),
                    amount: 1,
                })
                .sign(&*signers[0])
            })
            .collect();
        let mut pool = Pool::new(signers[0].clone(), storage.clone(), trie.clone(), BLOCK_SIZE);
        for transaction in transactions {
            pool.add_transaction(transaction).unwrap();
        }
        assert_eq!(pool.len(), BLOCK_SIZE as usize + 1);
        let snapshot_hash = pool.snapshot_payload();
        let payload = pool.pop_payload_snapshot(&snapshot_hash).unwrap();
        assert_eq!(payload.transactions.len(), BLOCK_SIZE as usize);
        assert!(payload.receipts.is_empty());
    }
}
