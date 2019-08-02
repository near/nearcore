use std::collections::{HashMap, HashSet};
use std::iter::Peekable;
use std::sync::{Arc, Mutex};

use bigint::{H256, H64, U256};
use kvdb::DBValue;

use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{Callback, ReceiptTransaction};
use near_primitives::types::{AccountId, CallbackId, Nonce, ReceiptId};
use near_primitives::utils::{create_nonce_with_nonce, prefix_for_data};
use near_store::{set_callback, TrieUpdate, TrieUpdateIterator};
use near_vm_logic::{External, ExternalError};

use crate::ethereum::EthashProvider;
use near_primitives::crypto::signature::PublicKey;

pub struct RuntimeExt<'a> {
    trie_update: &'a mut TrieUpdate,
    storage_prefix: Vec<u8>,
    pub receipts: HashMap<ReceiptId, ReceiptTransaction>,
    pub callbacks: HashMap<CallbackId, Callback>,
    account_id: &'a AccountId,
    refund_account_id: &'a AccountId,
    nonce: Nonce,
    transaction_hash: &'a CryptoHash,
    iters: HashMap<u64, Peekable<TrieUpdateIterator<'a>>>,
    last_iter_id: u64,
    ethash_provider: Arc<Mutex<EthashProvider>>,
    originator_id: &'a AccountId,
    public_key: &'a PublicKey,
}

impl<'a> RuntimeExt<'a> {
    pub fn new(
        trie_update: &'a mut TrieUpdate,
        account_id: &'a AccountId,
        refund_account_id: &'a AccountId,
        transaction_hash: &'a CryptoHash,
        ethash_provider: Arc<Mutex<EthashProvider>>,
        originator_id: &'a AccountId,
        public_key: &'a PublicKey,
    ) -> Self {
        RuntimeExt {
            trie_update,
            storage_prefix: prefix_for_data(account_id),
            receipts: HashMap::new(),
            callbacks: HashMap::new(),
            account_id,
            refund_account_id,
            nonce: 0,
            transaction_hash,
            iters: HashMap::new(),
            last_iter_id: 0,
            ethash_provider,
            originator_id,
            public_key,
        }
    }

    pub fn create_storage_key(&self, key: &[u8]) -> Vec<u8> {
        let mut storage_key = self.storage_prefix.clone();
        storage_key.extend_from_slice(key);
        storage_key
    }

    pub fn create_nonce(&mut self) -> CryptoHash {
        let nonce = create_nonce_with_nonce(self.transaction_hash, self.nonce);
        self.nonce += 1;
        nonce
    }

    pub fn get_receipts(&mut self) -> Vec<ReceiptTransaction> {
        let mut vec: Vec<ReceiptTransaction> = self.receipts.drain().map(|(_, v)| v).collect();
        vec.sort_by_key(|a| a.nonce);
        vec
    }

    /// write callbacks to stateUpdate
    pub fn flush_callbacks(&mut self) {
        for (id, callback) in self.callbacks.drain() {
            set_callback(self.trie_update, &id, &callback);
        }
    }
}

impl<'a> External for RuntimeExt<'a> {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        let storage_key = self.create_storage_key(key);
        Ok(self.trie_update.set(storage_key, DBValue::from_slice(value)))
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        let storage_key = self.create_storage_key(key);
        let value = self.trie_update.get(&storage_key);
        Ok(value.map(|buf| buf.to_vec()))
    }

    fn storage_remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        let storage_key = self.create_storage_key(key);
        Ok(self.trie_update.remove(&storage_key))
    }

    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool, ExternalError> {
        unimplemented!()
    }

    fn storage_iter(&mut self, prefix: &[u8]) -> Result<u64, ExternalError> {
        self.iters.insert(
            self.last_iter_id,
            // Danger: we're creating a read reference to trie_update while still
            // having a mutable reference.
            // Any function that mutates trie_update must drop all existing iterators first.
            unsafe { &*(self.trie_update as *const TrieUpdate) }
                .iter(&self.create_storage_key(prefix))
                // TODO(#1131): if storage fails we actually want to abort the block rather than panic in the contract.
                .expect("Error reading from storage")
                .peekable(),
        );
        self.last_iter_id += 1;
        Ok(self.last_iter_id - 1)
    }

    fn storage_range(&mut self, start: &[u8], end: &[u8]) -> Result<u64, ExternalError> {
        self.iters.insert(
            self.last_iter_id,
            unsafe { &mut *(self.trie_update as *mut TrieUpdate) }
                .range(&self.storage_prefix, start, end)
                .expect("Error reading from storage")
                .peekable(),
        );
        self.last_iter_id += 1;
        Ok(self.last_iter_id - 1)
    }

    fn storage_iter_next(
        &mut self,
        iterator_idx: u64,
    ) -> Result<Option<(Vec<u8>, Vec<u8>)>, ExternalError> {
        let result = match self.iters.get_mut(&iterator_idx) {
            Some(iter) => iter.next(),
            None => return Err(ExternalError::InvalidIteratorIndex),
        };
        if result.is_none() {
            self.iters.remove(&iterator_idx);
        }
        Ok(result.map(|key| {
            (
                key[self.storage_prefix.len()..].to_vec(),
                self.trie_update.get(&key).expect("key is guaranteed to be there").into_vec(),
            )
        }))
    }

    fn storage_iter_drop(&mut self, iterator_idx: u64) -> Result<(), ExternalError> {
        self.iters.remove(&iterator_idx);
        Ok(())
    }

    fn receipt_create(
        &mut self,
        receipt_indices: HashSet<u64>,
        account_id: String,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: u128,
        gas: u64,
    ) -> Result<u64, ExternalError> {
        unimplemented!()
    }

    fn storage_usage(&self) -> u64 {
        unimplemented!()
    }
}
