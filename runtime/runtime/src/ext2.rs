use std::collections::{HashMap, HashSet};
use std::iter::Peekable;
use std::sync::{Arc, Mutex};

use kvdb::DBValue;

use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    AsyncCall, Callback, CallbackInfo, ReceiptBody, ReceiptTransaction,
};
use near_primitives::types::{AccountId, CallbackId, Nonce, PromiseId, ReceiptId};
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

    // A temporary hack to make new bindings work with old `runtime` crate.
    pub receipt_idx_to_promise_id: HashMap<u64, PromiseId>,
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
            receipt_idx_to_promise_id: HashMap::new(),
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
        Ok(self.storage_get(key)?.is_some())
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
        _gas: u64,
    ) -> Result<u64, ExternalError> {
        // TODO: This code should be replaced according to https://github.com/nearprotocol/NEPs/pull/8
        // Currently we only make this code compatible with the old runtime crate.
        if receipt_indices.is_empty() {
            // The case of old `promise_create`.
            let nonce = self.create_nonce();
            let receipt = ReceiptTransaction::new(
                self.account_id.clone(),
                account_id,
                nonce,
                ReceiptBody::NewCall(AsyncCall::new(
                    method_name,
                    arguments,
                    amount,
                    self.refund_account_id.clone(),
                    self.originator_id.clone(),
                    self.public_key.clone(),
                )),
            );
            let promise_id = PromiseId::Receipt(nonce.as_ref().to_vec());
            self.receipts.insert(nonce.as_ref().to_vec(), receipt);
            let new_receipt_idx = self.receipt_idx_to_promise_id.len() as u64;
            self.receipt_idx_to_promise_id.insert(new_receipt_idx, promise_id);
            Ok(new_receipt_idx)
        } else {
            // The case of old `promise_then`, i.e. callback.
            let callback_id = self.create_nonce();
            let receipt_ids: Vec<_> = receipt_indices
                .iter()
                .map(|idx| {
                    if let PromiseId::Receipt(r) = &self.receipt_idx_to_promise_id[idx] {
                        r.clone()
                    } else {
                        unreachable!()
                    }
                })
                .collect();
            let mut callback = Callback::new(
                method_name,
                arguments,
                amount,
                self.refund_account_id.clone(),
                self.originator_id.clone(),
                self.public_key.clone(),
            );
            callback.results.resize(receipt_ids.len(), None);
            for (index, receipt_id) in receipt_ids.iter().enumerate() {
                let receipt = match self.receipts.get_mut(receipt_id) {
                    Some(r) => r,
                    _ => return Err(ExternalError::InvalidReceiptIndex),
                };
                match receipt.body {
                    ReceiptBody::NewCall(ref mut async_call) => {
                        let callback_info = CallbackInfo::new(
                            callback_id.as_ref().to_vec(),
                            index,
                            self.account_id.clone(),
                        );
                        match async_call.callback {
                            Some(_) => return Err(ExternalError::InvalidReceiptIndex),
                            None => {
                                async_call.callback = Some(callback_info);
                            }
                        }
                    }
                    _ => {
                        return Err(ExternalError::InvalidReceiptIndex);
                    }
                }
            }
            self.callbacks.insert(callback_id.as_ref().to_vec(), callback);
            let new_receipt_idx = self.receipt_idx_to_promise_id.len() as u64;
            self.receipt_idx_to_promise_id
                .insert(new_receipt_idx, PromiseId::Callback(callback_id.as_ref().to_vec()));
            Ok(new_receipt_idx)
        }
    }

    fn storage_usage(&self) -> u64 {
        unimplemented!()
    }
}
