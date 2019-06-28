use std::collections::HashMap;
use std::iter::Peekable;
use std::sync::{Arc, Mutex};

use bigint::{H256, H64, U256};
use kvdb::DBValue;

use near_primitives::hash::CryptoHash;
use near_primitives::transaction::{
    AsyncCall, Callback, CallbackInfo, ReceiptBody, ReceiptTransaction,
};
use near_primitives::types::{AccountId, Balance, CallbackId, Nonce, PromiseId, ReceiptId};
use near_primitives::utils::{create_nonce_with_nonce, key_for_account, key_for_callback};
use near_store::set;
use near_store::{TrieUpdate, TrieUpdateIterator};
use wasm::ext::{Error as ExtError, External, Result as ExtResult};

use crate::ethereum::EthashProvider;
use crate::POISONED_LOCK_ERR;
use near_primitives::crypto::signature::PublicKey;

pub const ACCOUNT_DATA_SEPARATOR: &[u8; 1] = b",";

pub struct RuntimeExt<'a> {
    trie_update: &'a mut TrieUpdate,
    storage_prefix: Vec<u8>,
    pub receipts: HashMap<ReceiptId, ReceiptTransaction>,
    pub callbacks: HashMap<CallbackId, Callback>,
    account_id: &'a AccountId,
    refund_account_id: &'a AccountId,
    nonce: Nonce,
    transaction_hash: &'a CryptoHash,
    iters: HashMap<u32, Peekable<TrieUpdateIterator<'a>>>,
    last_iter_id: u32,
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
        let mut prefix = key_for_account(account_id);
        prefix.append(&mut ACCOUNT_DATA_SEPARATOR.to_vec());
        RuntimeExt {
            trie_update,
            storage_prefix: prefix,
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
            set(self.trie_update, key_for_callback(&id), &callback);
        }
    }
}

impl<'a> External for RuntimeExt<'a> {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        let storage_key = self.create_storage_key(key);
        Ok(self.trie_update.set(storage_key, DBValue::from_slice(value)))
    }

    fn storage_get(&self, key: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        let storage_key = self.create_storage_key(key);
        let value = self.trie_update.get(&storage_key);
        Ok(value.map(|buf| buf.to_vec()))
    }

    fn storage_remove(&mut self, key: &[u8]) -> ExtResult<Option<Vec<u8>>> {
        let storage_key = self.create_storage_key(key);
        Ok(self.trie_update.remove(&storage_key))
    }

    fn storage_iter(&mut self, prefix: &[u8]) -> ExtResult<u32> {
        self.iters.insert(
            self.last_iter_id,
            // It is safe to insert an iterator of lifetime 'a into a HashMap of lifetime 'a.
            // We just could not convince Rust that `self.trie_update` has lifetime 'a as it
            // shrinks the lifetime to the lifetime of `self`.
            unsafe { &mut *(self.trie_update as *mut TrieUpdate) }
                .iter(&self.create_storage_key(prefix))
                .map_err(|_| ExtError::TrieIteratorError)?
                .peekable(),
        );
        self.last_iter_id += 1;
        Ok(self.last_iter_id - 1)
    }

    fn storage_range(&mut self, start: &[u8], end: &[u8]) -> ExtResult<u32> {
        self.iters.insert(
            self.last_iter_id,
            unsafe { &mut *(self.trie_update as *mut TrieUpdate) }
                .range(&self.storage_prefix, start, end)
                .map_err(|_| ExtError::TrieIteratorError)?
                .peekable(),
        );
        self.last_iter_id += 1;
        Ok(self.last_iter_id - 1)
    }

    fn storage_iter_next(&mut self, id: u32) -> ExtResult<Option<Vec<u8>>> {
        let result = match self.iters.get_mut(&id) {
            Some(iter) => iter.next(),
            None => return Err(ExtError::TrieIteratorMissing),
        };
        if result.is_none() {
            self.iters.remove(&id);
        }
        Ok(result.map(|x| x[self.storage_prefix.len()..].to_vec()))
    }

    fn storage_iter_peek(&mut self, id: u32) -> ExtResult<Option<Vec<u8>>> {
        let result = match self.iters.get_mut(&id) {
            Some(iter) => iter.peek().cloned(),
            None => return Err(ExtError::TrieIteratorMissing),
        };
        Ok(result.map(|x| x[self.storage_prefix.len()..].to_vec()))
    }

    fn storage_iter_remove(&mut self, id: u32) {
        self.iters.remove(&id);
    }

    fn promise_create(
        &mut self,
        account_id: AccountId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
    ) -> ExtResult<PromiseId> {
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
        Ok(promise_id)
    }

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
    ) -> ExtResult<PromiseId> {
        let callback_id = self.create_nonce();
        let receipt_ids = match promise_id {
            PromiseId::Receipt(r) => vec![r],
            PromiseId::Joiner(rs) => rs,
            PromiseId::Callback(_) => return Err(ExtError::WrongPromise),
        };
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
                _ => return Err(ExtError::PromiseIdNotFound),
            };
            match receipt.body {
                ReceiptBody::NewCall(ref mut async_call) => {
                    let callback_info = CallbackInfo::new(
                        callback_id.as_ref().to_vec(),
                        index,
                        self.account_id.clone(),
                    );
                    match async_call.callback {
                        Some(_) => return Err(ExtError::PromiseAlreadyHasCallback),
                        None => {
                            async_call.callback = Some(callback_info);
                        }
                    }
                }
                _ => {
                    return Err(ExtError::WrongPromise);
                }
            }
        }
        self.callbacks.insert(callback_id.as_ref().to_vec(), callback);
        Ok(PromiseId::Callback(callback_id.as_ref().to_vec()))
    }

    fn check_ethash(
        &mut self,
        block_number: u64,
        header_hash: &[u8],
        nonce: u64,
        mix_hash: &[u8],
        difficulty: u64,
    ) -> bool {
        self.ethash_provider.lock().expect(POISONED_LOCK_ERR).check_ethash(
            U256::from(block_number),
            H256::from(header_hash),
            H64::from(nonce),
            H256::from(mix_hash),
            U256::from(difficulty),
        )
    }
}
