use std::collections::HashMap;
use std::iter::Peekable;

use kvdb::DBValue;

use near_crypto::PublicKey;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, DataReceiver, Receipt, ReceiptEnum};
use near_primitives::transaction::{Action, FunctionCallAction};
use near_primitives::types::{AccountId, Balance};
use near_primitives::utils::{create_nonce_with_nonce, prefix_for_data};
use near_store::{TrieUpdate, TrieUpdateIterator};
use near_vm_logic::{External, ExternalError};

pub struct RuntimeExt<'a> {
    trie_update: &'a mut TrieUpdate,
    storage_prefix: Vec<u8>,
    action_receipts: Vec<(AccountId, ActionReceipt)>,
    iters: HashMap<u64, Peekable<TrieUpdateIterator<'a>>>,
    last_iter_id: u64,
    signer_id: &'a AccountId,
    signer_public_key: &'a PublicKey,
    gas_price: Balance,
    base_data_id: &'a CryptoHash,
    data_count: u64,
}

impl<'a> RuntimeExt<'a> {
    pub fn new(
        trie_update: &'a mut TrieUpdate,
        account_id: &'a AccountId,
        signer_id: &'a AccountId,
        signer_public_key: &'a PublicKey,
        gas_price: Balance,
        base_data_id: &'a CryptoHash,
    ) -> Self {
        RuntimeExt {
            trie_update,
            storage_prefix: prefix_for_data(account_id),
            action_receipts: vec![],
            iters: HashMap::new(),
            last_iter_id: 0,
            signer_id,
            signer_public_key,
            gas_price,
            base_data_id,
            data_count: 0,
        }
    }

    pub fn create_storage_key(&self, key: &[u8]) -> Vec<u8> {
        let mut storage_key = self.storage_prefix.clone();
        storage_key.extend_from_slice(key);
        storage_key
    }

    fn new_data_id(&mut self) -> CryptoHash {
        let data_id = create_nonce_with_nonce(&self.base_data_id, self.data_count);
        self.data_count += 1;
        data_id
    }

    pub fn into_receipts(self, predecessor_id: &AccountId) -> Vec<Receipt> {
        self.action_receipts
            .into_iter()
            .map(|(receiver_id, action_receipt)| Receipt {
                predecessor_id: predecessor_id.clone(),
                receiver_id,
                receipt_id: CryptoHash::default(),
                receipt: ReceiptEnum::Action(action_receipt),
            })
            .collect()
    }
}

impl<'a> External for RuntimeExt<'a> {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        let storage_key = self.create_storage_key(key);
        let evicted = self
            .trie_update
            .get(&storage_key)
            .map_err(|_| ExternalError::StorageError)?
            .map(DBValue::into_vec);
        self.trie_update.set(storage_key, DBValue::from_slice(value));
        Ok(evicted)
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        let storage_key = self.create_storage_key(key);
        self.trie_update
            .get(&storage_key)
            .map_err(|_| ExternalError::StorageError)
            .map(|opt| opt.map(DBValue::into_vec))
    }

    fn storage_remove(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, ExternalError> {
        let storage_key = self.create_storage_key(key);
        let evicted = self
            .trie_update
            .get(&storage_key)
            .map_err(|_| ExternalError::StorageError)?
            .map(DBValue::into_vec);
        self.trie_update.remove(&storage_key);
        Ok(evicted)
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

    fn storage_iter_range(&mut self, start: &[u8], end: &[u8]) -> Result<u64, ExternalError> {
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
                self.trie_update
                    .get(&key)
                    .expect("error cannot happen")
                    .expect("key is guaranteed to be there")
                    .into_vec(),
            )
        }))
    }

    fn storage_iter_drop(&mut self, iterator_idx: u64) -> Result<(), ExternalError> {
        self.iters.remove(&iterator_idx);
        Ok(())
    }

    fn receipt_create(
        &mut self,
        receipt_indices: Vec<u64>,
        receiver_id: String,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: u128,
        prepaid_gas: u64,
    ) -> Result<u64, ExternalError> {
        let mut input_data_ids = vec![];
        for receipt_index in receipt_indices {
            let data_id = self.new_data_id();
            self.action_receipts
                .get_mut(receipt_index as usize)
                .expect("receipt index should be present")
                .1
                .output_data_receivers
                .push(DataReceiver { data_id, receiver_id: receiver_id.clone() });
            input_data_ids.push(data_id);
        }

        let new_receipt = ActionReceipt {
            signer_id: self.signer_id.clone(),
            signer_public_key: self.signer_public_key.clone(),
            gas_price: self.gas_price,
            output_data_receivers: vec![],
            input_data_ids,
            actions: vec![Action::FunctionCall(FunctionCallAction {
                method_name: String::from_utf8(method_name)
                    .map_err(|_| ExternalError::InvalidMethodName)?,
                args,
                gas: prepaid_gas,
                deposit: attached_deposit,
            })],
        };
        let new_receipt_index = self.action_receipts.len() as u64;
        self.action_receipts.push((receiver_id, new_receipt));
        Ok(new_receipt_index)
    }

    fn sha256(&self, data: &[u8]) -> Result<Vec<u8>, ExternalError> {
        let value_hash = sodiumoxide::crypto::hash::sha256::hash(data);
        Ok(value_hash.as_ref().to_vec())
    }
}
