use std::collections::HashMap;
use std::iter::Peekable;
use std::sync::{Arc, Mutex};

use bigint::{H256, H64, U256};
use kvdb::DBValue;

use near_primitives::hash::CryptoHash;
use near_primitives::types::{AccountId, Balance, PromiseId};
use near_primitives::utils::{create_nonce_with_nonce, prefix_for_data};
use near_store::{TrieUpdate, TrieUpdateIterator};
use wasm::ext::{Error as ExtError, External, Result as ExtResult};

use crate::ethereum::EthashProvider;
use crate::POISONED_LOCK_ERR;
use near_primitives::crypto::signature::PublicKey;
use near_primitives::receipt::{ActionReceipt, DataReceiver, Receipt, ReceiptEnum};
use near_primitives::transaction::{Action, FunctionCallAction};

pub struct RuntimeExt<'a> {
    trie_update: &'a mut TrieUpdate,
    storage_prefix: Vec<u8>,
    action_receipts: Vec<(AccountId, ActionReceipt)>,
    account_id: &'a AccountId,
    iters: HashMap<u32, Peekable<TrieUpdateIterator<'a>>>,
    last_iter_id: u32,
    ethash_provider: Arc<Mutex<EthashProvider>>,
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
        ethash_provider: Arc<Mutex<EthashProvider>>,
        signer_id: &'a AccountId,
        signer_public_key: &'a PublicKey,
        gas_price: Balance,
        base_data_id: &'a CryptoHash,
    ) -> Self {
        RuntimeExt {
            trie_update,
            storage_prefix: prefix_for_data(account_id),
            action_receipts: vec![],
            account_id,
            iters: HashMap::new(),
            last_iter_id: 0,
            ethash_provider,
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

    fn new_function_call_action_receipt(
        &self,
        method_name: Vec<u8>,
        args: Vec<u8>,
        amount: u128,
        input_data_ids: Vec<CryptoHash>,
    ) -> ExtResult<ActionReceipt> {
        Ok(ActionReceipt {
            signer_id: self.signer_id.clone(),
            signer_public_key: self.signer_public_key.clone(),
            gas_price: self.gas_price,
            output_data_receivers: vec![],
            input_data_ids,
            actions: vec![Action::FunctionCall(FunctionCallAction {
                method_name: String::from_utf8(method_name).map_err(|_| ExtError::BadUtf8)?,
                args,
                gas: 10000,
                deposit: amount,
            })],
        })
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

    /*
        New promise APIs for reference


        fn promise_create(
        &mut self,
        account_id: AccountId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
        gas: Gas,
    ) -> Result<PromiseIndex>;

    fn promise_then(
        &mut self,
        promise_id: PromiseIndex,
        account_id: AccountId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
        gas: Gas,
    ) -> Result<PromiseIndex>;

    fn promise_and(&mut self, promise_indices: &[PromiseIndex]) -> Result<PromiseIndex>;

    */

    fn promise_create(
        &mut self,
        account_id: AccountId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
    ) -> ExtResult<PromiseId> {
        let new_receipt =
            self.new_function_call_action_receipt(method_name, arguments, amount, vec![])?;
        let new_promise_id = vec![self.action_receipts.len()];
        self.action_receipts.push((account_id, new_receipt));
        // self.promises.push(new_promise_id);
        Ok(new_promise_id)
    }

    fn promise_then(
        &mut self,
        promise_id: PromiseId,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        amount: Balance,
    ) -> ExtResult<PromiseId> {
        let receiver_id = self.account_id.clone();

        let mut input_data_ids = vec![];
        for receipt_index in promise_id {
            let data_id = self.new_data_id();
            self.action_receipts
                .get_mut(receipt_index)
                .expect("receipt index should be present")
                .1
                .output_data_receivers
                .push(DataReceiver { data_id, receiver_id: receiver_id.clone() });
            input_data_ids.push(data_id);
        }

        let new_receipt =
            self.new_function_call_action_receipt(method_name, arguments, amount, input_data_ids)?;
        let new_promise_id = vec![self.action_receipts.len()];
        self.action_receipts.push((self.account_id.clone(), new_receipt));
        // self.promises.push(new_promise_id);
        Ok(new_promise_id)
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
