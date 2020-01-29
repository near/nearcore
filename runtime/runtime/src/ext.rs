use std::collections::HashMap;
use std::iter::Peekable;

use borsh::BorshDeserialize;
use near_crypto::PublicKey;
use near_primitives::account::{AccessKey, AccessKeyPermission, FunctionCallPermission};
use near_primitives::errors::StorageError;
use near_primitives::hash::CryptoHash;
use near_primitives::receipt::{ActionReceipt, DataReceiver, Receipt, ReceiptEnum};
use near_primitives::transaction::{
    Action, AddKeyAction, CreateAccountAction, DeleteAccountAction, DeleteKeyAction,
    DeployContractAction, FunctionCallAction, StakeAction, TransferAction,
};
use near_primitives::types::{AccountId, Balance};
use near_primitives::utils::{create_nonce_with_nonce, prefix_for_data};
use near_store::{TrieUpdate, TrieUpdateIterator, TrieUpdateValuePtr};
use near_vm_logic::{External, HostError, VMLogicError, ValuePtr};

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

pub struct RuntimeExtValuePtr<'a>(TrieUpdateValuePtr<'a>);

impl<'a> ValuePtr for RuntimeExtValuePtr<'a> {
    fn len(&self) -> u32 {
        self.0.len()
    }

    fn deref(&self) -> ExtResult<Vec<u8>> {
        self.0.deref_value().map_err(wrap_error)
    }
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

    fn append_action(&mut self, receipt_index: u64, action: Action) {
        self.action_receipts
            .get_mut(receipt_index as usize)
            .expect("receipt index should be present")
            .1
            .actions
            .push(action);
    }
}

fn wrap_error(error: StorageError) -> VMLogicError {
    // TODO(#2010): Wrap StorageError into ExternalError.
    VMLogicError::ExternalError(
        borsh::BorshSerialize::try_to_vec(&error).expect("Borsh serialize cannot fail"),
    )
}

type ExtResult<T> = ::std::result::Result<T, VMLogicError>;

impl<'a> External for RuntimeExt<'a> {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> ExtResult<()> {
        let storage_key = self.create_storage_key(key);
        self.trie_update.set(storage_key, Vec::from(value));
        Ok(())
    }

    fn storage_get<'b>(&'b self, key: &[u8]) -> ExtResult<Option<Box<dyn ValuePtr + 'b>>> {
        let storage_key = self.create_storage_key(key);
        self.trie_update
            .get_ref(&storage_key)
            .map_err(wrap_error)
            .map(|option| option.map(|ptr| Box::new(RuntimeExtValuePtr(ptr)) as Box<_>))
    }

    fn storage_remove(&mut self, key: &[u8]) -> ExtResult<()> {
        let storage_key = self.create_storage_key(key);
        self.trie_update.remove(&storage_key);
        Ok(())
    }

    fn storage_has_key(&mut self, key: &[u8]) -> ExtResult<bool> {
        let storage_key = self.create_storage_key(key);
        self.trie_update.get_ref(&storage_key).map(|x| x.is_some()).map_err(wrap_error)
    }

    fn storage_iter(&mut self, prefix: &[u8]) -> ExtResult<u64> {
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

    fn storage_iter_range(&mut self, start: &[u8], end: &[u8]) -> ExtResult<u64> {
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

    fn storage_iter_next<'b>(
        &'b mut self,
        iterator_idx: u64,
    ) -> ExtResult<Option<(Vec<u8>, Box<dyn ValuePtr + 'b>)>> {
        let result = match self.iters.get_mut(&iterator_idx) {
            Some(iter) => iter.next(),
            None => {
                return Err(HostError::InvalidIteratorIndex { iterator_index: iterator_idx }.into())
            }
        };
        match result {
            None => {
                self.iters.remove(&iterator_idx);
                Ok(None)
            }
            Some(key) => {
                let key = key.map_err(wrap_error)?;
                let ptr = self
                    .trie_update
                    .get_ref(&key)
                    .expect("error cannot happen")
                    .expect("key is guaranteed to be there");
                let result = Box::new(RuntimeExtValuePtr(ptr)) as Box<_>;
                Ok(Some((key[self.storage_prefix.len()..].to_vec(), result)))
            }
        }
    }

    fn storage_iter_drop(&mut self, iterator_idx: u64) -> ExtResult<()> {
        self.iters.remove(&iterator_idx);
        Ok(())
    }

    fn create_receipt(&mut self, receipt_indices: Vec<u64>, receiver_id: String) -> ExtResult<u64> {
        let mut input_data_ids = vec![];
        for receipt_index in receipt_indices {
            let data_id = self.new_data_id();
            self.action_receipts
                .get_mut(receipt_index as usize)
                .ok_or_else(|| HostError::InvalidReceiptIndex { receipt_index })?
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
            actions: vec![],
        };
        let new_receipt_index = self.action_receipts.len() as u64;
        self.action_receipts.push((receiver_id, new_receipt));
        Ok(new_receipt_index)
    }

    fn append_action_create_account(&mut self, receipt_index: u64) -> ExtResult<()> {
        self.append_action(receipt_index, Action::CreateAccount(CreateAccountAction {}));
        Ok(())
    }

    fn append_action_deploy_contract(
        &mut self,
        receipt_index: u64,
        code: Vec<u8>,
    ) -> ExtResult<()> {
        self.append_action(receipt_index, Action::DeployContract(DeployContractAction { code }));
        Ok(())
    }

    fn append_action_function_call(
        &mut self,
        receipt_index: u64,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: u128,
        prepaid_gas: u64,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::FunctionCall(FunctionCallAction {
                method_name: String::from_utf8(method_name)
                    .map_err(|_| HostError::InvalidMethodName)?,
                args,
                gas: prepaid_gas,
                deposit: attached_deposit,
            }),
        );
        Ok(())
    }

    fn append_action_transfer(&mut self, receipt_index: u64, deposit: u128) -> ExtResult<()> {
        self.append_action(receipt_index, Action::Transfer(TransferAction { deposit }));
        Ok(())
    }

    fn append_action_stake(
        &mut self,
        receipt_index: u64,
        stake: u128,
        public_key: Vec<u8>,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::Stake(StakeAction {
                stake,
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
            }),
        );
        Ok(())
    }

    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: u64,
        public_key: Vec<u8>,
        nonce: u64,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::AddKey(AddKeyAction {
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
                access_key: AccessKey { nonce, permission: AccessKeyPermission::FullAccess },
            }),
        );
        Ok(())
    }

    fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: u64,
        public_key: Vec<u8>,
        nonce: u64,
        allowance: Option<u128>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::AddKey(AddKeyAction {
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
                access_key: AccessKey {
                    nonce,
                    permission: AccessKeyPermission::FunctionCall(FunctionCallPermission {
                        allowance,
                        receiver_id,
                        method_names: method_names
                            .into_iter()
                            .map(|method_name| {
                                String::from_utf8(method_name)
                                    .map_err(|_| HostError::InvalidMethodName)
                            })
                            .collect::<std::result::Result<Vec<_>, _>>()?,
                    }),
                },
            }),
        );
        Ok(())
    }

    fn append_action_delete_key(
        &mut self,
        receipt_index: u64,
        public_key: Vec<u8>,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::DeleteKey(DeleteKeyAction {
                public_key: PublicKey::try_from_slice(&public_key)
                    .map_err(|_| HostError::InvalidPublicKey)?,
            }),
        );
        Ok(())
    }

    fn append_action_delete_account(
        &mut self,
        receipt_index: u64,
        beneficiary_id: AccountId,
    ) -> ExtResult<()> {
        self.append_action(
            receipt_index,
            Action::DeleteAccount(DeleteAccountAction { beneficiary_id }),
        );
        Ok(())
    }

    fn sha256(&self, data: &[u8]) -> ExtResult<Vec<u8>> {
        use sha2::Digest;
        let value_hash = sha2::Sha256::digest(data);
        Ok(value_hash.as_ref().to_vec())
    }

    fn get_touched_nodes_count(&self) -> u64 {
        self.trie_update.trie.counter.get()
    }

    fn reset_touched_nodes_counter(&mut self) {
        self.trie_update.trie.counter.reset()
    }
}
