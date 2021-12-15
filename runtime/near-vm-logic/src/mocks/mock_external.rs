use crate::{External, ValuePtr};
use near_primitives_core::types::{AccountId, Balance, Gas};
use near_vm_errors::HostError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Default, Clone)]
/// Emulates the trie and the mock handling code.
pub struct MockedExternal {
    pub fake_trie: HashMap<Vec<u8>, Vec<u8>>,
    receipts: Vec<Receipt>,
    pub validators: HashMap<AccountId, Balance>,
}

pub struct MockedValuePtr {
    value: Vec<u8>,
}

impl MockedValuePtr {
    pub fn new<T>(value: T) -> Self
    where
        T: AsRef<[u8]>,
    {
        MockedValuePtr { value: value.as_ref().to_vec() }
    }
}

impl ValuePtr for MockedValuePtr {
    fn len(&self) -> u32 {
        self.value.len() as u32
    }

    fn deref(&self) -> crate::dependencies::Result<Vec<u8>> {
        Ok(self.value.clone())
    }
}

impl MockedExternal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Get calls to receipt create that were performed during contract call.
    pub fn get_receipt_create_calls(&self) -> &Vec<Receipt> {
        &self.receipts
    }
}

use crate::dependencies::Result;
use crate::types::PublicKey;

impl External for MockedExternal {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.fake_trie.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn storage_get(&self, key: &[u8]) -> Result<Option<Box<dyn ValuePtr>>> {
        Ok(self
            .fake_trie
            .get(key)
            .map(|value| Box::new(MockedValuePtr { value: value.clone() }) as Box<_>))
    }

    fn storage_remove(&mut self, key: &[u8]) -> Result<()> {
        self.fake_trie.remove(key);
        Ok(())
    }

    fn storage_remove_subtree(&mut self, prefix: &[u8]) -> Result<()> {
        self.fake_trie.retain(|key, _| !key.starts_with(prefix));
        Ok(())
    }

    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
        Ok(self.fake_trie.contains_key(key))
    }

    fn create_receipt(&mut self, receipt_indices: Vec<u64>, receiver_id: AccountId) -> Result<u64> {
        if let Some(index) = receipt_indices.iter().find(|&&el| el >= self.receipts.len() as u64) {
            return Err(HostError::InvalidReceiptIndex { receipt_index: *index }.into());
        }
        let res = self.receipts.len() as u64;
        self.receipts.push(Receipt { receipt_indices, receiver_id, actions: vec![] });
        Ok(res)
    }

    fn append_action_create_account(&mut self, receipt_index: u64) -> Result<()> {
        self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(Action::CreateAccount);
        Ok(())
    }

    fn append_action_deploy_contract(&mut self, receipt_index: u64, code: Vec<u8>) -> Result<()> {
        self.receipts
            .get_mut(receipt_index as usize)
            .unwrap()
            .actions
            .push(Action::DeployContract(DeployContractAction { code }));
        Ok(())
    }

    fn append_action_function_call(
        &mut self,
        receipt_index: u64,
        method_name: Vec<u8>,
        arguments: Vec<u8>,
        attached_deposit: u128,
        prepaid_gas: u64,
    ) -> Result<()> {
        self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(Action::FunctionCall(
            FunctionCallAction {
                method_name,
                args: arguments,
                deposit: attached_deposit,
                gas: prepaid_gas,
            },
        ));
        Ok(())
    }

    fn append_action_transfer(&mut self, receipt_index: u64, amount: u128) -> Result<()> {
        self.receipts
            .get_mut(receipt_index as usize)
            .unwrap()
            .actions
            .push(Action::Transfer(TransferAction { deposit: amount }));
        Ok(())
    }

    fn append_action_stake(
        &mut self,
        receipt_index: u64,
        stake: u128,
        public_key: Vec<u8>,
    ) -> Result<()> {
        self.receipts
            .get_mut(receipt_index as usize)
            .unwrap()
            .actions
            .push(Action::Stake(StakeAction { stake, public_key }));
        Ok(())
    }

    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: u64,
        public_key: Vec<u8>,
        nonce: u64,
    ) -> Result<()> {
        self.receipts
            .get_mut(receipt_index as usize)
            .unwrap()
            .actions
            .push(Action::AddKeyWithFullAccess(AddKeyWithFullAccessAction { public_key, nonce }));
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
    ) -> Result<()> {
        self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(
            Action::AddKeyWithFunctionCall(AddKeyWithFunctionCallAction {
                public_key,
                nonce,
                allowance,
                receiver_id,
                method_names,
            }),
        );
        Ok(())
    }

    fn append_action_delete_key(&mut self, receipt_index: u64, public_key: Vec<u8>) -> Result<()> {
        self.receipts
            .get_mut(receipt_index as usize)
            .unwrap()
            .actions
            .push(Action::DeleteKey(DeleteKeyAction { public_key }));
        Ok(())
    }

    fn append_action_delete_account(
        &mut self,
        receipt_index: u64,
        beneficiary_id: AccountId,
    ) -> Result<()> {
        self.receipts
            .get_mut(receipt_index as usize)
            .unwrap()
            .actions
            .push(Action::DeleteAccount(DeleteAccountAction { beneficiary_id }));
        Ok(())
    }

    fn get_touched_nodes_count(&self) -> u64 {
        0
    }

    fn validator_stake(&self, account_id: &AccountId) -> Result<Option<Balance>> {
        Ok(self.validators.get(account_id).cloned())
    }

    fn validator_total_stake(&self) -> Result<Balance> {
        Ok(self.validators.values().sum())
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Receipt {
    receipt_indices: Vec<u64>,
    receiver_id: AccountId,
    actions: Vec<Action>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Action {
    CreateAccount,
    DeployContract(DeployContractAction),
    FunctionCall(FunctionCallAction),
    Transfer(TransferAction),
    Stake(StakeAction),
    AddKeyWithFullAccess(AddKeyWithFullAccessAction),
    AddKeyWithFunctionCall(AddKeyWithFunctionCallAction),
    DeleteKey(DeleteKeyAction),
    DeleteAccount(DeleteAccountAction),
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeployContractAction {
    pub code: Vec<u8>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct FunctionCallAction {
    #[serde(with = "crate::serde_with::bytes_as_str")]
    method_name: Vec<u8>,
    /// Most function calls still take JSON as input, so we'll keep it there as a string.
    /// Once we switch to borsh, we'll have to switch to base64 encoding.
    /// Right now, it is only used with standalone runtime when passing in Receipts or expecting
    /// receipts. The workaround for input is to use a VMContext input.
    #[serde(with = "crate::serde_with::bytes_as_str")]
    args: Vec<u8>,
    gas: Gas,
    deposit: Balance,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TransferAction {
    deposit: Balance,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StakeAction {
    stake: Balance,
    #[serde(with = "crate::serde_with::bytes_as_base58")]
    public_key: PublicKey,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddKeyWithFullAccessAction {
    #[serde(with = "crate::serde_with::bytes_as_base58")]
    public_key: PublicKey,
    nonce: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddKeyWithFunctionCallAction {
    #[serde(with = "crate::serde_with::bytes_as_base58")]
    public_key: PublicKey,
    nonce: u64,
    allowance: Option<Balance>,
    receiver_id: AccountId,
    #[serde(with = "crate::serde_with::vec_bytes_as_str")]
    method_names: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeleteKeyAction {
    #[serde(with = "crate::serde_with::bytes_as_base58")]
    public_key: PublicKey,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeleteAccountAction {
    beneficiary_id: AccountId,
}
