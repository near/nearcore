use borsh::BorshDeserialize;
use near_primitives::config::VMConfig;
use near_primitives::errors::{ExternalError, StorageError};
use near_primitives::runtime::fees::RuntimeFeesConfig;
use near_primitives::trie_key::TrieKey;
use near_primitives::types::{AccountId, Balance, Gas};
use near_primitives::version::ProtocolVersion;
use near_store::{TrieUpdate, TrieUpdateValuePtr};
use near_vm_errors::HostError;
use near_vm_logic::mocks::mock_memory::MockedMemory;
use near_vm_logic::types::PromiseResult;
use near_vm_logic::{External, VMContext, VMLogic, VMLogicError, ValuePtr};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub struct RuntimeExtValuePtr<'a>(TrieUpdateValuePtr<'a>);

impl<'a> ValuePtr for RuntimeExtValuePtr<'a> {
    fn len(&self) -> u32 {
        self.0.len()
    }

    fn deref(&self) -> Result<Vec<u8>> {
        self.0.deref_value().map_err(wrap_storage_error)
    }
}

/// Emulates the trie and the mock handling code.
pub struct MockedExternal<'a> {
    pub trie_update: &'a mut TrieUpdate,
    pub receipts: Vec<Receipt>,
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

    fn deref(&self) -> Result<Vec<u8>> {
        Ok(self.value.clone())
    }
}

impl MockedExternal<'_> {
    /// Get calls to receipt create that were performed during contract call.
    pub fn get_receipt_create_calls(&self) -> &Vec<Receipt> {
        &self.receipts
    }
}

fn wrap_storage_error(error: StorageError) -> VMLogicError {
    VMLogicError::ExternalError(
        borsh::BorshSerialize::try_to_vec(&ExternalError::StorageError(error))
            .expect("Borsh serialize cannot fail"),
    )
}

impl MockedExternal<'_> {
    fn create_storage_key(&self, key: &[u8]) -> TrieKey {
        TrieKey::Account { account_id: std::str::from_utf8(key).unwrap().to_string() }
    }
}

pub type Result<T> = ::std::result::Result<T, VMLogicError>;
pub type PublicKey = Vec<u8>;

impl External for MockedExternal<'_> {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let storage_key = self.create_storage_key(key);
        self.trie_update.set(storage_key, Vec::from(value));
        Ok(())
    }

    fn storage_get<'b>(&'b self, key: &[u8]) -> Result<Option<Box<dyn ValuePtr + 'b>>> {
        let storage_key = self.create_storage_key(key);
        self.trie_update
            .get_ref(&storage_key)
            .map_err(wrap_storage_error)
            .map(|option| option.map(|ptr| Box::new(RuntimeExtValuePtr(ptr)) as Box<_>))
    }

    fn storage_remove(&mut self, key: &[u8]) -> Result<()> {
        Ok(())
    }

    fn storage_remove_subtree(&mut self, prefix: &[u8]) -> Result<()> {
        Ok(())
    }

    fn storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
        let storage_key = self.create_storage_key(key);
        self.trie_update.get_ref(&storage_key).map(|x| x.is_some()).map_err(wrap_storage_error)
    }

    fn create_receipt(&mut self, receipt_indices: Vec<u64>, receiver_id: String) -> Result<u64> {
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
        receiver_id: String,
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
        beneficiary_id: String,
    ) -> Result<()> {
        self.receipts
            .get_mut(receipt_index as usize)
            .unwrap()
            .actions
            .push(Action::DeleteAccount(DeleteAccountAction { beneficiary_id }));
        Ok(())
    }

    fn get_touched_nodes_count(&self) -> u64 {
        self.trie_update.trie.counter.get()
    }

    fn reset_touched_nodes_counter(&mut self) {
        self.trie_update.trie.counter.reset()
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
    receiver_id: String,
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
    #[serde(with = "near_vm_logic::serde_with::bytes_as_str")]
    method_name: Vec<u8>,
    /// Most function calls still take JSON as input, so we'll keep it there as a string.
    /// Once we switch to borsh, we'll have to switch to base64 encoding.
    /// Right now, it is only used with standalone runtime when passing in Receipts or expecting
    /// receipts. The workaround for input is to use a VMContext input.
    #[serde(with = "near_vm_logic::serde_with::bytes_as_str")]
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
    #[serde(with = "near_vm_logic::serde_with::bytes_as_base58")]
    public_key: PublicKey,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddKeyWithFullAccessAction {
    #[serde(with = "near_vm_logic::serde_with::bytes_as_base58")]
    public_key: PublicKey,
    nonce: u64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct AddKeyWithFunctionCallAction {
    #[serde(with = "near_vm_logic::serde_with::bytes_as_base58")]
    public_key: PublicKey,
    nonce: u64,
    allowance: Option<Balance>,
    receiver_id: AccountId,
    #[serde(with = "near_vm_logic::serde_with::vec_bytes_as_str")]
    method_names: Vec<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeleteKeyAction {
    #[serde(with = "near_vm_logic::serde_with::bytes_as_base58")]
    public_key: PublicKey,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeleteAccountAction {
    beneficiary_id: AccountId,
}

pub(crate) const LATEST_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion::MAX;

pub fn get_context(input: Vec<u8>, is_view: bool) -> VMContext {
    VMContext {
        current_account_id: "alice.near".to_string(),
        signer_account_id: "bob.near".to_string(),
        signer_account_pk: vec![0, 1, 2],
        predecessor_account_id: "carol.near".to_string(),
        input,
        block_index: 0,
        block_timestamp: 0,
        epoch_height: 0,
        account_balance: u128::MAX / 2,
        storage_usage: u64::MAX / 2,
        account_locked_balance: u128::MAX / 2,
        attached_deposit: 10,
        prepaid_gas: 10_u64.pow(17),
        random_seed: vec![],
        is_view,
        output_data_receivers: vec![],
    }
}
