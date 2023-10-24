use crate::logic::types::ReceiptIndex;
use crate::logic::TrieNodesCount;
use crate::logic::{External, StorageGetMode, ValuePtr};
use near_primitives_core::hash::{hash, CryptoHash};
use near_primitives_core::types::{AccountId, Balance, Gas, GasWeight};
use std::collections::HashMap;

#[derive(serde::Serialize)]
#[serde(remote = "GasWeight")]
struct GasWeightSer(u64);

#[derive(Debug, Clone, serde::Serialize)]
pub enum MockAction {
    CreateReceipt {
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    },
    CreateAccount {
        receipt_index: ReceiptIndex,
    },
    DeployContract {
        receipt_index: ReceiptIndex,
        code: Vec<u8>,
    },
    FunctionCallWeight {
        receipt_index: ReceiptIndex,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: u128,
        prepaid_gas: Gas,
        #[serde(with = "GasWeightSer")]
        gas_weight: GasWeight,
    },
    Transfer {
        receipt_index: ReceiptIndex,
        deposit: u128,
    },
    Stake {
        receipt_index: ReceiptIndex,
        stake: u128,
        public_key: near_crypto::PublicKey,
    },
    DeleteAccount {
        receipt_index: ReceiptIndex,
        beneficiary_id: AccountId,
    },
    DeleteKey {
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
    },
    AddKeyWithFunctionCall {
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: u64,
        allowance: Option<u128>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    },
    AddKeyWithFullAccess {
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: u64,
    },
}

#[derive(Default, Clone)]
/// Emulates the trie and the mock handling code.
pub struct MockedExternal {
    pub fake_trie: HashMap<Vec<u8>, Vec<u8>>,
    pub validators: HashMap<AccountId, Balance>,
    pub action_log: Vec<MockAction>,
    data_count: u64,
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

    fn deref(&self) -> crate::logic::dependencies::Result<Vec<u8>> {
        Ok(self.value.clone())
    }
}

impl MockedExternal {
    pub fn new() -> Self {
        Self::default()
    }
}

use crate::logic::dependencies::Result;

impl External for MockedExternal {
    fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.fake_trie.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    fn storage_get(&self, key: &[u8], _mode: StorageGetMode) -> Result<Option<Box<dyn ValuePtr>>> {
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

    fn storage_has_key(&mut self, key: &[u8], _mode: StorageGetMode) -> Result<bool> {
        Ok(self.fake_trie.contains_key(key))
    }

    fn generate_data_id(&mut self) -> CryptoHash {
        // Generates some hash for the data ID to receive data. This hash should not be functionally
        // used in any mocked contexts.
        let data_id = hash(&self.data_count.to_le_bytes());
        self.data_count += 1;
        data_id
    }

    fn get_trie_nodes_count(&self) -> TrieNodesCount {
        TrieNodesCount { db_reads: 0, mem_reads: 0 }
    }

    fn validator_stake(&self, account_id: &AccountId) -> Result<Option<Balance>> {
        Ok(self.validators.get(account_id).cloned())
    }

    fn validator_total_stake(&self) -> Result<Balance> {
        Ok(self.validators.values().sum())
    }

    fn create_receipt(
        &mut self,
        receipt_indices: Vec<ReceiptIndex>,
        receiver_id: AccountId,
    ) -> Result<ReceiptIndex, crate::logic::VMLogicError> {
        let index = self.action_log.len();
        self.action_log.push(MockAction::CreateReceipt { receipt_indices, receiver_id });
        Ok(index as u64)
    }

    fn append_action_create_account(
        &mut self,
        receipt_index: ReceiptIndex,
    ) -> Result<(), crate::logic::VMLogicError> {
        self.action_log.push(MockAction::CreateAccount { receipt_index });
        Ok(())
    }

    fn append_action_deploy_contract(
        &mut self,
        receipt_index: ReceiptIndex,
        code: Vec<u8>,
    ) -> Result<(), crate::logic::VMLogicError> {
        self.action_log.push(MockAction::DeployContract { receipt_index, code });
        Ok(())
    }

    fn append_action_function_call_weight(
        &mut self,
        receipt_index: ReceiptIndex,
        method_name: Vec<u8>,
        args: Vec<u8>,
        attached_deposit: Balance,
        prepaid_gas: Gas,
        gas_weight: GasWeight,
    ) -> Result<(), crate::logic::VMLogicError> {
        self.action_log.push(MockAction::FunctionCallWeight {
            receipt_index,
            method_name,
            args,
            attached_deposit,
            prepaid_gas,
            gas_weight,
        });
        Ok(())
    }

    fn append_action_transfer(
        &mut self,
        receipt_index: ReceiptIndex,
        deposit: Balance,
    ) -> Result<(), crate::logic::VMLogicError> {
        self.action_log.push(MockAction::Transfer { receipt_index, deposit });
        Ok(())
    }

    fn append_action_stake(
        &mut self,
        receipt_index: ReceiptIndex,
        stake: Balance,
        public_key: near_crypto::PublicKey,
    ) {
        self.action_log.push(MockAction::Stake { receipt_index, stake, public_key });
    }

    fn append_action_add_key_with_full_access(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: near_primitives_core::types::Nonce,
    ) {
        self.action_log.push(MockAction::AddKeyWithFullAccess { receipt_index, public_key, nonce });
    }

    fn append_action_add_key_with_function_call(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
        nonce: near_primitives_core::types::Nonce,
        allowance: Option<Balance>,
        receiver_id: AccountId,
        method_names: Vec<Vec<u8>>,
    ) -> Result<(), crate::logic::VMLogicError> {
        self.action_log.push(MockAction::AddKeyWithFunctionCall {
            receipt_index,
            public_key,
            nonce,
            allowance,
            receiver_id,
            method_names,
        });
        Ok(())
    }

    fn append_action_delete_key(
        &mut self,
        receipt_index: ReceiptIndex,
        public_key: near_crypto::PublicKey,
    ) {
        self.action_log.push(MockAction::DeleteKey { receipt_index, public_key });
    }

    fn append_action_delete_account(
        &mut self,
        receipt_index: ReceiptIndex,
        beneficiary_id: AccountId,
    ) -> Result<(), crate::logic::VMLogicError> {
        self.action_log.push(MockAction::DeleteAccount { receipt_index, beneficiary_id });
        Ok(())
    }

    fn get_receipt_receiver(&self, receipt_index: ReceiptIndex) -> &AccountId {
        match &self.action_log[receipt_index as usize] {
            MockAction::CreateReceipt { receiver_id, .. } => receiver_id,
            _ => panic!("not a valid receipt index!"),
        }
    }
}
