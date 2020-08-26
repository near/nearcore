#[cfg(feature = "combined_vm")]
pub mod combined_runner {

    use near_runtime_fees::RuntimeFeesConfig;
    use near_vm_errors::HostError;
    use near_vm_errors::{InconsistentStateError, VMError};
    use near_vm_logic::types::{AccountId, Balance, Gas, ProfileData, PromiseResult, PublicKey};
    use near_vm_logic::{External, VMConfig, VMContext, VMLogicError, VMOutcome, ValuePtr};
    use std::collections::HashMap;

    /// Proxy postponing the actual DB access, reads go via an actual external, writes are postponed
    /// and accumulated.
    pub struct ProxyExternal<'a> {
        ext: &'a mut dyn External,
        local_trie: HashMap<Vec<u8>, Vec<u8>>,
        receipts: Vec<Receipt>,
        validators: HashMap<AccountId, Balance>,
        log: Vec<Action>,
    }

    impl<'a> ProxyExternal<'a> {
        pub fn new(proxied: &'a mut dyn External) -> Self {
            Self {
                ext: proxied,
                local_trie: HashMap::new(),
                receipts: Vec::new(),
                validators: HashMap::new(),
                log: Vec::new(),
            }
        }

        fn commit_log(&'a self) -> Vec<Action> {
            self.log.clone()
        }
    }

    #[derive(Clone, Debug)]
    pub enum Action {
        CreateAccount(CreateAccountAction),
        CreateReceipt(Receipt),
        StorageSet(StorageSetAction),
        StorageRemove(StorageRemoveAction),
        DeployContract(DeployContractAction),
        FunctionCall(FunctionCallAction),
        Transfer(TransferAction),
        Stake(StakeAction),
        AddKeyWithFullAccess(AddKeyWithFullAccessAction),
        AddKeyWithFunctionCall(AddKeyWithFunctionCallAction),
        DeleteKey(DeleteKeyAction),
        DeleteAccount(DeleteAccountAction),
    }

    #[derive(Clone, Debug)]
    pub struct Receipt {
        receipt_indices: Vec<u64>,
        receiver_id: String,
        actions: Vec<Action>,
    }

    #[derive(Clone, Debug)]
    pub struct StorageSetAction {
        pub key: Vec<u8>,
        pub value: Vec<u8>,
    }

    #[derive(Clone, Debug)]
    pub struct StorageRemoveAction {
        pub key: Vec<u8>,
    }

    #[derive(Clone, Debug)]
    pub struct DeployContractAction {
        pub receipt_index: u64,
        pub code: Vec<u8>,
    }

    #[derive(Clone, Debug)]
    pub struct FunctionCallAction {
        pub receipt_index: u64,
        pub method_name: Vec<u8>,
        pub arguments: Vec<u8>,
        pub attached_deposit: Balance,
        pub prepaid_gas: Gas,
    }

    #[derive(Clone, Debug)]
    pub struct TransferAction {
        pub receipt_index: u64,
        pub amount: Balance,
    }

    #[derive(Clone, Debug)]
    pub struct StakeAction {
        pub receipt_index: u64,
        pub stake: Balance,
        pub public_key: PublicKey,
    }

    #[derive(Clone, Debug)]
    pub struct AddKeyWithFullAccessAction {
        pub receipt_index: u64,
        pub public_key: PublicKey,
        pub nonce: u64,
    }

    #[derive(Clone, Debug)]
    pub struct CreateAccountAction {
        pub receipt_index: u64,
    }

    #[derive(Clone, Debug)]
    pub struct AddKeyWithFunctionCallAction {
        pub receipt_index: u64,
        pub public_key: PublicKey,
        pub nonce: u64,
        pub allowance: Option<Balance>,
        pub receiver_id: AccountId,
        pub method_names: Vec<Vec<u8>>,
    }

    #[derive(Clone, Debug)]
    pub struct DeleteKeyAction {
        receipt_index: u64,
        public_key: PublicKey,
    }

    #[derive(Clone, Debug)]
    pub struct DeleteAccountAction {
        receipt_index: u64,
        beneficiary_id: AccountId,
    }

    pub struct MockedValuePtr {
        value: Vec<u8>,
    }

    impl MockedValuePtr {}

    impl ValuePtr for MockedValuePtr {
        fn len(&self) -> u32 {
            self.value.len() as u32
        }

        fn deref(&self) -> Result<Vec<u8>> {
            Ok(self.value.clone())
        }
    }

    type Result<T> = ::std::result::Result<T, VMLogicError>;

    impl<'a> External for ProxyExternal<'a> {
        fn storage_set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
            self.log.push(Action::StorageSet(StorageSetAction {
                key: key.to_vec(),
                value: value.to_vec(),
            }));
            self.local_trie.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        fn storage_get<'b>(&'b self, key: &[u8]) -> Result<Option<Box<dyn ValuePtr + 'b>>> {
            if self.local_trie.contains_key(key) {
                Ok(self
                    .local_trie
                    .get(key)
                    .map(|value| Box::new(MockedValuePtr { value: value.clone() }) as Box<_>))
            } else {
                self.ext.storage_get(key)
            }
        }

        fn storage_remove(&mut self, key: &[u8]) -> Result<()> {
            self.log.push(Action::StorageRemove(StorageRemoveAction { key: key.to_vec() }));
            self.local_trie.remove(key);
            Ok(())
        }

        fn storage_has_key(&mut self, key: &[u8]) -> Result<bool> {
            if self.local_trie.contains_key(key) {
                return Ok(true);
            } else {
                self.ext.storage_has_key(key)
            }
        }

        fn create_receipt(
            &mut self,
            receipt_indices: Vec<u64>,
            receiver_id: String,
        ) -> Result<u64> {
            if let Some(index) =
                receipt_indices.iter().find(|&&el| el >= self.receipts.len() as u64)
            {
                return Err(HostError::InvalidReceiptIndex { receipt_index: *index }.into());
            }
            let res = self.receipts.len() as u64;
            let receipt = Receipt { receipt_indices, receiver_id, actions: vec![] };
            self.log.push(Action::CreateReceipt(receipt.clone()));
            self.receipts.push(receipt);
            Ok(res)
        }

        fn append_action_create_account(&mut self, receipt_index: u64) -> Result<()> {
            let action = Action::CreateAccount(CreateAccountAction { receipt_index });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
            Ok(())
        }

        fn append_action_deploy_contract(
            &mut self,
            receipt_index: u64,
            code: Vec<u8>,
        ) -> Result<()> {
            let action = Action::DeployContract(DeployContractAction { receipt_index, code });
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action.clone());
            self.log.push(action);
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
            let action = Action::FunctionCall(FunctionCallAction {
                receipt_index,
                method_name,
                arguments,
                attached_deposit,
                prepaid_gas,
            });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
            Ok(())
        }

        fn append_action_transfer(&mut self, receipt_index: u64, amount: u128) -> Result<()> {
            let action = Action::Transfer(TransferAction { receipt_index, amount });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
            Ok(())
        }

        fn append_action_stake(
            &mut self,
            receipt_index: u64,
            stake: u128,
            public_key: Vec<u8>,
        ) -> Result<()> {
            let action = Action::Stake(StakeAction { receipt_index, stake, public_key });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
            Ok(())
        }

        fn append_action_add_key_with_full_access(
            &mut self,
            receipt_index: u64,
            public_key: Vec<u8>,
            nonce: u64,
        ) -> Result<()> {
            let action = Action::AddKeyWithFullAccess(AddKeyWithFullAccessAction {
                receipt_index,
                public_key,
                nonce,
            });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
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
            let action = Action::AddKeyWithFunctionCall(AddKeyWithFunctionCallAction {
                receipt_index,
                public_key,
                nonce,
                allowance,
                receiver_id,
                method_names,
            });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
            Ok(())
        }

        fn append_action_delete_key(
            &mut self,
            receipt_index: u64,
            public_key: Vec<u8>,
        ) -> Result<()> {
            let action = Action::DeleteKey(DeleteKeyAction { receipt_index, public_key });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
            Ok(())
        }

        fn append_action_delete_account(
            &mut self,
            receipt_index: u64,
            beneficiary_id: String,
        ) -> Result<()> {
            let action =
                Action::DeleteAccount(DeleteAccountAction { receipt_index, beneficiary_id });
            self.log.push(action.clone());
            self.receipts.get_mut(receipt_index as usize).unwrap().actions.push(action);
            Ok(())
        }

        // TODO: what shall we do here?
        fn get_touched_nodes_count(&self) -> u64 {
            0
        }

        fn reset_touched_nodes_counter(&mut self) {}

        fn validator_stake(&self, account_id: &AccountId) -> Result<Option<Balance>> {
            Ok(self.validators.get(account_id).cloned())
        }

        fn validator_total_stake(&self) -> Result<Balance> {
            Ok(self.validators.values().sum())
        }
    }

    fn commit(ext: &mut dyn External, op: &Action) -> Result<()> {
        println!("Commiting {:?}", op);
        match op {
            Action::AddKeyWithFullAccess(op) => ext.append_action_add_key_with_full_access(
                op.receipt_index,
                op.public_key.clone(),
                op.nonce,
            ),
            Action::AddKeyWithFunctionCall(op) => ext.append_action_add_key_with_function_call(
                op.receipt_index,
                op.public_key.clone(),
                op.nonce,
                op.allowance,
                op.receiver_id.clone(),
                op.method_names.clone(),
            ),
            Action::CreateAccount(op) => ext.append_action_create_account(op.receipt_index),
            Action::CreateReceipt(op) => {
                match ext.create_receipt(op.receipt_indices.clone(), op.receiver_id.clone()) {
                    Ok(_) => Ok(()),
                    Err(err) => panic!("Fix me {:?}", err),
                }
            }
            Action::DeleteAccount(op) => {
                ext.append_action_delete_account(op.receipt_index, op.beneficiary_id.clone())
            }
            Action::DeleteKey(op) => {
                ext.append_action_delete_key(op.receipt_index, op.public_key.clone())
            }
            Action::DeployContract(op) => {
                ext.append_action_deploy_contract(op.receipt_index, op.code.clone())
            }
            Action::FunctionCall(op) => ext.append_action_function_call(
                op.receipt_index,
                op.method_name.clone(),
                op.arguments.clone(),
                op.attached_deposit,
                op.prepaid_gas,
            ),
            Action::Stake(op) => {
                ext.append_action_stake(op.receipt_index, op.stake, op.public_key.clone())
            }
            Action::StorageRemove(op) => ext.storage_remove(&op.key),
            Action::StorageSet(op) => ext.storage_set(&op.key, &op.value),
            Action::Transfer(op) => ext.append_action_transfer(op.receipt_index, op.amount),
        }
    }

    fn results_match(
        ext: &mut dyn External,
        log1: &Vec<Action>,
        _log2: &Vec<Action>,
        result1: (Option<VMOutcome>, Option<VMError>),
        result2: (Option<VMOutcome>, Option<VMError>),
    ) -> (Option<VMOutcome>, Option<VMError>) {
        if (result1.0 == result2.0) && (result1.1 == result2.1) {
            // Commit states changes to the upper level external.
            // TODO: use iter_into and avoid cloning when committing.
            for op in log1.iter() {
                match commit(ext, op) {
                    Err(commit_error) => match commit_error {
                        VMLogicError::ExternalError(external) => {
                            return (None, Some(VMError::ExternalError(external)))
                        }
                        _ => panic!("Cannot be so!"),
                    },
                    Ok(()) => {}
                }
            }
            result1
        } else {
            (
                None,
                Some(VMError::InconsistentStateError(
                    InconsistentStateError::RuntimeBehaviorMismatch,
                )),
            )
        }
    }

    pub fn run_combined<'a>(
        code_hash: Vec<u8>,
        code: &[u8],
        method_name: &[u8],
        ext: &mut dyn External,
        context: VMContext,
        wasm_config: &'a VMConfig,
        fees_config: &'a RuntimeFeesConfig,
        promise_results: &'a [PromiseResult],
        profile: Option<ProfileData>,
    ) -> (Option<VMOutcome>, Option<VMError>) {
        // We operate in the following manner. We create a special purpose External implementation, which
        // doesn't commit DB changes, while make them visible for further invocation.
        // Then we compare side effects produced in Externals by different VMs, and if they coincide -
        // commit those changes, otherwise mark execution as not successful.
        let result1;
        let log1;
        {
            use crate::wasmer_runner::run_wasmer;
            let mut proxy = ProxyExternal::new(ext);
            result1 = run_wasmer(
                code_hash.clone(),
                code,
                method_name,
                &mut proxy,
                context.clone(),
                wasm_config,
                fees_config,
                promise_results,
                // By convention, we profile using Wasmer.
                profile,
            );
            log1 = proxy.commit_log();
        }
        let result2;
        let log2;
        {
            use crate::wasmtime_runner::wasmtime_runner::run_wasmtime;
            let mut proxy = ProxyExternal::new(ext);
            result2 = run_wasmtime(
                code_hash,
                code,
                method_name,
                &mut proxy,
                context,
                wasm_config,
                fees_config,
                promise_results,
                None,
            );
            log2 = proxy.commit_log();
        }
        results_match(ext, &log1, &log2, result1, result2)
    }
}
