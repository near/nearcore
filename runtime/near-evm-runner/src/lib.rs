use borsh::{BorshDeserialize, BorshSerialize};
use ethereum_types::{Address, H160, U256};
use evm::CreateContractAddress;

use near_vm_errors::{EvmError, FunctionCallError, VMError};
use near_vm_logic::types::{AccountId, Balance, ReturnData, StorageUsage};
use near_vm_logic::{External, VMLogicError, VMOutcome};

use crate::evm_state::{EvmAccount, EvmState, StateStore};
use crate::types::{AddressArg, GetStorageAtArgs, Result, TransferArgs, WithdrawArgs};

mod builtins;
mod evm_state;
mod interpreter;
mod near_ext;
pub mod types;
pub mod utils;

pub struct EvmContext<'a> {
    ext: &'a mut dyn External,
    predecessor_id: AccountId,
    attached_deposit: Balance,
    storage_usage: StorageUsage,
    logs: Vec<String>,
}

enum KeyPrefix {
    Account = 0,
    Contract = 1,
}

fn address_to_key(prefix: KeyPrefix, address: &H160) -> Vec<u8> {
    let mut result = Vec::with_capacity(21);
    result.push(prefix as u8);
    result.extend_from_slice(&address.0);
    result
}

impl<'a> EvmState for EvmContext<'a> {
    fn code_at(&self, address: &H160) -> Result<Option<Vec<u8>>> {
        self.ext
            .storage_get(&address_to_key(KeyPrefix::Contract, address))
            .map(|value| value.map(|x| x.deref().unwrap_or(vec![])))
    }

    fn set_code(&mut self, address: &H160, bytecode: &[u8]) -> Result<()> {
        self.ext.storage_set(&address_to_key(KeyPrefix::Contract, address), bytecode)
    }

    fn get_account(&self, address: &Address) -> Result<Option<EvmAccount>> {
        self.ext.storage_get(&address_to_key(KeyPrefix::Account, address)).map(|value| {
            value.map(|x| {
                EvmAccount::try_from_slice(&x.deref().expect("Failed to deref")).unwrap_or_default()
            })
        })
    }

    fn set_account(&mut self, address: &Address, account: &EvmAccount) -> Result<()> {
        self.ext.storage_set(
            &address_to_key(KeyPrefix::Account, address),
            &account.try_to_vec().expect("Failed to serialize"),
        )
    }

    fn _read_contract_storage(&self, key: [u8; 52]) -> Result<Option<[u8; 32]>> {
        self.ext
            .storage_get(&key)
            .map(|value| value.map(|x| utils::vec_to_arr_32(x.deref().expect("Failed to deref"))))
    }

    fn _set_contract_storage(&mut self, key: [u8; 52], value: [u8; 32]) -> Result<()> {
        self.ext.storage_set(&key, &value)
    }

    fn commit_changes(&mut self, other: &StateStore) -> Result<()> {
        //        self.commit_self_destructs(&other.self_destructs);
        //        self.commit_self_destructs(&other.recreated);
        for (address, code) in other.code.iter() {
            self.set_code(&H160(*address), code)?;
        }
        for (address, account) in other.accounts.iter() {
            self.set_account(&H160(*address), account)?;
        }
        for (key, value) in other.storages.iter() {
            let mut arr = [0; 52];
            arr.copy_from_slice(&key);
            self._set_contract_storage(arr, *value)?;
        }
        self.logs.extend_from_slice(&other.logs);
        Ok(())
    }

    fn recreate(&mut self, _address: [u8; 20]) {
        unreachable!()
    }
}

impl<'a> EvmContext<'a> {
    pub fn new(
        ext: &'a mut dyn External,
        predecessor_id: AccountId,
        attached_deposit: Balance,
        storage_usage: StorageUsage,
    ) -> Self {
        Self { ext, predecessor_id, attached_deposit, storage_usage, logs: Vec::default() }
    }

    pub fn deploy_code(&mut self, bytecode: Vec<u8>) -> Result<Address> {
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        self.add_balance(&sender, U256::from(self.attached_deposit))?;
        interpreter::deploy_code(
            self,
            &sender,
            &sender,
            U256::from(self.attached_deposit),
            0,
            CreateContractAddress::FromSenderAndNonce,
            false,
            &bytecode,
        )
    }

    pub fn call_function(&mut self, args: Vec<u8>) -> Result<Vec<u8>> {
        if args.len() <= 20 {
            return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
        }
        let contract_address = Address::from_slice(&args[..20]);
        let input = &args[20..];
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        self.add_balance(&sender, U256::from(self.attached_deposit))?;
        let value =
            if self.attached_deposit == 0 { None } else { Some(U256::from(self.attached_deposit)) };
        interpreter::call(self, &sender, &sender, value, 0, &contract_address, &input, true)
            .map(|rd| rd.to_vec())
    }

    /// Make an EVM transaction. Calls `contract_address` with `encoded_input`. Execution
    /// continues until all EVM messages have been processed. We expect this to behave identically
    /// to an Ethereum transaction, however there may be some edge cases.
    ///
    /// This function serves the eth_call functionality, and will NOT apply state changes.
    pub fn view_call_function(&mut self, args: Vec<u8>) -> Result<Vec<u8>> {
        if args.len() <= 20 {
            return Err(VMLogicError::EvmError(EvmError::ArgumentParseError));
        }
        let contract_address = Address::from_slice(&args[..20]);
        let input = &args[20..];
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        interpreter::call(self, &sender, &sender, None, 0, &contract_address, &input, false)
            .map(|rd| rd.to_vec())
    }

    pub fn get_code(&self, args: Vec<u8>) -> Result<Vec<u8>> {
        let args = AddressArg::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        Ok(self.code_at(&Address::from_slice(&args.address)).unwrap_or(None).unwrap_or(vec![]))
    }

    pub fn get_storage_at(&self, args: Vec<u8>) -> Result<Vec<u8>> {
        let args = GetStorageAtArgs::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        Ok(self
            .read_contract_storage(&Address::from_slice(&args.address), args.key)?
            .unwrap_or([0u8; 32])
            .to_vec())
    }

    pub fn get_balance(&self, args: Vec<u8>) -> Result<U256> {
        let args = AddressArg::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        self.balance_of(&Address::from_slice(&args.address))
    }

    pub fn get_nonce(&self, args: Vec<u8>) -> Result<U256> {
        let args = AddressArg::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        self.nonce_of(&Address::from_slice(&args.address))
    }

    pub fn deposit(&mut self, args: Vec<u8>) -> Result<U256> {
        if self.attached_deposit == 0 {
            return Err(VMLogicError::EvmError(EvmError::MissingDeposit));
        }
        let address = Address::from_slice(&args);
        self.add_balance(&address, U256::from(self.attached_deposit))?;
        self.balance_of(&address)
    }

    pub fn withdraw(&mut self, args: Vec<u8>) -> Result<()> {
        let args = WithdrawArgs::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        let amount = U256::from(args.amount);
        if amount > self.balance_of(&sender)? {
            return Err(VMLogicError::EvmError(EvmError::InsufficientFunds));
        }
        self.sub_balance(&sender, amount)?;
        let receipt_index = self.ext.create_receipt(vec![], args.account_id)?;
        self.ext.append_action_transfer(receipt_index, args.amount).map_err(|err| err.into())
    }

    /// Transfer tokens from sender to given EVM address.
    pub fn transfer(&mut self, args: Vec<u8>) -> Result<()> {
        let args = TransferArgs::try_from_slice(&args)
            .map_err(|_| VMLogicError::EvmError(EvmError::ArgumentParseError))?;
        let sender = utils::near_account_id_to_evm_address(&self.predecessor_id);
        let amount = U256::from(args.amount);
        if amount > self.balance_of(&sender)? {
            return Err(VMLogicError::EvmError(EvmError::InsufficientFunds));
        }
        self.transfer_balance(&sender, &Address::from(args.account_id), amount)
    }
}

pub fn run_evm(
    ext: &mut dyn External,
    predecessor_id: &AccountId,
    amount: Balance,
    attached_deposit: Balance,
    storage_usage: StorageUsage,
    method_name: String,
    args: Vec<u8>,
) -> (Option<VMOutcome>, Option<VMError>) {
    let mut context = EvmContext::new(ext, predecessor_id.clone(), attached_deposit, storage_usage);
    let result = match method_name.as_str() {
        // Change the state methods.
        "deploy_code" => context.deploy_code(args).map(|address| utils::address_to_vec(&address)),
        "call_function" => context.call_function(args),
        "deposit" => context.deposit(args).map(|balance| utils::u256_to_vec(&balance)),
        "withdraw" => context.withdraw(args).map(|_| vec![]),
        "transfer" => context.transfer(args).map(|_| vec![]),
        // View methods.
        "view_call_contract" => context.view_call_function(args),
        "get_code" => context.get_code(args),
        "get_storage_at" => context.get_storage_at(args),
        "get_nonce" => context.get_nonce(args).map(|nonce| utils::u256_to_vec(&nonce)),
        "get_balance" => context.get_balance(args).map(|balance| utils::u256_to_vec(&balance)),
        _ => Err(VMLogicError::EvmError(EvmError::MethodNotFound)),
    };
    match result {
        Ok(value) => {
            let outcome = VMOutcome {
                // This is total amount of all $NEAR inside this EVM.
                // Outcome balance is equal to amount with attached deposit, because all the accounting happens internally.
                // Should already validate that will not overflow external to this call.
                balance: amount.checked_add(attached_deposit).unwrap_or(amount),
                storage_usage: context.storage_usage,
                return_data: ReturnData::Value(value),
                // TODO: calculate how much gas was used.
                burnt_gas: 0,
                used_gas: 0,
                logs: context.logs,
            };
            (Some(outcome), None)
        }
        Err(VMLogicError::EvmError(err)) => {
            (None, Some(VMError::FunctionCallError(FunctionCallError::EvmError(err))))
        }
        Err(_) => (None, Some(VMError::FunctionCallError(FunctionCallError::WasmUnknownError))),
    }
}

#[cfg(test)]
mod tests {
    use near_vm_logic::mocks::mock_external::MockedExternal;

    use crate::evm_state::SubState;

    use super::*;

    #[test]
    fn state_management() {
        let mut fake_external = MockedExternal::new();
        let mut context = EvmContext::new(&mut fake_external, "alice".to_string(), 0);
        let addr_0 = Address::repeat_byte(0);
        let addr_1 = Address::repeat_byte(1);
        let addr_2 = Address::repeat_byte(2);

        let zero = U256::zero();
        let code: [u8; 3] = [0, 1, 2];
        let nonce = U256::from_dec_str("103030303").unwrap();
        let balance = U256::from_dec_str("3838209").unwrap();
        let storage_key_0 = [4u8; 32];
        let storage_key_1 = [5u8; 32];
        let storage_value_0 = [6u8; 32];
        let storage_value_1 = [7u8; 32];

        context.set_code(&addr_0, &code).unwrap();
        assert_eq!(context.code_at(&addr_0).unwrap(), Some(code.to_vec()));
        assert_eq!(context.code_at(&addr_1).unwrap(), None);
        assert_eq!(context.code_at(&addr_2).unwrap(), None);

        context.set_nonce(&addr_0, nonce).unwrap();
        assert_eq!(context.nonce_of(&addr_0).unwrap(), nonce);
        assert_eq!(context.nonce_of(&addr_1).unwrap(), zero);
        assert_eq!(context.nonce_of(&addr_2).unwrap(), zero);

        context.set_balance(&addr_0, balance).unwrap();
        assert_eq!(context.balance_of(&addr_0).unwrap(), balance);
        assert_eq!(context.balance_of(&addr_1).unwrap(), zero);
        assert_eq!(context.balance_of(&addr_2).unwrap(), zero);

        context.set_contract_storage(&addr_0, storage_key_0, storage_value_0).unwrap();
        assert_eq!(
            context.read_contract_storage(&addr_0, storage_key_0).unwrap(),
            Some(storage_value_0)
        );
        assert_eq!(context.read_contract_storage(&addr_1, storage_key_0).unwrap(), None);
        assert_eq!(context.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);

        let next = {
            // Open a new store
            let mut next = StateStore::default();
            let mut sub1 = SubState::new(&addr_0, &mut next, &context);

            sub1.set_code(&addr_1, &code).unwrap();
            assert_eq!(sub1.code_at(&addr_0).unwrap(), Some(code.to_vec()));
            assert_eq!(sub1.code_at(&addr_1).unwrap(), Some(code.to_vec()));
            assert_eq!(sub1.code_at(&addr_2).unwrap(), None);

            sub1.set_nonce(&addr_1, nonce).unwrap();
            assert_eq!(sub1.nonce_of(&addr_0).unwrap(), nonce);
            assert_eq!(sub1.nonce_of(&addr_1).unwrap(), nonce);
            assert_eq!(sub1.nonce_of(&addr_2).unwrap(), zero);

            sub1.set_balance(&addr_1, balance).unwrap();
            assert_eq!(sub1.balance_of(&addr_0).unwrap(), balance);
            assert_eq!(sub1.balance_of(&addr_1).unwrap(), balance);
            assert_eq!(sub1.balance_of(&addr_2).unwrap(), zero);

            sub1.set_contract_storage(&addr_1, storage_key_0, storage_value_0).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_0, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(sub1.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);

            sub1.set_contract_storage(&addr_1, storage_key_0, storage_value_1).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_0, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_1)
            );
            assert_eq!(sub1.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);

            sub1.set_contract_storage(&addr_1, storage_key_1, storage_value_1).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_1)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_1).unwrap(),
                Some(storage_value_1)
            );

            sub1.set_contract_storage(&addr_1, storage_key_0, storage_value_0).unwrap();
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_0).unwrap(),
                Some(storage_value_0)
            );
            assert_eq!(
                sub1.read_contract_storage(&addr_1, storage_key_1).unwrap(),
                Some(storage_value_1)
            );

            next
        };

        context.commit_changes(&next).unwrap();
        assert_eq!(context.code_at(&addr_0).unwrap(), Some(code.to_vec()));
        assert_eq!(context.code_at(&addr_1).unwrap(), Some(code.to_vec()));
        assert_eq!(context.code_at(&addr_2).unwrap(), None);
        assert_eq!(context.nonce_of(&addr_0).unwrap(), nonce);
        assert_eq!(context.nonce_of(&addr_1).unwrap(), nonce);
        assert_eq!(context.nonce_of(&addr_2).unwrap(), zero);
        assert_eq!(context.balance_of(&addr_0).unwrap(), balance);
        assert_eq!(context.balance_of(&addr_1).unwrap(), balance);
        assert_eq!(context.balance_of(&addr_2).unwrap(), zero);
        assert_eq!(
            context.read_contract_storage(&addr_0, storage_key_0).unwrap(),
            Some(storage_value_0)
        );
        assert_eq!(
            context.read_contract_storage(&addr_1, storage_key_0).unwrap(),
            Some(storage_value_0)
        );
        assert_eq!(
            context.read_contract_storage(&addr_1, storage_key_1).unwrap(),
            Some(storage_value_1)
        );
        assert_eq!(context.read_contract_storage(&addr_2, storage_key_0).unwrap(), None);
    }
}
