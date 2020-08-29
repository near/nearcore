use borsh::BorshDeserialize;
use ethereum_types::{Address, H160, U256};
use evm::CreateContractAddress;

use near_primitives::types::{AccountId, Balance};
use near_store::TrieUpdate;
use near_vm_errors::VMError;
use near_vm_logic::VMOutcome;

use crate::errors::EvmError;
use crate::evm_state::{EvmState, StateStore};
use crate::types::{GetCodeArgs, GetStorageAtArgs, WithdrawNearArgs};

mod builtins;
mod errors;
mod evm_state;
mod interpreter;
mod near_ext;
mod types;
pub mod utils;

pub struct EvmContext<'a> {
    trie_update: &'a mut TrieUpdate,
    sender_id: AccountId,
    attached_deposit: Balance,
}

impl<'a> EvmState for EvmContext<'a> {
    fn code_at(&self, address: &H160) -> Option<Vec<u8>> {
        unimplemented!()
    }

    fn set_code(&mut self, address: &H160, bytecode: &[u8]) {
        unimplemented!()
    }

    fn _set_balance(&mut self, address: [u8; 20], balance: [u8; 32]) -> Option<[u8; 32]> {
        unimplemented!()
    }

    fn _balance_of(&self, address: [u8; 20]) -> [u8; 32] {
        unimplemented!()
    }

    fn _set_nonce(&mut self, address: [u8; 20], nonce: [u8; 32]) -> Option<[u8; 32]> {
        unimplemented!()
    }

    fn _nonce_of(&self, address: [u8; 20]) -> [u8; 32] {
        unimplemented!()
    }

    fn _read_contract_storage(&self, key: [u8; 52]) -> Option<[u8; 32]> {
        unimplemented!()
    }

    fn _set_contract_storage(&mut self, key: [u8; 52], value: [u8; 32]) -> Option<[u8; 32]> {
        unimplemented!()
    }

    fn commit_changes(&mut self, other: &StateStore) {
        unimplemented!()
    }

    fn recreate(&mut self, address: [u8; 20]) {
        unimplemented!()
    }
}

impl<'a> EvmContext<'a> {
    pub fn deploy_code(&mut self, bytecode: Vec<u8>) -> Result<Vec<u8>, EvmError> {
        let sender = utils::near_account_id_to_evm_address(&self.sender_id);
        interpreter::deploy_code(
            self,
            &sender,
            &sender,
            U256::from(self.attached_deposit),
            0,
            CreateContractAddress::FromSenderAndNonce,
            true,
            &bytecode,
        )
        .map(|address| utils::address_to_vec(&address))
    }

    pub fn call_function(&mut self, args: Vec<u8>) -> Result<Vec<u8>, EvmError> {
        let contract_address = Address::from_slice(&args[..20]);
        let input = &args[20..];
        let sender = utils::near_account_id_to_evm_address(&self.sender_id);
        let value =
            if self.attached_deposit == 0 { None } else { Some(U256::from(self.attached_deposit)) };
        interpreter::call(self, &sender, &sender, value, 0, &contract_address, &input, true)
            .map(|rd| rd.to_vec())
    }

    pub fn get_code(&self, args: Vec<u8>) -> Result<Vec<u8>, EvmError> {
        let args = GetCodeArgs::try_from_slice(&args).map_err(|_| EvmError::ArgumentParseError)?;
        Ok(self.code_at(&Address::from_slice(&args.address)).unwrap_or(vec![]))
    }

    pub fn get_storage_at(&self, args: Vec<u8>) -> Result<Vec<u8>, EvmError> {
        let args =
            GetStorageAtArgs::try_from_slice(&args).map_err(|_| EvmError::ArgumentParseError)?;
        Ok(self
            .read_contract_storage(&Address::from_slice(&args.address), args.key)
            .unwrap_or([0u8; 32])
            .to_vec())
    }

    pub fn get_balance(&self, args: Vec<u8>) -> Result<Vec<u8>, EvmError> {
        Ok(utils::u256_to_vec(&self.balance_of(&Address::from_slice(&args))))
    }

    pub fn deposit_near(&mut self, args: Vec<u8>) -> Result<Vec<u8>, EvmError> {
        if self.attached_deposit == 0 {
            return Err(EvmError::MissingDeposit);
        }
        let sender = utils::near_account_id_to_evm_address(&self.sender_id);
        let address = Address::from_slice(&args);
        self.add_balance(&address, U256::from(self.attached_deposit));
        Ok(utils::u256_to_vec(&self.balance_of(&address)))
    }

    pub fn withdraw_near(&mut self, args: Vec<u8>) -> Result<Vec<u8>, EvmError> {
        let args =
            WithdrawNearArgs::try_from_slice(&args).map_err(|err| EvmError::ArgumentParseError)?;
        let sender = utils::near_account_id_to_evm_address(&self.sender_id);
        let amount = U256::from(args.amount);
        if amount > self.balance_of(&sender) {
            return Err(EvmError::InsufficientFunds);
        }
        self.sub_balance(&sender, amount);
        // TODO: add outgoing promise.
        Ok(vec![])
    }
}

pub fn run_evm(
    mut state_update: &mut TrieUpdate,
    predecessor_id: AccountId,
    attached_deposit: Balance,
    method_name: String,
    args: Vec<u8>,
) -> (Option<VMOutcome>, Option<VMError>) {
    let mut context =
        EvmContext { trie_update: &mut state_update, sender_id: predecessor_id, attached_deposit };
    let result = match method_name.as_str() {
        "deploy_code" => context.deploy_code(args),
        "get_code" => context.get_code(args),
        "call_function" => context.call_function(args),
        "get_storage_at" => context.get_storage_at(args),
        "get_balance" => context.get_balance(args),
        "deposit_near" => context.deposit_near(args),
        "withdraw_near" => context.withdraw_near(args),
        _ => Err(EvmError::UnknownError),
    };
    (None, None)
}
