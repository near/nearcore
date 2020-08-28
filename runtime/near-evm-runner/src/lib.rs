use ethereum_types::{Address, H160, U256};
use evm::CreateContractAddress;

use near_primitives::types::{AccountId, Balance};
use near_store::TrieUpdate;
use near_vm_errors::VMError;
use near_vm_logic::VMOutcome;

use crate::errors::EvmError;
use crate::evm_state::{EvmState, StateStore};

mod builtins;
mod errors;
mod evm_state;
mod interpreter;
mod near_ext;
mod utils;

struct EvmContext<'a> {
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

pub fn run_evm(
    mut state_update: &mut TrieUpdate,
    predecessor_id: AccountId,
    attached_deposit: Balance,
    method_name: String,
    args: Vec<u8>,
) -> (Option<VMOutcome>, Option<VMError>) {
    let mut context = EvmContext {
        trie_update: &mut state_update,
        sender_id: predecessor_id.clone(),
        attached_deposit,
    };
    let sender = utils::near_account_id_to_evm_address(&predecessor_id);
    let value = U256::from(attached_deposit);
    let result = match method_name.as_str() {
        "deploy_code" => interpreter::deploy_code(
            &mut context,
            &sender,
            &sender,
            value,
            0,
            CreateContractAddress::FromSenderAndNonce,
            true,
            &args,
        ),
        "get_code" => {
            let address = Address::from_slice(&args);
            // context.get_code(address)
            Ok(address)
        }
        "call_function" => {
            let contract_address = Address::from_slice(&args[..20]);
            let input = &args[20..];
            let result = interpreter::call(
                &mut context,
                &sender,
                &sender,
                Some(value),
                0, // call-stack depth
                &contract_address,
                &input,
                true,
            );
            Ok(contract_address)
        }
        _ => Err(EvmError::UnknownError),
    };
    (None, None)
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
