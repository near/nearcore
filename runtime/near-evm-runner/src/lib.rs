mod builtins;
mod errors;
mod evm_state;
mod interpreter;
mod near_ext;
mod utils;

use crate::evm_state::{EvmState, StateStore};
use ethereum_types::H160;
use near_primitives::{account::AccountId, types::Balance};
use near_vm_errors::VMError;
use near_vm_logic::VMOutcome;
use vm::Address;

struct EvmContext {
    sender_id: AccountId,
    attached_deposit: Balance,
}

impl EvmState for EvmContext {
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
}

pub fn run_evm(
    sender_id: AccountId,
    attached_deposit: Balance,
    method_name: String,
    args: Vec<u8>,
) -> (Option<VMOutcome>, Option<VMError>) {
    let mut context = EvmContext { sender_id, attached_deposit };
    let sender = utils::near_account_id_to_evm_address(sender_id);
    let result = match method_name.as_str() {
        "deploy_code" => interpreter::deploy_code(&mut context, &sender, &sender, val, 0, &args),
        "get_code" => {
            let address = Address::from(args);
            context.get_code(address)
        }
        "call_function" => {
            let contract_address = args[:20];
            let input = args[20:];
            let result = interpreter::call(
                &mut context,
                sender,
                sender,
                value,
                0, // call-stack depth
                &contract_address,
                &input,
                true,
            );
        }
        _ => (),
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
