use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::{env, ext_contract, near_bindgen, setup_alloc, AccountId, Promise};

setup_alloc!();

#[ext_contract]
pub trait ExtSimpleState {
    fn set_status(&mut self, message: String);
    fn get_status(&self, account_id: String) -> Option<String>;
}

#[ext_contract(ext)]
pub trait ExtCrossContract {
    fn get_from_other_callback(&mut self, #[callback] last_read: String);
}

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct CrossContract {
    last_read: String,
    state_contract: AccountId,
}

impl Default for CrossContract {
    fn default() -> Self {
        panic!("CrossContract should be initialized before usage")
    }
}

#[near_bindgen]
impl CrossContract {
    #[init]
    pub fn new(state_contract: AccountId) -> Self {
        Self { state_contract, last_read: "".to_string() }
    }

    #[payable]
    pub fn set_in_other_contract(&mut self, message: String) -> Promise {
        let account_id = env::signer_account_id();
        ext_simple_state::set_status(message, &self.state_contract, 0, env::prepaid_gas() / 2)
    }

    pub fn get_from_other_contract_and_record(&mut self, account_id: String) -> Promise {
        ext_simple_state::get_status(account_id, &self.state_contract, 0, env::prepaid_gas() / 3)
            .then(ext::get_from_other_callback(
                &env::current_account_id(),
                0,
                env::prepaid_gas() / 3,
            ))
    }

    pub fn get_from_other_callback(&mut self, last_read: String) {
        self.last_read = last_read;
    }
}
