use borsh::{BorshDeserialize, BorshSerialize};
use near_sdk::{env, ext_contract, near_bindgen, Promise, PromiseOrValue};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

// Prepaid gas for making a single simple call.
const SINGLE_CALL_GAS: u64 = 200000000000000;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct CrossContract {}
// If the name is not provided, the namespace for generated methods in derived by applying snake
// case to the trait name, e.g. ext_status_message.
#[ext_contract]
pub trait ExtStatusMessage {
    fn set_status(&mut self, message: String);
    fn get_status(&self, account_id: String) -> Option<String>;
}

#[near_bindgen]
impl CrossContract {
    pub fn simple_call(&mut self, account_id: String, message: String) {
        ext_status_message::set_status(message, &account_id, 0, SINGLE_CALL_GAS);
    }
    pub fn complex_call(&mut self, account_id: String, message: String) -> Promise {
        // 1) call status_message to record a message from the signer.
        // 2) call status_message to retrieve the message of the signer.
        // 3) return that message as its own result.
        // Note, for a contract to simply call another contract (1) is sufficient.
        ext_status_message::set_status(message, &account_id, 0, SINGLE_CALL_GAS).then(
            ext_status_message::get_status(
                env::signer_account_id(),
                &account_id,
                0,
                SINGLE_CALL_GAS,
            ),
        )
    }

    pub fn transfer_money(&mut self, account_id: String, amount: u64) {
        Promise::new(account_id).transfer(amount as u128);
    }
}
