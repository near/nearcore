// Very simple contract that can:
// - write to the state
// - delete from the state
// Independently from that the same contract can be used as a receiver for `ft_transfer_call`.
use near_contract_standards::fungible_token::receiver::FungibleTokenReceiver;
use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::json_types::{ValidAccountId, WrappedBalance, U128};
use near_sdk::near_bindgen;
use near_sdk::{assert_one_yocto, serde_json, PromiseOrValue};
use std::collections::HashMap;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct StatusMessage {
    records: HashMap<String, String>,
}

#[near_bindgen]
impl StatusMessage {
    pub fn set_state(&mut self, account_id: String, message: String) {
        self.records.insert(account_id, message);
    }

    pub fn delete_state(&mut self, account_id: String) {
        self.records.remove(&account_id);
    }
}

// Implements a callback which makes it possible to use `ft_transfer_call` with this contract as the
// receiver. The callback simply returns `1`.
#[near_bindgen]
impl FungibleTokenReceiver for StatusMessage {
    #[allow(unused_variables)]
    fn ft_on_transfer(
        &mut self,
        sender_id: ValidAccountId,
        amount: U128,
        msg: String,
    ) -> PromiseOrValue<U128> {
        PromiseOrValue::Value(1.into())
    }
}
