use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::{metadata, near_bindgen};
use std::collections::HashMap;

metadata! {
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
}
