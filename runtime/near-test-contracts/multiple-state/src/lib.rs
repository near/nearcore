use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::collections::UnorderedMap;
use near_sdk::{env, near_bindgen, setup_alloc};

setup_alloc!();

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct MultipleState {
    records: UnorderedMap<String, UnorderedMap<String, String>>,
}

impl Default for MultipleState {
    fn default() -> Self {
        panic!("MultipleState should be initialized before usage")
    }
}

#[near_bindgen]
impl MultipleState {
    #[init]
    pub fn new() -> Self {
        Self { records: UnorderedMap::new(b"records".to_vec()) }
    }

    #[payable]
    pub fn set(&mut self, title: String, message: String) {
        let account_id = env::signer_account_id();
        let mut account_records = match self.records.get(&account_id) {
            Some(existing) => existing,
            None => UnorderedMap::new(b"account-record".to_vec()),
        };
        account_records.insert(&title, &message);
        self.records.insert(&account_id, &account_records);
    }

    pub fn get(&self, account_id: String, title: String) -> Option<String> {
        assert!(env::is_valid_account_id(account_id.as_bytes()), "Given account ID is invalid");
        self.records.get(&account_id).and_then(|account_records| account_records.get(&title))
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;
    use near_sdk::test_utils::VMContextBuilder;
    use near_sdk::MockedBlockchain;
    use near_sdk::{testing_env, VMContext};
    use std::convert::TryInto;

    fn get_context(is_view: bool) -> VMContext {
        VMContextBuilder::new()
            .signer_account_id("bob_near".try_into().unwrap())
            .is_view(is_view)
            .build()
    }

    #[test]
    fn set_get_message() {
        let context = get_context(false);
        testing_env!(context);
        let mut contract = MultipleState::new();
        contract.set("hello".to_string(), "world".to_string());
        let context = get_context(true);
        testing_env!(context);
        assert_eq!(
            "world".to_string(),
            contract.get("bob_near".to_string(), "hello".to_string()).unwrap()
        );
    }

    #[test]
    fn get_nonexistent_message() {
        let context = get_context(true);
        testing_env!(context);
        let contract = MultipleState::new();
        assert_eq!(None, contract.get("francis.near".to_string(), "what".to_string()));
    }
}
