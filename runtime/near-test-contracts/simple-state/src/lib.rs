use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::{env, log, metadata, near_bindgen, setup_alloc};

use std::collections::HashMap;

setup_alloc!();

metadata! {
#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct SimpleState {
    records: HashMap<String, String>,
}

#[near_bindgen]
impl SimpleState {
    #[payable]
    pub fn set_status(&mut self, message: String) {
        let account_id = env::signer_account_id();
        log!("{} set_status with message {}", account_id, message);
        self.records.insert(account_id, message);
    }

    pub fn get_status(&self, account_id: String) -> Option::<String> {
        assert!(
            env::is_valid_account_id(account_id.as_bytes()),
            "Given account ID is invalid"
        );
        log!("get_status for account_id {}", account_id);
        self.records.get(&account_id).cloned()
    }
}
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;
    use near_sdk::test_utils::{get_logs, VMContextBuilder};
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
        let mut contract = SimpleState::default();
        contract.set_status("hello".to_string());
        assert_eq!(get_logs(), vec!["bob_near set_status with message hello"]);
        let context = get_context(true);
        testing_env!(context);
        assert_eq!("hello".to_string(), contract.get_status("bob_near".to_string()).unwrap());
        assert_eq!(get_logs(), vec!["get_status for account_id bob_near"])
    }

    #[test]
    fn get_nonexistent_message() {
        let context = get_context(true);
        testing_env!(context);
        let contract = SimpleState::default();
        assert_eq!(None, contract.get_status("francis.near".to_string()));
        assert_eq!(get_logs(), vec!["get_status for account_id francis.near"])
    }
}
