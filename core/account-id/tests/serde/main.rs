use bolero::check;
use near_account_id::AccountId;
use serde_json::json;
use std::str;

fn main() {
    check!().for_each(|input: &[u8]| {
        if let Ok(account_id) = str::from_utf8(input) {
            if let Ok(account_id) = serde_json::from_value::<AccountId>(json!(account_id)) {
                assert_eq!(
                    account_id,
                    serde_json::from_value(serde_json::to_value(&account_id).unwrap()).unwrap()
                );
            }
        }
    });
}
