#![no_main]

use std::str;

use libfuzzer_sys::fuzz_target;
use near_account_id::AccountId;
use serde_json::json;

fuzz_target!(|bytes: &[u8]| {
    if let Ok(account_id) = str::from_utf8(bytes) {
        if let Ok(account_id) = serde_json::from_value::<AccountId>(json!(account_id)) {
            assert_eq!(
                account_id,
                serde_json::from_value(serde_json::to_value(&account_id).unwrap()).unwrap()
            );
        }
    }
});
