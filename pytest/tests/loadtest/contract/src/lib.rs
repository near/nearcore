//! This contract implements simple counter backed by storage on blockchain.
//!
//! The contract provides methods to [increment] / [decrement] counter and
//! [get it's current value][get_num] or [reset].
//!
//! [increment]: struct.Counter.html#method.increment
//! [decrement]: struct.Counter.html#method.decrement
//! [get_num]: struct.Counter.html#method.get_num
//! [reset]: struct.Counter.html#method.reset

use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::collections::LookupMap;
use near_sdk::{env, near_bindgen};

near_sdk::setup_alloc!();

// add the following attributes to prepare your code for serialization and invocation on the blockchain
// More built-in Rust attributes here: https://doc.rust-lang.org/reference/attributes.html#built-in-attributes-index
#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct Counter {
    // See more data types at https://doc.rust-lang.org/book/ch03-02-data-types.html
    val: i8, // i8 is signed. unsigned integers are also available: u8, u16, u32, u64, u128
    records: LookupMap<String, String>,
}

impl Default for Counter {
    fn default() -> Self {
        Self { val: 0, records: LookupMap::new(b"r".to_vec()) }
    }
}
#[near_bindgen]
impl Counter {
    /// Returns 8-bit signed integer of the counter value.
    ///
    /// This must match the type from our struct's 'val' defined above.
    ///
    /// Note, the parameter is `&self` (without being mutable) meaning it doesn't modify state.
    /// In the frontend (/src/main.js) this is added to the "viewMethods" array
    /// using near-cli we can call this by:
    ///
    /// ```bash
    /// near view counter.YOU.testnet get_num
    /// ```
    pub fn get_num(&self) -> i8 {
        return self.val;
    }

    /// Increment the counter.
    ///
    /// Note, the parameter is "&mut self" as this function modifies state.
    /// In the frontend (/src/main.js) this is added to the "changeMethods" array
    /// using near-cli we can call this by:
    ///
    /// ```bash
    /// near call counter.YOU.testnet increment --accountId donation.YOU.testnet
    /// ```
    pub fn increment(&mut self) {
        // note: adding one like this is an easy way to accidentally overflow
        // real smart contracts will want to have safety checks
        // e.g. self.val = i8::wrapping_add(self.val, 1);
        // https://doc.rust-lang.org/std/primitive.i8.html#method.wrapping_add
        self.val += 1;
        let log_message = format!("Increased number to {}", self.val);
        env::log(log_message.as_bytes());
    }

    /// Reset to zero.
    pub fn reset(&mut self) {
        self.val = 0;
        // Another way to log is to cast a string into bytes, hence "b" below:
        env::log(b"Reset counter to zero");
    }

    // Similar to the methods above, but updating many fields (therefore using a lot more gas).
    pub fn increment_many(&mut self, how_many: u64) {
        for i in 1..how_many {
            let previous_val = match self.records.get(&i.to_string()) {
                Some(value) => value.parse::<i32>().unwrap(),
                None => 0,
            };

            self.records.insert(&i.to_string(), &(previous_val + 1).to_string());
        }
    }

    pub fn reset_increment_many(&mut self, how_many: u64) {
        for i in 1..how_many {
            self.records.insert(&i.to_string(), &(0).to_string());
        }
    }
    pub fn get_increment_many(&self) -> u64 {
        let i: i32 = 1;
        let previous_val = match self.records.get(&i.to_string()) {
            Some(value) => value.parse::<u64>().unwrap(),
            None => 0,
        };
        return previous_val;
    }
}
