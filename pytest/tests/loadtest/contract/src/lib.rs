//! Contract that can be used for different types of loadtesting.
//! Based on the rust-counter example.

use near_sdk::borsh::{self, BorshDeserialize, BorshSerialize};
use near_sdk::collections::LookupMap;
use near_sdk::{env, near_bindgen};

near_sdk::setup_alloc!();

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct Counter {
    val: u64,
    records: LookupMap<String, String>,
}

impl Default for Counter {
    fn default() -> Self {
        Self { val: 0, records: LookupMap::new(b"r".to_vec()) }
    }
}
#[near_bindgen]
impl Counter {
    pub fn get_num(&self) -> u64 {
        return self.val;
    }

    pub fn increment(&mut self) {
        self.val += 1;
        let log_message = format!("Increased number to {}", self.val);
        env::log(log_message.as_bytes());
    }

    pub fn reset(&mut self) {
        self.val = 0;
    }

    fn get_previous_val(&self, i: u64) -> u64 {
        match self.records.get(&i.to_string()) {
            Some(value) => value.parse::<u64>().unwrap(),
            None => 0,
        }
    }

    // Similar to the methods above, but updating many fields (therefore using a lot more gas).
    pub fn increment_many(&mut self, how_many: u64) {
        for i in 1..how_many {
            let previous_val = self.get_previous_val(i);
            self.records.insert(&i.to_string(), &(previous_val + 1).to_string());
        }
    }

    pub fn reset_increment_many(&mut self, how_many: u64) {
        for i in 1..how_many {
            self.records.insert(&i.to_string(), &(0).to_string());
        }
    }
    pub fn get_increment_many(&self) -> u64 {
        self.get_previous_val(1)
    }

    pub fn infinite_loop(&self) {
        loop {}
    }

    pub fn write_many(&mut self, how_many: u64) {
        for i in self.val..self.val + how_many {
            self.records.insert(&i.to_string(), &"a".to_string());
        }
        self.val += how_many;
    }
}
