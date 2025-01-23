use std::collections::HashMap;

use near_crypto::PublicKey;
use near_primitives_core::{
    account::{AccessKey, Account},
    types::AccountId,
};

#[derive(Default)]
pub struct HackyCache {
    data: HashMap<HackyCacheKey, HackyCacheValue>,
}

impl HackyCache {
    pub fn set(&mut self, key: HackyCacheKey, value: HackyCacheValue) -> Option<HackyCacheValue> {
        self.data.insert(key, value)
    }

    pub fn get(&self, key: &HackyCacheKey) -> Option<HackyCacheValue> {
        self.data.get(key).cloned()
    }
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub enum HackyCacheKey {
    AccessKey(AccountId, PublicKey),
    AccountId(AccountId),
}

#[derive(Clone)]
pub enum HackyCacheValue {
    AccessKey(AccessKey),
    Account(Account),
}
