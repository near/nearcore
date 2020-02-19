//! A smart contract that allows tokens lockup.

use borsh::{BorshDeserialize, BorshSerialize};
use common::key_management::{KeyType, PublicKey};
use near_bindgen::collections::Map;
use near_bindgen::{env, near_bindgen, Promise};

mod lockup_transfer_rules;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct LockupContract {
    lockup_amount: u128,
    lockup_timestamp: u64,
    keys: Map<PublicKey, KeyType>,
}

impl Default for LockupContract {
    fn default() -> Self {
        env::panic(b"The contract is not initialized.");
    }
}

#[near_bindgen]
impl LockupContract {
    /// Check that all timestamps are strictly in the future.
    fn check_timestamps_future(timestamps: &[u64]) {
        let block_timestamp = env::block_timestamp();
        for stamp in timestamps {
            assert!(*stamp > block_timestamp, "All timestamps should be strictly in the future.");
        }
    }

    /// Initializes an account with the given lockup amount, lockup timestamp (when it
    /// expires), and the keys that it needs to add
    #[init]
    pub fn new(
        lockup_amount: u128,
        lockup_timestamp: u64,
        initial_keys: Vec<(PublicKey, KeyType)>,
    ) -> Self {
        let mut res = Self { lockup_amount, lockup_timestamp, keys: Default::default() };
        // It does not make sense to have a lockup contract if the unlocking is in the past.
        Self::check_timestamps_future(&[lockup_timestamp]);
        for (key, key_type) in initial_keys {
            res.add_key_no_check(key, key_type);
        }
        res
    }

    /// Get the key type of the key used to sign the transaction.
    fn signer_key_type(&self) -> KeyType {
        let signer_key = env::signer_account_pk();
        self.get_key_type(&signer_key).expect("Key of the signer was not recorded.")
    }

    /// Get the key type of the given key.
    fn get_key_type(&self, key: &PublicKey) -> Option<KeyType> {
        self.keys.get(key)
    }

    /// Adds the key both to the internal collection and through promise without
    /// checking the permissions of the signer.
    fn add_key_no_check(&mut self, key: PublicKey, key_type: KeyType) {
        self.keys.insert(&key, &key_type);
        let account_id = env::current_account_id();
        Promise::new(account_id.clone()).add_access_key(
            key,
            0,
            account_id,
            key_type.allowed_methods().to_vec(),
        );
    }

    /// Add the new access key. The signer access key should have permission to do it.
    pub fn add_key(&mut self, key: PublicKey, key_type: KeyType) {
        self.signer_key_type().check_can_add_remove(&key_type);
        self.add_key_no_check(key, key_type);
    }

    /// Remove a known access key. The signer access key should have enough permission to do it.
    pub fn remove_key(&mut self, key: PublicKey) {
        let key_type_to_remove = self.get_key_type(&key).expect("Cannot remove unknown key");
        self.signer_key_type().check_can_add_remove(&key_type_to_remove);
        Promise::new(env::current_account_id()).delete_key(key);
    }

    /// Create a staking transaction on behalf of this account.
    pub fn stake(&self, amount: u128, public_key: PublicKey) {
        assert!(
            amount <= env::account_balance() + env::account_locked_balance(),
            "Not enough balance to stake."
        );
        Promise::new(env::current_account_id()).stake(amount, public_key);
    }

    /// Get the amount of tokens that can be transferred.
    pub fn get_transferrable(&self) -> u128 {
        lockup_transfer_rules::get_transferrable_amount(self.lockup_amount, self.lockup_timestamp)
    }

    /// Transfer amount to another account.
    pub fn transfer(&self, amount: u128, account: String) {
        assert!(amount <= self.get_transferrable(), "Not enough transferrable tokens to transfer.");
        Promise::new(account).transfer(amount);
    }
}
