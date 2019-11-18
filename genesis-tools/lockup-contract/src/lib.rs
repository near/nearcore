//! A smart contract that allows lockup of a smart contract.
use borsh::{BorshDeserialize, BorshSerialize};
use key_management::{KeyType, PublicKey};
use near_bindgen::{env, near_bindgen, Promise};
use std::collections::{HashMap, HashSet};

mod key_management;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[near_bindgen]
#[derive(Default, BorshDeserialize, BorshSerialize)]
pub struct LockupContract {
    lockup_amount: u128,
    lockup_timestamp: u64,
    keys: HashMap<KeyType, HashSet<PublicKey>>,
    /// Whether this account is disallowed to stake anymore.
    permanently_unstaked: bool,
    #[cfg(feature = "vesting")]
    vesting_start_timestamp: u64,
    #[cfg(feature = "vesting")]
    vesting_cliff_timestamp: u64,
    #[cfg(feature = "vesting")]
    vesting_end_timestamp: u64,
}

#[near_bindgen(init => new)]
impl LockupContract {
    /// Check that all timestamps are strictly in the future.
    fn check_timestamps_future(timestamps: &[u64]) {
        let block_timestamp = env::block_timestamp();
        for stamp in timestamps {
            assert!(*stamp > block_timestamp, "All timestamps should be strictly in the future.");
        }
    }

    /// Check that the timestamps are monotonically non-decreasing.
    #[cfg(feature = "vesting")]
    fn check_timestamp_ordering(timestamps: &[u64]) {
        for i in 1..timestamps.len() {
            assert!(
                timestamps[i - 1] <= timestamps[i],
                "Timestamps should be monotonically non-decreasing."
            );
        }
    }

    /// Initializes an account with the given lockup amount, lockup timestamp (when it
    /// expires), and the keys that it needs to add
    #[cfg(not(feature = "vesting"))]
    pub fn new(
        lockup_amount: u128,
        lockup_timestamp: u64,
        initial_keys: Vec<(KeyType, PublicKey)>,
    ) -> Self {
        let mut res = Self { lockup_amount, lockup_timestamp, ..Default::default() };
        Self::check_timestamps_future(&[lockup_timestamp]);
        for (key_type, key) in initial_keys {
            res.add_key_no_check(key_type, key);
        }
        res
    }

    /// Same initialization method as above, but with vesting functionality.
    #[cfg(feature = "vesting")]
    pub fn new(
        lockup_amount: u128,
        lockup_timestamp: u64,
        vesting_start_timestamp: u64,
        vesting_cliff_timestamp: u64,
        vesting_end_timestamp: u64,
        initial_keys: Vec<(KeyType, PublicKey)>,
    ) -> Self {
        let mut res = Self {
            lockup_amount,
            lockup_timestamp,
            vesting_start_timestamp,
            vesting_cliff_timestamp,
            vesting_end_timestamp,
            ..Default::default()
        };
        Self::check_timestamps_future(&[
            lockup_timestamp,
            vesting_start_timestamp,
            vesting_cliff_timestamp,
            vesting_end_timestamp,
        ]);
        // The cliff should be between start and end of the vesting, potentially inclusive.
        Self::check_timestamp_ordering(&[
            vesting_start_timestamp,
            vesting_cliff_timestamp,
            vesting_end_timestamp,
        ]);
        // The lockup should be after start of the vesting, potentially inclusive.
        Self::check_timestamp_ordering(&[vesting_start_timestamp, lockup_timestamp]);
        for (key_type, key) in initial_keys {
            res.add_key_no_check(key_type, key);
        }
        res
    }

    /// Get the key type of the key used to sign the transaction.
    fn signer_key_type(&self) -> KeyType {
        let signer_key = env::signer_account_pk();
        for (key_type, keys) in &self.keys {
            if keys.contains(&signer_key) {
                return *key_type;
            }
        }
        panic!("Key of the signer was not recorded.")
    }

    /// Get the key type of the given key.
    fn get_key_type(&self, key: &PublicKey) -> Option<KeyType> {
        self.keys
            .iter()
            .find_map(|(key_type, keys)| if keys.contains(key) { Some(*key_type) } else { None })
    }

    /// Adds the key both to the internal collection and through promise without
    /// checking the permissions of the signer.
    fn add_key_no_check(&mut self, key_type: KeyType, key: PublicKey) {
        self.keys.entry(key_type).or_default().insert(key.clone());
        match key_type {
            KeyType::Full => {
                let account_id = env::current_account_id();
                Promise::new(account_id).add_full_access_key(key);
            }
            key_type @ _ => {
                let account_id = env::current_account_id();
                Promise::new(account_id.clone()).add_access_key(
                    key,
                    0,
                    account_id,
                    key_type.allowed_methods().to_vec(),
                );
            }
        }
    }

    /// Add the new access key. The signer access key should have permission to do it.
    pub fn add_key(&mut self, key_type: KeyType, key: PublicKey) {
        self.signer_key_type().check_can_add_remove(&key_type);
        self.add_key_no_check(key_type, key);
    }

    /// Remove a known access key. The signer access key should have enough permission to do it.
    pub fn remove_key(&mut self, key: PublicKey) {
        let key_type_to_remove = self.get_key_type(&key).expect("Cannot remove unknown key");
        self.signer_key_type().check_can_add_remove(&key_type_to_remove);
        Promise::new(env::current_account_id()).delete_key(key);
    }

    /// Create a staking transaction on behalf of this account.
    pub fn stake(&self, amount: u128, public_key: PublicKey) {
        assert!(!self.permanently_unstaked, "The account was permanently unstaked.");
        assert!(amount <= env::account_balance(), "Not enough balance to stake.");
        Promise::new(env::current_account_id()).stake(amount, public_key);
    }

    /// Get the amount of liquid tokens that this account has.
    #[cfg(not(feature = "vesting"))]
    pub fn get_liquid(&self) -> u128 {
        if env::block_timestamp() >= self.lockup_timestamp {
            env::account_balance()
        } else {
            env::account_balance() - self.lockup_amount
        }
    }

    /// Get the amount of tokens that were vested.
    #[cfg(feature = "vesting")]
    pub fn get_vested(&self) -> u128 {
        let block_timestamp = env::block_timestamp();
        if block_timestamp <= self.vesting_cliff_timestamp {
            0
        } else if block_timestamp >= self.vesting_end_timestamp {
            self.lockup_amount
        } else {
            (block_timestamp - self.vesting_start_timestamp) as u128 * self.lockup_amount
                / (self.vesting_end_timestamp - self.vesting_start_timestamp) as u128
        }
    }

    /// Get the amount of liquid tokens that this account has. Takes vesting into account.
    #[cfg(feature = "vesting")]
    pub fn get_liquid(&self) -> u128 {
        let vested_tokens = self.get_vested();
        if env::block_timestamp() >= self.lockup_timestamp {
            env::account_balance() - self.lockup_amount + vested_tokens
        } else {
            env::account_balance() - self.lockup_amount
        }
    }

    /// Transfer liquid amount to another account.
    pub fn transfer(&self, amount: u128, account: String) {
        assert!(amount <= self.get_liquid(), "Not enough liquid tokens to transfer.");
        Promise::new(account).transfer(amount);
    }

    /// Permanently unstake this account disallowing it to stake. This is usually done
    /// in preparation of terminating this account. Unfortunately, unstaking and termination
    /// cannot be done atomically, because unstaking takes unknown amount of time.
    pub fn permanently_unstake(&mut self, key: PublicKey) {
        Promise::new(env::current_account_id()).stake(0, key);
        self.permanently_unstaked = true;
    }

    /// Stop vesting and transfer all unvested tokens to a beneficiary.
    #[cfg(feature = "vesting")]
    pub fn terminate(&mut self, beneficiary_id: String) {
        let vested = self.get_vested();
        let unvested = self.lockup_amount - vested;
        self.lockup_amount = vested;
        self.vesting_end_timestamp = env::block_timestamp();
        Promise::new(beneficiary_id).transfer(unvested);
    }
}
