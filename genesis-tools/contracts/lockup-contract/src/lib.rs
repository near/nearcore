//! A smart contract that allows tokens lockup.

use borsh::{BorshDeserialize, BorshSerialize};
use near_bindgen::{env, near_bindgen, Promise};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

/// A key is a sequence of bytes, potentially including the prefix determining the cryptographic type
/// of the key. For forward compatibility we do not enforce any specific length.
type PublicKey = Vec<u8>;

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct LockupContract {
    lockup_amount: u128,
    lockup_timestamp: u64,
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
        for timestamp in timestamps {
            assert!(
                *timestamp > block_timestamp,
                "All timestamps should be strictly in the future."
            );
        }
    }

    fn assert_self() {
        assert_eq!(env::predecessor_account_id(), env::current_account_id());
    }

    /// Initializes an account with the given lockup amount, lockup timestamp (when it
    /// expires), and the keys that it needs to add
    #[init]
    pub fn new(lockup_amount: u128, lockup_timestamp: u64, public_keys: Vec<PublicKey>) -> Self {
        assert!(
            near_bindgen::env::state_read::<LockupContract>().is_none(),
            "The contract is already initialized"
        );
        let res = Self { lockup_amount, lockup_timestamp };
        // It does not make sense to have a lockup contract if the unlocking is in the past.
        Self::check_timestamps_future(&[lockup_timestamp]);
        let account_id = env::current_account_id();
        for public_key in public_keys {
            Promise::new(account_id.clone()).add_access_key(
                public_key,
                0,
                account_id.clone(),
                vec![],
            );
        }
        res
    }

    /// Create a staking transaction on behalf of this account.
    pub fn stake(&self, amount: u128, public_key: PublicKey) {
        Self::assert_self();
        assert!(
            amount <= env::account_balance() + env::account_locked_balance(),
            "Not enough balance to stake."
        );
        Promise::new(env::current_account_id()).stake(amount, public_key);
    }

    /// Get the amount of tokens that can be transferred.
    pub fn get_transferrable(&self) -> u128 {
        let total_balance = env::account_balance() + env::account_locked_balance();
        if self.lockup_timestamp >= env::block_timestamp() {
            // entire balance is unlocked
            total_balance
        } else {
            // some balance is still locked
            total_balance.saturating_sub(self.lockup_amount)
        }
    }

    /// Transfer amount to another account.
    pub fn transfer(&self, amount: u128, account: String) {
        Self::assert_self();
        assert!(amount <= self.get_transferrable(), "Not enough transferrable tokens to transfer.");
        Promise::new(account).transfer(amount);
    }
}
