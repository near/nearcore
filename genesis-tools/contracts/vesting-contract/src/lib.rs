//! A smart contract that allows tokens vesting and lockup.

use borsh::{BorshDeserialize, BorshSerialize};
use key_management::{KeyType, PublicKey};
use near_bindgen::{env, near_bindgen, Promise};
use uint::construct_uint;

construct_uint! {
    /// 256-bit unsigned integer.
    pub struct U256(4);
}

mod key_management;
// mod lockup_vesting_transfer_rules;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct VestingContract {
    lockup_amount: u128,
    lockup_timestamp: u64,
    /// Whether this account is disallowed to stake anymore.
    permanently_unstaked: bool,
    vesting_start_timestamp: u64,
    vesting_cliff_timestamp: u64,
    vesting_end_timestamp: u64,
}

impl Default for VestingContract {
    fn default() -> Self {
        env::panic(b"The contract is not initialized.");
    }
}

#[near_bindgen]
impl VestingContract {
    /// Check that all timestamps are strictly in the future.
    fn check_timestamps_future(timestamps: &[u64]) {
        let block_timestamp = env::block_timestamp();
        for stamp in timestamps {
            assert!(*stamp > block_timestamp, "All timestamps should be strictly in the future.");
        }
    }

    /// Check that the timestamps are monotonically non-decreasing.
    fn check_timestamp_ordering(timestamps: &[u64]) {
        for i in 1..timestamps.len() {
            assert!(
                timestamps[i - 1] <= timestamps[i],
                "Timestamps should be monotonically non-decreasing."
            );
        }
    }

    fn assert_self() {
        assert_eq!(env::predecessor_account_id(), env::current_account_id());
    }

    /// Initializes an account with the given lockup amount, lockup timestamp (when it
    /// expires), and the keys that it needs to add
    /// Same initialization method as above, but with vesting functionality.
    #[init]
    pub fn new(
        lockup_amount: u128,
        lockup_timestamp: u64,
        vesting_start_timestamp: u64,
        vesting_cliff_timestamp: u64,
        vesting_end_timestamp: u64,
        owner_public_keys: Vec<PublicKey>,
        foundation_public_keys: Vec<PublicKey>,
    ) -> Self {
        assert!(
            near_bindgen::env::state_read::<VestingContract>().is_none(),
            "The contract is already initialized"
        );
        let res = Self {
            lockup_amount,
            lockup_timestamp,
            vesting_start_timestamp,
            vesting_cliff_timestamp,
            vesting_end_timestamp,
            permanently_unstaked: false,
        };
        // It is okay for vesting start and vesting cliff to be in the past, but it does not
        // makes sense to have lockup or vesting end to be in the past.
        Self::check_timestamps_future(&[lockup_timestamp, vesting_end_timestamp]);
        // The cliff should be between start and end of the vesting, potentially inclusive.
        Self::check_timestamp_ordering(&[
            vesting_start_timestamp,
            vesting_cliff_timestamp,
            vesting_end_timestamp,
        ]);
        // The lockup should be after start of the vesting, potentially inclusive.
        Self::check_timestamp_ordering(&[vesting_start_timestamp, lockup_timestamp]);
        let account_id = env::current_account_id();
        for public_key in owner_public_keys {
            Promise::new(account_id.clone()).add_access_key(
                public_key,
                0,
                account_id.clone(),
                KeyType::Owner.allowed_methods(),
            );
        }
        for public_key in foundation_public_keys {
            Promise::new(account_id.clone()).add_access_key(
                public_key,
                0,
                account_id.clone(),
                KeyType::Foundation.allowed_methods(),
            );
        }
        res
    }

    /// Create a staking transaction on behalf of this account.
    pub fn stake(&self, amount: u128, public_key: PublicKey) {
        Self::assert_self();
        assert!(!self.permanently_unstaked, "The account was permanently unstaked.");
        assert!(
            amount <= env::account_balance() + env::account_locked_balance(),
            "Not enough balance to stake."
        );
        Promise::new(env::current_account_id()).stake(amount, public_key);
    }

    /// Get the amount of tokens that were vested.
    pub fn get_unvested(&self) -> u128 {
        let block_timestamp = env::block_timestamp();
        if block_timestamp < self.vesting_cliff_timestamp {
            self.lockup_amount
        } else if block_timestamp >= self.vesting_end_timestamp {
            0
        } else {
            let time_left = U256::from(self.vesting_end_timestamp - block_timestamp);
            let total_time = U256::from(self.vesting_end_timestamp - self.vesting_start_timestamp);
            let unvested_u256 = U256::from(self.lockup_amount) * time_left / total_time;
            unvested_u256.as_u128()
        }
    }

    /// Get the amount of transferrable tokens that this account has. Takes vesting into account.
    pub fn get_transferrable(&self) -> u128 {
        let total_balance = env::account_balance() + env::account_locked_balance();
        if self.lockup_timestamp >= env::block_timestamp() {
            // some balance might be still unvested
            total_balance.saturating_sub(self.get_unvested())
        } else {
            // entire initial balance is locked
            total_balance.saturating_sub(self.lockup_amount)
        }
    }

    /// Transfer amount to another account.
    pub fn transfer(&self, amount: u128, account: String) {
        Self::assert_self();
        assert!(amount <= self.get_transferrable(), "Not enough transferrable tokens to transfer.");
        Promise::new(account).transfer(amount);
    }

    /// Permanently unstake this account disallowing it to stake. This is usually done
    /// in preparation of terminating this account. Unfortunately, unstaking and termination
    /// cannot be done atomically, because unstaking takes unknown amount of time.
    pub fn permanently_unstake(&mut self, key: PublicKey) {
        Self::assert_self();
        Promise::new(env::current_account_id()).stake(0, key);
        self.permanently_unstaked = true;
    }

    /// Stop vesting and transfer all unvested tokens to a beneficiary.
    pub fn terminate(&mut self, beneficiary_id: String) {
        Self::assert_self();
        let unvested = self.get_unvested();
        self.lockup_amount -= unvested;
        self.vesting_end_timestamp = env::block_timestamp();
        Promise::new(beneficiary_id).transfer(unvested);
    }
}
