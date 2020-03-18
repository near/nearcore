//! A smart contract that allows tokens lockup.

use borsh::{BorshDeserialize, BorshSerialize};
use near_bindgen::{env, near_bindgen, Promise};
use utils::{StrPublicKey, U128, U64};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct LockupContract {
    /// The amount in yacto-NEAR tokens locked for this account.
    lockup_amount: u128,
    /// The timestamp in nanoseconds when the lockup amount of tokens will be available.
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
    pub fn new(lockup_amount: U128, lockup_timestamp: U64, public_keys: Vec<StrPublicKey>) -> Self {
        let lockup_amount = lockup_amount.into();
        let lockup_timestamp = lockup_timestamp.into();
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
                public_key.into(),
                0,
                account_id.clone(),
                vec![],
            );
        }
        res
    }

    /// Create a staking transaction on behalf of this account.
    pub fn stake(&self, amount: U128, public_key: StrPublicKey) {
        let amount = amount.into();
        Self::assert_self();
        assert!(
            amount <= env::account_balance() + env::account_locked_balance(),
            "Not enough balance to stake."
        );
        Promise::new(env::current_account_id()).stake(amount, public_key.into());
    }

    /// Get the amount of tokens that can be transferred.
    pub fn get_transferrable(&self) -> U128 {
        let total_balance = env::account_balance() + env::account_locked_balance();
        if self.lockup_timestamp <= env::block_timestamp() {
            // entire balance is unlocked
            total_balance.into()
        } else {
            // some balance is still locked
            total_balance.saturating_sub(self.lockup_amount).into()
        }
    }

    /// Transfer amount to another account.
    pub fn transfer(&self, amount: U128, account: String) {
        let amount = amount.into();
        Self::assert_self();
        assert!(
            amount <= self.get_transferrable().into(),
            "Not enough transferrable tokens to transfer."
        );
        Promise::new(account).transfer(amount);
    }
}
