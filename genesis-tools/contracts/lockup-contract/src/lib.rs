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
    pub fn stake(&mut self, amount: U128, public_key: StrPublicKey) {
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
    pub fn transfer(&mut self, amount: U128, account: String) {
        let amount = amount.into();
        Self::assert_self();
        assert!(
            amount <= self.get_transferrable().into(),
            "Not enough transferrable tokens to transfer."
        );
        Promise::new(account).transfer(amount);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;

    use near_bindgen::{testing_env, MockedBlockchain, VMContext};
    use std::convert::TryInto;
    use utils::test_utils::*;

    fn basic_setup() -> (VMContext, LockupContract) {
        let context = get_context(
            system_account(),
            to_yocto(LOCKUP_NEAR),
            0,
            to_ts(GENESIS_TIME_IN_DAYS),
            false,
        );
        testing_env!(context.clone());
        // Contract Setup:
        // - Now is genesis time.
        // - Lockup amount is 1000 near tokens.
        // - Lockup for 1 year.
        // - Owner has 2 keys
        let contract = LockupContract::new(
            to_yocto(LOCKUP_NEAR).into(),
            to_ts(GENESIS_TIME_IN_DAYS + YEAR).into(),
            vec![public_key(1).try_into().unwrap(), public_key(2).try_into().unwrap()],
        );
        (context, contract)
    }

    #[test]
    fn test_basic() {
        let (mut context, contract) = basic_setup();
        // Checking initial values at genesis time
        context.is_view = true;
        testing_env!(context.clone());

        assert_eq!(contract.get_transferrable().0, 0);

        // Checking values in 1 day after genesis time
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + 1);

        assert_eq!(contract.get_transferrable().0, 0);

        // Checking values next day after lockup timestamp
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + YEAR + 1);
        testing_env!(context.clone());

        assert_almost_eq(contract.get_transferrable().0, to_yocto(LOCKUP_NEAR));
    }

    #[test]
    fn test_transferrable_with_different_stakes() {
        let (mut context, contract) = basic_setup();

        // Staking everything at the genesis
        context.account_locked_balance = to_yocto(999);
        context.account_balance = to_yocto(1);

        // Checking values in 1 day after genesis time
        context.is_view = true;

        for stake in &[1, 10, 100, 500, 999, 1001, 1005, 1100, 1500, 1999, 3000] {
            let stake = *stake;
            context.account_locked_balance = to_yocto(stake);
            let balance_near = std::cmp::max(1000u128.saturating_sub(stake), 1);
            context.account_balance = to_yocto(balance_near);
            let extra_balance_near = stake + balance_near - 1000;

            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + 1);
            testing_env!(context.clone());

            assert_eq!(contract.get_transferrable().0, to_yocto(extra_balance_near));

            // Checking values next day after lockup timestamp
            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + YEAR + 1);
            testing_env!(context.clone());

            assert_almost_eq(
                contract.get_transferrable().0,
                to_yocto(LOCKUP_NEAR + extra_balance_near),
            );
        }
    }

    #[test]
    fn test_transfer_call_by_owner() {
        let (mut context, mut contract) = basic_setup();
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + YEAR + 1);
        context.is_view = true;
        testing_env!(context.clone());
        assert_almost_eq(contract.get_transferrable().0, to_yocto(LOCKUP_NEAR));

        context.predecessor_account_id = account_owner();
        context.signer_account_id = account_owner();
        context.signer_account_pk = public_key(1);
        context.is_view = false;
        testing_env!(context.clone());

        assert_eq!(env::account_balance(), to_yocto(LOCKUP_NEAR));
        contract.transfer(to_yocto(100).into(), non_owner());
        assert_almost_eq(env::account_balance(), to_yocto(LOCKUP_NEAR - 100));
    }

    #[test]
    fn test_stake_call_by_owner() {
        let (mut context, mut contract) = basic_setup();
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + YEAR + 1);
        context.is_view = true;
        testing_env!(context.clone());
        assert_almost_eq(contract.get_transferrable().0, to_yocto(LOCKUP_NEAR));

        context.predecessor_account_id = account_owner();
        context.signer_account_id = account_owner();
        context.signer_account_pk = public_key(1);
        context.is_view = false;
        testing_env!(context.clone());

        assert_eq!(env::account_balance(), to_yocto(LOCKUP_NEAR));
        contract.stake(to_yocto(100).into(), public_key(10).try_into().unwrap());
        assert_almost_eq(env::account_balance(), to_yocto(LOCKUP_NEAR));
    }

    #[test]
    fn test_transfer_by_non_owner() {
        let (mut context, mut contract) = basic_setup();

        context.predecessor_account_id = non_owner();
        context.signer_account_id = non_owner();
        context.signer_account_pk = public_key(5);
        testing_env!(context.clone());

        std::panic::catch_unwind(move || {
            contract.transfer(to_yocto(100).into(), non_owner());
        })
        .unwrap_err();
    }

    #[test]
    fn test_stake_by_non_owner() {
        let (mut context, mut contract) = basic_setup();

        context.predecessor_account_id = non_owner();
        context.signer_account_id = non_owner();
        context.signer_account_pk = public_key(5);
        testing_env!(context.clone());

        std::panic::catch_unwind(move || {
            contract.stake(to_yocto(100).into(), public_key(4).try_into().unwrap());
        })
        .unwrap_err();
    }
}
