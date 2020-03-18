//! A smart contract that allows tokens vesting and lockup.

use borsh::{BorshDeserialize, BorshSerialize};
use near_bindgen::{env, near_bindgen, Promise};
use uint::construct_uint;
use utils::{StrPublicKey, U128, U64};

construct_uint! {
    /// 256-bit unsigned integer.
    pub struct U256(4);
}

/// Key type defines the access for the key owner.
pub enum KeyType {
    /// The key belongs to the owner of the Vesting Contract. The key provides the ability to
    /// stake and withdraw available funds.
    Owner,
    /// The key belongs to the NEAR foundation. The key provides ability to stop vesting in
    /// case the contract owner's vesting agreement was terminated, e.g. the employee left the
    /// company before the end of the vesting period.
    Foundation,
}

impl KeyType {
    /// Provides allowed methods name in a comma separated format used by `FunctionCallPermission`
    /// within an `AccessKey`.
    pub fn allowed_methods(&self) -> Vec<u8> {
        match self {
            KeyType::Owner => b"stake,transfer".to_vec(),
            KeyType::Foundation => b"permanently_unstake,terminate".to_vec(),
        }
    }
}

/// A key is a sequence of bytes, potentially including the prefix determining the cryptographic type
/// of the key. For forward compatibility we do not enforce any specific length.
pub type PublicKey = Vec<u8>;

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct VestingContract {
    /// The amount in yacto-NEAR tokens locked for this account.
    lockup_amount: u128,
    /// The timestamp in nanoseconds when the vested amount of tokens will be available.
    lockup_timestamp: u64,
    /// Whether this account is disallowed to stake anymore.
    /// In case the vesting contract is terminated, the foundation will issue a unstaking
    /// transaction and prevent account from staking again. This is needed to be able to withdraw
    /// the remaining unvested amount.
    permanently_unstaked: bool,
    /// The timestamp in nanosecond when the vesting starts. E.g. the start date of employment.
    vesting_start_timestamp: u64,
    /// The timestamp in nanosecond when the first part of lockup tokens becomes vested.
    /// The remaining tokens will vest continuously until they are fully vested.
    /// Example: a 1 year of employment at which moment the 1/4 of tokens become vested.
    vesting_cliff_timestamp: u64,
    /// The timestamp in nanosecond when the vesting ends.
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
        lockup_amount: U128,
        lockup_timestamp: U64,
        vesting_start_timestamp: U64,
        vesting_cliff_timestamp: U64,
        vesting_end_timestamp: U64,
        owner_public_keys: Vec<StrPublicKey>,
        foundation_public_keys: Vec<StrPublicKey>,
    ) -> Self {
        let lockup_amount = lockup_amount.into();
        let lockup_timestamp = lockup_timestamp.into();
        let vesting_start_timestamp = vesting_start_timestamp.into();
        let vesting_cliff_timestamp = vesting_cliff_timestamp.into();
        let vesting_end_timestamp = vesting_end_timestamp.into();
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
                public_key.into(),
                0,
                account_id.clone(),
                KeyType::Owner.allowed_methods(),
            );
        }
        for public_key in foundation_public_keys {
            Promise::new(account_id.clone()).add_access_key(
                public_key.into(),
                0,
                account_id.clone(),
                KeyType::Foundation.allowed_methods(),
            );
        }
        res
    }

    /// Create a staking transaction on behalf of this account.
    pub fn stake(&mut self, amount: U128, public_key: StrPublicKey) {
        let amount = amount.into();
        Self::assert_self();
        assert!(!self.permanently_unstaked, "The account was permanently unstaked.");
        assert!(
            amount <= env::account_balance() + env::account_locked_balance(),
            "Not enough balance to stake."
        );
        Promise::new(env::current_account_id()).stake(amount, public_key.into());
    }

    /// Returns a portion of `self.lockup_amount` which is not yet available to spend due to vesting
    /// The entire `self.lockup_amount` is locked up to `self.vesting_cliff_timestamp`
    pub fn get_unvested(&self) -> U128 {
        let block_timestamp = env::block_timestamp();
        if block_timestamp < self.vesting_cliff_timestamp {
            self.lockup_amount.into()
        } else if block_timestamp >= self.vesting_end_timestamp {
            0.into()
        } else {
            // cannot overflow since block_timestamp >= self.vesting_end_timestamp
            let time_left = U256::from(self.vesting_end_timestamp - block_timestamp);
            let total_time = U256::from(self.vesting_end_timestamp - self.vesting_start_timestamp);
            let unvested_u256 = U256::from(self.lockup_amount) * time_left / total_time;
            unvested_u256.as_u128().into()
        }
    }

    /// Get the amount of transferrable tokens that this account has. Takes vesting into account.
    pub fn get_transferrable(&self) -> U128 {
        let total_balance = env::account_balance() + env::account_locked_balance();
        if self.lockup_timestamp <= env::block_timestamp() {
            // some balance might be still unvested
            total_balance.saturating_sub(self.get_unvested().into()).into()
        } else {
            // entire initial balance is locked
            total_balance.saturating_sub(self.lockup_amount).into()
        }
    }

    /// Transfer amount to another account.
    pub fn transfer(&mut self, amount: U128, receiver_id: String) {
        let amount = amount.into();
        Self::assert_self();
        assert!(
            amount <= self.get_transferrable().into(),
            "Not enough transferrable tokens to transfer."
        );
        Promise::new(receiver_id).transfer(amount);
    }

    /// Permanently unstake this account disallowing it to stake. This is usually done
    /// in preparation of terminating this account. Unfortunately, unstaking and termination
    /// cannot be done atomically, because unstaking takes unknown amount of time.
    pub fn permanently_unstake(&mut self, key: StrPublicKey) {
        Self::assert_self();
        assert!(!self.permanently_unstaked, "The account was permanently unstaked.");
        Promise::new(env::current_account_id()).stake(0, key.into());
        self.permanently_unstaked = true;
    }

    /// Stop vesting and transfer all unvested tokens to a beneficiary.
    pub fn terminate(&mut self, beneficiary_id: String) {
        Self::assert_self();
        let unvested = self.get_unvested().into();
        self.lockup_amount -= unvested;
        self.vesting_end_timestamp = env::block_timestamp();
        Promise::new(beneficiary_id).transfer(unvested);
    }
}

#[cfg(not(target_arch = "wasm32"))]
#[cfg(test)]
mod tests {
    use super::*;

    use near_bindgen::MockedBlockchain;
    use near_bindgen::{testing_env, VMContext};
    use std::convert::TryInto;

    type AccountId = String;

    const LOCKUP_NEAR: u128 = 1000;
    const GENESIS_TIME_IN_DAYS: u64 = 500;
    const YEAR: u64 = 365;
    const ALMOST_HALF_YEAR: u64 = YEAR / 2;

    fn system_account() -> AccountId {
        "system".to_string()
    }
    fn account_owner() -> AccountId {
        "account_owner".to_string()
    }
    fn non_owner() -> AccountId {
        "non_owner".to_string()
    }
    fn public_key(byte_val: u8) -> PublicKey {
        let mut pk = vec![byte_val; 33];
        pk[0] = 0;
        pk
    }

    fn to_yacto(near_balance: u128) -> u128 {
        near_balance * 10u128.pow(24)
    }

    fn to_ts(num_days: u64) -> u64 {
        // 2018-08-01 UTC in nanoseconds
        1533081600000000000 + num_days * 86400000000000
    }

    fn assert_almost_eq_with_max_delta(left: u128, right: u128, max_delta: u128) {
        assert!(
            std::cmp::max(left, right) - std::cmp::min(left, right) < max_delta,
            format!(
                "Left {} is not even close to Right {} within delta {}",
                left, right, max_delta
            )
        );
    }

    fn assert_almost_eq(left: u128, right: u128) {
        assert_almost_eq_with_max_delta(left, right, to_yacto(10));
    }

    fn get_context(
        predecessor_account_id: AccountId,
        account_balance: u128,
        account_locked_balance: u128,
        block_timestamp: u64,
        is_view: bool,
    ) -> VMContext {
        VMContext {
            current_account_id: account_owner(),
            signer_account_id: predecessor_account_id.clone(),
            signer_account_pk: vec![0, 1, 2],
            predecessor_account_id,
            input: vec![],
            block_index: 0,
            block_timestamp,
            account_balance,
            account_locked_balance,
            storage_usage: 10u64.pow(6),
            attached_deposit: 0,
            prepaid_gas: 10u64.pow(15),
            random_seed: vec![0, 1, 2],
            is_view,
            output_data_receivers: vec![],
        }
    }

    fn basic_setup() -> (VMContext, VestingContract) {
        let context = get_context(
            system_account(),
            to_yacto(LOCKUP_NEAR),
            0,
            to_ts(GENESIS_TIME_IN_DAYS),
            false,
        );
        testing_env!(context.clone());
        // Contract Setup:
        // - Now is genesis time.
        // - Lockup amount is 1000 near tokens.
        // - Lockup for 1 year.
        // - Vesting started half a year ago.
        // - Vesting cliff is 1 year from the start date.
        // - Vesting ends in 1 year from the start date.
        // - Owner has 2 keys
        // - Foundation has 1 key
        let contract = VestingContract::new(
            to_yacto(LOCKUP_NEAR).into(),
            to_ts(GENESIS_TIME_IN_DAYS + YEAR).into(),
            to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR).into(),
            to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR).into(),
            to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR * 4).into(),
            vec![public_key(1).try_into().unwrap(), public_key(2).try_into().unwrap()],
            vec![public_key(3).try_into().unwrap()],
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
        assert_eq!(contract.get_unvested().0, to_yacto(LOCKUP_NEAR));

        // Checking values in 1 day after genesis time
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + 1);
        testing_env!(context.clone());

        assert_eq!(contract.get_transferrable().0, 0);
        assert_eq!(contract.get_unvested().0, to_yacto(LOCKUP_NEAR));

        // Checking values next day after cliff but before lockup timestamp
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR + 1);
        testing_env!(context.clone());

        assert_eq!(contract.get_transferrable().0, 0);
        assert_almost_eq(contract.get_unvested().0, to_yacto(750));

        // Checking values next day after lockup timestamp
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + YEAR + 1);
        testing_env!(context.clone());

        assert_almost_eq(contract.get_transferrable().0, to_yacto(375));
        assert_almost_eq(contract.get_unvested().0, to_yacto(625));

        // Checking values middle of vesting
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR * 2);
        testing_env!(context.clone());

        assert_almost_eq(contract.get_transferrable().0, to_yacto(500));
        assert_almost_eq(contract.get_unvested().0, to_yacto(500));

        // Checking values a day before vesting ends
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR * 4 - 1);
        testing_env!(context.clone());

        assert_almost_eq(contract.get_transferrable().0, to_yacto(1000));
        assert_almost_eq(contract.get_unvested().0, to_yacto(0));

        // Checking values a day after vesting ends
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR * 4 + 1);
        testing_env!(context.clone());

        assert_almost_eq(contract.get_transferrable().0, to_yacto(1000));
        assert_almost_eq(contract.get_unvested().0, to_yacto(0));
    }

    #[test]
    fn test_transferrable_with_different_stakes() {
        let (mut context, contract) = basic_setup();

        // Staking everything at the genesis
        context.account_locked_balance = to_yacto(999);
        context.account_balance = to_yacto(1);

        // Checking values in 1 day after genesis time
        context.is_view = true;

        for stake in &[1, 10, 100, 500, 999, 1001, 1005, 1100, 1500, 1999, 3000] {
            let stake = *stake;
            context.account_locked_balance = to_yacto(stake);
            let balance_near = std::cmp::max(1000u128.saturating_sub(stake), 1);
            context.account_balance = to_yacto(balance_near);
            let extra_balance_near = stake + balance_near - 1000;

            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + 1);
            testing_env!(context.clone());

            assert_eq!(contract.get_transferrable().0, to_yacto(extra_balance_near));

            // Checking values next day after cliff but before lockup timestamp
            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR + 1);
            testing_env!(context.clone());

            assert_eq!(contract.get_transferrable().0, to_yacto(extra_balance_near));

            // Checking values next day after lockup timestamp
            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + YEAR + 1);
            testing_env!(context.clone());

            assert_almost_eq(contract.get_transferrable().0, to_yacto(375 + extra_balance_near));

            // Checking values middle of vesting
            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR * 2);
            testing_env!(context.clone());

            assert_almost_eq(contract.get_transferrable().0, to_yacto(500 + extra_balance_near));

            // Checking values a day before vesting ends
            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR * 4 - 1);
            testing_env!(context.clone());

            assert_almost_eq(contract.get_transferrable().0, to_yacto(1000 + extra_balance_near));

            // Checking values a day after vesting ends
            context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS - ALMOST_HALF_YEAR + YEAR * 4 + 1);
            testing_env!(context.clone());

            assert_almost_eq(contract.get_transferrable().0, to_yacto(1000 + extra_balance_near));
        }
    }

    #[test]
    fn test_transfer_call_by_owner() {
        let (mut context, mut contract) = basic_setup();
        context.block_timestamp = to_ts(GENESIS_TIME_IN_DAYS + YEAR + 1);
        context.is_view = true;
        testing_env!(context.clone());
        assert_almost_eq(contract.get_transferrable().0, to_yacto(375));

        context.predecessor_account_id = account_owner();
        context.signer_account_id = account_owner();
        context.signer_account_pk = public_key(1);
        context.is_view = false;
        testing_env!(context.clone());

        assert_eq!(env::account_balance(), to_yacto(LOCKUP_NEAR));
        contract.transfer(to_yacto(100).into(), non_owner());
        assert_almost_eq(env::account_balance(), to_yacto(LOCKUP_NEAR - 100));
    }

    #[test]
    fn test_transfer_by_non_owner() {
        let (mut context, mut contract) = basic_setup();

        context.predecessor_account_id = non_owner();
        context.signer_account_id = non_owner();
        context.signer_account_pk = public_key(5);
        testing_env!(context.clone());

        std::panic::catch_unwind(move || {
            contract.transfer(to_yacto(100).into(), non_owner());
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
            contract.stake(to_yacto(100).into(), public_key(4).try_into().unwrap());
        })
        .unwrap_err();
    }

    #[test]
    fn test_permanently_unstake_by_non_owner() {
        let (mut context, mut contract) = basic_setup();

        context.predecessor_account_id = non_owner();
        context.signer_account_id = non_owner();
        context.signer_account_pk = public_key(5);
        testing_env!(context.clone());

        std::panic::catch_unwind(move || {
            contract.permanently_unstake(public_key(4).try_into().unwrap());
        })
        .unwrap_err();
    }

    #[test]
    fn test_terminate_by_non_owner() {
        let (mut context, mut contract) = basic_setup();

        context.predecessor_account_id = non_owner();
        context.signer_account_id = non_owner();
        context.signer_account_pk = public_key(5);
        testing_env!(context.clone());

        std::panic::catch_unwind(move || {
            contract.terminate(non_owner());
        })
        .unwrap_err();
    }
}
