//! A smart contract that allows tokens lockup.

use borsh::{BorshDeserialize, BorshSerialize};
use near_sdk::json_types::{Base58PublicKey, U128, U64};
use near_sdk::{env, ext_contract, near_bindgen, AccountId, Promise};

#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

pub type WrappedTimestamp = U64;
pub type WrappedBalance = U128;

pub type ProposalId = u64;
pub type VoteIndex = u64;

/// Method names allowed for the owner's access keys.
const OWNER_KEY_ALLOWED_METHODS: &[u8] = b"vote,delegate_stake,stake,transfer";
/// Method names allowed for the NEAR Foundation access key in case of vesting schedule that
/// can be terminated by foundation.
const FOUNDATION_KEY_ALLOWED_METHODS: &[u8] = b"terminate_vesting,withdraw_unvested";

/// Indicates there are no deposit for a cross contract call for better readability.
const NO_DEPOSIT: u128 = 0;

/// The amount of gas required for a voting through a staking pool.
/// The math is 100e12 for execution + 200e12 for attaching to a call on the actual voting contract.
const VOTE_GAS: u64 = 300_000_000_000_000;

/// Contains information about token lockups.
#[derive(BorshDeserialize, BorshSerialize)]
pub struct LockupInformation {
    /// The amount in yacto-NEAR tokens locked for this account.
    pub lockup_amount: WrappedBalance,
    /// The timestamp in nanoseconds when the lockup amount of tokens will be available.
    pub lockup_timestamp: WrappedTimestamp,
}

impl LockupInformation {
    pub fn assert_valid(&self) -> bool {
        assert!(self.lockup_amount.0 > 0, "Lockup amount has to be positive number");
    }
}

/// Contains information about current stake and delegation.
#[derive(BorshDeserialize, BorshSerialize)]
pub struct StakingInformation {
    /// The Account ID of the staking pool contract.
    pub staking_pool_account_id: AccountId,

    /// The amount of tokens transferred towards the staking pool and is currently in progress
    /// of delegation. This amount is not confirmed yet, so it doesn't count as delegated amount.
    pub depositing_amount: WrappedBalance,

    /// The delegated amount. The amount of tokens transferred to the staking pool contract for
    /// staking.
    pub delegated_amount: WrappedBalance,

    /// The last amount of tokens staked at the pool.
    pub staked_amount: WrappedBalance,
}

#[derive(BorshDeserialize, BorshSerialize)]
pub struct VestingSchedule {
    /// The timestamp in nanosecond when the vesting starts. E.g. the start date of employment.
    pub vesting_start_timestamp: WrappedTimestamp,
    /// The timestamp in nanosecond when the first part of lockup tokens becomes vested.
    /// The remaining tokens will vest continuously until they are fully vested.
    /// Example: a 1 year of employment at which moment the 1/4 of tokens become vested.
    pub vesting_cliff_timestamp: WrappedTimestamp,
    /// The timestamp in nanosecond when the vesting ends.
    pub vesting_end_timestamp: WrappedTimestamp,
}

impl VestingSchedule {
    pub fn assert_valid(&self) -> bool {
        assert!(
            self.vesting_start_timestamp.0 <= self.vesting_cliff_timestamp.0,
            "Cliff timestamp can't be earlier than vesting start timestamp"
        );
        assert!(
            self.vesting_cliff_timestamp.0 <= self.vesting_end_timestamp.0,
            "Cliff timestamp can't be later than vesting end timestamp"
        );
    }
}

/// Contains information about vesting for contracts that contain vesting schedule and termination information.
#[derive(BorshDeserialize, BorshSerialize)]
pub enum VestingInformation {
    /// The vesting is going on schedule.
    /// Once the vesting is completed `VestingInformation` is removed.
    Vesting(VestingSchedule),
    /// The information about the early termination of the vesting schedule.
    /// It means the termination of the vesting is currently in progress.
    /// Once the unvested amount is transferred out, `VestingInformation` is removed.
    Terminating(TerminationInformation),
}

/// Contains information about early termination of the vesting schedule.
#[derive(BorshDeserialize, BorshSerialize)]
pub struct TerminationInformation {
    /// The amount of tokens that are unvested and has to be transferred back to NEAR Foundation.
    /// These tokens are effectively locked and can't be transferred out and can't be restaked.
    pub unvested_amount: WrappedBalance,
}

/// Contains information about voting on enabling transfers.
#[derive(BorshDeserialize, BorshSerialize)]
pub struct TransferVotingInformation {
    /// The proposal ID to vote on transfers.
    pub transfer_proposal_id: ProposalId,

    /// Vote index indicating that the transfers are enabled.
    pub enable_transfers_vote_index: VoteIndex,

    /// Voting contract account ID
    pub voting_contract_account_id: AccountId,
}

impl TransferVotingInformation {
    pub fn assert_valid(&self) -> bool {
        assert!(
            is_valid_account_id(&self.voting_contract_account_id),
            "Voting contract account ID is invalid"
        );
    }
}

#[near_bindgen]
#[derive(BorshDeserialize, BorshSerialize)]
pub struct LockupContract {
    /// Information about lockup schedule and the amount.
    pub lockup_information: LockupInformation,

    /// Information about staking and delegation.
    /// `Some` means the staking information is available and the staking pool contract is available.
    /// `None` means there are currently no staking.
    pub staking_information: Option<StakingInformation>,

    /// Information about vesting if the lockup schedule includes vesting.
    /// `Some` means there is vesting information available.
    /// `None` means the lockup balance is unaffected by vesting.
    pub vesting_information: Option<VestingInformation>,

    /// Information about transfer voting. At the launch transfers are disabled, once transfers are
    /// enabled, they can't be disabled and don't need to be checked again.
    /// `Some` means transfers are disabled. `TransferVotingInformation` contains information
    /// required to check whether transfers were voted to be enabled.
    /// If transfers are disabled, every transfer attempt will try to first pull the results
    /// of transfer voting from the voting contract using transfer proposal ID.
    pub transfer_voting_information: Option<TransferVotingInformation>,
}

impl Default for LockupContract {
    fn default() -> Self {
        env::panic(b"The contract is not initialized.");
    }
}

fn assert_self() {
    assert_eq!(env::predecessor_account_id(), env::current_account_id());
}

#[ext_contract(staking_pool)]
pub trait ExtStakingPool {
    fn vote(&mut self, proposal_id: ProposalId, vote: VoteIndex);
}

#[near_bindgen]
impl LockupContract {
    /// Initializes lockup contract.
    /// - `lockup_information` - information about the lockup amount and the release timestamp.
    /// - `vesting_schedule` - if `Some` contains vesting schedule.
    /// - `transfer_voting_information` - if `Some` means transfers are disabled and can only be
    ///   enabled by voting on the proposal.
    /// - `owner_public_keys`
    #[init]
    pub fn new(
        lockup_information: LockupInformation,
        vesting_schedule: Option<VestingSchedule>,
        transfer_voting_information: Option<TransferVotingInformation>,
        owner_public_keys: Vec<Base58PublicKey>,
        foundation_public_keys: Vec<Base58PublicKey>,
    ) -> Self {
        assert!(!env::state_exists(), "The contract is already initialized");
        if !foundation_public_keys.is_empty() {
            assert!(
                vesting_schedule.is_some(),
                "Foundation keys can't be added without vesting schedule"
            )
        }
        assert!(
            !owner_public_keys.is_empty(),
            "At least one owner's public key has to be provided"
        );
        let vesting_information = vesting_schedule.map(|vesting_schedule| {
            vesting_schedule.assert_valid();
            VestingInformation::VestingInformation(vesting_schedule)
        });
        if let Some(ref transfer_voting_information) = transfer_voting_information {
            transfer_voting_information.assert_valid();
        }
        lockup_information.assert_valid();
        let account_id = env::current_account_id();
        for public_key in owner_public_keys {
            Promise::new(account_id.clone()).add_access_key(
                public_key.into(),
                0,
                account_id.clone(),
                OWNER_KEY_ALLOWED_METHODS.to_vec(),
            );
        }
        for public_key in foundation_public_keys {
            Promise::new(account_id.clone()).add_access_key(
                public_key.into(),
                0,
                account_id.clone(),
                FOUNDATION_KEY_ALLOWED_METHODS.to_vec(),
            );
        }
        Self {
            lockup_information,
            staking_information: None,
            vesting_information,
            transfer_voting_information,
        }
    }

    /// OWNER'S METHOD
    /// Vote on given proposal ID with a selected vote index.
    /// The owner has to first delegate the stake to some staking pool contract before voting on
    /// a proposal.
    pub fn vote(&mut self, proposal_id: ProposalId, vote: VoteIndex) -> Promise {
        assert_self();
        assert!(
            self.staking_information.is_some(),
            "Can't vote without delegating stake first to a staking pool contract"
        );
        let staking_information = self.staking_information.as_ref().unwrap();
        staking_pool::vote(
            proposal_id,
            vote,
            &staking_information.staking_pool_account_id,
            NO_DEPOSIT,
            VOTE_GAS,
        )
    }

    /// OWNER'S METHOD
    /// Delegate given amount to a given staking_pool_account_id.
    /// If the `staking_pool_account_id` is different from the current staking pool account ID in
    /// the `staking_information`, then the contract needs to check that this staking pool is
    /// approved in the staking pool whitelist.
    /// If the `staking_pool_account_id` is the same as the current staking pool account ID, then
    /// the contract will try to perform the adjustment operation on the staking pool contract.
    pub fn delegate_stake(&mut self, staking_pool_account_id: AccountId, amount: U128) {
        assert_self();
        let amount = amount.into();
        // TODO: Check account ID validity
    }

    /// OWNER'S METHOD
    /// Stake a new given total stake amount
    pub fn stake(&mut self, total_stake_amount: U128) {
        assert_self();
        let new_total_stake = new_total_stake.into();
    }

    /// OWNER'S METHOD
    /// Transfer the given amount of tokens to `account_id`.
    pub fn transfer(&mut self, account_id: String, amount: U128) {
        assert_self();
        let amount = amount.into();
        assert!(
            amount <= self.get_transferrable().into(),
            "Not enough transferrable tokens to transfer."
        );
        Promise::new(account).transfer(amount);
    }

    /// FOUNDATION'S METHOD
    pub fn terminate_vesting(&mut self) {
        assert_self();
    }

    /// FOUNDATION'S METHOD
    pub fn withdraw_unvested(&mut self, account_id: String) {
        assert_self();
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
