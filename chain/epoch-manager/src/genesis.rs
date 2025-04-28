use std::collections::HashMap;
use std::iter;
use std::sync::Arc;

use itertools::Itertools;
use near_primitives::chains::{MAINNET, TESTNET};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::{EpochInfo, EpochInfoV2, RngSeed};
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{AccountId, Balance, EpochId, NumSeats, ValidatorId};
use near_primitives::version::PROD_GENESIS_PROTOCOL_VERSION;
use rand::{RngCore, SeedableRng};
use rand_hc::Hc128Rng;

use crate::EpochManager;
use crate::validator_selection::proposals_to_epoch_info;

impl EpochManager {
    pub fn initialize_genesis_epoch_info(
        &mut self,
        validators: Vec<ValidatorStake>,
    ) -> Result<(), EpochError> {
        let genesis_epoch_info = self.make_genesis_epoch_info(validators)?;
        // Dummy block info.
        // Artificial block we add to simplify implementation: dummy block is the
        // parent of genesis block that points to itself.
        // If we view it as block in epoch -1 and height -1, it naturally extends the
        // EpochId formula using T-2 for T=1, and height field is unused.
        let block_info = Arc::new(BlockInfo::default());
        let mut store_update = self.store.store_update();
        self.save_epoch_info(&mut store_update, &EpochId::default(), Arc::new(genesis_epoch_info))?;
        self.save_block_info(&mut store_update, block_info)?;
        store_update.commit()?;
        Ok(())
    }

    fn make_genesis_epoch_info(
        &self,
        validators: Vec<ValidatorStake>,
    ) -> Result<EpochInfo, EpochError> {
        // Missing genesis epoch, means that there is no validator initialize yet.
        let genesis_protocol_version = self.reward_calculator.genesis_protocol_version;
        let genesis_epoch_config = self.config.for_protocol_version(genesis_protocol_version);
        let validator_reward =
            HashMap::from([(self.reward_calculator.protocol_treasury_account.clone(), 0u128)]);

        // use custom code for genesis protocol version 29 used in mainnet and testnet
        if genesis_protocol_version == PROD_GENESIS_PROTOCOL_VERSION {
            let genesis_epoch_info =
                Self::prod_genesis(&genesis_epoch_config, [0; 32], validators, validator_reward);
            let digest = CryptoHash::hash_borsh(&genesis_epoch_info).to_string();
            if self.config.chain_id() == MAINNET {
                assert_eq!(digest, "Hsc6BpTrd77H7J29w8uLXQY7bhfQzRDcFyo8Dj2CMZCj");
            }
            if self.config.chain_id() == TESTNET {
                assert_eq!(digest, "6oVGdXcDaLErQ3nHRgZVZVHPkV5gfBjVtYkjWrByrb53");
            }
            Ok(genesis_epoch_info)
        } else {
            proposals_to_epoch_info(
                &genesis_epoch_config,
                [0; 32],
                &EpochInfo::default(),
                validators,
                HashMap::default(),
                validator_reward,
                0,
                genesis_protocol_version,
                false,
                None,
            )
        }
    }

    /// This code is a simplified copy of `old_validator_selection::proposals_to_epoch_info`
    /// The epoch_info returned by this function is very specific to testnet and mainnet and should
    /// not be used for other scenarios.
    ///
    /// As an additional check, we're asserting the digest of the epoch_info from testnet and mainnet.
    fn prod_genesis(
        epoch_config: &EpochConfig,
        rng_seed: RngSeed,
        mut validators: Vec<ValidatorStake>,
        validator_reward: HashMap<AccountId, Balance>,
    ) -> EpochInfo {
        // Sort all validators by account id.
        validators.sort_by_cached_key(|v| v.account_id().clone());

        let validator_to_index = validators
            .iter()
            .enumerate()
            .map(|(index, s)| (s.account_id().clone(), index as ValidatorId))
            .collect();

        let stake_change = validators.iter().map(|v| (v.account_id().clone(), v.stake())).collect();

        // Get the threshold given current number of seats and stakes.
        let num_hidden_validator_seats: NumSeats =
            epoch_config.avg_hidden_validator_seats_per_shard.iter().sum();
        let num_total_seats = epoch_config.num_block_producer_seats + num_hidden_validator_seats;
        let stakes = validators.iter().map(|v| v.stake()).collect_vec();
        let threshold = find_threshold(&stakes, num_total_seats).unwrap();

        // Duplicate each proposal for number of seats it has.
        let mut dup_proposals = validators
            .iter()
            .enumerate()
            .flat_map(|(i, p)| iter::repeat(i as u64).take((p.stake() / threshold) as usize))
            .collect_vec();

        assert!(dup_proposals.len() >= num_total_seats as usize, "bug in find_threshold");
        shuffle_duplicate_proposals(&mut dup_proposals, rng_seed);

        // Block producers are first `num_block_producer_seats` proposals.
        let block_producers_settlement =
            dup_proposals[..epoch_config.num_block_producer_seats as usize].to_vec();

        // Collect proposals into block producer assignments.
        let mut chunk_producers_settlement: Vec<Vec<ValidatorId>> = vec![];
        let mut last_index: u64 = 0;
        for num_seats_in_shard in &epoch_config.num_block_producer_seats_per_shard {
            let mut shard_settlement: Vec<ValidatorId> = vec![];
            for _ in 0..*num_seats_in_shard {
                let proposal_index = block_producers_settlement[last_index as usize];
                shard_settlement.push(proposal_index);
                last_index = (last_index + 1) % epoch_config.num_block_producer_seats;
            }
            chunk_producers_settlement.push(shard_settlement);
        }

        EpochInfo::V2(EpochInfoV2 {
            epoch_height: 1,
            validators,
            validator_to_index,
            block_producers_settlement,
            chunk_producers_settlement,
            hidden_validators_settlement: vec![],
            fishermen: vec![],
            fishermen_to_index: Default::default(),
            stake_change,
            validator_reward,
            validator_kickout: Default::default(),
            minted_amount: 0,
            seat_price: threshold,
            protocol_version: PROD_GENESIS_PROTOCOL_VERSION,
        })
    }
}

/// Find threshold of stake per seat, given provided stakes and required number of seats.
pub(crate) fn find_threshold(
    stakes: &[Balance],
    num_seats: NumSeats,
) -> Result<Balance, EpochError> {
    let stake_sum: Balance = stakes.iter().sum();
    if stake_sum < num_seats.into() {
        return Err(EpochError::ThresholdError { stake_sum, num_seats });
    }
    let (mut left, mut right): (Balance, Balance) = (1, stake_sum + 1);
    'outer: loop {
        if left == right - 1 {
            break Ok(left);
        }
        let mid = (left + right) / 2;
        let mut current_sum: Balance = 0;
        for item in stakes {
            current_sum += item / mid;
            if current_sum >= u128::from(num_seats) {
                left = mid;
                continue 'outer;
            }
        }
        right = mid;
    }
}

fn shuffle_duplicate_proposals(dup_proposals: &mut Vec<u64>, rng_seed: RngSeed) {
    let mut rng: Hc128Rng = SeedableRng::from_seed(rng_seed);
    for i in (1..dup_proposals.len()).rev() {
        dup_proposals.swap(i, gen_index_old(&mut rng, (i + 1) as u64) as usize);
    }
}

fn gen_index_old(rng: &mut Hc128Rng, bound: u64) -> u64 {
    // This is a simplified copy of the rand gen_index implementation to ensure that
    // upgrades to the rand library will not cause a change in the shuffling behavior.
    let zone = (bound << bound.leading_zeros()).wrapping_sub(1);
    loop {
        let v = rng.next_u64();
        let mul = (v as u128) * (bound as u128);
        let (hi, lo) = ((mul >> 64) as u64, mul as u64);
        if lo < zone {
            return hi;
        }
    }
}
