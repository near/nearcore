use std::collections::{BTreeMap, HashMap};

use near_crypto::{KeyType, SecretKey};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochHeight, NumSeats, NumShards,
    ValidatorId, ValidatorKickoutReason, ValidatorStake,
};
use near_primitives::utils::get_num_seats_per_shard;
use near_store::test_utils::create_test_store;

use crate::types::{EpochConfig, EpochInfo, ValidatorWeight};
use crate::RewardCalculator;
use crate::{BlockInfo, EpochManager};
use near_primitives::challenge::SlashedValidator;
use num_rational::Rational;

pub const DEFAULT_GAS_PRICE: u128 = 100;
pub const DEFAULT_TOTAL_SUPPLY: u128 = 1_000_000_000_000;

pub fn hash_range(num: usize) -> Vec<CryptoHash> {
    let mut result = vec![];
    for i in 0..num {
        result.push(hash(i.to_le_bytes().as_ref()));
    }
    result
}

pub fn change_stake(stake_changes: Vec<(&str, Balance)>) -> BTreeMap<AccountId, Balance> {
    stake_changes.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub fn epoch_info(
    epoch_height: EpochHeight,
    mut accounts: Vec<(&str, Balance)>,
    block_producers_settlement: Vec<ValidatorId>,
    chunk_producers_settlement: Vec<Vec<ValidatorId>>,
    hidden_validators_settlement: Vec<ValidatorWeight>,
    fishermen: Vec<(&str, Balance)>,
    stake_change: BTreeMap<AccountId, Balance>,
    validator_kickout: Vec<(&str, ValidatorKickoutReason)>,
    validator_reward: HashMap<AccountId, Balance>,
    inflation: u128,
) -> EpochInfo {
    accounts.sort();
    let validator_to_index = accounts.iter().enumerate().fold(HashMap::new(), |mut acc, (i, x)| {
        acc.insert(x.0.to_string(), i as u64);
        acc
    });
    let fishermen_to_index =
        fishermen.iter().enumerate().map(|(i, (s, _))| (s.to_string(), i as ValidatorId)).collect();
    let account_to_validators = |accounts: Vec<(&str, Balance)>| -> Vec<ValidatorStake> {
        accounts
            .into_iter()
            .map(|(account_id, stake)| ValidatorStake {
                account_id: account_id.to_string(),
                public_key: SecretKey::from_seed(KeyType::ED25519, account_id).public_key(),
                stake,
            })
            .collect()
    };
    let validator_kickout =
        validator_kickout.into_iter().map(|(s, r)| (s.to_string(), r)).collect();
    EpochInfo {
        epoch_height,
        validators: account_to_validators(accounts),
        validator_to_index,
        block_producers_settlement,
        chunk_producers_settlement,
        hidden_validators_settlement,
        fishermen: account_to_validators(fishermen),
        fishermen_to_index,
        stake_change,
        validator_reward,
        inflation,
        validator_kickout,
    }
}

pub fn epoch_config(
    epoch_length: BlockHeightDelta,
    num_shards: NumShards,
    num_block_producer_seats: NumSeats,
    num_hidden_validator_seats: NumSeats,
    block_producer_kickout_threshold: u8,
    chunk_producer_kickout_threshold: u8,
    fishermen_threshold: Balance,
) -> EpochConfig {
    EpochConfig {
        epoch_length,
        num_shards,
        num_block_producer_seats,
        num_block_producer_seats_per_shard: get_num_seats_per_shard(
            num_shards,
            num_block_producer_seats,
        ),
        avg_hidden_validator_seats_per_shard: (0..num_shards)
            .map(|_| num_hidden_validator_seats)
            .collect(),
        block_producer_kickout_threshold,
        chunk_producer_kickout_threshold,
        fishermen_threshold,
    }
}

pub fn stake(account_id: &str, amount: Balance) -> ValidatorStake {
    let public_key = SecretKey::from_seed(KeyType::ED25519, account_id).public_key();
    ValidatorStake::new(account_id.to_string(), public_key, amount)
}

/// No-op reward calculator. Will produce no reward
pub fn default_reward_calculator() -> RewardCalculator {
    RewardCalculator {
        max_inflation_rate: Rational::from_integer(0),
        num_blocks_per_year: 1,
        epoch_length: 1,
        protocol_reward_percentage: Rational::from_integer(0),
        protocol_treasury_account: "near".to_string(),
    }
}

pub fn reward(info: Vec<(&str, Balance)>) -> HashMap<AccountId, Balance> {
    info.into_iter().map(|(account_id, r)| (account_id.to_string(), r)).collect()
}

pub fn setup_epoch_manager(
    validators: Vec<(&str, Balance)>,
    epoch_length: BlockHeightDelta,
    num_shards: NumShards,
    num_block_producer_seats: NumSeats,
    num_hidden_validator_seats: NumSeats,
    block_producer_kickout_threshold: u8,
    chunk_producer_kickout_threshold: u8,
    fishermen_threshold: Balance,
    reward_calculator: RewardCalculator,
) -> EpochManager {
    let store = create_test_store();
    let config = epoch_config(
        epoch_length,
        num_shards,
        num_block_producer_seats,
        num_hidden_validator_seats,
        block_producer_kickout_threshold,
        chunk_producer_kickout_threshold,
        fishermen_threshold,
    );
    EpochManager::new(
        store,
        config,
        reward_calculator,
        validators.iter().map(|(account_id, balance)| stake(*account_id, *balance)).collect(),
    )
    .unwrap()
}

pub fn setup_default_epoch_manager(
    validators: Vec<(&str, Balance)>,
    epoch_length: BlockHeightDelta,
    num_shards: NumShards,
    num_block_producer_seats: NumSeats,
    num_hidden_validator_seats: NumSeats,
    block_producer_kickout_threshold: u8,
    chunk_producer_kickout_threshold: u8,
) -> EpochManager {
    setup_epoch_manager(
        validators,
        epoch_length,
        num_shards,
        num_block_producer_seats,
        num_hidden_validator_seats,
        block_producer_kickout_threshold,
        chunk_producer_kickout_threshold,
        1,
        default_reward_calculator(),
    )
}

pub fn record_block_with_slashes(
    epoch_manager: &mut EpochManager,
    prev_h: CryptoHash,
    cur_h: CryptoHash,
    height: BlockHeight,
    proposals: Vec<ValidatorStake>,
    slashed: Vec<SlashedValidator>,
) {
    epoch_manager
        .record_block_info(
            &cur_h,
            BlockInfo::new(
                height,
                height.saturating_sub(2),
                prev_h,
                proposals,
                vec![],
                slashed,
                0,
                DEFAULT_TOTAL_SUPPLY,
            ),
            [0; 32],
        )
        .unwrap()
        .commit()
        .unwrap();
}

pub fn record_block(
    epoch_manager: &mut EpochManager,
    prev_h: CryptoHash,
    cur_h: CryptoHash,
    height: BlockHeight,
    proposals: Vec<ValidatorStake>,
) {
    record_block_with_slashes(epoch_manager, prev_h, cur_h, height, proposals, vec![]);
}
