use std::collections::{BTreeMap, HashMap};

use near_primitives::types::EpochId;
use near_store::Store;
use num_rational::Ratio;

use crate::proposals::find_threshold;
use crate::RewardCalculator;
use crate::RngSeed;
use crate::{BlockInfo, EpochManager};
use near_crypto::{KeyType, SecretKey};
use near_primitives::challenge::SlashedValidator;
use near_primitives::epoch_manager::block_info::BlockInfoV2;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig, ValidatorWeight};
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, Balance, BlockHeight, BlockHeightDelta, EpochHeight, NumSeats, NumShards,
    ValidatorId, ValidatorKickoutReason,
};
use near_primitives::utils::get_num_seats_per_shard;
use near_primitives::version::PROTOCOL_VERSION;
use near_store::test_utils::create_test_store;

use near_primitives::shard_layout::ShardLayout;
use {crate::reward_calculator::NUM_NS_IN_SECOND, crate::NUM_SECONDS_IN_A_YEAR};

pub const DEFAULT_GAS_PRICE: u128 = 100;
pub const DEFAULT_TOTAL_SUPPLY: u128 = 1_000_000_000_000;
pub const TEST_SEED: RngSeed = [3; 32];

pub fn hash_range(num: usize) -> Vec<CryptoHash> {
    let mut result = vec![];
    for i in 0..num {
        result.push(hash(i.to_le_bytes().as_ref()));
    }
    result
}

pub fn change_stake(stake_changes: Vec<(AccountId, Balance)>) -> BTreeMap<AccountId, Balance> {
    stake_changes.into_iter().collect()
}

pub fn epoch_info(
    epoch_height: EpochHeight,
    accounts: Vec<(AccountId, Balance)>,
    block_producers_settlement: Vec<ValidatorId>,
    chunk_producers_settlement: Vec<Vec<ValidatorId>>,
    hidden_validators_settlement: Vec<ValidatorWeight>,
    fishermen: Vec<(AccountId, Balance)>,
    stake_change: BTreeMap<AccountId, Balance>,
    validator_kickout: Vec<(AccountId, ValidatorKickoutReason)>,
    validator_reward: HashMap<AccountId, Balance>,
    minted_amount: Balance,
) -> EpochInfo {
    let num_seats = block_producers_settlement.len() as u64;
    epoch_info_with_num_seats(
        epoch_height,
        accounts,
        block_producers_settlement,
        chunk_producers_settlement,
        hidden_validators_settlement,
        fishermen,
        stake_change,
        validator_kickout,
        validator_reward,
        minted_amount,
        num_seats,
    )
}

pub fn epoch_info_with_num_seats(
    epoch_height: EpochHeight,
    mut accounts: Vec<(AccountId, Balance)>,
    block_producers_settlement: Vec<ValidatorId>,
    chunk_producers_settlement: Vec<Vec<ValidatorId>>,
    hidden_validators_settlement: Vec<ValidatorWeight>,
    fishermen: Vec<(AccountId, Balance)>,
    stake_change: BTreeMap<AccountId, Balance>,
    validator_kickout: Vec<(AccountId, ValidatorKickoutReason)>,
    validator_reward: HashMap<AccountId, Balance>,
    minted_amount: Balance,
    num_seats: NumSeats,
) -> EpochInfo {
    let seat_price =
        find_threshold(&accounts.iter().map(|(_, s)| *s).collect::<Vec<_>>(), num_seats).unwrap();
    accounts.sort();
    let validator_to_index = accounts.iter().enumerate().fold(HashMap::new(), |mut acc, (i, x)| {
        acc.insert(x.0.clone(), i as u64);
        acc
    });
    let fishermen_to_index =
        fishermen.iter().enumerate().map(|(i, (s, _))| (s.clone(), i as ValidatorId)).collect();
    let account_to_validators = |accounts: Vec<(AccountId, Balance)>| -> Vec<ValidatorStake> {
        accounts
            .into_iter()
            .map(|(account_id, stake)| {
                ValidatorStake::new(
                    account_id.clone(),
                    SecretKey::from_seed(KeyType::ED25519, account_id.as_ref()).public_key(),
                    stake,
                )
            })
            .collect()
    };
    EpochInfo::new(
        epoch_height,
        account_to_validators(accounts),
        validator_to_index,
        block_producers_settlement,
        chunk_producers_settlement,
        hidden_validators_settlement,
        account_to_validators(fishermen),
        fishermen_to_index,
        stake_change,
        validator_reward,
        validator_kickout.into_iter().collect(),
        minted_amount,
        seat_price,
        PROTOCOL_VERSION,
        TEST_SEED,
    )
}

pub fn epoch_config_with_production_config(
    epoch_length: BlockHeightDelta,
    num_shards: NumShards,
    num_block_producer_seats: NumSeats,
    num_hidden_validator_seats: NumSeats,
    block_producer_kickout_threshold: u8,
    chunk_producer_kickout_threshold: u8,
    fishermen_threshold: Balance,
    use_production_config: bool,
) -> AllEpochConfig {
    let epoch_config = EpochConfig {
        epoch_length,
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
        online_min_threshold: Ratio::new(90, 100),
        online_max_threshold: Ratio::new(99, 100),
        protocol_upgrade_stake_threshold: Ratio::new(80, 100),
        minimum_stake_divisor: 1,
        validator_selection_config: Default::default(),
        shard_layout: ShardLayout::v0(num_shards, 0),
        validator_max_kickout_stake_perc: 100,
    };
    AllEpochConfig::new(use_production_config, epoch_config)
}

pub fn epoch_config(
    epoch_length: BlockHeightDelta,
    num_shards: NumShards,
    num_block_producer_seats: NumSeats,
    num_hidden_validator_seats: NumSeats,
    block_producer_kickout_threshold: u8,
    chunk_producer_kickout_threshold: u8,
    fishermen_threshold: Balance,
) -> AllEpochConfig {
    epoch_config_with_production_config(
        epoch_length,
        num_shards,
        num_block_producer_seats,
        num_hidden_validator_seats,
        block_producer_kickout_threshold,
        chunk_producer_kickout_threshold,
        fishermen_threshold,
        false,
    )
}

pub fn stake(account_id: AccountId, amount: Balance) -> ValidatorStake {
    let public_key = SecretKey::from_seed(KeyType::ED25519, account_id.as_ref()).public_key();
    ValidatorStake::new(account_id, public_key, amount)
}

/// No-op reward calculator. Will produce no reward
pub fn default_reward_calculator() -> RewardCalculator {
    RewardCalculator {
        max_inflation_rate: Ratio::from_integer(0),
        num_blocks_per_year: 1,
        epoch_length: 1,
        protocol_reward_rate: Ratio::from_integer(0),
        protocol_treasury_account: "near".parse().unwrap(),
        online_min_threshold: Ratio::new(90, 100),
        online_max_threshold: Ratio::new(99, 100),
        num_seconds_per_year: NUM_SECONDS_IN_A_YEAR,
    }
}

pub fn reward(info: Vec<(AccountId, Balance)>) -> HashMap<AccountId, Balance> {
    info.into_iter().collect()
}

pub fn setup_epoch_manager(
    validators: Vec<(AccountId, Balance)>,
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
        PROTOCOL_VERSION,
        reward_calculator,
        validators
            .iter()
            .map(|(account_id, balance)| stake(account_id.clone(), *balance))
            .collect(),
    )
    .unwrap()
}

pub fn setup_default_epoch_manager(
    validators: Vec<(AccountId, Balance)>,
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

/// Makes an EpochManager with the given block and chunk producers,
/// automatically coming up with stakes for them to ensure the desired
/// election outcome.
pub fn setup_epoch_manager_with_block_and_chunk_producers(
    store: Store,
    block_producers: Vec<AccountId>,
    chunk_only_producers: Vec<AccountId>,
    num_shards: NumShards,
    epoch_length: BlockHeightDelta,
) -> EpochManager {
    let num_block_producers = block_producers.len() as u64;
    let block_producer_stake = 1_000_000 as u128;
    let mut total_stake = 0;
    let mut validators = vec![];
    for block_producer in &block_producers {
        validators.push((block_producer.clone(), block_producer_stake));
        total_stake += block_producer_stake;
    }
    for chunk_only_producer in &chunk_only_producers {
        let minimum_stake_to_ensure_election =
            total_stake * 160 / 1_000_000 / num_shards as u128 + 1;
        let stake = block_producer_stake - 1;
        assert!(
            stake >= minimum_stake_to_ensure_election,
            "Could not honor the specified list of producers"
        );
        validators.push((chunk_only_producer.clone(), stake));
        total_stake += stake;
    }
    let config = epoch_config(epoch_length, num_shards, num_block_producers, 0, 0, 0, 0);
    let epoch_manager = EpochManager::new(
        store,
        config,
        PROTOCOL_VERSION,
        default_reward_calculator(),
        validators
            .iter()
            .map(|(account_id, balance)| stake(account_id.clone(), *balance))
            .collect(),
    )
    .unwrap();
    // Sanity check that the election results are indeed as expected.
    let actual_block_producers = epoch_manager
        .get_all_block_producers_ordered(&EpochId::default(), &CryptoHash::default())
        .unwrap();
    assert_eq!(actual_block_producers.len(), block_producers.len());
    let actual_chunk_producers =
        epoch_manager.get_all_chunk_producers(&EpochId::default()).unwrap();
    assert_eq!(actual_chunk_producers.len(), block_producers.len() + chunk_only_producers.len());
    epoch_manager
}

pub fn record_block_with_final_block_hash(
    epoch_manager: &mut EpochManager,
    prev_h: CryptoHash,
    cur_h: CryptoHash,
    last_final_block_hash: CryptoHash,
    height: BlockHeight,
    proposals: Vec<ValidatorStake>,
) {
    epoch_manager
        .record_block_info(
            BlockInfo::new(
                cur_h,
                height,
                height.saturating_sub(2),
                last_final_block_hash,
                prev_h,
                proposals,
                vec![],
                vec![],
                DEFAULT_TOTAL_SUPPLY,
                PROTOCOL_VERSION,
                height * NUM_NS_IN_SECOND,
            ),
            [0; 32],
        )
        .unwrap()
        .commit()
        .unwrap();
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
            BlockInfo::new(
                cur_h,
                height,
                height.saturating_sub(2),
                prev_h,
                prev_h,
                proposals,
                vec![],
                slashed,
                DEFAULT_TOTAL_SUPPLY,
                PROTOCOL_VERSION,
                height * NUM_NS_IN_SECOND,
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

pub fn block_info(
    hash: CryptoHash,
    height: BlockHeight,
    last_finalized_height: BlockHeight,
    last_final_block_hash: CryptoHash,
    prev_hash: CryptoHash,
    epoch_first_block: CryptoHash,
    chunk_mask: Vec<bool>,
    total_supply: Balance,
) -> BlockInfo {
    BlockInfo::V2(BlockInfoV2 {
        hash,
        height,
        last_finalized_height,
        last_final_block_hash,
        prev_hash,
        epoch_first_block,
        epoch_id: Default::default(),
        proposals: vec![],
        chunk_mask,
        latest_protocol_version: PROTOCOL_VERSION,
        slashed: Default::default(),
        total_supply,
        timestamp_nanosec: height * NUM_NS_IN_SECOND,
    })
}

pub fn record_with_block_info(epoch_manager: &mut EpochManager, block_info: BlockInfo) {
    epoch_manager.record_block_info(block_info, [0; 32]).unwrap().commit().unwrap();
}
