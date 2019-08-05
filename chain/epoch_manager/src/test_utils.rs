use near_primitives::test_utils::get_key_pair_from_seed;
use near_primitives::types::{AccountId, Balance, BlockIndex, GasUsage, ShardId, ValidatorStake};
use std::collections::{BTreeMap, HashMap, HashSet};

use crate::types::{BlockInfo, EpochConfig, EpochInfo};
use crate::EpochManager;
use near_primitives::hash::{hash, CryptoHash};
use near_store::test_utils::create_test_store;

pub fn hash_range(num: usize) -> Vec<CryptoHash> {
    let mut result = vec![];
    for i in 0..num {
        result.push(hash(&vec![i as u8]));
    }
    result
}

pub fn change_stake(stake_changes: Vec<(&str, Balance)>) -> BTreeMap<AccountId, Balance> {
    stake_changes.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub fn epoch_info(
    mut accounts: Vec<(&str, Balance)>,
    block_producers: Vec<usize>,
    chunk_producers: Vec<Vec<usize>>,
    fishermen: Vec<(usize, u64)>,
    stake_change: BTreeMap<AccountId, Balance>,
    total_gas_used: GasUsage,
) -> EpochInfo {
    accounts.sort();
    let validator_to_index = accounts.iter().enumerate().fold(HashMap::new(), |mut acc, (i, x)| {
        acc.insert(x.0.to_string(), i);
        acc
    });
    EpochInfo {
        validators: accounts
            .into_iter()
            .map(|(account_id, amount)| ValidatorStake {
                account_id: account_id.to_string(),
                public_key: get_key_pair_from_seed(account_id).0,
                amount,
            })
            .collect(),
        validator_to_index,
        block_producers,
        chunk_producers,
        fishermen,
        stake_change,
        total_gas_used,
    }
}

pub fn epoch_config(
    epoch_length: BlockIndex,
    num_shards: ShardId,
    num_block_producers: usize,
    num_fisherman: usize,
    validator_kickout_threshold: u8,
) -> EpochConfig {
    EpochConfig {
        epoch_length,
        num_shards,
        num_block_producers,
        block_producers_per_shard: (0..num_shards).map(|_| num_block_producers).collect(),
        avg_fisherman_per_shard: (0..num_shards).map(|_| num_fisherman).collect(),
        validator_kickout_threshold,
    }
}

pub fn stake(account_id: &str, amount: Balance) -> ValidatorStake {
    let (public_key, _) = get_key_pair_from_seed(account_id);
    ValidatorStake::new(account_id.to_string(), public_key, amount)
}

pub fn setup_epoch_manager(
    validators: Vec<(&str, Balance)>,
    epoch_length: BlockIndex,
    num_shards: ShardId,
    num_seats: usize,
    num_fisherman: usize,
    kickout_threshold: u8,
) -> EpochManager {
    let store = create_test_store();
    let config =
        epoch_config(epoch_length, num_shards, num_seats, num_fisherman, kickout_threshold);
    EpochManager::new(
        store,
        config,
        validators.iter().map(|(account_id, balance)| stake(*account_id, *balance)).collect(),
    )
    .unwrap()
}

pub fn record_block(
    epoch_manager: &mut EpochManager,
    prev_h: CryptoHash,
    cur_h: CryptoHash,
    index: BlockIndex,
    proposals: Vec<ValidatorStake>,
) {
    epoch_manager
        .record_block_info(
            &cur_h,
            BlockInfo::new(index, prev_h, proposals, vec![], HashSet::default(), 0),
            [0; 32],
        )
        .unwrap()
        .commit()
        .unwrap();
}
