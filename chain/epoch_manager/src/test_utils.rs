use near_primitives::test_utils::get_key_pair_from_seed;
use near_primitives::types::{AccountId, Balance, BlockIndex, ShardId, ValidatorStake};
use std::collections::{BTreeMap, HashMap};

use crate::types::{EpochConfig, EpochInfo};
use near_primitives::hash::{hash, CryptoHash};

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
