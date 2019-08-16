use crate::validator_manager::ValidatorAssignment;
use near_primitives::test_utils::get_key_pair_from_seed;
use near_primitives::types::{AccountId, Balance, BlockIndex, ValidatorStake};
use std::collections::{BTreeMap, HashMap};

pub fn change_stake(stake_changes: Vec<(&str, Balance)>) -> BTreeMap<AccountId, Balance> {
    stake_changes.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub fn assignment(
    mut accounts: Vec<(&str, Balance)>,
    block_producers: Vec<usize>,
    chunk_producers: Vec<Vec<usize>>,
    fishermen: Vec<(usize, u64)>,
    expected_epoch_start: BlockIndex,
    stake_change: BTreeMap<AccountId, Balance>,
) -> ValidatorAssignment {
    accounts.sort();
    let validator_to_index = accounts.iter().enumerate().fold(HashMap::new(), |mut acc, (i, x)| {
        acc.insert(x.0.to_string(), i);
        acc
    });
    ValidatorAssignment {
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
        fishermen: fishermen.into_iter().collect(),
        expected_epoch_start,
        stake_change,
    }
}
