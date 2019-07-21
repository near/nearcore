use crate::validator_manager::ValidatorAssignment;
use near_client::BlockProducer;
use near_primitives::serialize::BaseEncode;
use near_primitives::test_utils::get_key_pair_from_seed;
use near_primitives::transaction::{SignedTransaction, StakeTransaction, TransactionBody};
use near_primitives::types::{AccountId, Balance, BlockIndex, Nonce, ValidatorStake};
use std::collections::BTreeMap;

pub fn stake(nonce: Nonce, sender: &BlockProducer, amount: Balance) -> SignedTransaction {
    TransactionBody::Stake(StakeTransaction {
        nonce,
        originator: sender.account_id.clone(),
        amount,
        public_key: sender.signer.public_key().to_base(),
    })
    .sign(&*sender.signer.clone())
}

pub fn change_stake(stake_changes: Vec<(&str, Balance)>) -> BTreeMap<AccountId, Balance> {
    stake_changes.into_iter().map(|(k, v)| (k.to_string(), v)).collect()
}

pub fn assignment(
    mut accounts: Vec<(&str, Balance)>,
    block_producers: Vec<u64>,
    chunk_producers: Vec<Vec<(usize, u64)>>,
    fishermen: Vec<(usize, u64)>,
    expected_epoch_start: BlockIndex,
    stake_change: BTreeMap<AccountId, Balance>,
) -> ValidatorAssignment {
    ValidatorAssignment {
        validators: accounts
            .drain(..)
            .map(|(account_id, amount)| {
                (
                    account_id.to_string(),
                    ValidatorStake {
                        account_id: account_id.to_string(),
                        public_key: get_key_pair_from_seed(account_id).0,
                        amount,
                    },
                )
            })
            .collect(),
        block_producers,
        chunk_producers,
        fishermen,
        expected_epoch_start,
        stake_change,
    }
}
