use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::sync::{Arc, RwLock};

use byteorder::{LittleEndian, ReadBytesExt};

use near_epoch_manager::EpochManager;
use near_primitives::hash::{hash, CryptoHash};
use near_primitives::types::{AccountId, NumShards, ShardId};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub fn account_id_to_shard_id(account_id: &AccountId, num_shards: NumShards) -> ShardId {
    let mut cursor = Cursor::new(hash(account_id.as_ref().as_bytes()).0);
    cursor.read_u64::<LittleEndian>().expect("Must not happened") % (num_shards)
}

/// Tracker that tracks shard ids and accounts. It maintains two items: `tracked_accounts` and
/// `tracked_shards`. The shards that are actually tracked are the union of shards that `tracked_accounts`
/// are in and `tracked_shards`.
#[derive(Clone)]
pub struct ShardTracker {
    /// Tracked accounts by shard id. For each shard id, the corresponding set of accounts should be
    /// non empty (otherwise the entry should not exist).
    tracked_accounts: HashMap<ShardId, HashSet<AccountId>>,
    /// Whether to track all shards
    track_all_shards: bool,
    /// Epoch manager that for given block hash computes the epoch id.
    epoch_manager: Arc<RwLock<EpochManager>>,
    /// Number of shards in the system.
    num_shards: NumShards,
}

impl ShardTracker {
    pub fn new(
        accounts: Vec<AccountId>,
        track_all_shards: bool,
        epoch_manager: Arc<RwLock<EpochManager>>,
        num_shards: NumShards,
    ) -> Self {
        let tracked_accounts = accounts.into_iter().fold(HashMap::new(), |mut acc, x| {
            let shard_id = account_id_to_shard_id(&x, num_shards);
            acc.entry(shard_id).or_insert_with(HashSet::new).insert(x);
            acc
        });
        ShardTracker { tracked_accounts, track_all_shards, epoch_manager, num_shards }
    }

    pub fn care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        if let Some(account_id) = account_id {
            let account_cares_about_shard = {
                let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
                epoch_manager
                    .cares_about_shard_from_prev_block(parent_hash, account_id, shard_id)
                    .unwrap_or(false)
            };
            if !is_me {
                return account_cares_about_shard;
            } else if account_cares_about_shard {
                return true;
            }
        }
        self.track_all_shards || self.tracked_accounts.contains_key(&shard_id)
    }

    pub fn will_care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        if let Some(account_id) = account_id {
            let account_cares_about_shard = {
                let mut epoch_manager = self.epoch_manager.write().expect(POISONED_LOCK_ERR);
                epoch_manager
                    .cares_about_shard_next_epoch_from_prev_block(parent_hash, account_id, shard_id)
                    .unwrap_or(false)
            };
            if !is_me {
                return account_cares_about_shard;
            } else if account_cares_about_shard {
                return true;
            }
        }
        self.track_all_shards || self.tracked_accounts.contains_key(&shard_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, RwLock};

    use near_crypto::{KeyType, PublicKey};
    use near_epoch_manager::{EpochManager, RewardCalculator};
    use near_primitives::epoch_manager::block_info::BlockInfo;
    use near_primitives::epoch_manager::EpochConfig;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountId, BlockHeight, NumShards, ShardId};
    use near_store::test_utils::create_test_store;

    use super::{account_id_to_shard_id, ShardTracker};
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Rational;

    const DEFAULT_TOTAL_SUPPLY: u128 = 1_000_000_000_000;

    fn get_epoch_manager() -> Arc<RwLock<EpochManager>> {
        let store = create_test_store();
        let initial_epoch_config = EpochConfig {
            epoch_length: 1,
            num_block_producer_seats: 1,
            num_block_producer_seats_per_shard: vec![1],
            avg_hidden_validator_seats_per_shard: vec![],
            block_producer_kickout_threshold: 90,
            chunk_producer_kickout_threshold: 60,
            fishermen_threshold: 0,
            online_max_threshold: Rational::from_integer(1),
            online_min_threshold: Rational::new(90, 100),
            minimum_stake_divisor: 1,
            protocol_upgrade_stake_threshold: Rational::new(80, 100),
            protocol_upgrade_num_epochs: 2,
        };
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::from_integer(0),
            num_blocks_per_year: 1000000,
            epoch_length: 1,
            protocol_reward_rate: Rational::from_integer(0),
            protocol_treasury_account: AccountId::test_account(),
            online_max_threshold: initial_epoch_config.online_max_threshold,
            online_min_threshold: initial_epoch_config.online_min_threshold,
            num_seconds_per_year: 1000000,
        };
        Arc::new(RwLock::new(
            EpochManager::new(
                store,
                initial_epoch_config,
                PROTOCOL_VERSION,
                reward_calculator,
                vec![ValidatorStake::new(
                    AccountId::test_account(),
                    PublicKey::empty(KeyType::ED25519),
                    100,
                )],
            )
            .unwrap(),
        ))
    }

    #[allow(unused)]
    pub fn record_block(
        epoch_manager: &mut EpochManager,
        prev_h: CryptoHash,
        cur_h: CryptoHash,
        height: BlockHeight,
        proposals: Vec<ValidatorStake>,
    ) {
        epoch_manager
            .record_block_info(
                BlockInfo::new(
                    cur_h,
                    height,
                    0,
                    prev_h,
                    prev_h,
                    proposals,
                    vec![],
                    vec![],
                    DEFAULT_TOTAL_SUPPLY,
                    PROTOCOL_VERSION,
                    height * 10u64.pow(9),
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
    }

    fn get_all_shards_care_about(
        tracker: &ShardTracker,
        num_shards: NumShards,
    ) -> HashSet<ShardId> {
        (0..num_shards)
            .filter(|shard_id| {
                tracker.care_about_shard(None, &CryptoHash::default(), *shard_id, true)
            })
            .collect()
    }

    fn get_all_shards_will_care_about(
        tracker: &ShardTracker,
        num_shards: NumShards,
    ) -> HashSet<ShardId> {
        (0..num_shards)
            .filter(|shard_id| {
                tracker.will_care_about_shard(None, &CryptoHash::default(), *shard_id, true)
            })
            .collect()
    }

    #[test]
    fn test_track_accounts() {
        let num_shards = 4;
        let epoch_manager = get_epoch_manager();
        let tracked_accounts = vec!["test1".parse().unwrap(), "test2".parse().unwrap()];
        let tracker = ShardTracker::new(tracked_accounts, false, epoch_manager, num_shards);
        let mut total_tracked_shards = HashSet::new();
        total_tracked_shards.insert(account_id_to_shard_id(&"test1".parse().unwrap(), num_shards));
        total_tracked_shards.insert(account_id_to_shard_id(&"test2".parse().unwrap(), num_shards));

        assert_eq!(get_all_shards_care_about(&tracker, num_shards), total_tracked_shards);
        assert_eq!(get_all_shards_will_care_about(&tracker, num_shards), total_tracked_shards);
    }

    #[test]
    fn test_track_all_shards() {
        let num_shards = 4;
        let epoch_manager = get_epoch_manager();
        let tracker = ShardTracker::new(vec![], true, epoch_manager, num_shards);
        let total_tracked_shards: HashSet<_> = (0..num_shards).collect();

        assert_eq!(get_all_shards_care_about(&tracker, num_shards), total_tracked_shards);
        assert_eq!(get_all_shards_will_care_about(&tracker, num_shards), total_tracked_shards);
    }
}
