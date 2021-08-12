use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use tracing::info;

use near_epoch_manager::EpochManager;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout};
use near_primitives::types::{AccountId, EpochId, ShardId};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

/// Tracker that tracks shard ids and accounts. It maintains two items: `tracked_accounts` and
/// `tracked_shards`. The shards that are actually tracked are the union of shards that `tracked_accounts`
/// are in and `tracked_shards`.
#[derive(Clone)]
pub struct ShardTracker {
    /// Tracked accounts by shard id. For each shard id, the corresponding set of accounts should be
    /// non empty (otherwise the entry should not exist).
    tracked_accounts: HashMap<ShardId, HashSet<AccountId>>,
    /// Tracked shards.
    tracked_shards: HashSet<ShardId>,
    /// Combination of shards that correspond to tracked accounts and tracked shards.
    actual_tracked_shards: HashSet<ShardId>,
    /// Epoch manager that for given block hash computes the epoch id.
    epoch_manager: Arc<RwLock<EpochManager>>,
    /// Current shard layout
    /// TODO: ShardTracker does not work if shard_layout is changed,
    ///       fix this when https://github.com/near/nearcore/pull/4668 is merged
    shard_layout: ShardLayout,
}

impl ShardTracker {
    pub fn new(
        accounts: Vec<AccountId>,
        shards: Vec<ShardId>,
        epoch_manager: Arc<RwLock<EpochManager>>,
    ) -> Self {
        let shard_layout = {
            let mut epoch_manager = epoch_manager.write().expect(POISONED_LOCK_ERR);
            epoch_manager.get_shard_layout(&EpochId::default()).unwrap()
        };
        let tracked_accounts = accounts.into_iter().fold(HashMap::new(), |mut acc, x| {
            let shard_id = account_id_to_shard_id(&x, &shard_layout);
            acc.entry(shard_id).or_insert_with(HashSet::new).insert(x);
            acc
        });
        let tracked_shards: HashSet<_> = shards.into_iter().collect();
        let mut actual_tracked_shards = tracked_shards.clone();
        for (shard_id, _) in tracked_accounts.iter() {
            actual_tracked_shards.insert(*shard_id);
        }
        info!(target: "runtime", "Tracking shards: {:?}", actual_tracked_shards);
        ShardTracker {
            tracked_accounts,
            tracked_shards,
            actual_tracked_shards,
            epoch_manager,
            shard_layout,
        }
    }

    fn track_account(&mut self, account_id: &AccountId) {
        let shard_id = account_id_to_shard_id(account_id, &self.shard_layout);
        self.tracked_accounts
            .entry(shard_id)
            .or_insert_with(HashSet::new)
            .insert(account_id.clone());
        self.actual_tracked_shards.insert(shard_id);
    }

    /// Track a list of accounts. The tracking will take effect immediately because
    /// even if we want to start tracking the accounts in the next epoch, it cannot harm
    /// us to start tracking them earlier.
    #[allow(unused)]
    pub fn track_accounts(&mut self, account_ids: &[AccountId]) {
        for account_id in account_ids.iter() {
            self.track_account(account_id);
        }
    }

    fn track_shard(&mut self, shard_id: ShardId) {
        self.tracked_shards.insert(shard_id);
        self.actual_tracked_shards.insert(shard_id);
    }

    /// Track a list of shards. Similar to tracking accounts, the tracking starts immediately.
    #[allow(unused)]
    pub fn track_shards(&mut self, shard_ids: &[ShardId]) {
        for shard_id in shard_ids.iter() {
            self.track_shard(*shard_id);
        }
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
            }
            account_cares_about_shard || self.actual_tracked_shards.contains(&shard_id)
        } else {
            self.actual_tracked_shards.contains(&shard_id)
        }
    }

    // If ShardLayout will change in the next epoch, `shard_id` refers to shard in the new
    // ShardLayout
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
        self.actual_tracked_shards.contains(&shard_id)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::{Arc, RwLock};

    use near_crypto::{KeyType, PublicKey};
    use near_epoch_manager::{EpochManager, RewardCalculator};
    use near_primitives::epoch_manager::block_info::BlockInfo;
    use near_primitives::epoch_manager::{AllEpochConfig, EpochConfig, ShardConfig};
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountId, BlockHeight, NumShards, ProtocolVersion};
    use near_store::test_utils::create_test_store;

    use super::{account_id_to_shard_id, ShardTracker};
    use near_primitives::shard_layout::ShardLayout;

    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Rational;

    const DEFAULT_TOTAL_SUPPLY: u128 = 1_000_000_000_000;

    fn get_epoch_manager(
        genesis_protocol_version: ProtocolVersion,
        num_shards: NumShards,
        simple_nightshade_shard_config: Option<ShardConfig>,
    ) -> Arc<RwLock<EpochManager>> {
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
            shard_layout: ShardLayout::v0(num_shards),
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
                AllEpochConfig::new(initial_epoch_config, simple_nightshade_shard_config),
                genesis_protocol_version,
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
        protocol_version: ProtocolVersion,
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
                    protocol_version,
                    height * 10u64.pow(9),
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
    }

    #[test]
    fn test_track_new_accounts_and_shards() {
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION, 4, None);
        let mut tracker = ShardTracker::new(vec![], vec![], epoch_manager);
        tracker.track_accounts(&["test1".parse().unwrap(), "test2".parse().unwrap()]);
        tracker.track_shards(&[2, 3]);
        let mut total_tracked_shards = HashSet::new();
        total_tracked_shards
            .insert(account_id_to_shard_id(&"test1".parse().unwrap(), &tracker.shard_layout));
        total_tracked_shards
            .insert(account_id_to_shard_id(&"test2".parse().unwrap(), &tracker.shard_layout));
        total_tracked_shards.insert(2);
        total_tracked_shards.insert(3);
        assert_eq!(tracker.actual_tracked_shards, total_tracked_shards);
    }

    /*
    #[test]
    #[cfg(feature = "protocol_feature_simple_nightshade")]
    fn test_track_shards_shard_layout_change() {
        let simple_nightshade_version = SimpleNightshade.protocol_version();
        let shard_layout = ShardLayout::v1(
            vec!["aurora".parse().unwrap()],
            vec!["h", "o"].into_iter().map(|x| x.parse().unwrap()).collect(),
            Some(vec![0, 0, 0, 0]),
        );
        let shard_config = ShardConfig {
            num_block_producer_seats_per_shard: get_num_seats_per_shard(4, 2),
            avg_hidden_validator_seats_per_shard: get_num_seats_per_shard(4, 0),
            shard_layout: shard_layout.clone(),
        };
        let epoch_manager = get_epoch_manager(simple_nightshade_version - 1, 1, Some(shard_config));
        let mut tracker = ShardTracker::new(vec![], vec![], epoch_manager.clone());
        tracker.track_accounts(&["near".parse().unwrap(), "zoo".parse().unwrap()]);
        tracker.track_shards(&[0]);

        let h = hash_range(8);
        {
            let mut epoch_manager = epoch_manager.write().expect(POISONED_LOCK_ERR);
            record_block(
                &mut epoch_manager,
                CryptoHash::default(),
                h[0],
                0,
                vec![],
                simple_nightshade_version,
            );
            for i in 1..4 {
                record_block(
                    &mut epoch_manager,
                    h[i - 1],
                    h[i],
                    i as u64,
                    vec![],
                    simple_nightshade_version,
                );
            }
            assert_eq!(
                epoch_manager.get_epoch_info(&EpochId(h[0])).unwrap().protocol_version(),
                simple_nightshade_version - 1
            );
            assert_eq!(
                epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap().protocol_version(),
                simple_nightshade_version
            );
        }

        // TODO: verify tracker is tracking the correct shards before and after resharding
    }
     */
}
