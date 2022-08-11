use std::sync::{Arc, RwLock};

use crate::append_only_map::AppendOnlyMap;
use near_chain_configs::ClientConfig;
use near_epoch_manager::EpochManager;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::account_id_to_shard_id;
use near_primitives::types::{AccountId, EpochId, ShardId};

const POISONED_LOCK_ERR: &str = "The lock was poisoned.";

pub enum TrackedConfig {
    Accounts(Vec<AccountId>),
    AllShards,
}

impl TrackedConfig {
    pub fn new_empty() -> Self {
        TrackedConfig::Accounts(vec![])
    }

    pub fn from_config(config: &ClientConfig) -> Self {
        if config.tracked_shards.is_empty() {
            TrackedConfig::Accounts(config.tracked_accounts.clone())
        } else {
            TrackedConfig::AllShards
        }
    }
}

// bit mask for which shard to track
type BitMask = Vec<bool>;

/// Tracker that tracks shard ids and accounts. Right now, it only supports two modes
/// TrackedConfig::Accounts(accounts): track the shards where `accounts` belong to
/// TrackedConfig::AllShards: track all shards
pub struct ShardTracker {
    tracked_config: TrackedConfig,
    /// Stores shard tracking information by epoch, only useful if TrackedState == Accounts
    tracking_shards: AppendOnlyMap<EpochId, BitMask>,
    /// Epoch manager that for given block hash computes the epoch id.
    epoch_manager: Arc<RwLock<EpochManager>>,
}

impl ShardTracker {
    pub fn new(tracked_config: TrackedConfig, epoch_manager: Arc<RwLock<EpochManager>>) -> Self {
        ShardTracker { tracked_config, tracking_shards: AppendOnlyMap::new(), epoch_manager }
    }

    fn tracks_shard_at_epoch(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<bool, EpochError> {
        match &self.tracked_config {
            TrackedConfig::Accounts(tracked_accounts) => {
                let epoch_manager = self.epoch_manager.read().expect(POISONED_LOCK_ERR);
                let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
                let tracking_mask = self.tracking_shards.get_or_insert(epoch_id, || {
                    let mut tracking_mask = vec![false; shard_layout.num_shards() as usize];
                    for account_id in tracked_accounts {
                        let shard_id = account_id_to_shard_id(account_id, &shard_layout);
                        *tracking_mask.get_mut(shard_id as usize).unwrap() = true;
                    }
                    tracking_mask
                });
                Ok(tracking_mask.get(shard_id as usize).copied().unwrap_or(false))
            }
            TrackedConfig::AllShards => Ok(true),
        }
    }

    fn tracks_shard(&self, shard_id: ShardId, prev_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_id = {
            let epoch_manager = self.epoch_manager.read().expect(POISONED_LOCK_ERR);
            epoch_manager.get_epoch_id_from_prev_block(prev_hash)?
        };
        self.tracks_shard_at_epoch(shard_id, &epoch_id)
    }

    pub fn care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        // TODO: fix these unwrap_or here and handle error correctly. The current behavior masks potential errors and bugs
        // https://github.com/near/nearcore/issues/4936
        if let Some(account_id) = account_id {
            let account_cares_about_shard = {
                let epoch_manager = self.epoch_manager.read().expect(POISONED_LOCK_ERR);
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
        matches!(self.tracked_config, TrackedConfig::AllShards)
            || self.tracks_shard(shard_id, parent_hash).unwrap_or(false)
    }

    // `shard_id` always refers to a shard in the current epoch that the next block from `parent_hash` belongs
    // If shard layout will change next epoch, returns true if it cares about any shard
    // that `shard_id` will split to
    pub fn will_care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        if let Some(account_id) = account_id {
            let account_cares_about_shard = {
                let epoch_manager = self.epoch_manager.read().expect(POISONED_LOCK_ERR);
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
        matches!(self.tracked_config, TrackedConfig::AllShards)
            || self.tracks_shard(shard_id, parent_hash).unwrap_or(false)
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
    use near_primitives::types::{BlockHeight, EpochId, NumShards, ProtocolVersion, ShardId};
    use near_store::test_utils::create_test_store;

    use super::{account_id_to_shard_id, ShardTracker};
    use near_primitives::shard_layout::ShardLayout;

    use crate::shard_tracker::TrackedConfig;
    use crate::shard_tracker::POISONED_LOCK_ERR;
    use near_epoch_manager::test_utils::hash_range;
    use near_primitives::utils::get_num_seats_per_shard;
    use near_primitives::version::ProtocolFeature::SimpleNightshade;
    use near_primitives::version::PROTOCOL_VERSION;
    use num_rational::Ratio;

    const DEFAULT_TOTAL_SUPPLY: u128 = 1_000_000_000_000;

    fn get_epoch_manager(
        genesis_protocol_version: ProtocolVersion,
        num_shards: NumShards,
        simple_nightshade_shard_config: Option<ShardConfig>,
    ) -> EpochManager {
        let store = create_test_store();
        let initial_epoch_config = EpochConfig {
            epoch_length: 1,
            num_block_producer_seats: 1,
            num_block_producer_seats_per_shard: vec![1],
            avg_hidden_validator_seats_per_shard: vec![],
            block_producer_kickout_threshold: 90,
            chunk_producer_kickout_threshold: 60,
            fishermen_threshold: 0,
            online_max_threshold: Ratio::from_integer(1),
            online_min_threshold: Ratio::new(90, 100),
            minimum_stake_divisor: 1,
            protocol_upgrade_stake_threshold: Ratio::new(80, 100),
            protocol_upgrade_num_epochs: 2,
            shard_layout: ShardLayout::v0(num_shards, 0),
            validator_selection_config: Default::default(),
            #[cfg(feature = "protocol_feature_max_kickout_stake")]
            validator_max_kickout_stake_perc: 100,
        };
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Ratio::from_integer(0),
            num_blocks_per_year: 1000000,
            epoch_length: 1,
            protocol_reward_rate: Ratio::from_integer(0),
            protocol_treasury_account: "test".parse().unwrap(),
            online_max_threshold: initial_epoch_config.online_max_threshold,
            online_min_threshold: initial_epoch_config.online_min_threshold,
            num_seconds_per_year: 1000000,
        };
        EpochManager::new(
            store,
            AllEpochConfig::new(false, initial_epoch_config, simple_nightshade_shard_config),
            genesis_protocol_version,
            reward_calculator,
            vec![ValidatorStake::new(
                "test".parse().unwrap(),
                PublicKey::empty(KeyType::ED25519),
                100,
            )],
        )
        .unwrap()
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

    fn get_all_shards_care_about(
        tracker: &ShardTracker,
        num_shards: NumShards,
        parent_hash: &CryptoHash,
    ) -> HashSet<ShardId> {
        (0..num_shards)
            .filter(|shard_id| tracker.care_about_shard(None, parent_hash, *shard_id, true))
            .collect()
    }

    fn get_all_shards_will_care_about(
        tracker: &ShardTracker,
        num_shards: NumShards,
        parent_hash: &CryptoHash,
    ) -> HashSet<ShardId> {
        (0..num_shards)
            .filter(|shard_id| tracker.will_care_about_shard(None, parent_hash, *shard_id, true))
            .collect()
    }

    #[test]
    fn test_track_accounts() {
        let num_shards = 4;
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION, num_shards, None);
        let shard_layout = epoch_manager.get_shard_layout(&EpochId::default()).unwrap().clone();
        let tracked_accounts = vec!["test1".parse().unwrap(), "test2".parse().unwrap()];
        let tracker = ShardTracker::new(
            TrackedConfig::Accounts(tracked_accounts),
            Arc::new(RwLock::new(epoch_manager)),
        );
        let mut total_tracked_shards = HashSet::new();
        total_tracked_shards
            .insert(account_id_to_shard_id(&"test1".parse().unwrap(), &shard_layout));
        total_tracked_shards
            .insert(account_id_to_shard_id(&"test2".parse().unwrap(), &shard_layout));

        assert_eq!(
            get_all_shards_care_about(&tracker, num_shards, &CryptoHash::default()),
            total_tracked_shards
        );
        assert_eq!(
            get_all_shards_will_care_about(&tracker, num_shards, &CryptoHash::default()),
            total_tracked_shards
        );
    }

    #[test]
    fn test_track_all_shards() {
        let num_shards = 4;
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION, num_shards, None);
        let tracker =
            ShardTracker::new(TrackedConfig::AllShards, Arc::new(RwLock::new(epoch_manager)));
        let total_tracked_shards: HashSet<_> = (0..num_shards).collect();

        assert_eq!(
            get_all_shards_care_about(&tracker, num_shards, &CryptoHash::default()),
            total_tracked_shards
        );
        assert_eq!(
            get_all_shards_will_care_about(&tracker, num_shards, &CryptoHash::default()),
            total_tracked_shards
        );
    }

    #[test]
    fn test_track_shards_shard_layout_change() {
        let simple_nightshade_version = SimpleNightshade.protocol_version();
        let shard_layout = ShardLayout::v1(
            vec!["aurora".parse().unwrap()],
            vec!["hhh", "ooo"].into_iter().map(|x| x.parse().unwrap()).collect(),
            Some(vec![vec![0, 1, 2, 3]]),
            1,
        );
        let shard_config = ShardConfig {
            num_block_producer_seats_per_shard: get_num_seats_per_shard(4, 2),
            avg_hidden_validator_seats_per_shard: get_num_seats_per_shard(4, 0),
            shard_layout: shard_layout,
        };
        let epoch_manager = Arc::new(RwLock::new(get_epoch_manager(
            simple_nightshade_version - 1,
            1,
            Some(shard_config),
        )));
        let tracked_accounts = vec!["near".parse().unwrap(), "zoo".parse().unwrap()];
        let tracker = ShardTracker::new(
            TrackedConfig::Accounts(tracked_accounts.clone()),
            epoch_manager.clone(),
        );

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
            for i in 1..8 {
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

        // verify tracker is tracking the correct shards before and after resharding
        for i in 1..8 {
            let mut total_tracked_shards = HashSet::new();
            let num_shards = {
                let epoch_manager = epoch_manager.read().expect(POISONED_LOCK_ERR);
                let epoch_id = epoch_manager.get_epoch_id_from_prev_block(&h[i - 1]).unwrap();
                let shard_layout = epoch_manager.get_shard_layout(&epoch_id).unwrap();
                for account_id in tracked_accounts.iter() {
                    total_tracked_shards.insert(account_id_to_shard_id(account_id, &shard_layout));
                }
                shard_layout.num_shards()
            };

            assert_eq!(
                get_all_shards_care_about(&tracker, num_shards, &h[i - 1]),
                total_tracked_shards
            );
            assert_eq!(
                get_all_shards_will_care_about(&tracker, num_shards, &h[i - 1]),
                total_tracked_shards
            );
        }
    }
}
