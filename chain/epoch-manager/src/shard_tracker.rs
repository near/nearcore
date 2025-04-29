use crate::EpochManagerAdapter;
use itertools::Itertools;
use near_cache::SyncLruCache;
use near_chain_configs::TrackedShardsConfig;
use near_chain_primitives::Error;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::StateSyncInfo;
use near_primitives::types::{AccountId, EpochId, ShardId};
use std::sync::Arc;

// bit mask for which shard to track
type BitMask = Vec<bool>;

/// Specifies which epoch we want to check for shard tracking
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EpochSelection {
    /// Previous epoch
    Previous,
    /// Current epoch
    Current,
    /// Next epoch
    Next,
}

/// A module responsible for determining which shards are tracked across epochs.
/// For supported configurations, see the `TrackedShardsConfig` documentation.
#[derive(Clone)]
pub struct ShardTracker {
    tracked_shards_config: TrackedShardsConfig,
    /// Stores shard tracking information by epoch, only useful if TrackedState == Accounts
    tracking_shards_cache: Arc<SyncLruCache<EpochId, BitMask>>,
    epoch_manager: Arc<dyn EpochManagerAdapter>,
}

impl ShardTracker {
    pub fn new(
        tracked_shards_config: TrackedShardsConfig,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
    ) -> Self {
        ShardTracker {
            tracked_shards_config,
            // 1024 epochs on mainnet is about 512 days which is more than enough,
            // and this is a cache anyway. The data size is pretty small as well,
            // only one bit per shard per epoch.
            tracking_shards_cache: Arc::new(SyncLruCache::new(1024)),
            epoch_manager,
        }
    }

    pub fn new_empty(epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self::new(TrackedShardsConfig::NoShards, epoch_manager)
    }

    fn tracks_shard_at_epoch(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<bool, EpochError> {
        match &self.tracked_shards_config {
            TrackedShardsConfig::NoShards => Ok(false),
            TrackedShardsConfig::AllShards => Ok(true),
            TrackedShardsConfig::Accounts(tracked_accounts) => {
                let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
                let tracking_mask = self.tracking_shards_cache.get_or_try_put(
                    *epoch_id,
                    |_| -> Result<Vec<bool>, EpochError> {
                        let mut tracking_mask =
                            shard_layout.shard_ids().map(|_| false).collect_vec();
                        for account_id in tracked_accounts {
                            let shard_id = shard_layout.account_id_to_shard_id(account_id);
                            let shard_index = shard_layout.get_shard_index(shard_id)?;
                            tracking_mask[shard_index] = true;
                        }
                        Ok(tracking_mask)
                    },
                )?;
                let shard_index = shard_layout.get_shard_index(shard_id)?;
                Ok(tracking_mask.get(shard_index).copied().unwrap_or(false))
            }
            TrackedShardsConfig::Schedule(schedule) => {
                assert_ne!(schedule.len(), 0);
                let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
                let epoch_height = epoch_info.epoch_height();
                let index = epoch_height % schedule.len() as u64;
                let subset = &schedule[index as usize];
                Ok(subset.contains(&shard_id))
            }
            TrackedShardsConfig::ShadowValidator(account_id) => {
                self.epoch_manager.cares_about_shard_in_epoch(epoch_id, account_id, shard_id)
            }
        }
    }

    fn tracks_shard(&self, shard_id: ShardId, prev_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(prev_hash)?;
        self.tracks_shard_at_epoch(shard_id, &epoch_id)
    }

    fn tracks_shard_next_epoch_from_prev_block(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
    ) -> Result<bool, EpochError> {
        let epoch_id = self.epoch_manager.get_next_epoch_id_from_prev_block(prev_hash)?;
        self.tracks_shard_at_epoch(shard_id, &epoch_id)
    }

    fn tracks_shard_prev_epoch_from_prev_block(
        &self,
        shard_id: ShardId,
        prev_hash: &CryptoHash,
    ) -> Result<bool, EpochError> {
        let epoch_id = self.epoch_manager.get_prev_epoch_id_from_prev_block(prev_hash)?;
        self.tracks_shard_at_epoch(shard_id, &epoch_id)
    }

    /// Whether the client cares about some shard in a specific epoch.
    /// * If `account_id` is None, `is_me` is not checked and the
    /// result indicates whether the client is tracking the shard
    /// * If `account_id` is not None, it is supposed to be a validator
    /// account and `is_me` indicates whether we check what shards
    /// the client tracks.
    pub fn cares_about_shard_in_epoch(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
        epoch_selection: EpochSelection,
    ) -> bool {
        // TODO: fix these unwrap_or here and handle error correctly. The current behavior masks potential errors and bugs
        // https://github.com/near/nearcore/issues/4936
        if let Some(account_id) = account_id {
            let account_cares_about_shard = match epoch_selection {
                EpochSelection::Previous => self
                    .epoch_manager
                    .cared_about_shard_prev_epoch_from_prev_block(parent_hash, account_id, shard_id)
                    .unwrap_or(false),
                EpochSelection::Current => self
                    .epoch_manager
                    .cares_about_shard_from_prev_block(parent_hash, account_id, shard_id)
                    .unwrap_or(false),
                EpochSelection::Next => self
                    .epoch_manager
                    .cares_about_shard_next_epoch_from_prev_block(parent_hash, account_id, shard_id)
                    .unwrap_or(false),
            };

            if account_cares_about_shard {
                // An account has to track this shard because of its validation duties.
                return true;
            }
            if !is_me {
                // We don't know how another node is configured.
                // It may track all shards, it may track no additional shards.
                return false;
            } else {
                // We have access to the node config. Use the config to find a definite answer.
            }
        }

        match self.tracked_shards_config {
            TrackedShardsConfig::NoShards => {
                // Avoid looking up EpochId as a performance optimization.
                false
            }
            TrackedShardsConfig::AllShards => {
                // Avoid looking up EpochId as a performance optimization.
                true
            }
            _ => match epoch_selection {
                EpochSelection::Previous => self
                    .tracks_shard_prev_epoch_from_prev_block(shard_id, parent_hash)
                    .unwrap_or(false),
                EpochSelection::Current => {
                    self.tracks_shard(shard_id, parent_hash).unwrap_or(false)
                }
                EpochSelection::Next => self
                    .tracks_shard_next_epoch_from_prev_block(shard_id, parent_hash)
                    .unwrap_or(false),
            },
        }
    }

    /// Whether the client cares about some shard in the previous epoch.
    /// * If `account_id` is None, `is_me` is not checked and the
    /// result indicates whether the client is tracking the shard
    /// * If `account_id` is not None, it is supposed to be a validator
    /// account and `is_me` indicates whether we check what shards
    /// the client tracks.
    pub fn cared_about_shard_in_prev_epoch(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.cares_about_shard_in_epoch(
            account_id,
            parent_hash,
            shard_id,
            is_me,
            EpochSelection::Previous,
        )
    }

    /// Whether the client cares about some shard right now.
    /// * If `account_id` is None, `is_me` is not checked and the
    /// result indicates whether the client is tracking the shard
    /// * If `account_id` is not None, it is supposed to be a validator
    /// account and `is_me` indicates whether we check what shards
    /// the client tracks.
    pub fn cares_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.cares_about_shard_in_epoch(
            account_id,
            parent_hash,
            shard_id,
            is_me,
            EpochSelection::Current,
        )
    }

    /// Whether the client cares about some shard in the next epoch.
    ///  Note that `shard_id` always refers to a shard in the current epoch
    ///  If shard layout will change next epoch,
    ///  returns true if it cares about any shard that `shard_id` will split to
    /// * If `account_id` is None, `is_me` is not checked and the
    /// result indicates whether the client will track the shard
    /// * If `account_id` is not None, it is supposed to be a validator
    /// account and `is_me` indicates whether we check what shards
    /// the client will track.
    pub fn will_care_about_shard(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.cares_about_shard_in_epoch(
            account_id,
            parent_hash,
            shard_id,
            is_me,
            EpochSelection::Next,
        )
    }

    // TODO(robin-near): I think we only need the shard_tracker if is_me is false.
    pub fn cares_about_shard_this_or_next_epoch(
        &self,
        account_id: Option<&AccountId>,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
        is_me: bool,
    ) -> bool {
        self.cares_about_shard(account_id, parent_hash, shard_id, is_me)
            || self.will_care_about_shard(account_id, parent_hash, shard_id, is_me)
    }

    /// Returns whether the node is configured for all shards tracking.
    pub fn tracks_all_shards(&self) -> bool {
        self.tracked_shards_config.tracks_all_shards()
    }

    /// Return all shards that whose states need to be caught up
    /// That has two cases:
    /// 1) Shard layout will change in the next epoch. In this case, the method returns all shards
    ///    in the current epoch that will be split into a future shard that `me` will track.
    /// 2) Shard layout will be the same. In this case, the method returns all shards that `me` will
    ///    track in the next epoch but not this epoch
    fn get_shards_to_state_sync(
        &self,
        me: &Option<AccountId>,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ShardId>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        let mut shards_to_sync = Vec::new();
        for shard_id in self.epoch_manager.shard_ids(&epoch_id)? {
            if self.should_catch_up_shard(me, parent_hash, shard_id)? {
                shards_to_sync.push(shard_id)
            }
        }
        Ok(shards_to_sync)
    }

    /// Returns whether we need to initiate state sync for the given `shard_id` for the epoch
    /// beginning after the block `epoch_last_block`. If that epoch is epoch T, the logic is:
    /// - will track the shard in epoch T+1
    /// - AND not tracking it in T
    /// - AND didn't track it in T-1
    /// We check that we didn't track it in T-1 because if so, and we're in the relatively rare case
    /// where we'll go from tracking it to not tracking it and back to tracking it in consecutive epochs,
    /// then we can just continue to apply chunks as if we were tracking it in epoch T, and there's no need to state sync.
    fn should_catch_up_shard(
        &self,
        me: &Option<AccountId>,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<bool, Error> {
        // Won't care about it next epoch, no need to state sync it.
        if !self.will_care_about_shard(me.as_ref(), prev_hash, shard_id, true) {
            return Ok(false);
        }
        // Currently tracking the shard, so no need to state sync it.
        if self.cares_about_shard(me.as_ref(), prev_hash, shard_id, true) {
            return Ok(false);
        }

        // Now we need to state sync it unless we were tracking the parent in the previous epoch,
        // in which case we don't need to because we already have the state, and can just continue applying chunks

        let tracked_before =
            self.cared_about_shard_in_prev_epoch(me.as_ref(), prev_hash, shard_id, true);
        Ok(!tracked_before)
    }

    /// Return a StateSyncInfo that includes the information needed for syncing state for shards needed
    /// in the next epoch.
    pub fn get_state_sync_info(
        &self,
        me: &Option<AccountId>,
        block_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<Option<StateSyncInfo>, Error> {
        let shards_to_state_sync = self.get_shards_to_state_sync(me, prev_hash)?;
        if shards_to_state_sync.is_empty() {
            Ok(None)
        } else {
            tracing::debug!(target: "chain", "Downloading state for {:?}, I'm {:?}", shards_to_state_sync, me);
            // Note that this block is the first block in an epoch because this function is only called
            // in get_catchup_and_state_sync_infos() when that is the case.
            let state_sync_info = StateSyncInfo::new(*block_hash, shards_to_state_sync);
            Ok(Some(state_sync_info))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ShardTracker;
    use crate::shard_tracker::TrackedShardsConfig;
    use crate::test_utils::hash_range;
    use crate::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
    use itertools::Itertools;
    use near_chain_configs::GenesisConfig;
    use near_chain_configs::test_genesis::TestEpochConfigBuilder;
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::epoch_block_info::BlockInfo;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::hash::CryptoHash;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountInfo, BlockHeight, EpochId, ProtocolVersion, ShardId};
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_test_store;
    use std::collections::HashSet;
    use std::sync::Arc;

    const DEFAULT_TOTAL_SUPPLY: u128 = 1_000_000_000_000;

    fn get_epoch_manager(genesis_protocol_version: ProtocolVersion) -> Arc<EpochManagerHandle> {
        let store = create_test_store();
        let mut genesis_config = GenesisConfig::default();
        genesis_config.protocol_version = genesis_protocol_version;
        genesis_config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 100,
        }];

        let epoch_config = TestEpochConfigBuilder::new().build();
        let config_store =
            EpochConfigStore::test_single_version(genesis_protocol_version, epoch_config);
        EpochManager::new_arc_handle_from_epoch_config_store(store, &genesis_config, config_store)
    }

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
                    DEFAULT_TOTAL_SUPPLY,
                    protocol_version,
                    height * 10u64.pow(9),
                    None,
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
    }

    fn get_all_shards_care_about(
        tracker: &ShardTracker,
        shard_ids: &[ShardId],
        parent_hash: &CryptoHash,
    ) -> HashSet<ShardId> {
        shard_ids
            .into_iter()
            .filter(|&&shard_id| tracker.cares_about_shard(None, parent_hash, shard_id, true))
            .cloned()
            .collect()
    }

    fn get_all_shards_will_care_about(
        tracker: &ShardTracker,
        shard_ids: &[ShardId],
        parent_hash: &CryptoHash,
    ) -> HashSet<ShardId> {
        shard_ids
            .into_iter()
            .filter(|&&shard_id| tracker.will_care_about_shard(None, parent_hash, shard_id, true))
            .cloned()
            .collect()
    }

    #[test]
    fn test_track_accounts() {
        let shard_ids = (0..4).map(ShardId::new).collect_vec();
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION);
        let shard_layout = epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
        let tracked_accounts = vec!["test1".parse().unwrap(), "test2".parse().unwrap()];
        let tracker =
            ShardTracker::new(TrackedShardsConfig::Accounts(tracked_accounts), epoch_manager);
        let mut total_tracked_shards = HashSet::new();
        total_tracked_shards.insert(shard_layout.account_id_to_shard_id(&"test1".parse().unwrap()));
        total_tracked_shards.insert(shard_layout.account_id_to_shard_id(&"test2".parse().unwrap()));

        assert_eq!(
            get_all_shards_care_about(&tracker, &shard_ids, &CryptoHash::default()),
            total_tracked_shards
        );
        assert_eq!(
            get_all_shards_will_care_about(&tracker, &shard_ids, &CryptoHash::default()),
            total_tracked_shards
        );
    }

    #[test]
    fn test_track_all_shards() {
        let shard_ids = (0..4).map(ShardId::new).collect_vec();
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION);
        let tracker = ShardTracker::new(TrackedShardsConfig::AllShards, epoch_manager);
        let total_tracked_shards: HashSet<_> = shard_ids.iter().cloned().collect();

        assert_eq!(
            get_all_shards_care_about(&tracker, &shard_ids, &CryptoHash::default()),
            total_tracked_shards
        );
        assert_eq!(
            get_all_shards_will_care_about(&tracker, &shard_ids, &CryptoHash::default()),
            total_tracked_shards
        );
    }

    #[test]
    fn test_track_schedule() {
        // Creates a ShardTracker that changes every epoch tracked shards.
        let shard_ids = (0..4).map(ShardId::new).collect_vec();

        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION);
        let subset1: HashSet<ShardId> =
            HashSet::from([0, 1]).into_iter().map(ShardId::new).collect();
        let subset2: HashSet<ShardId> =
            HashSet::from([1, 2]).into_iter().map(ShardId::new).collect();
        let subset3: HashSet<ShardId> =
            HashSet::from([2, 3]).into_iter().map(ShardId::new).collect();
        let tracker = ShardTracker::new(
            TrackedShardsConfig::Schedule(vec![
                subset1.clone().into_iter().collect(),
                subset2.clone().into_iter().map(Into::into).collect(),
                subset3.clone().into_iter().map(Into::into).collect(),
            ]),
            epoch_manager.clone(),
        );

        let h = hash_range(8);
        {
            let mut epoch_manager = epoch_manager.write();
            for i in 0..8 {
                record_block(
                    &mut epoch_manager,
                    if i > 0 { h[i - 1] } else { CryptoHash::default() },
                    h[i],
                    i as u64,
                    vec![],
                    PROTOCOL_VERSION,
                );
            }
        }

        assert_eq!(get_all_shards_care_about(&tracker, &shard_ids, &h[4]), subset2);
        assert_eq!(get_all_shards_care_about(&tracker, &shard_ids, &h[5]), subset3);
        assert_eq!(get_all_shards_care_about(&tracker, &shard_ids, &h[6]), subset1);
        assert_eq!(get_all_shards_care_about(&tracker, &shard_ids, &h[7]), subset2);

        assert_eq!(get_all_shards_will_care_about(&tracker, &shard_ids, &h[4]), subset3);
        assert_eq!(get_all_shards_will_care_about(&tracker, &shard_ids, &h[5]), subset1);
        assert_eq!(get_all_shards_will_care_about(&tracker, &shard_ids, &h[6]), subset2);
        assert_eq!(get_all_shards_will_care_about(&tracker, &shard_ids, &h[7]), subset3);
    }
}
