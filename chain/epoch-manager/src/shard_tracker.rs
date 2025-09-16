use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::EpochManagerAdapter;
use itertools::Itertools;
use near_cache::SyncLruCache;
use near_chain_configs::{MutableConfigValue, MutableValidatorSigner, TrackedShardsConfig};
use near_chain_primitives::{ApplyChunksMode, Error};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::sharding::StateSyncInfo;
use near_primitives::types::{AccountId, EpochId, ShardId};
use near_primitives::validator_signer::EmptyValidatorSigner;
use near_store::ShardUId;
use parking_lot::Mutex;

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
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    validator_signer: MutableValidatorSigner,
    /// Stores a bitmask of tracked shards for each epoch ID.
    /// This cache is used to avoid recomputing the set of tracked shards.
    /// Only relevant when `TrackedShardsConfig` is set to `Accounts`.
    tracked_accounts_shard_cache: Arc<SyncLruCache<EpochId, BitMask>>,
    /// Caches whether a given shard is a descendant of any of the `tracked_shards`.
    /// This is required in scenarios with resharding, where the node must continue tracking
    /// not only the originally configured shards but also their descendants.
    /// The result is cached to avoid recomputing descendant relationships repeatedly.
    /// Only relevant when `TrackedShardsConfig` is set to `Shards(tracked_shards)`.
    descendant_of_tracked_shard_cache: Arc<Mutex<HashMap<ShardId, bool>>>,
}

impl ShardTracker {
    pub fn new(
        tracked_shards_config: TrackedShardsConfig,
        epoch_manager: Arc<dyn EpochManagerAdapter>,
        validator_signer: MutableValidatorSigner,
    ) -> Self {
        ShardTracker {
            tracked_shards_config,
            epoch_manager,
            validator_signer,
            // 1024 epochs on mainnet is about 512 days which is more than enough,
            // and this is a cache anyway. The data size is pretty small as well,
            // only one bit per shard per epoch.
            tracked_accounts_shard_cache: Arc::new(SyncLruCache::new(1024)),
            descendant_of_tracked_shard_cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn new_empty(epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        let empty_validator_signer = MutableConfigValue::new(
            Some(Arc::new(EmptyValidatorSigner::default().into())),
            "validator_signer",
        );
        Self::new(TrackedShardsConfig::NoShards, epoch_manager, empty_validator_signer)
    }

    fn tracks_shard_at_epoch(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<bool, EpochError> {
        // TODO(#13445): Add a debug assertion that shard exists in the epoch.
        match &self.tracked_shards_config {
            TrackedShardsConfig::NoShards => Ok(false),
            TrackedShardsConfig::AllShards => Ok(true),
            TrackedShardsConfig::Shards(tracked_shards) => {
                // TODO(#13445): Turn the check below into a debug assert and call it earlier,
                // for all `tracked_shards_config` variants.
                let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
                if !shard_layout.shard_ids().contains(&shard_id) {
                    return Ok(false);
                }
                self.check_if_descendant_of_tracked_shard(shard_id, tracked_shards, epoch_id)
            }
            TrackedShardsConfig::Accounts(tracked_accounts) => {
                self.check_if_shard_contains_tracked_account(shard_id, tracked_accounts, epoch_id)
            }
            TrackedShardsConfig::Schedule(schedule) => {
                self.check_if_shard_is_tracked_according_to_schedule(shard_id, schedule, epoch_id)
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
    fn cares_about_shard_in_epoch_from_prev_hash(
        &self,
        account_id: Option<&AccountId>,
        prev_hash: &CryptoHash,
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
                    .cared_about_shard_prev_epoch_from_prev_block(prev_hash, account_id, shard_id)
                    .unwrap_or(false),
                EpochSelection::Current => self
                    .epoch_manager
                    .cares_about_shard_from_prev_block(prev_hash, account_id, shard_id)
                    .unwrap_or(false),
                EpochSelection::Next => self
                    .epoch_manager
                    .cares_about_shard_next_epoch_from_prev_block(prev_hash, account_id, shard_id)
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
                    .tracks_shard_prev_epoch_from_prev_block(shard_id, prev_hash)
                    .unwrap_or(false),
                EpochSelection::Current => self.tracks_shard(shard_id, prev_hash).unwrap_or(false),
                EpochSelection::Next => self
                    .tracks_shard_next_epoch_from_prev_block(shard_id, prev_hash)
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
    pub fn cared_about_shard_in_prev_epoch_from_prev_hash(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let account_id = self.validator_signer.get().map(|v| v.validator_id().clone());
        self.cares_about_shard_in_epoch_from_prev_hash(
            account_id.as_ref(),
            prev_hash,
            shard_id,
            true,
            EpochSelection::Previous,
        )
    }

    /// Whether the client cares about some shard right now.
    pub fn cares_about_shard(&self, parent_hash: &CryptoHash, shard_id: ShardId) -> bool {
        let account_id = self.validator_signer.get().map(|v| v.validator_id().clone());
        self.cares_about_shard_in_epoch_from_prev_hash(
            account_id.as_ref(),
            parent_hash,
            shard_id,
            true,
            EpochSelection::Current,
        )
    }

    /// Whether the client cares about some shard in the next epoch.
    ///
    /// Note that `shard_id` always refers to a shard in the current epoch. If shard layout will
    /// change next epoch, return true if it cares about any shard that `shard_id` will split to
    pub fn will_care_about_shard(&self, parent_hash: &CryptoHash, shard_id: ShardId) -> bool {
        let account_id = self.validator_signer.get().map(|v| v.validator_id().clone());
        self.cares_about_shard_in_epoch_from_prev_hash(
            account_id.as_ref(),
            parent_hash,
            shard_id,
            true,
            EpochSelection::Next,
        )
    }

    /// Whether the client cares about some shard in this or next epoch.
    ///
    /// Note that `shard_id` always refers to a shard in the current epoch. If shard layout will
    /// change next epoch, return true if it cares about any shard that `shard_id` will split to
    pub fn cares_about_shard_this_or_next_epoch(
        &self,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        self.cares_about_shard(parent_hash, shard_id)
            || self.will_care_about_shard(parent_hash, shard_id)
    }

    /// Whether some client tracking account_id cares about shard_id in this or next epoch.
    ///
    /// Note that `shard_id` always refers to a shard in the current epoch. If shard layout will
    /// change next epoch, return true if it cares about any shard that `shard_id` will split to
    pub fn cares_about_shard_this_or_next_epoch_for_account_id(
        &self,
        account_id: &AccountId,
        parent_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let cares_about_shard = self.cares_about_shard_in_epoch_from_prev_hash(
            Some(account_id),
            parent_hash,
            shard_id,
            false,
            EpochSelection::Current,
        );
        let will_care_about_shard = self.cares_about_shard_in_epoch_from_prev_hash(
            Some(account_id),
            parent_hash,
            shard_id,
            false,
            EpochSelection::Next,
        );
        cares_about_shard || will_care_about_shard
    }

    /// Returns whether the node is configured for all shards tracking.
    pub fn tracks_all_shards(&self) -> bool {
        self.tracked_shards_config.tracks_all_shards()
    }

    /// Returns whether the tracker configuration is valid for an archival node.
    pub fn is_valid_for_archival(&self) -> bool {
        match &self.tracked_shards_config {
            TrackedShardsConfig::AllShards => true,
            TrackedShardsConfig::Shards(shards) => !shards.is_empty(),
            // `Accounts` config is likely to work as well,
            // but this hasn't been fully tested or verified yet.
            // Consider enabling support after proper validation.
            _ => false,
        }
    }

    /// We want to guarantee that transactions are only applied once for each shard,
    /// even though apply_chunks may be called twice, once with
    /// ApplyChunksMode::NotCaughtUp once with ApplyChunksMode::CatchingUp. Note
    /// that it does not guard whether the children shards are ready or not, see the
    /// comments before `need_to_reshard`
    pub fn should_apply_chunk(
        &self,
        mode: ApplyChunksMode,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> bool {
        let cares_about_shard_this_epoch = self.cares_about_shard(prev_hash, shard_id);
        let cares_about_shard_next_epoch = self.will_care_about_shard(prev_hash, shard_id);
        let cared_about_shard_prev_epoch =
            self.cared_about_shard_in_prev_epoch_from_prev_hash(prev_hash, shard_id);
        match mode {
            // next epoch's shard states are not ready, only update this epoch's shards plus shards we will care about in the future
            // and already have state for
            ApplyChunksMode::NotCaughtUp => {
                cares_about_shard_this_epoch
                    || (cares_about_shard_next_epoch && cared_about_shard_prev_epoch)
            }
            // update both this epoch and next epoch
            ApplyChunksMode::IsCaughtUp => {
                cares_about_shard_this_epoch || cares_about_shard_next_epoch
            }
            // catching up next epoch's shard states, do not update this epoch's shard state
            // since it has already been updated through ApplyChunksMode::NotCaughtUp
            ApplyChunksMode::CatchingUp => {
                let syncing_shard = !cares_about_shard_this_epoch
                    && cares_about_shard_next_epoch
                    && !cared_about_shard_prev_epoch;
                syncing_shard
            }
        }
    }

    /// Return all shards that whose states need to be caught up
    /// That has two cases:
    /// 1) Shard layout will change in the next epoch. In this case, the method returns all shards
    ///    in the current epoch that will be split into a future shard that `me` will track in the
    ///    next epoch but not this epoch.
    /// 2) Shard layout will be the same. In this case, the method returns all shards that `me` will
    ///    track in the next epoch but not this epoch
    fn get_shards_to_state_sync(&self, parent_hash: &CryptoHash) -> Result<Vec<ShardId>, Error> {
        let epoch_id = self.epoch_manager.get_epoch_id_from_prev_block(parent_hash)?;
        let mut shards_to_sync = Vec::new();
        for shard_id in self.epoch_manager.shard_ids(&epoch_id)? {
            if self.should_catch_up_shard(parent_hash, shard_id)? {
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
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<bool, Error> {
        // Won't care about it next epoch, no need to state sync it.
        if !self.will_care_about_shard(prev_hash, shard_id) {
            return Ok(false);
        }
        // Currently tracking the shard, so no need to state sync it.
        if self.cares_about_shard(prev_hash, shard_id) {
            return Ok(false);
        }

        // Now we need to state sync it unless we were tracking the parent in the previous epoch,
        // in which case we don't need to because we already have the state, and can just continue applying chunks

        let tracked_before =
            self.cared_about_shard_in_prev_epoch_from_prev_hash(prev_hash, shard_id);
        Ok(!tracked_before)
    }

    /// Return a StateSyncInfo that includes the information needed for syncing state for shards needed
    /// in the next epoch.
    pub fn get_state_sync_info(
        &self,
        block_hash: &CryptoHash,
        prev_hash: &CryptoHash,
    ) -> Result<Option<StateSyncInfo>, Error> {
        let shards_to_state_sync = self.get_shards_to_state_sync(prev_hash)?;
        if shards_to_state_sync.is_empty() {
            Ok(None)
        } else {
            tracing::debug!(target: "chain", "Downloading state for {:?}", shards_to_state_sync);
            // Note that this block is the first block in an epoch because this function is only called
            // in get_catchup_and_state_sync_infos() when that is the case.
            let state_sync_info = StateSyncInfo::new(*block_hash, shards_to_state_sync);
            Ok(Some(state_sync_info))
        }
    }

    /// Checks whether the given `shard_id` is part of the scheduled tracked shards for the
    /// specified `epoch_id`, based on the provided `schedule`.
    fn check_if_shard_is_tracked_according_to_schedule(
        &self,
        shard_id: ShardId,
        schedule: &Vec<Vec<ShardId>>,
        epoch_id: &EpochId,
    ) -> Result<bool, EpochError> {
        assert_ne!(schedule.len(), 0);
        let epoch_info = self.epoch_manager.get_epoch_info(epoch_id)?;
        let epoch_height = epoch_info.epoch_height();
        let index = epoch_height % schedule.len() as u64;
        let subset = &schedule[index as usize];
        Ok(subset.contains(&shard_id))
    }

    /// Checks whether `shard_id` contains any of the `tracked_accounts` in the given `epoch_id`.
    fn check_if_shard_contains_tracked_account(
        &self,
        shard_id: ShardId,
        tracked_accounts: &Vec<AccountId>,
        epoch_id: &EpochId,
    ) -> Result<bool, EpochError> {
        let shard_layout = self.epoch_manager.get_shard_layout(epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        if let Some(tracking_mask) = self.tracked_accounts_shard_cache.lock().get(&epoch_id) {
            return Ok(tracking_mask.get(shard_index).copied().unwrap_or(false));
        }

        let mut tracking_mask = shard_layout.shard_ids().map(|_| false).collect_vec();
        for account_id in tracked_accounts {
            let shard_id = shard_layout.account_id_to_shard_id(account_id);
            let shard_index = shard_layout.get_shard_index(shard_id)?;
            tracking_mask[shard_index] = true;
        }
        let is_tracked = tracking_mask.get(shard_index).copied().unwrap_or(false);
        self.tracked_accounts_shard_cache.lock().put(*epoch_id, tracking_mask);
        Ok(is_tracked)
    }

    /// Checks whether `shard_id` is a descendant of any of the `tracked_shards`.
    /// Assumes that `shard_id` exists in the shard layout of `epoch_id`.
    pub fn check_if_descendant_of_tracked_shard(
        &self,
        shard_id: ShardId,
        tracked_shards: &Vec<ShardUId>,
        epoch_id: &EpochId,
    ) -> Result<bool, EpochError> {
        if let Some(is_tracked) = self.descendant_of_tracked_shard_cache.lock().get(&shard_id) {
            return Ok(*is_tracked);
        }

        let is_tracked = check_if_descendant_of_tracked_shard_impl(
            shard_id,
            &tracked_shards,
            &epoch_id,
            &self.epoch_manager,
        )?;
        self.descendant_of_tracked_shard_cache.lock().insert(shard_id, is_tracked);
        Ok(is_tracked)
    }

    /// Returns shards tracked in the given epoch by a non-validator (e.g. archival) node.
    /// Uses `tracks_shard_at_epoch` to filter based on the node's tracked shards config.
    pub fn get_tracked_shards_for_non_validator_in_epoch(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ShardUId>, EpochError> {
        let shard_layout = self.epoch_manager.get_shard_layout(&epoch_id)?;
        let mut tracked_shards = vec![];
        for shard_uid in shard_layout.shard_uids() {
            if self.tracks_shard_at_epoch(shard_uid.shard_id(), &epoch_id)? {
                tracked_shards.push(shard_uid);
            }
        }
        Ok(tracked_shards)
    }
}

fn check_if_descendant_of_tracked_shard_impl(
    shard_id: ShardId,
    tracked_shards: &Vec<ShardUId>,
    epoch_id: &EpochId,
    epoch_manager: &Arc<dyn EpochManagerAdapter>,
) -> Result<bool, EpochError> {
    let mut protocol_version = epoch_manager.get_epoch_protocol_version(epoch_id)?;
    let mut shard_layout = epoch_manager.get_shard_layout_from_protocol_version(protocol_version);
    let mut shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
    if tracked_shards.contains(&shard_uid) {
        // We explicitly track `shard_id` (the shard is a descendant of itself).
        return Ok(true);
    }
    // `shard_uid` does not belong to `tracked_shards`, but it might be a descendant of one.
    // Iterate through all ancestors of `shard_uid` to see if any belong to `tracked_shards`.
    let tracked_shards: HashSet<ShardUId> = tracked_shards.into_iter().cloned().collect();
    let genesis_protocol_version = epoch_manager.genesis_protocol_version();
    while protocol_version > genesis_protocol_version {
        protocol_version -= 1;
        let previous_protocol_version_shard_layout =
            epoch_manager.get_shard_layout_from_protocol_version(protocol_version);
        if previous_protocol_version_shard_layout == shard_layout {
            // The `ShardLayout` hasn't changed, so we keep decrementing `protocol_version`.
            continue;
        }
        // The `ShardLayout` changed after this protocol version — get the parent shard of `shard_uid`.
        let Some(parent_shard_id) = shard_layout.try_get_parent_shard_id(shard_uid.shard_id())?
        else {
            debug_assert!(
                false,
                "Parent shard is missing for shard {} in shard layout {:?}, protocol version {}",
                shard_uid, shard_layout, protocol_version
            );
            return Ok(false);
        };
        // Update `shard_uid` and `shard_layout` to their parent `ShardUId` and `ShardLayout`.
        shard_uid = ShardUId::from_shard_id_and_layout(
            parent_shard_id,
            &previous_protocol_version_shard_layout,
        );
        shard_layout = previous_protocol_version_shard_layout;
        // Check whether the ancestor shard belongs to `tracked_shards`.
        if tracked_shards.contains(&shard_uid) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::ShardTracker;
    use crate::shard_tracker::TrackedShardsConfig;
    use crate::test_utils::hash_range;
    use crate::{EpochManager, EpochManagerAdapter, EpochManagerHandle};
    use near_chain_configs::test_genesis::TestEpochConfigBuilder;
    use near_chain_configs::{GenesisConfig, MutableConfigValue};
    use near_crypto::{KeyType, PublicKey};
    use near_primitives::epoch_block_info::BlockInfo;
    use near_primitives::epoch_manager::EpochConfigStore;
    use near_primitives::hash::CryptoHash;
    use near_primitives::shard_layout::ShardLayout;
    use near_primitives::types::validator_stake::ValidatorStake;
    use near_primitives::types::{AccountInfo, BlockHeight, EpochId, ProtocolVersion, ShardId};
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::ShardUId;
    use near_store::test_utils::create_test_store;
    use std::collections::{BTreeMap, HashSet};
    use std::sync::Arc;

    const DEFAULT_TOTAL_SUPPLY: u128 = 1_000_000_000_000;
    const EPOCH_LENGTH: usize = 5;

    // Initializes an epoch manager, optionally including epoch configs for two reshardings.
    fn get_epoch_manager(
        genesis_protocol_version: ProtocolVersion,
        num_shards: u64,
        do_reshardings: bool,
    ) -> Arc<EpochManagerHandle> {
        let store = create_test_store();
        let mut genesis_config = GenesisConfig::default();
        genesis_config.protocol_version = genesis_protocol_version;
        genesis_config.validators = vec![AccountInfo {
            account_id: "test".parse().unwrap(),
            public_key: PublicKey::empty(KeyType::ED25519),
            amount: 100,
        }];
        let base_shard_layout = ShardLayout::multi_shard(num_shards, 0);
        let base_epoch_config = TestEpochConfigBuilder::new()
            .epoch_length(EPOCH_LENGTH as u64)
            .shard_layout(base_shard_layout.clone())
            .build();
        let mut epoch_configs =
            vec![(genesis_protocol_version, Arc::new(base_epoch_config.clone()))];

        if do_reshardings {
            let first_split_shard_layout =
                ShardLayout::derive_shard_layout(&base_shard_layout, "abc".parse().unwrap());
            let second_split_shard_layout = ShardLayout::derive_shard_layout(
                &first_split_shard_layout,
                "abcd".parse().unwrap(),
            );
            let mut first_split_epoch_config = base_epoch_config;
            first_split_epoch_config.shard_layout = first_split_shard_layout;
            let mut second_split_epoch_config = first_split_epoch_config.clone();
            second_split_epoch_config.shard_layout = second_split_shard_layout;
            epoch_configs.push((PROTOCOL_VERSION - 1, Arc::new(first_split_epoch_config)));
            epoch_configs.push((PROTOCOL_VERSION, Arc::new(second_split_epoch_config)));
        }

        let config_store = EpochConfigStore::test(BTreeMap::from_iter(epoch_configs));
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

    // Simulates block production over the given height range using the specified protocol version and block hashes.
    fn record_blocks(
        epoch_manager: &mut EpochManager,
        block_hashes: &Vec<CryptoHash>,
        protocol_version: ProtocolVersion,
        block_heights: std::ops::Range<usize>,
    ) {
        for height in block_heights {
            record_block(
                epoch_manager,
                if height > 0 { block_hashes[height - 1] } else { CryptoHash::default() },
                block_hashes[height],
                height as u64,
                vec![],
                protocol_version,
            );
        }
    }

    fn get_all_shards_care_about(
        tracker: &ShardTracker,
        shard_ids: &[ShardId],
        parent_hash: &CryptoHash,
    ) -> HashSet<ShardId> {
        shard_ids
            .into_iter()
            .filter(|&&shard_id| tracker.cares_about_shard(parent_hash, shard_id))
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
            .filter(|&&shard_id| tracker.will_care_about_shard(parent_hash, shard_id))
            .cloned()
            .collect()
    }

    /// Tests that arbitrary shard tracking works correctly across multiple reshardings. Verifies
    /// that tracked shards are properly propagated to their descendants after splits, and that
    /// untracked shards are not erroneously included after layout changes.
    #[test]
    fn test_track_arbitrary_shards() {
        let num_shards = 4;
        // Two reshardings will happen, at `PROTOCOL_VERSION - 1` and `PROTOCOL_VERSION`.
        let genesis_pv = PROTOCOL_VERSION - 3;
        let intermediate_pv = PROTOCOL_VERSION - 2;
        let epoch_manager = get_epoch_manager(genesis_pv, num_shards, true);

        // Simulate two reshardings by producing fake blocks.
        let block_hashes = hash_range(EPOCH_LENGTH * 4);
        for i in 0..4 {
            record_blocks(
                &mut epoch_manager.write(),
                &block_hashes,
                genesis_pv + i,
                EPOCH_LENGTH * i as usize..EPOCH_LENGTH * (i + 1) as usize,
            );
        }

        let genesis_epoch_id = EpochId::default();
        let base_shard_layout = epoch_manager.get_shard_layout(&genesis_epoch_id).unwrap();
        let epoch_id_after_first_resharding = epoch_manager
            .get_epoch_id_from_prev_block(&block_hashes[EPOCH_LENGTH * 2 + 1])
            .unwrap();
        let first_split_shard_layout =
            epoch_manager.get_shard_layout(&epoch_id_after_first_resharding).unwrap();
        // Ensure that the first resharding occurred.
        assert_ne!(base_shard_layout, first_split_shard_layout);
        let epoch_id_after_second_resharding = epoch_manager
            .get_epoch_id_from_prev_block(&block_hashes[EPOCH_LENGTH * 3 + 1])
            .unwrap();
        let second_split_shard_layout =
            epoch_manager.get_shard_layout(&epoch_id_after_second_resharding).unwrap();
        // Ensure that the second resharding occurred.
        assert_ne!(first_split_shard_layout, second_split_shard_layout);

        let intermediate_epoch_id =
            epoch_manager.get_epoch_id_from_prev_block(&block_hashes[EPOCH_LENGTH + 1]).unwrap();
        assert!(
            intermediate_epoch_id != genesis_epoch_id
                && intermediate_epoch_id != epoch_id_after_first_resharding
        );
        // Sanity check that we had an epoch with `intermediate_pv` and without shard split.
        assert_eq!(
            intermediate_pv,
            epoch_manager.get_epoch_protocol_version(&intermediate_epoch_id).unwrap()
        );

        let parent_shard_ids = first_split_shard_layout.get_split_parent_shard_ids();
        let parent_shard_uid = ShardUId::from_shard_id_and_layout(
            *parent_shard_ids.first().unwrap(),
            &base_shard_layout,
        );
        let shard_uids: Vec<ShardUId> = base_shard_layout.shard_uids().collect();
        let not_parent_shard_uid =
            if shard_uids[0] == parent_shard_uid { shard_uids[1] } else { shard_uids[0] };
        // Configure the tracker to follow the parent shard and one additional shard, but not all.
        let tracked_shards = vec![parent_shard_uid, not_parent_shard_uid];
        assert!(tracked_shards.len() < num_shards as usize);
        let tracker = ShardTracker::new(
            TrackedShardsConfig::Shards(tracked_shards),
            epoch_manager.clone(),
            MutableConfigValue::new(None, "validator_signer"),
        );

        let children_shards =
            first_split_shard_layout.get_children_shards_uids(parent_shard_uid.shard_id()).unwrap();
        let [left_child_shard_uid, right_child_shard_uid] = children_shards.try_into().unwrap();
        let [non_parent_shard_new_uid] = first_split_shard_layout
            .get_children_shards_uids(not_parent_shard_uid.shard_id())
            .unwrap()
            .try_into()
            .unwrap();
        // We expect the shard layout version to change in this test.
        assert_ne!(non_parent_shard_new_uid.version, not_parent_shard_uid.version);

        let grandchildren_shards = second_split_shard_layout
            .get_children_shards_uids(right_child_shard_uid.shard_id())
            .unwrap();
        let [left_grandchild_shard_uid, right_grandchild_shard_uid] =
            grandchildren_shards.try_into().unwrap();

        // Before resharding, we track exactly what is configured in the tracked shards config.
        assert_eq!(
            tracker
                .get_tracked_shards_for_non_validator_in_epoch(&genesis_epoch_id)
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::<ShardUId>::from([parent_shard_uid, not_parent_shard_uid]),
        );
        // `intermediate_epoch_id`'s protocol version does not exist in AllEpochConfig,
        // so `genesis_epoch_id`'s EpochConfig will be used.
        assert_eq!(
            tracker
                .get_tracked_shards_for_non_validator_in_epoch(&intermediate_epoch_id)
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::<ShardUId>::from([parent_shard_uid, not_parent_shard_uid]),
        );
        // After resharding, we track the child shards and continue tracking "non_parent_shard",
        // even though its shard layout version has changed.
        assert_eq!(
            tracker
                .get_tracked_shards_for_non_validator_in_epoch(&epoch_id_after_first_resharding)
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::<ShardUId>::from([
                non_parent_shard_new_uid,
                left_child_shard_uid,
                right_child_shard_uid
            ]),
        );
        // After the second resharding, we track grandchildren instead of the child that was split.
        assert_eq!(
            tracker
                .get_tracked_shards_for_non_validator_in_epoch(&epoch_id_after_second_resharding)
                .unwrap()
                .into_iter()
                .collect::<HashSet<_>>(),
            HashSet::<ShardUId>::from([
                non_parent_shard_new_uid,
                left_child_shard_uid,
                left_grandchild_shard_uid,
                right_grandchild_shard_uid,
            ]),
        );

        let tracker = ShardTracker::new(
            TrackedShardsConfig::Shards(vec![parent_shard_uid, non_parent_shard_new_uid]),
            epoch_manager.clone(),
            MutableConfigValue::new(None, "validator_signer"),
        );
        // Thanks to unique shard identifiers, we won't track the ancestor of "non_parent_shard",
        // even if the ShardId is the same.
        assert_eq!(
            tracker.get_tracked_shards_for_non_validator_in_epoch(&genesis_epoch_id).unwrap(),
            vec![parent_shard_uid],
        );

        let tracker = ShardTracker::new(
            TrackedShardsConfig::Shards(vec![left_child_shard_uid]),
            epoch_manager,
            MutableConfigValue::new(None, "validator_signer"),
        );
        // We won't track the parent or sibling shards if we are only configured to track the child shard.
        assert!(
            tracker
                .get_tracked_shards_for_non_validator_in_epoch(&genesis_epoch_id)
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            tracker
                .get_tracked_shards_for_non_validator_in_epoch(&epoch_id_after_first_resharding)
                .unwrap(),
            vec![left_child_shard_uid],
        );
        // We currently return false if a ShardID is passed that does not belong to the given epoch.
        // TODO(#13445): Turn this into an error or panic once we can guarantee that a valid ShardID is always provided.
        assert!(
            !tracker
                .tracks_shard_at_epoch(ShardId::new(42), &epoch_id_after_first_resharding)
                .unwrap()
        );
    }

    #[test]
    fn test_track_accounts() {
        let num_shards = 4;
        let shard_ids: Vec<ShardId> = (0..num_shards).map(ShardId::new).collect();
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION, num_shards, false);
        let shard_layout = epoch_manager.get_shard_layout(&EpochId::default()).unwrap();
        let tracked_accounts = vec!["test1".parse().unwrap(), "test2".parse().unwrap()];
        let tracker = ShardTracker::new(
            TrackedShardsConfig::Accounts(tracked_accounts),
            epoch_manager,
            MutableConfigValue::new(None, "validator_signer"),
        );
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
        let num_shards = 4;
        let shard_ids: Vec<ShardId> = (0..num_shards).map(ShardId::new).collect();
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION, num_shards, false);
        let tracker = ShardTracker::new(
            TrackedShardsConfig::AllShards,
            epoch_manager,
            MutableConfigValue::new(None, "validator_signer"),
        );
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

    // here
    #[test]
    fn test_track_schedule() {
        // Creates a ShardTracker that changes every epoch tracked shards.
        let num_shards = 4;
        let shard_ids: Vec<ShardId> = (0..num_shards).map(ShardId::new).collect();
        let epoch_manager = get_epoch_manager(PROTOCOL_VERSION, num_shards, false);

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
            MutableConfigValue::new(None, "validator_signer"),
        );

        let h = hash_range(8);
        record_blocks(&mut epoch_manager.write(), &h, PROTOCOL_VERSION, 0..8);

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
