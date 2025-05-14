#![cfg_attr(enable_const_type_id, feature(const_type_id))]

pub use crate::adapter::EpochManagerAdapter;
use crate::metrics::{PROTOCOL_VERSION_NEXT, PROTOCOL_VERSION_VOTES};
pub use crate::reward_calculator::NUM_SECONDS_IN_A_YEAR;
pub use crate::reward_calculator::RewardCalculator;
use epoch_info_aggregator::EpochInfoAggregator;
use itertools::Itertools;
use near_cache::SyncLruCache;
use near_chain_configs::{Genesis, GenesisConfig};
use near_primitives::block::{BlockHeader, Tip};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::{EpochInfo, RngSeed};
use near_primitives::epoch_manager::{
    AGGREGATOR_KEY, AllEpochConfig, EpochConfig, EpochConfigStore, EpochSummary,
};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
pub use near_primitives::shard_layout::ShardInfo;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::stateless_validation::validator_assignment::ChunkValidatorAssignments;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockChunkValidatorStats, BlockHeight, ChunkStats, EpochId,
    EpochInfoProvider, ShardId, ValidatorId, ValidatorInfoIdentifier, ValidatorKickoutReason,
    ValidatorStats,
};
use near_primitives::version::{ProtocolFeature, ProtocolVersion};
use near_primitives::views::{
    CurrentEpochValidatorInfo, EpochValidatorInfo, NextEpochValidatorInfo, ValidatorKickoutView,
};
use near_store::adapter::StoreAdapter;
use near_store::{DBCol, HEADER_HEAD_KEY, Store, StoreUpdate};
use num_rational::BigRational;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use reward_calculator::ValidatorOnlineThresholds;
use shard_assignment::build_assignment_restrictions_v77_to_v78;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, warn};
pub use validator_selection::proposals_to_epoch_info;
use validator_stats::get_sortable_validator_online_ratio;

mod adapter;
pub mod epoch_info_aggregator;
mod genesis;
mod metrics;
mod reward_calculator;
pub mod shard_assignment;
pub mod shard_tracker;
pub mod test_utils;
#[cfg(test)]
mod tests;
pub mod validate;
mod validator_selection;
mod validator_stats;

const EPOCH_CACHE_SIZE: usize = 50;
const BLOCK_CACHE_SIZE: usize = 1000;
const AGGREGATOR_SAVE_PERIOD: u64 = 1000;

/// In the current architecture, various components have access to the same
/// shared mutable instance of [`EpochManager`]. This handle manages locking
/// required for such access.
///
/// It's up to the caller to ensure that there are no logical races when using
/// `.write` access.
#[derive(Clone)]
pub struct EpochManagerHandle {
    inner: Arc<RwLock<EpochManager>>,
}

impl EpochManagerHandle {
    pub fn write(&self) -> RwLockWriteGuard<EpochManager> {
        self.inner.write()
    }

    pub fn read(&self) -> RwLockReadGuard<EpochManager> {
        self.inner.read()
    }
}

impl EpochInfoProvider for EpochManagerHandle {
    fn validator_stake(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<Balance>, EpochError> {
        let epoch_manager = self.read();
        let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
        Ok(epoch_info.get_validator_id(account_id).map(|id| epoch_info.validator_stake(*id)))
    }

    fn validator_total_stake(&self, epoch_id: &EpochId) -> Result<Balance, EpochError> {
        let epoch_manager = self.read();
        let epoch_info = epoch_manager.get_epoch_info(epoch_id)?;
        Ok(epoch_info.validators_iter().map(|info| info.stake()).sum())
    }

    fn minimum_stake(&self, prev_block_hash: &CryptoHash) -> Result<Balance, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.minimum_stake(prev_block_hash)
    }

    fn chain_id(&self) -> String {
        let epoch_manager = self.read();
        epoch_manager.config.chain_id().into()
    }

    fn shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_shard_layout(epoch_id)
    }
}

/// Tracks epoch information across different forks, such as validators.
/// Note: that even after garbage collection, the data about genesis epoch should be in the store.
pub struct EpochManager {
    store: Store,
    /// Current epoch config.
    config: AllEpochConfig,
    reward_calculator: RewardCalculator,

    /// Cache of epoch information.
    epochs_info: SyncLruCache<EpochId, Arc<EpochInfo>>,
    /// Cache of block information.
    blocks_info: SyncLruCache<CryptoHash, Arc<BlockInfo>>,
    /// Cache of epoch id to epoch start height
    epoch_id_to_start: SyncLruCache<EpochId, BlockHeight>,
    /// Epoch validators ordered by `block_producer_settlement`.
    epoch_validators_ordered: SyncLruCache<EpochId, Arc<[ValidatorStake]>>,
    /// Unique validators ordered by `block_producer_settlement`.
    epoch_validators_ordered_unique: SyncLruCache<EpochId, Arc<[ValidatorStake]>>,

    /// Unique chunk producers.
    epoch_chunk_producers_unique: SyncLruCache<EpochId, Arc<[ValidatorStake]>>,
    /// Aggregator that keeps statistics about the current epoch.  It’s data are
    /// synced up to the last final block.  The information are updated by
    /// [`Self::update_epoch_info_aggregator_upto_final`] method.  To get
    /// statistics up to a last block use
    /// [`Self::get_epoch_info_aggregator_upto_last`] method.
    epoch_info_aggregator: EpochInfoAggregator,
    /// Largest final height. Monotonically increasing.
    largest_final_height: BlockHeight,
    /// Cache for chunk_validators
    chunk_validators_cache:
        SyncLruCache<(EpochId, ShardId, BlockHeight), Arc<ChunkValidatorAssignments>>,

    /// Counts loop iterations inside of aggregate_epoch_info_upto method.
    /// Used for tests as a bit of white-box testing.
    #[cfg(test)]
    epoch_info_aggregator_loop_counter: std::sync::atomic::AtomicUsize,
}

impl EpochManager {
    /// Creates a new instance of `EpochManager` from the given `store`, `genesis_config`, and `home_dir`.
    /// For production environments such as mainnet ant testnet, the epoch config files will be ignored.
    /// In the test environment, the epoch config files will be loaded from the `home_dir` if it is not `None`.
    pub fn new_arc_handle(
        store: Store,
        genesis_config: &GenesisConfig,
        home_dir: Option<&Path>,
    ) -> Arc<EpochManagerHandle> {
        let chain_id = genesis_config.chain_id.as_str();
        if chain_id == near_primitives::chains::MAINNET
            || chain_id == near_primitives::chains::TESTNET
        {
            // Do not load epoch config files for mainnet and testnet.
            let epoch_config_store = EpochConfigStore::for_chain_id(chain_id, None).unwrap();
            return Self::new_arc_handle_from_epoch_config_store(
                store,
                genesis_config,
                epoch_config_store,
            );
        }

        let config_dir = home_dir.map(|home_dir| home_dir.join("epoch_configs"));
        let epoch_config_store = if config_dir.as_ref().map_or(false, |dir| dir.exists()) {
            EpochConfigStore::for_chain_id(chain_id, config_dir).unwrap()
        } else if chain_id.starts_with("test-chain-") {
            let epoch_config = EpochConfig::from(genesis_config);
            EpochConfigStore::test(BTreeMap::from_iter(vec![(
                genesis_config.protocol_version,
                Arc::new(epoch_config),
            )]))
        } else {
            let epoch_config = Genesis::test_epoch_config(
                genesis_config.num_block_producer_seats,
                genesis_config.shard_layout.clone(),
                genesis_config.epoch_length,
            );
            EpochConfigStore::test(BTreeMap::from_iter(vec![(
                genesis_config.protocol_version,
                Arc::new(epoch_config),
            )]))
        };
        Self::new_arc_handle_from_epoch_config_store(store, genesis_config, epoch_config_store)
    }

    pub fn new_arc_handle_from_epoch_config_store(
        store: Store,
        genesis_config: &GenesisConfig,
        epoch_config_store: EpochConfigStore,
    ) -> Arc<EpochManagerHandle> {
        let epoch_length = genesis_config.epoch_length;
        let reward_calculator = RewardCalculator::new(genesis_config, epoch_length);
        let all_epoch_config = AllEpochConfig::from_epoch_config_store(
            genesis_config.chain_id.as_str(),
            epoch_length,
            epoch_config_store,
        );
        Arc::new(
            Self::new(store, all_epoch_config, reward_calculator, genesis_config.validators())
                .unwrap()
                .into_handle(),
        )
    }

    pub fn new(
        store: Store,
        config: AllEpochConfig,
        reward_calculator: RewardCalculator,
        validators: Vec<ValidatorStake>,
    ) -> Result<Self, EpochError> {
        let epoch_info_aggregator =
            store.get_ser(DBCol::EpochInfo, AGGREGATOR_KEY)?.unwrap_or_default();
        let mut epoch_manager = EpochManager {
            store,
            config,
            reward_calculator,
            epochs_info: SyncLruCache::new(EPOCH_CACHE_SIZE),
            blocks_info: SyncLruCache::new(BLOCK_CACHE_SIZE),
            epoch_id_to_start: SyncLruCache::new(EPOCH_CACHE_SIZE),
            epoch_validators_ordered: SyncLruCache::new(EPOCH_CACHE_SIZE),
            epoch_validators_ordered_unique: SyncLruCache::new(EPOCH_CACHE_SIZE),
            epoch_chunk_producers_unique: SyncLruCache::new(EPOCH_CACHE_SIZE),
            chunk_validators_cache: SyncLruCache::new(BLOCK_CACHE_SIZE),
            epoch_info_aggregator,
            #[cfg(test)]
            epoch_info_aggregator_loop_counter: Default::default(),
            largest_final_height: 0,
        };
        if !epoch_manager.has_epoch_info(&EpochId::default())? {
            epoch_manager.initialize_genesis_epoch_info(validators)?;
        }
        Ok(epoch_manager)
    }

    pub fn into_handle(self) -> EpochManagerHandle {
        let inner = Arc::new(RwLock::new(self));
        EpochManagerHandle { inner }
    }

    pub fn init_after_epoch_sync(
        &mut self,
        store_update: &mut StoreUpdate,
        prev_epoch_first_block_info: BlockInfo,
        prev_epoch_prev_last_block_info: BlockInfo,
        prev_epoch_last_block_info: BlockInfo,
        prev_epoch_id: &EpochId,
        prev_epoch_info: EpochInfo,
        epoch_id: &EpochId,
        epoch_info: EpochInfo,
        next_epoch_id: &EpochId,
        next_epoch_info: EpochInfo,
    ) -> Result<(), EpochError> {
        // TODO(#11931): We need to initialize the aggregator to the previous epoch, because
        // we move the aggregator forward in the previous epoch we do not have previous epoch's
        // blocks to compute the aggregator data. See issue for details. Consider a cleaner way.
        self.epoch_info_aggregator =
            EpochInfoAggregator::new(*prev_epoch_id, *prev_epoch_prev_last_block_info.prev_hash());
        store_update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, &self.epoch_info_aggregator)?;

        self.save_block_info(store_update, Arc::new(prev_epoch_first_block_info))?;
        self.save_block_info(store_update, Arc::new(prev_epoch_prev_last_block_info))?;
        self.save_block_info(store_update, Arc::new(prev_epoch_last_block_info))?;
        self.save_epoch_info(store_update, prev_epoch_id, Arc::new(prev_epoch_info))?;
        self.save_epoch_info(store_update, epoch_id, Arc::new(epoch_info))?;
        self.save_epoch_info(store_update, next_epoch_id, Arc::new(next_epoch_info))?;
        Ok(())
    }

    /// When computing validators to kickout, we exempt some validators first so that
    /// the total stake of exempted validators exceed a threshold. This is to make sure
    /// we don't kick out too many validators in case of network instability.
    /// We also make sure that these exempted validators were not kicked out in the last epoch,
    /// so it is guaranteed that they will stay as validators after this epoch.
    ///
    /// `accounts_sorted_by_online_ratio`: Validator accounts sorted by online ratio in ascending order.
    fn compute_exempted_kickout(
        epoch_info: &EpochInfo,
        accounts_sorted_by_online_ratio: &Vec<AccountId>,
        total_stake: Balance,
        exempt_perc: u8,
        prev_validator_kickout: &HashMap<AccountId, ValidatorKickoutReason>,
    ) -> HashSet<AccountId> {
        // We want to make sure the total stake of validators that will be kicked out in this epoch doesn't exceed
        // config.validator_max_kickout_stake_ratio of total stake.
        // To achieve that, we sort all validators by their average uptime (average of block and chunk
        // uptime) and add validators to `exempted_validators` one by one, from high uptime to low uptime,
        // until the total excepted stake exceeds the ratio of total stake that we need to keep.
        // Later when we perform the check to kick out validators, we don't kick out validators in
        // exempted_validators.
        let mut exempted_validators = HashSet::new();
        let min_keep_stake = total_stake * (exempt_perc as u128) / 100;
        let mut exempted_stake: Balance = 0;
        for account_id in accounts_sorted_by_online_ratio.into_iter().rev() {
            if exempted_stake >= min_keep_stake {
                break;
            }
            if !prev_validator_kickout.contains_key(account_id) {
                exempted_stake += epoch_info
                    .get_validator_by_account(account_id)
                    .map(|v| v.stake())
                    .unwrap_or_default();
                exempted_validators.insert(account_id.clone());
            }
        }
        exempted_validators
    }

    /// Computes the set of validators to reward with stats and validators to kick out with reason.
    ///
    /// # Parameters
    /// epoch_info
    /// block_validator_tracker
    /// chunk_validator_tracker
    ///
    /// slashed: set of slashed validators
    /// prev_validator_kickout: previously kicked out
    ///
    /// # Returns
    /// (set of validators to reward with stats, set of validators to kickout)
    ///
    /// - Slashed validators are ignored (they are handled separately)
    /// - The total stake of validators that will be kicked out will not exceed
    ///   config.validator_max_kickout_stake_perc of total stake of all validators. This is
    ///   to ensure we don't kick out too many validators in case of network instability.
    /// - A validator is kicked out if he produced too few blocks or chunks
    /// - If all validators are either previously kicked out or to be kicked out, we choose one not to
    /// kick out
    fn compute_validators_to_reward_and_kickout(
        config: &EpochConfig,
        epoch_info: &EpochInfo,
        block_validator_tracker: &HashMap<ValidatorId, ValidatorStats>,
        chunk_stats_tracker: &HashMap<ShardId, HashMap<ValidatorId, ChunkStats>>,
        prev_validator_kickout: &HashMap<AccountId, ValidatorKickoutReason>,
    ) -> (HashMap<AccountId, BlockChunkValidatorStats>, HashMap<AccountId, ValidatorKickoutReason>)
    {
        let block_producer_kickout_threshold = config.block_producer_kickout_threshold;
        let chunk_producer_kickout_threshold = config.chunk_producer_kickout_threshold;
        let chunk_validator_only_kickout_threshold = config.chunk_validator_only_kickout_threshold;
        let mut validator_block_chunk_stats = HashMap::new();
        let mut total_stake: Balance = 0;
        let mut maximum_block_prod = 0;
        let mut max_validator = None;

        for (i, v) in epoch_info.validators_iter().enumerate() {
            let account_id = v.account_id();
            let block_stats = block_validator_tracker
                .get(&(i as u64))
                .unwrap_or(&ValidatorStats { expected: 0, produced: 0 })
                .clone();
            let mut chunk_stats = ChunkStats::default();
            for (_, tracker) in chunk_stats_tracker {
                if let Some(stat) = tracker.get(&(i as u64)) {
                    *chunk_stats.expected_mut() += stat.expected();
                    *chunk_stats.produced_mut() += stat.produced();
                    chunk_stats.endorsement_stats_mut().produced +=
                        stat.endorsement_stats().produced;
                    chunk_stats.endorsement_stats_mut().expected +=
                        stat.endorsement_stats().expected;
                }
            }
            total_stake += v.stake();
            let is_already_kicked_out = prev_validator_kickout.contains_key(account_id);
            if (max_validator.is_none() || block_stats.produced > maximum_block_prod)
                && !is_already_kicked_out
            {
                maximum_block_prod = block_stats.produced;
                max_validator = Some(account_id.clone());
            }
            validator_block_chunk_stats
                .insert(account_id.clone(), BlockChunkValidatorStats { block_stats, chunk_stats });
        }

        // Compares validator accounts by applying comparators in the following order:
        // First by online ratio, if equal then by stake, if equal then by account id.
        let validator_comparator =
            |left: &(BigRational, &AccountId), right: &(BigRational, &AccountId)| {
                let cmp_online_ratio = left.0.cmp(&right.0);
                cmp_online_ratio.then_with(|| {
                    // Note: The unwrap operations below must not fail because the accounts ids are
                    // taken from the validators in the same epoch info above.
                    let cmp_stake = epoch_info
                        .get_validator_stake(left.1)
                        .unwrap()
                        .cmp(&epoch_info.get_validator_stake(right.1).unwrap());
                    cmp_stake.then_with(|| {
                        let cmp_account_id = left.1.cmp(&right.1);
                        cmp_account_id
                    })
                })
            };

        let mut sorted_validators = validator_block_chunk_stats
            .iter()
            .map(|(account, stats)| (get_sortable_validator_online_ratio(stats), account))
            .collect_vec();
        sorted_validators.sort_by(validator_comparator);
        let accounts_sorted_by_online_ratio =
            sorted_validators.into_iter().map(|(_, account)| account.clone()).collect_vec();

        let exempt_perc =
            100_u8.checked_sub(config.validator_max_kickout_stake_perc).unwrap_or_default();
        let exempted_validators = Self::compute_exempted_kickout(
            epoch_info,
            &accounts_sorted_by_online_ratio,
            total_stake,
            exempt_perc,
            prev_validator_kickout,
        );
        let mut all_kicked_out = true;
        let mut validator_kickout = HashMap::new();
        for (account_id, stats) in &validator_block_chunk_stats {
            if exempted_validators.contains(account_id) {
                all_kicked_out = false;
                continue;
            }
            if stats.block_stats.less_than(block_producer_kickout_threshold) {
                validator_kickout.insert(
                    account_id.clone(),
                    ValidatorKickoutReason::NotEnoughBlocks {
                        produced: stats.block_stats.produced,
                        expected: stats.block_stats.expected,
                    },
                );
            }
            if stats.chunk_stats.production_stats().less_than(chunk_producer_kickout_threshold) {
                validator_kickout.entry(account_id.clone()).or_insert_with(|| {
                    ValidatorKickoutReason::NotEnoughChunks {
                        produced: stats.chunk_stats.produced(),
                        expected: stats.chunk_stats.expected(),
                    }
                });
            }
            let chunk_validator_only =
                stats.block_stats.expected == 0 && stats.chunk_stats.expected() == 0;
            if chunk_validator_only
                && stats
                    .chunk_stats
                    .endorsement_stats()
                    .less_than(chunk_validator_only_kickout_threshold)
            {
                validator_kickout.entry(account_id.clone()).or_insert_with(|| {
                    ValidatorKickoutReason::NotEnoughChunkEndorsements {
                        produced: stats.chunk_stats.endorsement_stats().produced,
                        expected: stats.chunk_stats.endorsement_stats().expected,
                    }
                });
            }
            let is_already_kicked_out = prev_validator_kickout.contains_key(account_id);
            if !validator_kickout.contains_key(account_id) {
                if !is_already_kicked_out {
                    all_kicked_out = false;
                }
            }
        }
        if all_kicked_out {
            tracing::info!(target:"epoch_manager", "We are about to kick out all validators in the next two epochs, so we are going to save one {:?}", max_validator);
            if let Some(validator) = max_validator {
                validator_kickout.remove(&validator);
            }
        }
        (validator_block_chunk_stats, validator_kickout)
    }

    fn collect_blocks_info(
        &self,
        last_block_info: &BlockInfo,
        last_block_hash: &CryptoHash,
    ) -> Result<EpochSummary, EpochError> {
        let epoch_info = self.get_epoch_info(last_block_info.epoch_id())?;
        let next_epoch_id = self.get_next_epoch_id(last_block_hash)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?;

        let EpochInfoAggregator {
            block_tracker: block_validator_tracker,
            shard_tracker: chunk_validator_tracker,
            all_proposals,
            version_tracker,
            ..
        } = self.get_epoch_info_aggregator_upto_last(last_block_hash)?;
        let mut proposals = vec![];

        let total_block_producer_stake: u128 = epoch_info
            .block_producers_settlement()
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .iter()
            .map(|&id| epoch_info.validator_stake(id))
            .sum();

        // Next protocol version calculation.
        // Implements https://github.com/near/NEPs/blob/master/specs/ChainSpec/Upgradability.md
        let mut versions = HashMap::new();
        for (validator_id, version) in &version_tracker {
            let (validator_id, version) = (*validator_id, *version);
            let stake = epoch_info.validator_stake(validator_id);
            *versions.entry(version).or_insert(0) += stake;
        }
        PROTOCOL_VERSION_VOTES.reset();
        for (version, stake) in &versions {
            let stake_percent = 100 * stake / total_block_producer_stake;
            let stake_percent = stake_percent as i64;
            PROTOCOL_VERSION_VOTES.with_label_values(&[&version.to_string()]).set(stake_percent);
            tracing::info!(target: "epoch_manager", ?version, ?stake_percent, "Protocol version voting.");
        }

        let protocol_version = next_epoch_info.protocol_version();

        let config = self.config.for_protocol_version(protocol_version);
        // Note: non-deterministic iteration is fine here, there can be only one
        // version with large enough stake.
        let next_next_epoch_version = if let Some((version, stake)) =
            versions.into_iter().max_by_key(|&(_version, stake)| stake)
        {
            let numer = *config.protocol_upgrade_stake_threshold.numer() as u128;
            let denom = *config.protocol_upgrade_stake_threshold.denom() as u128;
            let threshold = total_block_producer_stake * numer / denom;
            if stake > threshold { version } else { protocol_version }
        } else {
            protocol_version
        };

        PROTOCOL_VERSION_NEXT.set(next_next_epoch_version as i64);
        tracing::info!(target: "epoch_manager", ?next_next_epoch_version, "Protocol version voting.");

        let mut validator_kickout = HashMap::new();

        // Kickout validators voting for an old version.
        for (validator_id, version) in version_tracker {
            if version >= next_next_epoch_version {
                continue;
            }
            let validator = epoch_info.get_validator(validator_id);
            validator_kickout.insert(
                validator.take_account_id(),
                ValidatorKickoutReason::ProtocolVersionTooOld {
                    version,
                    network_version: next_next_epoch_version,
                },
            );
        }

        // Kickout unstaked validators.
        for (account_id, proposal) in all_proposals {
            if proposal.stake() == 0
                && *next_epoch_info.stake_change().get(&account_id).unwrap_or(&0) != 0
            {
                validator_kickout.insert(account_id.clone(), ValidatorKickoutReason::Unstaked);
            }
            proposals.push(proposal.clone());
        }

        let prev_epoch_last_block_hash =
            *self.get_block_info(last_block_info.epoch_first_block())?.prev_hash();
        let prev_validator_kickout = next_epoch_info.validator_kickout();

        let config = self.config.for_protocol_version(epoch_info.protocol_version());
        // Compute kick outs for validators who are offline.
        let (validator_block_chunk_stats, kickout) = Self::compute_validators_to_reward_and_kickout(
            &config,
            &epoch_info,
            &block_validator_tracker,
            &chunk_validator_tracker,
            prev_validator_kickout,
        );
        validator_kickout.extend(kickout);
        debug!(
            target: "epoch_manager",
            "All proposals: {:?}, Kickouts: {:?}, Block Tracker: {:?}, Shard Tracker: {:?}",
            proposals, validator_kickout, block_validator_tracker, chunk_validator_tracker
        );

        Ok(EpochSummary {
            prev_epoch_last_block_hash,
            all_proposals: proposals,
            validator_kickout,
            validator_block_chunk_stats,
            next_next_epoch_version,
        })
    }

    /// Finalizes epoch (T), where given last block hash is given, and returns next next epoch id (T + 2).
    fn finalize_epoch(
        &self,
        store_update: &mut StoreUpdate,
        block_info: &BlockInfo,
        last_block_hash: &CryptoHash,
        rng_seed: RngSeed,
    ) -> Result<(), EpochError> {
        let epoch_summary = self.collect_blocks_info(block_info, last_block_hash)?;
        let epoch_info = self.get_epoch_info(block_info.epoch_id())?;
        let epoch_protocol_version = epoch_info.protocol_version();
        let validator_stake =
            epoch_info.validators_iter().map(|r| r.account_and_stake()).collect::<HashMap<_, _>>();
        let next_epoch_id = self.get_next_epoch_id_from_info(block_info)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?;
        self.save_epoch_validator_info(store_update, block_info.epoch_id(), &epoch_summary)?;

        let EpochSummary {
            all_proposals,
            validator_kickout,
            mut validator_block_chunk_stats,
            next_next_epoch_version,
            ..
        } = epoch_summary;

        let (validator_reward, minted_amount) = {
            let last_epoch_last_block_hash =
                *self.get_block_info(block_info.epoch_first_block())?.prev_hash();
            let last_block_in_last_epoch = self.get_block_info(&last_epoch_last_block_hash)?;
            assert!(block_info.timestamp_nanosec() > last_block_in_last_epoch.timestamp_nanosec());
            let epoch_duration =
                block_info.timestamp_nanosec() - last_block_in_last_epoch.timestamp_nanosec();
            for (account_id, reason) in &validator_kickout {
                if matches!(
                    reason,
                    ValidatorKickoutReason::NotEnoughBlocks { .. }
                        | ValidatorKickoutReason::NotEnoughChunks { .. }
                        | ValidatorKickoutReason::NotEnoughChunkEndorsements { .. }
                ) {
                    validator_block_chunk_stats.remove(account_id);
                }
            }
            let epoch_config = self.get_epoch_config(epoch_protocol_version);
            // If ChunkEndorsementsInBlockHeader feature is enabled, we use the chunk validator kickout threshold
            // as the cutoff threshold for the endorsement ratio to remap the ratio to 0 or 1.
            let online_thresholds = ValidatorOnlineThresholds {
                online_min_threshold: epoch_config.online_min_threshold,
                online_max_threshold: epoch_config.online_max_threshold,
                endorsement_cutoff_threshold: Some(
                    epoch_config.chunk_validator_only_kickout_threshold,
                ),
            };
            self.reward_calculator.calculate_reward(
                validator_block_chunk_stats,
                &validator_stake,
                *block_info.total_supply(),
                epoch_protocol_version,
                epoch_duration,
                online_thresholds,
            )
        };
        let next_next_epoch_config = self.config.for_protocol_version(next_next_epoch_version);
        let next_epoch_version = next_epoch_info.protocol_version();
        let next_shard_layout = self.config.for_protocol_version(next_epoch_version).shard_layout;
        let has_same_shard_layout = next_shard_layout == next_next_epoch_config.shard_layout;

        let next_epoch_v6 = ProtocolFeature::SimpleNightshadeV6.enabled(next_epoch_version);
        let next_next_epoch_v6 =
            ProtocolFeature::SimpleNightshadeV6.enabled(next_next_epoch_version);
        let chunk_producer_assignment_restrictions =
            (!next_epoch_v6 && next_next_epoch_v6).then(|| {
                build_assignment_restrictions_v77_to_v78(
                    &next_epoch_info,
                    &next_shard_layout,
                    next_next_epoch_config.shard_layout.clone(),
                )
            });
        let next_next_epoch_info = match proposals_to_epoch_info(
            &next_next_epoch_config,
            rng_seed,
            &next_epoch_info,
            all_proposals,
            validator_kickout,
            validator_reward,
            minted_amount,
            next_next_epoch_version,
            has_same_shard_layout,
            chunk_producer_assignment_restrictions,
        ) {
            Ok(next_next_epoch_info) => next_next_epoch_info,
            Err(EpochError::ThresholdError { stake_sum, num_seats }) => {
                warn!(target: "epoch_manager", "Not enough stake for required number of seats (all validators tried to unstake?): amount = {} for {}", stake_sum, num_seats);
                let mut epoch_info = EpochInfo::clone(&next_epoch_info);
                *epoch_info.epoch_height_mut() += 1;
                epoch_info
            }
            Err(EpochError::NotEnoughValidators { num_validators, num_shards }) => {
                warn!(target: "epoch_manager", "Not enough validators for required number of shards (all validators tried to unstake?): num_validators={} num_shards={}", num_validators, num_shards);
                let mut epoch_info = EpochInfo::clone(&next_epoch_info);
                *epoch_info.epoch_height_mut() += 1;
                epoch_info
            }
            Err(err) => return Err(err),
        };
        let next_next_epoch_id = EpochId(*last_block_hash);
        debug!(target: "epoch_manager", "next next epoch height: {}, id: {:?}, protocol version: {} shard layout: {:?} config: {:?}",
               next_next_epoch_info.epoch_height(),
               &next_next_epoch_id,
               next_next_epoch_info.protocol_version(),
               self.config.for_protocol_version(next_next_epoch_info.protocol_version()).shard_layout,
            self.config.for_protocol_version(next_next_epoch_info.protocol_version()));
        // This epoch info is computed for the epoch after next (T+2),
        // where epoch_id of it is the hash of last block in this epoch (T).
        self.save_epoch_info(store_update, &next_next_epoch_id, Arc::new(next_next_epoch_info))?;
        Ok(())
    }

    pub fn record_block_info(
        &mut self,
        mut block_info: BlockInfo,
        rng_seed: RngSeed,
    ) -> Result<StoreUpdate, EpochError> {
        let current_hash = *block_info.hash();
        let mut store_update = self.store.store_update();
        // Check that we didn't record this block yet.
        if !self.has_block_info(&current_hash)? {
            if block_info.is_genesis() {
                // This is genesis block, we special case as new epoch.
                assert_eq!(block_info.proposals_iter().len(), 0);
                let pre_genesis_epoch_id = EpochId::default();
                let genesis_epoch_info = self.get_epoch_info(&pre_genesis_epoch_id)?;
                self.save_block_info(&mut store_update, Arc::new(block_info))?;
                self.save_epoch_info(
                    &mut store_update,
                    &EpochId(current_hash),
                    genesis_epoch_info,
                )?;
            } else {
                let prev_block_info = self.get_block_info(block_info.prev_hash())?;

                let mut is_epoch_start = false;
                if prev_block_info.is_genesis() {
                    // This is first real block, starts the new epoch.
                    *block_info.epoch_id_mut() = EpochId::default();
                    *block_info.epoch_first_block_mut() = current_hash;
                    is_epoch_start = true;
                } else if self.is_next_block_in_next_epoch(&prev_block_info)? {
                    // Current block is in the new epoch, finalize the one in prev_block.
                    *block_info.epoch_id_mut() =
                        self.get_next_epoch_id_from_info(&prev_block_info)?;
                    *block_info.epoch_first_block_mut() = current_hash;
                    is_epoch_start = true;
                } else {
                    // Same epoch as parent, copy epoch_id and epoch_start_height.
                    *block_info.epoch_id_mut() = *prev_block_info.epoch_id();
                    *block_info.epoch_first_block_mut() = *prev_block_info.epoch_first_block();
                }

                if is_epoch_start {
                    self.save_epoch_start(
                        &mut store_update,
                        block_info.epoch_id(),
                        block_info.height(),
                    )?;
                }

                let block_info = Arc::new(block_info);
                // Save current block info.
                self.save_block_info(&mut store_update, Arc::clone(&block_info))?;
                if block_info.last_finalized_height() > self.largest_final_height {
                    self.largest_final_height = block_info.last_finalized_height();

                    // Update epoch info aggregator.  We only update the if
                    // there is a change in the last final block.  This way we
                    // never need to rollback any information in
                    // self.epoch_info_aggregator.
                    self.update_epoch_info_aggregator_upto_final(
                        block_info.last_final_block_hash(),
                        &mut store_update,
                    )?;
                }

                // If this is the last block in the epoch, finalize this epoch.
                if self.is_next_block_in_next_epoch(&block_info)? {
                    self.finalize_epoch(&mut store_update, &block_info, &current_hash, rng_seed)?;
                }
            }
        }
        Ok(store_update)
    }

    /// Returns settlement of all block producers in current epoch
    pub fn get_all_block_producers_settlement(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Arc<[ValidatorStake]>, EpochError> {
        self.epoch_validators_ordered.get_or_try_put(*epoch_id, |epoch_id| {
            let epoch_info = self.get_epoch_info(epoch_id)?;
            let result = epoch_info
                .block_producers_settlement()
                .iter()
                .map(|&validator_id| epoch_info.get_validator(validator_id))
                .collect();
            Ok(result)
        })
    }

    /// Returns all unique block producers in current epoch sorted by account_id.
    pub fn get_all_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Arc<[ValidatorStake]>, EpochError> {
        self.epoch_validators_ordered_unique.get_or_try_put(*epoch_id, |epoch_id| {
            let settlement = self.get_all_block_producers_settlement(epoch_id)?;
            let mut validators: HashSet<AccountId> = HashSet::default();
            let result = settlement
                .iter()
                .filter(|validator_stake| {
                    let account_id = validator_stake.account_id();
                    validators.insert(account_id.clone())
                })
                .cloned()
                .collect();
            Ok(result)
        })
    }

    /// Returns settlement of all chunk producers in the current epoch.
    pub fn get_all_chunk_producers(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Arc<[ValidatorStake]>, EpochError> {
        self.epoch_chunk_producers_unique.get_or_try_put(*epoch_id, |epoch_id| {
            let mut producers: HashSet<u64> = HashSet::default();

            // Collect unique chunk producers.
            let epoch_info = self.get_epoch_info(epoch_id)?;
            for chunk_producers in epoch_info.chunk_producers_settlement() {
                producers.extend(chunk_producers);
            }

            Ok(producers.iter().map(|producer_id| epoch_info.get_validator(*producer_id)).collect())
        })
    }

    /// Returns the list of chunk_validators for the given shard_id and height and set of account ids.
    /// Generation of chunk_validators and their order is deterministic for given shard_id and height.
    /// We cache the generated chunk_validators.
    pub fn get_chunk_validator_assignments(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
        height: BlockHeight,
    ) -> Result<Arc<ChunkValidatorAssignments>, EpochError> {
        let cache_key = (*epoch_id, shard_id, height);
        if let Some(chunk_validators) = self.chunk_validators_cache.get(&cache_key) {
            return Ok(chunk_validators);
        }

        let epoch_info = self.get_epoch_info(epoch_id)?;
        let shard_layout = self.get_shard_layout(epoch_id)?;
        let chunk_validators_per_shard = epoch_info.sample_chunk_validators(height);
        for (shard_index, chunk_validators) in chunk_validators_per_shard.into_iter().enumerate() {
            let chunk_validators = chunk_validators
                .into_iter()
                .map(|(validator_id, assignment_weight)| {
                    (epoch_info.get_validator(validator_id).take_account_id(), assignment_weight)
                })
                .collect();
            let shard_id = shard_layout.get_shard_id(shard_index)?;
            let cache_key = (*epoch_id, shard_id, height);
            self.chunk_validators_cache
                .put(cache_key, Arc::new(ChunkValidatorAssignments::new(chunk_validators)));
        }

        self.chunk_validators_cache.get(&cache_key).ok_or_else(|| {
            EpochError::ChunkValidatorSelectionError(format!(
                "Invalid shard ID {} for height {}, epoch {:?} for chunk validation",
                shard_id, height, epoch_id,
            ))
        })
    }

    pub fn get_all_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
        current_epoch_id: EpochId,
        next_epoch_id: EpochId,
    ) -> Result<Vec<ApprovalStake>, EpochError> {
        let mut settlement = self.get_all_block_producers_settlement(&current_epoch_id)?.to_vec();

        let settlement_epoch_boundary = settlement.len();

        let block_info = self.get_block_info(parent_hash)?;
        if self.next_block_need_approvals_from_next_epoch(&block_info)? {
            settlement
                .extend(self.get_all_block_producers_settlement(&next_epoch_id)?.iter().cloned());
        }

        let mut result = vec![];
        let mut validators: HashMap<AccountId, usize> = HashMap::default();
        for (ord, validator_stake) in settlement.into_iter().enumerate() {
            let account_id = validator_stake.account_id();
            match validators.get(account_id) {
                None => {
                    validators.insert(account_id.clone(), result.len());
                    result
                        .push(validator_stake.get_approval_stake(ord >= settlement_epoch_boundary));
                }
                Some(old_ord) => {
                    if ord >= settlement_epoch_boundary {
                        result[*old_ord].stake_next_epoch = validator_stake.stake();
                    };
                }
            };
        }
        Ok(result)
    }

    /// Returns validator for given account id for given epoch.
    /// We don't require caller to know about EpochIds. Doesn't account for slashing.
    pub fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        epoch_info
            .get_validator_by_account(account_id)
            .ok_or_else(|| EpochError::NotAValidator(account_id.clone(), *epoch_id))
    }

    pub fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        Ok(*self.get_block_info(block_hash)?.epoch_id())
    }

    pub fn get_next_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let block_info = self.get_block_info(block_hash)?;
        self.get_next_epoch_id_from_info(&block_info)
    }

    pub fn get_epoch_info_from_hash(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Arc<EpochInfo>, EpochError> {
        let epoch_id = self.get_epoch_id(block_hash)?;
        self.get_epoch_info(&epoch_id)
    }

    /// Returns true if next block after given block hash is in the new epoch.
    pub fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let block_info = self.get_block_info(parent_hash)?;
        self.is_next_block_in_next_epoch(&block_info)
    }

    pub fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        if self.is_next_block_epoch_start(parent_hash)? {
            // Because we ID epochs based on the last block of T - 2, this is ID for next next epoch.
            Ok(EpochId(*parent_hash))
        } else {
            self.get_next_epoch_id(parent_hash)
        }
    }

    pub fn get_epoch_start_height(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<BlockHeight, EpochError> {
        let epoch_first_block = *self.get_block_info(block_hash)?.epoch_first_block();
        Ok(self.get_block_info(&epoch_first_block)?.height())
    }

    /// Compute stake return info based on the last block hash of the epoch that is just finalized
    /// return the hashmap of account id to max_of_stakes, which is used in the calculation of account
    /// updates.
    ///
    /// # Returns
    /// If successful, a triple of (hashmap of account id to max of stakes in the past three epochs,
    /// validator rewards in the last epoch, double sign slashing for the past epoch).
    pub fn compute_stake_return_info(
        &self,
        last_block_hash: &CryptoHash,
    ) -> Result<(HashMap<AccountId, Balance>, HashMap<AccountId, Balance>), EpochError> {
        let next_next_epoch_id = EpochId(*last_block_hash);
        let validator_reward = self.get_epoch_info(&next_next_epoch_id)?.validator_reward().clone();

        let next_epoch_id = self.get_next_epoch_id(last_block_hash)?;
        let epoch_id = self.get_epoch_id(last_block_hash)?;
        debug!(target: "epoch_manager",
            "epoch id: {:?}, prev_epoch_id: {:?}, prev_prev_epoch_id: {:?}",
            next_next_epoch_id, next_epoch_id, epoch_id
        );

        // Since stake changes for epoch T are stored in epoch info for T+2, the one stored by epoch_id
        // is the prev_prev_stake_change.
        let prev_prev_stake_change = self.get_epoch_info(&epoch_id)?.stake_change().clone();
        let prev_stake_change = self.get_epoch_info(&next_epoch_id)?.stake_change().clone();
        let stake_change = self.get_epoch_info(&next_next_epoch_id)?.stake_change().clone();
        debug!(target: "epoch_manager",
            "prev_prev_stake_change: {:?}, prev_stake_change: {:?}, stake_change: {:?}",
            prev_prev_stake_change, prev_stake_change, stake_change,
        );
        let all_stake_changes =
            prev_prev_stake_change.iter().chain(&prev_stake_change).chain(&stake_change);
        let all_keys: HashSet<&AccountId> = all_stake_changes.map(|(key, _)| key).collect();

        let mut stake_info = HashMap::new();
        for account_id in all_keys {
            let new_stake = *stake_change.get(account_id).unwrap_or(&0);
            let prev_stake = *prev_stake_change.get(account_id).unwrap_or(&0);
            let prev_prev_stake = *prev_prev_stake_change.get(account_id).unwrap_or(&0);
            let max_of_stakes =
                vec![prev_prev_stake, prev_stake, new_stake].into_iter().max().unwrap();
            stake_info.insert(account_id.clone(), max_of_stakes);
        }
        debug!(target: "epoch_manager", "stake_info: {:?}, validator_reward: {:?}", stake_info, validator_reward);
        Ok((stake_info, validator_reward))
    }

    /// Get validators for current epoch and next epoch.
    /// WARNING: this function calls EpochManager::get_epoch_info_aggregator_upto_last
    /// underneath which can be very expensive.
    pub fn get_validator_info(
        &self,
        epoch_identifier: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError> {
        let epoch_id = match epoch_identifier {
            ValidatorInfoIdentifier::EpochId(ref id) => *id,
            ValidatorInfoIdentifier::BlockHash(ref b) => self.get_epoch_id(b)?,
        };
        let cur_epoch_info = self.get_epoch_info(&epoch_id)?;
        let epoch_height = cur_epoch_info.epoch_height();
        let epoch_start_height = self.get_epoch_start_from_epoch_id(&epoch_id)?;

        // This ugly code arises because of the incompatible types between `block_tracker` in `EpochInfoAggregator`
        // and `validator_block_chunk_stats` in `EpochSummary`. Rust currently has no support for Either type
        // in std.
        let (current_validators, next_epoch_id, all_proposals) = match &epoch_identifier {
            ValidatorInfoIdentifier::EpochId(id) => {
                let cur_shard_layout = self.get_shard_layout(&epoch_id)?;
                let mut validator_to_shard = (0..cur_epoch_info.validators_len())
                    .map(|_| HashSet::default())
                    .collect::<Vec<HashSet<ShardId>>>();
                for (shard_index, validators) in
                    cur_epoch_info.chunk_producers_settlement().into_iter().enumerate()
                {
                    let shard_id = cur_shard_layout.get_shard_id(shard_index)?;
                    for validator_id in validators {
                        validator_to_shard[*validator_id as usize].insert(shard_id);
                    }
                }
                let epoch_summary = self.get_epoch_validator_info(id)?;
                let cur_validators = cur_epoch_info
                    .validators_iter()
                    .enumerate()
                    .map(|(validator_id, info)| {
                        let validator_stats = epoch_summary
                            .validator_block_chunk_stats
                            .get(info.account_id())
                            .unwrap_or(&BlockChunkValidatorStats {
                                block_stats: ValidatorStats { produced: 0, expected: 0 },
                                chunk_stats: ChunkStats {
                                    production: ValidatorStats { produced: 0, expected: 0 },
                                    endorsement: ValidatorStats { produced: 0, expected: 0 },
                                },
                            });
                        let mut shards_produced = validator_to_shard[validator_id]
                            .iter()
                            .cloned()
                            .collect::<Vec<ShardId>>();
                        shards_produced.sort();
                        // TODO: Compute the set of shards validated.
                        let shards_endorsed = vec![];
                        let (account_id, public_key, stake) = info.destructure();
                        Ok(CurrentEpochValidatorInfo {
                            is_slashed: false, // currently there is no slashing
                            account_id,
                            public_key,
                            stake,
                            // TODO: Maybe fill in the per shard info about the chunk produced for requests coming from RPC.
                            num_produced_chunks_per_shard: vec![0; shards_produced.len()],
                            num_expected_chunks_per_shard: vec![0; shards_produced.len()],
                            num_produced_blocks: validator_stats.block_stats.produced,
                            num_expected_blocks: validator_stats.block_stats.expected,
                            num_produced_chunks: validator_stats.chunk_stats.produced(),
                            num_expected_chunks: validator_stats.chunk_stats.expected(),
                            num_produced_endorsements: validator_stats
                                .chunk_stats
                                .endorsement_stats()
                                .produced,
                            num_expected_endorsements: validator_stats
                                .chunk_stats
                                .endorsement_stats()
                                .expected,
                            // Same TODO as above for `num_produced_chunks_per_shard`
                            num_produced_endorsements_per_shard: vec![0; shards_endorsed.len()],
                            num_expected_endorsements_per_shard: vec![0; shards_endorsed.len()],
                            shards_produced,
                            shards_endorsed,
                        })
                    })
                    .collect::<Result<Vec<CurrentEpochValidatorInfo>, EpochError>>()?;
                (
                    cur_validators,
                    EpochId(epoch_summary.prev_epoch_last_block_hash),
                    epoch_summary.all_proposals.into_iter().map(Into::into).collect(),
                )
            }
            ValidatorInfoIdentifier::BlockHash(h) => {
                // If we are here, `h` is hash of the latest block of the
                // current epoch.
                let aggregator = self.get_epoch_info_aggregator_upto_last(h)?;
                let cur_validators = cur_epoch_info
                    .validators_iter()
                    .enumerate()
                    .map(|(validator_id, info)| {
                        let block_stats = aggregator
                            .block_tracker
                            .get(&(validator_id as u64))
                            .unwrap_or(&ValidatorStats { produced: 0, expected: 0 })
                            .clone();

                        let mut chunks_stats_by_shard: HashMap<ShardId, ChunkStats> =
                            HashMap::new();
                        let mut chunk_stats = ChunkStats::default();
                        for (shard, tracker) in &aggregator.shard_tracker {
                            if let Some(stats) = tracker.get(&(validator_id as u64)) {
                                let produced = stats.produced();
                                let expected = stats.expected();
                                let endorsement_stats = stats.endorsement_stats();

                                *chunk_stats.produced_mut() += produced;
                                *chunk_stats.expected_mut() += expected;
                                chunk_stats.endorsement_stats_mut().produced +=
                                    endorsement_stats.produced;
                                chunk_stats.endorsement_stats_mut().expected +=
                                    endorsement_stats.expected;

                                let shard_stats = chunks_stats_by_shard.entry(*shard).or_default();
                                *shard_stats.produced_mut() += produced;
                                *shard_stats.expected_mut() += expected;
                                shard_stats.endorsement_stats_mut().produced +=
                                    endorsement_stats.produced;
                                shard_stats.endorsement_stats_mut().expected +=
                                    endorsement_stats.expected;
                            }
                        }
                        // Collect the shards for which the validator was *expected* to produce at least one chunk.
                        let mut shards_produced = chunks_stats_by_shard
                            .iter()
                            .filter_map(|(shard, stats)| (stats.expected() > 0).then_some(*shard))
                            .collect_vec();
                        shards_produced.sort();
                        // Collect the shards for which the validator was *expected* to validate at least one chunk.
                        let mut shards_endorsed = chunks_stats_by_shard
                            .iter()
                            .filter_map(|(shard, stats)| {
                                (stats.endorsement_stats().expected > 0).then_some(*shard)
                            })
                            .collect_vec();
                        shards_endorsed.sort();
                        let (account_id, public_key, stake) = info.destructure();
                        Ok(CurrentEpochValidatorInfo {
                            is_slashed: false, // currently there is no slashing
                            account_id,
                            public_key,
                            stake,
                            num_produced_blocks: block_stats.produced,
                            num_expected_blocks: block_stats.expected,
                            num_produced_chunks: chunk_stats.produced(),
                            num_expected_chunks: chunk_stats.expected(),
                            num_produced_chunks_per_shard: shards_produced
                                .iter()
                                .map(|shard| {
                                    chunks_stats_by_shard
                                        .get(shard)
                                        .map_or(0, |stats| stats.produced())
                                })
                                .collect(),
                            num_expected_chunks_per_shard: shards_produced
                                .iter()
                                .map(|shard| {
                                    chunks_stats_by_shard
                                        .get(shard)
                                        .map_or(0, |stats| stats.expected())
                                })
                                .collect(),
                            num_produced_endorsements: chunk_stats.endorsement_stats().produced,
                            num_expected_endorsements: chunk_stats.endorsement_stats().expected,
                            num_produced_endorsements_per_shard: shards_endorsed
                                .iter()
                                .map(|shard| {
                                    chunks_stats_by_shard
                                        .get(shard)
                                        .map_or(0, |stats| stats.endorsement_stats().produced)
                                })
                                .collect(),
                            num_expected_endorsements_per_shard: shards_endorsed
                                .iter()
                                .map(|shard| {
                                    chunks_stats_by_shard
                                        .get(shard)
                                        .map_or(0, |stats| stats.endorsement_stats().expected)
                                })
                                .collect(),
                            shards_produced,
                            shards_endorsed,
                        })
                    })
                    .collect::<Result<Vec<CurrentEpochValidatorInfo>, EpochError>>()?;
                let all_proposals =
                    aggregator.all_proposals.iter().map(|(_, p)| p.clone().into()).collect();
                let next_epoch_id = self.get_next_epoch_id(h)?;
                (cur_validators, next_epoch_id, all_proposals)
            }
        };

        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?;
        let next_shard_layout = self.get_shard_layout(&next_epoch_id)?;
        let mut next_validator_to_shard = (0..next_epoch_info.validators_len())
            .map(|_| HashSet::default())
            .collect::<Vec<HashSet<ShardId>>>();
        for (shard_index, validators) in
            next_epoch_info.chunk_producers_settlement().iter().enumerate()
        {
            let shard_id = next_shard_layout.get_shard_id(shard_index)?;
            for validator_id in validators {
                next_validator_to_shard[*validator_id as usize].insert(shard_id);
            }
        }
        let next_validators = next_epoch_info
            .validators_iter()
            .enumerate()
            .map(|(validator_id, info)| {
                let mut shards = next_validator_to_shard[validator_id]
                    .clone()
                    .into_iter()
                    .collect::<Vec<ShardId>>();
                shards.sort();
                let (account_id, public_key, stake) = info.destructure();
                NextEpochValidatorInfo { account_id, public_key, stake, shards }
            })
            .collect();
        let prev_epoch_kickout = next_epoch_info
            .validator_kickout()
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(account_id, reason)| ValidatorKickoutView { account_id, reason })
            .collect();

        Ok(EpochValidatorInfo {
            current_validators,
            next_validators,
            current_fishermen: cur_epoch_info.fishermen_iter().map(Into::into).collect(),
            next_fishermen: next_epoch_info.fishermen_iter().map(Into::into).collect(),
            current_proposals: all_proposals,
            prev_epoch_kickout,
            epoch_start_height,
            epoch_height,
        })
    }

    pub fn add_validator_proposals(
        &mut self,
        block_info: BlockInfo,
        random_value: CryptoHash,
    ) -> Result<StoreUpdate, EpochError> {
        // Check that genesis block doesn't have any proposals.
        let prev_validator_proposals = block_info.proposals_iter().collect::<Vec<_>>();
        assert!(block_info.height() > 0 || prev_validator_proposals.is_empty());
        debug!(target: "epoch_manager",
            height = block_info.height(),
            proposals = ?prev_validator_proposals,
            "add_validator_proposals");
        // Deal with validator proposals and epoch finishing.
        let rng_seed = random_value.0;
        self.record_block_info(block_info, rng_seed)
    }

    /// Get minimum stake allowed at current block. Attempts to stake with a lower stake will be
    /// rejected.
    pub fn minimum_stake(&self, prev_block_hash: &CryptoHash) -> Result<Balance, EpochError> {
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(prev_block_hash)?;
        let (protocol_version, seat_price) = {
            let epoch_info = self.get_epoch_info(&next_epoch_id)?;
            (epoch_info.protocol_version(), epoch_info.seat_price())
        };
        let config = self.config.for_protocol_version(protocol_version);
        let stake_divisor = { config.minimum_stake_divisor as Balance };
        Ok(seat_price / stake_divisor)
    }
}

/// Private utilities for EpochManager.
impl EpochManager {
    /// Returns true, if given current block info, next block supposed to be in the next epoch.
    fn is_next_block_in_next_epoch(&self, block_info: &BlockInfo) -> Result<bool, EpochError> {
        if block_info.is_genesis() {
            return Ok(true);
        }
        let protocol_version = self.get_epoch_info_from_hash(block_info.hash())?.protocol_version();
        let epoch_length = self.config.for_protocol_version(protocol_version).epoch_length;
        let estimated_next_epoch_start =
            self.get_block_info(block_info.epoch_first_block())?.height() + epoch_length;

        if epoch_length <= 3 {
            // This is here to make epoch_manager tests pass. Needs to be removed, tracked in
            // https://github.com/nearprotocol/nearcore/issues/2522
            return Ok(block_info.height() + 1 >= estimated_next_epoch_start);
        }

        Ok(block_info.last_finalized_height() + 3 >= estimated_next_epoch_start)
    }

    /// Returns true, if given current block info, next block must include the approvals from the next
    /// epoch (in addition to the approvals from the current epoch)
    fn next_block_need_approvals_from_next_epoch(
        &self,
        block_info: &BlockInfo,
    ) -> Result<bool, EpochError> {
        if self.is_next_block_in_next_epoch(block_info)? {
            return Ok(false);
        }
        let epoch_length = {
            let protocol_version =
                self.get_epoch_info_from_hash(block_info.hash())?.protocol_version();
            let config = self.config.for_protocol_version(protocol_version);
            config.epoch_length
        };
        let estimated_next_epoch_start =
            self.get_block_info(block_info.epoch_first_block())?.height() + epoch_length;
        Ok(block_info.last_finalized_height() + 3 < estimated_next_epoch_start
            && block_info.height() + 3 >= estimated_next_epoch_start)
    }

    /// Returns epoch id for the next epoch (T+1), given an block info in current epoch (T).
    fn get_next_epoch_id_from_info(&self, block_info: &BlockInfo) -> Result<EpochId, EpochError> {
        let first_block_info = self.get_block_info(block_info.epoch_first_block())?;
        Ok(EpochId(*first_block_info.prev_hash()))
    }

    pub fn get_epoch_config(&self, protocol_version: ProtocolVersion) -> EpochConfig {
        self.config.for_protocol_version(protocol_version)
    }

    pub fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        let protocol_version = self.get_epoch_info(epoch_id)?.protocol_version();
        Ok(self.get_shard_layout_from_protocol_version(protocol_version))
    }

    pub fn get_shard_layout_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> ShardLayout {
        self.config.for_protocol_version(protocol_version).shard_layout
    }

    pub fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        self.epochs_info.get_or_try_put(*epoch_id, |epoch_id| {
            self.store.epoch_store().get_epoch_info(epoch_id).map(Arc::new)
        })
    }

    fn has_epoch_info(&self, epoch_id: &EpochId) -> Result<bool, EpochError> {
        match self.get_epoch_info(epoch_id) {
            Ok(_) => Ok(true),
            Err(EpochError::EpochOutOfBounds(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn save_epoch_info(
        &self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        epoch_info: Arc<EpochInfo>,
    ) -> Result<(), EpochError> {
        store_update.set_ser(DBCol::EpochInfo, epoch_id.as_ref(), &epoch_info)?;
        self.epochs_info.put(*epoch_id, epoch_info);
        Ok(())
    }

    pub fn get_epoch_validator_info(&self, epoch_id: &EpochId) -> Result<EpochSummary, EpochError> {
        // We don't use cache here since this query happens rarely and only for rpc.
        self.store
            .get_ser(DBCol::EpochValidatorInfo, epoch_id.as_ref())?
            .ok_or(EpochError::EpochOutOfBounds(*epoch_id))
    }

    // Note(#6572): beware, after calling `save_epoch_validator_info`,
    // `get_epoch_validator_info` will return stale results.
    fn save_epoch_validator_info(
        &self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        epoch_summary: &EpochSummary,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(DBCol::EpochValidatorInfo, epoch_id.as_ref(), epoch_summary)
            .map_err(EpochError::from)
    }

    fn has_block_info(&self, hash: &CryptoHash) -> Result<bool, EpochError> {
        match self.get_block_info(hash) {
            Ok(_) => Ok(true),
            Err(EpochError::MissingBlock(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        self.blocks_info.get_or_try_put(*hash, |hash| {
            self.store.epoch_store().get_block_info(hash).map(Arc::new)
        })
    }

    fn save_block_info(
        &self,
        store_update: &mut StoreUpdate,
        block_info: Arc<BlockInfo>,
    ) -> Result<(), EpochError> {
        let block_hash = block_info.hash();
        store_update.insert_ser(DBCol::BlockInfo, block_hash.as_ref(), &block_info)?;
        self.blocks_info.put(*block_hash, block_info);
        Ok(())
    }

    fn save_epoch_start(
        &self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        epoch_start: BlockHeight,
    ) -> Result<(), EpochError> {
        store_update.set_ser(DBCol::EpochStart, epoch_id.as_ref(), &epoch_start)?;
        self.epoch_id_to_start.put(*epoch_id, epoch_start);
        Ok(())
    }

    fn get_epoch_start_from_epoch_id(&self, epoch_id: &EpochId) -> Result<BlockHeight, EpochError> {
        self.epoch_id_to_start.get_or_try_put(*epoch_id, |epoch_id| {
            self.store.epoch_store().get_epoch_start(epoch_id)
        })
    }

    /// Updates epoch info aggregator to state as of `last_final_block_hash`
    /// block.
    ///
    /// The block hash passed as argument should be a final block so that the
    /// method can perform efficient incremental updates.  Calling this method
    /// on a block which has not been finalized yet is likely to result in
    /// performance issues since handling forks will force it to traverse the
    /// entire epoch from scratch.
    ///
    /// The result of the aggregation is stored in `self.epoch_info_aggregator`.
    ///
    /// Saves the aggregator to `store_update` if epoch id changes or every
    /// [`AGGREGATOR_SAVE_PERIOD`] heights.
    pub fn update_epoch_info_aggregator_upto_final(
        &mut self,
        last_final_block_hash: &CryptoHash,
        store_update: &mut StoreUpdate,
    ) -> Result<(), EpochError> {
        if let Some((aggregator, replace)) =
            self.aggregate_epoch_info_upto(last_final_block_hash)?
        {
            let save = if replace {
                self.epoch_info_aggregator = aggregator;
                true
            } else {
                self.epoch_info_aggregator.merge(aggregator);
                let block_info = self.get_block_info(last_final_block_hash)?;
                block_info.height() % AGGREGATOR_SAVE_PERIOD == 0
            };
            if save {
                store_update.set_ser(
                    DBCol::EpochInfo,
                    AGGREGATOR_KEY,
                    &self.epoch_info_aggregator,
                )?;
            }
        }
        Ok(())
    }

    /// Returns epoch info aggregate with state up to `last_block_hash`.
    ///
    /// The block hash passed as argument should be the latest block belonging
    /// to current epoch.  Calling this method on any other block is likely to
    /// result in performance issues since handling something which is not past
    /// the final block will force it to traverse the entire epoch from scratch.
    ///
    /// This method does not change `self.epoch_info_aggregator`.
    pub fn get_epoch_info_aggregator_upto_last(
        &self,
        last_block_hash: &CryptoHash,
    ) -> Result<EpochInfoAggregator, EpochError> {
        if let Some((mut aggregator, replace)) = self.aggregate_epoch_info_upto(last_block_hash)? {
            if !replace {
                aggregator.merge_prefix(&self.epoch_info_aggregator);
            }
            Ok(aggregator)
        } else {
            Ok(self.epoch_info_aggregator.clone())
        }
    }

    /// Aggregates epoch info between last final block and given block.
    ///
    /// More specifically, aggregates epoch information from block denoted by
    /// `self.epoch_info_aggregator.last_block_hash` (excluding that block) up
    /// to one denoted by `block_hash` (including that block).  If the two
    /// blocks belong to different epochs, stops aggregating once it reaches
    /// start of epoch `block_hash` belongs to.
    ///
    /// The block hash passed as argument should be a latest final block or
    /// a descendant of a latest final block. Calling this method on any other
    /// block is likely to result in performance issues since handling forks
    /// will force it to traverse the entire epoch from scratch.
    ///
    /// If `block_hash` equals `self.epoch_info_aggregator.last_block_hash`
    /// returns None.  Otherwise returns `Some((aggregator, full_info))` tuple.
    /// The first element of the pair is aggregator with collected information;
    /// the second specifies whether the returned aggregator includes full
    /// information about an epoch (such that it does not need to be merged with
    /// `self.epoch_info_aggregator`).  That happens if the method reaches epoch
    /// boundary.
    fn aggregate_epoch_info_upto(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<Option<(EpochInfoAggregator, bool)>, EpochError> {
        if block_hash == &self.epoch_info_aggregator.last_block_hash {
            return Ok(None);
        }

        if cfg!(debug) {
            let agg_hash = self.epoch_info_aggregator.last_block_hash;
            let agg_height = self.get_block_info(&agg_hash)?.height();
            let block_height = self.get_block_info(block_hash)?.height();
            assert!(
                agg_height < block_height,
                "#{agg_hash} {agg_height} >= #{block_hash} {block_height}",
            );
        }

        let epoch_id = *self.get_block_info(block_hash)?.epoch_id();
        let epoch_info = self.get_epoch_info(&epoch_id)?;
        let shard_layout = self.get_shard_layout(&epoch_id)?;

        let mut aggregator = EpochInfoAggregator::new(epoch_id, *block_hash);
        let mut cur_hash = *block_hash;
        Ok(Some(loop {
            #[cfg(test)]
            {
                self.epoch_info_aggregator_loop_counter
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }

            // To avoid cloning BlockInfo we need to first get reference to the
            // current block, but then drop it so that we can call
            // get_block_info for previous block.
            let block_info = self.get_block_info(&cur_hash)?;
            let different_epoch = &epoch_id != block_info.epoch_id();

            if different_epoch || block_info.is_genesis() {
                // We’ve reached the beginning of an epoch or a genesis block
                // without seeing self.epoch_info_aggregator.last_block_hash.
                // This implies self.epoch_info_aggregator.last_block_hash
                // belongs to different epoch or we’re on different fork (though
                // the latter should never happen).  In either case, the
                // aggregator contains full epoch information.
                break (aggregator, true);
            }

            let prev_hash = *block_info.prev_hash();
            let (prev_height, prev_epoch) = match self.get_block_info(&prev_hash) {
                Ok(info) => (info.height(), *info.epoch_id()),
                Err(EpochError::MissingBlock(_)) => {
                    // In the case of epoch sync, we may not have the BlockInfo for the last final block
                    // of the epoch. In this case, check for this special case.
                    // TODO(11931): think of a better way to do this.
                    let tip = self
                        .store
                        .get_ser::<Tip>(DBCol::BlockMisc, HEADER_HEAD_KEY)?
                        .ok_or_else(|| EpochError::IOErr("Tip not found in store".to_string()))?;
                    let block_header = self
                        .store
                        .get_ser::<BlockHeader>(DBCol::BlockHeader, tip.prev_block_hash.as_bytes())?
                        .ok_or_else(|| {
                            EpochError::IOErr(
                                "BlockHeader for prev block of tip not found in store".to_string(),
                            )
                        })?;
                    if block_header.prev_hash() == block_info.hash() {
                        (block_info.height() - 1, *block_info.epoch_id())
                    } else {
                        return Err(EpochError::MissingBlock(prev_hash));
                    }
                }
                Err(e) => return Err(e),
            };

            let block_info = self.get_block_info(&cur_hash)?;
            aggregator.update_tail(&block_info, &epoch_info, &shard_layout, prev_height);

            if prev_hash == self.epoch_info_aggregator.last_block_hash {
                // We’ve reached sync point of the old aggregator.  If old
                // aggregator was for a different epoch, we have full info in
                // our aggregator; otherwise we don’t.
                break (aggregator, epoch_id != prev_epoch);
            }

            cur_hash = prev_hash;
        }))
    }
}
