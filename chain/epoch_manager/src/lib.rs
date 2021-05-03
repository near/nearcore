use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use cached::{Cached, SizedCache};
use log::{debug, warn};
use primitive_types::U256;

use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::{EpochInfo, EpochSummary};
use near_primitives::epoch_manager::{EpochConfig, SlashState, AGGREGATOR_KEY};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockChunkValidatorStats, BlockHeight, EpochId, ShardId,
    ValidatorId, ValidatorKickoutReason, ValidatorStats,
};
use near_primitives::version::{ProtocolVersion, UPGRADABILITY_FIX_PROTOCOL_VERSION};
use near_primitives::views::{
    CurrentEpochValidatorInfo, EpochValidatorInfo, NextEpochValidatorInfo, ValidatorKickoutView,
};
use near_store::{ColBlockInfo, ColEpochInfo, ColEpochStart, Store, StoreUpdate};

use crate::proposals::proposals_to_epoch_info;
pub use crate::reward_calculator::RewardCalculator;
use crate::types::EpochInfoAggregator;
pub use crate::types::RngSeed;

pub use crate::reward_calculator::NUM_SECONDS_IN_A_YEAR;
use near_chain::types::{BlockHeaderInfo, ValidatorInfoIdentifier};
use near_store::db::DBCol::ColEpochValidatorInfo;

mod proposals;
mod reward_calculator;
pub mod test_utils;
mod types;

const EPOCH_CACHE_SIZE: usize = 50;
const BLOCK_CACHE_SIZE: usize = 1000;
const AGGREGATOR_SAVE_PERIOD: u64 = 1000;

/// Tracks epoch information across different forks, such as validators.
/// Note: that even after garbage collection, the data about genesis epoch should be in the store.
pub struct EpochManager {
    store: Arc<Store>,
    /// Current epoch config.
    /// TODO: must be dynamically changing over time, so there should be a way to change it.
    config: EpochConfig,
    reward_calculator: RewardCalculator,
    /// Genesis protocol version. Useful when there are protocol upgrades.
    genesis_protocol_version: ProtocolVersion,

    /// Cache of epoch information.
    epochs_info: SizedCache<EpochId, EpochInfo>,
    /// Cache of block information.
    blocks_info: SizedCache<CryptoHash, BlockInfo>,
    /// Cache of epoch id to epoch start height
    epoch_id_to_start: SizedCache<EpochId, BlockHeight>,
    /// Epoch validators ordered by `block_producer_settlement`.
    epoch_validators_ordered: SizedCache<EpochId, Vec<(ValidatorStake, bool)>>,
    /// Unique validators ordered by `block_producer_settlement`.
    epoch_validators_ordered_unique: SizedCache<EpochId, Vec<(ValidatorStake, bool)>>,
    /// Aggregator that crunches data when we process block info
    epoch_info_aggregator: Option<EpochInfoAggregator>,
    /// Largest final height. Monotonically increasing.
    largest_final_height: BlockHeight,
}

impl EpochManager {
    pub fn new(
        store: Arc<Store>,
        config: EpochConfig,
        genesis_protocol_version: ProtocolVersion,
        reward_calculator: RewardCalculator,
        validators: Vec<ValidatorStake>,
    ) -> Result<Self, EpochError> {
        let validator_reward = vec![(reward_calculator.protocol_treasury_account.clone(), 0u128)]
            .into_iter()
            .collect();
        let mut epoch_manager = EpochManager {
            store,
            config,
            reward_calculator,
            genesis_protocol_version,
            epochs_info: SizedCache::with_size(EPOCH_CACHE_SIZE),
            blocks_info: SizedCache::with_size(BLOCK_CACHE_SIZE),
            epoch_id_to_start: SizedCache::with_size(EPOCH_CACHE_SIZE),
            epoch_validators_ordered: SizedCache::with_size(EPOCH_CACHE_SIZE),
            epoch_validators_ordered_unique: SizedCache::with_size(EPOCH_CACHE_SIZE),
            epoch_info_aggregator: None,
            largest_final_height: 0,
        };
        let genesis_epoch_id = EpochId::default();
        if !epoch_manager.has_epoch_info(&genesis_epoch_id)? {
            // Missing genesis epoch, means that there is no validator initialize yet.
            let epoch_info = proposals_to_epoch_info(
                &epoch_manager.config,
                [0; 32],
                &EpochInfo::default(),
                validators,
                HashMap::default(),
                validator_reward,
                0,
                genesis_protocol_version,
            )?;
            // Dummy block info.
            // Artificial block we add to simplify implementation: dummy block is the
            // parent of genesis block that points to itself.
            // If we view it as block in epoch -1 and height -1, it naturally extends the
            // EpochId formula using T-2 for T=1, and height field is unused.
            let block_info = BlockInfo::default();
            let mut store_update = epoch_manager.store.store_update();
            epoch_manager.save_epoch_info(&mut store_update, &genesis_epoch_id, epoch_info)?;
            epoch_manager.save_block_info(&mut store_update, block_info)?;
            store_update.commit()?;
        }
        Ok(epoch_manager)
    }

    pub fn init_after_epoch_sync(
        &mut self,
        prev_epoch_first_block_info: BlockInfo,
        prev_epoch_prev_last_block_info: BlockInfo,
        prev_epoch_last_block_info: BlockInfo,
        prev_epoch_id: &EpochId,
        prev_epoch_info: EpochInfo,
        epoch_id: &EpochId,
        epoch_info: EpochInfo,
        next_epoch_id: &EpochId,
        next_epoch_info: EpochInfo,
    ) -> Result<StoreUpdate, EpochError> {
        let mut store_update = self.store.store_update();
        self.save_block_info(&mut store_update, prev_epoch_first_block_info)?;
        self.save_block_info(&mut store_update, prev_epoch_prev_last_block_info)?;
        self.save_block_info(&mut store_update, prev_epoch_last_block_info)?;
        self.save_epoch_info(&mut store_update, &prev_epoch_id, prev_epoch_info)?;
        self.save_epoch_info(&mut store_update, &epoch_id, epoch_info)?;
        self.save_epoch_info(&mut store_update, &next_epoch_id, next_epoch_info)?;
        // TODO #3488
        // put unreachable! here to avoid warnings
        unreachable!();
        // Ok(store_update)
    }

    /// # Parameters
    /// epoch_info
    /// block_validator_tracker
    /// chunk_validator_tracker
    ///
    /// slashed: set of slashed validators
    /// prev_validator_kickout: previously kicked out
    ///
    /// # Returns
    /// (set of validators to kickout, set of validators to reward with stats)
    ///
    /// - Slashed validators are ignored (they are handled separately)
    /// - A validator is kicked out if he produced too few blocks or chunks
    /// - If all validators are either previously kicked out or to be kicked out, we choose one not to
    /// kick out
    fn compute_kickout_info(
        &self,
        epoch_info: &EpochInfo,
        block_validator_tracker: &HashMap<ValidatorId, ValidatorStats>,
        chunk_validator_tracker: &HashMap<ShardId, HashMap<ValidatorId, ValidatorStats>>,
        slashed: &HashMap<AccountId, SlashState>,
        prev_validator_kickout: &HashMap<AccountId, ValidatorKickoutReason>,
    ) -> (HashMap<AccountId, ValidatorKickoutReason>, HashMap<AccountId, BlockChunkValidatorStats>)
    {
        let mut all_kicked_out = true;
        let mut maximum_block_prod = 0;
        let mut max_validator = None;
        let block_producer_kickout_threshold = self.config.block_producer_kickout_threshold;
        let chunk_producer_kickout_threshold = self.config.chunk_producer_kickout_threshold;
        let mut validator_block_chunk_stats = HashMap::new();
        let mut validator_kickout = HashMap::new();

        for (i, v) in epoch_info.validators_iter().enumerate() {
            let account_id = v.account_id().clone();
            if slashed.contains_key(&account_id) {
                continue;
            }
            let block_stats = block_validator_tracker
                .get(&(i as u64))
                .unwrap_or_else(|| &ValidatorStats { expected: 0, produced: 0 });
            // Note, validator_kickout_threshold is 0..100, so we use * 100 to keep this in integer space.
            if block_stats.produced * 100
                < u64::from(block_producer_kickout_threshold) * block_stats.expected
            {
                validator_kickout.insert(
                    account_id.clone(),
                    ValidatorKickoutReason::NotEnoughBlocks {
                        produced: block_stats.produced,
                        expected: block_stats.expected,
                    },
                );
            }
            let mut chunk_stats = ValidatorStats { produced: 0, expected: 0 };
            for (_, tracker) in chunk_validator_tracker.iter() {
                if let Some(stat) = tracker.get(&(i as u64)) {
                    chunk_stats.expected += stat.expected;
                    chunk_stats.produced += stat.produced;
                }
            }
            if chunk_stats.produced * 100
                < u64::from(chunk_producer_kickout_threshold) * chunk_stats.expected
            {
                validator_kickout.entry(account_id.clone()).or_insert_with(|| {
                    ValidatorKickoutReason::NotEnoughChunks {
                        produced: chunk_stats.produced,
                        expected: chunk_stats.expected,
                    }
                });
            }

            let is_already_kicked_out = prev_validator_kickout.contains_key(&account_id);
            if !validator_kickout.contains_key(&account_id) {
                validator_block_chunk_stats.insert(
                    account_id.clone(),
                    BlockChunkValidatorStats { block_stats: block_stats.clone(), chunk_stats },
                );
                if !is_already_kicked_out {
                    all_kicked_out = false;
                }
            }
            if (max_validator.is_none() || block_stats.produced > maximum_block_prod)
                && !is_already_kicked_out
            {
                maximum_block_prod = block_stats.produced;
                max_validator = Some(v);
            }
        }
        if all_kicked_out {
            if let Some(validator) = max_validator {
                validator_kickout.remove(validator.account_id());
            }
        }
        (validator_kickout, validator_block_chunk_stats)
    }

    fn collect_blocks_info(
        &mut self,
        last_block_info: &BlockInfo,
        last_block_hash: &CryptoHash,
    ) -> Result<EpochSummary, EpochError> {
        let epoch_info = self.get_epoch_info(&last_block_info.epoch_id())?.clone();
        let next_epoch_id = self.get_next_epoch_id(&last_block_hash)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?.clone();
        let EpochInfoAggregator {
            block_tracker: block_validator_tracker,
            shard_tracker: chunk_validator_tracker,
            all_proposals,
            version_tracker,
            ..
        } = self.get_and_update_epoch_info_aggregator(
            &last_block_info.epoch_id(),
            last_block_hash,
            false,
        )?;
        let mut proposals = vec![];
        let mut validator_kickout = HashMap::new();

        // Next protocol version calculation.
        // Implements https://github.com/nearprotocol/NEPs/pull/64/files#diff-45f773511fe4321b446c3c4226324873R76
        let mut versions = HashMap::new();
        for (validator_id, version) in version_tracker.iter() {
            let stake = epoch_info.validator_stake(*validator_id);
            *versions.entry(version).or_insert(0) += stake;
        }
        let total_block_producer_stake: u128 = epoch_info
            .block_producers_settlement()
            .iter()
            .collect::<HashSet<_>>()
            .iter()
            .map(|&id| epoch_info.validator_stake(*id))
            .sum();

        let protocol_version =
            if epoch_info.protocol_version() >= UPGRADABILITY_FIX_PROTOCOL_VERSION {
                next_epoch_info.protocol_version()
            } else {
                epoch_info.protocol_version()
            };

        let next_version = if let Some((&version, stake)) =
            versions.into_iter().max_by(|left, right| left.1.cmp(&right.1))
        {
            if stake
                > (total_block_producer_stake
                    * *self.config.protocol_upgrade_stake_threshold.numer() as u128)
                    / *self.config.protocol_upgrade_stake_threshold.denom() as u128
            {
                version
            } else {
                protocol_version
            }
        } else {
            protocol_version
        };

        // Gather slashed validators and add them to kick out first.
        let slashed_validators = last_block_info.slashed().clone();
        for (account_id, _) in slashed_validators.iter() {
            validator_kickout.insert(account_id.clone(), ValidatorKickoutReason::Slashed);
        }

        for (account_id, proposal) in all_proposals {
            if !slashed_validators.contains_key(&account_id) {
                if proposal.stake() == 0
                    && *next_epoch_info.stake_change().get(&account_id).unwrap_or(&0) != 0
                {
                    validator_kickout.insert(account_id.clone(), ValidatorKickoutReason::Unstaked);
                }
                proposals.push(proposal);
            }
        }

        let prev_epoch_last_block_hash =
            *self.get_block_info(last_block_info.epoch_first_block())?.prev_hash();
        let prev_validator_kickout = next_epoch_info.validator_kickout();

        // Compute kick outs for validators who are offline.
        let (kickout, validator_block_chunk_stats) = self.compute_kickout_info(
            &epoch_info,
            &block_validator_tracker,
            &chunk_validator_tracker,
            &slashed_validators,
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
            next_version,
        })
    }

    /// Finalizes epoch (T), where given last block hash is given, and returns next next epoch id (T + 2).
    fn finalize_epoch(
        &mut self,
        store_update: &mut StoreUpdate,
        block_info: &BlockInfo,
        last_block_hash: &CryptoHash,
        rng_seed: RngSeed,
    ) -> Result<EpochId, EpochError> {
        let epoch_summary = self.collect_blocks_info(&block_info, last_block_hash)?;
        let epoch_info = self.get_epoch_info(&block_info.epoch_id())?;
        let epoch_protocol_version = epoch_info.protocol_version();
        let validator_stake =
            epoch_info.validators_iter().map(|r| r.account_and_stake()).collect::<HashMap<_, _>>();
        let next_epoch_id = self.get_next_epoch_id_from_info(block_info)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?.clone();
        self.save_epoch_validator_info(store_update, &block_info.epoch_id(), &epoch_summary)?;

        let EpochSummary {
            prev_epoch_last_block_hash,
            all_proposals,
            validator_kickout,
            validator_block_chunk_stats,
            next_version,
        } = epoch_summary;

        let (validator_reward, minted_amount) = {
            let last_epoch_last_block_hash =
                *self.get_block_info(block_info.epoch_first_block())?.prev_hash();
            let last_block_in_last_epoch = self.get_block_info(&last_epoch_last_block_hash)?;
            assert!(block_info.timestamp_nanosec() > last_block_in_last_epoch.timestamp_nanosec());
            let epoch_duration =
                block_info.timestamp_nanosec() - last_block_in_last_epoch.timestamp_nanosec();
            self.reward_calculator.calculate_reward(
                validator_block_chunk_stats,
                &validator_stake,
                *block_info.total_supply(),
                epoch_protocol_version,
                self.genesis_protocol_version,
                epoch_duration,
            )
        };
        let next_next_epoch_info = match proposals_to_epoch_info(
            &self.config,
            rng_seed,
            &next_epoch_info,
            all_proposals,
            validator_kickout,
            validator_reward,
            minted_amount,
            next_version,
        ) {
            Ok(next_next_epoch_info) => next_next_epoch_info,
            Err(EpochError::ThresholdError { stake_sum, num_seats }) => {
                warn!(target: "epoch_manager", "Not enough stake for required number of seats (all validators tried to unstake?): amount = {} for {}", stake_sum, num_seats);
                let mut epoch_info = next_epoch_info.clone();
                *epoch_info.epoch_height_mut() += 1;
                epoch_info
            }
            Err(err) => return Err(err),
        };
        // This epoch info is computed for the epoch after next (T+2),
        // where epoch_id of it is the hash of last block in this epoch (T).
        self.save_epoch_info(store_update, &EpochId(*last_block_hash), next_next_epoch_info)?;
        // Return next epoch (T+1) id as hash of last block in previous epoch (T-1).
        Ok(EpochId(prev_epoch_last_block_hash))
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
            if block_info.prev_hash() == &CryptoHash::default() {
                // This is genesis block, we special case as new epoch.
                assert_eq!(block_info.proposals_iter().len(), 0);
                let pre_genesis_epoch_id = EpochId::default();
                let genesis_epoch_info = self.get_epoch_info(&pre_genesis_epoch_id)?.clone();
                self.save_block_info(&mut store_update, block_info)?;
                self.save_epoch_info(
                    &mut store_update,
                    &EpochId(current_hash),
                    genesis_epoch_info,
                )?;
            } else {
                let prev_block_info = self.get_block_info(block_info.prev_hash())?.clone();

                let mut is_epoch_start = false;
                if prev_block_info.prev_hash() == &CryptoHash::default() {
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
                    *block_info.epoch_id_mut() = prev_block_info.epoch_id().clone();
                    *block_info.epoch_first_block_mut() = *prev_block_info.epoch_first_block();
                }
                let epoch_info = self.get_epoch_info(block_info.epoch_id())?.clone();

                // Keep `slashed` from previous block if they are still in the epoch info stake change
                // (e.g. we need to keep track that they are still slashed, because when we compute
                // returned stake we are skipping account ids that are slashed in `stake_change`).
                for (account_id, slash_state) in prev_block_info.slashed().iter() {
                    if is_epoch_start {
                        if slash_state == &SlashState::DoubleSign
                            || slash_state == &SlashState::Other
                        {
                            block_info
                                .slashed_mut()
                                .entry(account_id.clone())
                                .or_insert(SlashState::AlreadySlashed);
                        } else if epoch_info.stake_change().contains_key(account_id) {
                            block_info
                                .slashed_mut()
                                .entry(account_id.clone())
                                .or_insert_with(|| slash_state.clone());
                        }
                    } else {
                        block_info
                            .slashed_mut()
                            .entry(account_id.clone())
                            .and_modify(|e| {
                                if let SlashState::Other = slash_state {
                                    *e = SlashState::Other;
                                }
                            })
                            .or_insert_with(|| slash_state.clone());
                    }
                }

                if is_epoch_start {
                    self.save_epoch_start(
                        &mut store_update,
                        block_info.epoch_id(),
                        *block_info.height(),
                    )?;
                }

                // Save current block info.
                self.save_block_info(&mut store_update, block_info.clone())?;
                let mut is_new_final_block = false;
                if block_info.last_finalized_height() > &self.largest_final_height {
                    self.largest_final_height = *block_info.last_finalized_height();
                    is_new_final_block = true;
                }

                // Find the last block hash to properly update epoch info aggregator. We only update
                // the aggregator if there is a change in the last final block or it is the epoch
                // start.
                let last_block_hash = if !is_new_final_block {
                    None
                } else {
                    match self.get_block_info(block_info.last_final_block_hash()) {
                        Ok(final_block_info) => {
                            if final_block_info.epoch_id() != block_info.epoch_id() {
                                if is_epoch_start {
                                    Some(&current_hash)
                                } else {
                                    // This means there has been no final block in the epoch yet and
                                    // we have already done the update at epoch start. Therefore we
                                    // do no need to do anything.
                                    None
                                }
                            } else {
                                Some(block_info.last_final_block_hash())
                            }
                        }
                        Err(e) => {
                            warn!(target: "epoch_manger", "last final block of {} cannot be found: {}", current_hash, e);
                            None
                        }
                    }
                };
                if let Some(last_block_hash) = last_block_hash {
                    let epoch_info_aggregator = self.get_and_update_epoch_info_aggregator(
                        block_info.epoch_id(),
                        last_block_hash,
                        false,
                    )?;
                    self.save_epoch_info_aggregator(
                        &mut store_update,
                        epoch_info_aggregator,
                        is_epoch_start || *block_info.height() % AGGREGATOR_SAVE_PERIOD == 0,
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

    /// Given epoch id and height, returns validator information that suppose to produce
    /// the block at that height. We don't require caller to know about EpochIds.
    pub fn get_block_producer_info(
        &mut self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?.clone();
        let validator_id = Self::block_producer_from_info(&epoch_info, height);
        Ok(epoch_info.get_validator(validator_id))
    }

    /// Returns settlement of all block producers in current epoch, with indicator on whether they are slashed or not.
    pub fn get_all_block_producers_settlement(
        &mut self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<&[(ValidatorStake, bool)], EpochError> {
        // TODO(3674): Revisit this when we enable slashing
        if self.epoch_validators_ordered.cache_get(epoch_id).is_none() {
            let slashed = self.get_slashed_validators(last_known_block_hash)?.clone();
            let epoch_info = self.get_epoch_info(epoch_id)?;
            let mut settlement = Vec::with_capacity(epoch_info.block_producers_settlement().len());
            for validator_id in epoch_info.block_producers_settlement().into_iter() {
                let validator_stake = epoch_info.get_validator(*validator_id);
                let is_slashed = slashed.contains_key(validator_stake.account_id());
                settlement.push((validator_stake, is_slashed));
            }
            self.epoch_validators_ordered.cache_set(epoch_id.clone(), settlement);
        }
        Ok(self.epoch_validators_ordered.cache_get(epoch_id).unwrap())
    }

    /// Returns all unique block producers in current epoch sorted by account_id, with indicator on whether they are slashed or not.
    pub fn get_all_block_producers_ordered(
        &mut self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<&[(ValidatorStake, bool)], EpochError> {
        if self.epoch_validators_ordered_unique.cache_get(epoch_id).is_none() {
            let settlement =
                self.get_all_block_producers_settlement(epoch_id, last_known_block_hash)?;
            let mut result = vec![];
            let mut validators: HashSet<AccountId> = HashSet::default();
            for (validator_stake, is_slashed) in settlement.into_iter() {
                let account_id = validator_stake.account_id();
                if !validators.contains(account_id) {
                    validators.insert(account_id.clone());
                    result.push((validator_stake.clone(), *is_slashed));
                }
            }
            self.epoch_validators_ordered_unique.cache_set(epoch_id.clone(), result);
        }
        Ok(self.epoch_validators_ordered_unique.cache_get(epoch_id).unwrap())
    }

    /// get_heuristic_block_approvers_ordered: block producers for epoch
    /// get_all_block_producers_ordered: block producers for epoch, slashing info
    /// get_all_block_approvers_ordered: block producers for epoch, slashing info, sometimes block producers for next epoch
    pub fn get_heuristic_block_approvers_ordered(
        &mut self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ApprovalStake>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        let mut result = vec![];
        let mut validators: HashSet<AccountId> = HashSet::new();
        for validator_id in epoch_info.block_producers_settlement().into_iter() {
            let validator_stake = epoch_info.get_validator(*validator_id);
            let account_id = validator_stake.account_id();
            if !validators.contains(account_id) {
                validators.insert(account_id.clone());
                result.push(validator_stake.get_approval_stake(false));
            }
        }

        Ok(result)
    }

    pub fn get_all_block_approvers_ordered(
        &mut self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, EpochError> {
        let current_epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(parent_hash)?;

        let mut settlement =
            self.get_all_block_producers_settlement(&current_epoch_id, parent_hash)?.to_vec();

        let settlement_epoch_boundary = settlement.len();

        let block_info = self.get_block_info(parent_hash)?.clone();
        if self.next_block_need_approvals_from_next_epoch(&block_info)? {
            settlement.extend(
                self.get_all_block_producers_settlement(&next_epoch_id, parent_hash)?
                    .iter()
                    .cloned(),
            );
        }

        let mut result = vec![];
        let mut validators: HashMap<AccountId, usize> = HashMap::default();
        for (ord, (validator_stake, is_slashed)) in settlement.into_iter().enumerate() {
            let account_id = validator_stake.account_id();
            match validators.get(account_id) {
                None => {
                    validators.insert(account_id.clone(), result.len());
                    result.push((
                        validator_stake.get_approval_stake(ord >= settlement_epoch_boundary),
                        is_slashed,
                    ));
                }
                Some(old_ord) => {
                    if ord >= settlement_epoch_boundary {
                        result[*old_ord].0.stake_next_epoch = validator_stake.stake();
                    };
                }
            };
        }
        Ok(result)
    }

    /// For given epoch_id, height and shard_id returns validator that is chunk producer.
    pub fn get_chunk_producer_info(
        &mut self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?.clone();
        let validator_id = Self::chunk_producer_from_info(&epoch_info, height, shard_id);
        Ok(epoch_info.get_validator(validator_id))
    }

    /// Returns validator for given account id for given epoch.
    /// We don't require caller to know about EpochIds. Doesn't account for slashing.
    pub fn get_validator_by_account_id(
        &mut self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<ValidatorStake>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        Ok(epoch_info.get_validator_by_account(account_id))
    }

    /// Returns fisherman for given account id for given epoch.
    pub fn get_fisherman_by_account_id(
        &mut self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<ValidatorStake>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        Ok(epoch_info.get_fisherman_by_account(account_id))
    }

    pub fn get_slashed_validators(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&HashMap<AccountId, SlashState>, EpochError> {
        Ok(self.get_block_info(block_hash)?.slashed())
    }

    pub fn get_epoch_id(&mut self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        Ok(self.get_block_info(block_hash)?.epoch_id().clone())
    }

    pub fn get_next_epoch_id(&mut self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let block_info = self.get_block_info(block_hash)?.clone();
        self.get_next_epoch_id_from_info(&block_info)
    }

    pub fn get_prev_epoch_id(&mut self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let epoch_first_block = *self.get_block_info(block_hash)?.epoch_first_block();
        let prev_epoch_last_hash = *self.get_block_info(&epoch_first_block)?.prev_hash();
        self.get_epoch_id(&prev_epoch_last_hash)
    }

    pub fn get_epoch_info_from_hash(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&EpochInfo, EpochError> {
        let epoch_id = self.get_epoch_id(block_hash)?;
        self.get_epoch_info(&epoch_id)
    }

    pub fn cares_about_shard_from_prev_block(
        &mut self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        self.cares_about_shard_in_epoch(epoch_id, account_id, shard_id)
    }

    pub fn cares_about_shard_next_epoch_from_prev_block(
        &mut self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(parent_hash)?;
        self.cares_about_shard_in_epoch(next_epoch_id, account_id, shard_id)
    }

    /// Returns true if next block after given block hash is in the new epoch.
    #[allow(clippy::wrong_self_convention)]
    pub fn is_next_block_epoch_start(
        &mut self,
        parent_hash: &CryptoHash,
    ) -> Result<bool, EpochError> {
        let block_info = self.get_block_info(parent_hash)?.clone();
        self.is_next_block_in_next_epoch(&block_info)
    }

    pub fn get_epoch_id_from_prev_block(
        &mut self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        if self.is_next_block_epoch_start(parent_hash)? {
            self.get_next_epoch_id(parent_hash)
        } else {
            self.get_epoch_id(parent_hash)
        }
    }

    pub fn get_next_epoch_id_from_prev_block(
        &mut self,
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
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<BlockHeight, EpochError> {
        let epoch_first_block = *self.get_block_info(block_hash)?.epoch_first_block();
        Ok(*self.get_block_info(&epoch_first_block)?.height())
    }

    /// Compute stake return info based on the last block hash of the epoch that is just finalized
    /// return the hashmap of account id to max_of_stakes, which is used in the calculation of account
    /// updates.
    ///
    /// # Returns
    /// If successful, a triple of (hashmap of account id to max of stakes in the past three epochs,
    /// validator rewards in the last epoch, double sign slashing for the past epoch).
    pub fn compute_stake_return_info(
        &mut self,
        last_block_hash: &CryptoHash,
    ) -> Result<
        (HashMap<AccountId, Balance>, HashMap<AccountId, Balance>, HashMap<AccountId, Balance>),
        EpochError,
    > {
        let next_next_epoch_id = EpochId(*last_block_hash);
        let validator_reward = self.get_epoch_info(&next_next_epoch_id)?.validator_reward().clone();

        let next_epoch_id = self.get_next_epoch_id(last_block_hash)?;
        let epoch_id = self.get_epoch_id(last_block_hash)?;
        debug!(target: "epoch_manager",
            "epoch id: {:?}, prev_epoch_id: {:?}, prev_prev_epoch_id: {:?}",
            next_next_epoch_id, next_epoch_id, epoch_id
        );
        // Fetch last block info to get the slashed accounts.
        let last_block_info = self.get_block_info(last_block_hash)?.clone();
        // Since stake changes for epoch T are stored in epoch info for T+2, the one stored by epoch_id
        // is the prev_prev_stake_change.
        let prev_prev_stake_change = self.get_epoch_info(&epoch_id)?.stake_change().clone();
        let prev_stake_change = self.get_epoch_info(&next_epoch_id)?.stake_change().clone();
        let stake_change = self.get_epoch_info(&next_next_epoch_id)?.stake_change();
        debug!(target: "epoch_manager",
            "prev_prev_stake_change: {:?}, prev_stake_change: {:?}, stake_change: {:?}, slashed: {:?}",
            prev_prev_stake_change, prev_stake_change, stake_change, last_block_info.slashed()
        );
        let mut all_keys = HashSet::new();
        for (key, _) in
            prev_prev_stake_change.iter().chain(prev_stake_change.iter()).chain(stake_change.iter())
        {
            all_keys.insert(key);
        }
        let mut stake_info = HashMap::new();
        for account_id in all_keys {
            if last_block_info.slashed().contains_key(account_id) {
                if prev_prev_stake_change.contains_key(account_id)
                    && !prev_stake_change.contains_key(account_id)
                    && !stake_change.contains_key(account_id)
                {
                    // slashed in prev_prev epoch so it is safe to return the remaining stake in case of
                    // a double sign without violating the staking invariant.
                } else {
                    continue;
                }
            }
            let new_stake = *stake_change.get(account_id).unwrap_or(&0);
            let prev_stake = *prev_stake_change.get(account_id).unwrap_or(&0);
            let prev_prev_stake = *prev_prev_stake_change.get(account_id).unwrap_or(&0);
            let max_of_stakes =
                vec![prev_prev_stake, prev_stake, new_stake].into_iter().max().unwrap();
            stake_info.insert(account_id.to_string(), max_of_stakes);
        }
        let slashing_info = self.compute_double_sign_slashing_info(last_block_hash)?;
        debug!(target: "epoch_manager", "stake_info: {:?}, validator_reward: {:?}", stake_info, validator_reward);
        Ok((stake_info, validator_reward, slashing_info))
    }

    /// Compute slashing information. Returns a hashmap of account id to slashed amount for double sign
    /// slashing.
    fn compute_double_sign_slashing_info(
        &mut self,
        last_block_hash: &CryptoHash,
    ) -> Result<HashMap<AccountId, Balance>, EpochError> {
        let slashed = self.get_slashed_validators(last_block_hash)?.clone();
        let epoch_id = self.get_epoch_id(last_block_hash)?;
        let epoch_info = self.get_epoch_info(&epoch_id)?;
        let total_stake: Balance = epoch_info.validators_iter().map(|v| v.stake()).sum();
        let total_slashed_stake: Balance = slashed
            .iter()
            .filter_map(|(account_id, slashed)| match slashed {
                SlashState::DoubleSign => Some(
                    epoch_info
                        .get_validator_id(account_id)
                        .map_or(0, |id| epoch_info.validator_stake(*id)),
                ),
                _ => None,
            })
            .sum();
        let is_totally_slashed = total_slashed_stake * 3 >= total_stake;
        let mut res = HashMap::default();
        for (account_id, slash_state) in slashed {
            if let SlashState::DoubleSign = slash_state {
                if let Some(&idx) = epoch_info.get_validator_id(&account_id) {
                    let stake = epoch_info.validator_stake(idx);
                    let slashed_stake = if is_totally_slashed {
                        stake
                    } else {
                        let stake = U256::from(stake);
                        // 3 * (total_slashed_stake / total_stake) * stake
                        (U256::from(3) * U256::from(total_slashed_stake) * stake
                            / U256::from(total_stake))
                        .as_u128()
                    };
                    res.insert(account_id, slashed_stake);
                }
            }
        }
        Ok(res)
    }

    /// Get validators for current epoch and next epoch.
    pub fn get_validator_info(
        &mut self,
        epoch_identifier: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError> {
        let epoch_id = match epoch_identifier {
            ValidatorInfoIdentifier::EpochId(ref id) => id.clone(),
            ValidatorInfoIdentifier::BlockHash(ref b) => self.get_block_info(b)?.epoch_id().clone(),
        };
        let cur_epoch_info = self.get_epoch_info(&epoch_id)?.clone();
        let epoch_height = cur_epoch_info.epoch_height();
        let epoch_start_height = self.get_epoch_start_from_epoch_id(&epoch_id)?;
        let mut validator_to_shard = (0..cur_epoch_info.validators_len())
            .map(|_| HashSet::default())
            .collect::<Vec<HashSet<ShardId>>>();
        for (shard_id, validators) in
            cur_epoch_info.chunk_producers_settlement().into_iter().enumerate()
        {
            for validator_id in validators {
                validator_to_shard[*validator_id as usize].insert(shard_id as ShardId);
            }
        }

        // This ugly code arises because of the incompatible types between `block_tracker` in `EpochInfoAggregator`
        // and `validator_block_chunk_stats` in `EpochSummary`. Rust currently has no support for Either type
        // in std.
        let (current_validators, next_epoch_id, all_proposals) = match &epoch_identifier {
            ValidatorInfoIdentifier::EpochId(id) => {
                let epoch_summary = self.get_epoch_validator_info(id)?;
                let cur_validators = cur_epoch_info
                    .validators_iter()
                    .enumerate()
                    .map(|(validator_id, info)| {
                        let validator_stats = epoch_summary
                            .validator_block_chunk_stats
                            .get(info.account_id())
                            .map(|stats| stats.block_stats.clone())
                            .unwrap_or_else(|| ValidatorStats { produced: 0, expected: 0 });
                        let mut shards = validator_to_shard[validator_id]
                            .iter()
                            .cloned()
                            .collect::<Vec<ShardId>>();
                        shards.sort();
                        let (account_id, public_key, stake) = info.destructure();
                        Ok(CurrentEpochValidatorInfo {
                            is_slashed: false, // currently there is no slashing
                            account_id,
                            public_key,
                            stake,
                            shards,
                            num_produced_blocks: validator_stats.produced,
                            num_expected_blocks: validator_stats.expected,
                        })
                    })
                    .collect::<Result<Vec<CurrentEpochValidatorInfo>, EpochError>>()?;
                (
                    cur_validators,
                    EpochId(epoch_summary.prev_epoch_last_block_hash),
                    epoch_summary.all_proposals.into_iter().map(Into::into).collect(),
                )
            }
            ValidatorInfoIdentifier::BlockHash(ref h) => {
                let aggregator = self.get_and_update_epoch_info_aggregator(&epoch_id, h, true)?;
                let cur_validators = cur_epoch_info
                    .validators_iter()
                    .enumerate()
                    .map(|(validator_id, info)| {
                        let validator_stats = aggregator
                            .block_tracker
                            .get(&(validator_id as u64))
                            .unwrap_or_else(|| &ValidatorStats { produced: 0, expected: 0 })
                            .clone();
                        let mut shards = validator_to_shard[validator_id]
                            .clone()
                            .into_iter()
                            .collect::<Vec<ShardId>>();
                        shards.sort();
                        let (account_id, public_key, stake) = info.destructure();
                        Ok(CurrentEpochValidatorInfo {
                            is_slashed: false, // currently there is no slashing
                            account_id,
                            public_key,
                            stake,
                            shards,
                            num_produced_blocks: validator_stats.produced,
                            num_expected_blocks: validator_stats.expected,
                        })
                    })
                    .collect::<Result<Vec<CurrentEpochValidatorInfo>, EpochError>>()?;
                let next_epoch_id = self.get_next_epoch_id(h)?;
                (
                    cur_validators,
                    next_epoch_id,
                    aggregator.all_proposals.into_iter().map(|(_, p)| p.into()).collect(),
                )
            }
        };

        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?;
        let mut next_validator_to_shard = (0..next_epoch_info.validators_len())
            .map(|_| HashSet::default())
            .collect::<Vec<HashSet<ShardId>>>();
        for (shard_id, validators) in
            next_epoch_info.chunk_producers_settlement().iter().enumerate()
        {
            for validator_id in validators {
                next_validator_to_shard[*validator_id as usize].insert(shard_id as u64);
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

    /// Compare two epoch ids based on their start height. This works because finality gadget
    /// guarantees that we cannot have two different epochs on two forks
    pub fn compare_epoch_id(
        &mut self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, EpochError> {
        if epoch_id.0 == other_epoch_id.0 {
            return Ok(Ordering::Equal);
        }
        match (
            self.get_epoch_start_from_epoch_id(epoch_id),
            self.get_epoch_start_from_epoch_id(other_epoch_id),
        ) {
            (Ok(index1), Ok(index2)) => Ok(index1.cmp(&index2)),
            (Ok(_), Err(_)) => self.get_epoch_info(other_epoch_id).map(|_| Ordering::Less),
            (Err(_), Ok(_)) => self.get_epoch_info(epoch_id).map(|_| Ordering::Greater),
            (Err(_), Err(_)) => Err(EpochError::EpochOutOfBounds(epoch_id.clone())), // other_epoch_id may be out of bounds as well
        }
    }

    /// Get minimum stake allowed at current block. Attempts to stake with a lower stake will be
    /// rejected.
    pub fn minimum_stake(&mut self, prev_block_hash: &CryptoHash) -> Result<Balance, EpochError> {
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(prev_block_hash)?;
        let stake_divisor = self.config.minimum_stake_divisor as Balance;
        Ok(self.get_epoch_info(&next_epoch_id)?.seat_price() / stake_divisor)
    }

    // Note: this function should only be used in 18 -> 19 migration and should be removed in the
    // next release
    /// `block_header_info` must be the header info of the last block of an epoch.
    pub fn migrate_18_to_19(
        &mut self,
        block_header_info: &BlockHeaderInfo,
        store_update: &mut StoreUpdate,
    ) -> Result<(), EpochError> {
        let block_info = self.get_block_info(&block_header_info.hash)?.clone();
        self.finalize_epoch(
            store_update,
            &block_info,
            &block_header_info.hash,
            block_header_info.random_value.into(),
        )?;
        Ok(())
    }
}

/// Private utilities for EpochManager.
impl EpochManager {
    fn cares_about_shard_in_epoch(
        &mut self,
        epoch_id: EpochId,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let epoch_info = self.get_epoch_info(&epoch_id)?;
        let chunk_producers = epoch_info.chunk_producers_settlement();
        for validator_id in chunk_producers[shard_id as usize].iter() {
            if epoch_info.validator_account_id(*validator_id) == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn block_producer_from_info(
        epoch_info: &EpochInfo,
        height: BlockHeight,
    ) -> ValidatorId {
        let bp_settlement = epoch_info.block_producers_settlement();
        bp_settlement[(height as u64 % (bp_settlement.len() as u64)) as usize]
    }

    pub(crate) fn chunk_producer_from_info(
        epoch_info: &EpochInfo,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> ValidatorId {
        let cp_settlement = epoch_info.chunk_producers_settlement();
        let shard_cps = &cp_settlement[shard_id as usize];
        shard_cps[(height as u64 % (shard_cps.len() as u64)) as usize]
    }

    /// Returns true, if given current block info, next block supposed to be in the next epoch.
    #[allow(clippy::wrong_self_convention)]
    fn is_next_block_in_next_epoch(&mut self, block_info: &BlockInfo) -> Result<bool, EpochError> {
        if block_info.prev_hash() == &CryptoHash::default() {
            return Ok(true);
        }
        let estimated_next_epoch_start =
            *self.get_block_info(block_info.epoch_first_block())?.height()
                + self.config.epoch_length;

        if self.config.epoch_length <= 3 {
            // This is here to make epoch_manager tests pass. Needs to be removed, tracked in
            // https://github.com/nearprotocol/nearcore/issues/2522
            return Ok(*block_info.height() + 1 >= estimated_next_epoch_start);
        }

        Ok(*block_info.last_finalized_height() + 3 >= estimated_next_epoch_start)
    }

    /// Returns true, if given current block info, next block must include the approvals from the next
    /// epoch (in addition to the approvals from the current epoch)
    fn next_block_need_approvals_from_next_epoch(
        &mut self,
        block_info: &BlockInfo,
    ) -> Result<bool, EpochError> {
        if self.is_next_block_in_next_epoch(block_info)? {
            return Ok(false);
        }
        let estimated_next_epoch_start =
            *self.get_block_info(block_info.epoch_first_block())?.height()
                + self.config.epoch_length;
        Ok(*block_info.last_finalized_height() + 3 < estimated_next_epoch_start
            && *block_info.height() + 3 >= estimated_next_epoch_start)
    }

    /// Returns epoch id for the next epoch (T+1), given an block info in current epoch (T).
    fn get_next_epoch_id_from_info(
        &mut self,
        block_info: &BlockInfo,
    ) -> Result<EpochId, EpochError> {
        let first_block_info = self.get_block_info(block_info.epoch_first_block())?;
        Ok(EpochId(*first_block_info.prev_hash()))
    }

    pub fn get_epoch_info(&mut self, epoch_id: &EpochId) -> Result<&EpochInfo, EpochError> {
        if !self.epochs_info.cache_get(epoch_id).is_some() {
            let epoch_info = self
                .store
                .get_ser(ColEpochInfo, epoch_id.as_ref())
                .map_err(|err| err.into())
                .and_then(|value| {
                    value.ok_or_else(|| EpochError::EpochOutOfBounds(epoch_id.clone()))
                })?;
            self.epochs_info.cache_set(epoch_id.clone(), epoch_info);
        }
        self.epochs_info.cache_get(epoch_id).ok_or(EpochError::EpochOutOfBounds(epoch_id.clone()))
    }

    fn has_epoch_info(&mut self, epoch_id: &EpochId) -> Result<bool, EpochError> {
        match self.get_epoch_info(epoch_id) {
            Ok(_) => Ok(true),
            Err(EpochError::EpochOutOfBounds(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    fn save_epoch_info(
        &mut self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        epoch_info: EpochInfo,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(ColEpochInfo, epoch_id.as_ref(), &epoch_info)
            .map_err(EpochError::from)?;
        self.epochs_info.cache_set(epoch_id.clone(), epoch_info);
        Ok(())
    }

    pub fn get_epoch_validator_info(
        &mut self,
        epoch_id: &EpochId,
    ) -> Result<EpochSummary, EpochError> {
        // We don't use cache here since this query happens rarely and only for rpc.
        self.store
            .get_ser(ColEpochValidatorInfo, epoch_id.as_ref())
            .map_err(|err| err.into())
            .and_then(|value| value.ok_or_else(|| EpochError::EpochOutOfBounds(epoch_id.clone())))
    }

    fn save_epoch_validator_info(
        &self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        epoch_summary: &EpochSummary,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(ColEpochValidatorInfo, epoch_id.as_ref(), epoch_summary)
            .map_err(EpochError::from)
    }

    fn has_block_info(&mut self, hash: &CryptoHash) -> Result<bool, EpochError> {
        match self.get_block_info(hash) {
            Ok(_) => Ok(true),
            Err(EpochError::MissingBlock(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Get BlockInfo for a block
    /// # Errors
    /// EpochError::IOErr if storage returned an error
    /// EpochError::MissingBlock if block is not in storage
    pub fn get_block_info(&mut self, hash: &CryptoHash) -> Result<&BlockInfo, EpochError> {
        if self.blocks_info.cache_get(hash).is_none() {
            let block_info = self
                .store
                .get_ser(ColBlockInfo, hash.as_ref())
                .map_err(EpochError::from)
                .and_then(|value| value.ok_or_else(|| EpochError::MissingBlock(*hash)))?;
            self.blocks_info.cache_set(*hash, block_info);
        }
        self.blocks_info.cache_get(hash).ok_or(EpochError::MissingBlock(*hash))
    }

    fn save_block_info(
        &mut self,
        store_update: &mut StoreUpdate,
        block_info: BlockInfo,
    ) -> Result<(), EpochError> {
        let block_hash = *block_info.hash();
        store_update
            .set_ser(ColBlockInfo, block_hash.as_ref(), &block_info)
            .map_err(EpochError::from)?;
        self.blocks_info.cache_set(block_hash, block_info);
        Ok(())
    }

    fn save_epoch_start(
        &mut self,
        store_update: &mut StoreUpdate,
        epoch_id: &EpochId,
        epoch_start: BlockHeight,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(ColEpochStart, epoch_id.as_ref(), &epoch_start)
            .map_err(EpochError::from)?;
        self.epoch_id_to_start.cache_set(epoch_id.clone(), epoch_start);
        Ok(())
    }

    fn get_epoch_start_from_epoch_id(
        &mut self,
        epoch_id: &EpochId,
    ) -> Result<BlockHeight, EpochError> {
        if self.epoch_id_to_start.cache_get(epoch_id).is_none() {
            let epoch_start = self
                .store
                .get_ser(ColEpochStart, epoch_id.as_ref())
                .map_err(EpochError::from)
                .and_then(|value| {
                    value.ok_or_else(|| EpochError::EpochOutOfBounds(epoch_id.clone()))
                })?;
            self.epoch_id_to_start.cache_set(epoch_id.clone(), epoch_start);
        }
        Ok(*self.epoch_id_to_start.cache_get(epoch_id).unwrap())
    }

    /// Get epoch info aggregator and update it to block info as of `last_block_hash`. If `epoch_id`
    /// doesn't match the epoch id of the existing aggregator, re-initialize the aggregator.
    /// If `copy_only` is true, then we clone what is in the cache. Otherwise we take the aggregator
    /// from cache and invalidates the cache.
    pub fn get_and_update_epoch_info_aggregator(
        &mut self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
        copy_only: bool,
    ) -> Result<EpochInfoAggregator, EpochError> {
        let epoch_info_aggregator_cache = if copy_only {
            self.epoch_info_aggregator.clone()
        } else {
            self.epoch_info_aggregator.take()
        };
        let mut epoch_change = false;
        let mut aggregator = if let Some(aggregator) = epoch_info_aggregator_cache {
            aggregator
        } else {
            epoch_change = true;
            self.store
                .get_ser(ColEpochInfo, AGGREGATOR_KEY)
                .map_err(EpochError::from)?
                .unwrap_or_else(|| EpochInfoAggregator::new(epoch_id.clone(), *last_block_hash))
        };
        if &aggregator.epoch_id != epoch_id {
            aggregator = EpochInfoAggregator::new(epoch_id.clone(), *last_block_hash);
            epoch_change = true;
        }
        let epoch_info = self.get_epoch_info(epoch_id)?.clone();
        let mut new_aggregator = EpochInfoAggregator::new(epoch_id.clone(), *last_block_hash);
        let mut cur_hash = *last_block_hash;
        let mut overwrite = false;
        while cur_hash != aggregator.last_block_hash || epoch_change {
            // Avoid cloning
            let prev_hash = *self.get_block_info(&cur_hash)?.prev_hash();
            let prev_height = self.get_block_info(&prev_hash).map(|info| *info.height());

            let block_info = self.get_block_info(&cur_hash)?;
            if block_info.epoch_id() != epoch_id || block_info.prev_hash() == &CryptoHash::default()
            {
                // This means that we reached the previous epoch and still hasn't seen
                // `aggregator.last_block_hash` and therefore implies either a fork has happened
                // or we are at the start of an epoch. In this case, the new aggregator should
                // overwrite the old one.
                overwrite = true;
                break;
            }
            new_aggregator.update(&block_info, &epoch_info, prev_height?);
            cur_hash = *block_info.prev_hash();
        }
        aggregator.merge(new_aggregator, overwrite);

        Ok(aggregator)
    }

    fn save_epoch_info_aggregator(
        &mut self,
        store_update: &mut StoreUpdate,
        aggregator: EpochInfoAggregator,
        write_to_storage: bool,
    ) -> Result<(), EpochError> {
        if write_to_storage {
            store_update.set_ser(ColEpochInfo, AGGREGATOR_KEY, &aggregator)?;
        }
        self.epoch_info_aggregator = Some(aggregator);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use num_rational::Rational;

    use near_primitives::challenge::SlashedValidator;
    use near_primitives::hash::hash;
    use near_primitives::types::ValidatorKickoutReason::NotEnoughBlocks;
    use near_primitives::version::PROTOCOL_VERSION;
    use near_store::test_utils::create_test_store;

    use crate::test_utils::{
        block_info, change_stake, default_reward_calculator, epoch_config, epoch_info,
        epoch_info_with_num_seats, hash_range, record_block, record_block_with_final_block_hash,
        record_block_with_slashes, record_with_block_info, reward, setup_default_epoch_manager,
        setup_epoch_manager, stake, DEFAULT_TOTAL_SUPPLY,
    };

    use super::*;
    use crate::reward_calculator::NUM_NS_IN_SECOND;

    impl EpochManager {
        /// Returns number of produced and expected blocks by given validator.
        fn get_num_validator_blocks(
            &mut self,
            epoch_id: &EpochId,
            last_known_block_hash: &CryptoHash,
            account_id: &AccountId,
        ) -> Result<ValidatorStats, EpochError> {
            let epoch_info = self.get_epoch_info(&epoch_id)?;
            let validator_id = *epoch_info
                .get_validator_id(account_id)
                .ok_or_else(|| EpochError::NotAValidator(account_id.clone(), epoch_id.clone()))?;
            let aggregator =
                self.get_and_update_epoch_info_aggregator(epoch_id, last_known_block_hash, true)?;
            Ok(aggregator
                .block_tracker
                .get(&validator_id)
                .unwrap_or_else(|| &ValidatorStats { produced: 0, expected: 0 })
                .clone())
        }
    }

    #[test]
    fn test_stake_validator() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators.clone(), 1, 1, 2, 2, 90, 60);

        let h = hash_range(4);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

        let expected0 = epoch_info_with_num_seats(
            1,
            vec![("test1", amount_staked)],
            vec![0, 0],
            vec![vec![0, 0]],
            vec![],
            vec![],
            change_stake(vec![("test1", amount_staked)]),
            vec![],
            reward(vec![("near", 0)]),
            0,
            4,
        );
        let epoch0 = epoch_manager.get_epoch_id(&h[0]).unwrap();
        assert_eq!(epoch_manager.get_epoch_info(&epoch0).unwrap(), &expected0);

        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test2", amount_staked)]);
        let epoch1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
        assert_eq!(epoch_manager.get_epoch_info(&epoch1).unwrap(), &expected0);
        assert_eq!(epoch_manager.get_epoch_id(&h[2]), Err(EpochError::MissingBlock(h[2])));

        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        // test2 staked in epoch 1 and therefore should be included in epoch 3.
        let epoch2 = epoch_manager.get_epoch_id(&h[2]).unwrap();
        assert_eq!(epoch_manager.get_epoch_info(&epoch2).unwrap(), &expected0);

        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);

        let expected3 = epoch_info_with_num_seats(
            2,
            vec![("test1", amount_staked), ("test2", amount_staked)],
            vec![0, 1],
            vec![vec![0, 1]],
            vec![],
            vec![],
            change_stake(vec![("test1", amount_staked), ("test2", amount_staked)]),
            vec![],
            // only the validator who produced the block in this epoch gets the reward since epoch length is 1
            reward(vec![("test1", 0), ("near", 0)]),
            0,
            4,
        );
        // no validator change in the last epoch
        let epoch3 = epoch_manager.get_epoch_id(&h[3]).unwrap();
        assert_eq!(epoch_manager.get_epoch_info(&epoch3).unwrap(), &expected3);

        // Start another epoch manager from the same store to check that it saved the state.
        let mut epoch_manager2 = EpochManager::new(
            epoch_manager.store.clone(),
            epoch_manager.config.clone(),
            PROTOCOL_VERSION,
            epoch_manager.reward_calculator,
            validators.iter().map(|(account_id, balance)| stake(*account_id, *balance)).collect(),
        )
        .unwrap();
        assert_eq!(epoch_manager2.get_epoch_info(&epoch3).unwrap(), &expected3);
    }

    #[test]
    fn test_validator_change_of_stake() {
        let amount_staked = 1_000_000;
        let fishermen_threshold = 100;
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
        epoch_manager.config.fishermen_threshold = fishermen_threshold;

        let h = hash_range(4);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 10)]);
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        // New epoch starts here.
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                2,
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)]),
                vec![(
                    "test1",
                    ValidatorKickoutReason::NotEnoughStake {
                        stake: 10,
                        threshold: amount_staked / 2
                    }
                )],
                reward(vec![("test1", 0), ("test2", 0), ("near", 0)]),
                0
            )
        );
    }

    /// Test handling forks across the epoch finalization.
    /// Fork with where one BP produces blocks in one chain and 2 BPs are in another chain.
    ///     |   | /--1---4------|--7---10------|---13---
    ///   x-|-0-|-
    ///     |   | \--2---3---5--|---6---8---9--|----11---12--
    /// In upper fork, only test2 left + new validator test4.
    /// In lower fork, test1 and test3 are left.
    #[test]
    fn test_fork_finalization() {
        let amount_staked = 1_000_000;
        let validators =
            vec![("test1", amount_staked), ("test2", amount_staked), ("test3", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators.clone(), 3, 1, 3, 0, 90, 60);

        let h = hash_range(14);

        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test4", amount_staked)]);
        record_block(&mut epoch_manager, h[1], h[4], 4, vec![]);
        record_block(&mut epoch_manager, h[4], h[7], 7, vec![]);
        record_block(&mut epoch_manager, h[7], h[10], 10, vec![]);
        record_block(&mut epoch_manager, h[10], h[13], 13, vec![]);

        // Builds alternative fork in the network.
        let build_branch2 = |epoch_manager: &mut EpochManager| {
            record_block(epoch_manager, h[0], h[2], 2, vec![]);
            record_block(epoch_manager, h[2], h[3], 3, vec![]);
            record_block(epoch_manager, h[3], h[5], 5, vec![]);
            record_block(epoch_manager, h[5], h[6], 6, vec![]);
            record_block(epoch_manager, h[6], h[8], 8, vec![]);
            record_block(epoch_manager, h[8], h[9], 9, vec![]);
            record_block(epoch_manager, h[9], h[11], 11, vec![]);
            record_block(epoch_manager, h[11], h[12], 12, vec![]);
        };
        build_branch2(&mut epoch_manager);

        let epoch1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
        assert_eq!(
            epoch_manager
                .get_all_block_producers_ordered(&epoch1, &h[1])
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<Vec<_>>(),
            vec![
                ("test3".to_string(), false),
                ("test2".to_string(), false),
                ("test1".to_string(), false)
            ]
        );

        let epoch2_1 = epoch_manager.get_epoch_id(&h[13]).unwrap();
        assert_eq!(
            epoch_manager
                .get_all_block_producers_ordered(&epoch2_1, &h[1])
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<Vec<_>>(),
            vec![("test2".to_string(), false), ("test4".to_string(), false)]
        );

        let epoch2_2 = epoch_manager.get_epoch_id(&h[11]).unwrap();
        assert_eq!(
            epoch_manager
                .get_all_block_producers_ordered(&epoch2_2, &h[1])
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<Vec<_>>(),
            vec![("test1".to_string(), false), ("test3".to_string(), false),]
        );

        // Check that if we have a different epoch manager and apply only second branch we get the same results.
        let mut epoch_manager2 = setup_default_epoch_manager(validators, 3, 1, 3, 0, 90, 60);
        record_block(&mut epoch_manager2, CryptoHash::default(), h[0], 0, vec![]);
        build_branch2(&mut epoch_manager2);
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch2_2),
            epoch_manager2.get_epoch_info(&epoch2_2)
        );
    }

    /// In the case where there is only one validator and the
    /// number of blocks produced by the validator is under the
    /// threshold for some given epoch, the validator should not
    /// be kicked out
    #[test]
    fn test_one_validator_kickout() {
        let amount_staked = 1_000;
        let mut epoch_manager =
            setup_default_epoch_manager(vec![("test1", amount_staked)], 2, 1, 1, 0, 90, 60);

        let h = hash_range(6);
        // this validator only produces one block every epoch whereas they should have produced 2. However, since
        // this is the only validator left, we still keep them as validator.
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[2], 2, vec![]);
        record_block(&mut epoch_manager, h[2], h[4], 4, vec![]);
        record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
        let epoch_id = epoch_manager.get_next_epoch_id(&h[5]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                2,
                vec![("test1", amount_staked)],
                vec![0],
                vec![vec![0]],
                vec![],
                vec![],
                change_stake(vec![("test1", amount_staked)]),
                vec![],
                reward(vec![("near", 0)]),
                0
            )
        );
    }

    /// When computing validator kickout, we should not kickout validators such that the union
    /// of kickout for this epoch and last epoch equals the entire validator set.
    #[test]
    fn test_validator_kickout() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 4, 1, 2, 0, 90, 60);
        let h = hash_range(12);

        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![]);
        record_block(&mut epoch_manager, h[1], h[3], 3, vec![]);
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
        record_block(&mut epoch_manager, h[4], h[6], 6, vec![]);
        record_block(&mut epoch_manager, h[6], h[8], 8, vec![]);
        record_block(&mut epoch_manager, h[8], h[9], 9, vec![]);
        record_block(&mut epoch_manager, h[9], h[10], 10, vec![]);
        let epoch_id = epoch_manager.get_next_epoch_id(&h[6]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap().validator_kickout(),
            &vec![(
                "test2".to_string(),
                ValidatorKickoutReason::NotEnoughBlocks { produced: 1, expected: 2 }
            )]
            .into_iter()
            .collect()
        );
        let epoch_id = epoch_manager.get_next_epoch_id(&h[10]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                3,
                vec![("test1", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", amount_staked)]),
                vec![],
                reward(vec![("test2", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_validator_unstake() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            PROTOCOL_VERSION,
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();
        let h = hash_range(8);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 0)]);
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);

        let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                2,
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)]),
                vec![("test1", ValidatorKickoutReason::Unstaked)],
                reward(vec![("test1", 0), ("test2", 0), ("near", 0)]),
                0
            )
        );
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
        record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
        let epoch_id = epoch_manager.get_next_epoch_id(&h[5]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                3,
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                vec![],
                change_stake(vec![("test2", amount_staked)]),
                vec![],
                reward(vec![("test1", 0), ("test2", 0), ("near", 0)]),
                0
            )
        );
        record_block(&mut epoch_manager, h[5], h[6], 6, vec![]);
        record_block(&mut epoch_manager, h[6], h[7], 7, vec![]);
        let epoch_id = epoch_manager.get_next_epoch_id(&h[7]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                4,
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                vec![],
                change_stake(vec![("test2", amount_staked)]),
                vec![],
                reward(vec![("test2", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_slashing() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            PROTOCOL_VERSION,
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();

        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

        // Slash test1
        let mut slashed = HashMap::new();
        slashed.insert("test1".to_string(), SlashState::Other);
        record_block_with_slashes(
            &mut epoch_manager,
            h[0],
            h[1],
            1,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), false)],
        );

        let epoch_id = epoch_manager.get_epoch_id(&h[1]).unwrap();
        assert_eq!(
            epoch_manager
                .get_all_block_producers_ordered(&epoch_id, &h[1])
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id().clone(), x.1))
                .collect::<Vec<_>>(),
            vec![("test2".to_string(), false), ("test1".to_string(), true)]
        );

        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
        // Epoch 3 -> defined by proposals/slashes in h[1].
        record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);

        let epoch_id = epoch_manager.get_epoch_id(&h[5]).unwrap();
        assert_eq!(epoch_id.0, h[2]);
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                2,
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)]),
                vec![("test1", ValidatorKickoutReason::Slashed)],
                reward(vec![("test2", 0), ("near", 0)]),
                0
            )
        );

        let slashed1: Vec<_> =
            epoch_manager.get_slashed_validators(&h[2]).unwrap().clone().into_iter().collect();
        let slashed2: Vec<_> =
            epoch_manager.get_slashed_validators(&h[3]).unwrap().clone().into_iter().collect();
        let slashed3: Vec<_> =
            epoch_manager.get_slashed_validators(&h[5]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed1, vec![("test1".to_string(), SlashState::Other)]);
        assert_eq!(slashed2, vec![("test1".to_string(), SlashState::AlreadySlashed)]);
        assert_eq!(slashed3, vec![("test1".to_string(), SlashState::AlreadySlashed)]);
    }

    /// Test that double sign interacts with other challenges in the correct way.
    #[test]
    fn test_double_sign_slashing1() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            PROTOCOL_VERSION,
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();

        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![]);
        record_block_with_slashes(
            &mut epoch_manager,
            h[1],
            h[2],
            2,
            vec![],
            vec![
                SlashedValidator::new("test1".to_string(), true),
                SlashedValidator::new("test1".to_string(), false),
            ],
        );

        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[2]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::Other)]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        // new epoch
        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[3]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::AlreadySlashed)]);
        // slash test1 for double sign
        record_block_with_slashes(
            &mut epoch_manager,
            h[3],
            h[4],
            4,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), true)],
        );

        // Epoch 3 -> defined by proposals/slashes in h[1].
        record_block(&mut epoch_manager, h[4], h[5], 5, vec![]);
        let epoch_id = epoch_manager.get_epoch_id(&h[5]).unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                2,
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)]),
                vec![("test1", ValidatorKickoutReason::Slashed)],
                reward(vec![("test2", 0), ("near", 0)]),
                0
            )
        );

        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[5]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::AlreadySlashed)]);
    }

    /// Test that two double sign challenge in two epochs works
    #[test]
    fn test_double_sign_slashing2() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);

        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block_with_slashes(
            &mut epoch_manager,
            h[0],
            h[1],
            1,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), true)],
        );

        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[1]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::DoubleSign)]);

        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[2]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::DoubleSign)]);
        // new epoch
        record_block_with_slashes(
            &mut epoch_manager,
            h[2],
            h[3],
            3,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), true)],
        );
        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[3]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::DoubleSign)]);
    }

    /// If all current validator try to unstake, we disallow that.
    #[test]
    fn test_all_validators_unstake() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(5);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        // all validators are trying to unstake.
        record_block(
            &mut epoch_manager,
            h[0],
            h[1],
            1,
            vec![stake("test1", 0), stake("test2", 0), stake("test3", 0)],
        );
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        let next_epoch = epoch_manager.get_next_epoch_id(&h[2]).unwrap();
        assert_eq!(
            epoch_manager
                .get_epoch_info(&next_epoch)
                .unwrap()
                .validators_iter()
                .collect::<Vec<_>>(),
            vec![
                stake("test1", stake_amount),
                stake("test2", stake_amount),
                stake("test3", stake_amount)
            ],
        );
    }

    #[test]
    fn test_validator_reward_one_validator() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 2;
        let total_supply = stake_amount * validators.len() as u128;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(5, 100),
            num_blocks_per_year: 50,
            epoch_length,
            protocol_reward_rate: Rational::new(1, 10),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(90, 100),
            online_max_threshold: Rational::new(99, 100),
            num_seconds_per_year: 50,
        };
        let mut epoch_manager = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            1,
            0,
            90,
            60,
            100,
            reward_calculator.clone(),
        );
        let rng_seed = [0; 32];
        let h = hash_range(5);

        epoch_manager
            .record_block_info(
                block_info(
                    h[0],
                    0,
                    0,
                    Default::default(),
                    Default::default(),
                    h[0],
                    vec![true],
                    total_supply,
                ),
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true], total_supply),
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true], total_supply),
                rng_seed,
            )
            .unwrap();
        let mut validator_online_ratio = HashMap::new();
        validator_online_ratio.insert(
            "test2".to_string(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 1, expected: 1 },
                chunk_stats: ValidatorStats { produced: 1, expected: 1 },
            },
        );
        let mut validator_stakes = HashMap::new();
        validator_stakes.insert("test2".to_string(), stake_amount);
        let (validator_reward, inflation) = reward_calculator.calculate_reward(
            validator_online_ratio,
            &validator_stakes,
            total_supply,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
        );
        let test2_reward = *validator_reward.get("test2").unwrap();
        let protocol_reward = *validator_reward.get("near").unwrap();

        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap(),
            &epoch_info(
                2,
                vec![("test2", stake_amount + test2_reward)],
                vec![0],
                vec![vec![0]],
                vec![],
                vec![("test1", stake_amount)],
                change_stake(vec![("test1", stake_amount), ("test2", stake_amount + test2_reward)]),
                vec![],
                reward(vec![("test2", test2_reward), ("near", protocol_reward)]),
                inflation,
            )
        );
    }

    #[test]
    fn test_validator_reward_weight_by_stake() {
        let stake_amount1 = 1_000_000;
        let stake_amount2 = 500_000;
        let validators = vec![("test1", stake_amount1), ("test2", stake_amount2)];
        let epoch_length = 2;
        let total_supply = (stake_amount1 + stake_amount2) * validators.len() as u128;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(5, 100),
            num_blocks_per_year: 50,
            epoch_length,
            protocol_reward_rate: Rational::new(1, 10),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(90, 100),
            online_max_threshold: Rational::new(99, 100),
            num_seconds_per_year: 50,
        };
        let mut epoch_manager = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            2,
            0,
            90,
            60,
            100,
            reward_calculator.clone(),
        );
        let h = hash_range(5);
        record_with_block_info(
            &mut epoch_manager,
            block_info(
                h[0],
                0,
                0,
                Default::default(),
                Default::default(),
                h[0],
                vec![true],
                total_supply,
            ),
        );
        record_with_block_info(
            &mut epoch_manager,
            block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true], total_supply),
        );
        record_with_block_info(
            &mut epoch_manager,
            block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true], total_supply),
        );
        let mut validator_online_ratio = HashMap::new();
        validator_online_ratio.insert(
            "test1".to_string(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 1, expected: 1 },
                chunk_stats: ValidatorStats { produced: 1, expected: 1 },
            },
        );
        validator_online_ratio.insert(
            "test2".to_string(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 1, expected: 1 },
                chunk_stats: ValidatorStats { produced: 1, expected: 1 },
            },
        );
        let mut validators_stakes = HashMap::new();
        validators_stakes.insert("test1".to_string(), stake_amount1);
        validators_stakes.insert("test2".to_string(), stake_amount2);
        let (validator_reward, inflation) = reward_calculator.calculate_reward(
            validator_online_ratio,
            &validators_stakes,
            total_supply,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
        );
        let test1_reward = *validator_reward.get("test1").unwrap();
        let test2_reward = *validator_reward.get("test2").unwrap();
        assert_eq!(test1_reward, test2_reward * 2);
        let protocol_reward = *validator_reward.get("near").unwrap();

        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap(),
            &epoch_info(
                2,
                vec![
                    ("test1", stake_amount1 + test1_reward),
                    ("test2", stake_amount2 + test2_reward)
                ],
                vec![1, 0],
                vec![vec![1, 0]],
                vec![],
                vec![],
                change_stake(vec![
                    ("test1", stake_amount1 + test1_reward),
                    ("test2", stake_amount2 + test2_reward)
                ]),
                vec![],
                reward(vec![
                    ("test1", test1_reward),
                    ("test2", test2_reward),
                    ("near", protocol_reward)
                ]),
                inflation,
            )
        );
    }

    #[test]
    fn test_reward_multiple_shards() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 2;
        let total_supply = stake_amount * validators.len() as u128;
        let reward_calculator = RewardCalculator {
            max_inflation_rate: Rational::new(5, 100),
            num_blocks_per_year: 1_000_000,
            epoch_length,
            protocol_reward_rate: Rational::new(1, 10),
            protocol_treasury_account: "near".to_string(),
            online_min_threshold: Rational::new(90, 100),
            online_max_threshold: Rational::new(99, 100),
            num_seconds_per_year: 1_000_000,
        };
        let mut epoch_manager = setup_epoch_manager(
            validators,
            epoch_length,
            2,
            2,
            0,
            90,
            60,
            0,
            reward_calculator.clone(),
        );
        let h = hash_range(5);
        record_with_block_info(
            &mut epoch_manager,
            block_info(
                h[0],
                0,
                0,
                Default::default(),
                Default::default(),
                h[0],
                vec![true],
                total_supply,
            ),
        );
        record_with_block_info(
            &mut epoch_manager,
            block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true, false], total_supply),
        );
        record_with_block_info(
            &mut epoch_manager,
            block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true, true], total_supply),
        );
        let mut validator_online_ratio = HashMap::new();
        validator_online_ratio.insert(
            "test2".to_string(),
            BlockChunkValidatorStats {
                block_stats: ValidatorStats { produced: 1, expected: 1 },
                chunk_stats: ValidatorStats { produced: 1, expected: 1 },
            },
        );
        let mut validators_stakes = HashMap::new();
        validators_stakes.insert("test1".to_string(), stake_amount);
        validators_stakes.insert("test2".to_string(), stake_amount);
        let (validator_reward, inflation) = reward_calculator.calculate_reward(
            validator_online_ratio,
            &validators_stakes,
            total_supply,
            PROTOCOL_VERSION,
            PROTOCOL_VERSION,
            epoch_length * NUM_NS_IN_SECOND,
        );
        let test2_reward = *validator_reward.get("test2").unwrap();
        let protocol_reward = *validator_reward.get("near").unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap(),
            &epoch_info(
                2,
                vec![("test2", stake_amount + test2_reward)],
                vec![0, 0],
                vec![vec![0], vec![0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", stake_amount + test2_reward)]),
                vec![(
                    "test1",
                    ValidatorKickoutReason::NotEnoughChunks { produced: 1, expected: 2 }
                )],
                reward(vec![("test2", test2_reward), ("near", protocol_reward)]),
                inflation
            )
        );
    }

    #[test]
    fn test_unstake_and_then_change_stake() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
        let h = hash_range(8);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 0)]);
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![stake("test1", amount_staked)]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        let epoch_id = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
        assert_eq!(epoch_id, EpochId(h[2]));
        assert_eq!(
            epoch_manager.get_epoch_info(&epoch_id).unwrap(),
            &epoch_info(
                2,
                vec![("test1", amount_staked), ("test2", amount_staked)],
                vec![1, 0],
                vec![vec![1, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", amount_staked), ("test2", amount_staked)]),
                vec![],
                reward(vec![("test1", 0), ("test2", 0), ("near", 0)]),
                0
            )
        );
    }

    /// When a block producer fails to produce a block, check that other chunk producers who produce
    /// chunks for that block are not kicked out because of it.
    #[test]
    fn test_expected_chunks() {
        let stake_amount = 1_000_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let epoch_length = 3;
        let total_supply = stake_amount * validators.len() as u128;
        let mut epoch_manager = setup_epoch_manager(
            validators,
            epoch_length,
            3,
            3,
            0,
            90,
            60,
            0,
            default_reward_calculator(),
        );
        let rng_seed = [0; 32];
        let h = hash_range(5);
        epoch_manager
            .record_block_info(
                block_info(
                    h[0],
                    0,
                    0,
                    Default::default(),
                    Default::default(),
                    h[0],
                    vec![],
                    total_supply,
                ),
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true, true, true], total_supply),
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                block_info(h[3], 3, 3, h[1], h[1], h[2], vec![true, true, true], total_supply),
                rng_seed,
            )
            .unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap(),
            &epoch_info(
                2,
                vec![("test2", stake_amount), ("test3", stake_amount)],
                vec![0, 1, 0],
                vec![vec![0], vec![1], vec![0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", stake_amount), ("test3", stake_amount)]),
                vec![(
                    "test1",
                    ValidatorKickoutReason::NotEnoughBlocks { produced: 0, expected: 1 }
                )],
                reward(vec![("test2", 0), ("test3", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_expected_chunks_prev_block_not_produced() {
        let stake_amount = 1_000_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let epoch_length = 3;
        let total_supply = stake_amount * validators.len() as u128;
        let mut epoch_manager = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            3,
            0,
            90,
            90,
            0,
            default_reward_calculator(),
        );
        let rng_seed = [0; 32];
        let h = hash_range(5);
        record_block(&mut epoch_manager, Default::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![]);

        epoch_manager
            .record_block_info(
                block_info(h[3], 3, 1, h[1], h[1], h[1], vec![false], total_supply),
                rng_seed,
            )
            .unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap(),
            &epoch_info(
                2,
                vec![("test2", stake_amount), ("test3", stake_amount)],
                vec![0, 1, 0],
                vec![vec![0, 1, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", stake_amount), ("test3", stake_amount)]),
                vec![(
                    "test1",
                    ValidatorKickoutReason::NotEnoughBlocks { produced: 0, expected: 1 }
                )],
                reward(vec![("test2", 0), ("test3", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_epoch_info_aggregator() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 5;
        let mut em = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            2,
            0,
            10,
            10,
            0,
            default_reward_calculator(),
        );
        let h = hash_range(6);
        record_block(&mut em, Default::default(), h[0], 0, vec![]);
        record_block_with_final_block_hash(&mut em, h[0], h[1], h[0], 1, vec![]);
        record_block_with_final_block_hash(&mut em, h[1], h[3], h[0], 3, vec![]);
        let epoch_id = em.get_epoch_id(&h[3]).unwrap();

        let aggregator = em.get_and_update_epoch_info_aggregator(&epoch_id, &h[3], true).unwrap();
        assert_eq!(
            aggregator.block_tracker,
            vec![
                (0, ValidatorStats { produced: 2, expected: 2 }),
                (1, ValidatorStats { produced: 0, expected: 1 })
            ]
            .into_iter()
            .collect()
        );

        record_block_with_final_block_hash(&mut em, h[3], h[5], h[1], 5, vec![]);
        let aggregator = em.get_and_update_epoch_info_aggregator(&epoch_id, &h[5], true).unwrap();
        assert_eq!(
            aggregator.block_tracker,
            vec![
                (0, ValidatorStats { produced: 3, expected: 3 }),
                (1, ValidatorStats { produced: 0, expected: 2 })
            ]
            .into_iter()
            .collect()
        );
    }

    /// If the node stops and restarts, the aggregator should be able to recover
    #[test]
    fn test_epoch_info_aggregator_data_loss() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 5;
        let mut em = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            2,
            0,
            10,
            10,
            0,
            default_reward_calculator(),
        );
        let h = hash_range(6);
        record_block(&mut em, Default::default(), h[0], 0, vec![]);
        record_block(&mut em, h[0], h[1], 1, vec![stake("test1", stake_amount - 10)]);
        record_block(&mut em, h[1], h[3], 3, vec![stake("test2", stake_amount + 10)]);
        em.epoch_info_aggregator = None;
        record_block(&mut em, h[3], h[5], 5, vec![stake("test1", stake_amount - 1)]);
        let epoch_id = em.get_epoch_id(&h[5]).unwrap();
        let aggregator = em.get_and_update_epoch_info_aggregator(&epoch_id, &h[5], false).unwrap();
        assert_eq!(
            aggregator.block_tracker,
            vec![
                (0, ValidatorStats { produced: 3, expected: 3 }),
                (1, ValidatorStats { produced: 0, expected: 2 })
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(
            aggregator.all_proposals,
            vec![stake("test1", stake_amount - 1), stake("test2", stake_amount + 10)]
                .into_iter()
                .map(|p| (p.account_id().clone(), p))
                .collect()
        );
    }

    /// Aggregator should still work even if there is a reorg past the last final block.
    #[test]
    fn test_epoch_info_aggregator_reorg_past_final_block() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 6;
        let mut em = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            2,
            0,
            10,
            10,
            0,
            default_reward_calculator(),
        );
        let h = hash_range(6);
        record_block(&mut em, Default::default(), h[0], 0, vec![]);
        record_block_with_final_block_hash(&mut em, h[0], h[1], h[0], 1, vec![]);
        record_block_with_final_block_hash(&mut em, h[1], h[2], h[0], 2, vec![]);
        record_block_with_final_block_hash(
            &mut em,
            h[2],
            h[3],
            h[1],
            3,
            vec![stake("test1", stake_amount - 1)],
        );
        record_block_with_final_block_hash(&mut em, h[3], h[4], h[3], 4, vec![]);
        record_block_with_final_block_hash(&mut em, h[2], h[5], h[1], 5, vec![]);
        let epoch_id = em.get_epoch_id(&h[5]).unwrap();
        let aggregator = em.get_and_update_epoch_info_aggregator(&epoch_id, &h[5], false).unwrap();
        assert_eq!(
            aggregator.block_tracker,
            vec![
                (0, ValidatorStats { produced: 2, expected: 3 }),
                (1, ValidatorStats { produced: 1, expected: 2 })
            ]
            .into_iter()
            .collect()
        );
        assert!(aggregator.all_proposals.is_empty());
    }

    #[test]
    fn test_epoch_info_aggregator_reorg_beginning_of_epoch() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 4;
        let mut em = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            2,
            0,
            10,
            10,
            0,
            default_reward_calculator(),
        );
        let h = hash_range(10);
        record_block(&mut em, Default::default(), h[0], 0, vec![]);
        for i in 1..5 {
            record_block(&mut em, h[i - 1], h[i], i as u64, vec![]);
        }
        record_block(&mut em, h[4], h[5], 5, vec![stake("test1", stake_amount - 1)]);
        record_block_with_final_block_hash(
            &mut em,
            h[5],
            h[6],
            h[4],
            6,
            vec![stake("test2", stake_amount - 100)],
        );
        // reorg
        record_block(&mut em, h[4], h[7], 7, vec![]);
        let epoch_id = em.get_epoch_id(&h[7]).unwrap();
        let aggregator = em.get_and_update_epoch_info_aggregator(&epoch_id, &h[7], true).unwrap();
        assert_eq!(
            aggregator.block_tracker,
            vec![
                (0, ValidatorStats { produced: 1, expected: 2 }),
                (1, ValidatorStats { produced: 0, expected: 1 })
            ]
            .into_iter()
            .collect()
        );
        assert!(aggregator.all_proposals.is_empty());
    }

    #[test]
    fn test_num_missing_blocks() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 2;
        let mut em = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            2,
            0,
            10,
            10,
            0,
            default_reward_calculator(),
        );
        let h = hash_range(8);
        record_block(&mut em, Default::default(), h[0], 0, vec![]);
        record_block(&mut em, h[0], h[1], 1, vec![]);
        record_block(&mut em, h[1], h[3], 3, vec![]);
        let epoch_id = em.get_epoch_id(&h[1]).unwrap();
        assert_eq!(
            em.get_num_validator_blocks(&epoch_id, &h[3], &"test1".to_string()).unwrap(),
            ValidatorStats { produced: 2, expected: 2 }
        );
        assert_eq!(
            em.get_num_validator_blocks(&epoch_id, &h[3], &"test2".to_string()).unwrap(),
            ValidatorStats { produced: 0, expected: 1 }
        );

        // Build chain 0 <- x <- x <- x <- ( 4 <- 5 ) <- x <- 7
        record_block(&mut em, h[0], h[4], 4, vec![]);
        let epoch_id = em.get_epoch_id(&h[4]).unwrap();
        // Block 4 is first block after genesis and starts new epoch, but we actually count how many missed blocks have happened since block 0.
        assert_eq!(
            em.get_num_validator_blocks(&epoch_id, &h[4], &"test1".to_string()).unwrap(),
            ValidatorStats { produced: 0, expected: 2 }
        );
        assert_eq!(
            em.get_num_validator_blocks(&epoch_id, &h[4], &"test2".to_string()).unwrap(),
            ValidatorStats { produced: 1, expected: 2 }
        );
        record_block(&mut em, h[4], h[5], 5, vec![]);
        record_block(&mut em, h[5], h[7], 7, vec![]);
        let epoch_id = em.get_epoch_id(&h[7]).unwrap();
        // The next epoch started after 5 with 6, and test2 missed their slot from perspective of block 7.
        assert_eq!(
            em.get_num_validator_blocks(&epoch_id, &h[7], &"test2".to_string()).unwrap(),
            ValidatorStats { produced: 0, expected: 1 }
        );
    }

    /// Test when blocks are all produced, validators can be kicked out because of not producing
    /// enough chunks
    #[test]
    fn test_chunk_validator_kickout() {
        let stake_amount = 1_000_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let epoch_length = 2;
        let total_supply = stake_amount * validators.len() as u128;
        let mut em = setup_epoch_manager(
            validators,
            epoch_length,
            4,
            2,
            0,
            90,
            70,
            0,
            default_reward_calculator(),
        );
        let rng_seed = [0; 32];
        let h = hash_range(5);
        record_block(&mut em, Default::default(), h[0], 0, vec![]);
        em.record_block_info(
            block_info(h[1], 1, 1, h[0], h[0], h[1], vec![true, true, true, false], total_supply),
            rng_seed,
        )
        .unwrap();
        em.record_block_info(
            block_info(h[2], 2, 2, h[1], h[1], h[1], vec![true, true, true, false], total_supply),
            rng_seed,
        )
        .unwrap();
        em.record_block_info(
            block_info(h[3], 3, 3, h[2], h[2], h[3], vec![true, true, true, true], total_supply),
            rng_seed,
        )
        .unwrap();
        assert_eq!(
            em.get_epoch_info(&EpochId(h[2])).unwrap(),
            &epoch_info(
                2,
                vec![("test2", stake_amount)],
                vec![0, 0],
                vec![vec![0], vec![0], vec![0], vec![0]],
                vec![],
                vec![],
                change_stake(vec![("test1", 0), ("test2", stake_amount)]),
                vec![(
                    "test1",
                    ValidatorKickoutReason::NotEnoughChunks { produced: 2, expected: 4 }
                )],
                reward(vec![("test2", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_compare_epoch_id() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
        let h = hash_range(8);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        // test1 unstakes in epoch 1, and should be kicked out in epoch 3 (validators stored at h2).
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 0)]);
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![stake("test1", amount_staked)]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        let epoch_id0 = epoch_manager.get_epoch_id(&h[0]).unwrap();
        let epoch_id1 = epoch_manager.get_epoch_id(&h[1]).unwrap();
        let epoch_id2 = epoch_manager.get_next_epoch_id(&h[1]).unwrap();
        let epoch_id3 = epoch_manager.get_next_epoch_id(&h[3]).unwrap();
        assert_eq!(epoch_manager.compare_epoch_id(&epoch_id0, &epoch_id1), Ok(Ordering::Equal));
        assert_eq!(epoch_manager.compare_epoch_id(&epoch_id2, &epoch_id3), Ok(Ordering::Less));
        assert_eq!(epoch_manager.compare_epoch_id(&epoch_id3, &epoch_id1), Ok(Ordering::Greater));
        let random_epoch_id = EpochId(hash(&[100]));
        assert!(epoch_manager.compare_epoch_id(&epoch_id3, &random_epoch_id).is_err());
    }

    #[test]
    fn test_fishermen() {
        let stake_amount = 1_000_000;
        let fishermen_threshold = 100;
        let validators = vec![
            ("test1", stake_amount),
            ("test2", stake_amount),
            ("test3", fishermen_threshold),
            ("test4", fishermen_threshold / 2),
        ];
        let epoch_length = 4;
        let mut em = setup_epoch_manager(
            validators,
            epoch_length,
            1,
            4,
            0,
            90,
            70,
            fishermen_threshold,
            default_reward_calculator(),
        );
        let mut epoch_info = epoch_info(
            1,
            vec![("test1", stake_amount), ("test2", stake_amount)],
            vec![0, 1, 0, 1],
            vec![vec![0, 1, 0, 1]],
            vec![],
            vec![("test3", fishermen_threshold)],
            change_stake(vec![
                ("test1", stake_amount),
                ("test2", stake_amount),
                ("test3", fishermen_threshold),
                ("test4", 0),
            ]),
            vec![],
            reward(vec![("near", 0)]),
            0,
        );
        #[cfg(feature = "protocol_feature_block_header_v3")]
        match &mut epoch_info {
            EpochInfo::V1(info) => info.validator_kickout = HashMap::default(),
            EpochInfo::V2(info) => info.validator_kickout = HashMap::default(),
        }
        #[cfg(not(feature = "protocol_feature_block_header_v3"))]
        {
            epoch_info.validator_kickout = HashMap::default();
        }
        assert_eq!(em.get_epoch_info(&EpochId::default()).unwrap(), &epoch_info)
    }

    #[test]
    fn test_fishermen_unstake() {
        let stake_amount = 1_000;
        let fishermen_threshold = 100;
        let validators = vec![
            ("test1", stake_amount),
            ("test2", fishermen_threshold),
            ("test3", fishermen_threshold),
        ];
        let mut em = setup_epoch_manager(
            validators,
            2,
            1,
            1,
            0,
            90,
            70,
            fishermen_threshold,
            default_reward_calculator(),
        );
        let h = hash_range(5);
        record_block(&mut em, CryptoHash::default(), h[0], 0, vec![]);
        // fishermen unstake
        record_block(&mut em, h[0], h[1], 1, vec![stake("test2", 0)]);
        record_block(&mut em, h[1], h[2], 2, vec![stake("test3", 1)]);
        assert_eq!(
            em.get_epoch_info(&EpochId(h[2])).unwrap(),
            &epoch_info(
                2,
                vec![("test1", stake_amount)],
                vec![0],
                vec![vec![0]],
                vec![],
                vec![],
                change_stake(vec![("test1", stake_amount), ("test2", 0), ("test3", 0)]),
                vec![
                    ("test2", ValidatorKickoutReason::Unstaked),
                    ("test3", ValidatorKickoutReason::NotEnoughStake { stake: 1, threshold: 1000 })
                ],
                reward(vec![("test1", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_validator_consistency() {
        let stake_amount = 1_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 1, 0, 90, 60);
        let h = hash_range(5);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        let epoch_id = epoch_manager.get_epoch_id(&h[0]).unwrap();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id).unwrap();
        let mut actual_block_producers = HashSet::new();
        for index in epoch_info.block_producers_settlement().into_iter() {
            let bp = epoch_info.validator_account_id(*index).clone();
            actual_block_producers.insert(bp);
        }
        for index in epoch_info.chunk_producers_settlement().into_iter().flatten() {
            let bp = epoch_info.validator_account_id(*index).clone();
            actual_block_producers.insert(bp);
        }
        for bp in actual_block_producers {
            assert!(epoch_info.account_is_validator(&bp))
        }
    }

    #[test]
    fn test_validator_consistency_not_all_same_stake() {
        let stake_amount1 = 1_000;
        let stake_amount2 = 500;
        let validators =
            vec![("test1", stake_amount1), ("test2", stake_amount2), ("test3", stake_amount2)];
        // have two seats to that 500 would be the threshold
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
        let h = hash_range(5);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        let epoch_id = epoch_manager.get_epoch_id(&h[0]).unwrap();
        let epoch_info1 = epoch_manager.get_epoch_info(&epoch_id).unwrap();
        assert_eq!(
            epoch_info1,
            &epoch_info(
                1,
                vec![("test1", stake_amount1), ("test3", stake_amount2)],
                vec![0, 1],
                vec![vec![0, 1]],
                vec![],
                vec![("test2", stake_amount2)],
                change_stake(vec![
                    ("test1", stake_amount1),
                    ("test2", stake_amount2),
                    ("test3", stake_amount2)
                ]),
                vec![],
                reward(vec![("near", 0)]),
                0,
            )
        );
    }

    /// Test that when epoch length is larger than the cache size of block info cache, there is
    /// no unexpected error.
    #[test]
    fn test_finalize_epoch_large_epoch_length() {
        let stake_amount = 1_000;
        let validators = vec![("test1", stake_amount), ("test2", stake_amount)];
        let mut epoch_manager =
            setup_default_epoch_manager(validators, (BLOCK_CACHE_SIZE + 1) as u64, 1, 2, 0, 90, 60);
        let h = hash_range(BLOCK_CACHE_SIZE + 2);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        for i in 1..=(BLOCK_CACHE_SIZE + 1) {
            record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
        }
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[BLOCK_CACHE_SIZE + 1])).unwrap(),
            &epoch_info(
                2,
                vec![("test1", stake_amount), ("test2", stake_amount)],
                vec![1, 0],
                vec![vec![1, 0]],
                vec![],
                vec![],
                change_stake(vec![("test1", stake_amount), ("test2", stake_amount)]),
                vec![],
                reward(vec![("near", 0), ("test1", 0), ("test2", 0)]),
                0,
            )
        );
    }

    #[test]
    fn test_kickout_set() {
        let stake_amount = 1_000;
        let validators = vec![("test1", stake_amount), ("test2", 0), ("test3", 10)];
        // have two seats to that 500 would be the threshold
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90, 60);
        let h = hash_range(5);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test2", stake_amount)]);
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![stake("test2", 0)]);
        let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap();
        assert_eq!(
            epoch_info1.validators_iter().map(|r| r.account_id().clone()).collect::<Vec<_>>(),
            vec!["test1".to_string()]
        );
        assert_eq!(
            epoch_info1.stake_change().clone(),
            change_stake(vec![("test1", stake_amount), ("test2", 0), ("test3", 10)])
        );
        assert!(epoch_info1.validator_kickout().is_empty());
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![stake("test2", stake_amount)]);
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap(),
            &epoch_info(
                3,
                vec![("test1", stake_amount), ("test2", stake_amount)],
                vec![1, 0],
                vec![vec![1, 0]],
                vec![],
                vec![("test3", 10)],
                change_stake(vec![("test1", stake_amount), ("test2", stake_amount), ("test3", 10)]),
                vec![],
                reward(vec![("near", 0), ("test1", 0)]),
                0,
            )
        )
    }

    #[test]
    fn test_epoch_height_increase() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(5);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[2], 2, vec![stake("test1", 223)]);
        record_block(&mut epoch_manager, h[2], h[4], 4, vec![]);

        let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().clone();
        let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().clone();
        assert_ne!(epoch_info2.epoch_height(), epoch_info3.epoch_height());
    }

    #[test]
    /// Slashed after unstaking: slashed for 2 epochs
    fn test_unstake_slash() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(9);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 0)]);
        record_block_with_slashes(
            &mut epoch_manager,
            h[1],
            h[2],
            2,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), false)],
        );
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![stake("test1", stake_amount)]);

        let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap().clone();
        let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().clone();
        let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap().clone();
        let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().clone();
        assert_eq!(
            epoch_info1.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Unstaked)
        );
        assert_eq!(
            epoch_info2.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Slashed)
        );
        assert_eq!(
            epoch_info3.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Slashed)
        );
        assert!(epoch_info4.validator_kickout().is_empty());
        assert!(epoch_info4.account_is_validator("test1"));
    }

    #[test]
    /// Slashed with no unstake in previous epoch: slashed for 3 epochs
    fn test_no_unstake_slash() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(9);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block_with_slashes(
            &mut epoch_manager,
            h[0],
            h[1],
            1,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), false)],
        );
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![stake("test1", stake_amount)]);

        let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap().clone();
        let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().clone();
        let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap().clone();
        let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().clone();
        assert_eq!(
            epoch_info1.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Slashed)
        );
        assert_eq!(
            epoch_info2.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Slashed)
        );
        assert_eq!(
            epoch_info3.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Slashed)
        );
        assert!(epoch_info4.validator_kickout().is_empty());
        assert!(epoch_info4.account_is_validator("test1"));
    }

    #[test]
    /// Slashed right after validator rotated out
    fn test_slash_non_validator() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(9);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 0)]);
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        record_block_with_slashes(
            &mut epoch_manager,
            h[2],
            h[3],
            3,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), false)],
        );
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![]);
        record_block(&mut epoch_manager, h[4], h[5], 5, vec![stake("test1", stake_amount)]);

        let epoch_info1 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap().clone(); // Unstaked
        let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().clone(); // -
        let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap().clone(); // Slashed
        let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().clone(); // Slashed
        let epoch_info5 = epoch_manager.get_epoch_info(&EpochId(h[5])).unwrap().clone(); // Ok
        assert_eq!(
            epoch_info1.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Unstaked)
        );
        assert!(epoch_info2.validator_kickout().is_empty());
        assert_eq!(
            epoch_info3.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Slashed)
        );
        assert_eq!(
            epoch_info4.validator_kickout().get("test1"),
            Some(&ValidatorKickoutReason::Slashed)
        );
        assert!(epoch_info5.validator_kickout().is_empty());
        assert!(epoch_info5.account_is_validator("test1"));
    }

    #[test]
    /// Slashed and attempt to restake: proposal gets ignored
    fn test_slash_restake() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(9);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block_with_slashes(
            &mut epoch_manager,
            h[0],
            h[1],
            1,
            vec![],
            vec![SlashedValidator::new("test1".to_string(), false)],
        );
        record_block(&mut epoch_manager, h[1], h[2], 2, vec![stake("test1", stake_amount)]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        record_block(&mut epoch_manager, h[3], h[4], 4, vec![stake("test1", stake_amount)]);
        let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().clone();
        assert!(epoch_info2.stake_change().get("test1").is_none());
        let epoch_info4 = epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().clone();
        assert!(epoch_info4.stake_change().get("test1").is_some());
    }

    #[test]
    fn test_all_kickout_edge_case() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(9);
        // 1. kickout test2
        // 2. kickout test1 and test2
        // 3. test1 produces a block and test3 misses, but since test1 is about to get kicked we keep test3
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[2], 2, vec![]);
        record_block(&mut epoch_manager, h[2], h[6], 6, vec![]);
        record_block(&mut epoch_manager, h[6], h[8], 8, vec![]);

        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[8])).unwrap().validator_kickout(),
            &HashMap::default()
        );
    }

    #[test]
    fn test_fisherman_kickout() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 3, 0, 90, 60);
        let h = hash_range(6);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![stake("test1", 148)]);
        // test1 starts as validator,
        // - reduces stake in epoch T, will be fisherman in epoch T+2
        // - Misses a block in epoch T+1, will be kicked out in epoch T+3
        // - Finalize epoch T+1 => T+3 kicks test1 as fisherman without a record in stake_change
        record_block(&mut epoch_manager, h[1], h[3], 3, vec![]);

        let epoch_info2 = epoch_manager.get_epoch_info(&EpochId(h[1])).unwrap().clone();
        let epoch_info3 = epoch_manager.get_epoch_info(&EpochId(h[3])).unwrap().clone();
        assert_eq!(
            epoch_info2,
            epoch_info(
                2,
                vec![("test2", stake_amount), ("test3", stake_amount)],
                vec![0, 1, 0],
                vec![vec![0, 1, 0]],
                vec![],
                vec![("test1", 148)],
                change_stake(vec![
                    ("test1", 148),
                    ("test2", stake_amount),
                    ("test3", stake_amount)
                ]),
                vec![],
                reward(vec![("near", 0), ("test1", 0), ("test2", 0), ("test3", 0)]),
                0,
            )
        );
        assert_eq!(
            epoch_info3,
            epoch_info(
                3,
                vec![("test2", stake_amount), ("test3", stake_amount)],
                vec![0, 1, 0],
                vec![vec![0, 1, 0]],
                vec![],
                vec![],
                change_stake(vec![("test2", stake_amount), ("test3", stake_amount), ("test1", 0)]),
                vec![("test1", NotEnoughBlocks { produced: 0, expected: 1 })],
                reward(vec![("near", 0), ("test2", 0), ("test3", 0)]),
                0,
            )
        );
    }

    #[cfg(feature = "protocol_feature_block_header_v3")]
    fn set_block_info_protocol_version(info: &mut BlockInfo, protocol_version: ProtocolVersion) {
        match info {
            BlockInfo::V1(v1) => v1.latest_protocol_version = protocol_version,
            BlockInfo::V2(v2) => v2.latest_protocol_version = protocol_version,
        }
    }

    #[cfg(not(feature = "protocol_feature_block_header_v3"))]
    fn set_block_info_protocol_version(info: &mut BlockInfo, protocol_version: ProtocolVersion) {
        info.latest_protocol_version = protocol_version;
    }

    #[test]
    fn test_protocol_version_switch() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            0,
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();
        let h = hash_range(8);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        let mut block_info1 =
            block_info(h[1], 1, 1, h[0], h[0], h[0], vec![], DEFAULT_TOTAL_SUPPLY);
        set_block_info_protocol_version(&mut block_info1, 0);
        epoch_manager.record_block_info(block_info1, [0; 32]).unwrap();
        for i in 2..6 {
            record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
        }
        assert_eq!(epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().protocol_version(), 0);
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().protocol_version(),
            PROTOCOL_VERSION
        );
    }

    #[test]
    fn test_protocol_version_switch_with_many_seats() {
        let store = create_test_store();
        let mut config = epoch_config(10, 1, 4, 0, 90, 60, 0);
        config.num_block_producer_seats_per_shard = vec![10];
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked / 5)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            0,
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();
        let h = hash_range(50);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        let mut block_info1 =
            block_info(h[1], 1, 1, h[0], h[0], h[0], vec![], DEFAULT_TOTAL_SUPPLY);
        set_block_info_protocol_version(&mut block_info1, 0);
        epoch_manager.record_block_info(block_info1, [0; 32]).unwrap();
        for i in 2..32 {
            record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
        }
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[10])).unwrap().protocol_version(),
            PROTOCOL_VERSION
        );
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[20])).unwrap().protocol_version(),
            PROTOCOL_VERSION
        );
    }

    #[test]
    fn test_protocol_version_switch_after_switch() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            UPGRADABILITY_FIX_PROTOCOL_VERSION,
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();
        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        for i in 1..5 {
            let mut block_info = block_info(
                h[i],
                i as u64,
                i as u64 - 1,
                h[i - 1],
                h[i - 1],
                h[0],
                vec![],
                DEFAULT_TOTAL_SUPPLY,
            );
            if i != 4 {
                set_block_info_protocol_version(
                    &mut block_info,
                    UPGRADABILITY_FIX_PROTOCOL_VERSION + 1,
                );
            } else {
                set_block_info_protocol_version(
                    &mut block_info,
                    UPGRADABILITY_FIX_PROTOCOL_VERSION,
                );
            }
            epoch_manager.record_block_info(block_info, [0; 32]).unwrap();
        }

        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap().protocol_version(),
            UPGRADABILITY_FIX_PROTOCOL_VERSION + 1
        );

        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[4])).unwrap().protocol_version(),
            UPGRADABILITY_FIX_PROTOCOL_VERSION + 1
        );

        // if there are enough votes to use the old version, it should be allowed
        for i in 5..7 {
            let mut block_info = block_info(
                h[i],
                i as u64,
                i as u64 - 1,
                h[i - 1],
                h[i - 1],
                h[0],
                vec![],
                DEFAULT_TOTAL_SUPPLY,
            );
            set_block_info_protocol_version(&mut block_info, UPGRADABILITY_FIX_PROTOCOL_VERSION);
            epoch_manager.record_block_info(block_info, [0; 32]).unwrap();
        }
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[6])).unwrap().protocol_version(),
            UPGRADABILITY_FIX_PROTOCOL_VERSION
        );
    }

    /// Epoch aggregator should not need to be recomputed under the following scenario
    ///                      /-----------h+2
    /// h-2 ---- h-1 ------ h
    ///                      \------h+1
    /// even though from the perspective of h+2 the last final block is h-2.
    #[test]
    fn test_final_block_consistency() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 10, 1, 3, 0, 90, 60);

        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        for i in 1..5 {
            record_block_with_final_block_hash(
                &mut epoch_manager,
                h[i - 1],
                h[i],
                if i == 1 { CryptoHash::default() } else { h[i - 2] },
                i as u64,
                vec![],
            );
        }

        let epoch_aggregator_final_hash =
            epoch_manager.epoch_info_aggregator.as_ref().map(|a| a.last_block_hash).unwrap();

        epoch_manager
            .record_block_info(
                block_info(h[5], 5, 1, h[1], h[2], h[1], vec![], DEFAULT_TOTAL_SUPPLY),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
        let new_epoch_aggregator_final_hash =
            epoch_manager.epoch_info_aggregator.as_ref().map(|a| a.last_block_hash).unwrap();
        assert_eq!(epoch_aggregator_final_hash, new_epoch_aggregator_final_hash);
    }

    #[test]
    fn test_epoch_validators_cache() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 10, 0, 90, 60);
        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        for i in 1..4 {
            record_block(&mut epoch_manager, h[i - 1], h[i], i as u64, vec![]);
        }
        assert_eq!(epoch_manager.epoch_validators_ordered.cache_size(), 0);

        let epoch_id = EpochId(h[2]);
        let epoch_validators =
            epoch_manager.get_all_block_producers_settlement(&epoch_id, &h[3]).unwrap().to_vec();
        assert_eq!(epoch_manager.epoch_validators_ordered.cache_size(), 1);
        let epoch_validators_in_cache =
            epoch_manager.epoch_validators_ordered.cache_get(&epoch_id).unwrap().clone();
        assert_eq!(epoch_validators, epoch_validators_in_cache);

        assert_eq!(epoch_manager.epoch_validators_ordered_unique.cache_size(), 0);
        let epoch_validators_unique =
            epoch_manager.get_all_block_producers_ordered(&epoch_id, &h[3]).unwrap().to_vec();
        let epoch_validators_unique_in_cache =
            epoch_manager.epoch_validators_ordered_unique.cache_get(&epoch_id).unwrap().clone();
        assert_eq!(epoch_validators_unique, epoch_validators_unique_in_cache);
    }
}
