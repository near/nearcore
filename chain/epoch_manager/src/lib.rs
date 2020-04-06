use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use cached::{Cached, SizedCache};
use ethereum_types::U256;
use log::{debug, warn};

use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, Balance, BlockChunkValidatorStats, BlockHeight, EpochId, ShardId, ValidatorId,
    ValidatorKickoutReason, ValidatorStake, ValidatorStats,
};
use near_primitives::views::{
    CurrentEpochValidatorInfo, EpochValidatorInfo, NextEpochValidatorInfo, ValidatorKickoutView,
};
use near_store::{ColBlockInfo, ColEpochInfo, ColEpochStart, Store, StoreUpdate};

use crate::proposals::proposals_to_epoch_info;
pub use crate::reward_calculator::RewardCalculator;
use crate::types::EpochError::EpochOutOfBounds;
pub use crate::types::{BlockInfo, EpochConfig, EpochError, EpochInfo, RngSeed};
use crate::types::{EpochSummary, SlashState};

mod proposals;
mod reward_calculator;
pub mod test_utils;
mod types;

const EPOCH_CACHE_SIZE: usize = 10;
const BLOCK_CACHE_SIZE: usize = 1000;

/// Tracks epoch information across different forks, such as validators.
/// Note: that even after garbage collection, the data about genesis epoch should be in the store.
pub struct EpochManager {
    store: Arc<Store>,
    /// Current epoch config.
    /// TODO: must be dynamically changing over time, so there should be a way to change it.
    config: EpochConfig,
    reward_calculator: RewardCalculator,

    /// Cache of epoch information.
    epochs_info: SizedCache<EpochId, EpochInfo>,
    /// Cache of block information.
    blocks_info: SizedCache<CryptoHash, BlockInfo>,
    /// Cache of epoch id to epoch start height
    epoch_id_to_start: SizedCache<EpochId, BlockHeight>,
}

impl EpochManager {
    pub fn new(
        store: Arc<Store>,
        config: EpochConfig,
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
            epochs_info: SizedCache::with_size(EPOCH_CACHE_SIZE),
            blocks_info: SizedCache::with_size(BLOCK_CACHE_SIZE),
            epoch_id_to_start: SizedCache::with_size(EPOCH_CACHE_SIZE),
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
            )?;
            // Dummy block info.
            // Artificial block we add to simplify implementation: dummy block is the
            // parent of genesis block that points to itself.
            // If we view it as block in epoch -1 and height -1, it naturally extends the
            // EpochId formula using T-2 for T=1, and height field is unused.
            let block_info = BlockInfo::default();
            let mut store_update = epoch_manager.store.store_update();
            epoch_manager.save_epoch_info(&mut store_update, &genesis_epoch_id, epoch_info)?;
            epoch_manager.save_block_info(&mut store_update, &CryptoHash::default(), block_info)?;
            store_update.commit()?;
        }
        Ok(epoch_manager)
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
        let mut max_validator_id = None;
        let block_producer_kickout_threshold = self.config.block_producer_kickout_threshold;
        let chunk_producer_kickout_threshold = self.config.chunk_producer_kickout_threshold;
        let mut validator_block_chunk_stats = HashMap::new();
        let mut validator_kickout = HashMap::new();

        for (i, _) in epoch_info.validators.iter().enumerate() {
            let account_id = epoch_info.validators[i].account_id.clone();
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
                validator_kickout.insert(
                    account_id.clone(),
                    ValidatorKickoutReason::NotEnoughChunks {
                        produced: chunk_stats.produced,
                        expected: chunk_stats.expected,
                    },
                );
            }

            // Given the number of blocks we plan to have in one epoch, the following code should not overflow
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
            if block_stats.produced > maximum_block_prod && !is_already_kicked_out {
                maximum_block_prod = block_stats.produced;
                max_validator_id = Some(i);
            }
        }
        if all_kicked_out {
            if let Some(validator_id) = max_validator_id {
                validator_kickout.remove(&epoch_info.validators[validator_id].account_id);
            }
        }
        (validator_kickout, validator_block_chunk_stats)
    }

    fn collect_blocks_info(
        &mut self,
        last_block_info: &BlockInfo,
        last_block_hash: &CryptoHash,
    ) -> Result<EpochSummary, EpochError> {
        let epoch_info = self.get_epoch_info(&last_block_info.epoch_id)?.clone();
        let next_epoch_id = self.get_next_epoch_id(&last_block_hash)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?.clone();
        let mut proposals = BTreeMap::new();
        let mut validator_kickout = HashMap::new();
        let block_validator_tracker = last_block_info.block_tracker.clone();
        let chunk_validator_tracker = last_block_info.shard_tracker.clone();
        let total_validator_reward = last_block_info.total_validator_reward;

        // Gather slashed validators and add them to kick out first.
        let slashed_validators = last_block_info.slashed.clone();
        for (account_id, _) in slashed_validators.iter() {
            validator_kickout.insert(account_id.clone(), ValidatorKickoutReason::Slashed);
        }

        for proposal in last_block_info.all_proposals.iter().rev() {
            if !slashed_validators.contains_key(&proposal.account_id) {
                if proposal.stake == 0
                    && !proposals.contains_key(&proposal.account_id)
                    && *next_epoch_info.stake_change.get(&proposal.account_id).unwrap_or(&0) != 0
                {
                    validator_kickout
                        .insert(proposal.account_id.clone(), ValidatorKickoutReason::Unstaked);
                }
                // This code relies on the fact that within a block the proposals are ordered
                // in the order they are added. So we only take the last proposal for any given
                // account in this manner.
                proposals.entry(proposal.account_id.clone()).or_insert(proposal.clone());
            }
        }

        let all_proposals: Vec<_> = proposals.into_iter().map(|(_, v)| v).collect();

        let prev_epoch_last_block_hash =
            self.get_block_info(&last_block_info.epoch_first_block)?.prev_hash;
        let prev_validator_kickout = next_epoch_info.validator_kickout;

        // Compute kick outs for validators who are offline.
        let (kickout, validator_block_chunk_stats) = self.compute_kickout_info(
            &epoch_info,
            &block_validator_tracker,
            &chunk_validator_tracker,
            &slashed_validators,
            &prev_validator_kickout,
        );
        validator_kickout.extend(kickout);
        debug!(
            "All proposals: {:?}, Kickouts: {:?}, Block Tracker: {:?}, Shard Tracker: {:?}",
            all_proposals, validator_kickout, block_validator_tracker, chunk_validator_tracker
        );

        Ok(EpochSummary {
            prev_epoch_last_block_hash,
            all_proposals,
            validator_kickout,
            validator_block_chunk_stats,
            total_validator_reward,
        })
    }

    /// Returns number of produced and expected blocks by given validator.
    pub fn get_num_validator_blocks(
        &mut self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<ValidatorStats, EpochError> {
        let epoch_info = self.get_epoch_info(&epoch_id)?;
        let validator_id = *epoch_info.validator_to_index.get(account_id).ok_or_else(|| {
            EpochError::Other(format!("{} is not a validator in epoch {:?}", account_id, epoch_id))
        })?;
        let block_info = self.get_block_info(last_known_block_hash)?.clone();
        let validator_stats = block_info
            .block_tracker
            .get(&validator_id)
            .unwrap_or_else(|| &ValidatorStats { produced: 0, expected: 0 });
        assert!(validator_stats.expected >= validator_stats.produced);
        Ok(validator_stats.clone())
    }

    /// Finalizes epoch (T), where given last block hash is given, and returns next next epoch id (T + 2).
    fn finalize_epoch(
        &mut self,
        store_update: &mut StoreUpdate,
        block_info: &BlockInfo,
        last_block_hash: &CryptoHash,
        rng_seed: RngSeed,
    ) -> Result<EpochId, EpochError> {
        let EpochSummary {
            prev_epoch_last_block_hash,
            all_proposals,
            validator_kickout,
            validator_block_chunk_stats,
            total_validator_reward,
        } = self.collect_blocks_info(&block_info, last_block_hash)?;
        let epoch_id = self.get_epoch_id(last_block_hash)?;
        let epoch_info = self.get_epoch_info(&epoch_id)?;
        let validator_stake = epoch_info
            .validators
            .clone()
            .into_iter()
            .map(|r| (r.account_id, r.stake))
            .collect::<HashMap<_, _>>();
        let next_epoch_id = self.get_next_epoch_id(last_block_hash)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?.clone();
        let (validator_reward, inflation) = self.reward_calculator.calculate_reward(
            validator_block_chunk_stats,
            &validator_stake,
            total_validator_reward,
            block_info.total_supply,
        );
        let next_next_epoch_info = match proposals_to_epoch_info(
            &self.config,
            rng_seed,
            &next_epoch_info,
            all_proposals,
            validator_kickout,
            validator_reward,
            inflation,
        ) {
            Ok(next_next_epoch_info) => next_next_epoch_info,
            Err(EpochError::ThresholdError(amount, num_seats)) => {
                warn!(target: "epoch_manager", "Not enough stake for required number of seats (all validators tried to unstake?): amount = {} for {}", amount, num_seats);
                let mut epoch_info = next_epoch_info.clone();
                epoch_info.epoch_height += 1;
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
        current_hash: &CryptoHash,
        mut block_info: BlockInfo,
        rng_seed: RngSeed,
    ) -> Result<StoreUpdate, EpochError> {
        let mut store_update = self.store.store_update();
        // Check that we didn't record this block yet.
        if !self.has_block_info(current_hash)? {
            if block_info.prev_hash == CryptoHash::default() {
                // This is genesis block, we special case as new epoch.
                assert_eq!(block_info.proposals.len(), 0);
                let pre_genesis_epoch_id = EpochId::default();
                let genesis_epoch_info = self.get_epoch_info(&pre_genesis_epoch_id)?.clone();
                self.save_block_info(&mut store_update, current_hash, block_info.clone())?;
                self.save_epoch_info(
                    &mut store_update,
                    &EpochId(*current_hash),
                    genesis_epoch_info,
                )?;
            } else {
                let prev_block_info = self.get_block_info(&block_info.prev_hash)?.clone();
                let epoch_info = self.get_epoch_info(&prev_block_info.epoch_id)?.clone();

                let mut is_epoch_start = false;
                if prev_block_info.prev_hash == CryptoHash::default() {
                    // This is first real block, starts the new epoch.
                    block_info.epoch_id = EpochId::default();
                    block_info.epoch_first_block = *current_hash;
                    is_epoch_start = true;
                } else if self.is_next_block_in_next_epoch(&prev_block_info)? {
                    // Current block is in the new epoch, finalize the one in prev_block.
                    block_info.epoch_id = self.get_next_epoch_id_from_info(&prev_block_info)?;
                    block_info.epoch_first_block = *current_hash;
                    is_epoch_start = true;
                } else {
                    // Same epoch as parent, copy epoch_id and epoch_start_height.
                    block_info.epoch_id = prev_block_info.epoch_id;
                    block_info.epoch_first_block = prev_block_info.epoch_first_block;
                }

                // Keep `slashed` from previous block if they are still in the epoch info stake change
                // (e.g. we need to keep track that they are still slashed, because when we compute
                // returned stake we are skipping account ids that are slashed in `stake_change`).
                for (account_id, slash_state) in prev_block_info.slashed.iter() {
                    if is_epoch_start {
                        if slash_state == &SlashState::DoubleSign
                            || slash_state == &SlashState::Other
                        {
                            block_info
                                .slashed
                                .entry(account_id.clone())
                                .or_insert(SlashState::AlreadySlashed);
                        } else if epoch_info.stake_change.contains_key(account_id) {
                            block_info
                                .slashed
                                .entry(account_id.clone())
                                .or_insert(slash_state.clone());
                        }
                    } else {
                        block_info
                            .slashed
                            .entry(account_id.clone())
                            .and_modify(|e| {
                                if let SlashState::Other = slash_state {
                                    *e = SlashState::Other;
                                }
                            })
                            .or_insert(slash_state.clone());
                    }
                }

                let BlockInfo {
                    block_tracker,
                    mut all_proposals,
                    shard_tracker,
                    total_validator_reward,
                    ..
                } = prev_block_info;

                // Update block produced/expected tracker.
                block_info.update_block_tracker(
                    &epoch_info,
                    prev_block_info.height,
                    if is_epoch_start { HashMap::default() } else { block_tracker },
                );
                block_info.update_shard_tracker(
                    &epoch_info,
                    if is_epoch_start { HashMap::default() } else { shard_tracker },
                );
                // accumulate values
                if is_epoch_start {
                    block_info.all_proposals = block_info.proposals.clone();
                    block_info.total_validator_reward = block_info.validator_reward;
                    self.save_epoch_start(
                        &mut store_update,
                        &block_info.epoch_id,
                        block_info.height,
                    )?;
                } else {
                    all_proposals.extend(block_info.proposals.clone());
                    block_info.all_proposals = all_proposals;
                    block_info.total_validator_reward +=
                        total_validator_reward + block_info.validator_reward;
                }

                // Save current block info.
                self.save_block_info(&mut store_update, current_hash, block_info.clone())?;
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
        Ok(epoch_info.validators[validator_id as usize].clone())
    }

    /// Returns settlement of all block producers in current epoch, with indicator on whether they are slashed or not.
    pub fn get_all_block_producers_settlement(
        &mut self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, EpochError> {
        let slashed = self.get_slashed_validators(last_known_block_hash)?.clone();
        let epoch_info = self.get_epoch_info(epoch_id)?;
        let mut settlement = vec![];
        for validator_id in epoch_info.block_producers_settlement.iter() {
            let validator_stake = epoch_info.validators[*validator_id as usize].clone();
            let is_slashed = slashed.contains_key(&validator_stake.account_id);
            settlement.push((validator_stake, is_slashed));
        }
        Ok(settlement)
    }

    /// Returns all unique block producers in current epoch sorted by account_id, with indicator on whether they are slashed or not.
    pub fn get_all_block_producers_ordered(
        &mut self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, EpochError> {
        let settlement =
            self.get_all_block_producers_settlement(epoch_id, last_known_block_hash)?;
        let mut result = vec![];
        let mut validators: HashSet<AccountId> = HashSet::default();
        for (validator_stake, is_slashed) in settlement.into_iter() {
            if !validators.contains(&validator_stake.account_id) {
                validators.insert(validator_stake.account_id.clone());
                result.push((validator_stake, is_slashed));
            }
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
        Ok(epoch_info.validators[validator_id as usize].clone())
    }

    /// Returns validator for given account id for given epoch.
    /// We don't require caller to know about EpochIds. Doesn't account for slashing.
    pub fn get_validator_by_account_id(
        &mut self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<ValidatorStake>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        Ok(epoch_info
            .validator_to_index
            .get(account_id)
            .map(|idx| epoch_info.validators[*idx as usize].clone()))
    }

    /// Returns fisherman for given account id for given epoch.
    pub fn get_fisherman_by_account_id(
        &mut self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<ValidatorStake>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        Ok(epoch_info
            .fishermen_to_index
            .get(account_id)
            .map(|idx| epoch_info.fishermen[*idx as usize].clone()))
    }

    pub fn get_slashed_validators(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&HashMap<AccountId, SlashState>, EpochError> {
        Ok(&self.get_block_info(block_hash)?.slashed)
    }

    pub fn get_epoch_id(&mut self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        Ok(self.get_block_info(block_hash)?.epoch_id.clone())
    }

    pub fn get_next_epoch_id(&mut self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let block_info = self.get_block_info(block_hash)?.clone();
        self.get_next_epoch_id_from_info(&block_info)
    }

    pub fn get_prev_epoch_id(&mut self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let epoch_first_block = self.get_block_info(block_hash)?.epoch_first_block;
        let prev_epoch_last_hash = self.get_block_info(&epoch_first_block)?.prev_hash;
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
        let epoch_first_block = self.get_block_info(block_hash)?.epoch_first_block;
        Ok(self.get_block_info(&epoch_first_block)?.height)
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
        let validator_reward = self.get_epoch_info(&next_next_epoch_id)?.validator_reward.clone();

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
        let prev_prev_stake_change = self.get_epoch_info(&epoch_id)?.stake_change.clone();
        let prev_stake_change = self.get_epoch_info(&next_epoch_id)?.stake_change.clone();
        let stake_change = &self.get_epoch_info(&next_next_epoch_id)?.stake_change;
        debug!(target: "epoch_manager",
            "prev_prev_stake_change: {:?}, prev_stake_change: {:?}, stake_change: {:?}, slashed: {:?}",
            prev_prev_stake_change, prev_stake_change, stake_change, last_block_info.slashed
        );
        let mut all_keys = HashSet::new();
        for (key, _) in
            prev_prev_stake_change.iter().chain(prev_stake_change.iter()).chain(stake_change.iter())
        {
            all_keys.insert(key);
        }
        let mut stake_info = HashMap::new();
        for account_id in all_keys {
            if last_block_info.slashed.contains_key(account_id) {
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
        let total_stake: Balance = epoch_info.validators.iter().map(|v| v.stake).sum();
        let total_slashed_stake: Balance = slashed
            .iter()
            .filter_map(|(account_id, slashed)| match slashed {
                SlashState::DoubleSign => {
                    let idx = epoch_info.validator_to_index.get(account_id);
                    Some(if let Some(&idx) = idx {
                        epoch_info.validators[idx as usize].stake
                    } else {
                        0
                    })
                }
                _ => None,
            })
            .sum();
        let is_totally_slashed = total_slashed_stake * 3 >= total_stake;
        let mut res = HashMap::default();
        for (account_id, slash_state) in slashed {
            if let SlashState::DoubleSign = slash_state {
                if let Some(&idx) = epoch_info.validator_to_index.get(&account_id) {
                    let stake = epoch_info.validators[idx as usize].stake;
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
        block_hash: &CryptoHash,
    ) -> Result<EpochValidatorInfo, EpochError> {
        let epoch_id = self.get_epoch_id(block_hash)?;
        let slashed = self.get_slashed_validators(block_hash)?.clone();
        let cur_epoch_info = self.get_epoch_info(&epoch_id)?.clone();
        let mut validator_to_shard = (0..cur_epoch_info.validators.len())
            .map(|_| HashSet::default())
            .collect::<Vec<HashSet<ShardId>>>();
        for (shard_id, validators) in cur_epoch_info.chunk_producers_settlement.iter().enumerate() {
            for validator_id in validators {
                validator_to_shard[*validator_id as usize].insert(shard_id as ShardId);
            }
        }
        let current_validators = cur_epoch_info
            .validators
            .into_iter()
            .enumerate()
            .map(|(validator_id, info)| {
                let validator_stats =
                    self.get_num_validator_blocks(&epoch_id, &block_hash, &info.account_id)?;
                let mut shards =
                    validator_to_shard[validator_id].clone().into_iter().collect::<Vec<ShardId>>();
                shards.sort();
                Ok(CurrentEpochValidatorInfo {
                    is_slashed: slashed.contains_key(&info.account_id),
                    account_id: info.account_id,
                    public_key: info.public_key,
                    stake: info.stake,
                    shards,
                    num_produced_blocks: validator_stats.produced,
                    num_expected_blocks: validator_stats.expected,
                })
            })
            .collect::<Result<Vec<CurrentEpochValidatorInfo>, EpochError>>()?;
        let current_fishermen = cur_epoch_info.fishermen;
        let next_epoch_id = self.get_next_epoch_id(block_hash)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?;
        let mut next_validator_to_shard = (0..next_epoch_info.validators.len())
            .map(|_| HashSet::default())
            .collect::<Vec<HashSet<ShardId>>>();
        for (shard_id, validators) in next_epoch_info.chunk_producers_settlement.iter().enumerate()
        {
            for validator_id in validators {
                next_validator_to_shard[*validator_id as usize].insert(shard_id as u64);
            }
        }
        let next_validators = next_epoch_info
            .validators
            .iter()
            .enumerate()
            .map(|(validator_id, info)| {
                let mut shards = next_validator_to_shard[validator_id]
                    .clone()
                    .into_iter()
                    .collect::<Vec<ShardId>>();
                shards.sort();
                NextEpochValidatorInfo {
                    account_id: info.account_id.clone(),
                    public_key: info.public_key.clone(),
                    stake: info.stake,
                    shards,
                }
            })
            .collect();
        let next_fishermen = next_epoch_info.fishermen.clone();
        let prev_epoch_kickout = next_epoch_info
            .validator_kickout
            .clone()
            .into_iter()
            .collect::<BTreeMap<_, _>>()
            .into_iter()
            .map(|(account_id, reason)| ValidatorKickoutView { account_id, reason })
            .collect();
        let current_proposals = self.get_block_info(block_hash)?.all_proposals.clone();

        Ok(EpochValidatorInfo {
            current_validators,
            next_validators,
            current_fishermen: current_fishermen.into_iter().map(Into::into).collect(),
            next_fishermen: next_fishermen.into_iter().map(Into::into).collect(),
            current_proposals: current_proposals.into_iter().map(Into::into).collect(),
            prev_epoch_kickout,
        })
    }

    pub fn get_epoch_inflation(&mut self, epoch_id: &EpochId) -> Result<Balance, EpochError> {
        Ok(self.get_epoch_info(epoch_id)?.inflation)
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
            (Err(_), Err(_)) => Err(EpochOutOfBounds),
        }
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
        for validator_id in epoch_info.chunk_producers_settlement[shard_id as usize].iter() {
            if &epoch_info.validators[*validator_id as usize].account_id == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn block_producer_from_info(epoch_info: &EpochInfo, height: BlockHeight) -> ValidatorId {
        epoch_info.block_producers_settlement
            [(height as u64 % (epoch_info.block_producers_settlement.len() as u64)) as usize]
    }

    pub(crate) fn chunk_producer_from_info(
        epoch_info: &EpochInfo,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> ValidatorId {
        epoch_info.chunk_producers_settlement[shard_id as usize][(height as u64
            % (epoch_info.chunk_producers_settlement[shard_id as usize].len() as u64))
            as usize]
    }

    /// The epoch switches when a block at a particular height gets final. We cannot allow blocks
    /// beyond that height in the current epoch to get final, otherwise the safety of the finality
    /// gadget can get violated.
    pub fn push_final_block_back_if_needed(
        &mut self,
        parent_hash: CryptoHash,
        mut last_final_hash: CryptoHash,
    ) -> Result<CryptoHash, EpochError> {
        if last_final_hash == CryptoHash::default() {
            return Ok(last_final_hash);
        }

        let block_info = self.get_block_info(&parent_hash)?;
        let epoch_first_block = block_info.epoch_first_block;
        let estimated_next_epoch_start =
            self.get_block_info(&epoch_first_block)?.height + self.config.epoch_length;

        loop {
            let block_info = self.get_block_info(&last_final_hash)?;
            let prev_hash = block_info.prev_hash;
            let prev_block_info = self.get_block_info(&prev_hash)?;
            // See `is_next_block_in_next_epoch` for details on ` + 3`
            if prev_block_info.height + 3 >= estimated_next_epoch_start {
                last_final_hash = prev_hash;
            } else {
                return Ok(last_final_hash);
            }
        }
    }

    /// Returns true, if given current block info, next block supposed to be in the next epoch.
    #[allow(clippy::wrong_self_convention)]
    fn is_next_block_in_next_epoch(&mut self, block_info: &BlockInfo) -> Result<bool, EpochError> {
        if block_info.prev_hash == CryptoHash::default() {
            return Ok(true);
        }
        let estimated_next_epoch_start =
            self.get_block_info(&block_info.epoch_first_block)?.height + self.config.epoch_length;
        // Say the epoch length is 10, and say all the blocks have all the approvals.
        // Say the first block of a particular epoch has height 111. We want the block 121 to be
        //     the first block of the next epoch. For 121 to be the next block, the current block
        //     has height 120, 119 has the quorum pre-commit and 118 is finalized.
        // 121 - 118 = 3, hence the `last_finalized_height + 3`
        Ok((block_info.last_finalized_height + 3 >= estimated_next_epoch_start
            || self.config.num_block_producer_seats < 4)
            && block_info.height + 1 >= estimated_next_epoch_start)
    }

    /// Returns epoch id for the next epoch (T+1), given an block info in current epoch (T).
    fn get_next_epoch_id_from_info(
        &mut self,
        block_info: &BlockInfo,
    ) -> Result<EpochId, EpochError> {
        let first_block_info = self.get_block_info(&block_info.epoch_first_block)?;
        Ok(EpochId(first_block_info.prev_hash))
    }

    pub fn get_epoch_info(&mut self, epoch_id: &EpochId) -> Result<&EpochInfo, EpochError> {
        if !self.epochs_info.cache_get(epoch_id).is_some() {
            let epoch_info = self
                .store
                .get_ser(ColEpochInfo, epoch_id.as_ref())
                .map_err(|err| err.into())
                .and_then(|value| value.ok_or_else(|| EpochError::EpochOutOfBounds))?;
            self.epochs_info.cache_set(epoch_id.clone(), epoch_info);
        }
        self.epochs_info.cache_get(epoch_id).ok_or(EpochError::EpochOutOfBounds)
    }

    fn has_epoch_info(&mut self, epoch_id: &EpochId) -> Result<bool, EpochError> {
        match self.get_epoch_info(epoch_id) {
            Ok(_) => Ok(true),
            Err(EpochError::EpochOutOfBounds) => Ok(false),
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

    fn has_block_info(&mut self, hash: &CryptoHash) -> Result<bool, EpochError> {
        match self.get_block_info(hash) {
            Ok(_) => Ok(true),
            Err(EpochError::MissingBlock(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Get BlockInfo for a block
    /// # Errors
    /// EpochError::Other if storage returned an error
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
        block_hash: &CryptoHash,
        block_info: BlockInfo,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(ColBlockInfo, block_hash.as_ref(), &block_info)
            .map_err(EpochError::from)?;
        self.blocks_info.cache_set(*block_hash, block_info);
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
                .and_then(|value| value.ok_or_else(|| EpochError::EpochOutOfBounds))?;
            self.epoch_id_to_start.cache_set(epoch_id.clone(), epoch_start);
        }
        Ok(*self.epoch_id_to_start.cache_get(epoch_id).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use near_primitives::challenge::SlashedValidator;
    use near_primitives::hash::hash;
    use near_store::test_utils::create_test_store;

    use crate::test_utils::{
        change_stake, default_reward_calculator, epoch_config, epoch_info, hash_range,
        record_block, reward, setup_default_epoch_manager, setup_epoch_manager, stake,
        DEFAULT_TOTAL_SUPPLY,
    };

    use super::*;

    #[test]
    fn test_stake_validator() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators.clone(), 1, 1, 2, 2, 90, 60);

        let h = hash_range(4);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

        let expected0 = epoch_info(
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

        let expected3 = epoch_info(
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
        );
        // no validator change in the last epoch
        let epoch3 = epoch_manager.get_epoch_id(&h[3]).unwrap();
        assert_eq!(epoch_manager.get_epoch_info(&epoch3).unwrap(), &expected3);

        // Start another epoch manager from the same store to check that it saved the state.
        let mut epoch_manager2 = EpochManager::new(
            epoch_manager.store.clone(),
            epoch_manager.config.clone(),
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
                .map(|x| (x.0.account_id.clone(), x.1))
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
                .map(|x| (x.0.account_id.clone(), x.1))
                .collect::<Vec<_>>(),
            vec![("test2".to_string(), false), ("test4".to_string(), false)]
        );

        let epoch2_2 = epoch_manager.get_epoch_id(&h[11]).unwrap();
        assert_eq!(
            epoch_manager
                .get_all_block_producers_ordered(&epoch2_2, &h[1])
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id.clone(), x.1))
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
        let store = create_test_store();
        let config = epoch_config(4, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();
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
            epoch_manager.get_epoch_info(&epoch_id).unwrap().validator_kickout,
            vec![(
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
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();

        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

        // Slash test1
        let mut slashed = HashMap::new();
        slashed.insert("test1".to_string(), SlashState::Other);
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo::new(
                    1,
                    0,
                    h[0],
                    vec![],
                    vec![],
                    vec![SlashedValidator::new("test1".to_string(), false)],
                    0,
                    DEFAULT_TOTAL_SUPPLY,
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();

        let epoch_id = epoch_manager.get_epoch_id(&h[1]).unwrap();
        assert_eq!(
            epoch_manager
                .get_all_block_producers_ordered(&epoch_id, &h[1])
                .unwrap()
                .iter()
                .map(|x| (x.0.account_id.clone(), x.1))
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
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();

        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);
        record_block(&mut epoch_manager, h[0], h[1], 1, vec![]);

        epoch_manager
            .record_block_info(
                &h[2],
                BlockInfo::new(
                    2,
                    0,
                    h[1],
                    vec![],
                    vec![],
                    vec![
                        SlashedValidator::new("test1".to_string(), true),
                        SlashedValidator::new("test1".to_string(), false),
                    ],
                    0,
                    DEFAULT_TOTAL_SUPPLY,
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[2]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::Other)]);
        record_block(&mut epoch_manager, h[2], h[3], 3, vec![]);
        // new epoch
        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[3]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::AlreadySlashed)]);
        // slash test1 for double sign
        epoch_manager
            .record_block_info(
                &h[4],
                BlockInfo::new(
                    4,
                    0,
                    h[3],
                    vec![],
                    vec![],
                    vec![SlashedValidator::new("test1".to_string(), true)],
                    0,
                    DEFAULT_TOTAL_SUPPLY,
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
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
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();

        let h = hash_range(10);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo::new(
                    1,
                    0,
                    h[0],
                    vec![],
                    vec![],
                    vec![SlashedValidator::new("test1".to_string(), true)],
                    0,
                    DEFAULT_TOTAL_SUPPLY,
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();

        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[1]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::DoubleSign)]);

        record_block(&mut epoch_manager, h[1], h[2], 2, vec![]);
        let slashed: Vec<_> =
            epoch_manager.get_slashed_validators(&h[2]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed, vec![("test1".to_string(), SlashState::DoubleSign)]);
        // new epoch
        epoch_manager
            .record_block_info(
                &h[3],
                BlockInfo::new(
                    3,
                    0,
                    h[2],
                    vec![],
                    vec![],
                    vec![SlashedValidator::new("test1".to_string(), true)],
                    0,
                    DEFAULT_TOTAL_SUPPLY,
                ),
                [0; 32],
            )
            .unwrap()
            .commit()
            .unwrap();
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
            epoch_manager.get_epoch_info(&next_epoch).unwrap().validators,
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
            max_inflation_rate: 5,
            num_blocks_per_year: 50,
            epoch_length,
            validator_reward_percentage: 60,
            protocol_reward_percentage: 10,
            protocol_treasury_account: "near".to_string(),
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
                &h[0],
                BlockInfo {
                    height: 0,
                    last_finalized_height: 0,
                    prev_hash: Default::default(),
                    epoch_first_block: h[0],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    validator_reward: 0,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo {
                    height: 1,
                    last_finalized_height: 1,
                    prev_hash: h[0],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    validator_reward: 10,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[2],
                BlockInfo {
                    height: 2,
                    last_finalized_height: 2,
                    prev_hash: h[1],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    validator_reward: 10,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
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
            20,
            total_supply,
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
            max_inflation_rate: 5,
            num_blocks_per_year: 50,
            epoch_length,
            validator_reward_percentage: 60,
            protocol_reward_percentage: 10,
            protocol_treasury_account: "near".to_string(),
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
        let rng_seed = [0; 32];
        let h = hash_range(5);
        epoch_manager
            .record_block_info(
                &h[0],
                BlockInfo {
                    height: 0,
                    last_finalized_height: 0,
                    prev_hash: Default::default(),
                    epoch_first_block: h[0],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    validator_reward: 0,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo {
                    height: 1,
                    last_finalized_height: 1,
                    prev_hash: h[0],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    validator_reward: 10,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[2],
                BlockInfo {
                    height: 2,
                    last_finalized_height: 2,
                    prev_hash: h[1],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    validator_reward: 10,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
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
            20,
            total_supply,
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
            max_inflation_rate: 5,
            num_blocks_per_year: 1_000_000,
            epoch_length,
            validator_reward_percentage: 60,
            protocol_reward_percentage: 10,
            protocol_treasury_account: "near".to_string(),
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
        let rng_seed = [0; 32];
        let h = hash_range(5);
        epoch_manager
            .record_block_info(
                &h[0],
                BlockInfo {
                    height: 0,
                    last_finalized_height: 0,
                    prev_hash: Default::default(),
                    epoch_first_block: h[0],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![],
                    slashed: Default::default(),
                    validator_reward: 0,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo {
                    height: 1,
                    last_finalized_height: 1,
                    prev_hash: h[0],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true, false],
                    slashed: Default::default(),
                    validator_reward: 10,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[2],
                BlockInfo {
                    height: 2,
                    last_finalized_height: 2,
                    prev_hash: h[1],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true, true],
                    slashed: Default::default(),
                    validator_reward: 10,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
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
        let mut validators_stakes = HashMap::new();
        validators_stakes.insert("test1".to_string(), stake_amount);
        validators_stakes.insert("test2".to_string(), stake_amount);
        let (validator_reward, inflation) = reward_calculator.calculate_reward(
            validator_online_ratio,
            &validators_stakes,
            20,
            total_supply,
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
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();
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
                &h[0],
                BlockInfo {
                    height: 0,
                    last_finalized_height: 0,
                    prev_hash: Default::default(),
                    epoch_first_block: h[0],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![],
                    slashed: Default::default(),
                    validator_reward: 0,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo {
                    height: 1,
                    last_finalized_height: 1,
                    prev_hash: h[0],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true, true, true],
                    slashed: Default::default(),
                    validator_reward: 0,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[3],
                BlockInfo {
                    height: 3,
                    last_finalized_height: 3,
                    prev_hash: h[1],
                    epoch_first_block: h[2],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true, true, true],
                    slashed: Default::default(),
                    validator_reward: 0,
                    total_supply,
                    block_tracker: Default::default(),
                    shard_tracker: Default::default(),
                    all_proposals: vec![],
                    total_validator_reward: 0,
                },
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
    fn test_block_tracker() {
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
        let h = hash_range(5);
        record_block(&mut em, Default::default(), h[0], 0, vec![]);
        record_block(&mut em, h[0], h[1], 1, vec![]);
        record_block(&mut em, h[1], h[3], 3, vec![]);

        let block_info1 = em.get_block_info(&h[1]).unwrap().clone();
        assert_eq!(
            block_info1.block_tracker,
            vec![(0, ValidatorStats { produced: 1, expected: 1 })].into_iter().collect()
        );
        let block_info2 = em.get_block_info(&h[3]).unwrap().clone();
        assert_eq!(
            block_info2.block_tracker,
            vec![
                (0, ValidatorStats { produced: 2, expected: 2 }),
                (1, ValidatorStats { produced: 0, expected: 1 })
            ]
            .into_iter()
            .collect()
        );

        record_block(&mut em, h[3], h[4], 4, vec![]);
        let block_info3 = em.get_block_info(&h[4]).unwrap().clone();
        assert_eq!(
            block_info3.block_tracker,
            vec![(1, ValidatorStats { produced: 1, expected: 1 })].into_iter().collect()
        );
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
            &h[1],
            BlockInfo {
                height: 1,
                last_finalized_height: 1,
                prev_hash: h[0],
                epoch_first_block: h[1],
                epoch_id: Default::default(),
                proposals: vec![],
                chunk_mask: vec![true, true, true, false],
                slashed: Default::default(),
                validator_reward: 0,
                total_supply,
                block_tracker: Default::default(),
                shard_tracker: Default::default(),
                all_proposals: vec![],
                total_validator_reward: 0,
            },
            rng_seed,
        )
        .unwrap();
        em.record_block_info(
            &h[2],
            BlockInfo {
                height: 2,
                last_finalized_height: 2,
                prev_hash: h[1],
                epoch_first_block: h[1],
                epoch_id: Default::default(),
                proposals: vec![],
                chunk_mask: vec![true, true, true, false],
                slashed: Default::default(),
                validator_reward: 0,
                total_supply,
                block_tracker: Default::default(),
                shard_tracker: Default::default(),
                all_proposals: vec![],
                total_validator_reward: 0,
            },
            rng_seed,
        )
        .unwrap();
        em.record_block_info(
            &h[3],
            BlockInfo {
                height: 3,
                last_finalized_height: 3,
                prev_hash: h[2],
                epoch_first_block: h[3],
                epoch_id: Default::default(),
                proposals: vec![],
                chunk_mask: vec![true, true, true, true],
                slashed: Default::default(),
                validator_reward: 0,
                total_supply,
                block_tracker: Default::default(),
                shard_tracker: Default::default(),
                all_proposals: vec![],
                total_validator_reward: 0,
            },
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
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90, 60, 0);
        let amount_staked = 1_000_000;
        let validators = vec![stake("test1", amount_staked), stake("test2", amount_staked)];
        let mut epoch_manager = EpochManager::new(
            store.clone(),
            config.clone(),
            default_reward_calculator(),
            validators.clone(),
        )
        .unwrap();
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
        epoch_info.validator_kickout = HashMap::default();
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
        for index in epoch_info.block_producers_settlement.iter() {
            let bp = epoch_info.validators[*index as usize].account_id.clone();
            actual_block_producers.insert(bp);
        }
        for index in epoch_info.chunk_producers_settlement.iter().flatten() {
            let bp = epoch_info.validators[*index as usize].account_id.clone();
            actual_block_producers.insert(bp);
        }
        assert_eq!(
            epoch_info.validator_to_index.keys().cloned().into_iter().collect::<HashSet<_>>(),
            actual_block_producers
        );
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
            epoch_info1.validators.clone().into_iter().map(|r| r.account_id).collect::<Vec<_>>(),
            vec!["test1".to_string()]
        );
        assert_eq!(
            epoch_info1.stake_change.clone(),
            change_stake(vec![("test1", stake_amount), ("test2", 0), ("test3", 10)])
        );
        assert!(epoch_info1.validator_kickout.is_empty());
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
        assert_ne!(epoch_info2.epoch_height, epoch_info3.epoch_height);
    }
}
