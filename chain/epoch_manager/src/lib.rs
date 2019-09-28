use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use log::{debug, warn};

use near_primitives::hash::CryptoHash;
use near_primitives::types::{
    AccountId, Balance, BlockIndex, EpochId, ShardId, ValidatorId, ValidatorStake,
};
use near_primitives::views::EpochValidatorInfo;
use near_store::{Store, StoreUpdate, COL_BLOCK_INFO, COL_EPOCH_INFO};

use crate::proposals::proposals_to_epoch_info;
pub use crate::reward_calculator::RewardCalculator;
use crate::types::EpochSummary;
pub use crate::types::{BlockInfo, EpochConfig, EpochError, EpochInfo, RngSeed};

mod proposals;
mod reward_calculator;
pub mod test_utils;
mod types;

/// Tracks epoch information across different forks, such as validators.
/// Note: that even after garbage collection, the data about genesis epoch should be in the store.
pub struct EpochManager {
    store: Arc<Store>,
    /// Current epoch config.
    /// TODO: must be dynamically changing over time, so there should be a way to change it.
    config: EpochConfig,
    reward_calculator: RewardCalculator,

    /// Cache of epoch information.
    epochs_info: HashMap<EpochId, EpochInfo>,
    /// Cache of block information.
    blocks_info: HashMap<CryptoHash, BlockInfo>,
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
            epochs_info: HashMap::default(),
            blocks_info: HashMap::default(),
        };
        let genesis_epoch_id = EpochId::default();
        if !epoch_manager.has_epoch_info(&genesis_epoch_id)? {
            // Missing genesis epoch, means that there is no validator initialize yet.
            let epoch_info = proposals_to_epoch_info(
                &epoch_manager.config,
                [0; 32],
                &EpochInfo::default(),
                validators,
                HashSet::default(),
                validator_reward,
                0,
                0,
            )?;
            let block_info = BlockInfo::default();
            let mut store_update = epoch_manager.store.store_update();
            epoch_manager.save_epoch_info(&mut store_update, &genesis_epoch_id, epoch_info)?;
            epoch_manager.save_block_info(&mut store_update, &CryptoHash::default(), block_info)?;
            store_update.commit()?;
        }
        Ok(epoch_manager)
    }

    fn compute_kickout_info(
        &self,
        epoch_info: &EpochInfo,
        block_validator_tracker: &HashMap<ValidatorId, u64>,
        num_expected_blocks: &HashMap<ValidatorId, u64>,
        chunk_validator_tracker: &HashMap<ShardId, HashMap<ValidatorId, u64>>,
        num_expected_chunks: &HashMap<ShardId, HashMap<ValidatorId, u64>>,
        slashed: &HashSet<AccountId>,
    ) -> (HashSet<AccountId>, HashMap<AccountId, (u64, u64)>) {
        let mut all_kicked_out = true;
        let mut maximum_block_prod = 0;
        let mut max_validator_id = None;
        let validator_kickout_threshold = self.config.validator_kickout_threshold;
        let mut validator_online_ratio = HashMap::new();
        let mut validator_kickout = HashSet::new();

        for (i, _) in epoch_info.validators.iter().enumerate() {
            let account_id = epoch_info.validators[i].account_id.clone();
            if slashed.contains(&account_id) {
                continue;
            }
            let num_blocks = *block_validator_tracker.get(&i).unwrap_or(&0);
            // Note, validator_kickout_threshold is 0..100, so we use * 100 to keep this in integer space.
            let expected_blocks = *num_expected_blocks.get(&i).unwrap_or(&0);
            if num_blocks * 100 < u64::from(validator_kickout_threshold) * expected_blocks {
                validator_kickout.insert(account_id);
                continue;
            }
            let mut total_chunks_expected = 0;
            let mut total_chunks_produced = 0;
            for (shard_id, tracker) in num_expected_chunks.iter() {
                if tracker.contains_key(&i) {
                    let num_expected = *tracker.get(&i).unwrap();
                    let num_produced = *chunk_validator_tracker
                        .get(shard_id)
                        .and_then(|t| t.get(&i))
                        .unwrap_or(&0);
                    total_chunks_expected += num_expected;
                    total_chunks_produced += num_produced;
                }
            }
            if total_chunks_produced * 100
                < u64::from(validator_kickout_threshold) * total_chunks_expected
            {
                validator_kickout.insert(account_id.clone());
                continue;
            }

            // Given the number of blocks we plan to have in one epoch, the following code should not overflow
            validator_online_ratio.insert(
                account_id.clone(),
                (
                    num_blocks * total_chunks_expected + expected_blocks * total_chunks_produced,
                    total_chunks_expected * expected_blocks * 2,
                ),
            );
            if !validator_kickout.contains(&account_id) {
                all_kicked_out = false;
            }
            if num_blocks > maximum_block_prod {
                maximum_block_prod = num_blocks;
                max_validator_id = Some(i);
            }
        }
        if all_kicked_out {
            if let Some(validator_id) = max_validator_id {
                validator_kickout.remove(&epoch_info.validators[validator_id].account_id);
            }
        }
        (validator_kickout, validator_online_ratio)
    }

    fn collect_blocks_info(
        &mut self,
        epoch_id: &EpochId,
        last_block_hash: &CryptoHash,
    ) -> Result<EpochSummary, EpochError> {
        let mut proposals = BTreeMap::new();
        let mut validator_kickout = HashSet::new();
        let mut block_validator_tracker = HashMap::new();
        let mut chunk_validator_tracker = HashMap::new();
        let mut total_gas_used = 0;
        let mut total_storage_rent = 0;

        let epoch_info = self.get_epoch_info(epoch_id)?.clone();

        // Gather slashed validators and add them to kick out first.
        let slashed_validators = self.get_slashed_validators(last_block_hash)?.clone();
        for account_id in slashed_validators.iter() {
            validator_kickout.insert(account_id.clone());
        }

        let mut hash = *last_block_hash;
        loop {
            let info = self.get_block_info(&hash)?.clone();
            if &info.epoch_id != epoch_id || info.prev_hash == CryptoHash::default() {
                break;
            }

            for proposal in info.proposals.into_iter().rev() {
                if !slashed_validators.contains(&proposal.account_id) {
                    if proposal.amount == 0 {
                        validator_kickout.insert(proposal.account_id.clone());
                    }
                    // This code relies on the fact that within a block the proposals are ordered
                    // in the order they are added. So we only take the last proposal for any given
                    // account in this manner.
                    proposals.entry(proposal.account_id.clone()).or_insert(proposal);
                }
            }
            let block_validator_id = self.block_producer_from_info(&epoch_info, info.index);
            block_validator_tracker.entry(block_validator_id).and_modify(|e| *e += 1).or_insert(1);
            for (i, mask) in info.chunk_mask.iter().enumerate() {
                let chunk_validator_id =
                    self.chunk_producer_from_info(&epoch_info, info.index, i as ShardId);
                let tracker =
                    chunk_validator_tracker.entry(i as ShardId).or_insert_with(HashMap::new);
                if *mask {
                    tracker.entry(chunk_validator_id).and_modify(|e| *e += 1).or_insert(1);
                }
            }

            total_gas_used += info.gas_used;
            total_storage_rent += info.rent_paid;

            hash = info.prev_hash;
        }

        let all_proposals: Vec<_> = proposals.into_iter().map(|(_, v)| v).collect();

        let last_block_info = self.get_block_info(&last_block_hash)?.clone();
        let first_block_info = self.get_block_info(&last_block_info.epoch_first_block)?.clone();
        let (num_expected_blocks, num_expected_chunks) =
            self.get_num_expected_blocks_and_chunks(&epoch_info, &first_block_info)?;

        // Compute kick outs for validators who are offline.
        let (kickout, validator_online_ratio) = self.compute_kickout_info(
            &epoch_info,
            &block_validator_tracker,
            &num_expected_blocks,
            &chunk_validator_tracker,
            &num_expected_chunks,
            &slashed_validators,
        );
        validator_kickout = validator_kickout.union(&kickout).cloned().collect();
        debug!(
            "All proposals: {:?}, Kickouts: {:?}, Block Tracker: {:?}, Shard Tracker: {:?}, Num expected: {:?}",
            all_proposals, validator_kickout, block_validator_tracker, chunk_validator_tracker, num_expected_blocks
        );

        Ok(EpochSummary {
            last_block_hash: hash,
            all_proposals,
            validator_kickout,
            validator_online_ratio,
            total_gas_used,
            total_storage_rent,
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
        let EpochSummary {
            last_block_hash: last_block_hash_prev_epoch,
            all_proposals,
            validator_kickout,
            validator_online_ratio,
            total_gas_used,
            total_storage_rent,
        } = self.collect_blocks_info(&block_info.epoch_id, last_block_hash)?;
        let next_epoch_id = self.get_next_epoch_id(last_block_hash)?;
        let next_epoch_info = self.get_epoch_info(&next_epoch_id)?.clone();
        let (validator_reward, inflation) = self.reward_calculator.calculate_reward(
            validator_online_ratio,
            total_gas_used,
            block_info.gas_price,
            total_storage_rent,
            block_info.total_supply,
        );
        let next_next_epoch_info = match proposals_to_epoch_info(
            &self.config,
            rng_seed,
            &next_epoch_info,
            all_proposals,
            validator_kickout,
            validator_reward,
            total_gas_used,
            inflation,
        ) {
            Ok(next_next_epoch_info) => next_next_epoch_info,
            Err(EpochError::ThresholdError(amount, num_seats)) => {
                warn!(target: "epoch_manager", "Not enough stake for required number of seats (all validators tried to unstake?): amount = {} for {}", amount, num_seats);
                let mut next_next_epoch_info = next_epoch_info.clone();
                next_next_epoch_info.total_gas_used = total_gas_used;
                next_next_epoch_info
            }
            Err(err) => return Err(err),
        };
        // This epoch info is computed for the epoch after next (T+2),
        // where epoch_id of it is the hash of last block in this epoch (T).
        self.save_epoch_info(store_update, &EpochId(*last_block_hash), next_next_epoch_info)?;
        // Return next epoch (T+1) id as hash of last block in previous epoch (T-1).
        Ok(EpochId(last_block_hash_prev_epoch))
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
                for item in prev_block_info.slashed.iter() {
                    block_info.slashed.insert(item.clone());
                }
                if prev_block_info.prev_hash == CryptoHash::default() {
                    // This is first real block, starts the new epoch.
                    block_info.epoch_id = EpochId::default();
                    block_info.epoch_first_block = *current_hash;
                } else if self.is_next_block_in_next_epoch(&prev_block_info)? {
                    // Current block is in the new epoch, finalize the one in prev_block.
                    block_info.epoch_id = self.get_next_epoch_id_from_info(&prev_block_info)?;
                    block_info.epoch_first_block = *current_hash;
                } else {
                    // Same epoch as parent, copy epoch_id and epoch_start_index.
                    block_info.epoch_id = prev_block_info.epoch_id;
                    block_info.epoch_first_block = prev_block_info.epoch_first_block;
                }
                self.save_block_info(&mut store_update, current_hash, block_info.clone())?;
                // If this is the last block in the epoch, finalize this epoch.
                if self.is_next_block_in_next_epoch(&block_info)? {
                    self.finalize_epoch(&mut store_update, &block_info, &current_hash, rng_seed)?;
                }
            }
        }
        Ok(store_update)
    }

    /// Given epoch id and index, returns validator information that suppose to produce
    /// the block at that index. We don't require caller to know about EpochIds.
    pub fn get_block_producer_info(
        &mut self,
        epoch_id: &EpochId,
        index: BlockIndex,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?.clone();
        Ok(epoch_info.validators[self.block_producer_from_info(&epoch_info, index)].clone())
    }

    /// Returns all block producers in current epoch, with indicator is they are slashed or not.
    pub fn get_all_block_producers(
        &mut self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(AccountId, bool)>, EpochError> {
        let slashed = self.get_slashed_validators(last_known_block_hash)?.clone();
        let epoch_info = self.get_epoch_info(epoch_id)?;
        let mut result = vec![];
        let mut validators: HashSet<AccountId> = HashSet::default();
        for validator_id in epoch_info.block_producers.iter() {
            let account_id = epoch_info.validators[*validator_id].account_id.clone();
            if !validators.contains(&account_id) {
                let is_slashed = slashed.contains(&account_id);
                validators.insert(account_id.clone());
                result.push((account_id, is_slashed));
            }
        }
        Ok(result)
    }

    /// Given epoch id, index and shard id return validator that is chunk producer.
    pub fn get_chunk_producer_info(
        &mut self,
        epoch_id: &EpochId,
        index: BlockIndex,
        shard_id: ShardId,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?.clone();
        Ok(epoch_info.validators[self.chunk_producer_from_info(&epoch_info, index, shard_id)]
            .clone())
    }

    /// Returns validator for given account id for given epoch. We don't require caller to know about EpochIds.
    pub fn get_validator_by_account_id(
        &mut self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<Option<ValidatorStake>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        if let Some(idx) = epoch_info.validator_to_index.get(account_id) {
            return Ok(Some(epoch_info.validators[*idx].clone()));
        }
        Ok(None)
    }

    pub fn get_slashed_validators(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<&HashSet<AccountId>, EpochError> {
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
    ) -> Result<BlockIndex, EpochError> {
        let epoch_first_block = self.get_block_info(block_hash)?.epoch_first_block;
        Ok(self.get_block_info(&epoch_first_block)?.index)
    }

    /// Compute stake return info based on the last block hash of the epoch that is just finalized
    /// return the hashmap of account id to max_of_stakes, which is used in the calculation of account
    /// updates.
    pub fn compute_stake_return_info(
        &mut self,
        last_block_hash: &CryptoHash,
    ) -> Result<(HashMap<AccountId, Balance>, HashMap<AccountId, Balance>), EpochError> {
        let next_next_epoch_id = EpochId(*last_block_hash);
        let validator_reward = self.get_epoch_info(&next_next_epoch_id)?.validator_reward.clone();

        let next_epoch_id = self.get_next_epoch_id(last_block_hash)?;
        let epoch_id = self.get_epoch_id(last_block_hash)?;
        debug!(target: "epoch_manager",
            "epoch id: {:?}, prev_epoch_id: {:?}, prev_prev_epoch_id: {:?}",
            next_next_epoch_id, next_epoch_id, epoch_id
        );
        // Since stake changes for epoch T are stored in epoch info for T+2, the one stored by epoch_id
        // is the prev_prev_stake_change.
        let prev_prev_stake_change = self.get_epoch_info(&epoch_id)?.stake_change.clone();
        let prev_stake_change = self.get_epoch_info(&next_epoch_id)?.stake_change.clone();
        let stake_change = &self.get_epoch_info(&next_next_epoch_id)?.stake_change;
        debug!(target: "epoch_manager",
            "prev_prev_stake_change: {:?}, prev_stake_change: {:?}, stake_change: {:?}",
            prev_prev_stake_change, prev_stake_change, stake_change
        );
        let mut all_keys = HashSet::new();
        for (key, _) in
            prev_prev_stake_change.iter().chain(prev_stake_change.iter()).chain(stake_change.iter())
        {
            all_keys.insert(key);
        }
        let mut stake_info = HashMap::new();
        for account_id in all_keys {
            let new_stake = *stake_change.get(account_id).unwrap_or(&0);
            let prev_stake = *prev_stake_change.get(account_id).unwrap_or(&0);
            let prev_prev_stake = *prev_prev_stake_change.get(account_id).unwrap_or(&0);
            let max_of_stakes =
                vec![prev_prev_stake, prev_stake, new_stake].into_iter().max().unwrap();
            stake_info.insert(account_id.to_string(), max_of_stakes);
        }
        debug!(target: "epoch_manager", "stake_info: {:?}, validator_reward: {:?}", stake_info, validator_reward);
        Ok((stake_info, validator_reward))
    }

    /// Get validators for current epoch and next epoch.
    pub fn get_validator_info(
        &mut self,
        block_hash: &CryptoHash,
    ) -> Result<EpochValidatorInfo, EpochError> {
        let epoch_id = self.get_epoch_id(block_hash)?;
        let current_validators = self.get_epoch_info(&epoch_id)?.validators.clone();
        let next_epoch_id = self.get_next_epoch_id(block_hash)?;
        let next_validators = self.get_epoch_info(&next_epoch_id)?.validators.clone();
        let epoch_summary = self.collect_blocks_info(&epoch_id, block_hash)?;
        Ok(EpochValidatorInfo {
            current_validators: current_validators.into_iter().map(Into::into).collect(),
            next_validators: next_validators.into_iter().map(Into::into).collect(),
            current_proposals: epoch_summary.all_proposals.into_iter().map(Into::into).collect(),
        })
    }

    pub fn get_epoch_inflation(&mut self, epoch_id: &EpochId) -> Result<Balance, EpochError> {
        Ok(self.get_epoch_info(epoch_id)?.inflation)
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
        for validator_id in epoch_info.chunk_producers[shard_id as usize].iter() {
            if &epoch_info.validators[*validator_id].account_id == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_num_expected_blocks_and_chunks(
        &mut self,
        epoch_info: &EpochInfo,
        epoch_first_block_info: &BlockInfo,
    ) -> Result<(HashMap<ValidatorId, u64>, HashMap<ShardId, HashMap<ValidatorId, u64>>), EpochError>
    {
        let mut num_expected_blocks = HashMap::default();
        let mut num_expected_chunks = HashMap::default();
        let prev_epoch_last_block = self.get_block_info(&epoch_first_block_info.prev_hash)?;
        let num_shards = epoch_first_block_info.chunk_mask.len() as ShardId;
        // We iterate from next index after previous epoch's last block, for epoch_length blocks.
        for index in (prev_epoch_last_block.index + 1)
            ..=(prev_epoch_last_block.index + self.config.epoch_length)
        {
            num_expected_blocks
                .entry(self.block_producer_from_info(epoch_info, index))
                .and_modify(|e| *e += 1)
                .or_insert(1);
            for i in 0..num_shards {
                num_expected_chunks
                    .entry(i)
                    .or_insert_with(HashMap::new)
                    .entry(self.chunk_producer_from_info(epoch_info, index, i as ShardId))
                    .and_modify(|e| *e += 1)
                    .or_insert(1);
            }
        }
        Ok((num_expected_blocks, num_expected_chunks))
    }

    fn block_producer_from_info(&self, epoch_info: &EpochInfo, index: BlockIndex) -> ValidatorId {
        epoch_info.block_producers
            [(index % (epoch_info.block_producers.len() as BlockIndex)) as usize]
    }

    fn chunk_producer_from_info(
        &self,
        epoch_info: &EpochInfo,
        index: BlockIndex,
        shard_id: ShardId,
    ) -> ValidatorId {
        epoch_info.chunk_producers[shard_id as usize]
            [(index % (epoch_info.chunk_producers[shard_id as usize].len() as BlockIndex)) as usize]
    }

    /// Returns true, if given current block info, next block suppose to be in the next epoch.
    #[allow(clippy::wrong_self_convention)]
    fn is_next_block_in_next_epoch(&mut self, block_info: &BlockInfo) -> Result<bool, EpochError> {
        Ok(block_info.index + 1
            >= self.get_block_info(&block_info.epoch_first_block)?.index + self.config.epoch_length)
    }

    /// Returns epoch id for the next epoch (T+1), given an block info in current epoch (T).
    fn get_next_epoch_id_from_info(
        &mut self,
        block_info: &BlockInfo,
    ) -> Result<EpochId, EpochError> {
        let first_block_info = self.get_block_info(&block_info.epoch_first_block)?;
        Ok(EpochId(first_block_info.prev_hash))
    }

    fn get_epoch_info(&mut self, epoch_id: &EpochId) -> Result<&EpochInfo, EpochError> {
        if !self.epochs_info.contains_key(epoch_id) {
            let epoch_info = self
                .store
                .get_ser(COL_EPOCH_INFO, epoch_id.as_ref())
                .map_err(|err| err.into())
                .and_then(|value| value.ok_or_else(|| EpochError::EpochOutOfBounds))?;
            self.epochs_info.insert(epoch_id.clone(), epoch_info);
        }
        self.epochs_info.get(epoch_id).ok_or(EpochError::EpochOutOfBounds)
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
            .set_ser(COL_EPOCH_INFO, epoch_id.as_ref(), &epoch_info)
            .map_err(EpochError::from)?;
        self.epochs_info.insert(epoch_id.clone(), epoch_info);
        Ok(())
    }

    fn has_block_info(&mut self, hash: &CryptoHash) -> Result<bool, EpochError> {
        match self.get_block_info(hash) {
            Ok(_) => Ok(true),
            Err(EpochError::MissingBlock(_)) => Ok(false),
            Err(err) => Err(err),
        }
    }

    pub fn get_block_info(&mut self, hash: &CryptoHash) -> Result<&BlockInfo, EpochError> {
        if !self.blocks_info.contains_key(hash) {
            let block_info = self
                .store
                .get_ser(COL_BLOCK_INFO, hash.as_ref())
                .map_err(EpochError::from)
                .and_then(|value| value.ok_or_else(|| EpochError::MissingBlock(*hash)))?;
            self.blocks_info.insert(*hash, block_info);
        }
        self.blocks_info.get(hash).ok_or(EpochError::MissingBlock(*hash))
    }

    fn save_block_info(
        &mut self,
        store_update: &mut StoreUpdate,
        block_hash: &CryptoHash,
        block_info: BlockInfo,
    ) -> Result<(), EpochError> {
        store_update
            .set_ser(COL_BLOCK_INFO, block_hash.as_ref(), &block_info)
            .map_err(EpochError::from)?;
        self.blocks_info.insert(*block_hash, block_info);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use near_store::test_utils::create_test_store;

    use crate::test_utils::{
        change_stake, default_reward_calculator, epoch_config, epoch_info, hash_range,
        record_block, reward, setup_default_epoch_manager, setup_epoch_manager, stake,
        DEFAULT_GAS_PRICE, DEFAULT_TOTAL_SUPPLY,
    };

    use super::*;

    #[test]
    fn test_stake_validator() {
        let amount_staked = 1_000_000;
        let validators = vec![("test1", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators.clone(), 1, 1, 2, 2, 90);

        let h = hash_range(4);
        record_block(&mut epoch_manager, CryptoHash::default(), h[0], 0, vec![]);

        let expected0 = epoch_info(
            vec![("test1", amount_staked)],
            vec![0, 0],
            vec![vec![0, 0]],
            vec![],
            change_stake(vec![("test1", amount_staked)]),
            0,
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
            vec![("test1", amount_staked), ("test2", amount_staked)],
            vec![0, 1],
            vec![vec![0, 1]],
            vec![],
            change_stake(vec![("test1", amount_staked), ("test2", amount_staked)]),
            0,
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
        let validators = vec![("test1", amount_staked), ("test2", amount_staked)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 2, 1, 2, 0, 90);

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
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)]),
                0,
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
        let mut epoch_manager = setup_default_epoch_manager(validators.clone(), 3, 1, 3, 0, 90);

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
            epoch_manager.get_all_block_producers(&epoch1, &h[1]).unwrap(),
            vec![
                ("test3".to_string(), false),
                ("test2".to_string(), false),
                ("test1".to_string(), false)
            ]
        );

        let epoch2_1 = epoch_manager.get_epoch_id(&h[13]).unwrap();
        assert_eq!(
            epoch_manager.get_all_block_producers(&epoch2_1, &h[1]).unwrap(),
            vec![("test2".to_string(), false), ("test4".to_string(), false)]
        );

        let epoch2_2 = epoch_manager.get_epoch_id(&h[11]).unwrap();
        assert_eq!(
            epoch_manager.get_all_block_producers(&epoch2_2, &h[1]).unwrap(),
            vec![("test1".to_string(), false), ("test3".to_string(), false),]
        );

        // Check that if we have a different epoch manager and apply only second branch we get the same results.
        let mut epoch_manager2 = setup_default_epoch_manager(validators, 3, 1, 3, 0, 90);
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
            setup_default_epoch_manager(vec![("test1", amount_staked)], 2, 1, 1, 0, 90);

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
                vec![("test1", amount_staked)],
                vec![0],
                vec![vec![0]],
                vec![],
                change_stake(vec![("test1", amount_staked)]),
                0,
                reward(vec![("test1", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_validator_unstake() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90);
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
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)]),
                0,
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
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                change_stake(vec![("test2", amount_staked)]),
                0,
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
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                change_stake(vec![("test2", amount_staked)]),
                0,
                reward(vec![("test2", 0), ("near", 0)]),
                0
            )
        );
    }

    #[test]
    fn test_slashing() {
        let store = create_test_store();
        let config = epoch_config(2, 1, 2, 0, 90);
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
        let mut slashed = HashSet::new();
        slashed.insert("test1".to_string());
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo::new(
                    1,
                    h[0],
                    vec![],
                    vec![],
                    slashed,
                    0,
                    DEFAULT_GAS_PRICE,
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
            epoch_manager.get_all_block_producers(&epoch_id, &h[1]).unwrap(),
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
                vec![("test2", amount_staked)],
                vec![0, 0],
                vec![vec![0, 0]],
                vec![],
                change_stake(vec![("test1", 0), ("test2", amount_staked)]),
                0,
                reward(vec![("test2", 0), ("near", 0)]),
                0
            )
        );

        let slashed1: Vec<_> =
            epoch_manager.get_slashed_validators(&h[2]).unwrap().clone().into_iter().collect();
        let slashed2: Vec<_> =
            epoch_manager.get_slashed_validators(&h[3]).unwrap().clone().into_iter().collect();
        assert_eq!(slashed1, vec!["test1".to_string()]);
        assert_eq!(slashed2, slashed1);
    }

    /// If all current validator try to unstake, we disallow that.
    #[test]
    fn test_all_validators_unstake() {
        let stake_amount = 1_000;
        let validators =
            vec![("test1", stake_amount), ("test2", stake_amount), ("test3", stake_amount)];
        let mut epoch_manager = setup_default_epoch_manager(validators, 1, 1, 2, 0, 90);
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
    fn test_validator_reward() {
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
        let mut epoch_manager =
            setup_epoch_manager(validators, epoch_length, 1, 1, 0, 90, reward_calculator.clone());
        let rng_seed = [0; 32];
        let h = hash_range(5);
        epoch_manager
            .record_block_info(
                &h[0],
                BlockInfo {
                    index: 0,
                    prev_hash: Default::default(),
                    epoch_first_block: h[0],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    gas_used: 0,
                    gas_price: DEFAULT_GAS_PRICE,
                    rent_paid: 0,
                    total_supply,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo {
                    index: 1,
                    prev_hash: h[0],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    gas_used: 10,
                    gas_price: DEFAULT_GAS_PRICE,
                    rent_paid: 10,
                    total_supply,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[2],
                BlockInfo {
                    index: 2,
                    prev_hash: h[1],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true],
                    slashed: Default::default(),
                    gas_used: 10,
                    gas_price: DEFAULT_GAS_PRICE,
                    rent_paid: 10,
                    total_supply,
                },
                rng_seed,
            )
            .unwrap();
        let mut validator_online_ratio = HashMap::new();
        validator_online_ratio.insert("test1".to_string(), (0, 0));
        validator_online_ratio.insert("test2".to_string(), (1, 1));
        let (validator_reward, inflation) = reward_calculator.calculate_reward(
            validator_online_ratio,
            20,
            DEFAULT_GAS_PRICE,
            20,
            total_supply,
        );
        let test2_reward = *validator_reward.get("test2").unwrap();
        let protocol_reward = *validator_reward.get("near").unwrap();

        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap(),
            &epoch_info(
                vec![("test2", stake_amount + test2_reward)],
                vec![0],
                vec![vec![0]],
                vec![],
                change_stake(vec![("test1", 0), ("test2", stake_amount + test2_reward)]),
                20,
                reward(vec![("test1", 0), ("test2", test2_reward), ("near", protocol_reward)]),
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
        let mut epoch_manager =
            setup_epoch_manager(validators, epoch_length, 2, 2, 0, 90, reward_calculator.clone());
        let rng_seed = [0; 32];
        let h = hash_range(5);
        epoch_manager
            .record_block_info(
                &h[0],
                BlockInfo {
                    index: 0,
                    prev_hash: Default::default(),
                    epoch_first_block: h[0],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![],
                    slashed: Default::default(),
                    gas_used: 0,
                    gas_price: DEFAULT_GAS_PRICE,
                    rent_paid: 0,
                    total_supply,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[1],
                BlockInfo {
                    index: 1,
                    prev_hash: h[0],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true, false],
                    slashed: Default::default(),
                    gas_used: 10,
                    gas_price: DEFAULT_GAS_PRICE,
                    rent_paid: 10,
                    total_supply,
                },
                rng_seed,
            )
            .unwrap();
        epoch_manager
            .record_block_info(
                &h[2],
                BlockInfo {
                    index: 2,
                    prev_hash: h[1],
                    epoch_first_block: h[1],
                    epoch_id: Default::default(),
                    proposals: vec![],
                    chunk_mask: vec![true, true],
                    slashed: Default::default(),
                    gas_used: 10,
                    gas_price: DEFAULT_GAS_PRICE,
                    rent_paid: 10,
                    total_supply,
                },
                rng_seed,
            )
            .unwrap();
        let mut validator_online_ratio = HashMap::new();
        validator_online_ratio.insert("test2".to_string(), (1, 1));
        let (validator_reward, inflation) = reward_calculator.calculate_reward(
            validator_online_ratio,
            20,
            DEFAULT_GAS_PRICE,
            20,
            total_supply,
        );
        let test2_reward = *validator_reward.get("test2").unwrap();
        let protocol_reward = *validator_reward.get("near").unwrap();
        assert_eq!(
            epoch_manager.get_epoch_info(&EpochId(h[2])).unwrap(),
            &epoch_info(
                vec![("test2", stake_amount + test2_reward)],
                vec![0, 0],
                vec![vec![0, 0], vec![0, 0]],
                vec![],
                change_stake(vec![("test1", 0), ("test2", stake_amount + test2_reward)]),
                20,
                reward(vec![("test2", test2_reward), ("near", protocol_reward)]),
                inflation
            )
        );
    }
}
