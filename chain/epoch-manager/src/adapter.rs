use crate::EpochManagerHandle;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_primitives::block::Tip;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{EpochConfig, ShardConfig};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::ShardLayout;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::stateless_validation::validator_assignment::ChunkValidatorAssignments;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ApprovalStake, BlockHeight, EpochHeight, EpochId, ShardId, ShardIndex,
    ValidatorInfoIdentifier,
};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::EpochValidatorInfo;
use near_store::{ShardUId, StoreUpdate};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tracing::warn;

/// A trait that abstracts the interface of the EpochManager. The two
/// implementations are EpochManagerHandle and KeyValueEpochManager. Strongly
/// prefer the former whenever possible. The latter is for legacy tests.
///
/// TODO - Most of the methods here take the epoch id as an argument but often
/// the protocol version would be sufficient. Rename those methods by adding
/// "_from_epoch_id" suffix and add the more precise methods using only the
/// protocol version. This may simplify the usage of the EpochManagerAdapter in
/// a few places where it's cumbersome to get the epoch id.
pub trait EpochManagerAdapter: Send + Sync {
    fn get_epoch_config_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> EpochConfig;

    fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError>;

    fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError>;

    fn get_epoch_start_from_epoch_id(&self, epoch_id: &EpochId) -> Result<BlockHeight, EpochError>;

    /// Number of Reed-Solomon parts we split each chunk into.
    ///
    /// Note: this shouldn't be too large, our Reed-Solomon supports at most 256
    /// parts.
    fn num_total_parts(&self) -> usize;

    /// Check if epoch exists.
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        self.get_epoch_info(epoch_id).is_ok()
    }

    /// How many Reed-Solomon parts are data parts.
    ///
    /// That is, fetching this many parts should be enough to reconstruct a
    /// chunk, if there are no errors.
    fn num_data_parts(&self) -> usize {
        let total_parts = self.num_total_parts();
        if total_parts <= 3 { 1 } else { (total_parts - 1) / 3 }
    }

    /// Returns `account_id` that is supposed to have the `part_id`.
    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, EpochError> {
        let epoch_info = self.get_epoch_info(&epoch_id)?;
        let settlement = epoch_info.block_producers_settlement();
        let validator_id = settlement[part_id as usize % settlement.len()];
        Ok(epoch_info.get_validator(validator_id).account_id().clone())
    }

    fn get_epoch_config(&self, epoch_id: &EpochId) -> Result<EpochConfig, EpochError> {
        let protocol_version = self.get_epoch_info(epoch_id)?.protocol_version();
        Ok(self.get_epoch_config_from_protocol_version(protocol_version))
    }

    /// Returns true, if given hash is in an epoch that already finished.
    /// `is_next_block_epoch_start` works even if we didn't fully process the provided block.
    /// This function works even if we garbage collected `BlockInfo` of the first block of the epoch.
    /// Thus, this function is better suited for use in garbage collection.
    fn is_last_block_in_finished_epoch(&self, hash: &CryptoHash) -> Result<bool, EpochError> {
        match self.get_epoch_info(&EpochId(*hash)) {
            Ok(_) => Ok(true),
            Err(err @ EpochError::IOErr(_)) => Err(err),
            Err(EpochError::EpochOutOfBounds(_) | EpochError::MissingBlock(_)) => Ok(false),
            Err(err) => {
                warn!(target: "epoch_manager", ?err, "Unexpected error in is_last_block_in_finished_epoch");
                Ok(false)
            }
        }
    }

    /// Returns true, if given hash is last block in it's epoch.
    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let block_info = self.get_block_info(parent_hash)?;
        if block_info.is_genesis() {
            return Ok(true);
        }
        let estimated_next_epoch_start = self.get_estimated_next_epoch_start(&block_info)?;
        Ok(block_info.last_finalized_height() + 3 >= estimated_next_epoch_start)
    }

    /// Get epoch id given hash of previous block.
    fn get_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        if self.is_next_block_epoch_start(parent_hash)? {
            self.get_next_epoch_id(parent_hash)
        } else {
            self.get_epoch_id(parent_hash)
        }
    }

    /// Get epoch height given hash of previous block.
    fn get_epoch_height_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochHeight, EpochError> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        self.get_epoch_info(&epoch_id).map(|info| info.epoch_height())
    }

    /// Get next epoch id given hash of the current block.
    fn get_next_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let block_info = self.get_block_info(block_hash)?;
        let first_block_info = self.get_block_info(block_info.epoch_first_block())?;
        Ok(EpochId(*first_block_info.prev_hash()))
    }

    /// Get next epoch id given hash of previous block.
    fn get_next_epoch_id_from_prev_block(
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

    /// Get the list of shard ids
    fn shard_ids(&self, epoch_id: &EpochId) -> Result<Vec<ShardId>, EpochError> {
        Ok(self.get_shard_layout(epoch_id)?.shard_ids().collect())
    }

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        self.get_epoch_config(epoch_id).map(|config| config.shard_layout)
    }

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, EpochError> {
        self.get_epoch_config(epoch_id).map(ShardConfig::new)
    }

    /// For each `ShardId` in the current block, returns its parent `ShardId`
    /// from previous block.
    ///
    /// Most of the times parent of the shard is the shard itself, unless a
    /// resharding happened and some shards were split.
    /// If there was no resharding, it just returns `shard_ids` as is, without any validation.
    /// The resulting Vec will always be of the same length as the `shard_ids` argument.
    ///
    /// TODO(wacban) - rename to reflect the new return type
    fn get_prev_shard_ids(
        &self,
        prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<(ShardId, ShardIndex)>, Error> {
        let shard_layout = self.get_shard_layout_from_prev_block(prev_hash)?;
        let prev_shard_layout = self.get_shard_layout(&self.get_epoch_id(prev_hash)?)?;
        let is_resharding_boundary =
            self.is_next_block_epoch_start(prev_hash)? && prev_shard_layout != shard_layout;

        let mut result = vec![];
        if is_resharding_boundary {
            for shard_id in shard_ids {
                let parent_shard_id = shard_layout.get_parent_shard_id(shard_id)?;
                let parent_shard_index = prev_shard_layout.get_shard_index(parent_shard_id)?;
                result.push((parent_shard_id, parent_shard_index));
            }
            Ok(result)
        } else {
            for shard_id in shard_ids {
                let shard_index = shard_layout.get_shard_index(shard_id)?;
                result.push((shard_id, shard_index));
            }
            Ok(result)
        }
    }

    /// For a `ShardId` in the current block, returns its parent `ShardId`
    /// from previous block.
    ///
    /// Most of the times parent of the shard is the shard itself, unless a
    /// resharding happened and some shards were split.
    /// If there was no resharding, it just returns the `shard_id` as is, without any validation.
    ///
    /// TODO(wacban) - rename to reflect the new return type
    fn get_prev_shard_id_from_prev_hash(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<(ShardLayout, ShardId, ShardIndex), EpochError> {
        let shard_layout = self.get_shard_layout_from_prev_block(prev_hash)?;
        let prev_shard_layout = self.get_shard_layout(&self.get_epoch_id(prev_hash)?)?;
        let is_resharding_boundary =
            self.is_next_block_epoch_start(prev_hash)? && prev_shard_layout != shard_layout;

        if is_resharding_boundary {
            let parent_shard_id = shard_layout.get_parent_shard_id(shard_id)?;
            let parent_shard_index = prev_shard_layout.get_shard_index(parent_shard_id)?;
            Ok((prev_shard_layout, parent_shard_id, parent_shard_index))
        } else {
            let shard_index = shard_layout.get_shard_index(shard_id)?;
            Ok((shard_layout, shard_id, shard_index))
        }
    }

    /// Get shard layout given hash of previous block.
    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, EpochError> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        self.get_shard_layout(&epoch_id)
    }

    fn get_shard_layout_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> ShardLayout {
        self.get_epoch_config_from_protocol_version(protocol_version).shard_layout
    }

    /// Get [`EpochId`] from a block belonging to the epoch.
    fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        self.get_block_info(block_hash).map(|block_info| *block_info.epoch_id())
    }

    /// Which of the two epochs is earlier.
    ///
    /// This is well-defined because finality gadget guarantees that we cannot
    /// have two different epochs on two forks.
    fn compare_epoch_id(
        &self,
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
            (Err(_), Err(_)) => Err(EpochError::EpochOutOfBounds(*epoch_id)), // other_epoch_id may be out of bounds as well
        }
    }

    /// Get epoch start from a block belonging to the epoch.
    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, EpochError> {
        let epoch_first_block = *self.get_block_info(block_hash)?.epoch_first_block();
        self.get_block_info(&epoch_first_block).map(|block_info| block_info.height())
    }

    /// Get previous epoch id by hash of previous block.
    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        if self.is_next_block_epoch_start(prev_block_hash)? {
            self.get_epoch_id(prev_block_hash)
        } else {
            // get previous epoch_id
            let prev_block_info = self.get_block_info(prev_block_hash)?;
            let epoch_first_block_info =
                self.get_block_info(prev_block_info.epoch_first_block())?;
            self.get_epoch_id(epoch_first_block_info.prev_hash())
        }
    }

    fn get_estimated_next_epoch_start(
        &self,
        block_info: &BlockInfo,
    ) -> Result<BlockHeight, EpochError> {
        let epoch_length = self.get_epoch_config(block_info.epoch_id())?.epoch_length;
        let estimated_next_epoch_start =
            self.get_block_info(block_info.epoch_first_block())?.height() + epoch_length;
        Ok(estimated_next_epoch_start)
    }

    /// _If_ the next epoch will use a new protocol version, returns an
    /// estimated block height for when the epoch switch occurs.
    ///
    /// This is very approximate and is used for logging only.
    fn get_estimated_protocol_upgrade_block_height(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError> {
        let cur_epoch_info = self.get_epoch_info(&self.get_epoch_id(&block_hash)?)?;
        let next_epoch_info = self.get_epoch_info(&self.get_next_epoch_id(&block_hash)?)?;
        if cur_epoch_info.protocol_version() != next_epoch_info.protocol_version() {
            let block_info = self.get_block_info(&block_hash)?;
            let estimated_next_epoch_start = self.get_estimated_next_epoch_start(&block_info)?;
            Ok(Some(estimated_next_epoch_start))
        } else {
            Ok(None)
        }
    }

    /// Note: overwritten in EpochManagerHandler to apply caching
    fn get_all_block_producers_settlement(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Arc<[ValidatorStake]>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        let validators = epoch_info
            .block_producers_settlement()
            .iter()
            .map(|&validator_id| epoch_info.get_validator(validator_id))
            .collect();
        Ok(validators)
    }

    /// Epoch block producers ordered by their order in the proposals.
    /// Returns EpochError if height is outside of known boundaries.
    /// Note: overwritten in EpochManagerHandler to apply caching
    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
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
    }

    /// TODO(pugachag): consolidate this with EpochManager's impl
    /// Note: overwritten in EpochManagerHandler to apply caching
    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ApprovalStake>, EpochError> {
        let mut settlement = self
            .get_all_block_producers_settlement(&self.get_epoch_id_from_prev_block(parent_hash)?)?
            .to_vec();
        let settlement_epoch_boundary = settlement.len();

        let next_block_need_approvals_from_next_epoch = if self
            .is_next_block_epoch_start(parent_hash)?
        {
            false
        } else {
            let block_info = self.get_block_info(parent_hash)?;
            let estimated_next_epoch_start = self.get_estimated_next_epoch_start(&block_info)?;
            block_info.last_finalized_height() + 3 < estimated_next_epoch_start
                && block_info.height() + 3 >= estimated_next_epoch_start
        };
        if next_block_need_approvals_from_next_epoch {
            settlement.extend(
                self.get_all_block_producers_settlement(
                    &self.get_next_epoch_id_from_prev_block(parent_hash)?,
                )?
                .iter()
                .cloned(),
            );
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

    /// Returns all the chunk producers for a given epoch.
    /// Note: overwritten in EpochManagerHandler to apply caching
    fn get_epoch_chunk_producers(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        let mut producers: HashSet<u64> = HashSet::default();
        // Collect unique chunk producers.
        let epoch_info = self.get_epoch_info(epoch_id)?;
        for chunk_producers in epoch_info.chunk_producers_settlement() {
            producers.extend(chunk_producers);
        }
        Ok(producers.iter().map(|producer_id| epoch_info.get_validator(*producer_id)).collect())
    }

    /// Returns AccountIds of chunk producers that are assigned to a given shard-id in a given epoch.
    fn get_epoch_chunk_producers_for_shard(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<Vec<AccountId>, EpochError> {
        let epoch_info = self.get_epoch_info(&epoch_id)?;
        let shard_layout = self.get_shard_layout(&epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;

        let chunk_producers_settlement = epoch_info.chunk_producers_settlement();
        let chunk_producers = chunk_producers_settlement
            .get(shard_index)
            .ok_or_else(|| EpochError::ShardingError(format!("invalid shard id {shard_id}")))?;
        Ok(chunk_producers
            .iter()
            .map(|index| epoch_info.validator_account_id(*index).clone())
            .collect())
    }

    /// Returns all validators for a given epoch.
    fn get_epoch_all_validators(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        Ok(self.get_epoch_info(epoch_id)?.validators_iter().collect::<Vec<_>>())
    }

    /// Block producers for given height for the main block. Return EpochError if outside of known boundaries.
    /// TODO: Deprecate in favour of get_block_producer_info
    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, EpochError> {
        self.get_block_producer_info(epoch_id, height).map(|validator| validator.take_account_id())
    }

    /// Block producers and stake for given height for the main block. Return EpochError if outside of known boundaries.
    fn get_block_producer_info(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        let validator_id = epoch_info.sample_block_producer(height);
        Ok(epoch_info.get_validator(validator_id))
    }

    /// Chunk producer info for given height for given shard. Return EpochError if outside of known boundaries.
    fn get_chunk_producer_info(
        &self,
        key: &ChunkProductionKey,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(&key.epoch_id)?;
        let shard_layout = self.get_shard_layout(&key.epoch_id)?;
        let Some(validator_id) =
            epoch_info.sample_chunk_producer(&shard_layout, key.shard_id, key.height_created)
        else {
            return Err(EpochError::ChunkProducerSelectionError(format!(
                "Invalid shard {} for height {}",
                key.shard_id, key.height_created,
            )));
        };
        Ok(epoch_info.get_validator(validator_id))
    }

    /// Gets the chunk validators for a given height and shard.
    /// Note: overwritten in EpochManagerHandler to apply caching
    fn get_chunk_validator_assignments(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
        height: BlockHeight,
    ) -> Result<Arc<ChunkValidatorAssignments>, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        let shard_layout = self.get_shard_layout(epoch_id)?;
        let chunk_validators_per_shard = epoch_info.sample_chunk_validators(height);
        for (shard_index, chunk_validators) in chunk_validators_per_shard.into_iter().enumerate() {
            let cur_shard_id = shard_layout.get_shard_id(shard_index)?;
            if cur_shard_id != shard_id {
                continue;
            }
            let chunk_validators = chunk_validators
                .into_iter()
                .map(|(validator_id, assignment_weight)| {
                    (epoch_info.get_validator(validator_id).take_account_id(), assignment_weight)
                })
                .collect();
            return Ok(Arc::new(ChunkValidatorAssignments::new(chunk_validators)));
        }
        Err(EpochError::ChunkValidatorSelectionError(format!(
            "Invalid shard ID {shard_id} for height {height}, epoch {epoch_id:?} for chunk validation",
        )))
    }

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;
        epoch_info
            .get_validator_by_account(account_id)
            .ok_or_else(|| EpochError::NotAValidator(account_id.clone(), *epoch_id))
    }

    /// WARNING: this call may be expensive.
    ///
    /// This function is intended for diagnostic use in logging & rpc, don't use
    /// it for "production" code.
    fn get_validator_info(
        &self,
        epoch_identifier: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError>;

    fn add_validator_proposals(
        &self,
        block_info: BlockInfo,
        random_value: CryptoHash,
    ) -> Result<StoreUpdate, EpochError>;

    /// Epoch active protocol version.
    fn get_epoch_protocol_version(
        &self,
        epoch_id: &EpochId,
    ) -> Result<ProtocolVersion, EpochError> {
        self.get_epoch_info(epoch_id).map(|info| info.protocol_version())
    }

    /// Get protocol version of next epoch.
    fn get_next_epoch_protocol_version(
        &self,
        block_hash: &CryptoHash,
    ) -> Result<ProtocolVersion, EpochError> {
        self.get_epoch_protocol_version(&self.get_next_epoch_id(block_hash)?)
    }

    /// Get protocol version of next epoch with hash of previous block.network
    fn get_next_epoch_protocol_version_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ProtocolVersion, EpochError> {
        self.get_epoch_protocol_version(&self.get_next_epoch_id_from_prev_block(parent_hash)?)
    }

    // TODO #3488 this likely to be updated
    fn is_chunk_producer_for_epoch(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
    ) -> Result<bool, EpochError> {
        Ok(self.get_epoch_chunk_producers(epoch_id)?.iter().any(|v| v.account_id() == account_id))
    }

    /// Epoch Manager init procedure that is necessary after Epoch Sync.
    fn init_after_epoch_sync(
        &self,
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
    ) -> Result<(), EpochError>;

    /// This is needed as a temporary hack required for legacy tests
    /// using MockEpochManager to work.
    fn should_validate_signatures(&self) -> bool {
        true
    }

    /// Verify validator signature for the given epoch.
    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error> {
        if self.should_validate_signatures() {
            let validator = self.get_validator_by_account_id(epoch_id, account_id)?;
            Ok(signature.verify(data, validator.public_key()))
        } else {
            Ok(true)
        }
    }

    fn cares_about_shard_in_epoch(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let epoch_info = self.get_epoch_info(epoch_id)?;

        let shard_layout = self.get_shard_layout(epoch_id)?;
        let shard_index = shard_layout.get_shard_index(shard_id)?;

        let chunk_producers_settlement = epoch_info.chunk_producers_settlement();
        let chunk_producers = chunk_producers_settlement
            .get(shard_index)
            .ok_or_else(|| EpochError::ShardingError(format!("invalid shard id {shard_id}")))?;
        for validator_id in chunk_producers {
            if epoch_info.validator_account_id(*validator_id) == account_id {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn cares_about_shard_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        self.cares_about_shard_in_epoch(&epoch_id, account_id, shard_id)
    }

    // `shard_id` always refers to a shard in the current epoch that the next block from `parent_hash` belongs
    // If shard layout will change next epoch, returns true if it cares about any shard
    // that `shard_id` will split to
    fn cares_about_shard_next_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(parent_hash)?;
        if self.will_shard_layout_change(parent_hash)? {
            let shard_layout = self.get_shard_layout(&next_epoch_id)?;
            // The expect below may be triggered when the protocol version
            // changes by multiple versions at once and multiple shard layout
            // changes are captured. In this case the shards from the original
            // shard layout are not valid parents in the final shard layout.
            //
            // This typically occurs in tests that are pegged to start at a
            // certain protocol version and then upgrade to stable.
            let split_shards = shard_layout
                .get_children_shards_ids(shard_id)
                .unwrap_or_else(|| panic!("all shard layouts expect the first one must have a split map, shard_id={shard_id}, shard_layout={shard_layout:?}"));
            for next_shard_id in split_shards {
                if self.cares_about_shard_in_epoch(&next_epoch_id, account_id, next_shard_id)? {
                    return Ok(true);
                }
            }
            Ok(false)
        } else {
            self.cares_about_shard_in_epoch(&next_epoch_id, account_id, shard_id)
        }
    }

    // `shard_id` always refers to a shard in the current epoch that the next block from `parent_hash` belongs
    // If shard layout changed after the prev epoch, returns true if the account cared about the parent shard
    fn cared_about_shard_prev_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let (_layout, parent_shard_id, _index) =
            self.get_prev_shard_id_from_prev_hash(parent_hash, shard_id)?;
        let prev_epoch_id = self.get_prev_epoch_id_from_prev_block(parent_hash)?;

        self.cares_about_shard_in_epoch(&prev_epoch_id, account_id, parent_shard_id)
    }

    fn will_shard_layout_change(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(parent_hash)?;
        let shard_layout = self.get_shard_layout(&epoch_id)?;
        let next_shard_layout = self.get_shard_layout(&next_epoch_id)?;
        Ok(shard_layout != next_shard_layout)
    }

    /// Tries to estimate in which epoch the given height would reside.
    /// Looks at the previous, current and next epoch around the tip
    /// and adds them to the result if the height might be inside the epoch.
    /// It returns a list of possible epochs instead of a single value
    /// because sometimes it's impossible to determine the exact epoch
    /// in which the height will be. The exact starting height of the
    /// next epoch isn't known until it actually starts, so it's impossible
    /// to determine the exact epoch for heights which are ahead of the tip.
    fn possible_epochs_of_height_around_tip(
        &self,
        tip: &Tip,
        height: BlockHeight,
    ) -> Result<Vec<EpochId>, EpochError> {
        // If the tip is at the genesis block, it has to be handled in a special way.
        // For genesis block, epoch_first_block() is the dummy block (11111...)
        // with height 0, which could cause issues with estimating the epoch end
        // if the genesis height is nonzero. It's easier to handle it manually.
        if tip.prev_block_hash == CryptoHash::default() {
            if tip.height == height {
                return Ok(vec![tip.epoch_id]);
            }

            if height > tip.height {
                return Ok(vec![tip.next_epoch_id]);
            }

            return Ok(vec![]);
        }

        // See if the height is in the current epoch
        let current_epoch_first_block_hash =
            *self.get_block_info(&tip.last_block_hash)?.epoch_first_block();
        let current_epoch_first_block_info =
            self.get_block_info(&current_epoch_first_block_hash)?;

        let current_epoch_start = current_epoch_first_block_info.height();
        let current_epoch_length = self.get_epoch_config(&tip.epoch_id)?.epoch_length;
        let current_epoch_estimated_end = current_epoch_start.saturating_add(current_epoch_length);

        // All blocks with height lower than the estimated end are guaranteed to reside in the current epoch.
        // The situation is clear here.
        if (current_epoch_start..current_epoch_estimated_end).contains(&height) {
            return Ok(vec![tip.epoch_id]);
        }

        // If the height is higher than the current epoch's estimated end, then it's
        // not clear in which epoch it'll be. Under normal circumstances it would be
        // in the next epoch, but with missing blocks the current epoch could stretch out
        // past its estimated end, so the height might end up being in the current epoch,
        // even though its height is higher than the estimated end.
        if height >= current_epoch_estimated_end {
            return Ok(vec![tip.epoch_id, tip.next_epoch_id]);
        }

        // Finally try the previous epoch.
        // First and last blocks of the previous epoch are already known, so the situation is clear.
        let prev_epoch_last_block_hash = current_epoch_first_block_info.prev_hash();
        let prev_epoch_last_block_info = self.get_block_info(prev_epoch_last_block_hash)?;
        let prev_epoch_first_block_info =
            self.get_block_info(prev_epoch_last_block_info.epoch_first_block())?;

        // If the current epoch is the epoch after genesis, then the previous
        // epoch contains only the genesis block. This case has to be handled separately
        // because epoch_first_block() points to the dummy block (1111..), which has height 0.
        if tip.epoch_id == EpochId(CryptoHash::default()) {
            let genesis_block_info = prev_epoch_last_block_info;
            if height == genesis_block_info.height() {
                return Ok(vec![*genesis_block_info.epoch_id()]);
            } else {
                return Ok(vec![]);
            }
        }

        if (prev_epoch_first_block_info.height()..=prev_epoch_last_block_info.height())
            .contains(&height)
        {
            return Ok(vec![*prev_epoch_last_block_info.epoch_id()]);
        }

        // The height doesn't belong to any of the epochs around the tip, return an empty Vec.
        Ok(vec![])
    }

    /// Returns the list of ShardUIds in the current shard layout that will be
    /// resharded in the future within this client. Those shards should be
    /// loaded into memory on node startup.
    ///
    /// Please note that this method returns all shard uids that will be
    /// resharded in the future, regardless of whether the client tracks them.
    ///
    /// e.g. In the following resharding tree shards 0 and 1 would be returned.
    ///
    ///  0      1       2
    ///  |     / \      |
    ///  0    3   4     2
    ///  |\   |   |     |
    ///  5 6  3   4     2
    ///  | |  |   |\    |
    ///  5 6  3   7 8   2
    ///
    /// Please note that shard 4 is not returned even though it is split later
    /// on. That is because it is a child of another parent and it should
    /// already be loaded into memory after the first resharding.
    fn get_shard_uids_pending_resharding(
        &self,
        head_protocol_version: ProtocolVersion,
        client_protocol_version: ProtocolVersion,
    ) -> Result<HashSet<ShardUId>, Error> {
        let head_shard_layout = self.get_shard_layout_from_protocol_version(head_protocol_version);
        let mut shard_layouts = vec![];
        for protocol_version in head_protocol_version + 1..=client_protocol_version {
            let shard_layout = self.get_shard_layout_from_protocol_version(protocol_version);
            if shard_layout == head_shard_layout {
                continue;
            }

            let last_shard_layout = shard_layouts.last();
            if last_shard_layout != Some(&shard_layout) {
                shard_layouts.push(shard_layout);
            }
        }

        let mut result = HashSet::new();
        for shard_uid in head_shard_layout.shard_uids() {
            let shard_id = shard_uid.shard_id();
            for shard_layout in &shard_layouts {
                let children = shard_layout.get_children_shards_uids(shard_id);
                let Some(children) = children else {
                    break;
                };
                if children.len() > 1 {
                    result.insert(shard_uid);
                    break;
                }
            }
        }

        Ok(result)
    }
}

impl EpochManagerAdapter for EpochManagerHandle {
    fn num_total_parts(&self) -> usize {
        let epoch_manager = self.read();
        let genesis_protocol_version = epoch_manager.reward_calculator.genesis_protocol_version;
        let seats = epoch_manager
            .config
            .for_protocol_version(genesis_protocol_version)
            .num_block_producer_seats;
        if seats > 1 { seats as usize } else { 2 }
    }

    fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_block_info(hash)
    }

    fn get_epoch_config_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> EpochConfig {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_config(protocol_version)
    }

    fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_info(epoch_id)
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.is_next_block_epoch_start(parent_hash)
    }

    fn get_epoch_start_from_epoch_id(&self, epoch_id: &EpochId) -> Result<BlockHeight, EpochError> {
        self.read().get_epoch_start_from_epoch_id(epoch_id)
    }

    fn get_shard_layout_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> ShardLayout {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_config(protocol_version).shard_layout
    }

    fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_id(block_hash)
    }

    fn get_all_block_producers_settlement(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Arc<[ValidatorStake]>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_all_block_producers_settlement(epoch_id)
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_all_block_producers_ordered(epoch_id)?.to_vec())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<ApprovalStake>, EpochError> {
        let current_epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        let next_epoch_id = self.get_next_epoch_id_from_prev_block(parent_hash)?;
        let epoch_manager = self.read();
        epoch_manager.get_all_block_approvers_ordered(parent_hash, current_epoch_id, next_epoch_id)
    }

    fn get_epoch_chunk_producers(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_all_chunk_producers(epoch_id)?.to_vec())
    }

    fn get_chunk_validator_assignments(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
        height: BlockHeight,
    ) -> Result<Arc<ChunkValidatorAssignments>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_chunk_validator_assignments(epoch_id, shard_id, height)
    }

    /// WARNING: this function calls EpochManager::get_epoch_info_aggregator_upto_last
    /// underneath which can be very expensive.
    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_validator_info(epoch_id)
    }

    fn add_validator_proposals(
        &self,
        block_info: BlockInfo,
        random_value: CryptoHash,
    ) -> Result<StoreUpdate, EpochError> {
        let mut epoch_manager = self.write();
        epoch_manager.add_validator_proposals(block_info, random_value)
    }

    fn init_after_epoch_sync(
        &self,
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
        let mut epoch_manager = self.write();
        epoch_manager.init_after_epoch_sync(
            store_update,
            prev_epoch_first_block_info,
            prev_epoch_prev_last_block_info,
            prev_epoch_last_block_info,
            prev_epoch_id,
            prev_epoch_info,
            epoch_id,
            epoch_info,
            next_epoch_id,
            next_epoch_info,
        )
    }
}
