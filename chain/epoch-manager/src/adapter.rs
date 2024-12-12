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
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::contract_distribution::{
    ChunkContractAccesses, ContractCodeRequest,
};
use near_primitives::stateless_validation::validator_assignment::ChunkValidatorAssignments;
use near_primitives::stateless_validation::ChunkProductionKey;
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ApprovalStake, BlockHeight, EpochHeight, EpochId, ShardId, ShardIndex,
    ValidatorInfoIdentifier,
};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::EpochValidatorInfo;
use near_store::{ShardUId, StoreUpdate};
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

// TODO(wacban) rename to ShardInfo
pub struct ShardUIdAndIndex {
    pub shard_uid: ShardUId,
    pub shard_index: ShardIndex,
}

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
    /// Check if epoch exists.
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool;

    /// Get the list of shard ids
    fn shard_ids(&self, epoch_id: &EpochId) -> Result<Vec<ShardId>, EpochError>;

    /// Number of Reed-Solomon parts we split each chunk into.
    ///
    /// Note: this shouldn't be too large, our Reed-Solomon supports at most 256
    /// parts.
    fn num_total_parts(&self) -> usize;

    /// How many Reed-Solomon parts are data parts.
    ///
    /// That is, fetching this many parts should be enough to reconstruct a
    /// chunk, if there are no errors.
    fn num_data_parts(&self) -> usize;

    /// Returns `account_id` that is supposed to have the `part_id`.
    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, EpochError>;

    /// Which shard the account belongs to in the given epoch.
    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardId, EpochError>;

    /// Which shard the account belongs to in the given epoch.
    fn account_id_to_shard_info(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardUIdAndIndex, EpochError>;

    /// Converts `ShardId` (index of shard in the *current* layout) to
    /// `ShardUId` (`ShardId` + the version of shard layout itself.)
    fn shard_id_to_uid(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<ShardUId, EpochError>;

    fn shard_id_to_index(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<ShardIndex, EpochError>;

    fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError>;

    fn get_epoch_config(&self, epoch_id: &EpochId) -> Result<EpochConfig, EpochError>;

    fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError>;

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError>;

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, EpochError>;

    /// Returns true, if given hash is last block in it's epoch.
    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError>;

    /// Returns true, if given hash is in an epoch that already finished.
    /// `is_next_block_epoch_start` works even if we didn't fully process the provided block.
    /// This function works even if we garbage collected `BlockInfo` of the first block of the epoch.
    /// Thus, this function is better suited for use in garbage collection.
    fn is_last_block_in_finished_epoch(&self, hash: &CryptoHash) -> Result<bool, EpochError>;

    /// Get epoch id given hash of previous block.
    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash)
        -> Result<EpochId, EpochError>;

    /// Get epoch height given hash of previous block.
    fn get_epoch_height_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochHeight, EpochError>;

    /// Get next epoch id given hash of the current block.
    fn get_next_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError>;

    /// Get next epoch id given hash of previous block.
    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError>;

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
    ) -> Result<Vec<(ShardId, ShardIndex)>, Error>;

    /// For a `ShardId` in the current block, returns its parent `ShardId`
    /// from previous block.
    ///
    /// Most of the times parent of the shard is the shard itself, unless a
    /// resharding happened and some shards were split.
    /// If there was no resharding, it just returns the `shard_id` as is, without any validation.
    ///
    /// TODO(wacban) - rename to reflect the new return type
    fn get_prev_shard_id(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<(ShardId, ShardIndex), Error>;

    /// Get shard layout given hash of previous block.
    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, EpochError>;

    fn get_shard_layout_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> ShardLayout;

    /// Get [`EpochId`] from a block belonging to the epoch.
    fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError>;

    /// Which of the two epochs is earlier.
    ///
    /// This is well-defined because finality gadget guarantees that we cannot
    /// have two different epochs on two forks.
    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, EpochError>;

    /// Get epoch start from a block belonging to the epoch.
    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, EpochError>;

    /// Get previous epoch id by hash of previous block.
    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError>;

    /// _If_ the next epoch will use a new protocol version, returns an
    /// estimated block height for when the epoch switch occurs.
    ///
    /// This is very approximate and is used for logging only.
    fn get_estimated_protocol_upgrade_block_height(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError>;

    /// Epoch block producers ordered by their order in the proposals.
    /// Returns EpochError if height is outside of known boundaries.
    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, EpochError>;

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, EpochError>;

    /// Returns all the chunk producers for a given epoch.
    fn get_epoch_chunk_producers(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError>;

    fn get_epoch_chunk_producers_for_shard(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<Vec<AccountId>, EpochError>;

    /// Returns all validators for a given epoch.
    fn get_epoch_all_validators(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError>;

    /// Block producers for given height for the main block. Return EpochError if outside of known boundaries.
    /// TODO: Deprecate in favour of get_block_producer_info
    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, EpochError>;

    /// Block producers and stake for given height for the main block. Return EpochError if outside of known boundaries.
    fn get_block_producer_info(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<ValidatorStake, EpochError>;

    /// Chunk producer info for given height for given shard. Return EpochError if outside of known boundaries.
    fn get_chunk_producer_info(
        &self,
        key: &ChunkProductionKey,
    ) -> Result<ValidatorStake, EpochError>;

    /// Gets the chunk validators for a given height and shard.
    fn get_chunk_validator_assignments(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
        height: BlockHeight,
    ) -> Result<Arc<ChunkValidatorAssignments>, EpochError>;

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), EpochError>;

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), EpochError>;

    /// WARNING: this call may be expensive.
    ///
    /// This function is intended for diagnostic use in logging & rpc, don't use
    /// it for "production" code.
    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, EpochError>;

    fn add_validator_proposals(
        &self,
        block_info: BlockInfo,
        random_value: CryptoHash,
    ) -> Result<StoreUpdate, EpochError>;

    /// Epoch active protocol version.
    fn get_epoch_protocol_version(&self, epoch_id: &EpochId)
        -> Result<ProtocolVersion, EpochError>;

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

    fn should_validate_signatures(&self) -> bool {
        true
    }

    /// Verify validator signature for the given epoch.
    /// Note: doesn't account for slashed accounts within given epoch. USE WITH CAUTION.
    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error>;

    /// Verify signature for validator or fisherman. Used for validating challenges.
    fn verify_validator_or_fisherman_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error>;

    fn verify_chunk_endorsement_signature(
        &self,
        endorsement: &ChunkEndorsement,
    ) -> Result<bool, Error>;

    fn verify_witness_contract_accesses_signature(
        &self,
        accesses: &ChunkContractAccesses,
    ) -> Result<bool, Error>;

    fn verify_witness_contract_code_request_signature(
        &self,
        request: &ContractCodeRequest,
    ) -> Result<bool, Error>;

    fn cares_about_shard_in_epoch(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError>;

    fn cares_about_shard_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError>;

    fn cares_about_shard_next_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError>;

    fn will_shard_layout_change(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError>;

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
    ) -> Result<Vec<EpochId>, EpochError>;

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
        let mut shard_layouts = vec![];
        for protocol_version in head_protocol_version + 1..=client_protocol_version {
            let shard_layout = self.get_shard_layout_from_protocol_version(protocol_version);

            let last_shard_layout = shard_layouts.last();
            if last_shard_layout == None || last_shard_layout != Some(&shard_layout) {
                shard_layouts.push(shard_layout);
            }
        }

        let mut result = HashSet::new();
        let head_shard_layout = self.get_shard_layout_from_protocol_version(head_protocol_version);
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
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_info(epoch_id).is_ok()
    }

    fn shard_ids(&self, epoch_id: &EpochId) -> Result<Vec<ShardId>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_shard_layout(epoch_id)?.shard_ids().collect())
    }

    fn num_total_parts(&self) -> usize {
        let seats = self.read().genesis_num_block_producer_seats;
        if seats > 1 {
            seats as usize
        } else {
            2
        }
    }

    fn num_data_parts(&self) -> usize {
        let total_parts = self.num_total_parts();
        if total_parts <= 3 {
            1
        } else {
            (total_parts - 1) / 3
        }
    }

    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, EpochError> {
        let epoch_manager = self.read();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
        let settlement = epoch_info.block_producers_settlement();
        let validator_id = settlement[part_id as usize % settlement.len()];
        Ok(epoch_info.get_validator(validator_id).account_id().clone())
    }

    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardId, EpochError> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
        Ok(shard_layout.account_id_to_shard_id(account_id))
    }

    fn account_id_to_shard_info(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardUIdAndIndex, EpochError> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
        let shard_id = shard_layout.account_id_to_shard_id(account_id);
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        Ok(ShardUIdAndIndex { shard_uid, shard_index })
    }

    fn shard_id_to_uid(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<ShardUId, EpochError> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
        Ok(ShardUId::from_shard_id_and_layout(shard_id, &shard_layout))
    }

    fn shard_id_to_index(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<ShardIndex, EpochError> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
        Ok(shard_layout.get_shard_index(shard_id)?)
    }

    fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_block_info(hash)
    }

    fn get_epoch_config(&self, epoch_id: &EpochId) -> Result<EpochConfig, EpochError> {
        let epoch_manager = self.read();
        let protocol_version = self.get_epoch_info(epoch_id)?.protocol_version();
        Ok(epoch_manager.get_epoch_config(protocol_version))
    }

    fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_info(epoch_id)
    }

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_shard_layout(epoch_id)
    }

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, EpochError> {
        let epoch_manager = self.read();
        let protocol_version = self.get_epoch_info(epoch_id)?.protocol_version();
        let epoch_config = epoch_manager.get_epoch_config(protocol_version);
        Ok(ShardConfig::new(epoch_config))
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.is_next_block_epoch_start(parent_hash)
    }

    fn is_last_block_in_finished_epoch(&self, hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.is_last_block_in_finished_epoch(hash)
    }

    fn get_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_id_from_prev_block(parent_hash)
    }

    fn get_epoch_height_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochHeight, EpochError> {
        let epoch_manager = self.read();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        epoch_manager.get_epoch_info(&epoch_id).map(|info| info.epoch_height())
    }

    fn get_next_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_next_epoch_id(block_hash)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_next_epoch_id_from_prev_block(parent_hash)
    }

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

    fn get_prev_shard_id(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<(ShardId, ShardIndex), Error> {
        let shard_layout = self.get_shard_layout_from_prev_block(prev_hash)?;
        let prev_shard_layout = self.get_shard_layout(&self.get_epoch_id(prev_hash)?)?;
        let is_resharding_boundary =
            self.is_next_block_epoch_start(prev_hash)? && prev_shard_layout != shard_layout;

        if is_resharding_boundary {
            let parent_shard_id = shard_layout.get_parent_shard_id(shard_id)?;
            let parent_shard_index = prev_shard_layout.get_shard_index(parent_shard_id)?;
            Ok((parent_shard_id, parent_shard_index))
        } else {
            let shard_index = shard_layout.get_shard_index(shard_id)?;
            Ok((shard_id, shard_index))
        }
    }

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
        let epoch_manager = self.read();
        epoch_manager.get_epoch_config(protocol_version).shard_layout
    }

    fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_id(block_hash)
    }

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.compare_epoch_id(epoch_id, other_epoch_id).map_err(|e| e.into())
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_start_height(block_hash)
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        if epoch_manager.is_next_block_epoch_start(prev_block_hash)? {
            epoch_manager.get_epoch_id(prev_block_hash)
        } else {
            epoch_manager.get_prev_epoch_id(prev_block_hash)
        }
    }

    fn get_estimated_protocol_upgrade_block_height(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_protocol_upgrade_block_height(block_hash)
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_all_block_producers_ordered(epoch_id, last_known_block_hash)?.to_vec())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_all_block_approvers_ordered(parent_hash)
    }

    fn get_epoch_chunk_producers(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_all_chunk_producers(epoch_id)?.to_vec())
    }

    fn get_epoch_chunk_producers_for_shard(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<Vec<AccountId>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_chunk_producers_for_shard(epoch_id, shard_id)
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, EpochError> {
        self.get_block_producer_info(epoch_id, height).map(|validator| validator.take_account_id())
    }

    fn get_block_producer_info(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<ValidatorStake, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?)
    }

    fn get_chunk_producer_info(
        &self,
        key: &ChunkProductionKey,
    ) -> Result<ValidatorStake, EpochError> {
        self.read().get_chunk_producer_info(key)
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

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), EpochError> {
        let epoch_manager = self.read();
        let validator = epoch_manager.get_validator_by_account_id(epoch_id, account_id)?;
        let block_info = epoch_manager.get_block_info(last_known_block_hash)?;
        Ok((validator, block_info.slashed().contains_key(account_id)))
    }

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), EpochError> {
        let epoch_manager = self.read();
        let fisherman = epoch_manager.get_fisherman_by_account_id(epoch_id, account_id)?;
        let block_info = epoch_manager.get_block_info(last_known_block_hash)?;
        Ok((fisherman, block_info.slashed().contains_key(account_id)))
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

    fn get_epoch_protocol_version(
        &self,
        epoch_id: &EpochId,
    ) -> Result<ProtocolVersion, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.protocol_version())
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

    fn verify_validator_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error> {
        let (validator, is_slashed) =
            self.get_validator_by_account_id(epoch_id, last_known_block_hash, account_id)?;
        if is_slashed {
            return Ok(false);
        }
        Ok(signature.verify(data, validator.public_key()))
    }

    fn verify_validator_or_fisherman_signature(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
        data: &[u8],
        signature: &Signature,
    ) -> Result<bool, Error> {
        match self.verify_validator_signature(
            epoch_id,
            last_known_block_hash,
            account_id,
            data,
            signature,
        ) {
            Err(Error::NotAValidator(_)) => {
                let (fisherman, is_slashed) =
                    self.get_fisherman_by_account_id(epoch_id, last_known_block_hash, account_id)?;
                if is_slashed {
                    return Ok(false);
                }
                Ok(signature.verify(data, fisherman.public_key()))
            }
            other => other,
        }
    }

    fn verify_chunk_endorsement_signature(
        &self,
        endorsement: &ChunkEndorsement,
    ) -> Result<bool, Error> {
        let epoch_manager = self.read();
        let epoch_id = endorsement.chunk_production_key().epoch_id;
        let validator =
            epoch_manager.get_validator_by_account_id(&epoch_id, &endorsement.account_id())?;
        Ok(endorsement.verify(validator.public_key()))
    }

    fn verify_witness_contract_accesses_signature(
        &self,
        accesses: &ChunkContractAccesses,
    ) -> Result<bool, Error> {
        let chunk_producer =
            self.read().get_chunk_producer_info(accesses.chunk_production_key())?;
        Ok(accesses.verify_signature(chunk_producer.public_key()))
    }

    fn verify_witness_contract_code_request_signature(
        &self,
        request: &ContractCodeRequest,
    ) -> Result<bool, Error> {
        let validator = self.read().get_validator_by_account_id(
            &request.chunk_production_key().epoch_id,
            request.requester(),
        )?;
        Ok(request.verify_signature(validator.public_key()))
    }

    fn cares_about_shard_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.cares_about_shard_from_prev_block(parent_hash, account_id, shard_id)
    }

    fn cares_about_shard_next_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.cares_about_shard_next_epoch_from_prev_block(
            parent_hash,
            account_id,
            shard_id,
        )
    }

    fn will_shard_layout_change(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.will_shard_layout_change(parent_hash)
    }

    fn possible_epochs_of_height_around_tip(
        &self,
        tip: &Tip,
        height: BlockHeight,
    ) -> Result<Vec<EpochId>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.possible_epochs_of_height_around_tip(tip, height)
    }

    /// Returns the set of chunk validators for a given epoch
    fn get_epoch_all_validators(
        &self,
        epoch_id: &EpochId,
    ) -> Result<Vec<ValidatorStake>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.validators_iter().collect::<Vec<_>>())
    }

    fn cares_about_shard_in_epoch(
        &self,
        epoch_id: &EpochId,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.cares_about_shard_in_epoch(epoch_id, account_id, shard_id)
    }
}
