use crate::types::BlockHeaderInfo;
use crate::EpochManagerHandle;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_primitives::block_header::{Approval, ApprovalInner, BlockHeader};
use near_primitives::epoch_manager::block_info::BlockInfo;
use near_primitives::epoch_manager::epoch_info::EpochInfo;
use near_primitives::epoch_manager::EpochConfig;
use near_primitives::epoch_manager::ShardConfig;
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::shard_layout::{account_id_to_shard_id, ShardLayout, ShardLayoutError};
use near_primitives::sharding::{ChunkHash, ShardChunkHeader};
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{
    AccountId, ApprovalStake, Balance, BlockHeight, EpochHeight, EpochId, NumShards, ShardId,
    ValidatorInfoIdentifier,
};
use near_primitives::version::ProtocolVersion;
use near_primitives::views::EpochValidatorInfo;
use near_store::{ShardUId, StoreUpdate};
use std::cmp::Ordering;
use std::sync::Arc;

/// A trait that abstracts the interface of the EpochManager.
/// The two implementations are EpochManagerHandle and KeyValueEpochManager.
/// Strongly prefer the former whenever possible. The latter is for legacy
/// tests.
pub trait EpochManagerAdapter: Send + Sync {
    /// Check if epoch exists.
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool;

    /// Get current number of shards.
    fn num_shards(&self, epoch_id: &EpochId) -> Result<ShardId, EpochError>;

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

    /// Converts `ShardId` (index of shard in the *current* layout) to
    /// `ShardUId` (`ShardId` + the version of shard layout itself.)
    fn shard_id_to_uid(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<ShardUId, EpochError>;

    fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError>;

    fn get_epoch_config(&self, epoch_id: &EpochId) -> Result<EpochConfig, EpochError>;

    fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError>;

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError>;

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, EpochError>;

    /// Returns true, if given hash is last block in it's epoch.
    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError>;

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
    fn get_prev_shard_ids(
        &self,
        prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<ShardId>, Error>;

    /// Get shard layout given hash of previous block.
    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, EpochError>;

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

    /// Block producers for given height for the main block. Return EpochError if outside of known boundaries.
    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, EpochError>;

    /// Chunk producer for given height for given shard. Return EpochError if outside of known boundaries.
    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, EpochError>;

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
        block_header_info: BlockHeaderInfo,
    ) -> Result<StoreUpdate, EpochError>;

    /// Amount of tokens minted in given epoch.
    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, EpochError>;

    /// Epoch active protocol version.
    fn get_epoch_protocol_version(&self, epoch_id: &EpochId)
        -> Result<ProtocolVersion, EpochError>;

    // TODO #3488 this likely to be updated
    /// Data that is necessary for prove Epochs in Epoch Sync.
    fn get_epoch_sync_data(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<
        (
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
        ),
        EpochError,
    >;

    // TODO #3488 this likely to be updated
    /// Hash that is necessary for prove Epochs in Epoch Sync.
    fn get_epoch_sync_data_hash(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<CryptoHash, EpochError> {
        let (
            prev_epoch_first_block_info,
            prev_epoch_prev_last_block_info,
            prev_epoch_last_block_info,
            prev_epoch_info,
            cur_epoch_info,
            next_epoch_info,
        ) = self.get_epoch_sync_data(prev_epoch_last_block_hash, epoch_id, next_epoch_id)?;
        Ok(CryptoHash::hash_borsh(&(
            prev_epoch_first_block_info,
            prev_epoch_prev_last_block_info,
            prev_epoch_last_block_info,
            prev_epoch_info,
            cur_epoch_info,
            next_epoch_info,
        )))
    }

    /// Epoch Manager init procedure that is necessary after Epoch Sync.
    fn epoch_sync_init_epoch_manager(
        &self,
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

    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: BlockHeight,
        prev_random_value: &CryptoHash,
        vrf_value: &near_crypto::vrf::Value,
        vrf_proof: &near_crypto::vrf::Proof,
    ) -> Result<(), Error>;

    /// Verify validator signature for the given epoch.
    /// Note: doesnt't account for slashed accounts within given epoch. USE WITH CAUTION.
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

    /// Verify header signature.
    fn verify_header_signature(&self, header: &BlockHeader) -> Result<bool, Error>;

    /// Verify chunk header signature.
    /// return false if the header signature does not match the key for the assigned chunk producer
    /// for this chunk, or if the chunk producer has been slashed
    /// return `EpochError::NotAValidator` if cannot find chunk producer info for this chunk
    /// `header`: chunk header
    /// `epoch_id`: epoch_id that the chunk header belongs to
    /// `last_known_hash`: used to determine the list of chunk producers that are slashed
    fn verify_chunk_header_signature(
        &self,
        header: &ShardChunkHeader,
        epoch_id: &EpochId,
        last_known_hash: &CryptoHash,
    ) -> Result<bool, Error> {
        self.verify_chunk_signature_with_header_parts(
            &header.chunk_hash(),
            header.signature(),
            epoch_id,
            last_known_hash,
            header.height_created(),
            header.shard_id(),
        )
    }

    fn verify_chunk_signature_with_header_parts(
        &self,
        chunk_hash: &ChunkHash,
        signature: &Signature,
        epoch_id: &EpochId,
        last_known_hash: &CryptoHash,
        height_created: BlockHeight,
        shard_id: ShardId,
    ) -> Result<bool, Error>;

    /// Verify aggregated bls signature
    fn verify_approval(
        &self,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<bool, Error>;

    /// Verify approvals and check threshold, but ignore next epoch approvals and slashing
    fn verify_approvals_and_threshold_orphan(
        &self,
        epoch_id: &EpochId,
        can_approved_block_be_produced: &dyn Fn(
            &[Option<Signature>],
            // (stake this in epoch, stake in next epoch, is_slashed)
            &[(Balance, Balance, bool)],
        ) -> bool,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<(), Error>;

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
}

impl EpochManagerAdapter for EpochManagerHandle {
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_info(epoch_id).is_ok()
    }

    fn num_shards(&self, epoch_id: &EpochId) -> Result<NumShards, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_shard_layout(epoch_id).map_err(EpochError::from)?.num_shards())
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
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).map_err(EpochError::from)?;
        Ok(account_id_to_shard_id(account_id, &shard_layout))
    }

    fn shard_id_to_uid(
        &self,
        shard_id: ShardId,
        epoch_id: &EpochId,
    ) -> Result<ShardUId, EpochError> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).map_err(EpochError::from)?;
        Ok(ShardUId::from_shard_id_and_layout(shard_id, &shard_layout))
    }

    fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_block_info(hash).map_err(EpochError::from)?)
    }

    fn get_epoch_config(&self, epoch_id: &EpochId) -> Result<EpochConfig, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_config(epoch_id).map_err(EpochError::from)?)
    }

    fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_info(epoch_id).map_err(EpochError::from)?)
    }

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_shard_layout(epoch_id)
    }

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, EpochError> {
        let epoch_manager = self.read();
        let epoch_config = epoch_manager.get_epoch_config(epoch_id).map_err(EpochError::from)?;
        Ok(ShardConfig::new(epoch_config))
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.is_next_block_epoch_start(parent_hash).map_err(EpochError::from)
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
        epoch_manager
            .get_epoch_info(&epoch_id)
            .map(|info| info.epoch_height())
            .map_err(EpochError::from)
    }

    fn get_next_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_next_epoch_id(block_hash).map_err(EpochError::from)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_next_epoch_id_from_prev_block(parent_hash).map_err(EpochError::from)
    }

    fn get_prev_shard_ids(
        &self,
        prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<ShardId>, Error> {
        if self.is_next_block_epoch_start(prev_hash)? {
            let shard_layout = self.get_shard_layout_from_prev_block(prev_hash)?;
            let prev_shard_layout = self.get_shard_layout(&self.get_epoch_id(prev_hash)?)?;
            if prev_shard_layout != shard_layout {
                return Ok(shard_ids
                    .into_iter()
                    .map(|shard_id| {
                        shard_layout.get_parent_shard_id(shard_id).map(|parent_shard_id|{
                            assert!(parent_shard_id < prev_shard_layout.num_shards(),
                                    "invalid shard layout {:?}: parent shard {} does not exist in last shard layout",
                                    shard_layout,
                                    parent_shard_id
                            );
                            parent_shard_id
                        })
                    })
                    .collect::<Result<_, ShardLayoutError>>()?);
            }
        }
        Ok(shard_ids)
    }

    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, EpochError> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        self.get_shard_layout(&epoch_id)
    }

    fn get_epoch_id(&self, block_hash: &CryptoHash) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_id(block_hash).map_err(EpochError::from)
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
        epoch_manager.get_epoch_start_height(block_hash).map_err(EpochError::from)
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, EpochError> {
        let epoch_manager = self.read();
        if epoch_manager.is_next_block_epoch_start(prev_block_hash)? {
            epoch_manager.get_epoch_id(prev_block_hash).map_err(EpochError::from)
        } else {
            epoch_manager.get_prev_epoch_id(prev_block_hash).map_err(EpochError::from)
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

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?.take_account_id())
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_chunk_producer_info(epoch_id, height, shard_id)?.take_account_id())
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
        block_header_info: BlockHeaderInfo,
    ) -> Result<StoreUpdate, EpochError> {
        let mut epoch_manager = self.write();
        epoch_manager.add_validator_proposals(block_header_info)
    }

    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.minted_amount())
    }

    fn get_epoch_protocol_version(
        &self,
        epoch_id: &EpochId,
    ) -> Result<ProtocolVersion, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.protocol_version())
    }

    // TODO #3488 this likely to be updated
    fn get_epoch_sync_data(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<
        (
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<BlockInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
            Arc<EpochInfo>,
        ),
        EpochError,
    > {
        let epoch_manager = self.read();
        let last_block_info = epoch_manager.get_block_info(prev_epoch_last_block_hash)?;
        let prev_epoch_id = last_block_info.epoch_id().clone();
        Ok((
            epoch_manager.get_block_info(last_block_info.epoch_first_block())?,
            epoch_manager.get_block_info(last_block_info.prev_hash())?,
            last_block_info,
            epoch_manager.get_epoch_info(&prev_epoch_id)?,
            epoch_manager.get_epoch_info(epoch_id)?,
            epoch_manager.get_epoch_info(next_epoch_id)?,
        ))
    }

    fn epoch_sync_init_epoch_manager(
        &self,
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
        epoch_manager
            .init_after_epoch_sync(
                prev_epoch_first_block_info,
                prev_epoch_prev_last_block_info,
                prev_epoch_last_block_info,
                prev_epoch_id,
                prev_epoch_info,
                epoch_id,
                epoch_info,
                next_epoch_id,
                next_epoch_info,
            )?
            .commit()
            .map_err(|err| err.into())
    }

    fn verify_block_vrf(
        &self,
        epoch_id: &EpochId,
        block_height: BlockHeight,
        prev_random_value: &CryptoHash,
        vrf_value: &near_crypto::vrf::Value,
        vrf_proof: &near_crypto::vrf::Proof,
    ) -> Result<(), Error> {
        let epoch_manager = self.read();
        let validator = epoch_manager.get_block_producer_info(epoch_id, block_height)?;
        let public_key = near_crypto::key_conversion::convert_public_key(
            validator.public_key().unwrap_as_ed25519(),
        )
        .unwrap();

        if !public_key.is_vrf_valid(&prev_random_value.as_ref(), vrf_value, vrf_proof) {
            return Err(Error::InvalidRandomnessBeaconOutput);
        }
        Ok(())
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
            Err(Error::NotAValidator) => {
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

    /// Returns true if the header signature is signed by the assigned block producer and the block
    /// producer is not slashed
    /// This function requires that the previous block of `header` has been processed.
    /// If not, it returns EpochError::MissingBlock.
    fn verify_header_signature(&self, header: &BlockHeader) -> Result<bool, Error> {
        let epoch_manager = self.read();
        let block_producer =
            epoch_manager.get_block_producer_info(header.epoch_id(), header.height())?;
        match epoch_manager.get_block_info(header.prev_hash()) {
            Ok(block_info) => {
                if block_info.slashed().contains_key(block_producer.account_id()) {
                    return Ok(false);
                }
                Ok(header.signature().verify(header.hash().as_ref(), block_producer.public_key()))
            }
            Err(_) => return Err(EpochError::MissingBlock(*header.prev_hash()).into()),
        }
    }

    fn verify_chunk_signature_with_header_parts(
        &self,
        chunk_hash: &ChunkHash,
        signature: &Signature,
        epoch_id: &EpochId,
        last_known_hash: &CryptoHash,
        height_created: BlockHeight,
        shard_id: ShardId,
    ) -> Result<bool, Error> {
        let epoch_manager = self.read();
        let chunk_producer =
            epoch_manager.get_chunk_producer_info(epoch_id, height_created, shard_id)?;
        let block_info = epoch_manager.get_block_info(last_known_hash)?;
        if block_info.slashed().contains_key(chunk_producer.account_id()) {
            return Ok(false);
        }
        Ok(signature.verify(chunk_hash.as_ref(), chunk_producer.public_key()))
    }

    fn verify_approval(
        &self,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<bool, Error> {
        let info = {
            let epoch_manager = self.read();
            epoch_manager
                .get_all_block_approvers_ordered(prev_block_hash)
                .map_err(EpochError::from)?
        };
        if approvals.len() > info.len() {
            return Ok(false);
        }

        let message_to_sign = Approval::get_data_for_sig(
            &if prev_block_height + 1 == block_height {
                ApprovalInner::Endorsement(*prev_block_hash)
            } else {
                ApprovalInner::Skip(prev_block_height)
            },
            block_height,
        );

        for ((validator, is_slashed), may_be_signature) in info.into_iter().zip(approvals.iter()) {
            if let Some(signature) = may_be_signature {
                if is_slashed || !signature.verify(message_to_sign.as_ref(), &validator.public_key)
                {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn verify_approvals_and_threshold_orphan(
        &self,
        epoch_id: &EpochId,
        can_approved_block_be_produced: &dyn Fn(
            &[Option<Signature>],
            &[(Balance, Balance, bool)],
        ) -> bool,
        prev_block_hash: &CryptoHash,
        prev_block_height: BlockHeight,
        block_height: BlockHeight,
        approvals: &[Option<Signature>],
    ) -> Result<(), Error> {
        let info = {
            let epoch_manager = self.read();
            epoch_manager
                .get_heuristic_block_approvers_ordered(epoch_id)
                .map_err(EpochError::from)?
        };

        let message_to_sign = Approval::get_data_for_sig(
            &if prev_block_height + 1 == block_height {
                ApprovalInner::Endorsement(*prev_block_hash)
            } else {
                ApprovalInner::Skip(prev_block_height)
            },
            block_height,
        );

        for (validator, may_be_signature) in info.iter().zip(approvals.iter()) {
            if let Some(signature) = may_be_signature {
                if !signature.verify(message_to_sign.as_ref(), &validator.public_key) {
                    return Err(Error::InvalidApprovals);
                }
            }
        }
        let stakes = info
            .iter()
            .map(|stake| (stake.stake_this_epoch, stake.stake_next_epoch, false))
            .collect::<Vec<_>>();
        if !can_approved_block_be_produced(approvals, &stakes) {
            Err(Error::NotEnoughApprovals)
        } else {
            Ok(())
        }
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
}
