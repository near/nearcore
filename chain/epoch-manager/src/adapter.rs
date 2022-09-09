use std::{
    cmp::Ordering,
    sync::{Arc, RwLockReadGuard, RwLockWriteGuard},
};

use borsh::BorshSerialize;
use near_chain_primitives::Error;
use near_crypto::Signature;
use near_primitives::{
    block_header::{Approval, ApprovalInner, BlockHeader},
    epoch_manager::{block_info::BlockInfo, epoch_info::EpochInfo, ShardConfig},
    errors::EpochError,
    hash::CryptoHash,
    shard_layout::{account_id_to_shard_id, ShardLayout, ShardLayoutError},
    sharding::{ChunkHash, ShardChunkHeader},
    types::{
        validator_stake::ValidatorStake, AccountId, ApprovalStake, Balance, BlockHeight,
        EpochHeight, EpochId, NumShards, ShardId, ValidatorInfoIdentifier,
    },
    version::ProtocolVersion,
    views::EpochValidatorInfo,
};
use near_store::ShardUId;

use crate::{EpochManager, EpochManagerHandle};

/// A trait that abstracts the interface of the EpochManager.
///
/// It is intended to be an intermediate state in a refactor: we want to remove
/// epoch manager stuff from RuntimeAdapter's interface, and, as a first step,
/// we move it to a new trait. The end goal is for the code to use the concrete
/// epoch manager type directly. Though, we might want to still keep this trait
/// in, to allow for easy overriding of epoch manager in tests.
pub trait EpochManagerAdapter: Send + Sync {
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
    /// return `Error::NotAValidator` if cannot find chunk producer info for this chunk
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

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error>;

    fn get_fisherman_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error>;

    /// Epoch block producers ordered by their order in the proposals.
    /// Returns error if height is outside of known boundaries.
    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error>;

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, Error>;

    /// Returns all the chunk producers for a given epoch.
    fn get_epoch_chunk_producers(&self, epoch_id: &EpochId) -> Result<Vec<ValidatorStake>, Error>;

    /// Block producers for given height for the main block. Return error if outside of known boundaries.
    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, Error>;

    /// Chunk producer for given height for given shard. Return error if outside of known boundaries.
    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, Error>;

    /// Get current number of shards.
    fn num_shards(&self, epoch_id: &EpochId) -> Result<ShardId, Error>;

    fn num_total_parts(&self) -> usize;

    fn num_data_parts(&self) -> usize;

    /// Account Id to Shard Id mapping, given current number of shards.
    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardId, Error>;

    /// Returns `account_id` that suppose to have the `part_id`.
    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, Error>;

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, Error>;

    /// Get shard layout given hash of previous block.
    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, Error>;

    fn shard_id_to_uid(&self, shard_id: ShardId, epoch_id: &EpochId) -> Result<ShardUId, Error>;

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, Error>;

    /// Returns true if the shard layout will change in the next epoch
    /// Current epoch is the epoch of the block after `parent_hash`
    fn will_shard_layout_change_next_epoch(&self, parent_hash: &CryptoHash) -> Result<bool, Error>;

    /// Returns true, if given hash is last block in it's epoch.
    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, Error>;

    /// Get epoch id given hash of previous block.
    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error>;

    /// Get epoch height given hash of previous block.
    fn get_epoch_height_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochHeight, Error>;

    /// Get next epoch id given hash of previous block.
    fn get_next_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash)
        -> Result<EpochId, Error>;

    /// Get epoch start for given block hash.
    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error>;

    /// Check if epoch exists.
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool;

    /// Amount of tokens minted in given epoch.
    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, Error>;

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
        Error,
    >;

    // TODO #3488 this likely to be updated
    /// Hash that is necessary for prove Epochs in Epoch Sync.
    fn get_epoch_sync_data_hash(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<CryptoHash, Error>;

    /// Epoch active protocol version.
    fn get_epoch_protocol_version(&self, epoch_id: &EpochId) -> Result<ProtocolVersion, Error>;

    /// Get previous epoch id by hash of previous block.
    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, Error>;

    fn get_protocol_upgrade_block_height(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError>;

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error>;

    fn chunk_needs_to_be_fetched_from_archival(
        &self,
        chunk_prev_block_hash: &CryptoHash,
        header_head: &CryptoHash,
    ) -> Result<bool, Error>;

    /// WARNING: this call may be expensive.
    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, Error>;

    fn get_prev_shard_ids(
        &self,
        prev_hash: &CryptoHash,
        shard_ids: Vec<ShardId>,
    ) -> Result<Vec<ShardId>, Error>;

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
    ) -> Result<(), Error>;
}

pub trait HasEpochMangerHandle {
    fn write(&self) -> RwLockWriteGuard<EpochManager>;
    fn read(&self) -> RwLockReadGuard<EpochManager>;
    fn get_epoch_id(&self, hash: &CryptoHash) -> Result<EpochId, Error> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_id(hash).map_err(Error::from)
    }
}

impl HasEpochMangerHandle for EpochManagerHandle {
    fn write(&self) -> RwLockWriteGuard<EpochManager> {
        self.write()
    }
    fn read(&self) -> RwLockReadGuard<EpochManager> {
        self.read()
    }
}

impl<T: HasEpochMangerHandle + Send + Sync> EpochManagerAdapter for T {
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
            epoch_manager.get_all_block_approvers_ordered(prev_block_hash).map_err(Error::from)?
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

    fn get_validator_by_account_id(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
        account_id: &AccountId,
    ) -> Result<(ValidatorStake, bool), Error> {
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
    ) -> Result<(ValidatorStake, bool), Error> {
        let epoch_manager = self.read();
        let fisherman = epoch_manager.get_fisherman_by_account_id(epoch_id, account_id)?;
        let block_info = epoch_manager.get_block_info(last_known_block_hash)?;
        Ok((fisherman, block_info.slashed().contains_key(account_id)))
    }

    fn get_epoch_block_producers_ordered(
        &self,
        epoch_id: &EpochId,
        last_known_block_hash: &CryptoHash,
    ) -> Result<Vec<(ValidatorStake, bool)>, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_all_block_producers_ordered(epoch_id, last_known_block_hash)?.to_vec())
    }

    fn get_epoch_block_approvers_ordered(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<Vec<(ApprovalStake, bool)>, Error> {
        let epoch_manager = self.read();
        epoch_manager.get_all_block_approvers_ordered(parent_hash).map_err(Error::from)
    }
    fn get_epoch_chunk_producers(&self, epoch_id: &EpochId) -> Result<Vec<ValidatorStake>, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_all_chunk_producers(epoch_id)?.to_vec())
    }

    fn get_block_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
    ) -> Result<AccountId, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_block_producer_info(epoch_id, height)?.take_account_id())
    }

    fn get_chunk_producer(
        &self,
        epoch_id: &EpochId,
        height: BlockHeight,
        shard_id: ShardId,
    ) -> Result<AccountId, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_chunk_producer_info(epoch_id, height, shard_id)?.take_account_id())
    }

    fn num_shards(&self, epoch_id: &EpochId) -> Result<NumShards, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?.num_shards())
    }

    fn num_total_parts(&self) -> usize {
        let seats = self.read().config.for_genesis().num_block_producer_seats;
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

    fn account_id_to_shard_id(
        &self,
        account_id: &AccountId,
        epoch_id: &EpochId,
    ) -> Result<ShardId, Error> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?;
        Ok(account_id_to_shard_id(account_id, &shard_layout))
    }

    fn get_part_owner(&self, epoch_id: &EpochId, part_id: u64) -> Result<AccountId, Error> {
        let epoch_manager = self.read();
        let epoch_info = epoch_manager.get_epoch_info(&epoch_id)?;
        let settlement = epoch_info.block_producers_settlement();
        let validator_id = settlement[part_id as usize % settlement.len()];
        Ok(epoch_info.get_validator(validator_id).account_id().clone())
    }

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?.clone())
    }

    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, Error> {
        let epoch_id = self.get_epoch_id_from_prev_block(parent_hash)?;
        self.get_shard_layout(&epoch_id)
    }

    fn shard_id_to_uid(&self, shard_id: ShardId, epoch_id: &EpochId) -> Result<ShardUId, Error> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id).map_err(Error::from)?;
        Ok(ShardUId::from_shard_id_and_layout(shard_id, &shard_layout))
    }

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, Error> {
        let epoch_manager = self.read();
        let epoch_config = epoch_manager.get_epoch_config(epoch_id).map_err(Error::from)?;
        Ok(ShardConfig::new(epoch_config))
    }

    fn will_shard_layout_change_next_epoch(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.will_shard_layout_change(parent_hash)?)
    }

    fn is_next_block_epoch_start(&self, parent_hash: &CryptoHash) -> Result<bool, Error> {
        let epoch_manager = self.read();
        epoch_manager.is_next_block_epoch_start(parent_hash).map_err(Error::from)
    }

    fn get_epoch_id_from_prev_block(&self, parent_hash: &CryptoHash) -> Result<EpochId, Error> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_epoch_height_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochHeight, Error> {
        let epoch_manager = self.read();
        let epoch_id = epoch_manager.get_epoch_id_from_prev_block(prev_block_hash)?;
        epoch_manager.get_epoch_info(&epoch_id).map(|info| info.epoch_height()).map_err(Error::from)
    }

    fn get_next_epoch_id_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let epoch_manager = self.read();
        epoch_manager.get_next_epoch_id_from_prev_block(parent_hash).map_err(Error::from)
    }

    fn get_epoch_start_height(&self, block_hash: &CryptoHash) -> Result<BlockHeight, Error> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_start_height(block_hash).map_err(Error::from)
    }

    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_info(epoch_id).is_ok()
    }

    fn get_epoch_minted_amount(&self, epoch_id: &EpochId) -> Result<Balance, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.minted_amount())
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
        Error,
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

    // TODO #3488 this likely to be updated
    fn get_epoch_sync_data_hash(
        &self,
        prev_epoch_last_block_hash: &CryptoHash,
        epoch_id: &EpochId,
        next_epoch_id: &EpochId,
    ) -> Result<CryptoHash, Error> {
        let (
            prev_epoch_first_block_info,
            prev_epoch_prev_last_block_info,
            prev_epoch_last_block_info,
            prev_epoch_info,
            cur_epoch_info,
            next_epoch_info,
        ) = self.get_epoch_sync_data(prev_epoch_last_block_hash, epoch_id, next_epoch_id)?;
        let mut data = prev_epoch_first_block_info.try_to_vec().unwrap();
        data.extend(prev_epoch_prev_last_block_info.try_to_vec().unwrap());
        data.extend(prev_epoch_last_block_info.try_to_vec().unwrap());
        data.extend(prev_epoch_info.try_to_vec().unwrap());
        data.extend(cur_epoch_info.try_to_vec().unwrap());
        data.extend(next_epoch_info.try_to_vec().unwrap());
        Ok(CryptoHash::hash_bytes(data.as_slice()))
    }

    fn get_epoch_protocol_version(&self, epoch_id: &EpochId) -> Result<ProtocolVersion, Error> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_epoch_info(epoch_id)?.protocol_version())
    }

    fn get_prev_epoch_id_from_prev_block(
        &self,
        prev_block_hash: &CryptoHash,
    ) -> Result<EpochId, Error> {
        let epoch_manager = self.read();
        if epoch_manager.is_next_block_epoch_start(prev_block_hash)? {
            epoch_manager.get_epoch_id(prev_block_hash).map_err(Error::from)
        } else {
            epoch_manager.get_prev_epoch_id(prev_block_hash).map_err(Error::from)
        }
    }

    fn get_protocol_upgrade_block_height(
        &self,
        block_hash: CryptoHash,
    ) -> Result<Option<BlockHeight>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_protocol_upgrade_block_height(block_hash)
    }

    fn compare_epoch_id(
        &self,
        epoch_id: &EpochId,
        other_epoch_id: &EpochId,
    ) -> Result<Ordering, Error> {
        let epoch_manager = self.read();
        epoch_manager.compare_epoch_id(epoch_id, other_epoch_id).map_err(|e| e.into())
    }

    fn chunk_needs_to_be_fetched_from_archival(
        &self,
        chunk_prev_block_hash: &CryptoHash,
        header_head: &CryptoHash,
    ) -> Result<bool, Error> {
        let epoch_manager = self.read();
        let head_epoch_id = epoch_manager.get_epoch_id(header_head)?;
        let head_next_epoch_id = epoch_manager.get_next_epoch_id(header_head)?;
        let chunk_epoch_id = epoch_manager.get_epoch_id_from_prev_block(chunk_prev_block_hash)?;
        let chunk_next_epoch_id =
            epoch_manager.get_next_epoch_id_from_prev_block(chunk_prev_block_hash)?;

        // `chunk_epoch_id != head_epoch_id && chunk_next_epoch_id != head_epoch_id` covers the
        // common case: the chunk is in the current epoch, or in the previous epoch, relative to the
        // header head. The third condition (`chunk_epoch_id != head_next_epoch_id`) covers a
        // corner case, in which the `header_head` is the last block of an epoch, and the chunk is
        // for the next block. In this case the `chunk_epoch_id` will be one epoch ahead of the
        // `header_head`.
        Ok(chunk_epoch_id != head_epoch_id
            && chunk_next_epoch_id != head_epoch_id
            && chunk_epoch_id != head_next_epoch_id)
    }

    /// WARNING: this function calls EpochManager::get_epoch_info_aggregator_upto_last
    /// underneath which can be very expensive.
    fn get_validator_info(
        &self,
        epoch_id: ValidatorInfoIdentifier,
    ) -> Result<EpochValidatorInfo, Error> {
        let epoch_manager = self.read();
        epoch_manager.get_validator_info(epoch_id).map_err(|e| e.into())
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
    ) -> Result<(), Error> {
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
}
