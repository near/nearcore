use near_chain_primitives::Error;
use near_primitives::{
    hash::CryptoHash,
    types::{
        validator_stake::ValidatorStake, AccountId, ApprovalStake, BlockHeight, EpochId, ShardId,
    },
};

use crate::{EpochManager, EpochManagerHandle};
use std::sync::{RwLockReadGuard, RwLockWriteGuard};

/// A trait that abstracts the interface of the EpochManager.
///
/// It is intended to be an intermediate state in a refactor: we want to remove
/// epoch manager stuff from RuntimeAdapter's interface, and, as a first step,
/// we move it to a new trait. The end goal is for the code to use the concrete
/// epoch manager type directly. Though, we might want to still keep this trait
/// in, to allow for easy overriding of epoch manager in tests.
pub trait EpochManagerAdapter: Send + Sync {
    /// Check if epoch exists.
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool;

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
}

/// A technical plumbing trait to conveniently implement [`EpochManagerAdapter`]
/// for `NightshadeRuntime` without too much copy-paste.
///
/// Once we remove `RuntimeAdapter: EpochManagerAdapter` bound, we could get rid
/// of this trait and instead add inherent methods directly to
/// `EpochManagerHandle`.
pub trait HasEpochMangerHandle {
    fn write(&self) -> RwLockWriteGuard<EpochManager>;
    fn read(&self) -> RwLockReadGuard<EpochManager>;
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
    fn epoch_exists(&self, epoch_id: &EpochId) -> bool {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_info(epoch_id).is_ok()
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
}
