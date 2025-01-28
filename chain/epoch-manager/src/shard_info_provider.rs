use std::{collections::HashSet, sync::Arc};

use near_chain_primitives::Error;
use near_primitives::{
    epoch_manager::ShardConfig,
    errors::EpochError,
    hash::CryptoHash,
    shard_layout::{ShardInfo, ShardLayout},
    stateless_validation::{validator_assignment::ChunkValidatorAssignments, ChunkProductionKey},
    types::{
        validator_stake::ValidatorStake, AccountId, BlockHeight, EpochId, ProtocolVersion, ShardId,
        ShardIndex,
    },
};
use near_store::ShardUId;

use crate::{EpochManagerHandle, EpochManagerInfoProvider};

pub trait ShardInfoProvider {
    /// Get the list of shard ids
    fn shard_ids(&self, epoch_id: &EpochId) -> Result<Vec<ShardId>, EpochError>;

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
    ) -> Result<ShardInfo, EpochError>;

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

    fn get_shard_layout(&self, epoch_id: &EpochId) -> Result<ShardLayout, EpochError>;

    fn get_shard_config(&self, epoch_id: &EpochId) -> Result<ShardConfig, EpochError>;

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
    fn get_prev_shard_id_from_prev_hash(
        &self,
        prev_hash: &CryptoHash,
        shard_id: ShardId,
    ) -> Result<(ShardLayout, ShardId, ShardIndex), EpochError>;

    /// Get shard layout given hash of previous block.
    fn get_shard_layout_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
    ) -> Result<ShardLayout, EpochError>;

    fn get_shard_layout_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> ShardLayout;

    fn get_epoch_chunk_producers_for_shard(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<Vec<AccountId>, EpochError>;

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

    fn cared_about_shard_prev_epoch_from_prev_block(
        &self,
        parent_hash: &CryptoHash,
        account_id: &AccountId,
        shard_id: ShardId,
    ) -> Result<bool, EpochError>;

    fn will_shard_layout_change(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError>;

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

impl ShardInfoProvider for EpochManagerHandle {
    fn shard_ids(&self, epoch_id: &EpochId) -> Result<Vec<ShardId>, EpochError> {
        let epoch_manager = self.read();
        Ok(epoch_manager.get_shard_layout(epoch_id)?.shard_ids().collect())
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
    ) -> Result<ShardInfo, EpochError> {
        let epoch_manager = self.read();
        let shard_layout = epoch_manager.get_shard_layout(epoch_id)?;
        let shard_id = shard_layout.account_id_to_shard_id(account_id);
        let shard_uid = ShardUId::from_shard_id_and_layout(shard_id, &shard_layout);
        let shard_index = shard_layout.get_shard_index(shard_id)?;
        Ok(ShardInfo { shard_index, shard_uid })
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

    fn get_epoch_chunk_producers_for_shard(
        &self,
        epoch_id: &EpochId,
        shard_id: ShardId,
    ) -> Result<Vec<AccountId>, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.get_epoch_chunk_producers_for_shard(epoch_id, shard_id)
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

        let epoch_manager = self.read();
        epoch_manager.cares_about_shard_in_epoch(&prev_epoch_id, account_id, parent_shard_id)
    }

    fn will_shard_layout_change(&self, parent_hash: &CryptoHash) -> Result<bool, EpochError> {
        let epoch_manager = self.read();
        epoch_manager.will_shard_layout_change(parent_hash)
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
