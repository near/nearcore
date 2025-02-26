use std::collections::HashMap;
use std::sync::{self, Arc};

use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::{self, EpochInfo};
use near_primitives::epoch_manager::{EpochConfig, EpochConfigStore};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{self, EpochId, ProtocolVersion};

use crate::EpochManagerAdapter;


struct TestEpochManager {
    epoch_configs: HashMap<ProtocolVersion, EpochConfig>,
}

impl TestEpochManager {
    fn add_epoch_config(mut self, protocol_version: ProtocolVersion, epoch_config: EpochConfig) -> Self {
        self.epoch_configs.insert(protocol_version, epoch_config);
        self
    }
}

impl EpochManagerAdapter for TestEpochManager {
    fn get_epoch_config_from_protocol_version(
        &self,
        protocol_version: ProtocolVersion,
    ) -> EpochConfig {
        todo!()
    }

    fn get_block_info(&self, hash: &CryptoHash) -> Result<Arc<BlockInfo>, EpochError> {
        todo!()
    }

    fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<Arc<EpochInfo>, EpochError> {
        todo!()
    }

    fn get_epoch_start_from_epoch_id(&self, epoch_id: &EpochId) -> Result<types::BlockHeight, EpochError> {
        todo!()
    }

    fn num_total_parts(&self) -> usize {
        todo!()
    }

    fn get_validator_info(
        &self,
        epoch_identifier: types::ValidatorInfoIdentifier,
    ) -> Result<near_primitives::views::EpochValidatorInfo, EpochError> {
        todo!()
    }

    fn add_validator_proposals(
        &self,
        block_info: BlockInfo,
        random_value: CryptoHash,
    ) -> Result<near_store::StoreUpdate, EpochError> {
    }

    fn init_after_epoch_sync(
        &self,
        store_update: &mut near_store::StoreUpdate,
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
        todo!()
    }
}