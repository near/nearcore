use borsh::BorshDeserialize;
use near_chain_primitives::Error;
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::AGGREGATOR_KEY;
use near_primitives::epoch_sync::{EpochSyncProof, EpochSyncProofV1};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
use near_primitives::types::{BlockHeight, EpochId};

use crate::{DBCol, Store};

use super::StoreAdapter;

#[derive(Clone)]
pub struct EpochStoreAdapter {
    store: Store,
}

impl StoreAdapter for EpochStoreAdapter {
    fn store_ref(&self) -> &Store {
        &self.store
    }
}

impl EpochStoreAdapter {
    pub fn new(store: Store) -> Self {
        Self { store }
    }

    pub fn get_epoch_start(&self, epoch_id: &EpochId) -> Result<BlockHeight, EpochError> {
        self.store
            .get_ser::<BlockHeight>(DBCol::EpochStart, epoch_id.as_ref())?
            .ok_or(EpochError::EpochOutOfBounds(*epoch_id))
    }

    /// Get BlockInfo for a block
    /// Errors
    ///   EpochError::IOErr if storage returned an error
    ///   EpochError::MissingBlock if block is not in storage
    pub fn get_block_info(&self, hash: &CryptoHash) -> Result<BlockInfo, EpochError> {
        self.store
            .get_ser::<BlockInfo>(DBCol::BlockInfo, hash.as_ref())?
            .ok_or(EpochError::MissingBlock(*hash))
    }

    pub fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<EpochInfo, EpochError> {
        self.store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, epoch_id.as_ref())?
            .ok_or(EpochError::EpochOutOfBounds(*epoch_id))
    }

    // Iterate over all the epoch infos in store
    pub fn iter_epoch_info<'a>(&'a self) -> impl Iterator<Item = (EpochId, EpochInfo)> + 'a {
        self.store
            .iter(DBCol::EpochInfo)
            .map(Result::unwrap)
            .filter(|(key, _)| key.as_ref() != AGGREGATOR_KEY)
            .map(|(key, value)| {
                (
                    EpochId::try_from_slice(key.as_ref()).unwrap(),
                    EpochInfo::try_from_slice(value.as_ref()).unwrap(),
                )
            })
    }

    pub fn get_epoch_sync_proof(&self) -> Result<Option<EpochSyncProofV1>, Error> {
        Ok(self
            .store
            .get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[])?
            .map(|proof| proof.into_v1()))
    }
}
