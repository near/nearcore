use super::{BlockChainStorage, GenericStorage};
use crate::storages::ChainId;
use crate::Storage;
use primitives::beacon::SignedBeaconBlock;
use primitives::beacon::SignedBeaconBlockHeader;
use primitives::hash::CryptoHash;
use std::sync::Arc;

/// Beacon chain does not require additional behavior besides storing and retrieving blocks.
pub struct BeaconChainStorage {
    generic_storage: BlockChainStorage<SignedBeaconBlockHeader, SignedBeaconBlock>,
}

impl GenericStorage for BeaconChainStorage {
    type SignedHeader = SignedBeaconBlockHeader;
    type SignedBlock = SignedBeaconBlock;

    #[inline]
    fn blockchain_storage_mut(
        &mut self,
    ) -> &mut BlockChainStorage<Self::SignedHeader, Self::SignedBlock> {
        &mut self.generic_storage
    }
}

impl BeaconChainStorage {
    pub fn new(storage: Arc<Storage>, genesis_hash: CryptoHash) -> Self {
        Self {
            generic_storage: BlockChainStorage::new(storage, ChainId::BeaconChain, genesis_hash),
        }
    }
}
