use super::{BlockChainStorage, GenericStorage};
use crate::storages::ChainId;
use crate::KeyValueDB;
use primitives::beacon::SignedBeaconBlock;
use primitives::beacon::SignedBeaconBlockHeader;
use std::sync::Arc;

/// Beacon chain does not require additional behavior besides storing and retrieving blocks. Later,
/// we can add authority info.
pub struct BeaconChainStorage {
    generic_storage: BlockChainStorage<SignedBeaconBlockHeader, SignedBeaconBlock>,
}

impl GenericStorage<SignedBeaconBlockHeader, SignedBeaconBlock> for BeaconChainStorage {
    #[inline]
    fn blockchain_storage_mut(
        &mut self,
    ) -> &mut BlockChainStorage<SignedBeaconBlockHeader, SignedBeaconBlock> {
        &mut self.generic_storage
    }
}

impl BeaconChainStorage {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        Self { generic_storage: BlockChainStorage::new(storage, ChainId::BeaconChain) }
    }
}
