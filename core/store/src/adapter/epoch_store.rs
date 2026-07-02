use super::{StoreAdapter, StoreUpdateAdapter, StoreUpdateHolder};
use crate::db::COMPRESSED_EPOCH_SYNC_PROOF_KEY;
use crate::{DBCol, Store, StoreUpdate, metrics};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::epoch_block_info::BlockInfo;
use near_primitives::epoch_info::EpochInfo;
use near_primitives::epoch_manager::{AGGREGATOR_KEY, EpochSummary};
use near_primitives::epoch_sync::{CompressedEpochSyncProof, EpochSyncProof, EpochSyncProofV1};
use near_primitives::errors::EpochError;
use near_primitives::hash::CryptoHash;
#[cfg(feature = "nightly")]
use near_primitives::types::ShardId;
#[cfg(feature = "nightly")]
use near_primitives::types::validator_stake::ValidatorStake;
use near_primitives::types::{BlockHeight, EpochId};
use near_primitives::utils::compression::CompressedData;
#[cfg(feature = "nightly")]
use near_primitives::utils::get_block_shard_id;
use near_primitives::version::{PROTOCOL_VERSION, ProtocolFeature};

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

    pub fn store_update(&self) -> EpochStoreUpdateAdapter<'static> {
        EpochStoreUpdateAdapter {
            store_update: StoreUpdateHolder::Owned(self.store.store_update()),
        }
    }

    pub fn get_epoch_start(&self, epoch_id: &EpochId) -> Result<BlockHeight, EpochError> {
        self.store
            .get_ser::<BlockHeight>(DBCol::EpochStart, epoch_id.as_ref())
            .ok_or(EpochError::EpochOutOfBounds(*epoch_id))
    }

    /// Get BlockInfo for a block
    /// Errors
    ///   EpochError::IOErr if storage returned an error
    ///   EpochError::MissingBlock if block is not in storage
    pub fn get_block_info(&self, hash: &CryptoHash) -> Result<BlockInfo, EpochError> {
        self.store
            .get_ser::<BlockInfo>(DBCol::BlockInfo, hash.as_ref())
            .ok_or(EpochError::MissingBlock(*hash))
    }

    pub fn get_epoch_info(&self, epoch_id: &EpochId) -> Result<EpochInfo, EpochError> {
        self.store
            .get_ser::<EpochInfo>(DBCol::EpochInfo, epoch_id.as_ref())
            .ok_or(EpochError::EpochOutOfBounds(*epoch_id))
    }

    // Iterate over all the epoch infos in store
    pub fn iter_epoch_info<'a>(&'a self) -> impl Iterator<Item = (EpochId, EpochInfo)> + 'a {
        self.store.iter(DBCol::EpochInfo).filter(|(key, _)| key.as_ref() != AGGREGATOR_KEY).map(
            |(key, value)| {
                (
                    EpochId::try_from_slice(key.as_ref()).unwrap(),
                    EpochInfo::try_from_slice(value.as_ref()).unwrap(),
                )
            },
        )
    }

    pub fn get_epoch_info_aggregator<T: BorshDeserialize>(&self) -> Result<T, EpochError> {
        self.store
            .get_ser::<T>(DBCol::EpochInfo, AGGREGATOR_KEY)
            .ok_or_else(|| EpochError::IOErr("Missing epoch info aggregator".to_string()))
    }

    pub fn get_epoch_validator_info(&self, epoch_id: &EpochId) -> Result<EpochSummary, EpochError> {
        self.store
            .get_ser::<EpochSummary>(DBCol::EpochValidatorInfo, epoch_id.as_ref())
            .ok_or(EpochError::EpochOutOfBounds(*epoch_id))
    }

    pub fn get_compressed_epoch_sync_proof(&self) -> Option<CompressedEpochSyncProof> {
        // Use this function only when ProtocolFeature::ContinuousEpochSync is enabled
        assert!(ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION));
        let proof = self.store.caching_get_ser::<CompressedEpochSyncProof>(
            DBCol::EpochSyncProof,
            COMPRESSED_EPOCH_SYNC_PROOF_KEY,
        );
        proof.as_deref().cloned()
    }

    /// Slightly expensive function, decodes the compressed epoch sync proof
    pub fn get_epoch_sync_proof(&self) -> Result<Option<EpochSyncProofV1>, EpochError> {
        // It's fine to check ProtocolFeature::ContinuousEpochSync against PROTOCOL_VERSION here
        // Enabling ContinuousEpochSync performs a migration to store the compressed proof.
        if ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
            if let Some(proof) = self.get_compressed_epoch_sync_proof() {
                let (decoded_proof, _) = proof.decode()?;
                Ok(Some(decoded_proof.into_v1()))
            } else {
                Ok(None)
            }
        } else {
            Ok(self
                .store
                .get_ser::<EpochSyncProof>(DBCol::EpochSyncProof, &[])
                .map(|proof| proof.into_v1()))
        }
    }
}

pub struct EpochStoreUpdateAdapter<'a> {
    store_update: StoreUpdateHolder<'a>,
}

impl Into<StoreUpdate> for EpochStoreUpdateAdapter<'static> {
    fn into(self) -> StoreUpdate {
        self.store_update.into()
    }
}

impl EpochStoreUpdateAdapter<'static> {
    pub fn commit(self) {
        let store_update: StoreUpdate = self.into();
        store_update.commit();
    }
}

impl<'a> StoreUpdateAdapter for EpochStoreUpdateAdapter<'a> {
    fn store_update(&mut self) -> &mut StoreUpdate {
        &mut self.store_update
    }
}

impl<'a> EpochStoreUpdateAdapter<'a> {
    pub fn new(store_update: &'a mut StoreUpdate) -> Self {
        Self { store_update: StoreUpdateHolder::Reference(store_update) }
    }

    pub fn set_epoch_start(&mut self, epoch_id: &EpochId, start: BlockHeight) {
        self.store_update.set_ser(DBCol::EpochStart, epoch_id.as_ref(), &start);
    }

    pub fn set_block_info(&mut self, block_info: &BlockInfo) {
        self.store_update.insert_ser(DBCol::BlockInfo, block_info.hash().as_ref(), block_info);
    }

    /// Record the chunk producer sampled for chunks anchored at `block_hash` on
    /// `shard_id`, keyed by `(block_hash, shard_id)`. Read back by the
    /// epoch-manager chunk-producer resolvers. Nightly-only: `DBCol::ChunkProducers`
    /// (and an enabled EarlyKickout) only exist under the `nightly` feature.
    #[cfg(feature = "nightly")]
    pub fn set_chunk_producer(
        &mut self,
        block_hash: &CryptoHash,
        shard_id: ShardId,
        chunk_producer: &ValidatorStake,
    ) {
        self.store_update.insert_ser(
            DBCol::ChunkProducers,
            &get_block_shard_id(block_hash, shard_id),
            chunk_producer,
        );
    }

    pub fn set_epoch_info(&mut self, epoch_id: &EpochId, epoch_info: &EpochInfo) {
        self.store_update.set_ser(DBCol::EpochInfo, epoch_id.as_ref(), epoch_info);
    }

    pub fn set_epoch_info_aggregator<T: BorshSerialize + ?Sized>(
        &mut self,
        epoch_info_aggregator: &T,
    ) {
        self.store_update.set_ser(DBCol::EpochInfo, AGGREGATOR_KEY, epoch_info_aggregator);
    }

    pub fn set_epoch_validator_info(&mut self, epoch_id: &EpochId, epoch_summary: &EpochSummary) {
        self.store_update.set_ser(DBCol::EpochValidatorInfo, epoch_id.as_ref(), epoch_summary);
    }

    pub fn set_epoch_sync_proof(&mut self, proof: &EpochSyncProof) {
        // It's fine to check ProtocolFeature::ContinuousEpochSync against PROTOCOL_VERSION here
        // Enabling ContinuousEpochSync performs a migration to store the compressed proof.
        if ProtocolFeature::ContinuousEpochSync.enabled(PROTOCOL_VERSION) {
            let (compressed_proof, _) = CompressedEpochSyncProof::encode(proof).unwrap();
            self.store_update.set_ser(
                DBCol::EpochSyncProof,
                COMPRESSED_EPOCH_SYNC_PROOF_KEY,
                &compressed_proof,
            );
            let compressed_proof_size = compressed_proof.size_bytes() as i64;
            metrics::EPOCH_SYNC_LAST_GENERATED_COMPRESSED_PROOF_SIZE.set(compressed_proof_size);
        } else {
            self.store_update.set_ser(DBCol::EpochSyncProof, &[], &proof);
        }
    }
}
