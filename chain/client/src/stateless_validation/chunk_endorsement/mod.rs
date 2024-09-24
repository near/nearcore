use std::sync::Arc;

use near_chain::ChainStoreAccess;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::validator_assignment::ChunkEndorsementsState;
use near_primitives::version::ProtocolFeature;
use near_store::Store;

use crate::Client;

mod tracker_v1;
mod tracker_v2;

/// Module to track chunk endorsements received from chunk validators.
pub struct ChunkEndorsementTracker {
    epoch_manager: Arc<dyn EpochManagerAdapter>,
    pub tracker_v1: tracker_v1::ChunkEndorsementTracker,
    pub tracker_v2: tracker_v2::ChunkEndorsementTracker,
}

impl Client {
    pub fn process_chunk_endorsement(
        &mut self,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        // TODO(ChunkEndorsementV2): Remove chunk_header once tracker_v1 is deprecated
        let chunk_hash = match &endorsement {
            ChunkEndorsement::V1(endorsement) => endorsement.chunk_hash(),
            ChunkEndorsement::V2(endorsement) => endorsement.chunk_hash(),
        };
        let chunk_header = match self.chain.chain_store().get_partial_chunk(chunk_hash) {
            Ok(chunk) => Some(chunk.cloned_header()),
            Err(Error::ChunkMissing(_)) => None,
            Err(error) => return Err(error),
        };
        self.chunk_endorsement_tracker.process_chunk_endorsement(endorsement, chunk_header)
    }
}

impl ChunkEndorsementTracker {
    pub fn new(epoch_manager: Arc<dyn EpochManagerAdapter>, store: Store) -> Self {
        Self {
            tracker_v1: tracker_v1::ChunkEndorsementTracker::new(epoch_manager.clone()),
            tracker_v2: tracker_v2::ChunkEndorsementTracker::new(epoch_manager.clone(), store),
            epoch_manager,
        }
    }

    // TODO(ChunkEndorsementV2): Remove chunk_header once tracker_v1 is deprecated
    pub fn process_chunk_endorsement(
        &mut self,
        endorsement: ChunkEndorsement,
        chunk_header: Option<ShardChunkHeader>,
    ) -> Result<(), Error> {
        match endorsement {
            ChunkEndorsement::V1(endorsement) => {
                self.tracker_v1.process_chunk_endorsement(endorsement, chunk_header)
            }
            ChunkEndorsement::V2(endorsement) => {
                self.tracker_v2.process_chunk_endorsement(endorsement)
            }
        }
    }

    pub fn collect_chunk_endorsements(
        &mut self,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkEndorsementsState, Error> {
        let epoch_id =
            self.epoch_manager.get_epoch_id_from_prev_block(chunk_header.prev_block_hash())?;
        let protocol_version = self.epoch_manager.get_epoch_protocol_version(&epoch_id)?;
        if !ProtocolFeature::ChunkEndorsementV2.enabled(protocol_version) {
            self.tracker_v1.collect_chunk_endorsements(chunk_header)
        } else {
            self.tracker_v2.collect_chunk_endorsements(chunk_header)
        }
    }
}
