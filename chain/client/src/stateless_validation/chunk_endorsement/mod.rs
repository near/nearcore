use std::sync::Arc;

use near_chain::ChainStoreAccess;
use near_chain_primitives::Error;
use near_epoch_manager::EpochManagerAdapter;
use near_primitives::block_body::ChunkEndorsementSignatures;
use near_primitives::sharding::ShardChunkHeader;
use near_primitives::stateless_validation::chunk_endorsement::ChunkEndorsement;
use near_primitives::stateless_validation::validator_assignment::EndorsementStats;

use crate::Client;

mod tracker_v1;

pub enum ChunkEndorsementsState {
    Endorsed(Option<EndorsementStats>, ChunkEndorsementSignatures),
    NotEnoughStake(Option<EndorsementStats>),
}

impl ChunkEndorsementsState {
    pub fn stats(&self) -> Option<&EndorsementStats> {
        match self {
            Self::Endorsed(stats, _) => stats.as_ref(),
            Self::NotEnoughStake(stats) => stats.as_ref(),
        }
    }
}

/// Module to track chunk endorsements received from chunk validators.
pub struct ChunkEndorsementTracker {
    pub tracker_v1: tracker_v1::ChunkEndorsementTracker,
}

impl Client {
    pub fn process_chunk_endorsement(
        &mut self,
        endorsement: ChunkEndorsement,
    ) -> Result<(), Error> {
        // TODO(ChunkEndorsementV2): Remove chunk_header once tracker_v1 is deprecated
        let chunk_header =
            match self.chain.chain_store().get_partial_chunk(endorsement.chunk_hash()) {
                Ok(chunk) => Some(chunk.cloned_header()),
                Err(Error::ChunkMissing(_)) => None,
                Err(error) => return Err(error),
            };
        self.chunk_endorsement_tracker.process_chunk_endorsement(endorsement, chunk_header)
    }
}

impl ChunkEndorsementTracker {
    pub fn new(epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self { tracker_v1: tracker_v1::ChunkEndorsementTracker::new(epoch_manager) }
    }

    // TODO(ChunkEndorsementV2): Remove chunk_header once tracker_v1 is deprecated
    pub fn process_chunk_endorsement(
        &self,
        endorsement: ChunkEndorsement,
        chunk_header: Option<ShardChunkHeader>,
    ) -> Result<(), Error> {
        match endorsement {
            ChunkEndorsement::V1(endorsement) => {
                self.tracker_v1.process_chunk_endorsement(endorsement, chunk_header)
            }
            ChunkEndorsement::V2(endorsement) => {
                // TODO(ChunkEndorsementV2): Remove this once we implement tracker_v2
                self.tracker_v1.process_chunk_endorsement(endorsement.into_v1(), chunk_header)
            }
        }
    }

    /// Called by block producer.
    /// Returns ChunkEndorsementsState::Endorsed if node has enough signed stake for the chunk
    /// represented by chunk_header.
    /// Signatures have the same order as ordered_chunk_validators, thus ready to be included in a block as is.
    /// Returns ChunkEndorsementsState::NotEnoughStake if chunk doesn't have enough stake.
    /// For older protocol version, we return ChunkEndorsementsState::Endorsed with an empty array of
    /// chunk endorsements.
    pub fn compute_chunk_endorsements(
        &self,
        chunk_header: &ShardChunkHeader,
    ) -> Result<ChunkEndorsementsState, Error> {
        self.tracker_v1.compute_chunk_endorsements(chunk_header)
    }
}
