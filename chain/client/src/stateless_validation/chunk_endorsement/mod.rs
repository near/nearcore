use std::sync::Arc;

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
        match endorsement {
            ChunkEndorsement::V1(endorsement) => self.process_chunk_endorsement_v1(endorsement),
        }
    }
}

impl ChunkEndorsementTracker {
    pub fn new(epoch_manager: Arc<dyn EpochManagerAdapter>) -> Self {
        Self { tracker_v1: tracker_v1::ChunkEndorsementTracker::new(epoch_manager) }
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
