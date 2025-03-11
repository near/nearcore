use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

use crate::types::EpochId;

pub mod chunk_endorsement;
pub mod chunk_endorsements_bitmap;
pub mod contract_distribution;
pub mod partial_witness;
pub mod state_witness;
pub mod stored_chunk_state_transition_data;
pub mod validator_assignment;

/// This struct contains combination of fields that uniquely identify chunk production.
/// It means that for a given instance only one chunk could be produced.
#[derive(Debug, Hash, PartialEq, Eq, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkProductionKey {
    pub shard_id: ShardId,
    pub epoch_id: EpochId,
    pub height_created: BlockHeight,
}
