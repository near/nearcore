use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;
use std::fmt;

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

impl ChunkProductionKey {
    pub fn to_le_bytes(&self) -> [u8; size_of::<Self>()] {
        let mut res = Vec::with_capacity(size_of::<Self>());
        res.extend_from_slice(&self.shard_id.to_le_bytes());
        res.extend_from_slice(self.epoch_id.as_ref());
        res.extend_from_slice(&self.height_created.to_le_bytes());
        res.try_into().unwrap()
    }
}

#[derive(
    Debug, Hash, PartialEq, Eq, Copy, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
pub enum WitnessType {
    Optimistic,
    Validate,
    Full,
}

impl fmt::Display for WitnessType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl WitnessType {
    /// Returns the string representation of the witness type.
    /// This is more efficient than `to_string()` when you need a `&str`.
    pub fn as_str(&self) -> &'static str {
        match self {
            WitnessType::Optimistic => "optimistic",
            WitnessType::Validate => "validate",
            WitnessType::Full => "full",
        }
    }
}

/// This struct contains combination of fields that uniquely identify witness production.
/// It means that for a given instance only one witness could be produced.
#[derive(Debug, Hash, PartialEq, Eq, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct WitnessProductionKey {
    pub chunk: ChunkProductionKey,
    pub witness_type: WitnessType,
}

impl WitnessProductionKey {
    pub fn to_le_bytes(&self) -> [u8; size_of::<ChunkProductionKey>() + 1] {
        let mut res = Vec::with_capacity(size_of::<ChunkProductionKey>() + 1);
        res.extend_from_slice(&self.chunk.to_le_bytes());
        res.push(self.witness_type as u8);
        res.try_into().unwrap()
    }
}
