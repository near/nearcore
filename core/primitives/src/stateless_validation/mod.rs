use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

use crate::types::EpochId;

pub mod chunk_endorsement;
pub mod chunk_endorsements_bitmap;
pub mod contract_distribution;
pub mod partial_witness;
pub mod spice_chunk_endorsement;
pub mod spice_state_witness;
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

#[derive(
    Debug, Hash, PartialEq, Eq, Copy, Clone, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
#[repr(transparent)]
pub struct WitnessType(u8);

impl WitnessType {
    pub const OPTIMISTIC: Self = Self(0b01);
    pub const VALIDATE: Self    = Self(0b10);
    /// convenience: all bits set
    pub const FULL: Self        = Self(0b11);

    pub fn bits(self) -> u8 { self.0 }
    pub fn contains(self, other: Self) -> bool { (self.0 & other.0) == other.0 }
    pub fn insert(&mut self, other: Self) { self.0 |= other.0; }

    pub fn as_str(&self) -> &'static str {
        match self.0 {
            0 => "none",
            1 => "optimistic",
            2 => "validate",
            3 => "full",
            _ => panic!("invalid WitnessType value: {}", self.0),
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
        res.push(self.witness_type.bits());
        res.try_into().unwrap()
    }
}
