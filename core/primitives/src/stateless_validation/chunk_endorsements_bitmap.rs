use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;

#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    Default,
    ProtocolSchema,
)]
pub struct ChunkEndorsementsBitmap(Vec<Vec<u8>>);

impl ChunkEndorsementsBitmap {
    pub fn new(num_shards: usize) -> Self {
        Self(Vec::with_capacity(num_shards))
    }

    pub fn set(&mut self, shard_id: ShardId, endorsements: Vec<bool>) {
        unimplemented!()
    }

    pub fn is_set(&self, shard_id: ShardId, validator_index: usize) -> bool {
        unimplemented!()
    }
}
