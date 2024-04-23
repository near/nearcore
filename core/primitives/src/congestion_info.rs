use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{Gas, ShardId};

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CongestionInfo {
    V1(CongestionInfoV1),
}

/// Stores the congestion level of a shard.
/// TODO(congestion_control) - implement versioning
#[derive(BorshSerialize, BorshDeserialize, Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct CongestionInfoV1 {
    /// If fully congested, only this shard can forward receipts.
    pub allowed_shard: u32,

    /// The congestion level of the shard where 0 means not congested and 255
    /// means fully congested.
    pub congestion_level: u8,
}

impl Default for CongestionInfo {
    fn default() -> Self {
        Self::V1(CongestionInfoV1::default())
    }
}

impl CongestionInfo {
    /// How much gas another shard can send to us in the next block.
    pub fn outgoing_limit(&self, _sender_shard: ShardId) -> Gas {
        todo!()
    }

    /// How much gas we accept for executing new transactions going to any
    /// uncongested shards.
    pub fn process_tx_limit(&self) -> Gas {
        todo!()
    }

    /// Whether we can accept new transaction with the receiver set to this shard.
    pub fn shard_accepts_transactions(&self) -> bool {
        todo!()
    }
}
