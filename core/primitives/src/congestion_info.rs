use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{Gas, ShardId};

/// The CongestionInfo stores information about the congestion of a shard. It is
/// used by other shards to throttle the transactions and receipts to prevent
/// unbounded growth of the queues and buffers in the system.
///
/// The CongestionInfo is a part of the ChunkHeader. It is versioned and each
/// version should not be changed. Rather a new version with the desired changes
/// should be added and used in place of the old one. When adding new versions
/// please also update the default.
#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum CongestionInfo {
    V1(CongestionInfoV1),
}

/// Stores the congestion level of a shard.
#[derive(BorshSerialize, BorshDeserialize, Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct CongestionInfoV1 {
    /// Sum of gas in currently delayed receipts.
    pub delayed_receipts_gas: u128,
    /// Sum of gas in currently buffered receipts.
    pub buffered_receipts_gas: u128,
    /// Size of borsh serialized receipts stored in state because they
    /// were delayed, buffered, postponed, or yielded.
    pub receipt_bytes: u64,
    /// If fully congested, only this shard can forward receipts.
    pub allowed_shard: u16,
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
