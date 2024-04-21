use near_primitives_core::types::{Gas, ShardId};

/// Stores the congestion level of a shard.
///
/// [`CongestionInfo`] should remain an internal struct that is not borsh
/// serialized anywhere. This way we can change it more easily.
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct CongestionInfo {
    /// Sum of gas in currently delayed receipts.
    pub delayed_receipts_gas: u128,
    /// Sum of gas in currently buffered receipts.
    pub buffered_receipts_gas: u128,
    /// Size of borsh serialized receipts stored in state because they
    /// were delayed, buffered, postponed, or yielded.
    pub receipt_bytes: u64,
    /// If fully congested, only this shard can forward receipts.
    pub allowed_shard: u64,
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
