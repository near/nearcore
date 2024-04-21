use near_primitives_core::types::{Gas, ShardId};

// TODO: find better home for the const?
const MAX_CONGESTION_INCOMING_GAS: Gas = 20 * 10u64.pow(15);
const MAX_CONGESTION_OUTGOING_GAS: Gas = 2 * 10u64.pow(15);
const MAX_CONGESTION_MEMORY_CONSUMPTION: u64 = bytesize::ByteSize::mb(1000u64).0;
const MIN_GAS_FORWARDING: Gas = 1 * 10u64.pow(15);
const MAX_GAS_FORWARDING: Gas = 300 * 10u64.pow(15);
const RED_GAS: Gas = 1 * 10u64.pow(15);
const MIN_TX_GAS: Gas = 20 * 10u64.pow(12);
const MAX_TX_GAS: Gas = 500 * 10u64.pow(12);
// 0.25 * MAX_CONGESTION_INCOMING_GAS
const REJECT_TX_CONGESTION_THRESHOLD: Gas = MAX_CONGESTION_INCOMING_GAS / 4;

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
    pub fn outgoing_limit(&self, sender_shard: ShardId) -> Gas {
        let incoming_congestion = self.incoming_congestion();
        let outgoing_congestion = self.outgoing_congestion();
        let memory_congestion = self.memory_congestion();

        let congestion = incoming_congestion.max(outgoing_congestion).max(memory_congestion);

        if congestion == u16::MAX {
            // Red traffic light: reduce to minimum speed
            if sender_shard == self.allowed_shard {
                RED_GAS
            } else {
                0
            }
        } else {
            mix(MAX_GAS_FORWARDING, MIN_GAS_FORWARDING, congestion)
        }
    }

    fn incoming_congestion(&self) -> u16 {
        u16_fraction(self.delayed_receipts_gas, MAX_CONGESTION_INCOMING_GAS)
    }
    fn outgoing_congestion(&self) -> u16 {
        u16_fraction(self.buffered_receipts_gas, MAX_CONGESTION_OUTGOING_GAS)
    }
    fn memory_congestion(&self) -> u16 {
        u16_fraction(self.receipt_bytes as u128, MAX_CONGESTION_MEMORY_CONSUMPTION)
    }

    /// How much gas we accept for executing new transactions going to any
    /// uncongested shards.
    pub fn process_tx_limit(&self) -> Gas {
        mix(MAX_TX_GAS, MIN_TX_GAS, self.incoming_congestion())
    }

    /// Whether we can accept new transaction with the receiver set to this shard.
    pub fn shard_accepts_transactions(&self) -> bool {
        self.delayed_receipts_gas > REJECT_TX_CONGESTION_THRESHOLD as u128
    }
}

#[inline]
fn u16_fraction(value: u128, max: u64) -> u16 {
    let bounded_value = std::cmp::min(value as u128, max as u128);
    let in_u16_range = bounded_value * u16::MAX as u128 / max as u128;
    in_u16_range as u16
}

// linearly interpolate between two values
//
// This method treats u16 as a fraction of u16::MAX.
// This makes multiplication of numbers on the upper end of `u128` better behaved
// than using f64 which lacks precision for such high numbers.
//
// (TODO: overkill? maybe just use f64 and hope that we never have platform incompatibilities)
fn mix(left: u64, right: u64, ratio: u16) -> u64 {
    let left_part = left as u128 * (u16::MAX - ratio) as u128;
    let right_part = right as u128 * ratio as u128;
    let total = (left_part + right_part) / u16::MAX as u128;

    // conversion is save because left and right were both u64 and the result is
    // between the two
    return total as u64;
}
