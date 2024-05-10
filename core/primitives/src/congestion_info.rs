use crate::errors::RuntimeError;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::types::{Gas, ShardId};

const PGAS: Gas = 10u64.pow(15);
const TGAS: Gas = 10u64.pow(12);

// The following constants have been defined in
// [NEP-539](https://github.com/near/NEPs/pull/539) after extensive fine-tuning
// and discussions.

/// How much gas in delayed receipts of a shard is 100% incoming congestion.
///
/// Based on incoming congestion levels, a shard reduces the gas it spends on
/// accepting new transactions instead of working on incoming receipts. Plus,
/// incoming congestion contributes to overall congestion, which reduces how
/// much other shards are allowed to forward to this shard.
const MAX_CONGESTION_INCOMING_GAS: Gas = 20 * PGAS;

/// How much gas in outgoing buffered receipts of a shard is 100% congested.
///
/// Outgoing congestion contributes to overall congestion, which reduces how
/// much other shards are allowed to forward to this shard.
const MAX_CONGESTION_OUTGOING_GAS: Gas = 2 * PGAS;

/// How much memory space of all delayed and buffered receipts in a shard is
/// considered 100% congested.
///
/// Memory congestion contributes to overall congestion, which reduces how much
/// other shards are allowed to forward to this shard.
///
/// This threshold limits memory requirements of validators to a degree but it
/// is not a hard guarantee.
const MAX_CONGESTION_MEMORY_CONSUMPTION: u64 = bytesize::ByteSize::mb(1000u64).0;

/// How many missed chunks in a row in a shard is considered 100% congested.
/// TODO(congestion_control) - find a good limit for missed chunks.
const MAX_CONGESTION_MISSED_CHUNKS: u64 = 10;

/// The maximum amount of gas attached to receipts a shard can forward to
/// another shard per chunk.
///
/// The actual gas forwarding allowance is a linear interpolation between
/// [`MIN_OUTGOING_GAS`] and [`MAX_OUTGOING_GAS`], or 0 if the receiver is
/// fully congested.
const MAX_OUTGOING_GAS: Gas = 300 * PGAS;

/// The minimum gas each shard can send to a shard that is not fully congested.
///
/// The actual gas forwarding allowance is a linear interpolation between
/// [`MIN_OUTGOING_GAS`] and [`MAX_OUTGOING_GAS`], or 0 if the receiver is
/// fully congested.
const MIN_OUTGOING_GAS: Gas = 1 * PGAS;

/// How much gas the chosen allowed shard can send to a 100% congested shard.
///
/// This amount is the absolute minimum of new workload a congested shard has to
/// accept every round. It ensures deadlocks are provably impossible. But in
/// ideal conditions, the gradual reduction of new workload entering the system
/// combined with gradually limited forwarding to congested shards should
/// prevent shards from becoming 100% congested in the first place.
const RED_GAS: Gas = 1 * PGAS;

/// The maximum amount of gas in a chunk spent on converting new transactions to
/// receipts.
///
/// The actual gas forwarding allowance is a linear interpolation between
/// [`MIN_TX_GAS`] and [`MAX_TX_GAS`], based on the incoming congestion of the
/// local shard. Additionally, transactions can be rejected if the receiving
/// remote shard is congested more than [`REJECT_TX_CONGESTION_THRESHOLD`] based
/// on their general congestion level.
const MAX_TX_GAS: Gas = 500 * TGAS;

/// The minimum amount of gas in a chunk spent on converting new transactions
/// to receipts, as long as the receiving shard is not congested.
///
/// The actual gas forwarding allowance is a linear interpolation between
/// [`MIN_TX_GAS`] and [`MAX_TX_GAS`], based on the incoming congestion of the
/// local shard. Additionally, transactions can be rejected if the receiving
/// remote shard is congested more than [`REJECT_TX_CONGESTION_THRESHOLD`] based
/// on their general congestion level.
const MIN_TX_GAS: Gas = 20 * TGAS;

/// How much congestion a shard can tolerate before it stops all shards from
/// accepting new transactions with the receiver set to the congested shard.
const REJECT_TX_CONGESTION_THRESHOLD: f64 = 0.25;

/// Stores the congestion level of a shard.
///
/// The CongestionInfo is a part of the ChunkHeader. It is versioned and each
/// version should not be changed. Rather a new version with the desired changes
/// should be added and used in place of the old one. When adding new versions
/// please also update the default.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
)]
pub enum CongestionInfo {
    V1(CongestionInfoV1),
}

impl Default for CongestionInfo {
    fn default() -> Self {
        Self::V1(CongestionInfoV1::default())
    }
}

impl CongestionInfo {
    /// How much gas another shard can send to us in the next block.
    pub fn outgoing_limit(&self, sender_shard: ShardId, missed_chunks_count: u64) -> Gas {
        match self {
            CongestionInfo::V1(inner) => inner.outgoing_limit(sender_shard, missed_chunks_count),
        }
    }

    /// How much gas we accept for executing new transactions going to any
    /// uncongested shards.
    pub fn process_tx_limit(&self) -> Gas {
        match self {
            CongestionInfo::V1(inner) => inner.process_tx_limit(),
        }
    }

    /// Whether we can accept new transaction with the receiver set to this shard.
    pub fn shard_accepts_transactions(&self, missed_chunks_count: u64) -> bool {
        match self {
            CongestionInfo::V1(inner) => inner.shard_accepts_transactions(missed_chunks_count),
        }
    }

    /// Congestion level in the range [0.0, 1.0].
    pub fn congestion_level(&self, missed_chunks_count: u64) -> f64 {
        match self {
            CongestionInfo::V1(inner) => inner.congestion_level(missed_chunks_count),
        }
    }

    /// Computes and sets the `allowed_shard` field.
    ///
    /// If in a fully congested state, also known as RED state, decide which shard of `other_shards` is
    /// allowed to forward to `own_shard` this round.
    /// In this case, we stop all of `other_shards` from sending anything to `own_shard`.
    /// But to guarantee progress, we allow one shard of `other_shards` to send `RED_GAS` in the next chunk.
    ///
    /// Otherwise, when the congestion level is < 1.0, `allowed_shard` to
    /// `own_shard`. The field is ignored in this case but we still want a
    /// unique representation.
    pub fn finalize_allowed_shard(
        &mut self,
        own_shard: ShardId,
        other_shards: &[ShardId],
        congestion_seed: u64,
    ) {
        match self {
            CongestionInfo::V1(inner) => {
                inner.finalize_allowed_shard(own_shard, other_shards, congestion_seed)
            }
        }
    }

    pub fn add_receipt_bytes(&mut self, bytes: u64) -> Result<(), RuntimeError> {
        match self {
            CongestionInfo::V1(inner) => {
                inner.receipt_bytes = inner
                    .receipt_bytes
                    .checked_add(bytes)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
            }
        }
        Ok(())
    }

    pub fn remove_receipt_bytes(&mut self, bytes: u64) -> Result<(), RuntimeError> {
        match self {
            CongestionInfo::V1(inner) => {
                inner.receipt_bytes = inner
                    .receipt_bytes
                    .checked_sub(bytes)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
            }
        }
        Ok(())
    }

    pub fn add_delayed_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        match self {
            CongestionInfo::V1(inner) => {
                inner.delayed_receipts_gas = inner
                    .delayed_receipts_gas
                    .checked_add(gas as u128)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
            }
        }
        Ok(())
    }

    pub fn remove_delayed_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        match self {
            CongestionInfo::V1(inner) => {
                inner.delayed_receipts_gas = inner
                    .delayed_receipts_gas
                    .checked_sub(gas as u128)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
            }
        }
        Ok(())
    }

    pub fn add_buffered_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        match self {
            CongestionInfo::V1(inner) => {
                inner.buffered_receipts_gas = inner
                    .buffered_receipts_gas
                    .checked_add(gas as u128)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
            }
        }
        Ok(())
    }

    pub fn remove_buffered_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        match self {
            CongestionInfo::V1(inner) => {
                inner.buffered_receipts_gas = inner
                    .buffered_receipts_gas
                    .checked_sub(gas as u128)
                    .ok_or_else(|| RuntimeError::UnexpectedIntegerOverflow)?;
            }
        }
        Ok(())
    }

    /// TODO(congestion_info) - Fix all caller-sites and remove this function
    ///
    /// As we are still fixing things around congestion info, we need to fill valid
    /// congestion infos for all shards in a few places in tests.
    pub fn temp_test_shards_congestion_info(
        shard_ids: &[ShardId],
    ) -> std::collections::HashMap<ShardId, CongestionInfo> {
        shard_ids.iter().map(|&id| (id, CongestionInfo::default())).collect()
    }
}

/// The extended congestion info contains the congestion info and extra
/// information extracted from the block that is needed for congestion control.
///
/// It has simpler interface and it should be used instead of using the
/// [`CongestionInfo`] directly.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExtendedCongestionInfo {
    congestion_info: CongestionInfo,
    missed_chunks_count: u64,
}

impl ExtendedCongestionInfo {
    pub fn new(congestion_info: CongestionInfo, missed_chunks_count: u64) -> Self {
        Self { congestion_info, missed_chunks_count }
    }

    pub fn congestion_info(self) -> CongestionInfo {
        self.congestion_info
    }

    /// How much gas another shard can send to us in the next block.
    pub fn outgoing_limit(&self, sender_shard: ShardId) -> Gas {
        self.congestion_info.outgoing_limit(sender_shard, self.missed_chunks_count)
    }

    /// How much gas we accept for executing new transactions going to any
    /// uncongested shards.
    pub fn process_tx_limit(&self) -> Gas {
        self.congestion_info.process_tx_limit()
    }

    /// Whether we can accept new transaction with the receiver set to this shard.
    pub fn shard_accepts_transactions(&self) -> bool {
        self.congestion_info.shard_accepts_transactions(self.missed_chunks_count)
    }

    pub fn finalize_allowed_shard(
        &mut self,
        own_shard: ShardId,
        other_shards: &[ShardId],
        congestion_seed: u64,
    ) {
        self.congestion_info.finalize_allowed_shard(own_shard, other_shards, congestion_seed)
    }

    pub fn add_receipt_bytes(&mut self, bytes: u64) -> Result<(), RuntimeError> {
        self.congestion_info.add_receipt_bytes(bytes)
    }

    pub fn remove_receipt_bytes(&mut self, bytes: u64) -> Result<(), RuntimeError> {
        self.congestion_info.remove_receipt_bytes(bytes)
    }

    pub fn add_delayed_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        self.congestion_info.add_delayed_receipt_gas(gas)
    }

    pub fn remove_delayed_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        self.congestion_info.remove_delayed_receipt_gas(gas)
    }

    pub fn add_buffered_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        self.congestion_info.add_buffered_receipt_gas(gas)
    }

    pub fn remove_buffered_receipt_gas(&mut self, gas: Gas) -> Result<(), RuntimeError> {
        self.congestion_info.remove_buffered_receipt_gas(gas)
    }

    /// Congestion level in the range [0.0, 1.0].
    pub fn congestion_level(&self) -> f64 {
        match self.congestion_info {
            CongestionInfo::V1(inner) => inner.congestion_level(self.missed_chunks_count),
        }
    }
}

/// Stores the congestion level of a shard.
#[derive(
    BorshSerialize,
    BorshDeserialize,
    serde::Serialize,
    serde::Deserialize,
    Default,
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
)]
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

impl CongestionInfoV1 {
    /// How much gas another shard can send to us in the next block.
    pub fn outgoing_limit(&self, sender_shard: ShardId, missed_chunks_count: u64) -> Gas {
        let congestion = self.congestion_level(missed_chunks_count);

        // note: using float equality is okay here because
        // `clamped_f64_fraction` clamps to exactly 1.0.
        if congestion == 1.0 {
            // Red traffic light: reduce to minimum speed
            if sender_shard == self.allowed_shard as u64 {
                RED_GAS
            } else {
                0
            }
        } else {
            mix(MAX_OUTGOING_GAS, MIN_OUTGOING_GAS, congestion)
        }
    }

    fn congestion_level(&self, missed_chunks_count: u64) -> f64 {
        let incoming_congestion = self.incoming_congestion();
        let outgoing_congestion = self.outgoing_congestion();
        let memory_congestion = self.memory_congestion();
        let missed_chunks_congestion = self.missed_chunks_congestion(missed_chunks_count);

        incoming_congestion
            .max(outgoing_congestion)
            .max(memory_congestion)
            .max(missed_chunks_congestion)
    }

    fn incoming_congestion(&self) -> f64 {
        clamped_f64_fraction(self.delayed_receipts_gas, MAX_CONGESTION_INCOMING_GAS)
    }
    fn outgoing_congestion(&self) -> f64 {
        clamped_f64_fraction(self.buffered_receipts_gas, MAX_CONGESTION_OUTGOING_GAS)
    }
    fn memory_congestion(&self) -> f64 {
        clamped_f64_fraction(self.receipt_bytes as u128, MAX_CONGESTION_MEMORY_CONSUMPTION)
    }
    fn missed_chunks_congestion(&self, missed_chunks_count: u64) -> f64 {
        if missed_chunks_count <= 1 {
            return 0.0;
        }

        clamped_f64_fraction(missed_chunks_count as u128, MAX_CONGESTION_MISSED_CHUNKS)
    }

    /// How much gas we accept for executing new transactions going to any
    /// uncongested shards.
    pub fn process_tx_limit(&self) -> Gas {
        mix(MAX_TX_GAS, MIN_TX_GAS, self.incoming_congestion())
    }

    /// Whether we can accept new transaction with the receiver set to this shard.
    pub fn shard_accepts_transactions(&self, missed_chunks_count: u64) -> bool {
        self.congestion_level(missed_chunks_count) < REJECT_TX_CONGESTION_THRESHOLD
    }

    /// Computes and sets the `allowed_shard` field.
    ///
    /// Refer to [`CongestionInfo::finalize_allowed_shard`].
    pub fn finalize_allowed_shard(
        &mut self,
        own_shard: ShardId,
        other_shards: &[ShardId],
        congestion_seed: u64,
    ) {
        // For the purpose of setting the allowed shard ignore the missed chunks
        // congestion. This is to disallow any shard from sending traffic to
        // this shard if there are multiple missed chunks in a row in it.
        let missed_chunks_count = 0;
        if self.congestion_level(missed_chunks_count) < 1.0 {
            self.allowed_shard = own_shard as u16;
        } else {
            if let Some(index) = congestion_seed.checked_rem(other_shards.len() as u64) {
                // round robin for other shards based on the seed
                self.allowed_shard = *other_shards
                    .get(index as usize)
                    .expect("`checked_rem` should have ensured array access is in bound")
                    as u16;
            } else {
                // checked_rem failed, hence other_shards.len() is 0
                // own_shard is the only choice.
                self.allowed_shard = own_shard as u16;
            }
        }
    }
}

/// Returns `value / max` clamped to te range [0,1].
#[inline]
fn clamped_f64_fraction(value: u128, max: u64) -> f64 {
    assert!(max > 0);
    if max as u128 <= value {
        1.0
    } else {
        value as f64 / max as f64
    }
}

/// linearly interpolate between two values
///
/// This method treats u16 as a fraction of u16::MAX.
/// This makes multiplication of numbers on the upper end of `u128` better behaved
/// than using f64 which lacks precision for such high numbers and might have platform incompatibilities.
fn mix(left: u64, right: u64, ratio: f64) -> u64 {
    debug_assert!(ratio >= 0.0);
    debug_assert!(ratio <= 1.0);

    // Note on precision: f64 is only precise to 53 binary digits. That is
    // enough to represent ~9 PGAS without error. Precision above that is
    // rounded according to the IEEE 754-2008 standard which Rust's f64
    // implements.
    // For example, a value of 100 Pgas is rounded to steps of 8 gas.
    let left_part = left as f64 * (1.0 - ratio);
    let right_part = right as f64 * ratio;
    // Accumulated error is doubled again, up to 16 gas for 100 Pgas.
    let total = left_part + right_part;

    // Conversion is save because left and right were both u64 and the result is
    // between the two. Even with precision errors, we cannot breach the
    // boundaries.
    return total.round() as u64;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mix() {
        assert_eq!(500, mix(0, 1000, 0.5));
        assert_eq!(0, mix(0, 0, 0.3));
        assert_eq!(1000, mix(1000, 1000, 0.1));
        assert_eq!(60, mix(50, 80, 0.33));
    }

    #[test]
    fn test_mix_edge_cases() {
        // at `u64::MAX` we should see no precision errors
        assert_eq!(u64::MAX, mix(u64::MAX, u64::MAX, 0.33));
        assert_eq!(u64::MAX, mix(u64::MAX, u64::MAX, 0.63));
        assert_eq!(u64::MAX, mix(u64::MAX, u64::MAX, 0.99));

        // precision errors must be consistent
        assert_eq!(u64::MAX, mix(u64::MAX - 1, u64::MAX, 0.25));
        assert_eq!(u64::MAX, mix(u64::MAX - 255, u64::MAX, 0.25));
        assert_eq!(u64::MAX, mix(u64::MAX - 1023, u64::MAX, 0.25));

        assert_eq!(u64::MAX - 2047, mix(u64::MAX - 1024, u64::MAX, 0.25));
        assert_eq!(u64::MAX - 2047, mix(u64::MAX - 1500, u64::MAX, 0.25));
        assert_eq!(u64::MAX - 2047, mix(u64::MAX - 2047, u64::MAX, 0.25));
        assert_eq!(u64::MAX - 2047, mix(u64::MAX - 2048, u64::MAX, 0.25));
        assert_eq!(u64::MAX - 2047, mix(u64::MAX - 2049, u64::MAX, 0.25));
        assert_eq!(u64::MAX - 2047, mix(u64::MAX - 3000, u64::MAX, 0.25));

        assert_eq!(u64::MAX - 4095, mix(u64::MAX - 4000, u64::MAX, 0.25));
    }

    #[test]
    fn test_clamped_f64_fraction() {
        assert_eq!(0.0, clamped_f64_fraction(0, 10));
        assert_eq!(0.5, clamped_f64_fraction(5, 10));
        assert_eq!(1.0, clamped_f64_fraction(10, 10));

        assert_eq!(0.0, clamped_f64_fraction(0, 1));
        assert_eq!(0.0, clamped_f64_fraction(0, u64::MAX));

        assert_eq!(0.5, clamped_f64_fraction(1, 2));
        assert_eq!(0.5, clamped_f64_fraction(100, 200));
        assert_eq!(0.5, clamped_f64_fraction(u64::MAX as u128 / 2, u64::MAX));

        // test clamp
        assert_eq!(1.0, clamped_f64_fraction(11, 10));
        assert_eq!(1.0, clamped_f64_fraction(u128::MAX, 10));
        assert_eq!(1.0, clamped_f64_fraction(u128::MAX, u64::MAX));
    }

    /// Default congestion info should be no congestion => maximally permissive.
    #[test]
    fn test_default_congestion() {
        let inner_congestion_info = CongestionInfoV1::default();

        assert_eq!(0.0, inner_congestion_info.memory_congestion());
        assert_eq!(0.0, inner_congestion_info.incoming_congestion());
        assert_eq!(0.0, inner_congestion_info.outgoing_congestion());
        assert_eq!(0.0, inner_congestion_info.congestion_level(0));

        let congestion_info = CongestionInfo::V1(inner_congestion_info);
        assert_eq!(MAX_OUTGOING_GAS, congestion_info.outgoing_limit(0, 0));
        assert_eq!(MAX_TX_GAS, congestion_info.process_tx_limit());
        assert!(congestion_info.shard_accepts_transactions(0));
    }

    #[test]
    fn test_memory_congestion() {
        let mut congestion_info = CongestionInfo::default();

        congestion_info.add_receipt_bytes(MAX_CONGESTION_MEMORY_CONSUMPTION).unwrap();
        congestion_info.add_receipt_bytes(500).unwrap();
        congestion_info.remove_receipt_bytes(500).unwrap();

        assert_eq!(1.0, congestion_info.congestion_level(0));
        // fully congested, no more forwarding allowed
        assert_eq!(0, congestion_info.outgoing_limit(1, 0));
        assert!(!congestion_info.shard_accepts_transactions(0));
        // processing to other shards is not restricted by memory congestion
        assert_eq!(MAX_TX_GAS, congestion_info.process_tx_limit());

        // remove half the congestion
        congestion_info.remove_receipt_bytes(MAX_CONGESTION_MEMORY_CONSUMPTION / 2).unwrap();
        assert_eq!(0.5, congestion_info.congestion_level(0));
        assert_eq!(
            (0.5 * MIN_OUTGOING_GAS as f64 + 0.5 * MAX_OUTGOING_GAS as f64) as u64,
            congestion_info.outgoing_limit(1, 0)
        );
        // at 50%, still no new transactions are allowed
        assert!(!congestion_info.shard_accepts_transactions(0));

        // reduce congestion to 1/8
        congestion_info.remove_receipt_bytes(3 * MAX_CONGESTION_MEMORY_CONSUMPTION / 8).unwrap();
        assert_eq!(0.125, congestion_info.congestion_level(0));
        assert_eq!(
            (0.125 * MIN_OUTGOING_GAS as f64 + 0.875 * MAX_OUTGOING_GAS as f64) as u64,
            congestion_info.outgoing_limit(1, 0)
        );
        // at 12.5%, new transactions are allowed (threshold is 0.25)
        assert!(congestion_info.shard_accepts_transactions(0));
    }

    #[test]
    fn test_incoming_congestion() {
        let mut congestion_info = ExtendedCongestionInfo::default();

        congestion_info.add_delayed_receipt_gas(MAX_CONGESTION_INCOMING_GAS).unwrap();
        congestion_info.add_delayed_receipt_gas(500).unwrap();
        congestion_info.remove_delayed_receipt_gas(500).unwrap();

        assert_eq!(1.0, congestion_info.congestion_level());
        // fully congested, no more forwarding allowed
        assert_eq!(0, congestion_info.outgoing_limit(1));
        assert!(!congestion_info.shard_accepts_transactions());
        // processing to other shards is restricted by own incoming congestion
        assert_eq!(MIN_TX_GAS, congestion_info.process_tx_limit());

        // remove halve the congestion
        congestion_info.remove_delayed_receipt_gas(MAX_CONGESTION_INCOMING_GAS / 2).unwrap();
        assert_eq!(0.5, congestion_info.congestion_level());
        assert_eq!(
            (0.5 * MIN_OUTGOING_GAS as f64 + 0.5 * MAX_OUTGOING_GAS as f64) as u64,
            congestion_info.outgoing_limit(1)
        );
        // at 50%, still no new transactions to us are allowed
        assert!(!congestion_info.shard_accepts_transactions());
        // but we accept new transactions to other shards
        assert_eq!(
            (0.5 * MIN_TX_GAS as f64 + 0.5 * MAX_TX_GAS as f64) as u64,
            congestion_info.process_tx_limit()
        );

        // reduce congestion to 1/8
        congestion_info.remove_delayed_receipt_gas(3 * MAX_CONGESTION_INCOMING_GAS / 8).unwrap();
        assert_eq!(0.125, congestion_info.congestion_level());
        assert_eq!(
            (0.125 * MIN_OUTGOING_GAS as f64 + 0.875 * MAX_OUTGOING_GAS as f64) as u64,
            congestion_info.outgoing_limit(1)
        );
        // at 12.5%, new transactions are allowed (threshold is 0.25)
        assert!(congestion_info.shard_accepts_transactions());
        assert_eq!(
            (0.125 * MIN_TX_GAS as f64 + 0.875 * MAX_TX_GAS as f64) as u64,
            congestion_info.process_tx_limit()
        );
    }

    #[test]
    fn test_outgoing_congestion() {
        let mut congestion_info = ExtendedCongestionInfo::default();

        congestion_info.add_buffered_receipt_gas(MAX_CONGESTION_OUTGOING_GAS).unwrap();
        congestion_info.add_buffered_receipt_gas(500).unwrap();
        congestion_info.remove_buffered_receipt_gas(500).unwrap();

        assert_eq!(1.0, congestion_info.congestion_level());
        // fully congested, no more forwarding allowed
        assert_eq!(0, congestion_info.outgoing_limit(1));
        assert!(!congestion_info.shard_accepts_transactions());
        // processing to other shards is not restricted by own outgoing congestion
        assert_eq!(MAX_TX_GAS, congestion_info.process_tx_limit());

        // remove halve the congestion
        congestion_info.remove_buffered_receipt_gas(MAX_CONGESTION_OUTGOING_GAS / 2).unwrap();
        assert_eq!(0.5, congestion_info.congestion_level());
        assert_eq!(
            (0.5 * MIN_OUTGOING_GAS as f64 + 0.5 * MAX_OUTGOING_GAS as f64) as u64,
            congestion_info.outgoing_limit(1)
        );
        // at 50%, still no new transactions to us are allowed
        assert!(!congestion_info.shard_accepts_transactions());

        // reduce congestion to 1/8
        congestion_info.remove_buffered_receipt_gas(3 * MAX_CONGESTION_OUTGOING_GAS / 8).unwrap();
        assert_eq!(0.125, congestion_info.congestion_level());
        assert_eq!(
            (0.125 * MIN_OUTGOING_GAS as f64 + 0.875 * MAX_OUTGOING_GAS as f64) as u64,
            congestion_info.outgoing_limit(1)
        );
        // at 12.5%, new transactions are allowed (threshold is 0.25)
        assert!(congestion_info.shard_accepts_transactions());
    }

    #[test]
    fn test_missed_chunks_congestion() {
        // Test missed chunks congestion without any other congestion
        let make = |count| ExtendedCongestionInfo::new(CongestionInfo::default(), count);

        assert_eq!(make(0).congestion_level(), 0.0);
        assert_eq!(make(1).congestion_level(), 0.0);
        assert_eq!(make(2).congestion_level(), 0.2);
        assert_eq!(make(3).congestion_level(), 0.3);
        assert_eq!(make(10).congestion_level(), 1.0);
        assert_eq!(make(20).congestion_level(), 1.0);

        // Test missed chunks congestion with outgoing congestion
        let mut congestion_info = CongestionInfo::default();
        congestion_info.add_buffered_receipt_gas(MAX_CONGESTION_OUTGOING_GAS / 2).unwrap();
        let make = |count| ExtendedCongestionInfo::new(congestion_info, count);

        assert_eq!(make(0).congestion_level(), 0.5);
        assert_eq!(make(1).congestion_level(), 0.5);
        assert_eq!(make(2).congestion_level(), 0.5);
        assert_eq!(make(5).congestion_level(), 0.5);
        assert_eq!(make(6).congestion_level(), 0.6);
        assert_eq!(make(10).congestion_level(), 1.0);
        assert_eq!(make(20).congestion_level(), 1.0);
    }

    #[test]
    fn test_missed_chunks_finalize() {
        // Setup half congested congestion info.
        let mut congestion_info = CongestionInfo::default();
        congestion_info.add_buffered_receipt_gas(MAX_CONGESTION_OUTGOING_GAS / 2).unwrap();
        let shard = 2;
        let other_shards = [0, 1, 3, 4];

        // Test without missed chunks congestion.

        let missed_chunks_count = 0;
        let mut info = ExtendedCongestionInfo::new(congestion_info, missed_chunks_count);
        info.finalize_allowed_shard(shard, &other_shards, 3);

        let expected_outgoing_limit = 0.5 * MIN_OUTGOING_GAS as f64 + 0.5 * MAX_OUTGOING_GAS as f64;
        for other_shard in other_shards {
            assert_eq!(info.outgoing_limit(other_shard), expected_outgoing_limit as u64);
        }

        // Test with some missed chunks congestion.

        let expected_outgoing_limit = mix(MAX_OUTGOING_GAS, MIN_OUTGOING_GAS, 0.8) as f64;
        let mut info = ExtendedCongestionInfo::new(congestion_info, missed_chunks_count);
        info.finalize_allowed_shard(shard, &other_shards, 3);

        for other_shard in other_shards {
            assert_eq!(info.outgoing_limit(other_shard), expected_outgoing_limit as u64);
        }

        // Test with full missed chunks congestion.

        // The allowed shard should be set to own shard. None of the other
        // shards should be allowed to send anything.

        let missed_chunks_count = MAX_CONGESTION_MISSED_CHUNKS;
        let mut info = ExtendedCongestionInfo::new(congestion_info, missed_chunks_count);
        info.finalize_allowed_shard(shard, &other_shards, 3);

        let expected_outgoing_limit = 0;
        for other_shard in other_shards {
            assert_eq!(info.outgoing_limit(other_shard), expected_outgoing_limit as u64);
        }
    }
}
