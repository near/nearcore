use crate::errors::RuntimeError;
use borsh::{BorshDeserialize, BorshSerialize};
use near_parameters::config::CongestionControlConfig;
use near_primitives_core::types::{Gas, ShardId};

/// This class combines the congestion control config, congestion info and
/// missed chunks count. It contains the main congestion control logic and
/// exposes methods that can be used for congestion control.
///
/// Use this struct to make congestion control decisions, by looking at the
/// congestion info of a previous chunk produced on a remote shard. For building
/// up a congestion info for the local shard, this struct should not be
/// necessary. Use `CongestionInfo` directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CongestionControl {
    config: CongestionControlConfig,
    /// Finalized congestion info of a previous chunk.
    info: CongestionInfo,
    /// How many block heights had no chunk since the last successful chunk on
    /// the respective shard.
    missed_chunks_count: u64,
}

impl CongestionControl {
    pub fn new(
        config: CongestionControlConfig,
        info: CongestionInfo,
        missed_chunks_count: u64,
    ) -> Self {
        Self { config, info, missed_chunks_count }
    }

    pub fn config(&self) -> &CongestionControlConfig {
        &self.config
    }

    pub fn congestion_info(&self) -> &CongestionInfo {
        &self.info
    }

    pub fn congestion_level(&self) -> f64 {
        let incoming_congestion = self.incoming_congestion();
        let outgoing_congestion = self.outgoing_congestion();
        let memory_congestion = self.memory_congestion();
        let missed_chunks_congestion = self.missed_chunks_congestion();

        incoming_congestion
            .max(outgoing_congestion)
            .max(memory_congestion)
            .max(missed_chunks_congestion)
    }

    fn incoming_congestion(&self) -> f64 {
        self.info.incoming_congestion(&self.config)
    }

    fn outgoing_congestion(&self) -> f64 {
        self.info.outgoing_congestion(&self.config)
    }

    fn memory_congestion(&self) -> f64 {
        self.info.memory_congestion(&self.config)
    }

    fn missed_chunks_congestion(&self) -> f64 {
        if self.missed_chunks_count <= 1 {
            return 0.0;
        }

        clamped_f64_fraction(
            self.missed_chunks_count as u128,
            self.config.max_congestion_missed_chunks,
        )
    }

    /// How much gas another shard can send to us in the next block.
    pub fn outgoing_limit(&self, sender_shard: ShardId) -> Gas {
        let congestion = self.congestion_level();

        // note: using float equality is okay here because
        // `clamped_f64_fraction` clamps to exactly 1.0.
        if congestion == 1.0 {
            // Red traffic light: reduce to minimum speed
            if sender_shard == self.info.allowed_shard() as u64 {
                self.config.allowed_shard_outgoing_gas
            } else {
                0
            }
        } else {
            mix(self.config.max_outgoing_gas, self.config.min_outgoing_gas, congestion)
        }
    }

    /// How much gas we accept for executing new transactions going to any
    /// uncongested shards.
    pub fn process_tx_limit(&self) -> Gas {
        mix(self.config.max_tx_gas, self.config.min_tx_gas, self.incoming_congestion())
    }

    /// Whether we can accept new transaction with the receiver set to this shard.
    pub fn shard_accepts_transactions(&self) -> bool {
        self.congestion_level() < self.config.reject_tx_congestion_threshold
    }
}

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
    // A helper method to compare the congestion info from the chunk extra of
    // the previous chunk and the header of the current chunk. It returns true
    // if the congestion info was correctly set in the chunk header based on the
    // information from the chunk extra.
    //
    // TODO(congestion_control) validate allowed shard correctly
    // If the shard is fully congested the any of the other shards can be the allowed shard.
    // If the shard is not fully congested the allowed shard should be set to self.
    pub fn validate_extra_and_header(extra: &CongestionInfo, header: &CongestionInfo) -> bool {
        match (extra, header) {
            (CongestionInfo::V1(extra), CongestionInfo::V1(header)) => {
                extra.delayed_receipts_gas == header.delayed_receipts_gas
                    && extra.buffered_receipts_gas == header.buffered_receipts_gas
                    && extra.receipt_bytes == header.receipt_bytes
                    && extra.allowed_shard == header.allowed_shard
            }
        }
    }

    pub fn delayed_receipts_gas(&self) -> u128 {
        match self {
            CongestionInfo::V1(inner) => inner.delayed_receipts_gas,
        }
    }

    pub fn buffered_receipts_gas(&self) -> u128 {
        match self {
            CongestionInfo::V1(inner) => inner.buffered_receipts_gas,
        }
    }

    pub fn receipt_bytes(&self) -> u64 {
        match self {
            CongestionInfo::V1(inner) => inner.receipt_bytes,
        }
    }

    pub fn allowed_shard(&self) -> u16 {
        match self {
            CongestionInfo::V1(inner) => inner.allowed_shard,
        }
    }

    pub fn set_allowed_shard(&mut self, allowed_shard: u16) {
        match self {
            CongestionInfo::V1(inner) => inner.allowed_shard = allowed_shard,
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

    /// Congestion level ignoring the chain context (missed chunks count).
    pub fn localized_congestion_level(&self, config: &CongestionControlConfig) -> f64 {
        let incoming_congestion = self.incoming_congestion(config);
        let outgoing_congestion = self.outgoing_congestion(config);
        let memory_congestion = self.memory_congestion(config);
        incoming_congestion.max(outgoing_congestion).max(memory_congestion)
    }

    pub fn incoming_congestion(&self, config: &CongestionControlConfig) -> f64 {
        clamped_f64_fraction(self.delayed_receipts_gas(), config.max_congestion_incoming_gas)
    }

    pub fn outgoing_congestion(&self, config: &CongestionControlConfig) -> f64 {
        clamped_f64_fraction(self.buffered_receipts_gas(), config.max_congestion_outgoing_gas)
    }

    pub fn memory_congestion(&self, config: &CongestionControlConfig) -> f64 {
        clamped_f64_fraction(self.receipt_bytes() as u128, config.max_congestion_memory_consumption)
    }

    /// Computes and sets the `allowed_shard` field.
    ///
    /// If in a fully congested state, decide which shard of `other_shards` is
    /// allowed to forward to `own_shard` this round. In this case, we stop all
    /// of `other_shards` from sending anything to `own_shard`. But to guarantee
    /// progress, we allow one shard of `other_shards` to send
    /// `allowed_shard_outgoing_gas` in the next chunk.
    ///
    /// Otherwise, when the congestion level is < 1.0, set `allowed_shard` to
    /// `own_shard`. The field is ignored in this case but we still want a
    /// unique representation.
    pub fn finalize_allowed_shard(
        &mut self,
        own_shard: ShardId,
        other_shards: &[ShardId],
        congestion_seed: u64,
        config: &CongestionControlConfig,
    ) {
        let congestion_level = self.localized_congestion_level(config);
        let allowed_shard =
            Self::get_new_allowed_shard(own_shard, other_shards, congestion_seed, congestion_level);
        self.set_allowed_shard(allowed_shard as u16);
    }

    fn get_new_allowed_shard(
        own_shard: ShardId,
        other_shards: &[ShardId],
        congestion_seed: u64,
        congestion_level: f64,
    ) -> ShardId {
        if congestion_level < 1.0 {
            return own_shard;
        }
        if let Some(index) = congestion_seed.checked_rem(other_shards.len() as u64) {
            // round robin for other shards based on the seed
            return *other_shards
                .get(index as usize)
                .expect("`checked_rem` should have ensured array access is in bound");
        }
        // checked_rem failed, hence other_shards.len() is 0
        // own_shard is the only choice.
        return own_shard;
    }
}

/// The extended congestion info contains the congestion info and extra
/// information extracted from the block that is needed for congestion control.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ExtendedCongestionInfo {
    pub congestion_info: CongestionInfo,
    pub missed_chunks_count: u64,
}

impl ExtendedCongestionInfo {
    pub fn new(congestion_info: CongestionInfo, missed_chunks_count: u64) -> Self {
        Self { congestion_info, missed_chunks_count }
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
    use near_parameters::RuntimeConfigStore;
    use near_primitives_core::version::{ProtocolFeature, PROTOCOL_VERSION};

    use super::*;

    fn get_config() -> CongestionControlConfig {
        let runtime_config_store = RuntimeConfigStore::new(None);
        let runtime_config = runtime_config_store.get_config(PROTOCOL_VERSION);
        runtime_config.congestion_control_config
    }

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
        let config = get_config();
        let info = CongestionInfo::default();
        let congestion_control = CongestionControl::new(config, info, 0);

        assert_eq!(0.0, info.memory_congestion(&config));
        assert_eq!(0.0, info.incoming_congestion(&config));
        assert_eq!(0.0, info.outgoing_congestion(&config));
        assert_eq!(0.0, info.localized_congestion_level(&config));

        assert_eq!(0.0, congestion_control.memory_congestion());
        assert_eq!(0.0, congestion_control.incoming_congestion());
        assert_eq!(0.0, congestion_control.outgoing_congestion());
        assert_eq!(0.0, congestion_control.congestion_level());

        assert!(config.max_outgoing_gas.abs_diff(congestion_control.outgoing_limit(0)) <= 1);

        assert!(config.max_tx_gas.abs_diff(congestion_control.process_tx_limit()) <= 1);
        assert!(congestion_control.shard_accepts_transactions());
    }

    #[test]
    fn test_memory_congestion() {
        if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
            return;
        }

        let config = get_config();
        let mut info = CongestionInfo::default();

        info.add_receipt_bytes(config.max_congestion_memory_consumption).unwrap();
        info.add_receipt_bytes(500).unwrap();
        info.remove_receipt_bytes(500).unwrap();

        {
            let control = CongestionControl::new(config, info, 0);
            assert_eq!(1.0, control.congestion_level());
            // fully congested, no more forwarding allowed
            assert_eq!(0, control.outgoing_limit(1));
            assert!(!control.shard_accepts_transactions());
            // processing to other shards is not restricted by memory congestion
            assert_eq!(config.max_tx_gas, control.process_tx_limit());
        }

        // remove half the congestion
        info.remove_receipt_bytes(config.max_congestion_memory_consumption / 2).unwrap();
        {
            let control = CongestionControl::new(config, info, 0);
            assert_eq!(0.5, control.congestion_level());
            assert_eq!(
                (0.5 * config.min_outgoing_gas as f64 + 0.5 * config.max_outgoing_gas as f64)
                    as u64,
                control.outgoing_limit(1)
            );
            // at 50%, still no new transactions are allowed
            assert!(!control.shard_accepts_transactions());
        }

        // reduce congestion to 1/8
        info.remove_receipt_bytes(3 * config.max_congestion_memory_consumption / 8).unwrap();
        {
            let control = CongestionControl::new(config, info, 0);
            assert_eq!(0.125, control.congestion_level());
            assert_eq!(
                (0.125 * config.min_outgoing_gas as f64 + 0.875 * config.max_outgoing_gas as f64)
                    as u64,
                control.outgoing_limit(1)
            );
            // at 12.5%, new transactions are allowed (threshold is 0.25)
            assert!(control.shard_accepts_transactions());
        }
    }

    #[test]
    fn test_incoming_congestion() {
        if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
            return;
        }

        let config = get_config();
        let mut info = CongestionInfo::default();

        info.add_delayed_receipt_gas(config.max_congestion_incoming_gas).unwrap();
        info.add_delayed_receipt_gas(500).unwrap();
        info.remove_delayed_receipt_gas(500).unwrap();

        {
            let control = CongestionControl::new(config, info, 0);
            assert_eq!(1.0, control.congestion_level());
            // fully congested, no more forwarding allowed
            assert_eq!(0, control.outgoing_limit(1));
            assert!(!control.shard_accepts_transactions());
            // processing to other shards is restricted by own incoming congestion
            assert_eq!(config.min_tx_gas, control.process_tx_limit());
        }

        // remove halve the congestion
        info.remove_delayed_receipt_gas(config.max_congestion_incoming_gas / 2).unwrap();
        {
            let control = CongestionControl::new(config, info, 0);
            assert_eq!(0.5, control.congestion_level());
            assert_eq!(
                (0.5 * config.min_outgoing_gas as f64 + 0.5 * config.max_outgoing_gas as f64)
                    as u64,
                control.outgoing_limit(1)
            );
            // at 50%, still no new transactions to us are allowed
            assert!(!control.shard_accepts_transactions());
            // but we accept new transactions to other shards
            assert_eq!(
                (0.5 * config.min_tx_gas as f64 + 0.5 * config.max_tx_gas as f64) as u64,
                control.process_tx_limit()
            );
        }

        // reduce congestion to 1/8
        info.remove_delayed_receipt_gas(3 * config.max_congestion_incoming_gas / 8).unwrap();
        {
            let control = CongestionControl::new(config, info, 0);
            assert_eq!(0.125, control.congestion_level());
            assert_eq!(
                (0.125 * config.min_outgoing_gas as f64 + 0.875 * config.max_outgoing_gas as f64)
                    as u64,
                control.outgoing_limit(1)
            );
            // at 12.5%, new transactions are allowed (threshold is 0.25)
            assert!(control.shard_accepts_transactions());
            assert_eq!(
                (0.125 * config.min_tx_gas as f64 + 0.875 * config.max_tx_gas as f64) as u64,
                control.process_tx_limit()
            );
        }
    }

    #[test]
    fn test_outgoing_congestion() {
        if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
            return;
        }

        let config = get_config();
        let mut info = CongestionInfo::default();

        info.add_buffered_receipt_gas(config.max_congestion_outgoing_gas).unwrap();
        info.add_buffered_receipt_gas(500).unwrap();
        info.remove_buffered_receipt_gas(500).unwrap();

        let control = CongestionControl::new(config, info, 0);
        assert_eq!(1.0, control.congestion_level());
        // fully congested, no more forwarding allowed
        assert_eq!(0, control.outgoing_limit(1));
        assert!(!control.shard_accepts_transactions());
        // processing to other shards is not restricted by own outgoing congestion
        assert_eq!(config.max_tx_gas, control.process_tx_limit());

        // remove halve the congestion
        info.remove_buffered_receipt_gas(config.max_congestion_outgoing_gas / 2).unwrap();
        let control = CongestionControl::new(config, info, 0);
        assert_eq!(0.5, control.congestion_level());
        assert_eq!(
            (0.5 * config.min_outgoing_gas as f64 + 0.5 * config.max_outgoing_gas as f64) as u64,
            control.outgoing_limit(1)
        );
        // at 50%, still no new transactions to us are allowed
        assert!(!control.shard_accepts_transactions());

        // reduce congestion to 1/8
        info.remove_buffered_receipt_gas(3 * config.max_congestion_outgoing_gas / 8).unwrap();
        let control = CongestionControl::new(config, info, 0);
        assert_eq!(0.125, control.congestion_level());
        assert_eq!(
            (0.125 * config.min_outgoing_gas as f64 + 0.875 * config.max_outgoing_gas as f64)
                as u64,
            control.outgoing_limit(1)
        );
        // at 12.5%, new transactions are allowed (threshold is 0.25)
        assert!(control.shard_accepts_transactions());
    }

    #[test]
    fn test_missed_chunks_congestion() {
        if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
            return;
        }

        // The default config is quite restricting, allow more missed chunks for
        // this test to check the middle cases.
        let mut config = get_config();
        config.max_congestion_missed_chunks = 10;

        let info = CongestionInfo::default();

        // Test missed chunks congestion without any other congestion
        let make = |count| CongestionControl::new(config, info, count);

        assert_eq!(make(0).congestion_level(), 0.0);
        assert_eq!(make(1).congestion_level(), 0.0);
        assert_eq!(make(2).congestion_level(), 0.2);
        assert_eq!(make(3).congestion_level(), 0.3);
        assert_eq!(make(10).congestion_level(), 1.0);
        assert_eq!(make(20).congestion_level(), 1.0);

        // Test missed chunks congestion with outgoing congestion
        let mut info = CongestionInfo::default();
        info.add_buffered_receipt_gas(config.max_congestion_outgoing_gas / 2).unwrap();
        let make = |count| CongestionControl::new(config, info, count);

        // include missing chunks congestion
        assert_eq!(make(0).congestion_level(), 0.5);
        assert_eq!(make(1).congestion_level(), 0.5);
        assert_eq!(make(2).congestion_level(), 0.5);
        assert_eq!(make(5).congestion_level(), 0.5);
        assert_eq!(make(6).congestion_level(), 0.6);
        assert_eq!(make(10).congestion_level(), 1.0);
        assert_eq!(make(20).congestion_level(), 1.0);

        // exclude missing chunks congestion
        assert_eq!(make(0).info.localized_congestion_level(&config), 0.5);
        assert_eq!(make(1).info.localized_congestion_level(&config), 0.5);
        assert_eq!(make(2).info.localized_congestion_level(&config), 0.5);
        assert_eq!(make(5).info.localized_congestion_level(&config), 0.5);
        assert_eq!(make(6).info.localized_congestion_level(&config), 0.5);
        assert_eq!(make(10).info.localized_congestion_level(&config), 0.5);
        assert_eq!(make(20).info.localized_congestion_level(&config), 0.5);
    }

    #[test]
    fn test_missed_chunks_finalize() {
        if !ProtocolFeature::CongestionControl.enabled(PROTOCOL_VERSION) {
            return;
        }

        // The default config is quite restricting, allow more missed chunks for
        // this test to check the middle cases.
        let mut config = get_config();
        config.max_congestion_missed_chunks = 10;

        // Setup half congested congestion info.
        let mut info = CongestionInfo::default();
        info.add_buffered_receipt_gas(config.max_congestion_outgoing_gas / 2).unwrap();

        let shard = 2;
        let other_shards = [0, 1, 3, 4];

        // Test without missed chunks congestion.

        let missed_chunks_count = 0;
        let mut control = CongestionControl::new(config, info, missed_chunks_count);
        control.info.finalize_allowed_shard(shard, &other_shards, 3, &config);

        let expected_outgoing_limit =
            0.5 * config.min_outgoing_gas as f64 + 0.5 * config.max_outgoing_gas as f64;
        for other_shard in other_shards {
            assert_eq!(control.outgoing_limit(other_shard), expected_outgoing_limit as u64);
        }

        // Test with some missed chunks congestion.

        let missed_chunks_count = 8;
        let mut control = CongestionControl::new(config, info, missed_chunks_count);
        control.info.finalize_allowed_shard(shard, &other_shards, 3, &config);

        let expected_outgoing_limit =
            mix(config.max_outgoing_gas, config.min_outgoing_gas, 0.8) as f64;
        for other_shard in other_shards {
            assert_eq!(control.outgoing_limit(other_shard), expected_outgoing_limit as u64);
        }

        // Test with full missed chunks congestion.

        let missed_chunks_count = config.max_congestion_missed_chunks;
        let mut control = CongestionControl::new(config, info, missed_chunks_count);
        control.info.finalize_allowed_shard(shard, &other_shards, 3, &config);

        // The allowed shard should be set to own shard. None of the other
        // shards should be allowed to send anything.
        let expected_outgoing_limit = 0;
        for other_shard in other_shards {
            assert_eq!(control.outgoing_limit(other_shard), expected_outgoing_limit as u64);
        }
    }
}
