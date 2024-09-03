use near_primitives::types::BlockChunkValidatorStats;
use num_bigint::BigUint;
use num_rational::{BigRational, Ratio, Rational64};
use primitive_types::U256;

use crate::reward_calculator::{MinMaxRatio, ValidatorOnlineThresholds};

/// Computes the overall online (uptime) ratio of the validator.
/// This is an average of block produced / expected, chunk produced / expected,
/// and chunk endorsed produced / expected.
/// Note that it returns `Ratio<U256>` in raw form (not reduced).
pub(crate) fn get_validator_online_ratio(
    stats: &BlockChunkValidatorStats,
    thresholds: &ValidatorOnlineThresholds,
) -> Ratio<U256> {
    let expected_blocks = stats.block_stats.expected;
    let expected_chunks = stats.chunk_stats.expected();

    let endorsement_stats = stats.chunk_stats.endorsement_stats();
    // Apply the min/max endorsement threshold to the chunk endorsement stats to get new (remapped) values
    let (produced_endorsements, expected_endorsements) = apply_thresholds(
        endorsement_stats.produced,
        endorsement_stats.expected,
        &thresholds.endorsement,
    );

    let (average_produced_numer, average_produced_denom) =
        match (expected_blocks, expected_chunks, expected_endorsements) {
            // Validator was not expected to do anything
            (0, 0, 0) => (U256::from(0), U256::from(1)),
            // Validator was a stateless validator only (not expected to produce anything)
            (0, 0, expected_endorsements) => {
                (U256::from(produced_endorsements), U256::from(expected_endorsements))
            }
            // Validator was a chunk-only producer
            (0, expected_chunks, 0) => {
                (U256::from(stats.chunk_stats.produced()), U256::from(expected_chunks))
            }
            // Validator was only a block producer
            (expected_blocks, 0, 0) => {
                (U256::from(stats.block_stats.produced), U256::from(expected_blocks))
            }
            // Validator produced blocks and chunks, but not endorsements
            (expected_blocks, expected_chunks, 0) => {
                let numer = U256::from(
                    stats.block_stats.produced * expected_chunks
                        + stats.chunk_stats.produced() * expected_blocks,
                );
                let denom = U256::from(2 * expected_chunks * expected_blocks);
                (numer, denom)
            }
            // Validator produced chunks and endorsements, but not blocks
            (0, expected_chunks, expected_endorsements) => {
                let produced_chunks = stats.chunk_stats.produced();

                let numer = U256::from(
                    produced_endorsements * expected_chunks
                        + produced_chunks * expected_endorsements,
                );
                let denom = U256::from(2 * expected_chunks * expected_endorsements);
                (numer, denom)
            }
            // Validator produced blocks and endorsements, but not chunks
            (expected_blocks, 0, expected_endorsements) => {
                let produced_blocks = stats.block_stats.produced;

                let numer = U256::from(
                    produced_endorsements * expected_blocks
                        + produced_blocks * expected_endorsements,
                );
                let denom = U256::from(2 * expected_blocks * expected_endorsements);
                (numer, denom)
            }
            // Validator did all the things
            (expected_blocks, expected_chunks, expected_endorsements) => {
                let produced_blocks = stats.block_stats.produced;
                let produced_chunks = stats.chunk_stats.produced();

                let numer = U256::from(
                    produced_blocks * expected_chunks * expected_endorsements
                        + produced_chunks * expected_blocks * expected_endorsements
                        + produced_endorsements * expected_blocks * expected_chunks,
                );
                let denom =
                    U256::from(3 * expected_chunks * expected_blocks * expected_endorsements);
                (numer, denom)
            }
        };
    debug_assert_ne!(
        average_produced_denom,
        U256::zero(),
        "Denominator must be non-zero for Ratio."
    );
    // Note: This creates Ratio without checking if denom is zero and doing reduction.
    Ratio::<U256>::new_raw(average_produced_numer, average_produced_denom)
}

/// Computes the overall online (uptime) ratio of the validator for sorting.
/// The reason for this function is that U256 used in the core implementation
/// cannot be used with `Ratio<U256>` for sorting since it does not implement `num_integer::Integer`.
/// Instead of having a full-blown implementation of `U256`` for `num_integer::Integer`
/// we wrap the value in a `BigInt` for now.
/// TODO: Implement `num_integer::Integer` for `U256` and remove this function.
pub(crate) fn get_sortable_validator_online_ratio(
    stats: &BlockChunkValidatorStats,
    thresholds: &ValidatorOnlineThresholds,
) -> BigRational {
    let ratio = get_validator_online_ratio(stats, thresholds);
    let mut bytes: [u8; size_of::<U256>()] = [0; size_of::<U256>()];
    ratio.numer().to_little_endian(&mut bytes);
    let bignumer = BigUint::from_bytes_le(&bytes);
    ratio.denom().to_little_endian(&mut bytes);
    let bigdenom = BigUint::from_bytes_le(&bytes);
    BigRational::new(bignumer.try_into().unwrap(), bigdenom.try_into().unwrap())
}

/// Computes the overall online (uptime) ratio of the validator for sorting, ignoring the chunk endorsement stats.
pub(crate) fn get_sortable_validator_online_ratio_without_endorsements(
    stats: &BlockChunkValidatorStats,
) -> Rational64 {
    if stats.block_stats.expected == 0 && stats.chunk_stats.expected() == 0 {
        Rational64::from_integer(1)
    } else if stats.block_stats.expected == 0 {
        Rational64::new(stats.chunk_stats.produced() as i64, stats.chunk_stats.expected() as i64)
    } else if stats.chunk_stats.expected() == 0 {
        Rational64::new(stats.block_stats.produced as i64, stats.block_stats.expected as i64)
    } else {
        (Rational64::new(stats.chunk_stats.produced() as i64, stats.chunk_stats.expected() as i64)
            + Rational64::new(stats.block_stats.produced as i64, stats.block_stats.expected as i64))
            / 2
    }
}

/// Applies the min/max threshold to the produced/expected values to remap them between 0 and 1.
/// Returns numerator and denominator of the ratio computed as follows:
/// min(1, (produced/expected - min_threshold) / (max_threshold - min_threshold))
fn apply_thresholds(produced: u64, expected: u64, thresholds: &MinMaxRatio) -> (u64, u64) {
    if expected == 0 {
        return (0, 0);
    }
    if thresholds.min() == Ratio::from_integer(0) && thresholds.max() == Ratio::from_integer(1) {
        return (produced, expected);
    }
    let min_numer = *thresholds.min().numer() as u64;
    let min_denom = *thresholds.min().denom() as u64;
    if produced * min_denom < min_numer * expected {
        return (0, 0);
    }
    let max_numer = *thresholds.max().numer() as u64;
    let max_denom = *thresholds.max().denom() as u64;
    let online_numer = max_numer * min_denom - min_numer * max_denom;
    let uptime_numer = (produced * min_denom - min_numer * expected) * max_denom;
    let uptime_denom = online_numer * expected;
    // Apply min between 1. and computed uptime.
    if uptime_numer > uptime_denom {
        (1, 1)
    } else {
        (uptime_numer, uptime_denom)
    }
}
