use near_primitives::types::{BlockChunkValidatorStats, ValidatorStats};
use num_bigint::BigUint;
use num_rational::{BigRational, Ratio};
use primitive_types::U256;

/// Computes the overall online (uptime) ratio of the validator.
/// This is an average of block produced / expected, chunk produced / expected,
/// and chunk endorsed produced / expected.
/// Note that it returns `Ratio<U256>` in raw form (not reduced).
///
/// # Arguments
///
/// * `stats` - stats for block and chunk production and chunk endorsement
/// * `endorsement_cutoff_threshold` - if set, a number between 0 and 100 (percentage) that
///   represents the minimum endorsement ratio below which the ratio is treated 0, and 1 otherwise
pub(crate) fn get_validator_online_ratio(
    stats: &BlockChunkValidatorStats,
    endorsement_cutoff_threshold: Option<u8>,
) -> Ratio<U256> {
    let expected_blocks = stats.block_stats.expected;
    let expected_chunks = stats.chunk_stats.expected();

    let (produced_endorsements, expected_endorsements) =
        get_endorsement_ratio(stats.chunk_stats.endorsement_stats(), endorsement_cutoff_threshold);

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
                let produced_chunks = stats.chunk_stats.produced();

                (U256::from(produced_chunks), U256::from(expected_chunks))
            }
            // Validator was only a block producer
            (expected_blocks, 0, 0) => {
                let produced_blocks = stats.block_stats.produced;

                (U256::from(produced_blocks), U256::from(expected_blocks))
            }
            // Validator produced blocks and chunks, but not endorsements
            (expected_blocks, expected_chunks, 0) => {
                let produced_blocks = stats.block_stats.produced;
                let produced_chunks = stats.chunk_stats.produced();

                let numer = U256::from(
                    produced_blocks * expected_chunks + produced_chunks * expected_blocks,
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
/// cspell:words bigdenom bignumer
pub(crate) fn get_sortable_validator_online_ratio(stats: &BlockChunkValidatorStats) -> BigRational {
    let ratio = get_validator_online_ratio(stats, None);
    let mut bytes: [u8; size_of::<U256>()] = [0; size_of::<U256>()];
    ratio.numer().to_little_endian(&mut bytes);
    let bignumer = BigUint::from_bytes_le(&bytes);
    ratio.denom().to_little_endian(&mut bytes);
    let bigdenom = BigUint::from_bytes_le(&bytes);
    BigRational::new(bignumer.try_into().unwrap(), bigdenom.try_into().unwrap())
}

/// Applies the `cutoff_threshold` to the endorsement ratio encoded in the `stats`.
/// If `cutoff_threshold` is not provided, returns the same ratio from the `stats`.
/// If `cutoff_threshold` is provided, compares the endorsement ratio from the `stats` with the threshold.
/// If the ratio is below the threshold, it returns 0, otherwise it returns 1.
fn get_endorsement_ratio(stats: &ValidatorStats, cutoff_threshold: Option<u8>) -> (u64, u64) {
    let (numer, denom) = if stats.expected == 0 {
        debug_assert_eq!(stats.produced, 0);
        (0, 0)
    } else if let Some(threshold) = cutoff_threshold {
        if stats.less_than(threshold) { (0, 1) } else { (1, 1) }
    } else {
        (stats.produced, stats.expected)
    };
    (numer, denom)
}

#[cfg(test)]
mod test {
    use near_primitives::types::{BlockChunkValidatorStats, ChunkStats, ValidatorStats};
    use num_bigint::BigInt;
    use num_rational::{Ratio, Rational32};
    use primitive_types::U256;

    use crate::validator_stats::{get_sortable_validator_online_ratio, get_validator_online_ratio};

    const VALIDATOR_STATS: BlockChunkValidatorStats = BlockChunkValidatorStats {
        block_stats: ValidatorStats { produced: 98, expected: 100 },
        chunk_stats: ChunkStats {
            production: ValidatorStats { produced: 76, expected: 100 },
            endorsement: ValidatorStats { produced: 42, expected: 100 },
        },
    };

    const VALIDATOR_STATS_NO_ENDORSEMENT: BlockChunkValidatorStats = BlockChunkValidatorStats {
        block_stats: ValidatorStats { produced: 98, expected: 100 },
        chunk_stats: ChunkStats {
            production: ValidatorStats { produced: 76, expected: 100 },
            endorsement: ValidatorStats { produced: 0, expected: 0 },
        },
    };

    #[test]
    fn test_average_uptime_ratio_without_endorsement_cutoff() {
        let endorsement_cutoff = None;
        let actual_ratio: Ratio<U256> =
            get_validator_online_ratio(&VALIDATOR_STATS, endorsement_cutoff);
        let expected_ratio: Ratio<i32> =
            (Rational32::new(98, 100) + Rational32::new(76, 100) + Rational32::new(42, 100)) / 3;
        assert_eq!(
            actual_ratio.numer() * expected_ratio.denom(),
            actual_ratio.denom() * expected_ratio.numer()
        );
    }

    #[test]
    fn test_average_uptime_ratio_with_endorsement_cutoff_passed() {
        let endorsement_cutoff = Some(30);
        let actual_ratio: Ratio<U256> =
            get_validator_online_ratio(&VALIDATOR_STATS, endorsement_cutoff);
        let expected_ratio: Ratio<i32> =
            (Rational32::new(98, 100) + Rational32::new(76, 100) + Rational32::from_integer(1)) / 3;
        assert_eq!(
            actual_ratio.numer() * expected_ratio.denom(),
            actual_ratio.denom() * expected_ratio.numer()
        );
    }

    #[test]
    fn test_average_uptime_ratio_with_endorsement_cutoff_not_passed() {
        let endorsement_cutoff = Some(50);
        let actual_ratio: Ratio<U256> =
            get_validator_online_ratio(&VALIDATOR_STATS, endorsement_cutoff);
        let expected_ratio: Ratio<i32> =
            (Rational32::new(98, 100) + Rational32::new(76, 100) + Rational32::from_integer(0)) / 3;
        assert_eq!(
            actual_ratio.numer() * expected_ratio.denom(),
            actual_ratio.denom() * expected_ratio.numer()
        );
    }

    #[test]
    fn test_average_uptime_ratio_with_no_endorsement_expected() {
        let endorsement_cutoff = Some(50);
        let actual_ratio: Ratio<U256> =
            get_validator_online_ratio(&VALIDATOR_STATS_NO_ENDORSEMENT, endorsement_cutoff);
        let expected_ratio: Ratio<i32> = (Rational32::new(98, 100) + Rational32::new(76, 100)) / 2;
        assert_eq!(
            actual_ratio.numer() * expected_ratio.denom(),
            actual_ratio.denom() * expected_ratio.numer()
        );
    }

    #[test]
    fn test_sortable_average_uptime_ratio() {
        let actual_ratio: Ratio<BigInt> = get_sortable_validator_online_ratio(&VALIDATOR_STATS);
        let expected_ratio: Ratio<i32> =
            (Rational32::new(98, 100) + Rational32::new(76, 100) + Rational32::new(42, 100)) / 3;
        assert_eq!(
            actual_ratio.numer() * expected_ratio.denom(),
            actual_ratio.denom() * expected_ratio.numer()
        );
    }

    #[test]
    fn test_sortable_average_uptime_ratio_with_no_endorsement_expected() {
        let actual_ratio: Ratio<BigInt> =
            get_sortable_validator_online_ratio(&VALIDATOR_STATS_NO_ENDORSEMENT);
        let expected_ratio: Ratio<i32> = (Rational32::new(98, 100) + Rational32::new(76, 100)) / 2;
        assert_eq!(
            actual_ratio.numer() * expected_ratio.denom(),
            actual_ratio.denom() * expected_ratio.numer()
        );
    }
}
