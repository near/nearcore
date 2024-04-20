use {
    super::{NumBlocks, ValidatorStats},
    borsh::{self, BorshDeserialize, BorshSerialize},
};

/// An extension to `ValidatorStats` which also tracks endorsements
/// coming from stateless validators. This struct is backwards compatible
/// with `ValidatorStats` in the sense that the V1 variant contains the
/// original type.
#[derive(BorshSerialize, BorshDeserialize, Clone, Debug, Eq)]
pub enum ChunkValidatorStats {
    V1(ValidatorStats),
    V2(ChunkValidatorStatsV2),
}

impl Default for ChunkValidatorStats {
    fn default() -> Self {
        Self::new()
    }
}

// Custom implementation of `PartialEq` to allow for equality
// between V1 and V2 if the `endorsement` part of V2 is all zeros.
impl PartialEq for ChunkValidatorStats {
    fn eq(&self, other: &Self) -> bool {
        match self {
            Self::V1(v1) => match other {
                Self::V1(other_v1) => v1 == other_v1,
                Self::V2(other_v2) => v1_v2_eq(v1, other_v2),
            },
            Self::V2(v2) => match other {
                Self::V1(other_v1) => v1_v2_eq(other_v1, v2),
                Self::V2(other_v2) => v2 == other_v2,
            },
        }
    }
}

impl ChunkValidatorStats {
    const DEFAULT_STATS: ValidatorStats = ValidatorStats { produced: 0, expected: 0 };

    pub const fn new() -> Self {
        Self::V2(ChunkValidatorStatsV2 {
            production: ValidatorStats { produced: 0, expected: 0 },
            endorsement: ValidatorStats { produced: 0, expected: 0 },
        })
    }

    pub const fn new_with_production(produced: u64, expected: u64) -> Self {
        Self::V2(ChunkValidatorStatsV2 {
            production: ValidatorStats { produced, expected },
            endorsement: ValidatorStats { produced: 0, expected: 0 },
        })
    }

    pub const fn v1(produced: u64, expected: u64) -> Self {
        Self::V1(ValidatorStats { produced, expected })
    }

    pub const fn new_with_endorsement(produced: u64, expected: u64) -> Self {
        Self::V2(ChunkValidatorStatsV2 {
            production: ValidatorStats { produced: 0, expected: 0 },
            endorsement: ValidatorStats { produced, expected },
        })
    }

    pub fn produced(&self) -> NumBlocks {
        match self {
            Self::V1(v1) => v1.produced,
            Self::V2(v2) => v2.production.produced,
        }
    }

    pub fn expected(&self) -> NumBlocks {
        match self {
            Self::V1(v1) => v1.expected,
            Self::V2(v2) => v2.production.expected,
        }
    }

    pub fn produced_mut(&mut self) -> &mut NumBlocks {
        match self {
            Self::V1(v1) => &mut v1.produced,
            Self::V2(v2) => &mut v2.production.produced,
        }
    }

    pub fn expected_mut(&mut self) -> &mut NumBlocks {
        match self {
            Self::V1(v1) => &mut v1.expected,
            Self::V2(v2) => &mut v2.production.expected,
        }
    }

    pub fn endorsement_stats(&self) -> &ValidatorStats {
        match self {
            Self::V1(_) => &Self::DEFAULT_STATS,
            Self::V2(v2) => &v2.endorsement,
        }
    }

    pub fn endorsement_stats_mut(&mut self) -> &mut ValidatorStats {
        match self {
            Self::V1(v1) => {
                *self = Self::V2(ChunkValidatorStatsV2 {
                    production: ValidatorStats { produced: v1.produced, expected: v1.expected },
                    endorsement: ValidatorStats::default(),
                });
                match self {
                    Self::V2(v2) => &mut v2.endorsement,
                    _ => unreachable!("self assigned V2 variant"),
                }
            }
            Self::V2(v2) => &mut v2.endorsement,
        }
    }
}

#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct ChunkValidatorStatsV2 {
    pub production: ValidatorStats,
    pub endorsement: ValidatorStats,
}

// If the endorsement part of V2 is all zeros then it can possibly
// be considered equal to a V1 instance.
fn v1_v2_eq(v1: &ValidatorStats, v2: &ChunkValidatorStatsV2) -> bool {
    v2.endorsement.produced == 0
        && v2.endorsement.expected == 0
        && v1.produced == v2.production.produced
        && v1.expected == v2.production.expected
}

#[test]
fn test_mutability() {
    let mut stats = ChunkValidatorStats::v1(0, 0);

    *stats.expected_mut() += 1;
    assert_eq!(stats, ChunkValidatorStats::v1(0, 1));

    *stats.produced_mut() += 1;
    assert_eq!(stats, ChunkValidatorStats::v1(1, 1));

    // Getting endorsement stats for V1 automatically upgrades to V2.
    let endorsement_stats = stats.endorsement_stats_mut();
    endorsement_stats.produced += 10;
    endorsement_stats.expected += 10;

    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: ValidatorStats { produced: 1, expected: 1 },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        })
    );

    *stats.expected_mut() += 1;
    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: ValidatorStats { produced: 1, expected: 2 },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        })
    );

    *stats.produced_mut() += 1;
    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: ValidatorStats { produced: 2, expected: 2 },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        })
    );

    let endorsement_stats = stats.endorsement_stats_mut();
    endorsement_stats.produced += 10;
    endorsement_stats.expected += 10;

    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: ValidatorStats { produced: 2, expected: 2 },
            endorsement: ValidatorStats { produced: 20, expected: 20 }
        })
    );
}

#[test]
fn test_eq() {
    const PRODUCED: u64 = 123;
    const EXPECTED: u64 = 456;
    let v1 = ChunkValidatorStats::v1(PRODUCED, EXPECTED);
    let v2 = ChunkValidatorStats::new_with_production(PRODUCED, EXPECTED);

    // V1 and V2 can be equal if the endorsements parts of V2 are zero.
    assert_eq!(v1, v2);
}
