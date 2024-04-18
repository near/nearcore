use {
    super::{NumBlocks, ValidatorStats},
    borsh::{self, BorshDeserialize, BorshSerialize},
    std::{
        borrow::Cow,
        io::{self, Read, Write},
    },
};

/// An extension to `ValidatorStats` which also tracks endorsements
/// coming from stateless validators. This struct is backwards compatible
/// with `ValidatorStats` in the sense that the V1 serialization is identical
/// to the original `ValidatorStats`.
#[derive(Clone, Debug, Eq)]
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
    /// We assume the that number of chunks produced by a single validator never exceeds
    /// this number, otherwise we will not be able to accurately distinguish
    /// V1 vs V2 serialized stats.
    const V1_CUTOFF: u64 = 0xffffffff;

    pub const fn new() -> Self {
        Self::V2(ChunkValidatorStatsV2 {
            production: NValidatorStats { produced: NU64(0), expected: NU64(0) },
            endorsement: ValidatorStats { produced: 0, expected: 0 },
        })
    }

    pub const fn new_with_production(produced: u64, expected: u64) -> Self {
        Self::V2(ChunkValidatorStatsV2 {
            production: NValidatorStats { produced: NU64(produced), expected: NU64(expected) },
            endorsement: ValidatorStats { produced: 0, expected: 0 },
        })
    }

    pub const fn new_with_endorsement(produced: u64, expected: u64) -> Self {
        Self::V2(ChunkValidatorStatsV2 {
            production: NValidatorStats { produced: NU64(0), expected: NU64(0) },
            endorsement: ValidatorStats { produced, expected },
        })
    }

    pub fn produced(&self) -> NumBlocks {
        match self {
            Self::V1(v1) => v1.produced,
            Self::V2(v2) => v2.production.produced.0,
        }
    }

    pub fn expected(&self) -> NumBlocks {
        match self {
            Self::V1(v1) => v1.expected,
            Self::V2(v2) => v2.production.expected.0,
        }
    }

    pub fn produced_mut(&mut self) -> &mut NumBlocks {
        match self {
            Self::V1(v1) => &mut v1.produced,
            Self::V2(v2) => &mut v2.production.produced.0,
        }
    }

    pub fn expected_mut(&mut self) -> &mut NumBlocks {
        match self {
            Self::V1(v1) => &mut v1.expected,
            Self::V2(v2) => &mut v2.production.expected.0,
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
                    production: NValidatorStats {
                        produced: NU64(v1.produced),
                        expected: NU64(v1.expected),
                    },
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
    pub production: NValidatorStats,
    pub endorsement: ValidatorStats,
}

/// Identical to `ValidatorStats`, but with different serialization
/// to allow distinguishing later versions of `ChunkValidatorStats`
/// from the original `ValidatorStats`.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct NValidatorStats {
    pub produced: NU64,
    pub expected: NU64,
}

/// Wrapper around `u64` with a different Borsh serialization.
/// This allows distinguishing later versions of `ChunkValidatorStats`
/// from the original `ValidatorStats`.
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NU64(pub u64);

impl BorshSerialize for NU64 {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let x = !self.0;
        x.serialize(writer)
    }
}

impl BorshDeserialize for NU64 {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let x: u64 = BorshDeserialize::deserialize_reader(reader)?;
        Ok(Self(!x))
    }
}

// If the endorsement part of V2 is all zeros then it can possibly
// be considered equal to a V1 instance.
fn v1_v2_eq(v1: &ValidatorStats, v2: &ChunkValidatorStatsV2) -> bool {
    v2.endorsement.produced == 0
        && v2.endorsement.expected == 0
        && v1.produced == v2.production.produced.0
        && v1.expected == v2.production.expected.0
}

/// This type is structurally is identical to `ChunkValidatorStats`.
/// The difference is that this type derives the borsh traits to avoid
/// boiler-plate, while `ChunkValidatorStats` has a custom serialization
/// implementation for backwards compatibility with `ValidatorStats`.
#[derive(BorshSerialize, BorshDeserialize, Debug, PartialEq, Eq)]
enum AutoBorsh<'a> {
    V1(Cow<'a, ValidatorStats>),
    V2(Cow<'a, ChunkValidatorStatsV2>),
}

impl<'a> From<&'a ChunkValidatorStats> for AutoBorsh<'a> {
    fn from(value: &'a ChunkValidatorStats) -> Self {
        match value {
            ChunkValidatorStats::V1(v1) => Self::V1(Cow::Borrowed(v1)),
            ChunkValidatorStats::V2(v2) => Self::V2(Cow::Borrowed(v2)),
        }
    }
}

impl<'a> From<AutoBorsh<'a>> for ChunkValidatorStats {
    fn from(value: AutoBorsh<'a>) -> Self {
        match value {
            AutoBorsh::V1(v1) => Self::V1(v1.into_owned()),
            AutoBorsh::V2(v2) => Self::V2(v2.into_owned()),
        }
    }
}

impl BorshSerialize for ChunkValidatorStats {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            // V1 is backwards compatible with `ValidatorStats`
            Self::V1(v1) => v1.serialize(writer),
            // Later versions follow the usual borsh serialization convention
            later_version => AutoBorsh::from(later_version).serialize(writer),
        }
    }
}

impl BorshDeserialize for ChunkValidatorStats {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let x: u64 = BorshDeserialize::deserialize_reader(reader)?;

        // If the first 8 bytes deserialize as a small u64 number
        // then it must be a V1 type because the V2 type uses `NU64`,
        // which would look like big u64 numbers under normal borsh rules.
        if x <= Self::V1_CUTOFF {
            let expected: u64 = BorshDeserialize::deserialize_reader(reader)?;
            let v1 = ValidatorStats { produced: x, expected };
            return Ok(Self::V1(v1));
        }

        // Serialized size is 1 + 8 + 8 + 8 + 8
        // (enum variant byte, production stats, endorsement stats)
        let mut serialized_bytes = [0u8; 33];
        serialized_bytes[0..8].copy_from_slice(&x.to_le_bytes());
        reader.read_exact(&mut serialized_bytes[8..33])?;

        let auto_borsh: AutoBorsh = borsh::from_slice(&serialized_bytes)?;
        Ok(auto_borsh.into())
    }
}

#[test]
fn test_mutability() {
    let mut stats = ChunkValidatorStats::V1(ValidatorStats::default());

    *stats.expected_mut() += 1;
    assert_eq!(stats, ChunkValidatorStats::V1(ValidatorStats { produced: 0, expected: 1 }));

    *stats.produced_mut() += 1;
    assert_eq!(stats, ChunkValidatorStats::V1(ValidatorStats { produced: 1, expected: 1 }));

    // Getting endorsement stats for V1 automatically upgrades to V2.
    let endorsement_stats = stats.endorsement_stats_mut();
    endorsement_stats.produced += 10;
    endorsement_stats.expected += 10;

    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: NValidatorStats { produced: NU64(1), expected: NU64(1) },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        })
    );

    *stats.expected_mut() += 1;
    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: NValidatorStats { produced: NU64(1), expected: NU64(2) },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        })
    );

    *stats.produced_mut() += 1;
    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: NValidatorStats { produced: NU64(2), expected: NU64(2) },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        })
    );

    let endorsement_stats = stats.endorsement_stats_mut();
    endorsement_stats.produced += 10;
    endorsement_stats.expected += 10;

    assert_eq!(
        stats,
        ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
            production: NValidatorStats { produced: NU64(2), expected: NU64(2) },
            endorsement: ValidatorStats { produced: 20, expected: 20 }
        })
    );
}

#[test]
fn test_eq() {
    const PRODUCED: u64 = 123;
    const EXPECTED: u64 = 456;
    let v1 = ChunkValidatorStats::V1(ValidatorStats { produced: PRODUCED, expected: EXPECTED });
    let v2 = ChunkValidatorStats::new_with_production(PRODUCED, EXPECTED);

    // V1 and V2 can be equal if the endorsements parts of V2 are zero.
    assert_eq!(v1, v2);
}

#[test]
fn test_borsh_serialization() {
    let stats = ValidatorStats {
        produced: ChunkValidatorStats::V1_CUTOFF - 1,
        expected: ChunkValidatorStats::V1_CUTOFF,
    };
    let serialized = borsh::to_vec(&stats).unwrap();

    // `ChunkValidatorStats` is backwards compatible with `ValidatorStats` so
    // we can deserialize into `ChunkValidatorStats`.
    let result: ChunkValidatorStats = borsh::from_slice(&serialized).unwrap();
    assert_eq!(result, ChunkValidatorStats::V1(stats));

    let v2_stats = ChunkValidatorStats::V2(ChunkValidatorStatsV2 {
        production: NValidatorStats {
            produced: NU64(ChunkValidatorStats::V1_CUTOFF - 1),
            expected: NU64(ChunkValidatorStats::V1_CUTOFF),
        },
        endorsement: ValidatorStats { produced: 0, expected: 0 },
    });
    let serialized = borsh::to_vec(&v2_stats).unwrap();

    let result: ChunkValidatorStats = borsh::from_slice(&serialized).unwrap();
    assert_eq!(result, v2_stats);
}
