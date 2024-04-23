use {
    super::{NumBlocks, ValidatorStats},
    borsh::{self, BorshDeserialize, BorshSerialize},
};

/// An extension to `ValidatorStats` which also tracks endorsements
/// coming from stateless validators.
#[derive(Default, BorshSerialize, BorshDeserialize, Clone, Debug, PartialEq, Eq)]
pub struct ChunkValidatorStats {
    pub production: ValidatorStats,
    pub endorsement: ValidatorStats,
}

impl ChunkValidatorStats {
    pub const fn new_with_production(produced: u64, expected: u64) -> Self {
        ChunkValidatorStats {
            production: ValidatorStats { produced, expected },
            endorsement: ValidatorStats { produced: 0, expected: 0 },
        }
    }

    pub const fn new_with_endorsement(produced: u64, expected: u64) -> Self {
        ChunkValidatorStats {
            production: ValidatorStats { produced: 0, expected: 0 },
            endorsement: ValidatorStats { produced, expected },
        }
    }

    pub fn produced(&self) -> NumBlocks {
        self.production.produced
    }

    pub fn expected(&self) -> NumBlocks {
        self.production.expected
    }

    pub fn produced_mut(&mut self) -> &mut NumBlocks {
        &mut self.production.produced
    }

    pub fn expected_mut(&mut self) -> &mut NumBlocks {
        &mut self.production.expected
    }

    pub fn endorsement_stats(&self) -> &ValidatorStats {
        &self.endorsement
    }

    pub fn endorsement_stats_mut(&mut self) -> &mut ValidatorStats {
        &mut self.endorsement
    }
}

#[test]
fn test_mutability() {
    let mut stats = ChunkValidatorStats::new_with_production(0, 0);

    *stats.expected_mut() += 1;
    assert_eq!(stats, ChunkValidatorStats::new_with_production(0, 1));

    *stats.produced_mut() += 1;
    assert_eq!(stats, ChunkValidatorStats::new_with_production(1, 1));

    let endorsement_stats = stats.endorsement_stats_mut();
    endorsement_stats.produced += 10;
    endorsement_stats.expected += 10;

    assert_eq!(
        stats,
        ChunkValidatorStats {
            production: ValidatorStats { produced: 1, expected: 1 },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        }
    );

    *stats.expected_mut() += 1;
    assert_eq!(
        stats,
        ChunkValidatorStats {
            production: ValidatorStats { produced: 1, expected: 2 },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        }
    );

    *stats.produced_mut() += 1;
    assert_eq!(
        stats,
        ChunkValidatorStats {
            production: ValidatorStats { produced: 2, expected: 2 },
            endorsement: ValidatorStats { produced: 10, expected: 10 }
        }
    );

    let endorsement_stats = stats.endorsement_stats_mut();
    endorsement_stats.produced += 10;
    endorsement_stats.expected += 10;

    assert_eq!(
        stats,
        ChunkValidatorStats {
            production: ValidatorStats { produced: 2, expected: 2 },
            endorsement: ValidatorStats { produced: 20, expected: 20 }
        }
    );
}
