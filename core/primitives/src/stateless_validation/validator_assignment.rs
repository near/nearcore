use std::collections::HashSet;
use std::fmt::Debug;

use near_primitives_core::types::{AccountId, Balance};

#[derive(Debug)]
pub struct EndorsementStats {
    pub total_stake: Balance,
    pub endorsed_stake: Balance,
    pub total_validators_count: usize,
    pub endorsed_validators_count: usize,
}

impl EndorsementStats {
    pub fn has_enough_stake(&self) -> bool {
        self.endorsed_stake >= self.required_stake()
    }

    pub fn required_stake(&self) -> Balance {
        self.total_stake * 2 / 3 + 1
    }
}

#[derive(Debug, Default)]
pub struct ChunkValidatorAssignments {
    assignments: Vec<(AccountId, Balance)>,
    chunk_validators: HashSet<AccountId>,
}

impl ChunkValidatorAssignments {
    pub fn new(assignments: Vec<(AccountId, Balance)>) -> Self {
        let chunk_validators = assignments.iter().map(|(id, _)| id.clone()).collect();
        Self { assignments, chunk_validators }
    }

    pub fn len(&self) -> usize {
        self.assignments.len()
    }

    pub fn contains(&self, account_id: &AccountId) -> bool {
        self.chunk_validators.contains(account_id)
    }

    pub fn ordered_chunk_validators(&self) -> Vec<AccountId> {
        self.assignments.iter().map(|(id, _)| id.clone()).collect()
    }

    pub fn assignments(&self) -> &Vec<(AccountId, Balance)> {
        &self.assignments
    }

    pub fn compute_endorsement_stats(
        &self,
        endorsed_chunk_validators: &HashSet<&AccountId>,
    ) -> EndorsementStats {
        let mut total_stake = 0;
        let mut endorsed_stake = 0;
        let mut endorsed_validators_count = 0;
        for (account_id, stake) in &self.assignments {
            total_stake += stake;
            if endorsed_chunk_validators.contains(account_id) {
                endorsed_stake += stake;
                endorsed_validators_count += 1;
            }
        }
        EndorsementStats {
            total_stake,
            endorsed_stake,
            endorsed_validators_count,
            total_validators_count: self.assignments.len(),
        }
    }
}
