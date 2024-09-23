use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

use near_crypto::Signature;
use near_primitives_core::types::{AccountId, Balance};

use crate::block_body::ChunkEndorsementSignatures;

#[derive(Debug, Default)]
pub struct ChunkEndorsementsState {
    pub total_stake: Balance,
    pub endorsed_stake: Balance,
    pub total_validators_count: usize,
    pub endorsed_validators_count: usize,
    pub is_endorsed: bool,
    // Signatures are empty if the chunk is not endorsed
    pub signatures: ChunkEndorsementSignatures,
}

fn has_enough_stake(total_stake: Balance, endorsed_stake: Balance) -> bool {
    endorsed_stake >= required_stake(total_stake)
}

fn required_stake(total_stake: Balance) -> Balance {
    total_stake * 2 / 3 + 1
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

    pub fn compute_endorsement_state(
        &self,
        mut validator_signatures: HashMap<&AccountId, Signature>,
    ) -> ChunkEndorsementsState {
        let mut total_stake = 0;
        let mut endorsed_stake = 0;
        let mut endorsed_validators_count = 0;
        let mut signatures = vec![];
        for (account_id, stake) in &self.assignments {
            total_stake += stake;
            match validator_signatures.remove(account_id) {
                Some(signature) => {
                    endorsed_stake += stake;
                    endorsed_validators_count += 1;
                    signatures.push(Some(Box::new(signature)));
                }
                None => signatures.push(None),
            }
        }
        // Signatures are empty if the chunk is not endorsed
        let is_endorsed = has_enough_stake(total_stake, endorsed_stake);
        if !is_endorsed {
            signatures.clear();
        }
        ChunkEndorsementsState {
            total_stake,
            endorsed_stake,
            endorsed_validators_count,
            total_validators_count: self.assignments.len(),
            is_endorsed,
            signatures,
        }
    }
}
