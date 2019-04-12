/// Nightshade v2
use std::cmp::{max, min};
use std::collections::HashSet;
use std::sync::Arc;

use crate::verifier::NightshadeVerifier;
use primitives::crypto::aggregate_signature::BlsPublicKey;
use primitives::crypto::group_signature::GroupSignature;
use primitives::crypto::signer::BLSSigner;
use primitives::nightshade::NSResult;
pub use primitives::nightshade::{BareState, BlockProposal, ConsensusBlockProposal, Proof, State};
use primitives::types::AuthorityId;

fn merge(state0: &State, state1: &State) -> State {
    let mut max_state = max(state0, state1).clone();
    let min_state = min(state0, state1);

    if max_state.endorses() != min_state.endorses() {
        if min_state.bare_state.primary_confidence > max_state.bare_state.secondary_confidence {
            max_state.bare_state.secondary_confidence = min_state.bare_state.primary_confidence;
            max_state.secondary_proof = min_state.primary_proof.clone();
        }
    } else {
        if min_state.bare_state.secondary_confidence > max_state.bare_state.secondary_confidence {
            max_state.bare_state.secondary_confidence = min_state.bare_state.secondary_confidence;
            max_state.secondary_proof = min_state.secondary_proof.clone();
        }
    }

    max_state
}

/// Check when two states received from the same authority are incompatible.
/// Two incompatible states are evidence of malicious behavior.
fn incompatible_states(state0: &State, state1: &State) -> bool {
    let merged = merge(state0, state1);
    let max_state = max(state0, state1);

    &merged != max_state
}

/// # Nightshade
///
/// Each authority must have one Nightshade instance to compute its state, given updates from
/// other authorities. It contains the logic of the consensus algorithm.
pub struct Nightshade {
    /// Id of the authority holding this Nightshade instance.
    /// It is an integer from [0..num_authorities).
    pub owner_id: AuthorityId,
    /// Number of authorities running consensus
    pub num_authorities: usize,
    /// Weight of each authority on the consensus. Different authority have different weights
    /// depending on their stake. Whenever we refer to more than 2 / 3 * num_authorities approvals,
    /// it means that weights sum of approving authorities is more than 2 / 3 of the total weight sum.
    pub weights: Vec<usize>,
    /// Current state (triplet) of each authority in the consensus from the point of view
    /// of the authority holding this Nightshade instance.
    pub states: Vec<State>,
    /// Bitmask informing if an authority has been marked as an adversary. All further updates
    /// from it are ignored.
    is_adversary: Vec<bool>,
    /// Weight sum of participants endorsing same triplet as the authority holding this Nightshade instance.
    pub best_state_weight: usize,
    /// Sum of weights of all participants on the consensus.
    pub total_weight: usize,
    /// Triplets that have been already verified. Each different triplet is going to be verified
    /// as correct at most one time. (If verification fails, the triplet is not stored, and need
    /// to be verified again).
    seen_bare_states: HashSet<BareState>,
    /// It is Some(outcome) when the authority holding this Nightshade instance has committed
    /// to some proposal. Nightshade consensus "guarantees" that if an authority commits to some
    /// value then every other honest authority will not commit to a different value.
    pub committed: Option<BlockProposal>,
    /// BLS Public Keys of all authorities participating in consensus.
    bls_public_keys: Vec<BlsPublicKey>,
    /// Signer object that can sign bytes with appropriate BLS secret key.
    signer: Arc<BLSSigner>,
}

impl Nightshade {
    pub fn new(
        owner_id: AuthorityId,
        num_authorities: usize,
        block_proposal: BlockProposal,
        bls_public_keys: Vec<BlsPublicKey>,
        signer: Arc<BLSSigner>,
    ) -> Self {
        assert_eq!(owner_id, block_proposal.author);
        let mut states = vec![];

        for a in 0..num_authorities {
            if a == owner_id {
                states.push(State::new(a, block_proposal.hash, signer.clone()));
            } else {
                states.push(State::empty());
            }
        }

        // Mark first authority triplet as seen from the beginning
        let mut seen_bare_states = HashSet::new();
        seen_bare_states.insert(states[owner_id].bare_state.clone());

        // TODO(#378): Use real weights from stake. This info should be public through the beacon chain.
        let weights = vec![1; num_authorities];
        let best_state_weight = weights[owner_id];
        let total_weight = weights.iter().sum();

        Self {
            owner_id,
            num_authorities,
            weights,
            states,
            is_adversary: vec![false; num_authorities],
            best_state_weight,
            total_weight,
            seen_bare_states,
            committed: None,
            bls_public_keys,
            signer,
        }
    }

    /// Current state of the authority
    pub fn state(&self) -> &State {
        &self.states[self.owner_id]
    }

    pub fn set_adversary(&mut self, authority_id: AuthorityId) {
        self.is_adversary[authority_id] = true;
    }

    pub fn update_state(&mut self, authority_id: AuthorityId, state: State) -> NSResult {
        if self.is_adversary[authority_id]
            || incompatible_states(&self.states[authority_id], &state)
        {
            self.is_adversary[authority_id] = true;
            return Err("Not processing adversaries updates".to_string());
        }

        // Verify this BareState only if it has not been successfully verified previously and ignore it forever
        if !self.seen_bare_states.contains(&state.bare_state) {
            match state.verify(authority_id, &self.bls_public_keys, &self.weights) {
                Ok(_) => self.seen_bare_states.insert(state.bare_state.clone()),
                Err(_e) => {
                    // TODO: return more information about why verification fails
                    return Err(format!("Not a valid signature on state from {}", authority_id));
                }
            };
        }

        if state.bare_state > self.states[authority_id].bare_state {
            self.states[authority_id] = state.clone();

            // We always take the best state seen so far
            // Note: If our state changes after merging with new state,
            // we are sure that this state and its proofs have been verified.
            let mut new_state = merge(&self.states[self.owner_id], &state);

            if new_state != self.states[self.owner_id] {
                // Sign new state (Only sign this state if we are going to accept it)
                new_state.signature = self.signer.bls_sign(&new_state.bare_state.bs_encode());
                self.states[self.owner_id] = new_state;
                // Reset `best_state_weight` with own weight, since state just changed.
                self.best_state_weight = self.weights[self.owner_id];
            }

            if state == self.states[self.owner_id] {
                self.best_state_weight += self.weights[state.bare_state.endorses.author];
            }

            // We MIGHT NEED to increase confidence AT MOST ONCE after have committed for first time.
            // But we don't need to increase it more than one time since if we commit at (C, C - 3)
            // nobody's second higher confidence can be C - 1 ever. The current implementation
            // doesn't bound confidence.
            if self.can_increase_confidence() {
                let my_state = &self.states[self.owner_id];
                let mut aggregated_signature = GroupSignature::default();

                // Collect proofs to create new state
                for a in 0..self.num_authorities {
                    if self.states[a] == *my_state {
                        aggregated_signature.add_signature(&self.states[a].signature, a);
                    }
                }

                let proof = Proof::new(my_state.bare_state.clone(), aggregated_signature);

                let new_state = my_state.increase_confidence(proof, self.signer.clone());
                // New state must be valid. Verifying is expensive! Enable this assert for testing.
                // assert_eq!(new_state.verify(self.owner_id, &self.bls_public_keys), true);
                self.seen_bare_states.insert(new_state.bare_state.clone());
                self.states[self.owner_id] = new_state;
                self.best_state_weight = self.weights[self.owner_id];
            }

            if self.states[self.owner_id].can_commit() {
                if let Some(endorse) = self.committed.clone() {
                    assert_eq!(endorse, self.states[self.owner_id].endorses());
                } else {
                    self.committed = Some(self.states[self.owner_id].endorses());
                }
            }

            Ok(Some(self.states[self.owner_id].clone()))
        } else {
            // It is not expected to receive a worst state than previously received,
            // unless there is an underlying gossiping mechanism that is not aware of which states
            // were previously delivered.

            Ok(None)
        }
    }

    /// Check if current authority can increase its confidence on its current endorsed outcome.
    /// Confidence is increased whenever we see that more than 2/3 of authorities endorsed our current state.
    fn can_increase_confidence(&self) -> bool {
        // We can use some fancy mechanism to not increase confidence every time we can, to avoid
        // being manipulated by malicious actors into a metastable equilibrium
        self.best_state_weight > self.total_weight * 2 / 3
    }

    /// Check if this authority have committed to some outcome.
    ///
    /// Note: The internal state of an authority might change after having committed, but the outcome
    /// will not change.
    pub fn is_final(&self) -> bool {
        self.committed.is_some()
    }
}

#[cfg(test)]
mod tests {
    use primitives::crypto::aggregate_signature::{AggregatePublicKey, AggregateSignature};

    use crate::testing_utils::*;

    use super::*;

    fn check_state_proofs(state: &State) {
        assert_eq!(state.bare_state.primary_confidence == 0, state.primary_proof.is_none());
        assert_eq!(state.bare_state.secondary_confidence == 0, state.secondary_proof.is_none());
    }

    #[test]
    fn bls_on_bare_states() {
        let signers = generate_signers(2);
        let triplet =
            BareState { primary_confidence: 0, endorses: proposal(1), secondary_confidence: 0 };
        // Aggregate signature
        let mut aggregated_signature = AggregateSignature::new();
        for signer in signers.iter() {
            let s = signer.bls_sign(&triplet.bs_encode());
            aggregated_signature.aggregate(&s);
        }
        let signature = aggregated_signature.get_signature();
        // Aggregate public keys
        let mut aggregated_pk = AggregatePublicKey::new();
        for signer in signers.iter() {
            aggregated_pk.aggregate(&signer.bls_public_key());
        }
        let a_pk = aggregated_pk.get_key();

        assert_eq!(a_pk.verify(&triplet.bs_encode(), &signature), true);
    }

    /// Check that nodes arrive consensus on sync environment if only first `prefix` authorities
    /// participate in the consensus.
    fn nightshade_partial_sync(num_rounds: usize, prefix: usize, mut ns: Vec<Nightshade>) {
        for _ in 0..num_rounds {
            let mut states = vec![];

            for i in 0..prefix {
                let state = ns[i].state();
                check_state_proofs(&state);
                states.push(state.clone());
            }

            for i in 0..prefix {
                for j in 0..prefix {
                    if i != j {
                        let result = ns[i].update_state(j, states[j].clone());
                        assert_eq!(result.is_ok(), true);
                    }
                }
            }
        }

        for i in 0..prefix {
            let s = ns[i].state();
            check_state_proofs(&s);
            assert_eq!(s.can_commit(), true);
        }
    }

    /// Check that nodes arrive consensus on sync environment
    fn nightshade_all_sync(num_authorities: usize, num_rounds: usize) {
        let ns = generate_nightshades(num_authorities);
        nightshade_partial_sync(num_rounds, num_authorities, ns);
    }

    #[test]
    fn test_nightshade_two_authorities() {
        nightshade_all_sync(2, 5);
    }

    #[test]
    fn test_nightshade_three_authorities() {
        nightshade_all_sync(3, 5);
    }

    #[test]
    fn test_nightshade_ten_authorities() {
        nightshade_all_sync(10, 5);
    }

    /// Run nightshade and where only authorities (0, 1, 2) are participating.
    /// They have more than 2/3 of the total weight:
    ///
    ///      (5 + 1 + 3) > 2 / 3 * (5 + 1 + 3 + 2 + 2)
    ///                9 > 2 / 3 * 13
    ///
    /// Hence they can make progress.
    #[test]
    fn test_nightshade_weights_ok() {
        let ns = generate_nightshades_from_weights(vec![5, 1, 3, 2, 2]);
        nightshade_partial_sync(7, 3, ns);
    }

    /// Run nightshade and where only authorities (0, 1, 2) are participating.
    /// They don't have more than 2/3 of the total weight:
    ///
    ///      (4 + 1 + 3) <= 2 / 3 * (4 + 1 + 3 + 2 + 2)
    ///                8 <= 2 / 3 * 12
    ///
    /// Hence they can't make progress.
    #[test]
    #[should_panic]
    fn test_nightshade_weights_fail() {
        let ns = generate_nightshades_from_weights(vec![4, 1, 3, 2, 2]);
        nightshade_partial_sync(10, 3, ns);
    }

    #[test]
    fn test_incompatible() {
        assert_eq!(incompatible_states(&state_empty(4, 1, 2), &state_empty(3, 1, 3)), true);
        assert_eq!(incompatible_states(&state_empty(4, 1, 3), &state_empty(3, 1, 3)), false);
        assert_eq!(incompatible_states(&state_empty(4, 2, 2), &state_empty(3, 1, 3)), true);
        assert_eq!(incompatible_states(&state_empty(4, 2, 2), &state_empty(3, 1, 2)), true);
    }

    #[test]
    fn test_order() {
        // Antisymmetry
        assert_eq!(bare_state(3, 3, 1) > bare_state(2, 3, 2), true);
        assert_eq!(bare_state(2, 3, 2) > bare_state(3, 3, 1), false);
        // No reflexive
        assert_eq!(bare_state(3, 3, 1) > bare_state(3, 3, 1), false);
        // Lexicographically correct
        assert_eq!(bare_state(3, 4, 1) > bare_state(3, 3, 2), true);
        assert_eq!(bare_state(3, 3, 3) > bare_state(3, 3, 2), true);
    }

    #[test]
    fn test_nightshade_basics() {
        let mut ns = generate_nightshades(2);
        let state0 = ns[0].state();
        assert_eq!(state0.endorses().author, 0);
        let state1 = ns[1].state().clone();
        assert_eq!(ns[0].update_state(1, state1).is_ok(), true);
        let state0 = ns[0].state();
        assert_eq!(state0.endorses().author, 1);
    }

    #[test]
    fn test_nightshade_basics_confidence() {
        let num_authorities = 4;
        let mut ns = generate_nightshades(num_authorities);

        for i in 0..2 {
            let state2 = ns[2].state().clone();
            assert_eq!(ns[i].update_state(2, state2).is_ok(), true);
            let state_i = ns[i].state().clone();
            assert_eq!(state_i.endorses().author, 2);

            assert_eq!(ns[2].update_state(i, state_i).is_ok(), true);
            let state2 = ns[2].state();

            // After update from authority 2 expected confidence is 0 since only authorities 1 and 2
            // endorse outcome 1. After update from authority 3, there are 3 authorities endorsing 1
            // with triplet (0, 1, 0) so confidence must be 1.
            assert_eq!(state2.endorses().author, 2);
            assert_eq!(state2.bare_state.primary_confidence, i as i64);
        }
    }

    /// Detect adversarial actions inside the consensus.
    /// Malicious authority send two incompatible states.
    #[test]
    fn malicious_detection() {
        let mut ns = nightshade(1, 2);
        let s0 = state(0, bare_state(1, 0, 0), 2);
        let s1 = state(0, bare_state(1, 1, 0), 2);

        assert_eq!(ns.update_state(0, s0).is_ok(), true);
        assert_eq!(ns.is_adversary[0], false);

        assert_eq!(ns.update_state(0, s1).is_err(), true);
        assert_eq!(ns.is_adversary[0], true);
    }
}
