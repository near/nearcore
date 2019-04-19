use std::sync::Arc;

use primitives::crypto::aggregate_signature::BlsAggregateSignature;
use primitives::hash::hash_struct;
use primitives::crypto::signer::BLSSigner;
use primitives::crypto::signer::InMemorySigner;
use primitives::types::AuthorityId;

use crate::nightshade::{BareState, BlockProposal, Nightshade, Proof, State};

/// Dummy proposal from authority id
pub fn proposal(authority: AuthorityId) -> BlockProposal {
    BlockProposal { author: authority, hash: hash_struct(&authority) }
}

/// Dummy signer from authority id (use default seed)
fn signer(authority: AuthorityId) -> Arc<InMemorySigner> {
    Arc::new(InMemorySigner::from_seed(&format!("{}", authority), &format!("{}", authority)))
}

pub fn generate_signers(total: usize) -> Vec<Arc<InMemorySigner>> {
    (0..total).map(|i| signer(i)).collect()
}

/// Create single nightshade instance from authority id
pub fn nightshade(authority: AuthorityId, num_authorities: usize) -> Nightshade {
    let public_keys = generate_signers(num_authorities).iter().map(|s| s.bls_public_key()).collect();

    Nightshade::new(
        authority,
        num_authorities,
        proposal(authority),
        public_keys,
        signer(authority),
    )
}

pub fn generate_nightshades(num_authorities: usize) -> Vec<Nightshade> {
    (0..num_authorities).map(|i| nightshade(i, num_authorities)).collect()
}

pub fn generate_nightshades_from_weights(weights: Vec<usize>) -> Vec<Nightshade> {
    let mut nightshades = generate_nightshades(weights.len());

    let total_weight = weights.iter().sum();

    for i in 0..weights.len() {
        nightshades[i].weights = weights.clone();
        nightshades[i].total_weight = total_weight;
        nightshades[i].best_state_weight = weights[i];
    }

    nightshades
}

/// BareState builder from (primary/secondary confidences and outcome)
pub fn bare_state(primary_confidence: i64, endorses: AuthorityId, secondary_confidence: i64) -> BareState {
    BareState { primary_confidence, endorses: proposal(endorses), secondary_confidence }
}

/// Create valid proof for BareState triplet (confidence, endorses, 0)
fn proof(confidence: i64, endorses: AuthorityId, num_authorities: usize) -> Option<Proof> {
    if confidence == 0 {
        return None;
    }

    let signers = generate_signers(num_authorities);
    let previous_bare_state = bare_state(confidence - 1, endorses, 0);

    let signature = (0..num_authorities)
        .map(|i| signers[i].bls_sign(&previous_bare_state.bs_encode()))
        .fold(BlsAggregateSignature::new(), |mut a_signature, signature| {
            a_signature.aggregate(&signature);
            a_signature
        })
        .get_signature();

    Some(Proof::new(
        previous_bare_state,
        vec![true; num_authorities],
        signature,
    ))
}

/// Build a state with valid proofs containing the given bare state
pub fn state(authority: AuthorityId, bare_state: BareState, num_authorities: usize) -> State {
    let signature = signer(authority).bls_sign(&bare_state.bs_encode());

    let primary_confidence = bare_state.primary_confidence;
    let secondary_confidence = bare_state.secondary_confidence;
    let primary_endorse = bare_state.endorses.author;
    let secondary_endorse = if primary_endorse == 0 { 1usize } else { 0usize };

    State {
        bare_state,
        primary_proof: proof(primary_confidence, primary_endorse, num_authorities),
        secondary_proof: proof(secondary_confidence, secondary_endorse, num_authorities),
        signature,
    }
}

/// Create a state with given bare_state information from empty state
/// It is expected that proofs and signature for the created state are incorrect.
pub fn state_empty(primary_confidence: i64, endorses: AuthorityId, secondary_confidence: i64) -> State {
    let mut state: State = State::empty();
    state.bare_state = bare_state(primary_confidence, endorses, secondary_confidence);
    state
}

/// Create an instance of nightshade setting the states directly
fn create_hardcoded_nightshade(owner_id: AuthorityId, bare_states: Vec<BareState>) -> Nightshade {
    let num_authorities = bare_states.len();

    let mut ns = nightshade(owner_id, num_authorities);

    ns.states = vec![];
    ns.best_state_weight = 0;

    for bare_state in bare_states.iter() {
        let state = state(owner_id, bare_state.clone(), num_authorities);
        ns.states.push(state);
        if bare_state == &bare_states[owner_id] {
            ns.best_state_weight += 1;
        }
    }

    ns
}

/// Create nightshade instance from `bare_sates`. Each authority will have given `bare_state`.
/// Internal states will be created with proper proofs. With this macros impossible situations
/// can be created manually.
///
/// Example: Create a nightshade instance with `owner_id=2` and state of participant 2 (4, 1, 4).
/// It is impossible for a set of honest nodes to arrive to such situations, but it is useful,
/// to test different aspects of the consensus.
///
/// ```
/// let mut ns = ns_like!(2, [
///     (0, 0, 0),
///     (0, 0, 0),
///     (4, 1, 4),
/// ]);
/// ```
macro_rules! ns_like {
    ($owner_id:expr, $bare_states:expr) => {{
        {
            let bs = ($bare_states).iter().map(|(a, b, c)| bare_state(*a, *b, *c)).collect();
            create_hardcoded_nightshade($owner_id, bs)
        }
    }};
}

/// Compare nightshade instance with a vector of `bare_states` and check both are equal
///
/// ```
/// let ns = nightshade(0, 3);
///
/// // Make some operations with `ns` instance ...
///
/// ns_eq!(ns, [
///     (2, 0, 1),
///     (1, 0, 0),
///     (1, 0, 0),
/// ]);
/// ```
macro_rules! ns_eq {
    ($nightshade:expr, $bare_states:expr) => {
        $bare_states
            .iter()
            .map(|(a, b, c)| bare_state(*a, *b, *c))
            .zip($nightshade.states
                .iter()
                .map(|s| s.bare_state.clone())
                )
            .all(|(bs0, bs1)| bs0 == bs1)
    };
}

/// Update a state on a nightshade instance using `bare_state` description
///
/// Example: send update state from authority 0 with triplet (2, 0, 1)
/// primary_confidence=2, endorse=0, secondary_confidence=1
///
/// ```
/// let ns = nightshade(0, 3);
/// update_state!(ns, 0, (2, 0, 1));
/// ```
macro_rules! update_state {
    ($nightshade:expr, $owner_id:expr, ($a:expr, $b:expr, $c:expr)) => {{
        {
            let s = state($owner_id, bare_state($a, $b, $c), $nightshade.num_authorities);
            $nightshade.update_state($owner_id, s)
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Check best state counter is properly updated after an state update
    #[test]
    fn simple_hardcoded_situation() {
        let mut ns = ns_like!(2, [
            (0, 0, 0),
            (0, 2, 0),
            (0, 2, 0)
        ]);
        assert_eq!(ns.best_state_weight, 2);
        assert_eq!(update_state!(ns, 0, (0, 2, 0)).is_ok(), true);
        assert_eq!(ns.best_state_weight, 1);
        assert_eq!(ns_eq!(ns, [
            (0, 2, 0),
            (0, 2, 0),
            (1, 2, 0),
        ]), true);
    }

    /// Check secondary confidence is updated after an update with different endorsement
    /// than ours, but higher primary confidence than our secondary confidence.
    #[test]
    fn correct_secondary_confidence() {
        // If we are at the state (4, B, 4)
        // and get update (5, A, 3)
        // the next state must be (5, A, 4)
        let mut ns = ns_like!(2, [
            (0, 0, 0),
            (0, 0, 0),
            (4, 1, 4),
        ]);
        assert_eq!(update_state!(ns, 0, (5, 0, 3)).is_ok(), true);
        assert_eq!(ns_eq!(ns, [
            (5, 0, 3),
            (0, 0, 0),
            (5, 0, 4),
        ]), true);
    }
}
