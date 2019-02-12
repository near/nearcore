use std::cmp::max;
use std::cmp::min;
use std::cmp::Ordering;
use std::collections::HashSet;

pub type AuthorityId = usize;
pub type BLSSignature = u64;

const COMMIT_THRESHOLD: i64 = 3;

pub enum NSResult {
    Updated(Option<State>),
    Error(String),
}

#[derive(Debug, Clone, PartialOrd, Ord, PartialEq, Eq, Hash)]
struct BareState {
    confidence0: i64,
    endorses: AuthorityId,
    confidence1: i64,
}

impl BareState {
    fn new(endorses: AuthorityId) -> Self {
        Self {
            endorses,
            confidence0: 0,
            confidence1: 0,
        }
    }

    fn empty() -> Self {
        Self {
            endorses: 0,
            confidence0: -1,
            confidence1: -1,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SignedState {
    signature: BLSSignature,
    parent: Vec<BareState>,
}

impl SignedState {
    fn new() -> Self {
        Self {
            signature: 0,
            parent: vec![],
        }
    }

    fn update(&mut self, state: &State) {
        // TODO: Update self.signature using state.get_signature
        self.parent.push(state.bare_state.clone());
    }
}

#[derive(Debug, Clone, Eq)]
pub struct State {
    bare_state: BareState,
    proof0: Option<SignedState>,
    proof1: Option<SignedState>,
}

impl State {
    fn new(endorses: AuthorityId) -> Self {
        Self {
            bare_state: BareState::new(endorses),
            proof0: None,
            proof1: None,
        }
    }

    fn empty() -> Self {
        Self {
            bare_state: BareState::empty(),
            proof0: None,
            proof1: None,
        }
    }

    /// Create new State with increased confidence using some proof
    fn increase_confidence(&self, proof: SignedState) -> Self {
        Self {
            bare_state: BareState {
                endorses: self.bare_state.endorses,
                confidence0: self.bare_state.confidence0 + 1,
                confidence1: self.bare_state.confidence1,
            },
            proof0: Some(proof),
            proof1: self.proof1.clone(),
        }
    }

    fn can_commit(&self) -> bool {
        self.bare_state.confidence0 >= self.bare_state.confidence1 + COMMIT_THRESHOLD
    }

    fn verify(&self) -> bool {
        true
    }

    fn get_signature(&self) -> BLSSignature {
        0
    }

    fn endorses(&self) -> usize {
        self.bare_state.endorses
    }
}

impl PartialEq for State {
    fn eq(&self, other: &State) -> bool {
        self.bare_state.eq(&other.bare_state)
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &State) -> Option<Ordering> {
        self.bare_state.partial_cmp(&other.bare_state)
    }
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        self.bare_state.cmp(&other.bare_state)
    }
}


fn merge(state0: &State, state1: &State) -> State {
    let mut max_state = max(state0, state1).clone();
    let min_state = min(state0, state1);

    if max_state.endorses() != min_state.endorses() {
        if min_state.bare_state.confidence0 > max_state.bare_state.confidence1 {
            max_state.bare_state.confidence1 = min_state.bare_state.confidence0;
            max_state.proof1 = min_state.proof0.clone();
        }
    } else {
        if min_state.bare_state.confidence1 > max_state.bare_state.confidence1 {
            max_state.bare_state.confidence1 = min_state.bare_state.confidence1;
            max_state.proof1 = min_state.proof1.clone();
        }
    }

    max_state
}

/// Check when two states received from the same authority are incompatible
/// Two incompatible states are evidence of malicious behavior.
fn incompatible_states(state0: &State, state1: &State) -> bool {
    let merged = merge(state0, state1);
    let max_state = max(state0, state1);

    &merged != max_state
}

pub struct Nightshade {
    owner_id: AuthorityId,
    num_authorities: usize,
    states: Vec<State>,
    // TODO: Use bitmask
    is_adversary: Vec<bool>,
    best_state_counter: usize,
    seen_bare_states: HashSet<BareState>,
    committed: Option<AuthorityId>,
}

impl Nightshade {
    fn new(owner_id: AuthorityId, num_authorities: usize) -> Self {
        let mut states = vec![];

        for i in 0..num_authorities {
            if i == owner_id {
                states.push(State::new(i));
            } else {
                states.push(State::empty());
            }
        }

        Self {
            owner_id,
            num_authorities,
            states,
            is_adversary: vec![false; num_authorities],
            best_state_counter: 1,
            seen_bare_states: HashSet::new(),
            committed: None,
        }
    }

    fn state(&self) -> State {
        self.states[self.owner_id].clone()
    }

    fn update_state(&mut self, authority_id: AuthorityId, state: State) -> NSResult {
        if self.is_adversary[authority_id] ||
            incompatible_states(&self.states[authority_id], &state) {
            self.is_adversary[authority_id] = true;
            return NSResult::Error("Not processing adversaries updates".to_string());
        }

        // Verify this BareState only if it has not been successfully verified previously
        // and ignore it forever
        if !self.seen_bare_states.contains(&state.bare_state) {
            if state.verify() {
                self.seen_bare_states.insert(state.bare_state.clone());
            } else {
                return NSResult::Error("Not valid state".to_string());
            }
        }

        if state.bare_state > self.states[authority_id].bare_state {
            self.states[authority_id] = state.clone();

            // We always take the best state seen so far
            let new_state = merge(&self.states[self.owner_id], &state);

            if new_state != self.states[self.owner_id] {
                self.states[self.owner_id] = new_state;
                self.best_state_counter = 1;
            }

            if state == self.states[self.owner_id] {
                self.best_state_counter += 1;
            }

            // We MIGHT NEED to increase confidence AT MOST ONCE after have committed for first time.
            // But we don't need to increase it more than one time since if we commit at (C, C - 3)
            // nobody's second higher confidence can be C - 1 ever. The current implementation
            // doesn't bound confidence.
            if self.can_increase_confidence() {
                let mut proof = SignedState::new();

                // Collect proofs to create new state
                for i in 0..self.num_authorities {
                    if &self.states[i] == &self.states[self.owner_id] {
                        proof.update(&self.states[i]);
                    }
                }

                let new_state = self.states[self.owner_id].increase_confidence(proof);

                self.states[self.owner_id] = new_state;

                self.best_state_counter = 1;
            }

            if self.states[self.owner_id].can_commit() {
                if let Some(endorse) = self.committed {
                    assert_eq!(endorse, self.states[self.owner_id].endorses());
                } else {
                    self.committed = Some(self.states[self.owner_id].endorses());
                }
            }

            NSResult::Updated(Some(self.states[self.owner_id].clone()))
        } else {
            // It is not expected to receive a worst state than previously received,
            // unless there is an underlying gossiping mechanism that is not aware of which states
            // were previously delivered.

            NSResult::Updated(None)
        }
    }

    fn can_increase_confidence(&self) -> bool {
        // Confidence is increased whenever we see that more than 2/3 of participants endorsed
        // our current state.
        // We can use some fancy mechanism to not increase confidence every time we can, to avoid
        // being manipulated by malicious actors into a metastable equilibrium
        self.best_state_counter > self.num_authorities * 2 / 3
    }

    fn is_final(&self) -> bool {
        self.committed.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nightshade_all_sync(num_authorities: usize, num_rounds: usize) {
        let mut ns = vec![];

        for i in 0..num_authorities {
            ns.push(Nightshade::new(i, num_authorities));
        }

        for _ in 0..num_rounds {
            let mut states = vec![];

            for i in 0..num_authorities {
                let state = ns[i].state();
                states.push(state);
            }

            for i in 0..num_authorities {
                for j in 0..num_authorities {
                    if i != j {
                        ns[i].update_state(j, states[j].clone());
                    }
                }
            }
        }

        for i in 0..num_authorities {
            let m = ns[i].state();
            assert_eq!(m.can_commit(), true);
        }
    }

    #[test]
    fn test_nightshade_two_authority() {
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

    fn bare_state(confidence0: i64, endorses: AuthorityId, confidence1: i64) -> BareState {
        BareState {
            confidence0,
            endorses,
            confidence1,
        }
    }

    fn state(confidence0: i64, endorses: AuthorityId, confidence1: i64) -> State {
        State {
            bare_state: bare_state(confidence0, endorses, confidence1),
            proof0: None,
            proof1: None,
        }
    }

    #[test]
    fn test_incompatible() {
        assert_eq!(incompatible_states(&state(4, 1, 2), &state(3, 1, 3)), true);
        assert_eq!(incompatible_states(&state(4, 1, 3), &state(3, 1, 3)), false);
        assert_eq!(incompatible_states(&state(4, 2, 2), &state(3, 1, 3)), true);
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
        let mut ns0 = Nightshade::new(0, 2);
        let mut ns1 = Nightshade::new(1, 2);
        let state0 = ns0.state();
        assert_eq!(state0.endorses(), 0);
        let state1 = ns1.state();
        ns0.update_state(1, state1.clone());
        let state0 = ns0.state();
        assert_eq!(state0.endorses(), 1);
    }

    #[test]
    fn test_nightshade_basics_confidence() {
        let num_authorities = 4;

        let mut ns = vec![];

        for i in 0..num_authorities {
            ns.push(Nightshade::new(i, num_authorities));
        }

        for i in 0..2 {
            let state2 = ns[2].state();
            ns[i].update_state(2, state2);
            let state_i = ns[i].state();
            assert_eq!(state_i.endorses(), 2);

            ns[2].update_state(i, state_i);
            let state2 = ns[2].state();

            // After update from authority 2 expected confidence is 0 since only authorities 1 and 2
            // endorse outcome 1. After update from authority 3, there are 3 authorities endorsing 1
            // with triplet (0, 1, 0) so confidence must be 1.
            assert_eq!(state2.endorses(), 2);
            assert_eq!(state2.bare_state.confidence0, i as i64);
        }
    }

    // TODO: Test proofs are collected properly
    // TODO: Test malicious actors are detected and handled properly

    /// Create an instance of nightshade setting the states directly
    fn create_hardcoded_nightshade(owner_id: AuthorityId, bare_states: Vec<BareState>) -> Nightshade {
        let num_authorities = bare_states.len();

        let mut ns = Nightshade::new(owner_id, num_authorities);

        ns.states = vec![];
        ns.best_state_counter = 0;

        for (i, bare_state) in bare_states.iter().enumerate() {
            let state = State { bare_state: bare_state.clone(), proof0: None, proof1: None };
            ns.states.push(state);

            if bare_state == &bare_states[owner_id] {
                ns.best_state_counter += 1;
            }
        }

        ns
    }

    /// Compare nightshades only by their states (believe on other authorities states including himself)
    fn nightshade_equal(ns0: &Nightshade, ns1: &Nightshade) -> bool {
        if ns1.num_authorities != ns0.num_authorities {
            return false;
        }
        let num_authorities = ns0.num_authorities;
        for i in 0..num_authorities {
            if ns0.states[i].bare_state != ns1.states[i].bare_state {
                return false;
            }
        }
        true
    }

    #[test]
    fn simple_hardcoded_situation() {
        let mut ns = create_hardcoded_nightshade(2, vec![
            bare_state(0, 0, 0),
            bare_state(0, 2, 0),
            bare_state(0, 2, 0),
        ]);

        assert_eq!(ns.best_state_counter, 2);
        ns.update_state(0, state(0, 2, 0));
        assert_eq!(ns.best_state_counter, 1);

        assert_eq!(nightshade_equal(&ns, &create_hardcoded_nightshade(0, vec![
            bare_state(0, 2, 0),
            bare_state(0, 2, 0),
            bare_state(1, 2, 0),
        ])), true);
    }
}
