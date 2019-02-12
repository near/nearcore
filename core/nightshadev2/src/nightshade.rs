use std::collections::HashSet;
use std::cmp::Ordering;

pub type AuthorityId = usize;
pub type BLSSignature = u64;

const COMMIT_THRESHOLD: usize = 3;

pub enum NSResult{
    Updated(Option<State>),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BareState {
    endorses: AuthorityId,
    confidence0: usize,
    confidence1: usize,
}

impl PartialOrd for BareState{
    fn partial_cmp(&self, other: &BareState) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BareState{
    fn cmp(&self, other: &Self) -> Ordering {
        if self.confidence0 != other.confidence0 {
            return if self.confidence0 > other.confidence0 {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }

        if self.endorses != other.endorses {
            return if self.endorses < other.endorses {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }

        if self.confidence1 != other.confidence1 {
            return if self.confidence1 > other.confidence1 {
                Ordering::Greater
            } else {
                Ordering::Less
            };
        }

        Ordering::Equal
    }
}

impl BareState {
    fn new(endorses: AuthorityId) -> Self {
        Self {
            endorses,
            confidence0: 0,
            confidence1: 0,
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct State {
    bare_state: BareState,

    // TODO: Proof might be empty at the beginning of consensus. Use enum instead?
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


fn update_confidence1(state0: &mut State, state1: &State){
    // When this function is called it MUST hold that state0 >= state1. i.e.
    // assert_eq!(state1 > state0, false);

    if state0.endorses() != state1.endorses() {
        if state1.bare_state.confidence0 > state0.bare_state.confidence1 {
            state0.bare_state.confidence1 = state1.bare_state.confidence0;
            state0.proof1 = state1.proof0.clone();
        }
    }
}

pub struct Nightshade {
    owner_id: AuthorityId,
    num_authorities: usize,
    states: Vec<State>,
    best_state_counter: usize,
    seen_bare_states: HashSet<BareState>,
    commited: bool,
}

impl Nightshade {
    fn new(owner_id: AuthorityId, num_authorities: usize) -> Self {
        let mut states = vec![];

        for i in 0..num_authorities {
            states.push(State::new(i));
        }

        Self {
            owner_id,
            num_authorities,
            states,
            best_state_counter: 1,
            seen_bare_states: HashSet::new(),
            commited: false,
        }
    }

    fn update_state(&mut self, authority_id: AuthorityId, state: State) -> NSResult {
        if commited {
            return NSResult::Updated(None)
        }

        // Verify this BareState only if it has not been successfully verified previously
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
            if state > self.states[self.owner_id] {
                self.best_state_counter = 0;
                self.states[self.owner_id] = state.clone();
            }

            if state == self.states[self.owner_id] {
                self.best_state_counter += 1;
            }

            if self.can_increase_confidence() {
                let mut proof = SignedState::new();

                // Collect proofs to create new state
                for i in 0..self.num_authorities {
                    if self.states[i] == state {
                        proof.update(&self.states[i]);
                    }
                }

                let new_state = self.states[self.owner_id].increase_confidence(proof);

                self.states[self.owner_id] = new_state;
            }

            update_confidence1(&mut self.states[self.owner_id], &state);

            if self.states[self.owner_id].can_commit() {
                self.commited = true
            }

            NSResult::Updated(Some(self.states[self.owner_id].clone()))
        } else {
            // TODO: It is not expected to receive a worst state than previously received,
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
        self.best_state_counter > self.owner_id * 2 / 3
    }

    fn is_final(&self) -> bool {
        self.commited
    }
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
