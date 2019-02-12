use std::cmp::Ordering;
use std::collections::HashSet;

pub type AuthorityId = usize;
pub type BLSSignature = u64;

const COMMIT_THRESHOLD: i64 = 3;

pub enum NSResult {
    Updated(Option<State>),
    Error(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BareState {
    endorses: AuthorityId,
    confidence0: i64,
    confidence1: i64,
}

impl PartialOrd for BareState {
    fn partial_cmp(&self, other: &BareState) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BareState {
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

    fn empty() -> Self{
        Self{
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

    fn empty() -> Self{
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


fn update_confidence1(state0: &mut State, state1: &State) {
    // When this function is called it MUST hold that state0 >= state1. i.e.
    // assert_eq!(state1 > state0, false);

    if state0.endorses() != state1.endorses() {
        if state1.bare_state.confidence0 > state0.bare_state.confidence1 {
            state0.bare_state.confidence1 = state1.bare_state.confidence0;
            state0.proof1 = state1.proof0.clone();
        }
    }
    else{
        if state1.bare_state.confidence1 > state0.bare_state.confidence1 {
            state0.bare_state.confidence1 = state1.bare_state.confidence1;
            state0.proof1 = state1.proof1.clone();
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
            if i == owner_id{
                states.push(State::new(i));
            }
            else{
                states.push(State::empty());
            }
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

    fn state(&self) -> State {
        self.states[self.owner_id].clone()
    }

    fn update_state(&mut self, authority_id: AuthorityId, state: State) -> NSResult {
        if self.commited {
            return NSResult::Updated(None);
        }

        // Verify this BareState only if it has not been successfully verified previously
        if !self.seen_bare_states.contains(&state.bare_state) {
            if state.verify() {
                self.seen_bare_states.insert(state.bare_state.clone());
            } else {
                return NSResult::Error("Not valid state".to_string());
            }
        }

        // TODO: Check whether this two states are incompatible and tag this authority as malicious
        // and ignore it forever
        if state.bare_state > self.states[authority_id].bare_state {
            self.states[authority_id] = state.clone();

            // We always take the best state seen so far
            if state > self.states[self.owner_id] {
                self.best_state_counter = 1;

                let mut tmp_state = state.clone();
                update_confidence1(&mut tmp_state, &self.states[self.owner_id]);

                self.states[self.owner_id] = tmp_state;
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
        self.commited
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TODO: Test best_state_counter

    // TODO: Create special scenarios and test update_state on them

    // TODO: Test proofs are collected properly

    // TODO: Test consensus is reached on a sync scenario
    fn nightshade_all_sync(num_authorities: usize, num_rounds: usize) {
        let mut ns = vec![];

        for i in 0..num_authorities {
            ns.push(Nightshade::new(i, num_authorities));
        }

        for _ in 0..num_rounds {
            let mut states = vec![];

            for i in 0..num_authorities {
                let state = ns[i].state();
                println!("{:?}", state);

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
    }

    #[test]
    fn test_nightshade_two_authority(){
        nightshade_all_sync(2, 5);
    }

    #[test]
    fn test_nightshade_basics(){
        let mut ns0 = Nightshade::new(0, 2);
        let mut ns1 = Nightshade::new(1, 2);
        let state1 = ns1.state();
        assert_eq!(state1.endorses(), 1);
        let state0 = ns0.state();
        ns1.update_state(0, state0.clone());
        let state1 = ns1.state();
        assert_eq!(state1.endorses(), 1);
    }

    #[test]
    fn test_nightshade_basics_confidence() {
        let num_authorities = 4;

        let mut ns = vec![];

        for i in 0..num_authorities {
            ns.push(Nightshade::new(i, num_authorities));
        }

        for i in 2..4 {
            let state1 = ns[1].state();
            println!("{:?} -> {:?}", i, state1);
            ns[i].update_state(1, state1);
            let state_i = ns[i].state();
            assert_eq!(state_i.endorses(), 1);
            println!("{:?}", state_i);

            ns[1].update_state(i, state_i);
            let state1 = ns[1].state();
            println!("{:?} -> {:?}", i, state1);

            // After update from authority 2 expected confidence is 0 since only authorities 1 and 2
            // endorse outcome 1. After update from authority 3, there are 3 authorities endorsing 1
            // with triplet (1, 0, 0) so confidence must be 1.
            assert_eq!(state1.endorses(), 1);
            assert_eq!(state1.bare_state.confidence0, (i - 2) as i64);
        }
    }
}
