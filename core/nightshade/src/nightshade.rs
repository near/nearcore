/// Nightshade v2
use std::cmp::{max, min, Ordering};
use std::collections::HashSet;

use serde::Serialize;

use primitives::hash::CryptoHash;
use primitives::hash::hash_struct;

pub type AuthorityId = usize;
pub type BLSSignature = u64;
pub type NSSignature = usize;

const COMMIT_THRESHOLD: i64 = 3;

pub enum NSResult {
    Updated(Option<State>),
    Error(String),
}

fn empty_cryptohash() -> CryptoHash {
    CryptoHash::new(&[0u8; 32])
}

/// Nightshade consensus run on top of outcomes proposed by each authority.
/// Blocks represent authorities proposal.
#[derive(Debug, Clone, Serialize)]
pub struct Block<P> {
    pub header: BlockHeader,
    payload: P,
}

impl<P: Serialize> Block<P> {
    pub fn new(author: AuthorityId, payload: P) -> Self {
        Self {
            header: BlockHeader {
                author,
                hash: hash_struct(&payload),
            },
            payload,
        }
    }

    /// Authority proposing the block
    pub fn author(&self) -> AuthorityId {
        self.header.author
    }

    /// Hash of the payload contained in the block
    pub fn hash(&self) -> CryptoHash {
        self.header.hash
    }
}

/// BlockHeaders are used instead of Blocks as authorities proposal in the consensus.
/// They are used to avoid receiving two different proposals from the same authority,
/// and penalize such behavior.
#[derive(Debug, Clone, Serialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct BlockHeader {
    /// Authority proposing the block.
    pub author: AuthorityId,
    /// Hash of the payload contained in the block.
    pub hash: CryptoHash,
}


/// Triplet that describe the state of each authority in the consensus.
///
/// Notes:
/// We are running consensus on authorities rather than on outcomes, `endorses` refers to an authority.
/// "outcome" will be used instead of "authority" to avoid confusion.
///
/// The order of the fields are very important since lexicographical comparison is used derived from `PartialEq`.
#[derive(Debug, Clone, Serialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct BareState {
    /// How much confidence we have on `endorses`.
    primary_confidence: i64,
    /// It is the outcome with higher confidence. (Higher `endorses` values are used as tie breaker)
    pub endorses: BlockHeader,
    /// Confidence of outcome with second higher confidence.
    secondary_confidence: i64,
}

impl BareState {
    /// Empty triplets are used as starting point believe on authorities from which
    /// we have not received any update. This state is less than any valid triplet.
    fn empty() -> Self {
        Self {
            primary_confidence: -1,
            endorses: BlockHeader { author: 0, hash: empty_cryptohash() },
            secondary_confidence: -1,
        }
    }

    fn new(author: AuthorityId, hash: CryptoHash) -> Self {
        Self {
            primary_confidence: 0,
            endorses: BlockHeader { author, hash },
            secondary_confidence: 0,
        }
    }
}

/// `BLSProof` contains the evidence that we can have confidence `C` on some outcome `O` (and second higher confidence is `C'`)
/// It must have signatures from more than 2/3 authorities on triplets of the form `(C - 1, O, C')`
///
/// This is a lazy data structure. Aggregated signature is computed after all BLS parts are supplied.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct BLSProof;

impl BLSProof {
    fn new() -> Self {
        Self {}
    }

    /// Add a state that will be used as evidence for the proof.
    fn update(&mut self, _state: &State) {
        // TODO: Update self.signature using state.get_signature
    }
}

/// `State` is a wrapper for `BareState` that contains evidence for such triplet.
///
/// Proof for `primary_confidence` is a set of states of size greater than 2 / 3 * num_authorities signed
/// by different authorities such that our current confidence (`primary_confidence`) on outcome `endorses`
/// is consistent whit this set according to Nightshade rules.
#[derive(Debug, Clone, Eq, Serialize)]
pub struct State {
    /// Triplet that describe the state
    pub bare_state: BareState,
    /// Proof for `primary_confidence`.
    proof0: Option<BLSProof>,
    /// Proof for `secondary_confidence`. This is `proof0` field of the state endorsing different outcome
    /// which has highest confidence from this authority point of view.
    proof1: Option<BLSProof>,
}

impl State {
    fn new(author: AuthorityId, hash: CryptoHash) -> Self {
        Self {
            bare_state: BareState::new(author, hash),
            proof0: None,
            proof1: None,
        }
    }

    /// Create state with empty triplet.
    /// See `BareState::empty` for more information
    fn empty() -> Self {
        Self {
            bare_state: BareState::empty(),
            proof0: None,
            proof1: None,
        }
    }

    /// Create new State with increased confidence using `proof`
    fn increase_confidence(&self, proof: BLSProof) -> Self {
        Self {
            bare_state: BareState {
                endorses: self.bare_state.endorses.clone(),
                primary_confidence: self.bare_state.primary_confidence + 1,
                secondary_confidence: self.bare_state.secondary_confidence,
            },
            proof0: Some(proof),
            proof1: self.proof1.clone(),
        }
    }

    /// Returns whether an authority having this triplet should commit to this triplet outcome.
    fn can_commit(&self) -> bool {
        self.bare_state.primary_confidence >= self.bare_state.secondary_confidence + COMMIT_THRESHOLD
    }

    /// Check if this state contains correct proofs about the triplet it contains.
    /// Authority will check if this state is valid only if it has not successfully verified another
    /// state with the same triplet before. Once it has verified that at least one authority has such
    /// triplet, it accepts all further states with the same triplet.
    fn verify(&self) -> bool {
        true
    }

    #[allow(dead_code)]
    fn get_signature(&self) -> BLSSignature {
        0
    }

    /// BlockHeader (Authority and Block) that this state is endorsing.
    fn endorses(&self) -> BlockHeader {
        self.bare_state.endorses.clone()
    }

    pub fn block_hash(&self) -> CryptoHash {
        self.bare_state.endorses.hash
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
        if min_state.bare_state.primary_confidence > max_state.bare_state.secondary_confidence {
            max_state.bare_state.secondary_confidence = min_state.bare_state.primary_confidence;
            max_state.proof1 = min_state.proof0.clone();
        }
    } else {
        if min_state.bare_state.secondary_confidence > max_state.bare_state.secondary_confidence {
            max_state.bare_state.secondary_confidence = min_state.bare_state.secondary_confidence;
            max_state.proof1 = min_state.proof1.clone();
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
    owner_id: AuthorityId,
    num_authorities: usize,
    states: Vec<State>,
    is_adversary: Vec<bool>,
    best_state_counter: usize,
    seen_bare_states: HashSet<BareState>,
    pub committed: Option<BlockHeader>,
}

impl Nightshade {
    pub fn new(owner_id: AuthorityId, num_authorities: usize, block_header: BlockHeader) -> Self {
        assert_eq!(owner_id, block_header.author);
        let mut states = vec![];

        for a in 0..num_authorities {
            if a == owner_id {
                states.push(State::new(a, block_header.hash));
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

    /// Current state of the authority
    pub fn state(&self) -> State {
        self.states[self.owner_id].clone()
    }

    pub fn set_adversary(&mut self, authority_id: AuthorityId) {
        self.is_adversary[authority_id] = true;
    }

    pub fn update_state(&mut self, authority_id: AuthorityId, state: State) -> NSResult {
        if self.is_adversary[authority_id] ||
            incompatible_states(&self.states[authority_id], &state) {
            self.is_adversary[authority_id] = true;
            return NSResult::Error("Not processing adversaries updates".to_string());
        }

        // Verify this BareState only if it has not been successfully verified previously and ignore it forever
        if !self.seen_bare_states.contains(&state.bare_state) {
            if state.verify() {
                self.seen_bare_states.insert(state.bare_state.clone());
            } else {
                return NSResult::Error("Not a valid state".to_string());
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
                let mut proof = BLSProof::new();

                // Collect proofs to create new state
                for i in 0..self.num_authorities {
                    if self.states[i] == self.states[self.owner_id] {
                        proof.update(&self.states[i]);
                    }
                }

                let new_state = self.states[self.owner_id].increase_confidence(proof);

                assert_eq!(new_state.verify(), true);
                self.seen_bare_states.insert(new_state.bare_state.clone());

                self.states[self.owner_id] = new_state;

                self.best_state_counter = 1;
            }

            if self.states[self.owner_id].can_commit() {
                if let Some(endorse) = self.committed.clone() {
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

    /// Check if current authority can increase its confidence on its current endorsed outcome.
    /// Confidence is increased whenever we see that more than 2/3 of authorities endorsed our current state.
    fn can_increase_confidence(&self) -> bool {
        // We can use some fancy mechanism to not increase confidence every time we can, to avoid
        // being manipulated by malicious actors into a metastable equilibrium
        self.best_state_counter > self.num_authorities * 2 / 3
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
    use super::*;

    fn check_state_proofs(state: &State) {
        // TODO: Check signature
        assert_eq!(state.bare_state.primary_confidence == 0, state.proof0 == None);
        assert_eq!(state.bare_state.secondary_confidence == 0, state.proof1 == None);
    }

    fn header(author: AuthorityId) -> BlockHeader {
        BlockHeader {
            author,
            hash: empty_cryptohash(),
        }
    }

    fn nightshade_all_sync(num_authorities: usize, num_rounds: usize) {
        let mut ns: Vec<_> = (0..num_authorities).map(|i| Nightshade::new(i, num_authorities, header(i))).collect();

        for _ in 0..num_rounds {
            let mut states = vec![];

            for i in 0..num_authorities {
                let state = ns[i].state();
                check_state_proofs(&state);
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
            let s = ns[i].state();
            check_state_proofs(&s);
            assert_eq!(s.can_commit(), true);
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

    fn bare_state(primary_confidence: i64, endorses: AuthorityId, secondary_confidence: i64) -> BareState {
        BareState {
            primary_confidence,
            endorses: header(endorses),
            secondary_confidence,
        }
    }

    fn state(primary_confidence: i64, endorses: AuthorityId, secondary_confidence: i64) -> State {
        State {
            bare_state: bare_state(primary_confidence, endorses, secondary_confidence),
            proof0: None,
            proof1: None,
        }
    }

    #[test]
    fn test_incompatible() {
        assert_eq!(incompatible_states(&state(4, 1, 2), &state(3, 1, 3)), true);
        assert_eq!(incompatible_states(&state(4, 1, 3), &state(3, 1, 3)), false);
        assert_eq!(incompatible_states(&state(4, 2, 2), &state(3, 1, 3)), true);
        assert_eq!(incompatible_states(&state(4, 2, 2), &state(3, 1, 2)), true);
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
        let mut ns0 = Nightshade::new(0, 2, header(0));
        let ns1 = Nightshade::new(1, 2, header(1));
        let state0 = ns0.state();
        assert_eq!(state0.endorses().author, 0);
        let state1 = ns1.state();
        ns0.update_state(1, state1.clone());
        let state0 = ns0.state();
        assert_eq!(state0.endorses().author, 1);
    }

    #[test]
    fn test_nightshade_basics_confidence() {
        let num_authorities = 4;

        let mut ns = vec![];

        for i in 0..num_authorities {
            ns.push(Nightshade::new(i, num_authorities, header(i)));
        }

        for i in 0..2 {
            let state2 = ns[2].state();
            ns[i].update_state(2, state2);
            let state_i = ns[i].state();
            assert_eq!(state_i.endorses().author, 2);

            ns[2].update_state(i, state_i);
            let state2 = ns[2].state();

            // After update from authority 2 expected confidence is 0 since only authorities 1 and 2
            // endorse outcome 1. After update from authority 3, there are 3 authorities endorsing 1
            // with triplet (0, 1, 0) so confidence must be 1.
            assert_eq!(state2.endorses().author, 2);
            assert_eq!(state2.bare_state.primary_confidence, i as i64);
        }
    }

    #[test]
    fn malicious_detection() {
        // Note: This test will become invalid after signatures are checked properly.
        let mut ns = Nightshade::new(1, 2, header(1));
        let s0 = State { bare_state: bare_state(1, 0, 0), proof0: None, proof1: None };
        let s1 = State { bare_state: bare_state(1, 1, 0), proof0: None, proof1: None };
        ns.update_state(0, s0);
        assert_eq!(ns.is_adversary[0], false);
        ns.update_state(0, s1);
        assert_eq!(ns.is_adversary[0], true);
    }

    /// Create an instance of nightshade setting the states directly
    fn create_hardcoded_nightshade(owner_id: AuthorityId, bare_states: Vec<BareState>) -> Nightshade {
        let num_authorities = bare_states.len();

        let mut ns = Nightshade::new(owner_id, num_authorities, header(owner_id));

        ns.states = vec![];
        ns.best_state_counter = 0;

        for bare_state in bare_states.iter() {
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

    #[test]
    fn correct_secondary_confidence() {
        // If we are at the state (4, B, 4)
        // and get update (5, A, 3)
        // the next state must be (5, A, 4)
        let mut ns = create_hardcoded_nightshade(2, vec![
            bare_state(0, 0, 0),
            bare_state(0, 0, 0),
            bare_state(4, 1, 4),
        ]);

        ns.update_state(0, state(5, 0, 3));

        assert_eq!(nightshade_equal(&ns, &create_hardcoded_nightshade(0, vec![
            bare_state(5, 0, 3),
            bare_state(0, 0, 0),
            bare_state(5, 0, 4),
        ])), true);
    }
}
