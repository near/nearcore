/// Nightshade v2
use std::cmp::{max, min, Ordering};
use std::collections::HashSet;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

use near_protos::nightshade as nightshade_proto;
use primitives::crypto::aggregate_signature::{
    uncompressed_bs58_signature_serializer, AggregatePublicKey, BlsAggregateSignature,
    BlsPublicKey, BlsSignature,
};
use primitives::hash::CryptoHash;
use primitives::serialize::Encode;
use primitives::crypto::signature::{Signature, bs58_serializer};
use primitives::crypto::signer::{BLSSigner, EDSigner};
use primitives::traits::Base58Encoded;
use primitives::types::{AuthorityId, BlockIndex};
use primitives::utils::proto_to_type;
use protobuf::SingularPtrField;

const PROTO_ERROR: &str = "Bad Proto";
const COMMIT_THRESHOLD: i64 = 3;

// TODO: Move common types from nightshade to primitives and remove pub from nightshade.
// Only exposed interface should be NightshadeTask

/// Result of updating Nightshade instance with new triplet
pub type NSResult = Result<Option<State>, String>;

/// BlockHeaders are used instead of Blocks as authorities proposal in the consensus.
/// They are used to avoid receiving two different proposals from the same authority,
/// and penalize such behavior.
#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct BlockProposal {
    /// Authority proposing the block.
    pub author: AuthorityId,
    /// Hash of the payload contained in the block.
    pub hash: CryptoHash,
}

impl BlockProposal {
    pub fn sign(&self, signer: &EDSigner) -> Signature {
        signer.sign(self.hash.as_ref())
    }
}

impl TryFrom<nightshade_proto::BlockProposal> for BlockProposal {
    type Error = String;

    fn try_from(proto: nightshade_proto::BlockProposal) -> Result<Self, Self::Error> {
        Ok(BlockProposal { hash: proto.hash.try_into()?, author: proto.author as AuthorityId })
    }
}

impl From<BlockProposal> for nightshade_proto::BlockProposal {
    fn from(block_proposal: BlockProposal) -> Self {
        nightshade_proto::BlockProposal {
            hash: block_proposal.hash.into(),
            author: block_proposal.author as u64,
            ..Default::default()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConsensusBlockProposal {
    pub proposal: BlockProposal,
    pub index: BlockIndex,
}

/// Triplet that describe the state of each authority in the consensus.
///
/// Notes:
/// We are running consensus on authorities rather than on outcomes, `endorses` refers to an authority.
/// "outcome" will be used instead of "authority" to avoid confusion.
///
/// The order of the fields are very important since lexicographical comparison is used derived from `PartialEq`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq, Hash)]
pub struct BareState {
    /// How much confidence we have on `endorses`.
    pub primary_confidence: i64,
    /// It is the outcome with higher confidence. (Higher `endorses` values are used as tie breaker)
    pub endorses: BlockProposal,
    /// Confidence of outcome with second higher confidence.
    pub secondary_confidence: i64,
}

impl TryFrom<nightshade_proto::BareState> for BareState {
    type Error = String;

    fn try_from(proto: nightshade_proto::BareState) -> Result<Self, Self::Error> {
        let endorses = proto_to_type(proto.endorses)?;
        Ok(BareState {
            primary_confidence: proto.primary_confidence,
            endorses,
            secondary_confidence: proto.secondary_confidence,
        })
    }
}

impl From<BareState> for nightshade_proto::BareState {
    fn from(state: BareState) -> Self {
        nightshade_proto::BareState {
            primary_confidence: state.primary_confidence,
            endorses: SingularPtrField::some(state.endorses.into()),
            secondary_confidence: state.secondary_confidence,
            ..Default::default()
        }
    }
}

impl BareState {
    /// Empty triplets are used as starting point believe on authorities from which
    /// we have not received any update. This state is less than any valid triplet.
    fn empty() -> Self {
        Self {
            primary_confidence: -1,
            endorses: BlockProposal { author: 0, hash: CryptoHash::default() },
            secondary_confidence: -1,
        }
    }

    pub fn new(author: AuthorityId, hash: CryptoHash) -> Self {
        Self {
            primary_confidence: 0,
            endorses: BlockProposal { author, hash },
            secondary_confidence: 0,
        }
    }

    pub fn bs_encode(&self) -> Vec<u8> {
        self.encode().expect("Fail serializing triplet.")
    }

    pub fn verify(&self) -> Result<(), NSVerifyErr> {
        if self.primary_confidence >= self.secondary_confidence && self.secondary_confidence >= 0 {
            Ok(())
        } else {
            Err(NSVerifyErr::InvalidTriplet)
        }
    }
}

/// `Proof` contains the evidence that we can have confidence `C` on some outcome `O` (and second higher confidence is `C'`)
/// It must have signatures from more than 2/3 authorities on triplets of the form `(C - 1, O, C')`
///
/// This is a lazy data structure. Aggregated signature is computed after all BLS parts are supplied.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    pub bare_state: BareState,
    mask: Vec<bool>,
    #[serde(with = "bs58_serializer")]
    signature: BlsSignature,
}

impl TryFrom<nightshade_proto::Proof> for Proof {
    type Error = String;

    fn try_from(proto: nightshade_proto::Proof) -> Result<Self, Self::Error> {
        let signature = Base58Encoded::from_base58(&proto.signature);
        match (proto_to_type(proto.bare_state), signature) {
            (Ok(bare_state), Ok(signature)) => {
                Ok(Proof { bare_state, mask: proto.mask, signature })
            }
            _ => Err(PROTO_ERROR.to_string()),
        }
    }
}

impl From<Proof> for nightshade_proto::Proof {
    fn from(proof: Proof) -> nightshade_proto::Proof {
        nightshade_proto::Proof {
            bare_state: SingularPtrField::some(proof.bare_state.into()),
            mask: proof.mask,
            signature: proof.signature.to_base58(),
            ..Default::default()
        }
    }
}

impl Proof {
    pub fn new(bare_state: BareState, mask: Vec<bool>, signature: BlsSignature) -> Self {
        Self { bare_state, mask, signature }
    }

    pub fn verify(
        &self,
        public_keys: &Vec<BlsPublicKey>,
        weights: &Vec<usize>,
    ) -> Result<(), NSVerifyErr> {
        // Verify that this proof contains enough signature in order to be accepted as valid.
        let current_weight: usize = self
            .mask
            .iter()
            .zip(weights)
            .filter_map(|(bit, weight)| if *bit { Some(weight) } else { None })
            .sum();

        let total_weight: usize = weights.iter().sum();

        if current_weight <= total_weight * 2 / 3 {
            return Err(NSVerifyErr::MissingSignatures);
        }

        let mut aggregated_pk = AggregatePublicKey::new();
        for (active, pk) in self.mask.iter().zip(public_keys) {
            if *active {
                aggregated_pk.aggregate(pk);
            }
        }
        let pk = aggregated_pk.get_key();

        if pk.verify(&self.bare_state.bs_encode(), &self.signature) {
            Ok(())
        } else {
            Err(NSVerifyErr::InvalidProof)
        }
    }
}

/// Possible error after verification one state
pub enum NSVerifyErr {
    // Bad formed triplet. Primary confidence must be equal or greater than secondary confidence.
    // Both confidence must be non negative integers.
    InvalidTriplet,
    // Bls signature provided doesn't match against bls public key + triplet data
    InvalidBlsSignature,
    // Proof doesn't contain enough signatures to increase confidence
    MissingSignatures,
    // Proofs provided are incorrect. Aggregated signature doesn't match with aggregated public keys + triplet data
    InvalidProof,
    // The proof for the current triplet is wrong. Confidence/Outcomes are not valid.
    InconsistentState,
    // There is a proof for a triplet that doesn't require it.
    ExtraProof,
    // Triplet requires a proof that is not present
    MissingProof,
}

/// `State` is a wrapper for `BareState` that contains evidence for such triplet.
///
/// Proof for `primary_confidence` is a set of states of size greater than 2 / 3 * num_authorities signed
/// by different authorities such that our current confidence (`primary_confidence`) on outcome `endorses`
/// is consistent whit this set according to Nightshade rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct State {
    /// Triplet that describe the state
    pub bare_state: BareState,
    /// Proof for `primary_confidence`.
    pub primary_proof: Option<Proof>,
    /// Proof for `secondary_bare_state`.
    pub secondary_proof: Option<Proof>,
    /// Signature of the `bare_state` from the authority emitting this state.
    #[serde(with = "uncompressed_bs58_signature_serializer")]
    pub signature: BlsSignature,
}

impl TryFrom<nightshade_proto::State> for State {
    type Error = String;

    fn try_from(proto: nightshade_proto::State) -> Result<Self, Self::Error> {
        let signature = Base58Encoded::from_base58(&proto.signature);
        let bare_state = proto_to_type(proto.bare_state);
        let primary_proof =
            proto.primary_proof.into_option().and_then(|proof| proof.try_into().ok());
        let secondary_proof =
            proto.secondary_proof.into_option().and_then(|proof| proof.try_into().ok());
        match (bare_state, signature) {
            (Ok(bare_state), Ok(signature)) => {
                Ok(State { bare_state, primary_proof, secondary_proof, signature })
            }
            _ => Err(PROTO_ERROR.to_string()),
        }
    }
}

impl From<State> for nightshade_proto::State {
    fn from(state: State) -> nightshade_proto::State {
        nightshade_proto::State {
            bare_state: SingularPtrField::some(state.bare_state.into()),
            primary_proof: SingularPtrField::from_option(
                state.primary_proof.map(std::convert::Into::into),
            ),
            secondary_proof: SingularPtrField::from_option(
                state.secondary_proof.map(std::convert::Into::into),
            ),
            signature: state.signature.to_base58(),
            ..Default::default()
        }
    }
}

impl State {
    /// Create new state
    pub fn new(author: AuthorityId, hash: CryptoHash, signer: Arc<BLSSigner>) -> Self {
        let bare_state = BareState::new(author, hash);
        let signature = signer.bls_sign(&bare_state.bs_encode());
        Self { bare_state, primary_proof: None, secondary_proof: None, signature }
    }

    /// Create state with empty triplet.
    /// See `BareState::empty` for more information
    ///
    /// Note: The signature of this state is going to be incorrect, but this state
    /// will never be sended to other participants as current state.
    pub fn empty() -> Self {
        Self {
            bare_state: BareState::empty(),
            primary_proof: None,
            secondary_proof: None,
            signature: BlsSignature::empty(),
        }
    }

    /// Create new State with increased confidence using `proof`
    pub fn increase_confidence(&self, proof: Proof, signer: Arc<BLSSigner>) -> Self {
        let bare_state = BareState {
            primary_confidence: self.bare_state.primary_confidence + 1,
            endorses: self.bare_state.endorses.clone(),
            secondary_confidence: self.bare_state.secondary_confidence,
        };

        let signature = signer.bls_sign(&bare_state.bs_encode());

        Self {
            bare_state,
            primary_proof: Some(proof),
            secondary_proof: self.secondary_proof.clone(),
            signature,
        }
    }

    /// Returns whether an authority having this triplet should commit to this triplet outcome.
    fn can_commit(&self) -> bool {
        self.bare_state.primary_confidence
            >= self.bare_state.secondary_confidence + COMMIT_THRESHOLD
    }

    /// BlockHeader (Authority and Block) that this state is endorsing.
    fn endorses(&self) -> BlockProposal {
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

impl Eq for State {}

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
                let mut aggregated_signature = BlsAggregateSignature::new();
                let mut mask = vec![false; self.num_authorities];

                // Collect proofs to create new state
                for (a, bit) in mask.iter_mut().enumerate() {
                    if self.states[a] == *my_state {
                        *bit = true;
                        aggregated_signature.aggregate(&self.states[a].signature);
                    }
                }

                let proof = Proof::new(
                    my_state.bare_state.clone(),
                    mask,
                    aggregated_signature.get_signature(),
                );

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
    use primitives::crypto::aggregate_signature::AggregateSignature;

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
