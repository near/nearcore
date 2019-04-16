use std::convert::{TryFrom, TryInto};

use crate::crypto::aggregate_signature::{
    uncompressed_bs58_signature_serializer,
    BlsPublicKey, BlsSignature,
};
use crate::crypto::group_signature::GroupSignature;
use crate::crypto::signature::Signature;
use crate::crypto::signer::{BLSSigner, EDSigner};
use crate::hash::CryptoHash;
pub use crate::serialize::{Decode, Encode};
use crate::traits::Base58Encoded;
use crate::types::{AuthorityId, BlockIndex};
use crate::utils::proto_to_type;
use near_protos::nightshade as nightshade_proto;
use near_protos::chain as chain_proto;
use protobuf::SingularPtrField;
use std::cmp::Ordering;
use std::sync::Arc;

pub type GenericResult = Result<(), &'static str>;
const PROTO_ERROR: &str = "Bad Proto";

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

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq)]
pub struct ConsensusBlockProposal {
    pub proposal_with_proof: Proof,
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

/// `Proof` contains the evidence that we can have confidence `C` on some outcome `O` (and second higher confidence is `C'`)
/// It must have signatures from more than 2/3 authorities on triplets of the form `(C - 1, O, C')`
///
/// This is a lazy data structure. Aggregated signature is computed after all BLS parts are supplied.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Proof {
    pub bare_state: BareState,
    pub signature: GroupSignature,
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

// TODO: Move common types from nightshade to primitives and remove pub from nightshade.
// Only exposed interface should be NightshadeTask

/// Result of updating Nightshade instance with new triplet
pub type NSResult = Result<Option<State>, String>;

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

impl BlockProposal {
    pub fn sign(&self, signer: &EDSigner) -> Signature {
        signer.sign(self.hash.as_ref())
    }
}

impl TryFrom<chain_proto::BlockProposal> for BlockProposal {
    type Error = String;

    fn try_from(proto: chain_proto::BlockProposal) -> Result<Self, Self::Error> {
        Ok(BlockProposal { hash: proto.hash.try_into()?, author: proto.author as AuthorityId })
    }
}

impl From<BlockProposal> for chain_proto::BlockProposal {
    fn from(block_proposal: BlockProposal) -> Self {
        chain_proto::BlockProposal {
            hash: block_proposal.hash.into(),
            author: block_proposal.author as u64,
            ..Default::default()
        }
    }
}

impl TryFrom<chain_proto::BareState> for BareState {
    type Error = String;

    fn try_from(proto: chain_proto::BareState) -> Result<Self, Self::Error> {
        let endorses = proto_to_type(proto.endorses)?;
        Ok(BareState {
            primary_confidence: proto.primary_confidence,
            endorses,
            secondary_confidence: proto.secondary_confidence,
        })
    }
}

impl From<BareState> for chain_proto::BareState {
    fn from(state: BareState) -> chain_proto::BareState {
        chain_proto::BareState {
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
    pub fn empty() -> Self {
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

impl TryFrom<chain_proto::Proof> for Proof {
    type Error = String;

    fn try_from(proto: chain_proto::Proof) -> Result<Self, Self::Error> {
        let signature = Base58Encoded::from_base58(&proto.signature);
        match (proto_to_type(proto.bare_state), signature) {
            (Ok(bare_state), Ok(signature)) => Ok(Proof {
                bare_state,
                signature: GroupSignature { authority_mask: proto.mask, signature },
            }),
            _ => Err(PROTO_ERROR.to_string()),
        }
    }
}

impl From<Proof> for chain_proto::Proof {
    fn from(proof: Proof) -> Self {
        Self {
            bare_state: SingularPtrField::some(proof.bare_state.into()),
            mask: proof.signature.authority_mask,
            signature: proof.signature.signature.to_base58(),
            ..Default::default()
        }
    }
}

impl Proof {
    pub fn new(bare_state: BareState, signature: GroupSignature) -> Self {
        Self { bare_state, signature }
    }

    pub fn verify(
        &self,
        public_keys: &[BlsPublicKey],
        weights: &[usize],
    ) -> Result<(), NSVerifyErr> {
        let mut mask = self.signature.authority_mask.clone();
        mask.resize(weights.len(), false);
        // Verify that this proof contains enough signature in order to be accepted as valid.
        let current_weight: usize = mask
            .iter()
            .zip(weights)
            .filter_map(|(bit, weight)| if *bit { Some(weight) } else { None })
            .sum();

        let total_weight: usize = weights.iter().sum();

        if current_weight <= total_weight * 2 / 3 {
            return Err(NSVerifyErr::MissingSignatures);
        }

        if self.signature.verify(public_keys, &self.bare_state.bs_encode()) {
            Ok(())
        } else {
            Err(NSVerifyErr::InvalidProof)
        }
    }
}

impl Default for Proof {
    fn default() -> Self {
        Proof { bare_state: BareState::empty(), signature: GroupSignature::default() }
    }
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

const COMMIT_THRESHOLD: i64 = 3;

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
    pub fn can_commit(&self) -> bool {
        self.bare_state.primary_confidence
            >= self.bare_state.secondary_confidence + COMMIT_THRESHOLD
    }

    /// BlockHeader (Authority and Block) that this state is endorsing.
    pub fn endorses(&self) -> BlockProposal {
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
