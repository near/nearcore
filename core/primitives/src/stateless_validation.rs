use std::collections::{HashMap, HashSet};

use crate::challenge::PartialState;
use crate::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderV3};
use crate::transaction::SignedTransaction;
use crate::types::EpochId;
use crate::validator_signer::{EmptyValidatorSigner, ValidatorSigner};
use borsh::{BorshDeserialize, BorshSerialize};
use near_crypto::{PublicKey, Signature};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, Balance, BlockHeight, ShardId};

/// An arbitrary static string to make sure that this struct cannot be
/// serialized to look identical to another serialized struct. For chunk
/// production we are signing a chunk hash, so we need to make sure that
/// this signature means something different.
///
/// This is a messy workaround until we know what to do with NEP 483.
type SignatureDifferentiator = String;

/// Represents bytes of encoded ChunkStateWitness.
/// For now encoding is raw borsh serialization, later we plan
/// adding compression on top of that.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EncodedChunkStateWitness(Box<[u8]>);

impl EncodedChunkStateWitness {
    pub fn encode(witness: &ChunkStateWitness) -> Self {
        Self(borsh::to_vec(witness).expect("borsh serialization should not fail").into())
    }

    pub fn decode(&self) -> Result<ChunkStateWitness, std::io::Error> {
        ChunkStateWitness::try_from_slice(&self.0)
    }

    pub fn size_bytes(&self) -> usize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct SignedEncodedChunkStateWitness {
    /// The content of the witness. It is convenient have it as bytes in order
    /// to perform signature verification along with decoding.
    pub witness_bytes: EncodedChunkStateWitness,
    /// Signature corresponds to `witness_bytes.as_slice()` signed by the chunk producer
    pub signature: Signature,
}

impl SignedEncodedChunkStateWitness {
    pub fn new(witness: &ChunkStateWitness, signer: &dyn ValidatorSigner) -> Self {
        let witness_bytes = EncodedChunkStateWitness::encode(witness);
        let signature = signer.sign_chunk_state_witness(&witness_bytes);
        Self { witness_bytes, signature }
    }
}

/// An acknowledgement sent from the chunk producer upon receiving the state witness to
/// the originator of the witness (chunk producer).
///
/// This message is currently used for computing
/// the network round-trip time of sending the state witness to the chunk producer and receiving the
/// endorsement message. Note that the endorsement message is sent to the next block producer,
/// while this message is sent back to the originator of the state witness, though this allows
/// us to approximate the time for transmitting the state witness + transmitting the endorsement.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkStateWitnessAck {
    /// Hash of the chunk for which the state witness was generated.
    pub chunk_hash: ChunkHash,
}

impl ChunkStateWitnessAck {
    pub fn new(witness: &ChunkStateWitness) -> Self {
        Self { chunk_hash: witness.chunk_header.chunk_hash() }
    }
}

/// The state witness for a chunk; proves the state transition that the
/// chunk attests to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkStateWitness {
    pub chunk_producer: AccountId,
    /// EpochId corresponds to the next block after chunk's previous block.
    /// This is effectively the output of EpochManager::get_epoch_id_from_prev_block
    /// with chunk_header.prev_block_hash().
    /// This is needed to validate signature when the previous block is not yet
    /// available on the validator side (aka orphan state witness).
    pub epoch_id: EpochId,
    /// The chunk header that this witness is for. While this is not needed
    /// to apply the state transition, it is needed for a chunk validator to
    /// produce a chunk endorsement while knowing what they are endorsing.
    pub chunk_header: ShardChunkHeader,
    /// The base state and post-state-root of the main transition where we
    /// apply transactions and receipts. Corresponds to the state transition
    /// that takes us from the pre-state-root of the last new chunk of this
    /// shard to the post-state-root of that same chunk.
    pub main_state_transition: ChunkStateTransition,
    /// For the main state transition, we apply transactions and receipts.
    /// Exactly which of them must be applied is a deterministic property
    /// based on the blockchain history this chunk is based on.
    ///
    /// The set of receipts is exactly
    ///   Filter(R, |receipt| receipt.target_shard = S), where
    ///     - R is the set of outgoing receipts included in the set of chunks C
    ///       (defined below),
    ///     - S is the shard of this chunk.
    ///
    /// The set of chunks C, from which the receipts are sourced, is defined as
    /// all new chunks included in the set of blocks B.
    ///
    /// The set of blocks B is defined as the contiguous subsequence of blocks
    /// B1 (EXCLUSIVE) to B2 (inclusive) in this chunk's chain (i.e. the linear
    /// chain that this chunk's parent block is on), where B2 is the block that
    /// contains the last new chunk of shard S before this chunk, and B1 is the
    /// block that contains the last new chunk of shard S before B2.
    ///
    /// Furthermore, the set of transactions to apply is exactly the
    /// transactions included in the chunk of shard S at B2.
    ///
    /// For the purpose of this text, a "new chunk" is defined as a chunk that
    /// is proposed by a chunk producer, not one that was copied from the
    /// previous block (commonly called a "missing chunk").
    ///
    /// This field, `source_receipt_proofs`, is a (non-strict) superset of the
    /// receipts that must be applied, along with information that allows these
    /// receipts to be verifiable against the blockchain history.
    pub source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
    /// An overall hash of the list of receipts that should be applied. This is
    /// redundant information but is useful for diagnosing why a witness might
    /// fail. This is the hash of the borsh encoding of the Vec<Receipt> in the
    /// order that they should be applied.
    pub applied_receipts_hash: CryptoHash,
    /// The transactions to apply. These must be in the correct order in which
    /// they are to be applied.
    pub transactions: Vec<SignedTransaction>,
    /// For each missing chunk after the last new chunk of the shard, we need
    /// to carry out an implicit state transition. Mostly, this is for
    /// distributing validator rewards. This list contains one for each such
    /// chunk, in forward chronological order.
    ///
    /// After these are applied as well, we should arrive at the pre-state-root
    /// of the chunk that this witness is for.
    pub implicit_transitions: Vec<ChunkStateTransition>,
    /// Finally, we need to be able to verify that the new transitions proposed
    /// by the chunk (that this witness is for) are valid. For that, we need
    /// the transactions as well as another partial storage (based on the
    /// pre-state-root of this chunk) in order to verify that the sender
    /// accounts have appropriate balances, access keys, nonces, etc.
    pub new_transactions: Vec<SignedTransaction>,
    pub new_transactions_validation_state: PartialState,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkStateWitness {
    pub fn new(
        chunk_producer: AccountId,
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        main_state_transition: ChunkStateTransition,
        source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
        applied_receipts_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        implicit_transitions: Vec<ChunkStateTransition>,
        new_transactions: Vec<SignedTransaction>,
        new_transactions_validation_state: PartialState,
    ) -> Self {
        Self {
            chunk_producer,
            epoch_id,
            chunk_header,
            main_state_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            implicit_transitions,
            new_transactions,
            new_transactions_validation_state,
            signature_differentiator: "ChunkStateWitness".to_owned(),
        }
    }

    pub fn new_dummy(height: BlockHeight, shard_id: ShardId, prev_block_hash: CryptoHash) -> Self {
        let header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
            prev_block_hash,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            height,
            shard_id,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            &EmptyValidatorSigner::default(),
        ));
        Self::new(
            "alice.near".parse().unwrap(),
            EpochId::default(),
            header,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
    }
}

/// Represents the base state and the expected post-state-root of a chunk's state
/// transition. The actual state transition itself is not included here.
#[derive(Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkStateTransition {
    /// The block that contains the chunk; this identifies which part of the
    /// state transition we're talking about.
    pub block_hash: CryptoHash,
    /// The partial state before the state transition. This includes whatever
    /// initial state that is necessary to compute the state transition for this
    /// chunk.
    pub base_state: PartialState,
    /// The expected final state root after applying the state transition.
    /// This is redundant information, because the post state root can be
    /// derived by applying the state transition onto the base state, but
    /// this makes it easier to debug why a state witness may fail to validate.
    pub post_state_root: CryptoHash,
}

/// The endorsement of a chunk by a chunk validator. By providing this, a
/// chunk validator has verified that the chunk state witness is correct.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsement {
    inner: ChunkEndorsementInner,
    pub account_id: AccountId,
    pub signature: Signature,
}

impl ChunkEndorsement {
    pub fn new(chunk_hash: ChunkHash, signer: &dyn ValidatorSigner) -> ChunkEndorsement {
        let inner = ChunkEndorsementInner::new(chunk_hash);
        let account_id = signer.validator_id().clone();
        let signature = signer.sign_chunk_endorsement(&inner);
        Self { inner, account_id, signature }
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
        let data = borsh::to_vec(&self.inner).unwrap();
        self.signature.verify(&data, public_key)
    }

    pub fn chunk_hash(&self) -> &ChunkHash {
        &self.inner.chunk_hash
    }

    pub fn validate_signature(
        chunk_hash: ChunkHash,
        signature: &Signature,
        public_key: &PublicKey,
    ) -> bool {
        let inner = ChunkEndorsementInner::new(chunk_hash);
        let data = borsh::to_vec(&inner).unwrap();
        signature.verify(&data, public_key)
    }
}

/// This is the part of the chunk endorsement that is actually being signed.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct ChunkEndorsementInner {
    chunk_hash: ChunkHash,
    signature_differentiator: SignatureDifferentiator,
}

impl ChunkEndorsementInner {
    fn new(chunk_hash: ChunkHash) -> Self {
        Self { chunk_hash, signature_differentiator: "ChunkEndorsement".to_owned() }
    }
}

/// Stored on disk for each chunk, including missing chunks, in order to
/// produce a chunk state witness when needed.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct StoredChunkStateTransitionData {
    /// The partial state that is needed to apply the state transition,
    /// whether it is a new chunk state transition or a implicit missing chunk
    /// state transition.
    pub base_state: PartialState,
    /// If this is a new chunk state transition, the hash of the receipts that
    /// were used to apply the state transition. This is redundant information,
    /// but is used to validate against `StateChunkWitness::exact_receipts_hash`
    /// to ease debugging of why a state witness may be incorrect.
    pub receipts_hash: CryptoHash,
}

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
