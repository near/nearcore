use std::collections::HashMap;
use std::fmt::Debug;

use crate::sharding::ReceiptProof;
use crate::state::PartialState;
use crate::transaction::SignedTransaction;
use crate::types::{ChunkExecutionResultHash, SpiceChunkId};
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;

/// The state witness for a chunk with spice; proves the state transition that the
/// chunk attests to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum SpiceChunkStateWitness {
    V1(SpiceChunkStateWitnessV1) = 0,
}

/// Represents the base state and the expected post-state-root of a chunk's state
/// transition for spice. The actual state transition itself is not included here.
#[derive(
    Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
pub struct SpiceChunkStateTransition {
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

/// There are following differences with ChunkStateWitnessV2
/// - removed chunk_header, epoch_id, new_transactions,
/// - added chunk_id, execution_result_hash,
/// - changed source_receipt_proofs key from chunk hash to shard id and adjusted comment for spice,
/// - renamed implicit_transitions to resharding_transitions and adjusted comment.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct SpiceChunkStateWitnessV1 {
    /// Witness contains information to derive execution results of chunk corresponding to
    /// chunk_id.
    pub chunk_id: SpiceChunkId,
    /// The base state and post-state-root of the main transition where we
    /// apply transactions and receipts. Corresponds to the state transition
    /// that takes us from the pre-state-root of the chunk of this shard to
    /// the post-state-root of that same chunk.
    pub main_state_transition: SpiceChunkStateTransition,
    /// For the main state transition, we apply transactions and receipts.
    /// Exactly which of them must be applied is a deterministic property
    /// based on the blockchain history this chunk is based on.
    ///
    /// The set of receipts is exactly
    ///   Filter(R, |receipt| receipt.target_shard = S), where
    ///     - R is the set of outgoing receipts included in the set of chunks of
    ///       previous block.
    ///     - S is the shard of this chunk.
    ///
    /// This field, `source_receipt_proofs`, is a (non-strict) superset of the
    /// receipts that must be applied, along with information that allows these
    /// receipts to be verifiable against the blockchain history.
    pub source_receipt_proofs: HashMap<ShardId, ReceiptProof>,
    /// An overall hash of the list of receipts that should be applied. This is
    /// redundant information but is useful for diagnosing why a witness might
    /// fail. This is the hash of the borsh encoding of the Vec<Receipt> in the
    /// order that they should be applied.
    pub applied_receipts_hash: CryptoHash,
    /// The transactions to apply. These must be in the correct order in which
    /// they are to be applied.
    pub transactions: Vec<SignedTransaction>,
    /// Hash of the execution results after executing the chunk. This is redundant
    /// since it can be calculated by applying the chunk, but it's still useful
    /// for debugging.
    pub execution_result_hash: ChunkExecutionResultHash,
}

impl SpiceChunkStateWitness {
    pub fn new(
        chunk_id: SpiceChunkId,
        main_state_transition: SpiceChunkStateTransition,
        source_receipt_proofs: HashMap<ShardId, ReceiptProof>,
        applied_receipts_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        execution_result_hash: ChunkExecutionResultHash,
    ) -> Self {
        Self::V1(SpiceChunkStateWitnessV1 {
            chunk_id,
            main_state_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            execution_result_hash,
        })
    }

    pub fn chunk_id(&self) -> &SpiceChunkId {
        match self {
            Self::V1(witness) => &witness.chunk_id,
        }
    }

    pub fn main_state_transition(&self) -> &SpiceChunkStateTransition {
        match self {
            Self::V1(witness) => &witness.main_state_transition,
        }
    }

    pub fn source_receipt_proofs(&self) -> &HashMap<ShardId, ReceiptProof> {
        match self {
            Self::V1(witness) => &witness.source_receipt_proofs,
        }
    }

    pub fn applied_receipts_hash(&self) -> &CryptoHash {
        match self {
            Self::V1(witness) => &witness.applied_receipts_hash,
        }
    }

    pub fn transactions(&self) -> &[SignedTransaction] {
        match self {
            Self::V1(witness) => &witness.transactions,
        }
    }

    pub fn execution_result_hash(&self) -> &ChunkExecutionResultHash {
        match self {
            Self::V1(witness) => &witness.execution_result_hash,
        }
    }
}
