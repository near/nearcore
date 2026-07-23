use crate::sharding::{EncodedShardChunkBody, ReceiptProof};
use crate::state::PartialState;
use crate::stateless_validation::contract_distribution::{CodeBytes, CodeHash};
use crate::transaction::SignedTransaction;
use crate::types::SpiceChunkId;
use borsh::{BorshDeserialize, BorshSerialize};
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::ShardId;
use near_schema_checker_lib::ProtocolSchema;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;

/// Computes a deterministic hash of a set of contract code hashes.
/// The hashes are sorted lexicographically, concatenated, and hashed.
pub fn compute_contract_accesses_hash(accesses: &HashSet<CodeHash>) -> CryptoHash {
    let mut sorted: Vec<_> = accesses.iter().collect();
    sorted.sort();
    let mut buf = Vec::with_capacity(sorted.len() * CryptoHash::LENGTH);
    for h in &sorted {
        buf.extend_from_slice(h.0.as_bytes());
    }
    CryptoHash::hash_bytes(&buf)
}

/// The state witness for a chunk with spice; proves the state transition that the
/// chunk attests to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum SpiceChunkStateWitness {
    V1(SpiceChunkStateWitnessV1) = 0,
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
    /// Recorded partial state before the chunk's main state transition.
    pub pre_state: PartialState,
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
    /// Hash of the borsh-encoded receipts to apply, in application order.
    pub applied_receipts_hash: CryptoHash,
    /// The transactions to apply. These must be in the correct order in which
    /// they are to be applied.
    pub transactions: Vec<SignedTransaction>,
    /// Code hashes of the contracts accessed during chunk application. Validators
    /// check the contract accesses message against this and fetch missing code.
    pub contract_accesses: Vec<CodeHash>,
    /// Proof that the chunk body is invalid (e.g. bad tx_root). Present when
    /// a malicious chunk producer sends an invalid chunk body. The body must
    /// be RS-reconstructed (all parts filled) so validators can independently
    /// verify the invalidity. When present, validators accept empty
    /// transactions instead of the chunk header's tx_root.
    pub proof_of_invalid_chunk: Option<Box<EncodedShardChunkBody>>,
}

impl SpiceChunkStateWitness {
    pub fn new(
        chunk_id: SpiceChunkId,
        pre_state: PartialState,
        source_receipt_proofs: HashMap<ShardId, ReceiptProof>,
        applied_receipts_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        contract_accesses: Vec<CodeHash>,
        proof_of_invalid_chunk: Option<Box<EncodedShardChunkBody>>,
    ) -> Self {
        Self::V1(SpiceChunkStateWitnessV1 {
            chunk_id,
            pre_state,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            contract_accesses,
            proof_of_invalid_chunk,
        })
    }

    pub fn chunk_id(&self) -> &SpiceChunkId {
        match self {
            Self::V1(witness) => &witness.chunk_id,
        }
    }

    pub fn pre_state(&self) -> &PartialState {
        match self {
            Self::V1(witness) => &witness.pre_state,
        }
    }

    /// Merge contract bytes into the recorded pre-state's trie values.
    pub fn merge_contracts(&mut self, contracts: Vec<CodeBytes>) {
        match self {
            Self::V1(witness) => {
                let PartialState::TrieValues(values) = &mut witness.pre_state;
                values.extend(contracts.into_iter().map(|code| code.0));
            }
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

    pub fn contract_accesses(&self) -> &[CodeHash] {
        match self {
            Self::V1(witness) => &witness.contract_accesses,
        }
    }

    pub fn proof_of_invalid_chunk(&self) -> Option<&EncodedShardChunkBody> {
        match self {
            Self::V1(witness) => witness.proof_of_invalid_chunk.as_deref(),
        }
    }
}
