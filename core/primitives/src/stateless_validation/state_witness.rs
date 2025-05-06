use std::fmt::Debug;
use {crate::state::PartialState, std::collections::HashMap};

use super::ChunkProductionKey;
#[cfg(feature = "solomon")]
use crate::reed_solomon::{ReedSolomonEncoderDeserialize, ReedSolomonEncoderSerialize};
use crate::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader};
use crate::transaction::SignedTransaction;
use crate::types::{EpochId, SignatureDifferentiator};
use crate::utils::compression::CompressedData;
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, BlockHeight, ProtocolVersion, ShardId};
use near_primitives_core::version::{PROTOCOL_VERSION, ProtocolFeature};
use near_schema_checker_lib::ProtocolSchema;

/// Represents max allowed size of the raw (not compressed) state witness,
/// corresponds to the size of borsh-serialized ChunkStateWitness.
pub const MAX_UNCOMPRESSED_STATE_WITNESS_SIZE: u64 =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 64 }).0;
pub const STATE_WITNESS_COMPRESSION_LEVEL: i32 = 3;

/// Represents bytes of encoded ChunkStateWitness.
/// This is the compressed version of borsh-serialized state witness.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    BorshSerialize,
    BorshDeserialize,
    ProtocolSchema,
    derive_more::From,
    derive_more::AsRef,
)]
pub struct EncodedChunkStateWitness(Box<[u8]>);

impl
    CompressedData<
        ChunkStateWitness,
        MAX_UNCOMPRESSED_STATE_WITNESS_SIZE,
        STATE_WITNESS_COMPRESSION_LEVEL,
    > for EncodedChunkStateWitness
{
}

impl
    CompressedData<
        ChunkStateWitnessV1,
        MAX_UNCOMPRESSED_STATE_WITNESS_SIZE,
        STATE_WITNESS_COMPRESSION_LEVEL,
    > for EncodedChunkStateWitness
{
}

#[cfg(feature = "solomon")]
impl ReedSolomonEncoderSerialize for EncodedChunkStateWitness {
    fn serialize_single_part(&self) -> std::io::Result<Vec<u8>> {
        Ok(self.0.to_vec())
    }
}

#[cfg(feature = "solomon")]
impl ReedSolomonEncoderDeserialize for EncodedChunkStateWitness {
    fn deserialize_single_part(data: &[u8]) -> std::io::Result<Self> {
        Ok(EncodedChunkStateWitness(data.to_vec().into_boxed_slice()))
    }
}

impl EncodedChunkStateWitness {
    pub fn size_bytes(&self) -> usize {
        self.0.len()
    }
}

pub type ChunkStateWitnessSize = usize;

/// An acknowledgement sent from the chunk producer upon receiving the state witness to
/// the originator of the witness (chunk producer).
///
/// This message is currently used for computing
/// the network round-trip time of sending the state witness to the chunk producer and receiving the
/// endorsement message. Note that the endorsement message is sent to the next block producer,
/// while this message is sent back to the originator of the state witness, though this allows
/// us to approximate the time for transmitting the state witness + transmitting the endorsement.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkStateWitnessAck {
    /// Hash of the chunk for which the state witness was generated.
    pub chunk_hash: ChunkHash,
}

impl ChunkStateWitnessAck {
    pub fn new(witness: &ChunkStateWitness) -> Self {
        Self { chunk_hash: witness.chunk_header().chunk_hash() }
    }
}

/// The state witness for a chunk; proves the state transition that the
/// chunk attests to.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub enum ChunkStateWitness {
    V1(ChunkStateWitnessV1),
    V2(ChunkStateWitnessV2),
}

#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkStateWitnessV1 {
    pub chunk_producer: AccountId,
    pub epoch_id: EpochId,
    pub chunk_header: ShardChunkHeader,
    pub main_state_transition: ChunkStateTransition,
    pub source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
    pub applied_receipts_hash: CryptoHash,
    pub transactions: Vec<SignedTransaction>,
    pub implicit_transitions: Vec<ChunkStateTransition>,
    pub _deprecated_new_transactions: Vec<SignedTransaction>,
    pub _deprecated_new_transactions_validation_state: PartialState,
    signature_differentiator: SignatureDifferentiator,
}

/// From V1 -> V2 we have the following changes:
/// - The `chunk_producer`, `new_transactions`, `new_transactions_validation_state`,
///   and `signature_differentiator` fields are removed.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkStateWitnessV2 {
    /// EpochId corresponds to the next block after chunk's previous block.
    /// This is effectively the output of EpochManager::get_epoch_id_from_prev_block
    /// with chunk_header.prev_block_hash().
    pub epoch_id: EpochId,
    /// The chunk header that this witness is for. While this is not needed
    /// to apply the state transition, it is needed for a chunk validator to
    /// produce a chunk endorsement while knowing what they are endorsing.
    // TODO(stateless_validation): Deprecate this field in the next version of the state witness.
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
        protocol_version: ProtocolVersion,
    ) -> Self {
        if ProtocolFeature::VersionedStateWitness.enabled(protocol_version) {
            return Self::V2(ChunkStateWitnessV2 {
                epoch_id,
                chunk_header,
                main_state_transition,
                source_receipt_proofs,
                applied_receipts_hash,
                transactions,
                implicit_transitions,
            });
        }

        Self::V1(ChunkStateWitnessV1 {
            chunk_producer,
            epoch_id,
            chunk_header,
            main_state_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            implicit_transitions,
            signature_differentiator: "ChunkStateWitness".to_string(),
            _deprecated_new_transactions: vec![],
            _deprecated_new_transactions_validation_state: PartialState::default(),
        })
    }

    /// Used for testing.
    pub fn new_dummy(height: BlockHeight, shard_id: ShardId, prev_block_hash: CryptoHash) -> Self {
        let header = ShardChunkHeader::new_dummy(height, shard_id, prev_block_hash);
        Self::new(
            "alice.near".parse().unwrap(),
            EpochId::default(),
            header,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            PROTOCOL_VERSION,
        )
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        match self {
            ChunkStateWitness::V1(witness) => witness.chunk_production_key(),
            ChunkStateWitness::V2(witness) => witness.chunk_production_key(),
        }
    }

    pub fn chunk_header(&self) -> &ShardChunkHeader {
        match self {
            ChunkStateWitness::V1(witness) => &witness.chunk_header,
            ChunkStateWitness::V2(witness) => &witness.chunk_header,
        }
    }

    pub fn main_state_transition(&self) -> &ChunkStateTransition {
        match self {
            ChunkStateWitness::V1(witness) => &witness.main_state_transition,
            ChunkStateWitness::V2(witness) => &witness.main_state_transition,
        }
    }

    pub fn mut_main_state_transition(&mut self) -> &mut ChunkStateTransition {
        match self {
            ChunkStateWitness::V1(witness) => &mut witness.main_state_transition,
            ChunkStateWitness::V2(witness) => &mut witness.main_state_transition,
        }
    }

    pub fn source_receipt_proofs(&self) -> &HashMap<ChunkHash, ReceiptProof> {
        match self {
            ChunkStateWitness::V1(witness) => &witness.source_receipt_proofs,
            ChunkStateWitness::V2(witness) => &witness.source_receipt_proofs,
        }
    }

    pub fn applied_receipts_hash(&self) -> &CryptoHash {
        match self {
            ChunkStateWitness::V1(witness) => &witness.applied_receipts_hash,
            ChunkStateWitness::V2(witness) => &witness.applied_receipts_hash,
        }
    }

    pub fn transactions(&self) -> &Vec<SignedTransaction> {
        match self {
            ChunkStateWitness::V1(witness) => &witness.transactions,
            ChunkStateWitness::V2(witness) => &witness.transactions,
        }
    }

    pub fn implicit_transitions(&self) -> &Vec<ChunkStateTransition> {
        match self {
            ChunkStateWitness::V1(witness) => &witness.implicit_transitions,
            ChunkStateWitness::V2(witness) => &witness.implicit_transitions,
        }
    }
}

impl ChunkStateWitnessV1 {
    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.chunk_header.shard_id(),
            epoch_id: self.epoch_id,
            height_created: self.chunk_header.height_created(),
        }
    }
}

impl ChunkStateWitnessV2 {
    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.chunk_header.shard_id(),
            epoch_id: self.epoch_id,
            height_created: self.chunk_header.height_created(),
        }
    }
}

/// Represents the base state and the expected post-state-root of a chunk's state
/// transition. The actual state transition itself is not included here.
#[derive(
    Debug, Default, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema,
)]
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
