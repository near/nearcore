use std::collections::HashMap;
use std::fmt::Debug;

use super::ChunkProductionKey;
use crate::block::ApplyChunkBlockContext;
#[cfg(feature = "solomon")]
use crate::reed_solomon::{ReedSolomonEncoderDeserialize, ReedSolomonEncoderSerialize};
use crate::state::PartialState;
use crate::stateless_validation::{WitnessProductionKey, WitnessType};
use crate::transaction::SignedTransaction;
use crate::types::EpochId;
use crate::utils::compression::CompressedData;
use crate::{
    receipt::Receipt,
    sharding::{ChunkHash, ReceiptProof, ShardChunkHeader},
};
use borsh::{BorshDeserialize, BorshSerialize};
use bytesize::ByteSize;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{BlockHeight, ShardId};
use near_schema_checker_lib::ProtocolSchema;

/// Represents max allowed size of the raw (not compressed) state witness,
/// corresponds to the size of borsh-serialized ChunkStateWitness.
pub const MAX_UNCOMPRESSED_STATE_WITNESS_SIZE: u64 =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 64 }).0;
pub const STATE_WITNESS_COMPRESSION_LEVEL: i32 = 1;
pub const STATE_WITNESS_COMPRESSION_NUM_WORKERS: u32 = 4;

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
        STATE_WITNESS_COMPRESSION_NUM_WORKERS,
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
        Self { chunk_hash: witness.latest_chunk_header().chunk_hash().clone() }
    }
}

/// The state witness for a chunk; proves the state transition that the
/// chunk attests to.
// todo(slavas): remove the clippy warning stub once the data duplication between the witness parts
// is removed
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum ChunkStateWitness {
    V1 = 0, // Deprecated
    V2(Box<ChunkStateWitnessV2>) = 1,
    V3(ChunkStateWitnessV3) = 2,
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
    pub new_transactions: Vec<SignedTransaction>,
}

/// The part of the witness that the validator needs to receive in order to apply the chunk.
/// Does not include the data needed for actual validation and production of endorsements.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkApplyWitness {
    /// EpochId corresponds to the next block after chunk's previous block.
    /// This is effectively the output of EpochManager::get_epoch_id_from_prev_block
    /// with chunk_header.prev_block_hash().
    pub epoch_id: EpochId,

    /// header of the chunk being applied, same height.
    pub chunk_header: ShardChunkHeader, // ShardChunkApplyHeader,

    /// Context of the block being applied.
    /// Needed to avoid chain store access.
    pub block_context: ApplyChunkBlockContext,
    pub chunks: Vec<ShardChunkHeader>,
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
    pub receipts: Vec<Receipt>,
    /// An overall hash of the list of receipts that should be applied. This is
    /// redundant information but is useful for diagnosing why a witness might
    /// fail. This is the hash of the borsh encoding of the Vec<Receipt> in the
    /// order that they should be applied.
    pub applied_receipts_hash: CryptoHash,
    /// The transactions to apply. These must be in the correct order in which
    /// they are to be applied.
    pub transactions: Vec<SignedTransaction>,
}

/// doc me
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkValidateWitness {
    pub epoch_id: EpochId,
    /// The chunk header that this witness is for. While this is not needed
    /// to apply the state transition, it is needed for a chunk validator to
    /// produce a chunk endorsement while knowing what they are endorsing.
    // TODO(stateless_validation): Deprecate this field in the next version of the state witness.
    pub chunk_header: ShardChunkHeader,
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
    /// todo: move to apply witness?
    pub source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
    /// todo: remove
    pub applied_receipts_hash: CryptoHash,
    /// For each missing chunk after the last new chunk of the shard, we need
    /// to carry out an implicit state transition. Mostly, this is for
    /// distributing validator rewards. This list contains one for each such
    /// chunk, in forward chronological order.
    ///
    /// After these are applied as well, we should arrive at the pre-state-root
    /// of the chunk that this witness is for.
    pub implicit_transitions: Vec<ChunkStateTransition>,
    pub new_transactions: Vec<SignedTransaction>,
}

/// doc me
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize, ProtocolSchema)]
pub struct ChunkStateWitnessV3 {
    pub chunk_apply_witness: Option<ChunkApplyWitness>,
    pub chunk_validate_witness: Option<ChunkValidateWitness>,
}

impl ChunkStateWitness {
    pub fn new(
        epoch_id: EpochId,
        chunk_header: ShardChunkHeader,
        main_state_transition: ChunkStateTransition,
        source_receipt_proofs: HashMap<ChunkHash, ReceiptProof>,
        applied_receipts_hash: CryptoHash,
        transactions: Vec<SignedTransaction>,
        implicit_transitions: Vec<ChunkStateTransition>,
        new_transactions: Vec<SignedTransaction>,
    ) -> Self {
        Self::V2(Box::new(ChunkStateWitnessV2 {
            epoch_id,
            chunk_header,
            main_state_transition,
            source_receipt_proofs,
            applied_receipts_hash,
            transactions,
            implicit_transitions,
            new_transactions,
        }))
    }

    /// Used for testing.
    pub fn new_dummy(height: BlockHeight, shard_id: ShardId, prev_block_hash: CryptoHash) -> Self {
        let header = ShardChunkHeader::new_dummy(height, shard_id, prev_block_hash);
        Self::new(
            EpochId::default(),
            header,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        )
    }

    pub fn production_key(&self) -> WitnessProductionKey {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => witness.production_key(),
            ChunkStateWitness::V3(witness) => witness.production_key(),
        }
    }

    pub fn epoch_id(&self) -> &EpochId {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &witness.epoch_id,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_apply_witness) = &witness.chunk_apply_witness {
                    &chunk_apply_witness.epoch_id
                } else if let Some(chunk_validate_witness) = &witness.chunk_validate_witness {
                    &chunk_validate_witness.epoch_id
                } else {
                    panic!("ChunkApplyWitness and ChunkValidateWitness are not given");
                }
            }
        }
    }

    // This is chunk header to be signed. Not always available.
    pub fn chunk_header(&self) -> &ShardChunkHeader {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &witness.chunk_header,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_validate_witness) = &witness.chunk_validate_witness {
                    &chunk_validate_witness.chunk_header
                } else {
                    panic!("ChunkValidateWitness is not given");
                }
            }
        }
    }

    // This is latest available chunk header in the witness.
    // Used for orphan detection, logging.
    // Consider using production key instead.
    // But then we lose navigation by chunk hash.
    pub fn latest_chunk_header(&self) -> &ShardChunkHeader {
        if let ChunkStateWitness::V3(witness) = self {
            return if let Some(chunk_validate_witness) = &witness.chunk_validate_witness {
                &chunk_validate_witness.chunk_header
            } else if let Some(chunk_apply_witness) = &witness.chunk_apply_witness {
                &chunk_apply_witness.chunk_header
            } else {
                panic!("ChunkValidateWitness and ChunkApplyWitness are not given");
            };
        }

        self.chunk_header()
    }

    pub fn main_state_transition(&self) -> &ChunkStateTransition {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &witness.main_state_transition,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_apply_witness) = &witness.chunk_apply_witness {
                    &chunk_apply_witness.main_state_transition
                } else {
                    panic!("ChunkApplyWitness is not given");
                }
            }
        }
    }

    pub fn mut_main_state_transition(&mut self) -> &mut ChunkStateTransition {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &mut witness.main_state_transition,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_apply_witness) = &mut witness.chunk_apply_witness {
                    &mut chunk_apply_witness.main_state_transition
                } else {
                    panic!("ChunkApplyWitness is not given");
                }
            }
        }
    }

    pub fn source_receipt_proofs(&self) -> &HashMap<ChunkHash, ReceiptProof> {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &witness.source_receipt_proofs,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_validate_witness) = &witness.chunk_validate_witness {
                    &chunk_validate_witness.source_receipt_proofs
                } else {
                    panic!("ChunkValidateWitness is not given");
                }
            }
        }
    }

    pub fn applied_receipts_hash(&self) -> &CryptoHash {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &witness.applied_receipts_hash,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_validate_witness) = &witness.chunk_validate_witness {
                    &chunk_validate_witness.applied_receipts_hash
                } else if let Some(chunk_apply_witness) = &witness.chunk_apply_witness {
                    &chunk_apply_witness.applied_receipts_hash
                } else {
                    panic!("ChunkValidateWitness and ChunkApplyWitness are not given");
                }
            }
        }
    }

    pub fn transactions(&self) -> &Vec<SignedTransaction> {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &witness.transactions,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_apply_witness) = &witness.chunk_apply_witness {
                    &chunk_apply_witness.transactions
                } else {
                    panic!("ChunkApplyWitness is not given");
                }
            }
        }
    }

    pub fn implicit_transitions(&self) -> &Vec<ChunkStateTransition> {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => &witness.implicit_transitions,
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_validate_witness) = &witness.chunk_validate_witness {
                    &chunk_validate_witness.implicit_transitions
                } else {
                    panic!("ChunkValidateWitness is not given");
                }
            }
        }
    }

    pub fn new_transactions(&self) -> Option<&Vec<SignedTransaction>> {
        match self {
            ChunkStateWitness::V1 => unreachable!("ChunkStateWitness V1 is deprecated"),
            ChunkStateWitness::V2(witness) => Some(&witness.new_transactions),
            ChunkStateWitness::V3(witness) => {
                if let Some(chunk_validate_witness) = &witness.chunk_validate_witness {
                    Some(&chunk_validate_witness.new_transactions)
                } else {
                    None
                }
            }
        }
    }

    pub fn block_context(&self) -> &ApplyChunkBlockContext {
        let ChunkStateWitness::V3(witness) = self else {
            panic!("ChunkStateWitness is not V3");
        };
        let Some(chunk_apply_witness) = &witness.chunk_apply_witness else {
            panic!("ChunkApplyWitness is not given");
        };
        &chunk_apply_witness.block_context
    }

    pub fn chunks(&self) -> &Vec<ShardChunkHeader> {
        let ChunkStateWitness::V3(witness) = self else {
            panic!("ChunkStateWitness is not V3");
        };
        let Some(chunk_apply_witness) = &witness.chunk_apply_witness else {
            panic!("ChunkApplyWitness is not given");
        };
        &chunk_apply_witness.chunks
    }

    pub fn raw_receipts(&self) -> &Vec<Receipt> {
        let ChunkStateWitness::V3(witness) = self else {
            panic!("ChunkStateWitness is not V3");
        };
        let Some(chunk_apply_witness) = &witness.chunk_apply_witness else {
            panic!("ChunkApplyWitness is not given");
        };
        &chunk_apply_witness.receipts
    }
}

impl ChunkStateWitnessV2 {
    pub fn production_key(&self) -> WitnessProductionKey {
        WitnessProductionKey {
            chunk: ChunkProductionKey {
                shard_id: self.chunk_header.shard_id(),
                epoch_id: self.epoch_id,
                height_created: self.chunk_header.height_created(),
            },
            witness_type: WitnessType::Full,
        }
    }
}

impl ChunkStateWitnessV3 {
    pub fn production_key(&self) -> WitnessProductionKey {
        if let Some(chunk_validate_witness) = &self.chunk_validate_witness {
            // If validate witness is provided, we can perform full validation
            // and have unique identifier.
            let witness_type = if let Some(chunk_apply_witness) = &self.chunk_apply_witness {
                assert!(
                    chunk_apply_witness.chunk_header.height_created() + 1
                        == chunk_validate_witness.chunk_header.height_created()
                );
                WitnessType::Full
            } else {
                WitnessType::Validate
            };
            WitnessProductionKey {
                chunk: ChunkProductionKey {
                    shard_id: chunk_validate_witness.chunk_header.shard_id(),
                    epoch_id: chunk_validate_witness.epoch_id,
                    height_created: chunk_validate_witness.chunk_header.height_created(),
                },
                witness_type,
            }
        } else if let Some(chunk_apply_witness) = &self.chunk_apply_witness {
            // Otherwise we just do optimistic execution and speculate on chunk
            // production key!
            WitnessProductionKey {
                chunk: ChunkProductionKey {
                    shard_id: chunk_apply_witness.chunk_header.shard_id(),
                    epoch_id: chunk_apply_witness.epoch_id,
                    // speculative! the witness will verify the NEXT chunk!
                    // if there is a fork, it won't work, but optimism
                    // doesn't work too.
                    height_created: chunk_apply_witness.chunk_header.height_created() + 1,
                },
                witness_type: WitnessType::Optimistic,
            }
        } else {
            panic!("ChunkValidateWitness and ChunkApplyWitness are not given");
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
