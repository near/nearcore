use std::collections::HashMap;
use std::fmt::Debug;

use crate::challenge::PartialState;
use crate::congestion_info::CongestionInfo;
use crate::sharding::{ChunkHash, ReceiptProof, ShardChunkHeader, ShardChunkHeaderV3};
use crate::transaction::SignedTransaction;
use crate::types::EpochId;
use crate::utils::io::{CountingRead, CountingWrite};
use crate::validator_signer::EmptyValidatorSigner;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::{Buf, BufMut};
use bytesize::ByteSize;
use near_primitives_core::hash::CryptoHash;
use near_primitives_core::types::{AccountId, BlockHeight, ShardId};
use near_primitives_core::version::{ProtocolFeature, PROTOCOL_VERSION};

use super::{ChunkProductionKey, SignatureDifferentiator};

/// Represents max allowed size of the raw (not compressed) state witness,
/// corresponds to the size of borsh-serialized ChunkStateWitness.
pub const MAX_UNCOMPRESSED_STATE_WITNESS_SIZE: ByteSize =
    ByteSize::mib(if cfg!(feature = "test_features") { 512 } else { 64 });

/// Represents bytes of encoded ChunkStateWitness.
/// This is the compressed version of borsh-serialized state witness.
#[derive(Debug, Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct EncodedChunkStateWitness(Box<[u8]>);

pub type ChunkStateWitnessSize = usize;

impl EncodedChunkStateWitness {
    /// Only use this if you are sure that the data is already encoded.
    pub fn from_boxed_slice(data: Box<[u8]>) -> Self {
        Self(data)
    }

    /// Borsh-serialize and compress state witness.
    /// Returns encoded witness along with the raw (uncompressed) witness size.
    pub fn encode(witness: &ChunkStateWitness) -> std::io::Result<(Self, ChunkStateWitnessSize)> {
        const STATE_WITNESS_COMPRESSION_LEVEL: i32 = 3;

        // Flow of data: State witness --> Borsh serialization --> Counting write --> zstd compression --> Bytes.
        // CountingWrite will count the number of bytes for the Borsh-serialized witness, before compression.
        let mut counting_write = CountingWrite::new(zstd::stream::Encoder::new(
            Vec::new().writer(),
            STATE_WITNESS_COMPRESSION_LEVEL,
        )?);
        borsh::to_writer(&mut counting_write, witness)?;

        let borsh_bytes_len = counting_write.bytes_written();
        let encoded_bytes = counting_write.into_inner().finish()?.into_inner();

        Ok((Self(encoded_bytes.into()), borsh_bytes_len.as_u64() as usize))
    }

    /// Decompress and borsh-deserialize encoded witness bytes.
    /// Returns decoded witness along with the raw (uncompressed) witness size.
    pub fn decode(&self) -> std::io::Result<(ChunkStateWitness, ChunkStateWitnessSize)> {
        // We want to limit the size of decompressed data to address "Zip bomb" attack.
        self.decode_with_limit(MAX_UNCOMPRESSED_STATE_WITNESS_SIZE)
    }

    /// Decompress and borsh-deserialize encoded witness bytes.
    /// Returns decoded witness along with the raw (uncompressed) witness size.
    pub fn decode_with_limit(
        &self,
        limit: ByteSize,
    ) -> std::io::Result<(ChunkStateWitness, ChunkStateWitnessSize)> {
        // Flow of data: Bytes --> zstd decompression --> Counting read --> Borsh deserialization --> State witness.
        // CountingRead will count the number of bytes for the Borsh-deserialized witness, after decompression.
        let mut counting_read = CountingRead::new_with_limit(
            zstd::stream::Decoder::new(self.0.as_ref().reader())?,
            limit,
        );

        match borsh::from_reader(&mut counting_read) {
            Err(err) => {
                // If decompressed data exceeds the limit then CountingRead will return a WriteZero error.
                // Here we convert it to a more descriptive error to make debugging easier.
                let err = if err.kind() == std::io::ErrorKind::WriteZero {
                    std::io::Error::other(format!(
                        "Decompressed data exceeded limit of {limit}: {err}"
                    ))
                } else {
                    err
                };
                Err(err)
            }
            Ok(witness) => Ok((witness, counting_read.bytes_read().as_u64().try_into().unwrap())),
        }
    }

    pub fn size_bytes(&self) -> ChunkStateWitnessSize {
        self.0.len()
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
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
    /// TODO(stateless_validation): Deprecate once we send state witness in parts.
    pub chunk_producer: AccountId,
    /// EpochId corresponds to the next block after chunk's previous block.
    /// This is effectively the output of EpochManager::get_epoch_id_from_prev_block
    /// with chunk_header.prev_block_hash().
    /// This is needed to validate signature when the previous block is not yet
    /// available on the validator side (aka orphan state witness).
    /// TODO(stateless_validation): Deprecate once we send state witness in parts.
    pub epoch_id: EpochId,
    /// The chunk header that this witness is for. While this is not needed
    /// to apply the state transition, it is needed for a chunk validator to
    /// produce a chunk endorsement while knowing what they are endorsing.
    /// TODO(stateless_validation): Deprecate once we send state witness in parts.
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
    // TODO(stateless_validation): Deprecate once we send state witness in parts.
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

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.chunk_header.shard_id(),
            epoch_id: self.epoch_id,
            height_created: self.chunk_header.height_created(),
        }
    }

    pub fn new_dummy(height: BlockHeight, shard_id: ShardId, prev_block_hash: CryptoHash) -> Self {
        let congestion_info = ProtocolFeature::CongestionControl
            .enabled(PROTOCOL_VERSION)
            .then_some(CongestionInfo::default());

        let header = ShardChunkHeader::V3(ShardChunkHeaderV3::new(
            PROTOCOL_VERSION,
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
            congestion_info,
            &EmptyValidatorSigner::default().into(),
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

#[cfg(test)]
mod tests {
    use bytesize::ByteSize;
    use near_primitives_core::hash::CryptoHash;
    use std::io::ErrorKind;

    use crate::stateless_validation::state_witness::{ChunkStateWitness, EncodedChunkStateWitness};

    #[test]
    fn encode_decode_state_dummy_witness_default_limit() {
        let original_witness = ChunkStateWitness::new_dummy(42, 0, CryptoHash::default());
        let (encoded_witness, borsh_bytes_from_encode) =
            EncodedChunkStateWitness::encode(&original_witness).unwrap();
        let (decoded_witness, borsh_bytes_from_decode) =
            EncodedChunkStateWitness::from_boxed_slice(encoded_witness.0).decode().unwrap();
        assert_eq!(decoded_witness, original_witness);
        assert_eq!(borsh_bytes_from_encode, borsh_bytes_from_decode);
        assert_eq!(borsh::to_vec(&original_witness).unwrap().len(), borsh_bytes_from_encode);
    }

    #[test]
    fn encode_decode_state_dummy_witness_within_limit() {
        const LIMIT: ByteSize = ByteSize::mib(32);
        let original_witness = ChunkStateWitness::new_dummy(42, 0, CryptoHash::default());
        let (encoded_witness, borsh_bytes_from_encode) =
            EncodedChunkStateWitness::encode(&original_witness).unwrap();
        let (decoded_witness, borsh_bytes_from_decode) =
            EncodedChunkStateWitness::from_boxed_slice(encoded_witness.0)
                .decode_with_limit(LIMIT)
                .unwrap();
        assert_eq!(decoded_witness, original_witness);
        assert_eq!(borsh_bytes_from_encode, borsh_bytes_from_decode);
        assert_eq!(borsh::to_vec(&original_witness).unwrap().len(), borsh_bytes_from_encode);
    }

    #[test]
    fn encode_decode_state_dummy_witness_exceeds_limit() {
        const LIMIT: ByteSize = ByteSize::b(32);
        let original_witness = ChunkStateWitness::new_dummy(42, 0, CryptoHash::default());
        let (encoded_witness, borsh_bytes_from_encode) =
            EncodedChunkStateWitness::encode(&original_witness).unwrap();
        assert!(borsh_bytes_from_encode > LIMIT.as_u64() as usize);
        let error = EncodedChunkStateWitness::from_boxed_slice(encoded_witness.0)
            .decode_with_limit(LIMIT)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Other);
        assert_eq!(
            error.to_string(),
            "Decompressed data exceeded limit of 32 B: Exceeded the limit of 32 bytes"
        );
    }

    #[test]
    fn decode_state_dummy_witness_invalid_data() {
        let invalid_data = [0; 10];
        let error = EncodedChunkStateWitness::from_boxed_slice(Box::new(invalid_data))
            .decode()
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Other);
    }
}
