use super::state_witness::MAX_UNCOMPRESSED_STATE_WITNESS_SIZE;
use crate::sharding::ShardChunkHeader;
use crate::state::PartialState;
use crate::stateless_validation::contract_distribution::CodeBytes;
use crate::stateless_validation::state_witness::{ChunkStateWitnessV1, EncodedChunkStateWitness};
use crate::types::{AccountId, EpochId};
use crate::utils::compression::CompressedData;
use crate::utils::io::CountingRead;
use borsh::BorshDeserialize;
use bytes::Buf;
use bytesize::ByteSize;
use std::io::Cursor;
use std::io::Read;
use std::sync::{Arc, OnceLock};

use super::{ChunkProductionKey, state_witness::ChunkStateWitness};

/// LazyChunkStateWitness provides immediate access to header fields while
/// deserializing heavy data in the background. When heavy data is first needed,
/// consume this witness to get a normal ChunkStateWitness.
#[derive(Clone)]
pub struct LazyChunkStateWitness {
    // Light fields - available immediately
    epoch_id: EpochId,
    chunk_header: ShardChunkHeader,

    // Complete witness data - loaded lazily via background thread
    complete_witness: Arc<OnceLock<ChunkStateWitness>>,

    // Accessed contracts to be merged when converting to full witness
    accessed_contracts: Vec<CodeBytes>,
}

impl LazyChunkStateWitness {
    /// Creates a LazyChunkStateWitness from compressed witness data.
    pub fn from_encoded_witness(
        encoded_witness: &EncodedChunkStateWitness,
        protocol_version: near_primitives_core::types::ProtocolVersion,
    ) -> Result<(Self, usize), Box<dyn std::error::Error + Send + Sync>> {
        use near_primitives_core::version::ProtocolFeature;

        let (raw_bytes, raw_size) =
            if ProtocolFeature::VersionedStateWitness.enabled(protocol_version) {
                Self::safe_decompress_to_bytes(encoded_witness)?
            } else {
                // Slow, but just for backward compatibility.
                let (witness, raw_size): (ChunkStateWitnessV1, _) = encoded_witness.decode()?;
                let wrapped = ChunkStateWitness::V1(witness);
                let bytes = borsh::to_vec(&wrapped)?;
                (bytes, raw_size)
            };

        let lazy_witness = Self::from_bytes(&raw_bytes)?;
        Ok((lazy_witness, raw_size))
    }

    /// Create a LazyChunkStateWitness from an already-decoded ChunkStateWitness.
    pub fn from_full_witness(witness: ChunkStateWitness) -> Self {
        let epoch_id = witness.epoch_id().clone();
        let chunk_header = witness.chunk_header().clone();

        let complete_witness = Arc::new(OnceLock::new());
        // The witness is already available, so populate it immediately
        let _ = complete_witness.set(witness);

        Self { epoch_id, chunk_header, complete_witness, accessed_contracts: Vec::new() }
    }

    /// Safely decompress EncodedChunkStateWitness to raw bytes with the same
    /// decompression bomb protection as CompressedData::decode() but without
    /// doing immediate borsh deserialization.
    fn safe_decompress_to_bytes(
        encoded: &EncodedChunkStateWitness,
    ) -> Result<(Vec<u8>, usize), Box<dyn std::error::Error + Send + Sync>> {
        let limit = ByteSize(MAX_UNCOMPRESSED_STATE_WITNESS_SIZE);

        let mut counting_read = CountingRead::new_with_limit(
            zstd::stream::Decoder::new(encoded.as_ref().reader())?,
            limit,
        );

        let mut decompressed = Vec::new();
        counting_read.read_to_end(&mut decompressed)?;

        let raw_size = counting_read.bytes_read().as_u64() as usize;
        Ok((decompressed, raw_size))
    }

    /// Creates a LazyChunkStateWitness from serialized bytes with header-first parsing.
    /// Header fields (epoch_id, chunk_header) are available immediately.
    /// Complete witness is loaded asynchronously in the background via dedicated thread.
    pub fn from_bytes(data: &[u8]) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut cursor = Cursor::new(data);

        let variant: u8 = BorshDeserialize::deserialize_reader(&mut cursor)?;

        let (epoch_id, chunk_header) = match variant {
            0 => {
                // V1: chunk_producer, epoch_id, chunk_header, then heavy data
                let _chunk_producer: AccountId = BorshDeserialize::deserialize_reader(&mut cursor)?;
                let epoch_id: EpochId = BorshDeserialize::deserialize_reader(&mut cursor)?;
                let chunk_header: ShardChunkHeader =
                    BorshDeserialize::deserialize_reader(&mut cursor)?;
                (epoch_id, chunk_header)
            }
            1 => {
                // V2: epoch_id, chunk_header, then heavy data
                let epoch_id: EpochId = BorshDeserialize::deserialize_reader(&mut cursor)?;
                let chunk_header: ShardChunkHeader =
                    BorshDeserialize::deserialize_reader(&mut cursor)?;
                (epoch_id, chunk_header)
            }
            _ => return Err(format!("Unknown ChunkStateWitness variant: {}", variant).into()),
        };

        let complete_witness = Arc::new(OnceLock::new());
        let complete_witness_clone = complete_witness.clone();

        // Spawn dedicated thread to deserialize complete witness in background
        let data_shared: Arc<[u8]> = Arc::from(data);
        std::thread::spawn(move || {
            if let Ok(witness) = Self::deserialize_complete_witness(&data_shared) {
                let _ = complete_witness_clone.set(witness);
            }
        });

        Ok(LazyChunkStateWitness {
            epoch_id,
            chunk_header,
            complete_witness,
            accessed_contracts: Vec::new(),
        })
    }

    /// Deserialize the complete witness from serialized bytes
    fn deserialize_complete_witness(
        data: &[u8],
    ) -> Result<ChunkStateWitness, Box<dyn std::error::Error + Send + Sync>> {
        // TODO: could be optimized to skip header re-parsing
        let witness: ChunkStateWitness = borsh::from_slice(data)?;
        Ok(witness)
    }

    /// Check if the complete witness is ready without blocking
    pub fn is_ready(&self) -> bool {
        self.complete_witness.get().is_some()
    }

    /// Add accessed contracts that will be merged into the main state transition
    /// when converting to full ChunkStateWitness.
    pub fn add_accessed_contracts(&mut self, contracts: Vec<CodeBytes>) {
        self.accessed_contracts.extend(contracts);
    }

    /// Consume this lazy witness to get a complete ChunkStateWitness.
    /// This will block if the background deserialization is not yet complete.
    /// Accessed contracts will be merged into the main state transition.
    pub fn into_chunk_state_witness(self) -> ChunkStateWitness {
        // Wait for background thread to complete and get the witness
        let mut witness = loop {
            if let Some(_) = self.complete_witness.get() {
                // Try to extract the witness from the Arc if we're the only owner
                match Arc::try_unwrap(self.complete_witness) {
                    Ok(once_lock) => {
                        break once_lock.into_inner().expect("OnceLock should be filled");
                    }
                    Err(arc) => {
                        break arc.get().expect("OnceLock should be filled").clone();
                    }
                }
            }
            // If not ready, yield and retry
            // TODO: could be improved with condvar for more efficient waiting
            std::thread::yield_now();
        };

        // Merge accessed contracts into the main state transition
        if !self.accessed_contracts.is_empty() {
            let PartialState::TrieValues(values) =
                &mut witness.mut_main_state_transition().base_state;
            values.extend(self.accessed_contracts.into_iter().map(|code| code.0.into()));
        }

        witness
    }

    pub fn chunk_production_key(&self) -> ChunkProductionKey {
        ChunkProductionKey {
            shard_id: self.chunk_header.shard_id(),
            epoch_id: self.epoch_id,
            height_created: self.chunk_header.height_created(),
        }
    }

    pub fn epoch_id(&self) -> &EpochId {
        &self.epoch_id
    }

    pub fn chunk_header(&self) -> &ShardChunkHeader {
        &self.chunk_header
    }
}

impl std::fmt::Debug for LazyChunkStateWitness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyChunkStateWitness")
            .field("epoch_id", &self.epoch_id)
            .field("chunk_header", &self.chunk_header)
            .field("is_ready", &self.is_ready())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stateless_validation::state_witness::ChunkStateWitness;

    #[test]
    fn test_witness() {
        use crate::sharding::ShardChunkHeader;
        use crate::stateless_validation::state_witness::ChunkStateTransition;
        use crate::transaction::SignedTransaction;
        use crate::types::EpochId;
        use near_primitives_core::hash::CryptoHash;
        use near_primitives_core::types::ShardId;
        use std::collections::HashMap;

        // Create a normal ChunkStateWitness with some random values
        let epoch_id = EpochId(CryptoHash::hash_bytes(b"test_epoch"));
        let shard_id: ShardId = ShardId::new(42);
        let height = 12345;
        let prev_block_hash = CryptoHash::hash_bytes(b"prev_block");

        let chunk_header = ShardChunkHeader::new_dummy(height, shard_id, prev_block_hash);

        let main_state_transition = ChunkStateTransition {
            block_hash: CryptoHash::hash_bytes(b"block_hash"),
            base_state: Default::default(),
            post_state_root: CryptoHash::hash_bytes(b"post_state_root"),
        };

        let applied_receipts_hash = CryptoHash::hash_bytes(b"receipts_hash");

        let transactions = vec![
            SignedTransaction::empty(CryptoHash::hash_bytes(b"tx_hash_1")),
            SignedTransaction::empty(CryptoHash::hash_bytes(b"tx_hash_2")),
        ];

        let implicit_transitions = vec![
            ChunkStateTransition {
                block_hash: CryptoHash::hash_bytes(b"implicit_block_1"),
                base_state: Default::default(),
                post_state_root: CryptoHash::hash_bytes(b"implicit_post_state_1"),
            },
            ChunkStateTransition {
                block_hash: CryptoHash::hash_bytes(b"implicit_block_2"),
                base_state: Default::default(),
                post_state_root: CryptoHash::hash_bytes(b"implicit_post_state_2"),
            },
        ];

        let original_witness = ChunkStateWitness::new(
            "test_producer.near".parse().unwrap(),
            epoch_id,
            chunk_header.clone(),
            main_state_transition.clone(),
            HashMap::new(),
            applied_receipts_hash,
            transactions,
            implicit_transitions,
            near_primitives_core::version::PROTOCOL_VERSION,
        );

        // Serialize the witness
        let serialized = borsh::to_vec(&original_witness).expect("Serialization should work");

        // Create LazyChunkStateWitness from serialized data
        let lazy_witness = LazyChunkStateWitness::from_bytes(&serialized)
            .expect("LazyChunkStateWitness creation should work");

        // Check header fields are immediately accessible and correct
        assert_eq!(lazy_witness.epoch_id(), &epoch_id);
        assert_eq!(lazy_witness.chunk_header().shard_id(), shard_id);
        assert_eq!(lazy_witness.chunk_header().height_created(), height);
        assert_eq!(lazy_witness.chunk_header().prev_block_hash(), &prev_block_hash);

        let production_key = lazy_witness.chunk_production_key();
        assert_eq!(production_key.shard_id, shard_id);
        assert_eq!(production_key.epoch_id, epoch_id);
        assert_eq!(production_key.height_created, height);

        // Convert back to ChunkStateWitness (this will wait for background deserialization)
        let reconstructed_witness = lazy_witness.into_chunk_state_witness();

        // The entire witness should be equal
        assert_eq!(reconstructed_witness, original_witness);
    }
}
